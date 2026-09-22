//! HTTP -> Mainline -> signed tracker -> real iroh-blobs integration tests.

use std::{
    net::{Ipv4Addr, SocketAddrV4},
    time::Duration,
};

use iroh::{Endpoint, address_lookup::memory::MemoryLookup, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, Hash, format::collection::Collection, store::mem::MemStore};
use iroh_local_gateway::Gateway;
use iroh_mainline_endpoint_discovery::{Directory, Resolver, SignedRecord, infohash_from_blake3};
use n0_mainline::Dht;
use reqwest::{Client, StatusCode};
use udp_address_records::{Limits, Server};

#[tokio::test]
async fn streams_video_and_ranges_through_discovery() {
    tokio::time::timeout(Duration::from_secs(60), run())
        .await
        .expect("gateway integration timed out");
}

async fn run() {
    let network = n0_mainline::Testnet::new(3).await.unwrap();
    let node = || {
        Dht::builder()
            .bootstrap(&network.bootstrap)
            .port(0)
            .build()
            .unwrap()
    };
    let tracker = Server::new(Limits::for_tests());
    let tracker_handle = tracker.attach(node()).await.unwrap();
    let tracker_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, tracker_handle.local_addr().port());

    // A blob larger than the Bao block size, with an MP4 ftyp header.
    let mut video: Vec<u8> = (0..512 * 1024 + 123).map(|n| (n % 251) as u8).collect();
    video[..24].copy_from_slice(b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom");
    let text = b"hello from a discovered peer\n".to_vec();
    let store = MemStore::new();
    let video_tag = store.blobs().add_bytes(video.clone()).await.unwrap();
    let text_tag = store.blobs().add_bytes(text.clone()).await.unwrap();
    let empty_tag = store.blobs().add_bytes(Vec::new()).await.unwrap();
    let collection: Collection = [
        ("notes/hello world.txt".to_string(), text_tag.hash),
        ("video.mp4".to_string(), video_tag.hash),
    ]
    .into_iter()
    .collect();
    let collection_tag = collection.store(&store).await.unwrap();
    let provider = Endpoint::builder(presets::Minimal)
        .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
        .unwrap()
        .bind()
        .await
        .unwrap();
    let provider_router = Router::builder(provider.clone())
        .accept(iroh_blobs::ALPN, BlobsProtocol::new(&store, None))
        .spawn();
    let publisher_dht = node();
    let directory = Directory::udp(publisher_dht.clone(), tracker_addr)
        .await
        .unwrap();
    directory
        .publish(&SignedRecord::sign(provider.secret_key()))
        .await
        .unwrap();
    for hash in [
        video_tag.hash,
        text_tag.hash,
        empty_tag.hash,
        collection_tag.hash(),
    ] {
        let infohash = infohash_from_blake3(&blake3::Hash::from_bytes(*hash.as_bytes()));
        publisher_dht
            .announce_peer(infohash.into(), None)
            .await
            .unwrap();
    }
    let client_endpoint = Endpoint::builder(presets::Minimal)
        .address_lookup(MemoryLookup::from_endpoint_info([provider.addr()]))
        .bind()
        .await
        .unwrap();
    let gateway_dht = node();
    let directory = Directory::udp(gateway_dht.clone(), tracker_addr)
        .await
        .unwrap();
    let resolver = Resolver::bind(gateway_dht, directory).await.unwrap();
    let gateway = Gateway::new(client_endpoint.clone(), resolver);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        gateway
            .serve(listener, async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let url = |hash: Hash| format!("{base}/blake3/{}", z32::encode(hash.as_bytes()));
    let video_url = url(video_tag.hash);

    let res = client.get(&video_url).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-type"], "video/mp4");
    assert_eq!(res.headers()["accept-ranges"], "bytes");
    assert_eq!(res.content_length(), Some(video.len() as u64));
    let etag = res.headers()["etag"].to_str().unwrap().to_owned();
    assert_eq!(res.bytes().await.unwrap().as_ref(), video);

    for (range, start, end) in [
        ("bytes=0-0", 0, 1),
        ("bytes=1023-32770", 1023, 32771),
        ("bytes=520000-", 520000, video.len()),
        ("bytes=-123", video.len() - 123, video.len()),
        ("bytes=524400-999999", 524400, video.len()),
    ] {
        let res = client
            .get(&video_url)
            .header("range", range)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT, "{range}");
        assert_eq!(res.headers()["content-type"], "video/mp4");
        assert_eq!(
            res.headers()["content-range"],
            format!("bytes {start}-{}/{}", end - 1, video.len())
        );
        assert_eq!(res.content_length(), Some((end - start) as u64));
        assert_eq!(res.bytes().await.unwrap().as_ref(), &video[start..end]);
    }
    for range in ["bytes=999999-", "bytes=-0"] {
        let res = client
            .get(&video_url)
            .header("range", range)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            res.headers()["content-range"],
            format!("bytes */{}", video.len())
        );
    }
    // HEAD ignores Range and returns full metadata with no body.
    let res = client
        .head(&video_url)
        .header("range", "bytes=0-0")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-length"], video.len().to_string());
    assert!(res.bytes().await.unwrap().is_empty());
    for range in ["nonsense"] {
        let res = client
            .get(&video_url)
            .header("range", range)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.bytes().await.unwrap().as_ref(), video);
    }
    let res = client
        .get(&video_url)
        .header("range", "bytes=0-0,1023-1025,-2,999999-")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert!(res.headers().get("content-range").is_none());
    let content_type = res.headers()["content-type"].to_str().unwrap();
    let boundary = content_type
        .strip_prefix("multipart/byteranges; boundary=")
        .unwrap()
        .to_owned();
    let length = res.content_length().unwrap();
    let body = res.bytes().await.unwrap();
    let mut expected = Vec::new();
    for range in [0..1, 1023..1026, video.len() - 2..video.len()] {
        expected.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Type: video/mp4\r\nContent-Range: bytes {}-{}/{}\r\n\r\n",
                range.start,
                range.end - 1,
                video.len()
            )
            .as_bytes(),
        );
        expected.extend_from_slice(&video[range]);
        expected.extend_from_slice(b"\r\n");
    }
    expected.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    assert_eq!(body.as_ref(), expected);
    assert_eq!(body.len() as u64, length);
    let res = client
        .get(&video_url)
        .header("range", "bytes=0-0")
        .header("if-range", &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(res.bytes().await.unwrap().len(), 1);
    let res = client
        .get(&video_url)
        .header("range", "bytes=0-0")
        .header("if-range", "\"other\"")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.bytes().await.unwrap().as_ref(), video);
    let res = client
        .get(&video_url)
        .header("if-none-match", &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_MODIFIED);
    assert!(res.bytes().await.unwrap().is_empty());
    let res = client.get(url(text_tag.hash)).send().await.unwrap();
    assert!(
        res.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/plain")
    );
    assert_eq!(res.bytes().await.unwrap().as_ref(), text);
    let res = client.get(url(empty_tag.hash)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.content_length(), Some(0));
    assert!(res.bytes().await.unwrap().is_empty());
    let res = client
        .get(url(empty_tag.hash))
        .header("range", "bytes=0-0")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    let res = client
        .get(format!("{base}/blake3/not-a-hash"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let res = client
        .get(url(Hash::new(b"not announced")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let res = client
        .request(reqwest::Method::OPTIONS, &video_url)
        .header("origin", "http://example.com")
        .header("access-control-request-method", "GET")
        .header("access-control-request-headers", "range")
        .send()
        .await
        .unwrap();
    assert!(res.status().is_success());
    assert_eq!(res.headers()["access-control-allow-origin"], "*");

    let tree = format!("/tree/{}", z32::encode(collection_tag.hash().as_bytes()));
    let collection_url = format!("{base}{tree}");
    for top in [collection_url.clone(), format!("{collection_url}/")] {
        let res = client.get(&top).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()["content-type"], "text/html; charset=utf-8");
        let html = res.text().await.unwrap();
        assert!(html.contains(&format!("href=\"{tree}/notes/\"")));
        assert!(html.contains(&format!("href=\"{tree}/video.mp4\"")));
        assert!(!html.contains("hello"));
    }
    let res = client
        .get(format!("{collection_url}/notes"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let html = res.text().await.unwrap();
    assert!(html.contains(&format!("href=\"{tree}/\">../")));
    assert!(html.contains(&format!("href=\"{tree}/notes/hello%20world.txt\"")));
    assert!(!html.contains("video.mp4"));
    let res = client
        .get(format!("{collection_url}/notes/hello%20world.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.bytes().await.unwrap().as_ref(), text);
    let res = client
        .get(format!("{collection_url}/video.mp4"))
        .header("range", "bytes=1023-32770")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(res.headers()["content-type"], "video/mp4");
    assert_eq!(res.bytes().await.unwrap().as_ref(), &video[1023..32771]);
    let res = client
        .get(format!("{collection_url}/missing.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let res = client
        .get(format!(
            "{base}/tree/{}",
            z32::encode(text_tag.hash.as_bytes())
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);

    shutdown_tx.send(()).unwrap();
    task.await.unwrap();
    client_endpoint.close().await;
    provider_router.shutdown().await.unwrap();
}
