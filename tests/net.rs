//! End-to-end: UDP and mapping probes.

use std::time::Duration;

use iroh::{SecretKey, endpoint::presets, protocol::Router};
use iroh_endpoint_tracker::{
    Limits, PROBE_ALPN, ProbeAccept, Server, SignedRecord, UdpClient, confirm_records,
    confirm_socket,
};

const PING_ALPN: &[u8] = b"iroh/ping/0";

async fn listening() -> (Router, iroh::Endpoint, std::net::SocketAddrV4) {
    let ep = iroh::Endpoint::bind(presets::Minimal).await.unwrap();
    let router = Router::builder(ep.clone())
        .accept(PROBE_ALPN, ProbeAccept)
        .spawn();
    let addr = router
        .endpoint()
        .addr()
        .ip_addrs()
        .find_map(|a| match a {
            std::net::SocketAddr::V4(addr) => Some(*addr),
            _ => None,
        })
        .expect("bound ipv4");
    (router, ep, addr)
}

#[tokio::test]
async fn udp_publish_resolve() {
    let server = Server::new(Limits::for_tests());
    let udp = server
        .bind_udp("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let dir = udp.local_addr();
    let client = UdpClient::bind().await.unwrap();

    let sk = SecretKey::generate();
    let mapping: std::net::SocketAddrV4 = "127.0.0.1:6881".parse().unwrap();
    let rec = SignedRecord::sign(&sk, vec![mapping], [b"test/0"]);
    client.publish_to(dir, rec).await.unwrap();

    let res = client.resolve_from(dir, mapping).await.unwrap();
    assert!(!res.truncated);
    assert_eq!(res.records.len(), 1);
    assert_eq!(res.records[0].eid, sk.public());
}

#[tokio::test]
async fn udp_source_verification_is_configurable() {
    let mapping = "203.0.113.9:6881".parse().unwrap();
    let record = SignedRecord::sign(&SecretKey::generate(), vec![mapping], [b"test/0"]);

    let checked = Server::new(Limits {
        verify_udp_source_ip: true,
        ..Limits::for_tests()
    });
    let checked_udp = checked
        .bind_udp("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let client = UdpClient::bind().await.unwrap();
    client
        .publish_to(checked_udp.local_addr(), record.clone())
        .await
        .unwrap();
    assert!(client.resolve(mapping).await.unwrap().records.is_empty());

    let unchecked = Server::new(Limits {
        verify_udp_source_ip: false,
        ..Limits::for_tests()
    });
    let unchecked_udp = unchecked
        .bind_udp("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    client
        .publish_to(unchecked_udp.local_addr(), record)
        .await
        .unwrap();
    assert_eq!(client.resolve(mapping).await.unwrap().records.len(), 1);
}

#[tokio::test]
async fn concurrent_udp_resolves_for_same_addr_both_complete() {
    let server = Server::new(Limits::for_tests());
    let udp = server
        .bind_udp("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let client = UdpClient::bind().await.unwrap();
    client.add_tracker(udp.local_addr()).await.unwrap();

    let mapping = "127.0.0.1:6881".parse().unwrap();
    let rec = SignedRecord::sign(&SecretKey::generate(), vec![mapping], [b"test/0"]);
    client.publish(rec).await.unwrap();

    let (a, b) = tokio::join!(client.resolve(mapping), client.resolve(mapping));
    assert_eq!(a.unwrap().records.len(), 1);
    assert_eq!(b.unwrap().records.len(), 1);
}

#[tokio::test]
async fn invalid_contended_publish_does_not_evict() {
    let server = Server::new(Limits::for_tests());
    let probe = iroh::Endpoint::bind(presets::Minimal).await.unwrap();
    server.set_probe(probe);

    let mapping = "203.0.113.9:6881".parse().unwrap();
    let original = SignedRecord::sign(&SecretKey::generate(), vec![mapping], [b"test/0"]);
    server.publish_local(original.clone()).unwrap();

    let mut invalid = SignedRecord::sign(&SecretKey::generate(), vec![mapping], [b"test/0"]);
    invalid.payload.v1_mut().alpns[0] = b"tampered/0".as_slice().into();
    assert!(server.publish_confirmed_local(invalid).await.is_err());

    let rows = server.lookup_local(mapping);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].eid, original.eid);
}

#[tokio::test]
async fn udp_rejects_bad_sig() {
    let server = Server::new(Limits::for_tests());
    let udp = server
        .bind_udp("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let client = UdpClient::bind().await.unwrap();
    let mut rec = SignedRecord::sign(
        &SecretKey::generate(),
        vec!["127.0.0.1:1".parse().unwrap()],
        [b"test/0"],
    );
    rec.payload.v1_mut().addrs[0] = "127.0.0.1:2".parse().unwrap();
    client.publish_to(udp.local_addr(), rec).await.unwrap();
    let res = client
        .resolve_from(udp.local_addr(), "127.0.0.1:2".parse().unwrap())
        .await
        .unwrap();
    assert!(res.records.is_empty());
}

#[tokio::test]
async fn probe_accepts_real_host() {
    let (router, ep, addr) = listening().await;
    let probe = iroh::Endpoint::bind(presets::Minimal).await.unwrap();
    assert!(
        confirm_socket(&probe, ep.id(), addr, [PROBE_ALPN], Duration::from_secs(3)).await,
        "owner of {addr} should complete probe"
    );
    router.shutdown().await.unwrap();
}

#[tokio::test]
async fn probe_rejects_foreign_eid_on_known_socket() {
    let (router_a, ep_a, addr_a) = listening().await;
    let (router_b, ep_b, _) = listening().await;
    let probe = iroh::Endpoint::bind(presets::Minimal).await.unwrap();

    assert!(
        !confirm_socket(
            &probe,
            ep_b.id(),
            addr_a,
            [PROBE_ALPN],
            Duration::from_secs(2)
        )
        .await,
        "eid B must not confirm on A's socket {addr_a}"
    );

    let rec_a = SignedRecord::sign(ep_a.secret_key(), vec![addr_a], [PROBE_ALPN]);
    let rec_b = SignedRecord::sign(ep_b.secret_key(), vec![addr_a], [PROBE_ALPN]);
    rec_b.verify_sig().unwrap();

    let kept = confirm_records(
        &probe,
        addr_a,
        vec![rec_a.clone(), rec_b],
        Duration::from_secs(3),
    )
    .await;
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].eid, rec_a.eid);

    router_a.shutdown().await.unwrap();
    router_b.shutdown().await.unwrap();
}

#[tokio::test]
async fn probe_uses_announced_alpns_not_guesses() {
    let ep = iroh::Endpoint::bind(presets::Minimal).await.unwrap();
    let router = Router::builder(ep.clone())
        .accept(PING_ALPN, ProbeAccept)
        .spawn();
    let addr = router
        .endpoint()
        .addr()
        .ip_addrs()
        .find_map(|a| match a {
            std::net::SocketAddr::V4(addr) => Some(*addr),
            _ => None,
        })
        .expect("bound ipv4");
    let probe = iroh::Endpoint::bind(presets::Minimal).await.unwrap();

    let rec_wrong = SignedRecord::sign(ep.secret_key(), vec![addr], [PROBE_ALPN]);
    let rec_right = SignedRecord::sign(ep.secret_key(), vec![addr], [PING_ALPN]);

    assert!(
        !confirm_socket(&probe, ep.id(), addr, [PROBE_ALPN], Duration::from_secs(2)).await,
        "must not guess ping/blobs when the announcement lists a different ALPN"
    );
    assert!(
        confirm_socket(&probe, ep.id(), addr, [PING_ALPN], Duration::from_secs(3)).await,
        "peer that only speaks ping should confirm when ping is offered"
    );

    let kept = confirm_records(
        &probe,
        addr,
        vec![rec_wrong, rec_right.clone()],
        Duration::from_secs(3),
    )
    .await;
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].alpns, rec_right.alpns);

    router.shutdown().await.unwrap();
}
