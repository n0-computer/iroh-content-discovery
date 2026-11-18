//! End-to-end tests for the opaque UDP protocol.

use std::{net::SocketAddr, time::Duration};

use iroh_base::SecretKey;
use iroh_mainline_endpoint_discovery::{AddrIndex, Publisher, Resolver, UdpClient};
use n0_future::StreamExt;
use n0_mainline::Dht;
use tokio::net::UdpSocket;
use udp_addr_index::{Limits, Server};
use udp_addr_index_proto::{MAX_DGRAM, Proto, Request, RequestV1, Response, ResponseV1};

#[tokio::test]
async fn an_unannounced_server_stores_and_returns_opaque_bytes() {
    let server = Server::new(Limits::for_tests());
    let handle = server
        .attach_with_rendezvous(test_dht(), None)
        .await
        .unwrap();
    let dht = test_dht();
    let client = UdpClient::attach(dht.clone()).await.unwrap();
    client
        .add_server(v4(loopback(handle.local_addr())))
        .await
        .unwrap();

    let value = b"not a signed record".to_vec();
    let addrs = client
        .publish({
            let value = value.clone();
            move |_| value.clone()
        })
        .await
        .unwrap();
    assert_eq!(addrs.len(), 1);
    assert!(addrs[0].ip().is_loopback());
    assert_eq!(
        addrs[0].port(),
        dht.info().await.unwrap().local_addr().port()
    );
    assert_eq!(client.resolve(addrs[0]).await.unwrap().values, [value]);
}

#[tokio::test]
async fn put_token_is_bound_to_exact_udp_socket_but_get_is_public() {
    let server = Server::new(Limits::for_tests());
    let handle = server.attach(test_dht()).await.unwrap();
    let owner = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let attacker = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    let prepare = Request::V1(RequestV1::Prepare {
        tx: 1,
        padding: [0; 24],
    });
    send(&owner, loopback(handle.local_addr()), prepare.clone()).await;
    let Response::V1(ResponseV1::Prepared { addr, token, .. }) = recv(&owner).await else {
        panic!("unexpected prepare response")
    };

    let stolen = Request::V1(RequestV1::Put {
        tx: 2,
        token,
        value: b"stolen".to_vec(),
    });
    send(&attacker, loopback(handle.local_addr()), stolen.clone()).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), recv(&attacker))
            .await
            .is_err(),
        "invalid put must not be acknowledged"
    );
    assert!(server.get_local(addr).is_none());

    let valid = Request::V1(RequestV1::Put {
        tx: 4,
        token,
        value: b"owned".to_vec(),
    });
    send(&owner, loopback(handle.local_addr()), valid.clone()).await;
    let Response::V1(ResponseV1::Stored { addr: stored, .. }) = recv(&owner).await else {
        panic!("unexpected put response")
    };
    assert_eq!(stored, addr);
    assert_eq!(server.get_local(addr).unwrap(), b"owned");

    let public_get = Request::V1(RequestV1::Get { tx: 5, addr });
    send(&attacker, loopback(handle.local_addr()), public_get.clone()).await;
    let Response::V1(ResponseV1::Value {
        addr: returned,
        value,
        ..
    }) = recv(&attacker).await
    else {
        panic!("unexpected get response")
    };
    assert_eq!(returned, addr);
    assert_eq!(value.unwrap(), b"owned");
}

#[tokio::test]
async fn get_requires_full_sized_datagram() {
    let server = Server::new(Limits::for_tests());
    let handle = server.attach(test_dht()).await.unwrap();
    let reader = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = "203.0.113.1:1234".parse().unwrap();
    let value = vec![42; udp_addr_index_proto::MAX_VALUE_LEN];
    server.put_local(addr, value.clone()).unwrap();
    let request = Request::V1(RequestV1::Get { tx: 7, addr });
    let mut buf = [0; MAX_DGRAM];
    let bytes = Proto::Request(request.clone()).encode(&mut buf).unwrap();
    reader
        .send_to(&bytes[..MAX_DGRAM - 1], loopback(handle.local_addr()))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), recv(&reader))
            .await
            .is_err()
    );
    let mut oversized = bytes.to_vec();
    oversized.push(0);
    reader
        .send_to(&oversized, loopback(handle.local_addr()))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), recv(&reader))
            .await
            .is_err()
    );
    send(&reader, loopback(handle.local_addr()), request.clone()).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), recv(&reader))
            .await
            .unwrap(),
        Response::V1(ResponseV1::Value {
            tx: 7,
            addr,
            value: Some(value)
        })
    );
}

#[tokio::test]
async fn concurrent_reads_are_demultiplexed_by_transaction() {
    let server = Server::new(Limits::for_tests());
    let handle = server.attach(test_dht()).await.unwrap();
    let client = UdpClient::attach(test_dht()).await.unwrap();
    client
        .add_server(v4(loopback(handle.local_addr())))
        .await
        .unwrap();
    let addr = client.publish(|_| b"value".to_vec()).await.unwrap()[0];

    let (left, right) = tokio::join!(client.resolve(addr), client.resolve(addr));
    assert_eq!(left.unwrap().values, [b"value".to_vec()]);
    assert_eq!(right.unwrap().values, [b"value".to_vec()]);
}

#[tokio::test]
async fn discovery_directory_validates_opaque_record() {
    let server = Server::new(Limits::for_tests());
    let handle = server.attach(test_dht()).await.unwrap();
    let index = AddrIndex::udp(test_dht(), v4(loopback(handle.local_addr())))
        .await
        .unwrap();
    let secret = SecretKey::generate();
    let addr = index.publish(&secret).await.unwrap()[0];
    let records = index.lookup(addr).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].endpoint_id, secret.public());
    assert_eq!(records[0].addr(), addr);

    server.put_local(addr, b"invalid".to_vec()).unwrap();
    assert!(index.lookup(addr).await.unwrap().is_empty());
}

#[tokio::test]
async fn cached_lookups_reach_the_server_once() {
    let server = Server::new(Limits::for_tests());
    let handle = server.attach(test_dht()).await.unwrap();
    let index = AddrIndex::builder(test_dht())
        .server(v4(loopback(handle.local_addr())))
        .lookup_cache(Duration::from_secs(60))
        .build()
        .await
        .unwrap();
    let addr = index.publish(&SecretKey::generate()).await.unwrap()[0];
    // Concurrent lookups share one request; later ones, from any clone, hit the cache.
    let (first, second) = tokio::join!(index.lookup(addr), index.lookup(addr));
    assert_eq!(first.unwrap().len(), 1);
    assert_eq!(second.unwrap().len(), 1);
    assert_eq!(index.clone().lookup(addr).await.unwrap().len(), 1);
    assert_eq!(server.metrics().gets.get(), 1);
}

#[tokio::test]
async fn a_dead_index_server_does_not_delay_found_records() {
    let server = Server::new(Limits::for_tests());
    let handle = server.attach(test_dht()).await.unwrap();
    let timeout = Duration::from_millis(500);
    let index = AddrIndex::builder(test_dht())
        .server(v4(loopback(handle.local_addr())))
        // Nothing listens on the discard port.
        .server("127.0.0.1:9".parse().unwrap())
        .timeout(timeout)
        .build()
        .await
        .unwrap();
    let secret = SecretKey::generate();
    // Publishing succeeds with the live server alone.
    let addr = index.publish(&secret).await.unwrap()[0];
    let started = std::time::Instant::now();
    let records = index.lookup(addr).await.unwrap();
    assert_eq!(records[0].endpoint_id, secret.public());
    assert!(started.elapsed() < timeout / 2, "{:?}", started.elapsed());
    // Without a record, the dead server might hold one, so the lookup waits.
    let started = std::time::Instant::now();
    let missing = "127.0.0.1:1".parse().unwrap();
    assert!(index.lookup(missing).await.unwrap().is_empty());
    assert!(started.elapsed() >= timeout);
}

/// Reads are public, so a record can be copied.
///
/// It is signed for the socket it was stored under, so a reader discards it
/// anywhere else.
#[tokio::test]
async fn a_copied_record_does_not_resolve_under_another_socket() {
    let server = Server::new(Limits::for_tests());
    let handle = server.attach(test_dht()).await.unwrap();
    let server_addr = v4(loopback(handle.local_addr()));
    let index = AddrIndex::udp(test_dht(), server_addr).await.unwrap();
    let secret = SecretKey::generate();
    let victim_addr = index.publish(&secret).await.unwrap()[0];
    let stolen = index.lookup(victim_addr).await.unwrap()[0].encode();

    let attacker = UdpClient::attach(test_dht()).await.unwrap();
    attacker.add_server(server_addr).await.unwrap();
    let attacker_addr = attacker.publish(move |_| stolen.clone()).await.unwrap()[0];

    assert_ne!(attacker_addr, victim_addr);
    // The server stored the bytes, because the attacker does receive there.
    assert!(server.get_local(attacker_addr).is_some());
    // A reader still only finds the victim's endpoint at the victim's socket.
    assert!(index.lookup(attacker_addr).await.unwrap().is_empty());
    assert_eq!(
        index.lookup(victim_addr).await.unwrap()[0].endpoint_id,
        secret.public()
    );
}

#[tokio::test]
async fn servers_are_discovered_on_mainline_and_share_the_announced_socket() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let network = n0_mainline::Testnet::new(3).await.unwrap();
        let server_dht = Dht::builder()
            .bootstrap(&network.bootstrap)
            .port(0)
            .build()
            .unwrap();
        let server_port = server_dht.info().await.unwrap().local_addr().port();
        let server = Server::default();
        let handle = server.attach(server_dht.clone()).await.unwrap();
        assert_eq!(handle.local_addr().port(), server_port);
        let reader_dht = Dht::builder()
            .bootstrap(&network.bootstrap)
            .port(0)
            .build()
            .unwrap();
        let index = loop {
            match AddrIndex::builder(reader_dht.clone())
                .rendezvous_hash(udp_addr_index_proto::RENDEZVOUS_INFOHASH)
                .build()
                .await
            {
                Ok(index) => break index,
                Err(iroh_mainline_endpoint_discovery::UdpError::NoServers { .. }) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(err) => panic!("discovery failed: {err}"),
            }
        };
        let secret = SecretKey::generate();
        let addr = index.publish(&secret).await.unwrap()[0];
        assert_eq!(
            addr.port(),
            reader_dht.info().await.unwrap().local_addr().port()
        );
        let records = index.lookup(addr).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].endpoint_id, secret.public());
        assert!(server.get_local(addr).is_some());
        drop(handle);
        // Detaching the index leaves the externally owned Mainline node alive.
        assert_eq!(
            server_dht.info().await.unwrap().local_addr().port(),
            server_port
        );
    })
    .await
    .expect("local rendezvous discovery timed out");
}

#[tokio::test]
async fn resolver_stream_yields_all_announced_endpoints() {
    tokio::time::timeout(Duration::from_secs(30), async {
        // Each DHT store retains one peer per IP and infohash. Keep the two
        // localhost publishers in independent stores, then query both stores.
        let networks = [
            n0_mainline::Testnet::new(1).await.unwrap(),
            n0_mainline::Testnet::new(1).await.unwrap(),
        ];
        let bootstrap: Vec<_> = networks
            .iter()
            .flat_map(|network| network.bootstrap.clone())
            .collect();
        let node =
            |bootstrap: &[String]| Dht::builder().bootstrap(bootstrap).port(0).build().unwrap();
        let server = Server::new(Limits::for_tests());
        let handle = server.attach(node(&bootstrap)).await.unwrap();
        let server_addr = v4(loopback(handle.local_addr()));
        let infohash = n0_mainline::Id::from([71; 20]);
        let mut expected = Vec::new();
        for network in &networks {
            let dht = node(&network.bootstrap);
            let index = AddrIndex::udp(dht.clone(), server_addr).await.unwrap();
            let secret = SecretKey::generate();
            expected.push(secret.public());
            index.publish(&secret).await.unwrap();
            dht.get_closest_nodes(infohash).await.unwrap();
            dht.announce_peer(infohash, None).await.unwrap();
        }
        let reader = node(&bootstrap);
        let resolver = Resolver::new(
            reader.clone(),
            AddrIndex::udp(reader, server_addr).await.unwrap(),
        );
        let mut stream = resolver.resolve_stream(infohash);
        let mut actual = Vec::new();
        while let Some(id) = stream.next().await {
            actual.push(id);
        }
        actual.sort();
        actual.dedup();
        expected.sort();
        assert_eq!(actual, expected);

        let mut continuous = resolver.resolve_continuously(infohash);
        let first = continuous.next().await.unwrap();
        assert!(expected.contains(&first));
        for _ in 0..4 {
            let next = tokio::time::timeout(Duration::from_secs(10), continuous.next())
                .await
                .expect("continuous lookup stalled")
                .unwrap();
            assert!(expected.contains(&next));
        }
    })
    .await
    .expect("streaming discovery timed out");
}

#[tokio::test]
async fn publisher_announces_endpoint_for_resolver() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let network = n0_mainline::Testnet::new(3).await.unwrap();
        let node = || {
            Dht::builder()
                .bootstrap(&network.bootstrap)
                .port(0)
                .build()
                .unwrap()
        };
        let server = Server::new(Limits::for_tests());
        let handle = server.attach(node()).await.unwrap();
        let server_addr = v4(loopback(handle.local_addr()));

        let dht = node();
        let publisher = Publisher::new(
            SecretKey::generate(),
            dht.clone(),
            AddrIndex::udp(dht, server_addr).await.unwrap(),
        );
        let infohash = n0_mainline::Id::from([72; 20]);
        publisher.add_infohash(infohash);

        let reader = node();
        let resolver = Resolver::new(
            reader.clone(),
            AddrIndex::udp(reader, server_addr).await.unwrap(),
        );
        // `wait_published` fires before the announcements, so keep looking
        // until the publisher's endpoint shows up.
        let mut found = resolver.resolve_continuously(infohash);
        assert_eq!(found.next().await, Some(publisher.id()));
    })
    .await
    .expect("publisher announcement was not resolved");
}

#[tokio::test]
async fn announcing_many_hashes_publishes_the_index_record_once() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let network = n0_mainline::Testnet::new(3).await.unwrap();
        let node = || {
            Dht::builder()
                .bootstrap(&network.bootstrap)
                .port(0)
                .build()
                .unwrap()
        };
        let server = Server::new(Limits::for_tests());
        let handle = server.attach(node()).await.unwrap();
        let server_addr = v4(loopback(handle.local_addr()));
        let dht = node();
        let publisher = Publisher::new(
            SecretKey::generate(),
            dht.clone(),
            AddrIndex::udp(dht, server_addr).await.unwrap(),
        );
        let hashes: Vec<_> = (80..84)
            .map(|byte| n0_mainline::Id::from([byte; 20]))
            .collect();
        for hash in &hashes {
            publisher.add_infohash(*hash);
        }
        let reader = node();
        let resolver = Resolver::new(
            reader.clone(),
            AddrIndex::udp(reader, server_addr).await.unwrap(),
        );
        for hash in &hashes {
            let mut found = resolver.resolve_continuously(*hash);
            assert_eq!(found.next().await, Some(publisher.id()));
        }
        // Each announcement checked the record, but nothing changed.
        assert_eq!(server.metrics().puts.get(), 1);
    })
    .await
    .expect("announcements were not resolved");
}

/// Readers could not resolve an announcement no index server knows about.
#[tokio::test]
async fn nothing_is_announced_without_an_index_server() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let network = n0_mainline::Testnet::new(3).await.unwrap();
        let node = || {
            Dht::builder()
                .bootstrap(&network.bootstrap)
                .port(0)
                .build()
                .unwrap()
        };
        // Nothing listens on the discard port, so every publication fails.
        let unreachable = "127.0.0.1:9".parse().unwrap();
        let dht = node();
        let publisher = Publisher::new(
            SecretKey::generate(),
            dht.clone(),
            AddrIndex::udp(dht, unreachable).await.unwrap(),
        );
        let infohash = n0_mainline::Id::from([90; 20]);
        publisher.add_infohash(infohash);
        // Give the publisher time to try: the index times out after two seconds.
        tokio::time::sleep(Duration::from_secs(5)).await;
        let mut peers = node().get_peers(infohash).await.unwrap();
        while let Some(batch) = peers.next().await {
            assert!(
                batch.is_empty(),
                "announced without an index record: {batch:?}"
            );
        }
        assert_eq!(publisher.public_v4(), None);
    })
    .await
    .expect("test timed out");
}

async fn send(socket: &UdpSocket, destination: std::net::SocketAddr, message: Request) {
    let mut buf = [0; MAX_DGRAM];
    let bytes = Proto::Request(message).encode(&mut buf).unwrap();
    socket.send_to(bytes, destination).await.unwrap();
}

async fn recv(socket: &UdpSocket) -> Response {
    let mut buf = [0; MAX_DGRAM];
    let (len, _) = socket.recv_from(&mut buf).await.unwrap();
    match Proto::decode(&buf[..len]).unwrap() {
        Proto::Response(response) => response,
        Proto::Request(request) => panic!("server answered with a request: {request:?}"),
    }
}

fn test_dht() -> Dht {
    Dht::builder().no_bootstrap().port(0).build().unwrap()
}

fn v4(addr: SocketAddr) -> std::net::SocketAddrV4 {
    match addr {
        SocketAddr::V4(addr) => addr,
        SocketAddr::V6(_) => panic!("test server must use IPv4"),
    }
}

fn loopback(addr: SocketAddr) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], addr.port()))
}

#[tokio::test]
async fn signed_bootstrap_works_without_rendezvous_announcements() {
    use iroh_mainline_endpoint_discovery::ServerList;
    tokio::time::timeout(Duration::from_secs(30), async {
        let network = n0_mainline::Testnet::new(3).await.unwrap();
        let writer = Dht::builder()
            .bootstrap(&network.bootstrap)
            .port(0)
            .build()
            .unwrap();
        let reader = Dht::builder()
            .bootstrap(&network.bootstrap)
            .port(0)
            .build()
            .unwrap();
        let key = n0_mainline::SigningKey::from_bytes(&[42; 32]);
        let list = ServerList::new(vec!["127.0.0.1:12345".parse().unwrap()]).unwrap();
        writer
            .put_mutable(list.sign(&key, 1).unwrap(), None)
            .await
            .unwrap();
        // No node has announced a rendezvous peer. Only the trusted record can
        // supply a candidate (discovery does not require it to be responsive).
        AddrIndex::builder(reader.clone())
            .list_key(key.verifying_key().to_bytes())
            .build()
            .await
            .unwrap();
        let other = n0_mainline::SigningKey::from_bytes(&[43; 32]);
        assert!(matches!(
            AddrIndex::builder(reader)
                .list_key(other.verifying_key().to_bytes())
                .build()
                .await,
            Err(iroh_mainline_endpoint_discovery::UdpError::NoServers { .. })
        ));
    })
    .await
    .expect("signed bootstrap timed out");
}

#[tokio::test]
async fn signed_list_precedes_custom_rendezvous_fallback() {
    use iroh_mainline_endpoint_discovery::ServerList;
    use std::net::{Ipv4Addr, SocketAddrV4};
    tokio::time::timeout(Duration::from_secs(30), async {
        let network = n0_mainline::Testnet::new(3).await.unwrap();
        let node = || {
            Dht::builder()
                .bootstrap(&network.bootstrap)
                .port(0)
                .build()
                .unwrap()
        };
        let signed_dht = node();
        let signed_server = Server::default();
        let _signed_handle = signed_server.attach(signed_dht.clone()).await.unwrap();
        let signed_addr = SocketAddrV4::new(
            Ipv4Addr::LOCALHOST,
            signed_dht.info().await.unwrap().local_addr().port(),
        );
        let fallback_dht = node();
        let fallback_server = Server::default();
        let hash = [91; 20];
        let _fallback_handle = fallback_server
            .attach_with_rendezvous(fallback_dht.clone(), Some(hash))
            .await
            .unwrap();
        // Explicitly await publication so the custom fallback is ready.
        fallback_dht.announce_peer(hash.into(), None).await.unwrap();
        let key = n0_mainline::SigningKey::from_bytes(&[44; 32]);
        signed_dht
            .put_mutable(
                ServerList::new(vec![signed_addr])
                    .unwrap()
                    .sign(&key, 1)
                    .unwrap(),
                None,
            )
            .await
            .unwrap();
        let index = AddrIndex::builder(node())
            .list_key(key.verifying_key().to_bytes())
            .rendezvous_hash(hash)
            .build()
            .await
            .unwrap();
        let secret = SecretKey::generate();
        let addr = index.publish(&secret).await.unwrap()[0];
        assert!(signed_server.get_local(addr).is_some());
        assert!(fallback_server.get_local(addr).is_none());
        let other = n0_mainline::SigningKey::from_bytes(&[45; 32]);
        let index = AddrIndex::builder(node())
            .list_key(other.verifying_key().to_bytes())
            .rendezvous_hash(hash)
            .build()
            .await
            .unwrap();
        let addr = index.publish(&secret).await.unwrap()[0];
        assert!(fallback_server.get_local(addr).is_some());
    })
    .await
    .expect("priority and fallback test timed out");
}

#[tokio::test]
async fn explicit_server_bypasses_both_discovery_sources() {
    let server = Server::new(Limits::for_tests());
    let handle = server.attach(test_dht()).await.unwrap();
    let server_addr = v4(loopback(handle.local_addr()));
    let dht = Dht::builder().no_bootstrap().port(0).build().unwrap();
    let index = tokio::time::timeout(
        Duration::from_secs(1),
        AddrIndex::builder(dht)
            .server(server_addr)
            .list_key([42; 32])
            .rendezvous_hash([91; 20])
            .build(),
    )
    .await
    .unwrap()
    .unwrap();
    let addr = index.publish(&SecretKey::generate()).await.unwrap()[0];
    assert!(server.get_local(addr).is_some());
}
