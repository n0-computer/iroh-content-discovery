//! End-to-end tests for the opaque UDP protocol.

use std::{net::SocketAddr, time::Duration};

use iroh_addr_index::{Limits, Server};
use iroh_addr_index_proto::{MAX_DGRAM, Request, RequestV1, Response, ResponseV1};
use iroh_base::SecretKey;
use iroh_mainline_endpoint_discovery::{Directory, SignedRecord, UdpClient};
use n0_mainline::Dht;
use tokio::net::UdpSocket;

#[tokio::test]
async fn publish_and_resolve_opaque_bytes() {
    let server = Server::new(Limits::for_tests());
    let handle = server
        .bind_udp("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let dht = test_dht();
    let client = UdpClient::attach(dht.clone()).await.unwrap();
    client.add_replica(v4(handle.local_addr())).await.unwrap();

    let value = b"not a signed record".to_vec();
    let addrs = client.publish(value.clone()).await.unwrap();
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
    let handle = server
        .bind_udp("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let owner = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let attacker = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    let prepare = Request::V1(RequestV1::Prepare {
        tx: 1,
        padding: [0; 24],
    });
    send(&owner, handle.local_addr(), &prepare).await;
    let Response::V1(ResponseV1::Prepared { addr, token, .. }) = recv(&owner).await else {
        panic!("unexpected prepare response")
    };

    let stolen = Request::V1(RequestV1::Put {
        tx: 2,
        token,
        value: b"stolen".to_vec(),
    });
    send(&attacker, handle.local_addr(), &stolen).await;
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
    send(&owner, handle.local_addr(), &valid).await;
    let Response::V1(ResponseV1::Stored { addr: stored, .. }) = recv(&owner).await else {
        panic!("unexpected put response")
    };
    assert_eq!(stored, addr);
    assert_eq!(server.get_local(addr).unwrap(), b"owned");

    let public_get = Request::V1(RequestV1::Get { tx: 5, addr });
    send(&attacker, handle.local_addr(), &public_get).await;
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
    let handle = server
        .bind_udp("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let reader = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = "203.0.113.1:1234".parse().unwrap();
    let value = vec![42; iroh_addr_index_proto::MAX_VALUE_LEN];
    server.put_local(addr, value.clone()).unwrap();
    let request = Request::V1(RequestV1::Get { tx: 7, addr });
    let mut buf = [0; MAX_DGRAM];
    let bytes = request.encode(&mut buf).unwrap();
    reader
        .send_to(&bytes[..MAX_DGRAM - 1], handle.local_addr())
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
        .send_to(&oversized, handle.local_addr())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), recv(&reader))
            .await
            .is_err()
    );
    send(&reader, handle.local_addr(), &request).await;
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
    let handle = server
        .bind_udp("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let client = UdpClient::attach(test_dht()).await.unwrap();
    client.add_replica(v4(handle.local_addr())).await.unwrap();
    let addr = client.publish(b"value".to_vec()).await.unwrap()[0];

    let (left, right) = tokio::join!(client.resolve(addr), client.resolve(addr));
    assert_eq!(left.unwrap().values, [b"value".to_vec()]);
    assert_eq!(right.unwrap().values, [b"value".to_vec()]);
}

#[tokio::test]
async fn discovery_directory_validates_opaque_record() {
    let server = Server::new(Limits::for_tests());
    let handle = server
        .bind_udp("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let directory = Directory::udp(test_dht(), v4(handle.local_addr()))
        .await
        .unwrap();
    let record = SignedRecord::sign(&SecretKey::generate());
    let addr = directory.publish(&record).await.unwrap()[0];
    assert_eq!(directory.lookup(addr).await.unwrap(), [record]);

    server.put_local(addr, b"invalid".to_vec()).unwrap();
    assert!(directory.lookup(addr).await.unwrap().is_empty());
}

async fn send(socket: &UdpSocket, destination: std::net::SocketAddr, message: &Request) {
    let mut buf = [0; MAX_DGRAM];
    let bytes = message.encode(&mut buf).unwrap();
    socket.send_to(bytes, destination).await.unwrap();
}

async fn recv(socket: &UdpSocket) -> Response {
    let mut buf = [0; MAX_DGRAM];
    let (len, _) = socket.recv_from(&mut buf).await.unwrap();
    Response::decode(&buf[..len]).unwrap()
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
