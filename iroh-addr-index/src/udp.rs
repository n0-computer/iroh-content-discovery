//! UDP server for the address-index protocol.

use std::{
    io,
    net::{IpAddr, SocketAddr, SocketAddrV4},
};

use iroh_addr_index_proto::{MAX_DGRAM, Request, RequestV1, Response, ResponseV1, SignedRecord};
use serde::Deserialize;
use tokio::{net::UdpSocket, task::JoinHandle};
use tracing::{debug, trace};

use crate::Server;

fn decode<T: for<'de> Deserialize<'de>>(buf: &[u8]) -> Option<T> {
    if buf.is_empty() || buf.len() > MAX_DGRAM {
        return None;
    }
    postcard::from_bytes(buf).ok()
}

fn encode_resolve(
    addr: SocketAddrV4,
    mut hosts: Vec<SignedRecord>,
    buf: &mut [u8; MAX_DGRAM],
) -> &[u8] {
    let mut truncated = false;
    loop {
        let response = Response::V1(ResponseV1::Resolve {
            addr,
            hosts: hosts.clone(),
            truncated,
        });
        match postcard::to_slice(&response, buf).map(|slice| slice.len()) {
            Ok(len) => return &buf[..len],
            Err(_) if !hosts.is_empty() => {
                hosts.pop();
                truncated = true;
            }
            Err(_) => {
                let response = Response::V1(ResponseV1::Resolve {
                    addr,
                    hosts: Vec::new(),
                    truncated: true,
                });
                let len = postcard::to_slice(&response, buf)
                    .expect("empty resolve fits")
                    .len();
                return &buf[..len];
            }
        }
    }
}

/// Running UDP listener. Dropping the handle aborts its receive loop.
#[derive(Debug)]
pub struct UdpHandle {
    local_addr: SocketAddr,
    task: JoinHandle<()>,
}

impl UdpHandle {
    /// Bound address, including the assigned port when binding port zero.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Abort the receive loop.
    pub fn abort(&self) {
        self.task.abort();
    }
}

impl Drop for UdpHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    /// Bind a UDP socket and serve until the returned handle is dropped.
    pub async fn bind_udp(&self, bind: SocketAddr) -> io::Result<UdpHandle> {
        let socket = UdpSocket::bind(bind).await?;
        let local_addr = socket.local_addr()?;
        let server = self.clone();
        let task = tokio::spawn(async move {
            server.udp_loop(socket).await;
        });
        Ok(UdpHandle { local_addr, task })
    }

    async fn udp_loop(&self, socket: UdpSocket) {
        let mut buf = [0u8; MAX_DGRAM];
        loop {
            let (n, from) = match socket.recv_from(&mut buf).await {
                Ok(pair) => pair,
                Err(err) => {
                    debug!(%err, "udp receive");
                    continue;
                }
            };
            if let Err(err) = self.handle_udp_packet(&buf[..n], &socket, from).await {
                debug!(%from, %err, "udp packet");
            }
        }
    }

    async fn handle_udp_packet(
        &self,
        data: &[u8],
        socket: &UdpSocket,
        from: SocketAddr,
    ) -> io::Result<()> {
        trace!(%from, bytes = data.len(), "udp packet");
        let Some(Request::V1(request)) = decode::<Request>(data) else {
            return Ok(());
        };
        match request {
            RequestV1::Publish(record) => {
                if self.verify_udp_source_ip() && !source_ip_matches(&record, from.ip()) {
                    trace!(%from, eid = %record.eid, "udp publish source IP not claimed");
                    return Ok(());
                }
                if record.verify_sig().is_err() || !self.allow_udp_publish(record.eid, from.ip()) {
                    return Ok(());
                }
                if let Err(err) = self.publish_confirmed_local(record).await {
                    debug!(%from, %err, "udp publish");
                }
            }
            RequestV1::Resolve(addr) => {
                if !self.allow_udp(from.ip()) {
                    return Ok(());
                }
                let hosts = self.lookup_local(addr);
                let mut out = [0u8; MAX_DGRAM];
                socket
                    .send_to(encode_resolve(addr, hosts, &mut out), from)
                    .await?;
            }
        }
        Ok(())
    }
}

fn source_ip_matches(record: &SignedRecord, source: IpAddr) -> bool {
    let IpAddr::V4(source) = source else {
        return false;
    };
    record.addrs.iter().any(|addr| *addr.ip() == source)
}

#[cfg(test)]
mod tests {
    use iroh_addr_index_proto::SecretKey;

    use super::*;

    #[test]
    fn resolve_truncates_to_mtu() {
        let hosts = (0..32)
            .map(|_| {
                SignedRecord::sign(
                    &SecretKey::generate(),
                    vec!["198.51.100.7:1".parse().unwrap()],
                    [b"test/0"],
                )
            })
            .collect();
        let mut buf = [0u8; MAX_DGRAM];
        let addr = "198.51.100.7:1".parse().unwrap();
        let bytes = encode_resolve(addr, hosts, &mut buf);
        assert!(bytes.len() <= MAX_DGRAM);
        match decode::<Response>(bytes).unwrap() {
            Response::V1(ResponseV1::Resolve {
                truncated, hosts, ..
            }) => {
                assert!(truncated);
                assert!(!hosts.is_empty());
            }
        }
    }

    #[test]
    fn publish_source_must_be_claimed() {
        let rec = SignedRecord::sign(
            &SecretKey::generate(),
            vec!["203.0.113.9:6881".parse().unwrap()],
            [b"test/0"],
        );
        assert!(source_ip_matches(&rec, "203.0.113.9".parse().unwrap()));
        assert!(!source_ip_matches(
            &rec,
            "::ffff:203.0.113.9".parse().unwrap()
        ));
        assert!(!source_ip_matches(&rec, "198.51.100.7".parse().unwrap()));
    }
}
