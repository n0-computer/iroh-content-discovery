//! UDP transport for an address-index replica.

use std::{io, net::SocketAddr};

use iroh_addr_index_proto::{MAX_DGRAM, Request, RequestV1, Response, ResponseV1};
use tokio::{net::UdpSocket, task::JoinHandle};
use tracing::{debug, trace};

use crate::{Server, unix_secs};

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
        let task = tokio::spawn(async move { server.udp_loop(socket).await });
        Ok(UdpHandle { local_addr, task })
    }

    async fn udp_loop(&self, socket: UdpSocket) {
        // Keep one extra byte so oversized datagrams cannot look valid after truncation.
        let mut buf = [0; MAX_DGRAM + 1];
        loop {
            let (n, from) = match socket.recv_from(&mut buf).await {
                Ok(packet) => packet,
                Err(err) => {
                    debug!(%err, "UDP receive");
                    continue;
                }
            };
            if let Err(err) = self.handle_packet(&buf[..n], &socket, from).await {
                debug!(%from, %err, "UDP packet");
            }
        }
    }

    async fn handle_packet(
        &self,
        data: &[u8],
        socket: &UdpSocket,
        from: SocketAddr,
    ) -> io::Result<()> {
        trace!(%from, bytes = data.len(), "UDP packet");
        if data.is_empty() || data.len() > MAX_DGRAM {
            return Ok(());
        }
        let SocketAddr::V4(from) = from else {
            return Ok(());
        };
        let Some(Request::V1(request)) = Request::decode(data) else {
            return Ok(());
        };
        let now = unix_secs();
        let response = match request {
            RequestV1::Prepare { tx, padding: _ } => {
                if !self.allow_request((*from.ip()).into()) {
                    return Ok(());
                }
                Some(Response::V1(ResponseV1::Prepared {
                    tx,
                    addr: from,
                    token: self.issue_token(from, now),
                }))
            }
            RequestV1::Put { tx, token, value } => {
                if !self.verify_token(from, &token, now) || !self.allow_request((*from.ip()).into())
                {
                    trace!(%from, "invalid put token");
                    return Ok(());
                }
                if let Err(err) = self.put_local(from, value) {
                    debug!(%from, %err, "put rejected");
                    return Ok(());
                }
                Some(Response::V1(ResponseV1::Stored { tx, addr: from }))
            }
            RequestV1::Get { tx, addr } => {
                if !self.allow_request((*from.ip()).into()) {
                    return Ok(());
                }
                Some(Response::V1(ResponseV1::Value {
                    tx,
                    addr,
                    value: self.get_local(addr),
                }))
            }
        };
        if let Some(response) = response {
            let mut out = [0; MAX_DGRAM];
            let bytes = response.encode(&mut out).expect("bounded response");
            socket.send_to(bytes, from).await?;
        }
        Ok(())
    }
}
