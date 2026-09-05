//! Single-MTU UDP protocol, same shape as the old
//! [iroh-experiments content-discovery][cd] tracker.
//!
//! Postcard [`Request`] / [`Response`] in one datagram, max [`MAX_DGRAM`]
//! (1200) bytes. Outer enum is a version tag (`V1`); unknown versions are
//! dropped. Publish is fire-and-forget (signature is the auth).
//!
//! The client keeps a set of tracker sockets, sends to all of them, and only
//! accepts replies from that set.
//!
//! [cd]: https://github.com/n0-computer/iroh-experiments/tree/main/content-discovery

use std::{
    collections::{HashMap, HashSet},
    io,
    net::{IpAddr, SocketAddr, SocketAddrV4},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::{debug, trace, warn};

use crate::record::SignedRecord;

/// Maximum UDP payload (one MTU, no fragmentation). Same as the old tracker.
pub const MAX_DGRAM: usize = 1200;

/// Default time to collect resolve replies from all trackers.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_millis(500);

/// Versioned postcard request. Unknown versions deserialize as failure (dropped).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Current request body.
    V1(RequestV1),
}

/// v1 request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
// Publish is transient; boxing it would add an allocation to every datagram.
#[allow(clippy::large_enum_variant)]
pub enum RequestV1 {
    /// Upsert this eid's signed row. No datagram reply.
    Publish(SignedRecord),
    /// Lookup eids that listed this exact ip:port.
    Resolve(SocketAddrV4),
}

/// Versioned postcard response. Unknown versions deserialize as failure (dropped).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    /// Current response body.
    V1(ResponseV1),
}

/// v1 response body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponseV1 {
    /// Resolve result. `addr` echoes the query so replies can be demuxed.
    Resolve {
        /// The queried compact mapping.
        addr: SocketAddrV4,
        /// Live signed rows. Client must still verify.
        hosts: Vec<SignedRecord>,
        /// True if more rows existed than fit in [`MAX_DGRAM`].
        truncated: bool,
    },
}

fn encode<'a, T: Serialize>(value: &T, buf: &'a mut [u8; MAX_DGRAM]) -> Result<&'a [u8], UdpError> {
    postcard::to_slice(value, buf)
        .map(|s| &*s)
        .map_err(|_| UdpError::TooLarge)
}

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
        let resp = Response::V1(ResponseV1::Resolve {
            addr,
            hosts: hosts.clone(),
            truncated,
        });
        match postcard::to_slice(&resp, buf) {
            Ok(slice) => {
                let n = slice.len();
                return &buf[..n];
            }
            Err(_) if !hosts.is_empty() => {
                hosts.pop();
                truncated = true;
            }
            Err(_) => {
                let resp = Response::V1(ResponseV1::Resolve {
                    addr,
                    hosts: Vec::new(),
                    truncated: true,
                });
                let slice = postcard::to_slice(&resp, buf).expect("empty resolve fits");
                let n = slice.len();
                return &buf[..n];
            }
        }
    }
}

/// UDP client/server error.
#[derive(Debug)]
pub enum UdpError {
    /// Socket I/O.
    Io(io::Error),
    /// No matching response before the timeout.
    Timeout,
    /// Encoded datagram would exceed [`MAX_DGRAM`].
    TooLarge,
    /// No tracker addresses configured.
    NoTrackers,
    /// Actor dropped.
    Closed,
}

impl std::fmt::Display for UdpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "{e}"),
            Self::Timeout => write!(f, "udp timeout"),
            Self::TooLarge => write!(f, "datagram exceeds {MAX_DGRAM} bytes"),
            Self::NoTrackers => write!(f, "no udp trackers"),
            Self::Closed => write!(f, "udp client closed"),
        }
    }
}

impl std::error::Error for UdpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for UdpError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

/// Running UDP listener sharing a [`crate::Server`] store.
#[derive(Debug)]
pub struct UdpHandle {
    local_addr: SocketAddr,
    task: JoinHandle<()>,
}

impl UdpHandle {
    /// Bound address (useful when the port was 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Abort the recv loop.
    pub fn abort(&self) {
        self.task.abort();
    }
}

impl Drop for UdpHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl crate::Server {
    /// Bind a UDP socket and serve publish/resolve on it until the handle is dropped.
    pub async fn bind_udp(&self, bind: SocketAddr) -> io::Result<UdpHandle> {
        let sock = UdpSocket::bind(bind).await?;
        let local_addr = sock.local_addr()?;
        let this = self.clone();
        let task = tokio::spawn(async move {
            this.udp_loop(sock).await;
        });
        Ok(UdpHandle { local_addr, task })
    }

    async fn udp_loop(&self, sock: UdpSocket) {
        let mut buf = [0u8; MAX_DGRAM];
        loop {
            let (n, from) = match sock.recv_from(&mut buf).await {
                Ok(pair) => pair,
                Err(err) => {
                    debug!(%err, "udp recv");
                    continue;
                }
            };
            if let Err(err) = self.handle_udp_packet(&buf[..n], &sock, from).await {
                debug!(%from, %err, "udp packet");
            }
        }
    }

    async fn handle_udp_packet(
        &self,
        data: &[u8],
        sock: &UdpSocket,
        from: SocketAddr,
    ) -> Result<(), UdpError> {
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
                if record.verify_sig().is_err() {
                    return Ok(());
                }
                if !self.allow_udp_publish(record.eid, from.ip()) {
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
                let bytes = encode_resolve(addr, hosts, &mut out);
                sock.send_to(bytes, from).await?;
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

enum ActorMsg {
    AddTracker(SocketAddr),
    RemoveTracker(SocketAddr),
    Publish(SignedRecord, oneshot::Sender<Result<(), UdpError>>),
    Resolve(
        SocketAddrV4,
        oneshot::Sender<Result<ResolveResult, UdpError>>,
    ),
}

/// UDP client: a set of tracker sockets, one datagram per publish/resolve.
///
/// Same idea as `UdpDiscovery` in iroh-experiments content-discovery.
#[derive(Debug, Clone)]
pub struct UdpClient {
    tx: mpsc::Sender<ActorMsg>,
}

impl UdpClient {
    /// Bind an ephemeral local UDP socket.
    pub async fn bind() -> io::Result<Self> {
        Self::bind_addr(SocketAddr::from(([0, 0, 0, 0], 0)), DEFAULT_TIMEOUT).await
    }

    /// Bind a specific local address.
    pub async fn bind_addr(addr: SocketAddr, timeout: Duration) -> io::Result<Self> {
        let sock = UdpSocket::bind(addr).await?;
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(actor_loop(sock, rx, timeout));
        Ok(Self { tx })
    }

    /// Add a directory replica to query and publish to.
    pub async fn add_tracker(&self, tracker: SocketAddr) -> Result<(), UdpError> {
        self.tx
            .send(ActorMsg::AddTracker(tracker))
            .await
            .map_err(|_| UdpError::Closed)
    }

    /// Stop talking to a replica.
    pub async fn remove_tracker(&self, tracker: SocketAddr) -> Result<(), UdpError> {
        self.tx
            .send(ActorMsg::RemoveTracker(tracker))
            .await
            .map_err(|_| UdpError::Closed)
    }

    /// Fire-and-forget publish to all configured trackers.
    ///
    /// Auth is the record signature. There is no ACK, same as the old tracker.
    pub async fn publish(&self, record: SignedRecord) -> Result<(), UdpError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMsg::Publish(record, tx))
            .await
            .map_err(|_| UdpError::Closed)?;
        rx.await.map_err(|_| UdpError::Closed)?
    }

    /// [`publish`](Self::publish) to one tracker (adds it if needed).
    pub async fn publish_to(
        &self,
        tracker: SocketAddr,
        record: SignedRecord,
    ) -> Result<(), UdpError> {
        self.add_tracker(tracker).await?;
        self.publish(record).await
    }

    /// Resolve `addr` from all trackers, union, drop invalid sigs.
    pub async fn resolve(&self, addr: SocketAddrV4) -> Result<ResolveResult, UdpError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMsg::Resolve(addr, tx))
            .await
            .map_err(|_| UdpError::Closed)?;
        rx.await.map_err(|_| UdpError::Closed)?
    }

    /// [`resolve`](Self::resolve) against one tracker (adds it if needed).
    pub async fn resolve_from(
        &self,
        tracker: SocketAddr,
        addr: SocketAddrV4,
    ) -> Result<ResolveResult, UdpError> {
        self.add_tracker(tracker).await?;
        self.resolve(addr).await
    }
}

/// Result of [`UdpClient::resolve`].
#[derive(Debug, Clone, Default)]
pub struct ResolveResult {
    /// Verified, live rows covering the queried address.
    pub records: Vec<SignedRecord>,
    /// At least one replica indicated more rows than fit in one datagram.
    pub truncated: bool,
}

struct Actor {
    sock: UdpSocket,
    rx: mpsc::Receiver<ActorMsg>,
    trackers: HashSet<SocketAddr>,
    pending: HashMap<SocketAddrV4, Vec<Pending>>,
    timeout: Duration,
}

struct Pending {
    hosts: HashMap<iroh::EndpointId, SignedRecord>,
    truncated: bool,
    tx: oneshot::Sender<Result<ResolveResult, UdpError>>,
    deadline: tokio::time::Instant,
}

async fn actor_loop(sock: UdpSocket, rx: mpsc::Receiver<ActorMsg>, timeout: Duration) {
    Actor {
        sock,
        rx,
        trackers: HashSet::new(),
        pending: HashMap::new(),
        timeout,
    }
    .run()
    .await;
}

impl Actor {
    async fn run(mut self) {
        let mut buf = [0u8; MAX_DGRAM];
        loop {
            let next_deadline = self.pending.values().flatten().map(|p| p.deadline).min();
            tokio::select! {
                msg = self.rx.recv() => {
                    let Some(msg) = msg else { break };
                    self.handle_msg(msg, &mut buf).await;
                }
                res = self.sock.recv_from(&mut buf) => {
                    match res {
                        Ok((n, from)) => self.handle_packet(&buf[..n], from),
                        Err(err) => warn!(%err, "udp recv"),
                    }
                }
                _ = sleep_until(next_deadline) => {
                    self.flush_expired();
                }
            }
        }
    }

    async fn handle_msg(&mut self, msg: ActorMsg, buf: &mut [u8; MAX_DGRAM]) {
        match msg {
            ActorMsg::AddTracker(addr) => {
                self.trackers.insert(addr);
            }
            ActorMsg::RemoveTracker(addr) => {
                self.trackers.remove(&addr);
            }
            ActorMsg::Publish(record, tx) => {
                if self.trackers.is_empty() {
                    let _ = tx.send(Err(UdpError::NoTrackers));
                    return;
                }
                let req = Request::V1(RequestV1::Publish(record));
                let res = match encode(&req, buf) {
                    Ok(bytes) => {
                        for tracker in &self.trackers {
                            if let Err(err) = self.sock.send_to(bytes, *tracker).await {
                                debug!(%tracker, %err, "udp publish send");
                            }
                        }
                        Ok(())
                    }
                    Err(e) => Err(e),
                };
                let _ = tx.send(res);
            }
            ActorMsg::Resolve(addr, tx) => {
                if self.trackers.is_empty() {
                    let _ = tx.send(Err(UdpError::NoTrackers));
                    return;
                }
                let req = Request::V1(RequestV1::Resolve(addr));
                let bytes = match encode(&req, buf) {
                    Ok(b) => b.to_vec(),
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        return;
                    }
                };
                for tracker in &self.trackers {
                    if let Err(err) = self.sock.send_to(&bytes, *tracker).await {
                        debug!(%tracker, %err, "udp resolve send");
                    }
                }
                self.pending.entry(addr).or_default().push(Pending {
                    hosts: HashMap::new(),
                    truncated: false,
                    tx,
                    deadline: tokio::time::Instant::now() + self.timeout,
                });
            }
        }
    }

    fn handle_packet(&mut self, data: &[u8], from: SocketAddr) {
        if !self.trackers.contains(&from) {
            trace!(%from, "udp reply from unknown tracker");
            return;
        }
        let Some(Response::V1(ResponseV1::Resolve {
            addr,
            hosts,
            truncated,
        })) = decode::<Response>(data)
        else {
            return;
        };
        let Some(pending) = self.pending.get_mut(&addr) else {
            return;
        };
        for pending in pending.iter_mut() {
            pending.truncated |= truncated;
            for rec in &hosts {
                if rec.verify_sig().is_err() || !rec.covers_addr(addr) {
                    continue;
                }
                pending.hosts.entry(rec.eid).or_insert_with(|| rec.clone());
            }
        }
        // One tracker is enough to answer; still wait a tick for others via deadline.
        // If this was the only tracker, finish immediately.
        if self.trackers.len() == 1 {
            for pending in self.pending.remove(&addr).expect("just used") {
                finish_pending(pending);
            }
        }
    }

    fn flush_expired(&mut self) {
        let now = tokio::time::Instant::now();
        let expired: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.iter().any(|p| p.deadline <= now))
            .map(|(k, _)| *k)
            .collect();
        for addr in expired {
            if let Some(mut pending) = self.pending.remove(&addr) {
                let mut keep = Vec::new();
                for pending in pending.drain(..) {
                    if pending.deadline <= now {
                        finish_pending(pending);
                    } else {
                        keep.push(pending);
                    }
                }
                if !keep.is_empty() {
                    self.pending.insert(addr, keep);
                }
            }
        }
    }
}

fn finish_pending(pending: Pending) {
    let mut records: Vec<_> = pending.hosts.into_values().collect();
    records.sort_by_key(|r| r.eid);
    let _ = pending.tx.send(Ok(ResolveResult {
        records,
        truncated: pending.truncated,
    }));
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending::<()>().await,
    }
}

#[cfg(test)]
mod tests {
    use iroh::SecretKey;

    use super::*;
    use crate::record::SignedRecord;

    #[test]
    fn publish_request_fits() {
        let rec = SignedRecord::sign(
            &SecretKey::generate(),
            vec!["203.0.113.9:6881".parse().unwrap()],
            [b"test/0"],
        );
        let mut buf = [0u8; MAX_DGRAM];
        let bytes = encode(&Request::V1(RequestV1::Publish(rec.clone())), &mut buf).unwrap();
        assert!(bytes.len() <= MAX_DGRAM);
        match decode::<Request>(bytes).unwrap() {
            Request::V1(RequestV1::Publish(got)) => assert_eq!(got.eid, rec.eid),
            _ => panic!("wrong msg"),
        }
    }

    #[test]
    fn resolve_truncates_to_mtu() {
        let mut hosts = Vec::new();
        for _ in 0..32 {
            hosts.push(SignedRecord::sign(
                &SecretKey::generate(),
                vec!["198.51.100.7:1".parse().unwrap()],
                [b"test/0"],
            ));
        }
        let mut buf = [0u8; MAX_DGRAM];
        let addr: SocketAddrV4 = "198.51.100.7:1".parse().unwrap();
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
    fn garbage_is_dropped() {
        assert!(decode::<Request>(b"xxxx").is_none());
        assert!(decode::<Request>(&[]).is_none());
    }

    #[test]
    fn udp_publish_source_must_be_claimed() {
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
