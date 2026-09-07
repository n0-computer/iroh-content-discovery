//! Single-MTU UDP client for address-index replicas.

use std::{
    collections::{HashMap, HashSet},
    io,
    net::{SocketAddr, SocketAddrV4},
    time::Duration,
};

use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
};
use tracing::{debug, trace, warn};

use iroh_addr_index_proto::{MAX_DGRAM, Request, RequestV1, Response, ResponseV1, SignedRecord};

/// Default time to collect resolve replies from all replicas.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_millis(500);

fn encode<'a, T: serde::Serialize>(
    value: &T,
    buf: &'a mut [u8; MAX_DGRAM],
) -> Result<&'a [u8], UdpError> {
    postcard::to_slice(value, buf)
        .map(|s| &*s)
        .map_err(|_| UdpError::TooLarge)
}

fn decode<T: for<'de> serde::Deserialize<'de>>(buf: &[u8]) -> Option<T> {
    if buf.is_empty() || buf.len() > MAX_DGRAM {
        return None;
    }
    postcard::from_bytes(buf).ok()
}

/// UDP client error.
#[derive(Debug)]
pub enum UdpError {
    /// Socket I/O.
    Io(io::Error),
    /// No matching response before the timeout.
    Timeout,
    /// Encoded datagram would exceed [`MAX_DGRAM`].
    TooLarge,
    /// No replica addresses configured.
    NoReplicas,
    /// Actor dropped.
    Closed,
}

impl std::fmt::Display for UdpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "{e}"),
            Self::Timeout => write!(f, "udp timeout"),
            Self::TooLarge => write!(f, "datagram exceeds {MAX_DGRAM} bytes"),
            Self::NoReplicas => write!(f, "no UDP replicas"),
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

enum ActorMsg {
    AddReplica(SocketAddr),
    RemoveReplica(SocketAddr),
    Publish(SignedRecord, oneshot::Sender<Result<(), UdpError>>),
    Resolve(
        SocketAddrV4,
        oneshot::Sender<Result<ResolveResult, UdpError>>,
    ),
}

/// UDP client: a set of replica sockets, one datagram per publish/resolve.
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
    pub async fn add_replica(&self, replica: SocketAddr) -> Result<(), UdpError> {
        self.tx
            .send(ActorMsg::AddReplica(replica))
            .await
            .map_err(|_| UdpError::Closed)
    }

    /// Stop talking to a replica.
    pub async fn remove_replica(&self, replica: SocketAddr) -> Result<(), UdpError> {
        self.tx
            .send(ActorMsg::RemoveReplica(replica))
            .await
            .map_err(|_| UdpError::Closed)
    }

    /// Fire-and-forget publish to all configured replicas.
    ///
    /// Authentication is the record signature; the protocol has no ACK.
    pub async fn publish(&self, record: SignedRecord) -> Result<(), UdpError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMsg::Publish(record, tx))
            .await
            .map_err(|_| UdpError::Closed)?;
        rx.await.map_err(|_| UdpError::Closed)?
    }

    /// [`publish`](Self::publish) to one replica, adding it if needed.
    pub async fn publish_to(
        &self,
        replica: SocketAddr,
        record: SignedRecord,
    ) -> Result<(), UdpError> {
        self.add_replica(replica).await?;
        self.publish(record).await
    }

    /// Resolve `addr` from all replicas, unioning valid signed results.
    pub async fn resolve(&self, addr: SocketAddrV4) -> Result<ResolveResult, UdpError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMsg::Resolve(addr, tx))
            .await
            .map_err(|_| UdpError::Closed)?;
        rx.await.map_err(|_| UdpError::Closed)?
    }

    /// [`resolve`](Self::resolve) against one replica, adding it if needed.
    pub async fn resolve_from(
        &self,
        replica: SocketAddr,
        addr: SocketAddrV4,
    ) -> Result<ResolveResult, UdpError> {
        self.add_replica(replica).await?;
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
    replicas: HashSet<SocketAddr>,
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
        replicas: HashSet::new(),
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
            ActorMsg::AddReplica(addr) => {
                self.replicas.insert(addr);
            }
            ActorMsg::RemoveReplica(addr) => {
                self.replicas.remove(&addr);
            }
            ActorMsg::Publish(record, tx) => {
                if self.replicas.is_empty() {
                    let _ = tx.send(Err(UdpError::NoReplicas));
                    return;
                }
                let req = Request::V1(RequestV1::Publish(record));
                let res = match encode(&req, buf) {
                    Ok(bytes) => {
                        for replica in &self.replicas {
                            if let Err(err) = self.sock.send_to(bytes, *replica).await {
                                debug!(%replica, %err, "udp publish send");
                            }
                        }
                        Ok(())
                    }
                    Err(e) => Err(e),
                };
                let _ = tx.send(res);
            }
            ActorMsg::Resolve(addr, tx) => {
                if self.replicas.is_empty() {
                    let _ = tx.send(Err(UdpError::NoReplicas));
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
                for replica in &self.replicas {
                    if let Err(err) = self.sock.send_to(&bytes, *replica).await {
                        debug!(%replica, %err, "udp resolve send");
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
        if !self.replicas.contains(&from) {
            trace!(%from, "udp reply from unknown replica");
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
        // One replica is enough to answer. With several, collect until the deadline.
        if self.replicas.len() == 1 {
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
