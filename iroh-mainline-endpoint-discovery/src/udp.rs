//! Concurrent UDP client for opaque address-index replicas.

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddrV4,
    time::Duration,
};

use iroh_addr_index_proto::{
    MAGIC, MAX_DGRAM, MAX_VALUE_LEN, Request, RequestV1, Response, ResponseV1, TransactionId,
};
use n0_mainline::{ActorShutdown, DatagramHookGuard, Dht};
use tokio::sync::{mpsc, mpsc::error::TrySendError, oneshot};
use tracing::{debug, trace};

/// Default timeout for an address-index operation.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

/// UDP address-index client error.
#[derive(Debug)]
pub enum UdpError {
    /// The Mainline node's datagram hook could not be attached.
    Attach(ActorShutdown),
    /// No matching response arrived before the deadline.
    Timeout,
    /// The opaque value exceeds [`MAX_VALUE_LEN`] or the UDP datagram limit.
    TooLarge,
    /// No replica addresses are configured.
    NoReplicas,
    /// The client actor stopped.
    Closed,
}

impl std::fmt::Display for UdpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Attach(err) => err.fmt(f),
            Self::Timeout => write!(f, "UDP operation timed out"),
            Self::TooLarge => write!(f, "opaque value exceeds {MAX_VALUE_LEN} bytes"),
            Self::NoReplicas => write!(f, "no UDP replicas configured"),
            Self::Closed => write!(f, "UDP client closed"),
        }
    }
}

impl std::error::Error for UdpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Attach(err) => Some(err),
            _ => None,
        }
    }
}

impl From<ActorShutdown> for UdpError {
    fn from(value: ActorShutdown) -> Self {
        Self::Attach(value)
    }
}

enum ActorMsg {
    AddReplica(SocketAddrV4),
    RemoveReplica(SocketAddrV4),
    Publish(
        Vec<u8>,
        oneshot::Sender<Result<Vec<SocketAddrV4>, UdpError>>,
    ),
    Resolve(
        SocketAddrV4,
        oneshot::Sender<Result<ResolveResult, UdpError>>,
    ),
}

/// Address-index client attached to a Mainline node's UDP socket.
#[derive(Debug, Clone)]
pub struct UdpClient {
    tx: mpsc::Sender<ActorMsg>,
}

impl UdpClient {
    /// Attach to `dht` using the default operation timeout.
    pub async fn attach(dht: Dht) -> Result<Self, UdpError> {
        Self::attach_with_timeout(dht, DEFAULT_TIMEOUT).await
    }

    /// Attach to `dht` and configure the operation timeout.
    pub async fn attach_with_timeout(dht: Dht, timeout: Duration) -> Result<Self, UdpError> {
        let (incoming_tx, incoming_rx) = mpsc::channel(256);
        let hook = dht
            .add_datagram_hook(move |bytes, from| {
                if !bytes.starts_with(MAGIC) {
                    return false;
                }
                match incoming_tx.try_send((Box::from(bytes), from)) {
                    Ok(()) | Err(TrySendError::Full(_)) => true,
                    Err(TrySendError::Closed(_)) => false,
                }
            })
            .await?;
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(Actor::new(dht, hook, incoming_rx, rx, timeout).run());
        Ok(Self { tx })
    }

    /// Add a replica used by subsequent operations.
    pub async fn add_replica(&self, replica: SocketAddrV4) -> Result<(), UdpError> {
        self.tx
            .send(ActorMsg::AddReplica(replica))
            .await
            .map_err(|_| UdpError::Closed)
    }

    /// Remove a configured replica.
    pub async fn remove_replica(&self, replica: SocketAddrV4) -> Result<(), UdpError> {
        self.tx
            .send(ActorMsg::RemoveReplica(replica))
            .await
            .map_err(|_| UdpError::Closed)
    }

    /// Obtain tokens and publish `value` to every responsive replica.
    ///
    /// Returns the public IPv4 sockets under which replicas stored the value.
    pub async fn publish(&self, value: Vec<u8>) -> Result<Vec<SocketAddrV4>, UdpError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMsg::Publish(value, tx))
            .await
            .map_err(|_| UdpError::Closed)?;
        rx.await.map_err(|_| UdpError::Closed)?
    }

    /// Publish to one replica, adding it to this client first.
    pub async fn publish_to(
        &self,
        replica: SocketAddrV4,
        value: Vec<u8>,
    ) -> Result<Vec<SocketAddrV4>, UdpError> {
        self.add_replica(replica).await?;
        self.publish(value).await
    }

    /// Read and deduplicate opaque values from all configured replicas.
    pub async fn resolve(&self, addr: SocketAddrV4) -> Result<ResolveResult, UdpError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMsg::Resolve(addr, tx))
            .await
            .map_err(|_| UdpError::Closed)?;
        rx.await.map_err(|_| UdpError::Closed)?
    }

    /// Resolve through one replica, adding it to this client first.
    pub async fn resolve_from(
        &self,
        replica: SocketAddrV4,
        addr: SocketAddrV4,
    ) -> Result<ResolveResult, UdpError> {
        self.add_replica(replica).await?;
        self.resolve(addr).await
    }
}

/// Result of an opaque address lookup.
#[derive(Debug, Clone, Default)]
pub struct ResolveResult {
    /// Deduplicated opaque values returned by replicas.
    pub values: Vec<Vec<u8>>,
}

struct PendingPublish {
    value: Vec<u8>,
    awaiting: HashSet<SocketAddrV4>,
    stored: HashSet<SocketAddrV4>,
    response: oneshot::Sender<Result<Vec<SocketAddrV4>, UdpError>>,
    deadline: tokio::time::Instant,
}

struct PendingResolve {
    addr: SocketAddrV4,
    awaiting: HashSet<SocketAddrV4>,
    values: HashSet<Vec<u8>>,
    responded: bool,
    response: oneshot::Sender<Result<ResolveResult, UdpError>>,
    deadline: tokio::time::Instant,
}

struct Actor {
    dht: Dht,
    _hook: DatagramHookGuard,
    incoming: mpsc::Receiver<(Box<[u8]>, SocketAddrV4)>,
    rx: mpsc::Receiver<ActorMsg>,
    replicas: HashSet<SocketAddrV4>,
    publishes: HashMap<TransactionId, PendingPublish>,
    resolves: HashMap<TransactionId, PendingResolve>,
    next_tx: TransactionId,
    timeout: Duration,
}

impl Actor {
    fn new(
        dht: Dht,
        hook: DatagramHookGuard,
        incoming: mpsc::Receiver<(Box<[u8]>, SocketAddrV4)>,
        rx: mpsc::Receiver<ActorMsg>,
        timeout: Duration,
    ) -> Self {
        Self {
            dht,
            _hook: hook,
            incoming,
            rx,
            replicas: HashSet::new(),
            publishes: HashMap::new(),
            resolves: HashMap::new(),
            next_tx: 0,
            timeout,
        }
    }

    async fn run(mut self) {
        let mut send_buf = [0; MAX_DGRAM];
        loop {
            let deadline = self.next_deadline();
            tokio::select! {
                message = self.rx.recv() => {
                    let Some(message) = message else { break };
                    self.handle_message(message, &mut send_buf).await;
                }
                packet = self.incoming.recv() => match packet {
                    Some((data, from)) => self.handle_packet(&data, from, &mut send_buf).await,
                    None => break,
                },
                _ = sleep_until(deadline) => self.flush_expired(),
            }
        }
    }

    fn next_id(&mut self) -> TransactionId {
        let tx = self.next_tx;
        self.next_tx = self.next_tx.wrapping_add(1);
        tx
    }

    async fn handle_message(&mut self, message: ActorMsg, buf: &mut [u8; MAX_DGRAM]) {
        match message {
            ActorMsg::AddReplica(addr) => {
                self.replicas.insert(addr);
            }
            ActorMsg::RemoveReplica(addr) => {
                self.replicas.remove(&addr);
            }
            ActorMsg::Publish(value, response) => {
                if value.len() > MAX_VALUE_LEN {
                    let _ = response.send(Err(UdpError::TooLarge));
                    return;
                }
                if self.replicas.is_empty() {
                    let _ = response.send(Err(UdpError::NoReplicas));
                    return;
                }
                let tx = self.next_id();
                let request = Request::V1(RequestV1::Prepare {
                    tx,
                    padding: [0; 24],
                });
                if let Some(bytes) = encode(&request, buf) {
                    for replica in &self.replicas {
                        if let Err(err) = self.dht.send_datagram(bytes, *replica).await {
                            debug!(%replica, %err, "send prepare");
                        }
                    }
                    self.publishes.insert(
                        tx,
                        PendingPublish {
                            value,
                            awaiting: self.replicas.clone(),
                            stored: HashSet::new(),
                            response,
                            deadline: tokio::time::Instant::now() + self.timeout,
                        },
                    );
                } else {
                    let _ = response.send(Err(UdpError::TooLarge));
                }
            }
            ActorMsg::Resolve(addr, response) => {
                if self.replicas.is_empty() {
                    let _ = response.send(Err(UdpError::NoReplicas));
                    return;
                }
                let tx = self.next_id();
                let request = Request::V1(RequestV1::Get { tx, addr });
                if let Some(bytes) = encode(&request, buf) {
                    for replica in &self.replicas {
                        if let Err(err) = self.dht.send_datagram(bytes, *replica).await {
                            debug!(%replica, %err, "send get");
                        }
                    }
                    self.resolves.insert(
                        tx,
                        PendingResolve {
                            addr,
                            awaiting: self.replicas.clone(),
                            values: HashSet::new(),
                            responded: false,
                            response,
                            deadline: tokio::time::Instant::now() + self.timeout,
                        },
                    );
                } else {
                    let _ = response.send(Err(UdpError::TooLarge));
                }
            }
        }
    }

    async fn handle_packet(&mut self, data: &[u8], from: SocketAddrV4, buf: &mut [u8; MAX_DGRAM]) {
        if !self.replicas.contains(&from) {
            trace!(%from, "reply from unknown replica");
            return;
        }
        let Some(Response::V1(response)) = Response::decode(data) else {
            return;
        };
        match response {
            ResponseV1::Prepared { tx, addr, token } => {
                let Some(pending) = self.publishes.get(&tx) else {
                    return;
                };
                if !pending.awaiting.contains(&from) {
                    return;
                }
                let request = Request::V1(RequestV1::Put {
                    tx,
                    token,
                    value: pending.value.clone(),
                });
                if let Some(bytes) = encode(&request, buf)
                    && let Err(err) = self.dht.send_datagram(bytes, from).await
                {
                    debug!(%from, %addr, %err, "send authorized request");
                }
            }
            ResponseV1::Stored { tx, addr } => {
                let Some(pending) = self.publishes.get_mut(&tx) else {
                    return;
                };
                pending.awaiting.remove(&from);
                pending.stored.insert(addr);
                if pending.awaiting.is_empty() {
                    self.finish_publish(tx);
                }
            }
            ResponseV1::Value { tx, addr, value } => {
                let Some(pending) = self.resolves.get_mut(&tx) else {
                    return;
                };
                if pending.addr != addr || !pending.awaiting.remove(&from) {
                    return;
                }
                pending.responded = true;
                if let Some(value) = value
                    && value.len() <= MAX_VALUE_LEN
                {
                    pending.values.insert(value);
                }
                if pending.awaiting.is_empty() {
                    self.finish_resolve(tx);
                }
            }
        }
    }

    fn next_deadline(&self) -> Option<tokio::time::Instant> {
        self.publishes
            .values()
            .map(|pending| pending.deadline)
            .chain(self.resolves.values().map(|pending| pending.deadline))
            .min()
    }

    fn flush_expired(&mut self) {
        let now = tokio::time::Instant::now();
        let publishes: Vec<_> = self
            .publishes
            .iter()
            .filter_map(|(tx, pending)| (pending.deadline <= now).then_some(*tx))
            .collect();
        for tx in publishes {
            self.finish_publish(tx);
        }
        let resolves: Vec<_> = self
            .resolves
            .iter()
            .filter_map(|(tx, pending)| (pending.deadline <= now).then_some(*tx))
            .collect();
        for tx in resolves {
            self.finish_resolve(tx);
        }
    }

    fn finish_publish(&mut self, tx: TransactionId) {
        let Some(pending) = self.publishes.remove(&tx) else {
            return;
        };
        let result = if pending.stored.is_empty() {
            Err(UdpError::Timeout)
        } else {
            let mut addrs: Vec<_> = pending.stored.into_iter().collect();
            addrs.sort();
            Ok(addrs)
        };
        let _ = pending.response.send(result);
    }

    fn finish_resolve(&mut self, tx: TransactionId) {
        let Some(pending) = self.resolves.remove(&tx) else {
            return;
        };
        let result = if !pending.responded {
            Err(UdpError::Timeout)
        } else {
            let mut values: Vec<_> = pending.values.into_iter().collect();
            values.sort();
            Ok(ResolveResult { values })
        };
        let _ = pending.response.send(result);
    }
}

fn encode<'a>(value: &Request, buf: &'a mut [u8; MAX_DGRAM]) -> Option<&'a [u8]> {
    value.encode(buf).ok()
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}
