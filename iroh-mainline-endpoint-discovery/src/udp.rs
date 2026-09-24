//! Concurrent UDP client for opaque address-index servers.

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddrV4,
    time::Duration,
};

use n0_error::e;
use n0_mainline::{ActorShutdown, DatagramHook, Dht};
use tokio::sync::{mpsc, mpsc::error::TrySendError, oneshot};
use tracing::debug;
use udp_addr_index_proto::{
    MAGIC, MAX_DGRAM, MAX_VALUE_LEN, Proto, Request, RequestV1, Response, ResponseV1, TransactionId,
};

/// Default timeout for an address-index operation.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

/// UDP address-index client error.
#[n0_error::stack_error(derive, add_meta)]
pub enum UdpError {
    /// The Mainline node's datagram hook could not be attached.
    Attach {
        /// Underlying Mainline actor error.
        #[error(from, source)]
        source: ActorShutdown,
    },
    /// No matching response arrived before the deadline.
    #[error("UDP operation timed out")]
    Timeout {},
    /// The opaque value exceeds [`MAX_VALUE_LEN`] or the UDP datagram limit.
    #[error("opaque value exceeds {MAX_VALUE_LEN} bytes")]
    TooLarge {},
    /// No server addresses are configured.
    #[error("no UDP servers configured")]
    NoServers {},
    /// The client actor stopped.
    #[error("UDP client closed")]
    Closed {},
}

/// Builds the value to store from the public socket a server observed.
pub type ValueFor = Box<dyn Fn(SocketAddrV4) -> Vec<u8> + Send>;

enum ActorMsg {
    AddServer(SocketAddrV4),
    ReplaceServers(HashSet<SocketAddrV4>),
    RemoveServer(SocketAddrV4),
    Publish(
        ValueFor,
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
    /// Attaches to `dht` using the default operation timeout.
    pub async fn attach(dht: Dht) -> Result<Self, UdpError> {
        Self::attach_with_timeout(dht, DEFAULT_TIMEOUT).await
    }

    /// Attaches to `dht` with the given operation timeout.
    pub async fn attach_with_timeout(dht: Dht, timeout: Duration) -> Result<Self, UdpError> {
        let (incoming_tx, incoming_rx) = mpsc::channel(256);
        dht.set_datagram_hook(Some(DatagramHook::new(move |bytes, from| {
            if !bytes.starts_with(MAGIC) {
                return false;
            }
            match incoming_tx.try_send((Box::from(bytes), from)) {
                Ok(()) | Err(TrySendError::Full(_)) => true,
                Err(TrySendError::Closed(_)) => false,
            }
        })))
        .await?;
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(Actor::new(dht, incoming_rx, rx, timeout).run());
        Ok(Self { tx })
    }

    /// Adds a server used by subsequent operations.
    pub async fn add_server(&self, server: SocketAddrV4) -> Result<(), UdpError> {
        self.tx
            .send(ActorMsg::AddServer(server))
            .await
            .map_err(|_| e!(UdpError::Closed))
    }

    pub(crate) async fn replace_servers(
        &self,
        servers: HashSet<SocketAddrV4>,
    ) -> Result<(), UdpError> {
        self.tx
            .send(ActorMsg::ReplaceServers(servers))
            .await
            .map_err(|_| e!(UdpError::Closed))
    }

    /// Removes a configured server.
    pub async fn remove_server(&self, server: SocketAddrV4) -> Result<(), UdpError> {
        self.tx
            .send(ActorMsg::RemoveServer(server))
            .await
            .map_err(|_| e!(UdpError::Closed))
    }

    /// Obtains tokens and publishes a value to every responsive server.
    ///
    /// The value is built per server, from the public socket that server
    /// observed, which is only known once it has answered. Servers behind
    /// different paths can legitimately see different sockets.
    ///
    /// Returns the public IPv4 sockets under which servers stored the value.
    pub async fn publish(
        &self,
        value: impl Fn(SocketAddrV4) -> Vec<u8> + Send + 'static,
    ) -> Result<Vec<SocketAddrV4>, UdpError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMsg::Publish(Box::new(value), tx))
            .await
            .map_err(|_| e!(UdpError::Closed))?;
        rx.await.map_err(|_| e!(UdpError::Closed))?
    }

    /// Reads and deduplicates opaque values from all configured servers.
    pub async fn resolve(&self, addr: SocketAddrV4) -> Result<ResolveResult, UdpError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMsg::Resolve(addr, tx))
            .await
            .map_err(|_| e!(UdpError::Closed))?;
        rx.await.map_err(|_| e!(UdpError::Closed))?
    }
}

/// Result of an opaque address lookup.
#[derive(Debug, Clone, Default)]
pub struct ResolveResult {
    /// Deduplicated opaque values returned by servers.
    pub values: Vec<Vec<u8>>,
}

struct PendingPublish {
    value: ValueFor,
    /// Whether a built value was too large, so the failure is not a timeout.
    too_large: bool,
    awaiting: HashSet<SocketAddrV4>,
    /// Servers already sent a put.
    ///
    /// A repeated or forged `Prepared` cannot make us send the value again.
    prepared: HashSet<SocketAddrV4>,
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
    incoming: mpsc::Receiver<(Box<[u8]>, SocketAddrV4)>,
    rx: mpsc::Receiver<ActorMsg>,
    servers: HashSet<SocketAddrV4>,
    publishes: HashMap<TransactionId, PendingPublish>,
    resolves: HashMap<TransactionId, PendingResolve>,
    timeout: Duration,
}

impl Actor {
    fn new(
        dht: Dht,
        incoming: mpsc::Receiver<(Box<[u8]>, SocketAddrV4)>,
        rx: mpsc::Receiver<ActorMsg>,
        timeout: Duration,
    ) -> Self {
        Self {
            dht,
            incoming,
            rx,
            servers: HashSet::new(),
            publishes: HashMap::new(),
            resolves: HashMap::new(),
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

    /// Returns a transaction id an off-path attacker cannot guess.
    ///
    /// Our socket and the servers we talk to are both public, so a predictable
    /// id would be enough to answer a lookup on a server's behalf.
    fn next_id(&mut self) -> TransactionId {
        rand::random()
    }

    async fn handle_message(&mut self, message: ActorMsg, buf: &mut [u8; MAX_DGRAM]) {
        match message {
            ActorMsg::ReplaceServers(servers) => {
                self.servers = servers;
            }
            ActorMsg::AddServer(addr) => {
                self.servers.insert(addr);
            }
            ActorMsg::RemoveServer(addr) => {
                self.servers.remove(&addr);
            }
            ActorMsg::Publish(value, response) => {
                if self.servers.is_empty() {
                    let _ = response.send(Err(e!(UdpError::NoServers)));
                    return;
                }
                let tx = self.next_id();
                let request = Request::V1(RequestV1::Prepare {
                    tx,
                    padding: [0; 24],
                });
                if let Some(bytes) = encode(request, buf) {
                    for server in &self.servers {
                        if let Err(err) = self.dht.send_datagram(bytes.to_vec(), *server).await {
                            debug!(%server, %err, "send prepare");
                        }
                    }
                    self.publishes.insert(
                        tx,
                        PendingPublish {
                            value,
                            too_large: false,
                            awaiting: self.servers.clone(),
                            prepared: HashSet::new(),
                            stored: HashSet::new(),
                            response,
                            deadline: tokio::time::Instant::now() + self.timeout,
                        },
                    );
                } else {
                    let _ = response.send(Err(e!(UdpError::TooLarge)));
                }
            }
            ActorMsg::Resolve(addr, response) => {
                if self.servers.is_empty() {
                    let _ = response.send(Err(e!(UdpError::NoServers)));
                    return;
                }
                let tx = self.next_id();
                let request = Request::V1(RequestV1::Get { tx, addr });
                if let Some(bytes) = encode(request, buf) {
                    for server in &self.servers {
                        if let Err(err) = self.dht.send_datagram(bytes.to_vec(), *server).await {
                            debug!(%server, %err, "send get");
                        }
                    }
                    self.resolves.insert(
                        tx,
                        PendingResolve {
                            addr,
                            awaiting: self.servers.clone(),
                            values: HashSet::new(),
                            responded: false,
                            response,
                            deadline: tokio::time::Instant::now() + self.timeout,
                        },
                    );
                } else {
                    let _ = response.send(Err(e!(UdpError::TooLarge)));
                }
            }
        }
    }

    async fn handle_packet(&mut self, data: &[u8], from: SocketAddrV4, buf: &mut [u8; MAX_DGRAM]) {
        let Some(Proto::Response(Response::V1(response))) = Proto::decode(data) else {
            return;
        };
        match response {
            ResponseV1::Prepared { tx, addr, token } => {
                let Some(pending) = self.publishes.get_mut(&tx) else {
                    return;
                };
                // One put per server per transaction: otherwise every repeated
                // or forged `Prepared` reflects the whole value at that server.
                if !pending.awaiting.contains(&from) || !pending.prepared.insert(from) {
                    return;
                }
                let value = (pending.value)(addr);
                if value.len() > MAX_VALUE_LEN {
                    pending.too_large = true;
                    pending.awaiting.remove(&from);
                    if pending.awaiting.is_empty() {
                        self.finish_publish(tx);
                    }
                    return;
                }
                let request = Request::V1(RequestV1::Put { tx, token, value });
                if let Some(bytes) = encode(request, buf)
                    && let Err(err) = self.dht.send_datagram(bytes.to_vec(), from).await
                {
                    debug!(%from, %addr, %err, "send authorized request");
                }
            }
            ResponseV1::Stored { tx, addr } => {
                let Some(pending) = self.publishes.get_mut(&tx) else {
                    return;
                };
                if !pending.awaiting.remove(&from) {
                    return;
                }
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
            if pending.too_large {
                Err(e!(UdpError::TooLarge))
            } else {
                Err(e!(UdpError::Timeout))
            }
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
            Err(e!(UdpError::Timeout))
        } else {
            let mut values: Vec<_> = pending.values.into_iter().collect();
            values.sort();
            Ok(ResolveResult { values })
        };
        let _ = pending.response.send(result);
    }
}

/// Frames a request for sending, or `None` if it does not fit a datagram.
fn encode(value: Request, buf: &mut [u8; MAX_DGRAM]) -> Option<&[u8]> {
    Proto::Request(value).encode(buf).ok()
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}
