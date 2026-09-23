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
    MAGIC, MAX_DGRAM, MAX_VALUE_LEN, Request, RequestV1, Response, ResponseV1, TransactionId,
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

enum ActorMsg {
    AddServer(SocketAddrV4),
    ReplaceServers(HashSet<SocketAddrV4>),
    RemoveServer(SocketAddrV4),
    Publish(
        Vec<u8>,
        oneshot::Sender<Result<Vec<SocketAddrV4>, UdpError>>,
    ),
    Resolve(
        SocketAddrV4,
        Option<SocketAddrV4>,
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

    /// Add a server used by subsequent operations.
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

    /// Remove a configured server.
    pub async fn remove_server(&self, server: SocketAddrV4) -> Result<(), UdpError> {
        self.tx
            .send(ActorMsg::RemoveServer(server))
            .await
            .map_err(|_| e!(UdpError::Closed))
    }

    /// Obtain tokens and publish `value` to every responsive server.
    ///
    /// Returns the public IPv4 sockets under which servers stored the value.
    pub async fn publish(&self, value: Vec<u8>) -> Result<Vec<SocketAddrV4>, UdpError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMsg::Publish(value, tx))
            .await
            .map_err(|_| e!(UdpError::Closed))?;
        rx.await.map_err(|_| e!(UdpError::Closed))?
    }

    /// Publish to one server, adding it to this client first.
    pub async fn publish_to(
        &self,
        server: SocketAddrV4,
        value: Vec<u8>,
    ) -> Result<Vec<SocketAddrV4>, UdpError> {
        self.add_server(server).await?;
        self.publish(value).await
    }

    /// Read and deduplicate opaque values from all configured servers.
    pub async fn resolve(&self, addr: SocketAddrV4) -> Result<ResolveResult, UdpError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMsg::Resolve(addr, None, tx))
            .await
            .map_err(|_| e!(UdpError::Closed))?;
        rx.await.map_err(|_| e!(UdpError::Closed))?
    }

    /// Check one candidate without changing the configured server set.
    pub(crate) async fn probe(&self, server: SocketAddrV4) -> Result<(), UdpError> {
        let addr = SocketAddrV4::new(rand::random::<[u8; 4]>().into(), rand::random());
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMsg::Resolve(addr, Some(server), tx))
            .await
            .map_err(|_| e!(UdpError::Closed))?;
        // An empty Value response is just as useful as a hit for liveness.
        rx.await.map_err(|_| e!(UdpError::Closed))?.map(|_| ())
    }

    /// Resolve through one server, adding it to this client first.
    pub async fn resolve_from(
        &self,
        server: SocketAddrV4,
        addr: SocketAddrV4,
    ) -> Result<ResolveResult, UdpError> {
        self.add_server(server).await?;
        self.resolve(addr).await
    }
}

/// Result of an opaque address lookup.
#[derive(Debug, Clone, Default)]
pub struct ResolveResult {
    /// Deduplicated opaque values returned by servers.
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
    incoming: mpsc::Receiver<(Box<[u8]>, SocketAddrV4)>,
    rx: mpsc::Receiver<ActorMsg>,
    servers: HashSet<SocketAddrV4>,
    publishes: HashMap<TransactionId, PendingPublish>,
    resolves: HashMap<TransactionId, PendingResolve>,
    next_tx: TransactionId,
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
                if value.len() > MAX_VALUE_LEN {
                    let _ = response.send(Err(e!(UdpError::TooLarge)));
                    return;
                }
                if self.servers.is_empty() {
                    let _ = response.send(Err(e!(UdpError::NoServers)));
                    return;
                }
                let tx = self.next_id();
                let request = Request::V1(RequestV1::Prepare {
                    tx,
                    padding: [0; 24],
                });
                if let Some(bytes) = encode(&request, buf) {
                    for server in &self.servers {
                        if let Err(err) = self.dht.send_datagram(bytes.to_vec(), *server).await {
                            debug!(%server, %err, "send prepare");
                        }
                    }
                    self.publishes.insert(
                        tx,
                        PendingPublish {
                            value,
                            awaiting: self.servers.clone(),
                            stored: HashSet::new(),
                            response,
                            deadline: tokio::time::Instant::now() + self.timeout,
                        },
                    );
                } else {
                    let _ = response.send(Err(e!(UdpError::TooLarge)));
                }
            }
            ActorMsg::Resolve(addr, server, response) => {
                let servers = server
                    .map(|server| HashSet::from([server]))
                    .unwrap_or_else(|| self.servers.clone());
                if servers.is_empty() {
                    let _ = response.send(Err(e!(UdpError::NoServers)));
                    return;
                }
                let tx = self.next_id();
                let request = Request::V1(RequestV1::Get { tx, addr });
                if let Some(bytes) = encode(&request, buf) {
                    for server in &servers {
                        if let Err(err) = self.dht.send_datagram(bytes.to_vec(), *server).await {
                            debug!(%server, %err, "send get");
                        }
                    }
                    self.resolves.insert(
                        tx,
                        PendingResolve {
                            addr,
                            awaiting: servers,
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
            Err(e!(UdpError::Timeout))
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

fn encode<'a>(value: &Request, buf: &'a mut [u8; MAX_DGRAM]) -> Option<&'a [u8]> {
    value.encode(buf).ok()
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}
