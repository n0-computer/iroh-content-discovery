//! Convenience wrapper around the UDP index client.

use n0_error::e;
use n0_future::{BufferedStreamExt, Stream, StreamExt, stream};
use std::{collections::HashSet, net::SocketAddrV4, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use udp_addr_index_proto::RENDEZVOUS_INFOHASH;

use n0_mainline::Dht;

use crate::{SignedRecord, UdpClient, UdpError};

/// Initial index server discovery sources, tried in priority order.
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// Explicit index server socket, used directly instead of either discovery source.
    pub server: Option<SocketAddrV4>,
    /// Trusted BEP44 signing key, tried first when set.
    pub public_key: Option<[u8; 32]>,
    /// Untrusted rendezvous fallback; `None` disables it.
    pub rendezvous_hash: Option<[u8; 20]>,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            server: None,
            public_key: None,
            rendezvous_hash: Some(RENDEZVOUS_INFOHASH),
        }
    }
}

/// UDP client for one or more index servers.
#[derive(Debug, Clone)]
pub struct AddrIndex {
    client: UdpClient,
    discovery: Option<Arc<Discovery>>,
}

#[derive(Debug)]
struct Discovery {
    dht: Dht,
    config: DiscoveryConfig,
    state: Mutex<DiscoveryState>,
}

#[derive(Debug, Default)]
struct DiscoveryState {
    refreshed: Option<tokio::time::Instant>,
    signed: Option<n0_mainline::MutableItem>,
}

impl DiscoveryState {
    fn accept(&mut self, item: n0_mainline::MutableItem) {
        if item.seq() < 0 || crate::ServerList::decode(item.value()).is_none() {
            return;
        }
        if self
            .signed
            .as_ref()
            .is_none_or(|old| (item.seq(), item.value()) > (old.seq(), old.value()))
        {
            self.signed = Some(item);
        }
    }
}

impl AddrIndex {
    /// Attach to a Mainline node's UDP socket and add one server.
    pub async fn udp(dht: Dht, server: SocketAddrV4) -> Result<Self, UdpError> {
        let client = UdpClient::attach(dht).await?;
        client.add_server(server).await?;
        Ok(Self::from_udp(client))
    }

    /// Discover servers through Mainline using the same socket for index traffic.
    ///
    /// Refreshes on use after ten minutes. Probes candidates concurrently and
    /// takes the first two to reply, with a thirty-second discovery deadline.
    pub async fn discover(dht: Dht) -> Result<Self, UdpError> {
        Self::discover_with_config(dht, DiscoveryConfig::default()).await
    }

    /// Discover with a trusted BEP44 server list in addition to rendezvous peers.
    ///
    /// Uses the default rendezvous hash only if no signed addresses are available.
    pub async fn discover_with_authority(dht: Dht, public_key: [u8; 32]) -> Result<Self, UdpError> {
        Self::discover_with_config(
            dht,
            DiscoveryConfig {
                public_key: Some(public_key),
                ..DiscoveryConfig::default()
            },
        )
        .await
    }

    /// Discover at most two index servers, trying BEP44 before the rendezvous fallback.
    ///
    /// Each lookup has a thirty-second deadline. Refreshes on use after ten
    /// minutes, retaining the highest signed sequence for this index's lifetime.
    /// Candidates must answer an address lookup before they are selected.
    pub async fn discover_with_config(dht: Dht, config: DiscoveryConfig) -> Result<Self, UdpError> {
        tracing::debug!(?config, "configuring index server discovery");
        if let Some(server) = config.server {
            return Self::udp(dht, server).await;
        }
        let client = UdpClient::attach(dht.clone()).await?;
        let index = Self {
            client,
            discovery: Some(Arc::new(Discovery {
                dht,
                config,
                state: Mutex::new(DiscoveryState::default()),
            })),
        };
        index.refresh_replicas().await?;
        Ok(index)
    }

    async fn refresh_replicas(&self) -> Result<(), UdpError> {
        let Some(discovery) = &self.discovery else {
            return Ok(());
        };
        let mut state = discovery.state.lock().await;
        if state
            .refreshed
            .is_some_and(|time| time.elapsed() < Duration::from_secs(600))
        {
            return Ok(());
        }
        let signed_lookup = async {
            let Some(key) = discovery.config.public_key else {
                return Ok::<_, UdpError>(());
            };
            let mut stream = discovery
                .dht
                .get_mutable(&key, Some(crate::SERVER_LIST_SALT), None)
                .await?;
            while let Some(item) = stream.next().await {
                state.accept(item);
            }
            Ok(())
        };
        let signed_result = tokio::time::timeout(Duration::from_secs(30), signed_lookup).await;
        tracing::debug!(?signed_result, "signed index-list lookup completed");
        let signed_peers = state
            .signed
            .as_ref()
            .and_then(|item| crate::ServerList::decode(item.value()))
            .map(|list| list.addresses().to_vec())
            .unwrap_or_default();
        let mut peers = HashSet::new();
        let lookup = async {
            let candidates = if !signed_peers.is_empty() {
                stream::iter(signed_peers).boxed()
            } else if let Some(hash) = discovery.config.rendezvous_hash {
                tracing::debug!(infohash = %crate::infohash_hex(&hash), "discovering index servers through Mainline rendezvous");
                discovery
                    .dht
                    .get_peers(hash.into())
                    .await?
                    .flat_map(stream::iter)
                    .boxed()
            } else {
                signed_result.map_err(|_| e!(UdpError::Timeout))??;
                return Err(e!(UdpError::NoServers));
            };
            let mut responsive = responsive_servers(self.client.clone(), candidates).take(2);
            while let Some(server) = responsive.next().await {
                peers.insert(server);
            }
            Ok(())
        };
        match tokio::time::timeout(Duration::from_secs(30), lookup).await {
            Ok(result) => result?,
            Err(_) if peers.is_empty() => return Err(e!(UdpError::Timeout)),
            Err(_) => {}
        }
        if peers.is_empty() {
            tracing::debug!("index server discovery found no servers");
            return Err(e!(UdpError::NoServers));
        }
        tracing::debug!(?peers, "using discovered index servers");
        self.client.replace_servers(peers).await?;
        state.refreshed = Some(tokio::time::Instant::now());
        Ok(())
    }

    /// Wrap an existing UDP client.
    pub fn from_udp(client: UdpClient) -> Self {
        Self {
            client,
            discovery: None,
        }
    }

    /// Publish a signed endpoint record to all responsive servers.
    ///
    /// Returns the public UDP sockets under which servers stored it.
    pub async fn publish(
        &self,
        record: &SignedRecord,
    ) -> Result<Vec<SocketAddrV4>, AddrIndexError> {
        self.refresh_replicas().await?;
        let value = record.encode().map_err(|_| e!(AddrIndexError::Encoding))?;
        self.client.publish(value).await.map_err(Into::into)
    }

    /// Lookup the endpoint that listed `addr`.
    pub async fn lookup(&self, addr: SocketAddrV4) -> Result<Vec<SignedRecord>, AddrIndexError> {
        self.refresh_replicas().await?;
        let result = self.client.resolve(addr).await?;
        let received = result.values.len();
        let records: Vec<_> = result
            .values
            .into_iter()
            .filter_map(|value| SignedRecord::decode(&value))
            .collect();
        tracing::debug!(%addr, received, valid = records.len(), "validated index server endpoint records");
        Ok(records)
    }
}

/// Yield distinct candidates that answer an address lookup, in completion order.
fn responsive_servers(
    client: UdpClient,
    candidates: impl Stream<Item = SocketAddrV4> + Send + 'static,
) -> stream::Boxed<SocketAddrV4> {
    let mut seen = HashSet::new();
    candidates
        .filter(move |server| {
            server.port() != 0
                && !server.ip().is_unspecified()
                && !server.ip().is_multicast()
                && !server.ip().is_broadcast()
                && seen.insert(*server)
        })
        .map(move |server| {
            let client = client.clone();
            async move {
                match client.probe(server).await {
                    Ok(()) => {
                        tracing::debug!(%server, "index server responded to probe");
                        Some(server)
                    }
                    Err(error) => {
                        tracing::debug!(%server, %error, "index server probe failed; skipping");
                        None
                    }
                }
            }
        })
        .buffered_unordered(3)
        .filter_map(|server| server)
        .boxed()
}

impl From<UdpClient> for AddrIndex {
    fn from(value: UdpClient) -> Self {
        Self::from_udp(value)
    }
}

/// Error from [`AddrIndex::publish`] or [`AddrIndex::lookup`].
#[n0_error::stack_error(derive, add_meta)]
pub enum AddrIndexError {
    /// UDP transport failed.
    #[error(transparent)]
    Udp {
        /// Underlying UDP client error.
        #[error(from, source)]
        source: UdpError,
    },
    /// The signed record could not be encoded.
    #[error("could not encode signed endpoint record")]
    Encoding {},
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ServerList;

    #[tokio::test]
    async fn selects_first_two_responders_without_changing_configured_servers() {
        use tokio::net::UdpSocket;
        use udp_addr_index_proto::{MAX_DGRAM, Request, RequestV1, Response, ResponseV1};

        async fn responder() -> (SocketAddrV4, tokio::task::JoinHandle<()>) {
            let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let std::net::SocketAddr::V4(addr) = socket.local_addr().unwrap() else {
                unreachable!();
            };
            let task = tokio::spawn(async move {
                let mut buf = [0; MAX_DGRAM];
                loop {
                    let (len, from) = socket.recv_from(&mut buf).await.unwrap();
                    let Some(Request::V1(RequestV1::Get { tx, addr })) =
                        Request::decode(&buf[..len])
                    else {
                        panic!("expected a lookup probe");
                    };
                    let reply = Response::V1(ResponseV1::Value {
                        tx,
                        addr,
                        value: None,
                    });
                    let bytes = reply.encode(&mut buf).unwrap();
                    socket.send_to(bytes, from).await.unwrap();
                }
            });
            (addr, task)
        }

        let (first, first_task) = responder().await;
        let (second, second_task) = responder().await;
        let silent = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(silent_addr) = silent.local_addr().unwrap() else {
            unreachable!();
        };
        let dht = Dht::builder().no_bootstrap().port(0).build().unwrap();
        let client = UdpClient::attach(dht).await.unwrap();
        client.add_server(first).await.unwrap();
        let candidates = stream::iter([silent_addr, first, first, second]);
        let selected = tokio::time::timeout(
            Duration::from_secs(1),
            responsive_servers(client.clone(), candidates)
                .take(2)
                .collect::<Vec<_>>(),
        )
        .await
        .expect("silent candidate must not delay responsive ones");
        assert_eq!(selected.len(), 2);
        assert_eq!(
            selected.into_iter().collect::<HashSet<_>>(),
            HashSet::from([first, second])
        );

        // A configured healthy server cannot answer on behalf of the target.
        assert!(matches!(
            client.probe(silent_addr).await,
            Err(UdpError::Timeout { .. })
        ));
        client.remove_server(first).await.unwrap();
        assert!(matches!(
            client.resolve(first).await,
            Err(UdpError::NoServers { .. })
        ));
        first_task.abort();
        second_task.abort();
    }

    #[test]
    fn signed_updates_reject_rollback_and_malformed_values() {
        let key = n0_mainline::SigningKey::from_bytes(&[42; 32]);
        let list = ServerList::new(vec!["203.0.113.1:1234".parse().unwrap()]).unwrap();
        let mut state = DiscoveryState::default();
        state.accept(list.sign(&key, 2).unwrap());
        state.accept(list.sign(&key, 1).unwrap());
        assert_eq!(state.signed.as_ref().unwrap().seq(), 2);
        state.accept(n0_mainline::MutableItem::new(
            &key,
            &[255],
            3,
            Some(crate::SERVER_LIST_SALT),
        ));
        assert_eq!(state.signed.as_ref().unwrap().seq(), 2);
        // A newer empty list explicitly withdraws the signed candidates.
        state.accept(ServerList::new(vec![]).unwrap().sign(&key, 4).unwrap());
        assert_eq!(state.signed.as_ref().unwrap().seq(), 4);
        assert!(
            ServerList::decode(state.signed.as_ref().unwrap().value())
                .unwrap()
                .addresses()
                .is_empty()
        );
    }
}
