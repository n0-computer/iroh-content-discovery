//! Convenience wrapper around the UDP index client.

use n0_error::e;
use n0_future::StreamExt;
use std::{collections::HashSet, net::SocketAddrV4, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use udp_addr_index_proto::RENDEZVOUS_INFOHASH;

use n0_mainline::Dht;

use iroh_base::SecretKey;

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
    /// Refreshes on use after ten minutes. Discovery is limited to two candidates
    /// and thirty seconds; announcements are untrusted and do not prove availability.
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
    /// Addresses are discovery candidates; they do not prove availability.
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
        let mut peers = state
            .signed
            .as_ref()
            .and_then(|item| crate::ServerList::decode(item.value()))
            .map(|list| list.addresses().iter().copied().collect::<HashSet<_>>())
            .unwrap_or_default();
        if peers.is_empty() {
            if let Some(hash) = discovery.config.rendezvous_hash {
                tracing::debug!(infohash = %crate::infohash_hex(&hash), "discovering index servers through Mainline rendezvous");
                let lookup = async {
                    let mut stream = discovery.dht.get_peers(hash.into()).await?;
                    while let Some(batch) = stream.next().await {
                        for peer in batch {
                            if peer.port() != 0
                                && !peer.ip().is_unspecified()
                                && !peer.ip().is_multicast()
                                && !peer.ip().is_broadcast()
                            {
                                peers.insert(peer);
                                if peers.len() == 2 {
                                    return Ok::<_, UdpError>(());
                                }
                            }
                        }
                    }
                    Ok(())
                };
                match tokio::time::timeout(Duration::from_secs(30), lookup).await {
                    Ok(result) => result?,
                    Err(_) if peers.is_empty() => return Err(e!(UdpError::Timeout)),
                    Err(_) => {}
                }
            } else {
                signed_result.map_err(|_| e!(UdpError::Timeout))??;
            }
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

    /// Publish a record for `secret`'s endpoint to all responsive servers.
    ///
    /// Each server gets a record signed for the socket it observed, so a
    /// reader can tell a record published here from one copied out of another
    /// publisher's slot.
    ///
    /// Returns the public UDP sockets under which servers stored it.
    pub async fn publish(&self, secret: &SecretKey) -> Result<Vec<SocketAddrV4>, AddrIndexError> {
        self.refresh_replicas().await?;
        let secret = secret.clone();
        self.client
            .publish(move |addr| SignedRecord::sign(&secret, addr).encode())
            .await
            .map_err(Into::into)
    }

    /// Lookup the endpoints that listed `addr`.
    ///
    /// Records that were not signed for `addr` are discarded, so a record
    /// republished under another socket is not returned.
    pub async fn lookup(&self, addr: SocketAddrV4) -> Result<Vec<SignedRecord>, AddrIndexError> {
        self.refresh_replicas().await?;
        let result = self.client.resolve(addr).await?;
        let received = result.values.len();
        let records: Vec<_> = result
            .values
            .into_iter()
            .filter_map(|value| SignedRecord::decode(&value, addr))
            .collect();
        tracing::debug!(%addr, received, valid = records.len(), "validated index server endpoint records");
        Ok(records)
    }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ServerList;

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
