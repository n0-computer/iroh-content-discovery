//! Convenience wrapper around the UDP directory client.

use iroh_addr_index_proto::RENDEZVOUS_INFOHASH;
use n0_future::StreamExt;
use std::{collections::HashSet, net::SocketAddrV4, sync::Arc, time::Duration};
use tokio::sync::Mutex;

use n0_mainline::Dht;

use crate::{SignedRecord, UdpClient, UdpError};

/// Initial tracker discovery sources, tried in priority order.
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// Explicit tracker socket, used directly instead of either discovery source.
    pub tracker: Option<SocketAddrV4>,
    /// Trusted BEP44 signing key, tried first when set.
    pub public_key: Option<[u8; 32]>,
    /// Untrusted rendezvous fallback; `None` disables it.
    pub rendezvous_hash: Option<[u8; 20]>,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            tracker: None,
            public_key: None,
            rendezvous_hash: Some(RENDEZVOUS_INFOHASH),
        }
    }
}

/// UDP client for one or more directory replicas.
#[derive(Debug, Clone)]
pub struct Directory {
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
        if item.seq() < 0 || crate::TrackerList::decode(item.value()).is_none() {
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

impl Directory {
    /// Attach to a Mainline node's UDP socket and add one replica.
    pub async fn udp(dht: Dht, replica: SocketAddrV4) -> Result<Self, UdpError> {
        let client = UdpClient::attach(dht).await?;
        client.add_replica(replica).await?;
        Ok(Self::from_udp(client))
    }

    /// Discover replicas through Mainline using the same socket for index traffic.
    ///
    /// Refreshes on use after ten minutes. Discovery is limited to two candidates
    /// and thirty seconds; announcements are untrusted and do not prove availability.
    pub async fn discover(dht: Dht) -> Result<Self, UdpError> {
        Self::discover_with_config(dht, DiscoveryConfig::default()).await
    }

    /// Discover with a trusted BEP44 tracker list in addition to rendezvous peers.
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

    /// Discover at most two trackers, trying BEP44 before the rendezvous fallback.
    ///
    /// Each lookup has a thirty-second deadline. Refreshes on use after ten
    /// minutes, retaining the highest signed sequence for this directory's lifetime.
    /// Addresses are discovery candidates; they do not prove availability.
    pub async fn discover_with_config(dht: Dht, config: DiscoveryConfig) -> Result<Self, UdpError> {
        if let Some(tracker) = config.tracker {
            return Self::udp(dht, tracker).await;
        }
        let client = UdpClient::attach(dht.clone()).await?;
        let directory = Self {
            client,
            discovery: Some(Arc::new(Discovery {
                dht,
                config,
                state: Mutex::new(DiscoveryState::default()),
            })),
        };
        directory.refresh_replicas().await?;
        Ok(directory)
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
                .get_mutable(&key, Some(crate::TRACKER_LIST_SALT), None)
                .await?;
            while let Some(item) = stream.next().await {
                state.accept(item);
            }
            Ok(())
        };
        let signed_result = tokio::time::timeout(Duration::from_secs(30), signed_lookup).await;
        let mut peers = state
            .signed
            .as_ref()
            .and_then(|item| crate::TrackerList::decode(item.value()))
            .map(|list| list.addresses().iter().copied().collect::<HashSet<_>>())
            .unwrap_or_default();
        if peers.is_empty() {
            if let Some(hash) = discovery.config.rendezvous_hash {
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
                    Err(_) if peers.is_empty() => return Err(UdpError::Timeout),
                    Err(_) => {}
                }
            } else {
                signed_result.map_err(|_| UdpError::Timeout)??;
            }
        }
        if peers.is_empty() {
            return Err(UdpError::NoReplicas);
        }
        self.client.replace_replicas(peers).await?;
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

    /// Publish a signed endpoint record to all responsive replicas.
    ///
    /// Returns the public UDP sockets under which replicas stored it.
    pub async fn publish(
        &self,
        record: &SignedRecord,
    ) -> Result<Vec<SocketAddrV4>, DirectoryError> {
        self.refresh_replicas().await?;
        let value = record.encode().map_err(|_| DirectoryError::Encoding)?;
        self.client.publish(value).await.map_err(Into::into)
    }

    /// Lookup the endpoint that listed `addr`.
    pub async fn lookup(&self, addr: SocketAddrV4) -> Result<Vec<SignedRecord>, DirectoryError> {
        self.refresh_replicas().await?;
        let result = self.client.resolve(addr).await?;
        Ok(result
            .values
            .into_iter()
            .filter_map(|value| SignedRecord::decode(&value))
            .collect())
    }
}

impl From<UdpClient> for Directory {
    fn from(value: UdpClient) -> Self {
        Self::from_udp(value)
    }
}

/// Error from [`Directory::publish`] or [`Directory::lookup`].
#[derive(Debug)]
pub enum DirectoryError {
    /// UDP transport failed.
    Udp(UdpError),
    /// The signed record could not be encoded.
    Encoding,
}

impl std::fmt::Display for DirectoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Udp(err) => err.fmt(f),
            Self::Encoding => write!(f, "could not encode signed endpoint record"),
        }
    }
}

impl std::error::Error for DirectoryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Udp(err) => Some(err),
            Self::Encoding => None,
        }
    }
}

impl From<UdpError> for DirectoryError {
    fn from(value: UdpError) -> Self {
        Self::Udp(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TrackerList;

    #[test]
    fn signed_updates_reject_rollback_and_malformed_values() {
        let key = n0_mainline::SigningKey::from_bytes(&[42; 32]);
        let list = TrackerList::new(vec!["203.0.113.1:1234".parse().unwrap()]).unwrap();
        let mut state = DiscoveryState::default();
        state.accept(list.sign(&key, 2).unwrap());
        state.accept(list.sign(&key, 1).unwrap());
        assert_eq!(state.signed.as_ref().unwrap().seq(), 2);
        state.accept(n0_mainline::MutableItem::new(
            &key,
            &[255],
            3,
            Some(crate::TRACKER_LIST_SALT),
        ));
        assert_eq!(state.signed.as_ref().unwrap().seq(), 2);
        // A newer empty list explicitly withdraws the signed candidates.
        state.accept(TrackerList::new(vec![]).unwrap().sign(&key, 4).unwrap());
        assert_eq!(state.signed.as_ref().unwrap().seq(), 4);
        assert!(
            TrackerList::decode(state.signed.as_ref().unwrap().value())
                .unwrap()
                .addresses()
                .is_empty()
        );
    }
}
