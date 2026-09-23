//! Publish signed Pkarr names and keep them alive on Mainline.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use n0_error::{Result, StdResultExt};
use n0_future::task::{self, AbortOnDropHandle};
use n0_mainline::{Dht, MutableItem, SigningKey};
use simple_dns::{
    CLASS, Name, Packet, ResourceRecord,
    rdata::{HTTPS, RData, SVCB},
};
use tokio::sync::{Notify, watch};

/// How often each packet is republished.
///
/// Mainline drops records that are not refreshed, so the publisher has to stay
/// alive for its names to stay resolvable.
pub const PKARR_REFRESH: Duration = Duration::from_secs(10 * 60);

/// Domain under which a BLAKE3 hash names content, as served by the gateway.
pub const BLAKE3_DOMAIN: &str = "blake3.net";

/// Domain under which a Pkarr public key names content.
pub const PKARR_DOMAIN: &str = "pkarr.net";

/// Time to wait before retrying after a failed publish.
const RETRY: Duration = Duration::from_secs(30);

/// Time to live of published records, in seconds.
const TTL: u32 = 300;

/// The name a public key is published under, in z-base-32.
pub fn pkarr_name(public_key: &[u8; 32]) -> String {
    z32::encode(public_key)
}

/// Publishes signed Pkarr packets and republishes them until dropped.
///
/// One packet is held per key, so a publisher can serve several names at once.
/// Setting a key's contents replaces its packet and publishes it immediately;
/// every packet is republished every [`PKARR_REFRESH`].
///
/// Publishing runs in a background task owned by this handle. Dropping the
/// handle stops it, and the names expire from the DHT soon after.
///
/// # Examples
///
/// ```no_run
/// # use iroh_mainline_endpoint_discovery::PkarrPublisher;
/// # use n0_mainline::{Dht, SigningKey};
/// # fn main() -> n0_error::Result<()> {
/// let publisher = PkarrPublisher::new(Dht::client()?);
/// let key = SigningKey::from_bytes(&[7; 32]);
/// publisher.set_https(&key, "", "example.com")?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PkarrPublisher {
    state: Arc<State>,
    _task: Arc<AbortOnDropHandle<()>>,
}

#[derive(Debug)]
struct State {
    dht: Dht,
    packets: Mutex<HashMap<[u8; 32], Entry>>,
    changed: Notify,
    /// Counts edits, so a waiter can tell whether its own change is published.
    generation: Mutex<u64>,
    /// The newest generation that has been published successfully.
    published: watch::Sender<u64>,
    /// Held while publishing, so two rounds cannot race for a key. Mainline
    /// rejects a write whose sequence is not the newest it has seen.
    publishing: tokio::sync::Mutex<i64>,
}

#[derive(Debug)]
struct Entry {
    key: SigningKey,
    packet: Vec<u8>,
}

impl PkarrPublisher {
    /// Start a publisher on `dht`, with no names yet.
    pub fn new(dht: Dht) -> Self {
        let state = Arc::new(State {
            dht,
            packets: Mutex::new(HashMap::new()),
            changed: Notify::new(),
            generation: Mutex::new(0),
            published: watch::channel(0).0,
            publishing: tokio::sync::Mutex::new(0),
        });
        let task = task::spawn({
            let state = state.clone();
            async move { state.run().await }
        });
        Self {
            state,
            _task: Arc::new(AbortOnDropHandle::new(task)),
        }
    }

    /// Publish an `HTTPS` record under `key`, pointing at `target`.
    ///
    /// `name` is the label inside the key's zone; an empty label is the apex,
    /// which is what resolvers look up for the bare name. `target` is a
    /// hostname without a scheme, port or path.
    ///
    /// # Errors
    ///
    /// Fails if `target` is not a hostname or the packet exceeds the BEP44
    /// size limit.
    pub fn set_https(&self, key: &SigningKey, name: &str, target: &str) -> Result<()> {
        n0_error::ensure_any!(
            is_hostname(target),
            "target must be a hostname, without a scheme, port, or path"
        );
        let zone = pkarr_name(&key.verifying_key().to_bytes());
        let record = match name {
            "" | "@" => zone,
            label => format!("{label}.{zone}"),
        };
        let mut packet = Packet::new_reply(0);
        packet.answers.push(ResourceRecord::new(
            Name::new(&record).anyerr()?,
            CLASS::IN,
            TTL,
            RData::HTTPS(HTTPS(SVCB::new(0, Name::new(target).anyerr()?))),
        ));
        self.set_raw(key, &packet)
    }

    /// Publish `hash` under `key`, as an apex `HTTPS` record on [`BLAKE3_DOMAIN`].
    ///
    /// The key then names content that a gateway serves directly, and the
    /// name survives the content changing.
    ///
    /// # Errors
    ///
    /// Fails like [`Self::set_https`].
    pub fn set_blake3(&self, key: &SigningKey, hash: &[u8; 32]) -> Result<()> {
        self.set_https(key, "", &format!("{}.{BLAKE3_DOMAIN}", z32::encode(hash)))
    }

    /// Publish `packet` under `key`, replacing anything set for it.
    ///
    /// Record names are not checked; resolvers only read records named for
    /// the key's own zone.
    ///
    /// # Errors
    ///
    /// Fails if the packet cannot be encoded or exceeds the BEP44 size limit.
    pub fn set_raw(&self, key: &SigningKey, packet: &Packet<'_>) -> Result<()> {
        let bytes = packet.build_bytes_vec_compressed().anyerr()?;
        n0_error::ensure_any!(
            bytes.len() <= 1000,
            "DNS packet exceeds the BEP44 size limit"
        );
        let entry = Entry {
            key: key.clone(),
            packet: bytes,
        };
        self.state
            .packets
            .lock()
            .expect("poisoned")
            .insert(key.verifying_key().to_bytes(), entry);
        self.state.edited();
        Ok(())
    }

    /// Stop publishing the name of `public_key`.
    ///
    /// The DHT keeps serving it until the record expires.
    pub fn remove(&self, public_key: &[u8; 32]) -> bool {
        let removed = self
            .state
            .packets
            .lock()
            .expect("poisoned")
            .remove(public_key)
            .is_some();
        if removed {
            self.state.edited();
        }
        removed
    }

    /// Wait until everything set so far has been published at least once.
    ///
    /// Names set after this call may still be unpublished when it returns.
    pub async fn wait_published(&self) {
        let generation = *self.state.generation.lock().expect("poisoned");
        let mut published = self.state.published.subscribe();
        while *published.borrow() < generation {
            if published.changed().await.is_err() {
                return;
            }
        }
    }

    /// Publish every name now, instead of waiting for the background task.
    ///
    /// # Errors
    ///
    /// Returns the first failure; the remaining names are still attempted by
    /// the background task.
    pub async fn publish_all(&self) -> Result<()> {
        self.state.publish_round().await
    }
}

impl State {
    /// Publish every name, then wait for a change or the refresh interval.
    async fn run(&self) {
        loop {
            let delay = match self.publish_round().await {
                Ok(()) => PKARR_REFRESH,
                Err(err) => {
                    tracing::warn!(%err, "Pkarr publish failed");
                    RETRY
                }
            };
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = self.changed.notified() => {}
            }
        }
    }

    /// Record an edit, and wake the publishing task.
    fn edited(&self) {
        *self.generation.lock().expect("poisoned") += 1;
        self.changed.notify_one();
    }

    /// Sign and publish every packet once.
    ///
    /// Returns the first failure, after attempting all of them.
    async fn publish_round(&self) -> Result<()> {
        let mut previous = self.publishing.lock().await;
        // Read the generation before publishing, so an edit made while this
        // round runs is not reported as published.
        let generation = *self.generation.lock().expect("poisoned");
        let mut result = Ok(());
        for item in self.sign_all(&mut previous)? {
            match self.dht.put_mutable(item.clone(), None).await {
                Ok(_) => tracing::debug!(
                    name = %pkarr_name(item.key()),
                    sequence = item.seq(),
                    "published Pkarr name"
                ),
                Err(err) if result.is_ok() => result = Err(err.into()),
                Err(_) => {}
            }
        }
        if result.is_ok() {
            self.published.send_replace(generation);
        }
        result
    }

    /// Sign the current packets, with a sequence number that always grows.
    fn sign_all(&self, previous: &mut i64) -> Result<Vec<MutableItem>> {
        // Mainline keeps the highest sequence it has seen, so a round that
        // lands in the same microsecond as the last one still has to count up.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .anyerr()?
            .as_micros() as i64;
        *previous = now.max(*previous + 1);
        let sequence = *previous;
        Ok(self
            .packets
            .lock()
            .expect("poisoned")
            .values()
            .map(|entry| MutableItem::new(&entry.key, &entry.packet, sequence, None))
            .collect())
    }
}

/// Returns whether `value` is a plain DNS hostname.
fn is_hostname(value: &str) -> bool {
    let value = value.strip_suffix('.').unwrap_or(value);
    value.len() <= 253
        && value.contains('.')
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use n0_future::StreamExt;

    fn publisher() -> PkarrPublisher {
        PkarrPublisher::new(Dht::builder().no_bootstrap().port(0).build().unwrap())
    }

    #[tokio::test]
    async fn rejects_targets_that_are_not_hostnames() {
        let publisher = publisher();
        let key = SigningKey::from_bytes(&[17; 32]);
        for target in ["https://example.com", "example.com/path", "localhost", "."] {
            assert!(publisher.set_https(&key, "", target).is_err(), "{target}");
        }
        assert!(publisher.set_https(&key, "", "example.com").is_ok());
        assert!(publisher.set_https(&key, "www", "sub.example.com.").is_ok());
        assert!(publisher.set_blake3(&key, &[3; 32]).is_ok());
    }

    #[tokio::test]
    async fn publishes_several_names_and_updates_them() {
        let network = n0_mainline::Testnet::new(3).await.unwrap();
        let node = || {
            Dht::builder()
                .bootstrap(&network.bootstrap)
                .port(0)
                .build()
                .unwrap()
        };
        let publisher = PkarrPublisher::new(node());
        let site = SigningKey::from_bytes(&[23; 32]);
        let content = SigningKey::from_bytes(&[24; 32]);
        publisher.set_https(&site, "", "example.com").unwrap();
        publisher.set_blake3(&content, &[5; 32]).unwrap();
        publisher.publish_all().await.unwrap();

        let reader = node();
        assert_eq!(target_of(&reader, &site).await, "example.com");
        assert_eq!(
            target_of(&reader, &content).await,
            format!("{}.{BLAKE3_DOMAIN}", z32::encode(&[5; 32]))
        );

        // A new target replaces the old one under the same name.
        publisher.set_blake3(&content, &[6; 32]).unwrap();
        publisher.publish_all().await.unwrap();
        assert_eq!(
            target_of(&reader, &content).await,
            format!("{}.{BLAKE3_DOMAIN}", z32::encode(&[6; 32]))
        );

        assert!(publisher.remove(&content.verifying_key().to_bytes()));
        assert!(!publisher.remove(&content.verifying_key().to_bytes()));

        // The background task publishes edits on its own.
        let late = SigningKey::from_bytes(&[25; 32]);
        publisher.set_https(&late, "", "late.example").unwrap();
        tokio::time::timeout(Duration::from_secs(10), publisher.wait_published())
            .await
            .expect("publishing a new name timed out");
        assert_eq!(target_of(&reader, &late).await, "late.example");
    }

    /// Resolve the newest packet for `key` and return its HTTPS target.
    async fn target_of(dht: &Dht, key: &SigningKey) -> String {
        let public = key.verifying_key().to_bytes();
        let mut items = dht.get_mutable(&public, None, None).await.unwrap();
        let mut newest: Option<MutableItem> = None;
        while let Some(item) = items.next().await {
            if newest.as_ref().is_none_or(|old| item.seq() > old.seq()) {
                newest = Some(item);
            }
        }
        let newest = newest.expect("record not found");
        let packet = Packet::parse(newest.value()).unwrap();
        assert_eq!(packet.answers[0].name.to_string(), pkarr_name(&public));
        let RData::HTTPS(https) = &packet.answers[0].rdata else {
            panic!("expected an HTTPS record")
        };
        https.0.target.to_string()
    }
}
