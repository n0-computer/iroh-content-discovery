//! Optional content validation between provider discovery and downloading.

use std::{collections::HashSet, future::Future, sync::Arc, time::Duration};

use iroh::{Endpoint, EndpointId};
use iroh_blobs::Hash;
use n0_future::{BufferedStreamExt, Stream, StreamExt, stream};
use tokio::time::Instant;
use tracing::debug;

use crate::provider_cache::{Probed, ProviderCache};

const CONCURRENT_PROBES: usize = 3;
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Where a candidate provider came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Provenance {
    /// Announced for the content and found through discovery: a claim.
    Discovered,
    /// Named by a link for the content: also a claim.
    Linked,
    /// Passed a probe for the content before.
    ///
    /// Probed again all the same for now. A recent or fast probe could let a
    /// provider skip it, or go ahead of others.
    Verified(Probed),
}

/// Filters a provider stream to endpoints that serve a verified size for `hash`.
///
/// This optional stage is independent of discovery. Each distinct endpoint is
/// connected to and queried for the blob's last Bao chunk, validating the size
/// against the requested hash rather than trusting the response's size header.
/// Up to three probes run concurrently, each with a ten-second total deadline,
/// so the fastest provider tends to come first. Failed probes are logged and
/// skipped; endpoints are yielded in completion order. Dropping the stream
/// cancels pending probes.
///
/// Only endpoint IDs are returned. The consumer connects again for downloading;
/// successful validation does not guarantee that this later transfer succeeds.
/// The caller should impose an overall deadline on discovery and filtering.
pub fn filter_verified_providers(
    endpoint: Endpoint,
    hash: Hash,
    providers: impl Stream<Item = EndpointId> + Send + 'static,
) -> stream::Boxed<EndpointId> {
    verified_providers(
        endpoint,
        hash,
        providers.map(|provider| (provider, Provenance::Discovered)),
        None,
    )
}

/// Like [`filter_verified_providers`], recording each probe's outcome in `cache`.
pub(crate) fn verified_providers(
    endpoint: Endpoint,
    hash: Hash,
    providers: impl Stream<Item = (EndpointId, Provenance)> + Send + 'static,
    cache: Option<Arc<ProviderCache>>,
) -> stream::Boxed<EndpointId> {
    race_probes(providers, move |provider, provenance| {
        let endpoint = endpoint.clone();
        let cache = cache.clone();
        async move {
            let latency = probe(&endpoint, hash, provider, provenance).await;
            if let Some(cache) = cache {
                match latency {
                    Some(latency) => cache.confirm(hash, provider, latency),
                    None => cache.forget(hash, provider),
                }
            }
            latency.is_some()
        }
    })
}

/// Returns how long it took `provider` to serve a verified size for `hash`,
/// or `None` if it did not.
async fn probe(
    endpoint: &Endpoint,
    hash: Hash,
    provider: EndpointId,
    provenance: Provenance,
) -> Option<Duration> {
    let started = Instant::now();
    debug!(%hash, %provider, ?provenance, "probing provider with verified size request");
    let probe = async {
        let connection = endpoint.connect(provider, iroh_blobs::ALPN).await?;
        crate::verified_size(&connection, hash).await
    };
    match tokio::time::timeout(PROBE_TIMEOUT, probe).await {
        Ok(Ok(size)) => {
            let latency = started.elapsed();
            debug!(%hash, %provider, size, elapsed_ms = latency.as_millis(), "provider size validated");
            Some(latency)
        }
        Ok(Err(error)) => {
            debug!(%hash, %provider, ?error, elapsed_ms = started.elapsed().as_millis(), "provider probe failed; skipping");
            None
        }
        Err(_) => {
            debug!(%hash, %provider, elapsed_ms = started.elapsed().as_millis(), "provider probe timed out; skipping");
            None
        }
    }
}

/// Yields the candidates whose probe passes, fastest first, probing up to
/// [`CONCURRENT_PROBES`] at once and each endpoint only once.
fn race_probes<F>(
    providers: impl Stream<Item = (EndpointId, Provenance)> + Send + 'static,
    probe: impl Fn(EndpointId, Provenance) -> F + Send + 'static,
) -> stream::Boxed<EndpointId>
where
    F: Future<Output = bool> + Send + 'static,
{
    let mut seen = HashSet::new();
    providers
        .filter(move |(provider, _)| seen.insert(*provider))
        .map(move |(provider, provenance)| {
            let passed = probe(provider, provenance);
            async move { passed.await.then_some(provider) }
        })
        .buffered_unordered(CONCURRENT_PROBES)
        .filter_map(|provider| provider)
        .boxed()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use iroh::SecretKey;

    use super::*;

    fn provider() -> EndpointId {
        SecretKey::generate().public()
    }

    #[tokio::test(start_paused = true)]
    async fn the_fastest_passing_candidate_wins() {
        let (slow, fast, bad) = (provider(), provider(), provider());
        let candidates = stream::iter([slow, fast, bad]).map(|p| (p, Provenance::Discovered));
        let mut found = race_probes(candidates, move |candidate, _| async move {
            let delay = if candidate == slow { 900 } else { 100 };
            tokio::time::sleep(Duration::from_millis(delay)).await;
            candidate != bad
        });
        assert_eq!(found.next().await, Some(fast));
        assert_eq!(found.next().await, Some(slow));
        assert_eq!(found.next().await, None);
    }

    #[tokio::test]
    async fn repeated_candidates_are_probed_once() {
        let twice = provider();
        let probed = Arc::new(AtomicUsize::new(0));
        let verified = Provenance::Verified(Probed {
            at: Instant::now(),
            latency: Duration::from_millis(50),
        });
        let candidates = stream::iter([(twice, verified), (twice, Provenance::Discovered)]);
        let found: Vec<_> = race_probes(candidates, {
            let probed = probed.clone();
            move |_, _| {
                probed.fetch_add(1, Ordering::SeqCst);
                async { false }
            }
        })
        .collect()
        .await;
        assert!(found.is_empty());
        assert_eq!(probed.load(Ordering::SeqCst), 1);
    }
}
