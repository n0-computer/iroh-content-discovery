//! Optional content validation between provider discovery and downloading.

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use iroh::{Endpoint, EndpointId};
use iroh_blobs::Hash;
use n0_future::{BufferedStreamExt, Stream, StreamExt, stream};

const CONCURRENT_PROBES: usize = 3;
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Filter a provider stream to endpoints that serve a verified size for `hash`.
///
/// This optional stage is independent of discovery. Each distinct endpoint is
/// connected to and queried for the blob's last Bao chunk, validating the size
/// against the requested hash rather than trusting the response's size header.
/// Up to three probes run concurrently, each with a ten-second total deadline.
/// Failed probes are logged and skipped; endpoints are yielded in completion
/// order. Dropping the stream cancels pending probes.
///
/// Only endpoint IDs are returned. The consumer connects again for downloading;
/// successful validation does not guarantee that this later transfer succeeds.
/// The caller should impose an overall deadline on discovery and filtering.
pub fn filter_verified_providers(
    endpoint: Endpoint,
    hash: Hash,
    providers: impl Stream<Item = EndpointId> + Send + 'static,
) -> stream::Boxed<EndpointId> {
    let mut seen = HashSet::new();
    providers
        .filter(move |provider| seen.insert(*provider))
        .map(move |provider| {
            let endpoint = endpoint.clone();
            async move {
                let started = Instant::now();
                tracing::debug!(%hash, %provider, "probing provider with verified size request");
                let probe = async {
                    let connection = endpoint.connect(provider, iroh_blobs::ALPN).await?;
                    crate::verified_size(&connection, hash).await
                };
                match tokio::time::timeout(PROBE_TIMEOUT, probe).await {
                    Ok(Ok(size)) => {
                        tracing::debug!(%hash, %provider, size, elapsed_ms = started.elapsed().as_millis(), "provider size validated");
                        Some(provider)
                    }
                    Ok(Err(error)) => {
                        tracing::debug!(%hash, %provider, ?error, elapsed_ms = started.elapsed().as_millis(), "provider probe failed; skipping");
                        None
                    }
                    Err(_) => {
                        tracing::debug!(%hash, %provider, elapsed_ms = started.elapsed().as_millis(), "provider probe timed out; skipping");
                        None
                    }
                }
            }
        })
        .buffered_unordered(CONCURRENT_PROBES)
        .filter_map(|provider| provider)
        .boxed()
}
