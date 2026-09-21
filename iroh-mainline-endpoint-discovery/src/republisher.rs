//! Renewal of a signed tracker list, independent of individual tracker servers.

use std::{future::Future, time::Duration};

use anyhow::Result;
use n0_mainline::{
    Dht, MutableItem,
    errors::{PutMutableError, PutQueryError},
};

use crate::{TRACKER_LIST_SALT, TrackerList};

const RENEW: Duration = Duration::from_secs(600);
const RETRY: Duration = Duration::from_secs(30);
const TIMEOUT: Duration = Duration::from_secs(30);

/// Publish immediately and renew a signed tracker list every ten minutes.
///
/// This future owns no signing key. Run it as a separate task and cancel it to
/// stop renewal. Transient failures and thirty-second timeouts retry after thirty
/// seconds. DHT shutdown and sequence conflicts return an error; supply a newly
/// signed item with an increased sequence when changing the list.
/// The item must have been signed for [`TRACKER_LIST_SALT`].
pub async fn republish_tracker_list(dht: Dht, item: MutableItem) -> Result<()> {
    anyhow::ensure!(
        item.salt() == Some(TRACKER_LIST_SALT),
        "incorrect tracker-list salt"
    );
    anyhow::ensure!(
        item.seq() >= 0 && TrackerList::decode(item.value()).is_some(),
        "invalid tracker list"
    );
    renew(|| async { dht.put_mutable(item.clone(), None).await.map(|_| ()) }).await
}

async fn renew<F, Fut>(mut publish: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<(), PutMutableError>>,
{
    loop {
        let delay = match tokio::time::timeout(TIMEOUT, publish()).await {
            Ok(Ok(())) => {
                tracing::info!("published signed tracker list");
                RENEW
            }
            Ok(Err(err @ PutMutableError::Concurrency(_)))
            | Ok(Err(err @ PutMutableError::Query(PutQueryError::Shutdown))) => {
                return Err(err.into());
            }
            Ok(Err(err)) => {
                tracing::warn!(%err, "tracker-list publication failed; retrying in thirty seconds");
                RETRY
            }
            Err(_) => {
                tracing::warn!("tracker-list publication timed out; retrying in thirty seconds");
                RETRY
            }
        };
        tokio::time::sleep(delay).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use n0_mainline::errors::ConcurrencyError;

    #[tokio::test(start_paused = true)]
    async fn renews_retries_and_stops_on_conflict() {
        let start = tokio::time::Instant::now();
        let mut calls = Vec::new();
        let result = renew(|| {
            calls.push(start.elapsed());
            let result = match calls.len() {
                1 => Ok(()),
                2 => Err(PutMutableError::Query(PutQueryError::NoClosestNodes)),
                _ => Err(PutMutableError::Concurrency(
                    ConcurrencyError::NotMostRecent,
                )),
            };
            std::future::ready(result)
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls, [Duration::ZERO, RENEW, RENEW + RETRY]);
    }

    #[tokio::test(start_paused = true)]
    async fn times_out_then_stops_on_shutdown() {
        let start = tokio::time::Instant::now();
        let mut calls = Vec::new();
        let result = renew(|| {
            calls.push(start.elapsed());
            let first = calls.len() == 1;
            async move {
                if first {
                    std::future::pending::<()>().await;
                }
                Err(PutMutableError::Query(PutQueryError::Shutdown))
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls, [Duration::ZERO, TIMEOUT + RETRY]);
    }
}
