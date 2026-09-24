//! Renewal of a signed server list, independently of the servers it names.

use std::{future::Future, time::Duration};

use n0_error::Result;
use n0_mainline::{
    Dht, MutableItem,
    errors::{PutMutableError, PutQueryError},
};

use crate::{SERVER_LIST_SALT, ServerList};
use tracing::{info, warn};

const RENEW: Duration = Duration::from_secs(600);
const RETRY: Duration = Duration::from_secs(30);
const TIMEOUT: Duration = Duration::from_secs(30);

/// Publish immediately and renew a signed server list every ten minutes.
///
/// This future owns no signing key. Run it as a separate task and cancel it to
/// stop renewal. Transient failures and thirty-second timeouts retry after thirty
/// seconds. DHT shutdown and sequence conflicts return an error; supply a newly
/// signed item with an increased sequence when changing the list.
/// The item must have been signed for [`SERVER_LIST_SALT`].
pub async fn republish_server_list(dht: Dht, item: MutableItem) -> Result<()> {
    n0_error::ensure_any!(
        item.salt() == Some(SERVER_LIST_SALT),
        "incorrect index-list salt"
    );
    n0_error::ensure_any!(
        item.seq() >= 0 && ServerList::decode(item.value()).is_some(),
        "invalid server list"
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
                info!("published signed server list");
                RENEW
            }
            Ok(Err(err @ PutMutableError::Concurrency(_)))
            | Ok(Err(err @ PutMutableError::Query(PutQueryError::Shutdown))) => {
                return Err(err.into());
            }
            Ok(Err(err)) => {
                warn!(%err, "index-list publication failed; retrying in thirty seconds");
                RETRY
            }
            Err(_) => {
                warn!("index-list publication timed out; retrying in thirty seconds");
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
