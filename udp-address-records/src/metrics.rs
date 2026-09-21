//! Prometheus metrics for the address-index replica.

use iroh_metrics::{Counter, Gauge, MetricsGroup};

/// Request and storage metrics for one replica.
#[derive(Debug, Default, MetricsGroup)]
#[metrics(name = "addr_index")]
pub struct Metrics {
    /// Valid prepare requests.
    pub prepares: Counter,
    /// Accepted put requests.
    pub puts: Counter,
    /// Get requests.
    pub gets: Counter,
    /// Get requests with a value.
    pub get_hits: Counter,
    /// Rejected put requests with invalid tokens.
    pub invalid_tokens: Counter,
    /// Requests rejected by rate limiting.
    pub rate_limited: Counter,
    /// Put requests rejected due to size or capacity limits.
    pub rejected_puts: Counter,
    /// Index datagrams dropped because the request queue was full.
    pub queue_drops: Counter,
    /// Currently retained entries, including entries pending lazy collection.
    pub entries: Gauge,
}
