//! Verified reverse index from public IPv4 sockets to iroh endpoint identities.
//!
//! Rows are signed by the endpoint and expire according to replica policy.
//! An externally supplied iroh endpoint can probe contested mappings.

#![deny(missing_docs, rustdoc::broken_intra_doc_links)]

mod net;
mod store;

pub mod udp;

pub use iroh_addr_index_proto::{
    Alpn, EndpointAddr, EndpointId, Index, MAX_ALPN_LEN, RecordLimits, RecordPayload,
    RecordPayloadV1, SignedRecord, VerifyError,
};
pub use net::{
    DEFAULT_PROBE_TIMEOUT, PROBE_ALPN, ProbeAccept, Server, confirm_records, confirm_socket,
};
pub use store::{Limits, PublishError, Store};
pub use udp::UdpHandle;
