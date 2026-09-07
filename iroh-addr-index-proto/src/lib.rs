//! Signed records and UDP wire types for the iroh address index.
//!
//! This crate contains no sockets, server state, or Mainline integration.

#![deny(missing_docs, rustdoc::broken_intra_doc_links)]

mod record;
mod wire;

pub use iroh_base::{EndpointAddr, EndpointId, SecretKey, Signature};
pub use record::{
    Alpn, Index, MAX_ALPN_LEN, RecordLimits, RecordPayload, RecordPayloadV1, SignedRecord,
    VerifyError,
};
pub use wire::{MAX_DGRAM, Request, RequestV1, Response, ResponseV1};
