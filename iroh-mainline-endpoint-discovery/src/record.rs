//! Iroh-specific signed value, stored opaquely by an address index server.

use std::{
    ops::Deref,
    time::{SystemTime, UNIX_EPOCH},
};

use iroh_base::{EndpointId, SecretKey, Signature};
use serde::{Deserialize, Serialize};

/// Versioned fields covered by an endpoint record's signature.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum RecordPayload {
    /// Initial payload format.
    V1(RecordPayloadV1),
}

impl RecordPayload {
    /// Access the initial payload.
    pub fn v1(&self) -> &RecordPayloadV1 {
        match self {
            Self::V1(payload) => payload,
        }
    }
}

/// Initial signed endpoint-record payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordPayloadV1 {
    /// Unix seconds when the value was signed.
    pub ts: u64,
}

/// Signed assertion that the publisher controls an iroh endpoint identity.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedRecord {
    /// Endpoint identity whose key verifies `sig`.
    pub endpoint_id: EndpointId,
    /// Versioned fields covered by `sig`.
    pub payload: RecordPayload,
    /// Signature over the postcard-encoded payload.
    pub sig: Signature,
}

impl Deref for SignedRecord {
    type Target = RecordPayloadV1;

    fn deref(&self) -> &Self::Target {
        self.payload.v1()
    }
}

impl SignedRecord {
    /// Sign a new endpoint record at the current time.
    pub fn sign(secret: &SecretKey) -> Self {
        let payload = RecordPayload::V1(RecordPayloadV1 { ts: unix_secs() });
        let sig = secret.sign(&postcard::to_stdvec(&payload).expect("record payload"));
        Self {
            endpoint_id: secret.public(),
            payload,
            sig,
        }
    }

    /// Verify that `endpoint_id` signed this record's payload.
    pub fn verify(&self) -> bool {
        let Ok(payload) = postcard::to_stdvec(&self.payload) else {
            return false;
        };
        self.endpoint_id.verify(&payload, &self.sig).is_ok()
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_stdvec(self)
    }

    pub(crate) fn decode(value: &[u8]) -> Option<Self> {
        let record: Self = postcard::from_bytes(value).ok()?;
        record.verify().then_some(record)
    }
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_record_roundtrip() {
        let record = SignedRecord::sign(&SecretKey::generate());
        assert!(record.verify());
        assert_eq!(
            SignedRecord::decode(&record.encode().unwrap()),
            Some(record)
        );
    }

    #[test]
    fn another_endpoint_cannot_sign_for_it() {
        let mut record = SignedRecord::sign(&SecretKey::generate());
        record.endpoint_id = SecretKey::generate().public();
        assert!(!record.verify());
    }
}
