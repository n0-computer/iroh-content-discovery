//! Iroh-specific signed value, stored opaquely by an address index server.

use std::{
    net::SocketAddrV4,
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
    /// Public socket an index server observed for the publisher.
    ///
    /// The slot a record is stored under is the same socket, so a reader can
    /// tell a record that was published here from one copied out of another
    /// slot. Without it a record is portable: reads are public, so anyone
    /// could store someone else's record in their own slot.
    pub addr: SocketAddrV4,
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

impl SignedRecord {
    /// Sign a record binding an endpoint identity to `addr`.
    ///
    /// `addr` is the public socket an index server observed, which is only
    /// known once that server has answered, so one record is signed per
    /// server. Behind a symmetric NAT they legitimately differ.
    pub fn sign(secret: &SecretKey, addr: SocketAddrV4) -> Self {
        let payload = RecordPayload::V1(RecordPayloadV1 {
            addr,
            ts: unix_secs(),
        });
        let sig = secret.sign(&postcard::to_stdvec(&payload).expect("record payload"));
        Self {
            endpoint_id: secret.public(),
            payload,
            sig,
        }
    }

    /// The socket this record was signed for.
    pub fn addr(&self) -> SocketAddrV4 {
        self.payload.v1().addr
    }

    /// Verify that `endpoint_id` signed this record's payload.
    ///
    /// A valid signature says nothing about where the record was found. Use
    /// [`Self::addr`] to check that it is the slot it was signed for.
    pub fn verify(&self) -> bool {
        let Ok(payload) = postcard::to_stdvec(&self.payload) else {
            return false;
        };
        self.endpoint_id.verify(&payload, &self.sig).is_ok()
    }

    /// Encodes the record as the opaque value an index server stores.
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("a signed record encodes")
    }

    /// Decodes a record that was read from the slot for `addr`.
    ///
    /// Returns `None` unless the signature verifies and the record was signed
    /// for that slot.
    pub fn decode(value: &[u8], addr: SocketAddrV4) -> Option<Self> {
        let record: Self = postcard::from_bytes(value).ok()?;
        (record.addr() == addr && record.verify()).then_some(record)
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

    const ADDR: &str = "203.0.113.7:6881";

    fn addr() -> SocketAddrV4 {
        ADDR.parse().unwrap()
    }

    #[test]
    fn signed_record_roundtrip() {
        let record = SignedRecord::sign(&SecretKey::generate(), addr());
        assert!(record.verify());
        assert_eq!(SignedRecord::decode(&record.encode(), addr()), Some(record));
    }

    #[test]
    fn another_endpoint_cannot_sign_for_it() {
        let mut record = SignedRecord::sign(&SecretKey::generate(), addr());
        record.endpoint_id = SecretKey::generate().public();
        assert!(!record.verify());
    }

    #[test]
    fn a_record_does_not_decode_under_another_socket() {
        let record = SignedRecord::sign(&SecretKey::generate(), addr());
        let value = record.encode();
        assert!(SignedRecord::decode(&value, "203.0.113.7:6882".parse().unwrap()).is_none());
        assert!(SignedRecord::decode(&value, "198.51.100.7:6881".parse().unwrap()).is_none());
    }
}
