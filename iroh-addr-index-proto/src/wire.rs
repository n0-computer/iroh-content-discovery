//! Versioned postcard messages carried in one UDP datagram.

use std::net::SocketAddrV4;

use serde::{Deserialize, Serialize};

use crate::SignedRecord;

/// Maximum UDP payload, chosen to avoid IP fragmentation.
pub const MAX_DGRAM: usize = 1200;

/// Versioned postcard request. Unknown versions are dropped by receivers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Current request body.
    V1(RequestV1),
}

/// Version-one request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum RequestV1 {
    /// Upsert this endpoint's signed row. No datagram reply is sent.
    Publish(SignedRecord),
    /// Look up the endpoint that listed this exact IPv4 socket.
    Resolve(SocketAddrV4),
}

/// Versioned postcard response. Unknown versions are dropped by receivers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    /// Current response body.
    V1(ResponseV1),
}

/// Version-one response body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponseV1 {
    /// Result of an address lookup.
    Resolve {
        /// The queried compact mapping, echoed to demultiplex replies.
        addr: SocketAddrV4,
        /// Live signed rows.
        hosts: Vec<SignedRecord>,
        /// Whether additional rows did not fit in [`MAX_DGRAM`].
        truncated: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SecretKey;

    #[test]
    fn publish_request_fits() {
        let rec = SignedRecord::sign(
            &SecretKey::generate(),
            vec!["203.0.113.9:6881".parse().unwrap()],
            [b"test/0"],
        );
        let req = Request::V1(RequestV1::Publish(rec.clone()));
        let bytes = postcard::to_stdvec(&req).unwrap();
        assert!(bytes.len() <= MAX_DGRAM);
        match postcard::from_bytes::<Request>(&bytes).unwrap() {
            Request::V1(RequestV1::Publish(got)) => assert_eq!(got.eid, rec.eid),
            _ => panic!("wrong message"),
        }
    }

    #[test]
    fn garbage_is_rejected() {
        assert!(postcard::from_bytes::<Request>(b"xxxx").is_err());
        assert!(postcard::from_bytes::<Request>(&[]).is_err());
    }
}
