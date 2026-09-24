//! Publish and resolve iroh endpoints through the Mainline DHT.
//!
//! Mainline maps an application-defined infohash to a compact IPv4 socket.
//! An iroh address-index server then maps that socket to a signed endpoint
//! identity. The endpoint and DHT node are supplied by the caller and may be
//! shared with other protocols.

#![deny(missing_docs, rustdoc::broken_intra_doc_links)]

use data_encoding::{HEXLOWER, HEXLOWER_PERMISSIVE};

mod addr_index;
mod pkarr;
mod publisher;
mod record;
mod republisher;
mod resolver;
mod server_list;
mod udp;

pub use addr_index::{AddrIndex, AddrIndexError, DiscoveryConfig};
pub use blake3::Hash;
pub use pkarr::{
    BLAKE3_DOMAIN, PKARR_DOMAIN, PKARR_REFRESH, PkarrPublisher, is_hostname, pkarr_name,
};
pub use publisher::{ANNOUNCE_SPACING, Publisher, REFRESH, RETRY};
pub use record::{RecordPayload, RecordPayloadV1, SignedRecord};
pub use republisher::republish_server_list;
pub use resolver::Resolver;
pub use server_list::{SERVER_LIST_SALT, ServerList};
pub use udp::{DEFAULT_TIMEOUT, ResolveResult, UdpClient, UdpError};

/// Mainline infohash for a BLAKE3 hash: `SHA-1(blake3)`.
pub fn infohash_from_blake3(hash: &Hash) -> [u8; 20] {
    sha1_smol::Sha1::from(hash.as_bytes().as_slice())
        .digest()
        .bytes()
}

/// Parse a 40-character infohash or a 64-character BLAKE3 hash.
pub fn parse_infohash(value: &str) -> Result<[u8; 20], HashParseError> {
    let value = value.trim();
    match value.len() {
        40 => decode_hex20(value),
        64 => {
            let hash = Hash::from_hex(value).map_err(|_| HashParseError::InvalidHex)?;
            Ok(infohash_from_blake3(&hash))
        }
        _ => Err(HashParseError::InvalidLength),
    }
}

/// Format a 20-byte infohash as lowercase hexadecimal.
pub fn infohash_hex(id: &[u8; 20]) -> String {
    HEXLOWER.encode(id)
}

/// Error parsing an infohash or BLAKE3 hash.
#[n0_error::stack_error(derive)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum HashParseError {
    /// Neither 40 nor 64 hexadecimal characters were supplied.
    #[error("expected 40-char infohash hex or 64-char BLAKE3 hex")]
    InvalidLength,
    /// A non-hexadecimal character was supplied.
    #[error("invalid hex")]
    InvalidHex,
}

fn decode_hex20(value: &str) -> Result<[u8; 20], HashParseError> {
    HEXLOWER_PERMISSIVE
        .decode(value.as_bytes())
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(HashParseError::InvalidHex)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_of_32_zeros() {
        let hash = Hash::from_bytes([0; 32]);
        let infohash = infohash_from_blake3(&hash);
        assert_eq!(
            infohash_hex(&infohash),
            "de8a847bff8c343d69b853a215e6ee775ef2ef96"
        );
        assert_eq!(parse_infohash(&hash.to_hex()).unwrap(), infohash);
    }

    #[test]
    fn rejects_bad_length() {
        assert_eq!(parse_infohash("abcd"), Err(HashParseError::InvalidLength));
    }
}
