//! Publish and resolve iroh endpoints through the Mainline DHT.
//!
//! Mainline maps an application-defined infohash to a compact IPv4 socket.
//! An iroh address-index replica then maps that socket to a signed endpoint
//! identity. The endpoint and DHT node are supplied by the caller and may be
//! shared with other protocols.

#![deny(missing_docs, rustdoc::broken_intra_doc_links)]

mod directory;
mod publisher;
mod record;
mod republisher;
mod resolver;
mod tracker_list;
mod udp;

pub use blake3::Hash;
pub use directory::{Directory, DirectoryError, DiscoveryConfig};
pub use publisher::{ANNOUNCE_SPACING, Publisher, REFRESH};
pub use record::{RecordPayload, RecordPayloadV1, SignedRecord};
pub use republisher::republish_tracker_list;
pub use resolver::Resolver;
pub use tracker_list::{TRACKER_LIST_SALT, TrackerList};
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
    let mut out = String::with_capacity(40);
    for byte in id {
        use std::fmt::Write;
        write!(out, "{byte:02x}").expect("writing to String cannot fail");
    }
    out
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
    let mut out = [0; 20];
    for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        out[index] = (from_hex(pair[0])? << 4) | from_hex(pair[1])?;
    }
    Ok(out)
}

fn from_hex(byte: u8) -> Result<u8, HashParseError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(HashParseError::InvalidHex),
    }
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
