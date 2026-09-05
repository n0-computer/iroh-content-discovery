//! Addr → [`EndpointId`] directory.
//!
//! Join Mainline `get_peers` contacts to iroh identities. Content discovery
//! stays on the DHT. This crate does **not** store content hashes. It answers:
//! given a compact `ip:port`, which [`EndpointId`]s currently claim that
//! mapping, listening with which ALPNs.
//!
//! Rows are signed by the eid secret, including [`Index`] values for what
//! replicas may index (this crate is reverse `addr` → eid). A signature does
//! not prove socket ownership — confirm with [`confirm_socket`] (offer the
//! announced ALPNs).
//!
//! Transport: single-MTU UDP. An externally supplied iroh endpoint can probe
//! contested mappings.

#![deny(missing_docs, rustdoc::broken_intra_doc_links)]

mod directory;
mod net;
mod record;
mod store;

pub mod udp;

pub use directory::{Directory, DirectoryError};
pub use net::{
    DEFAULT_PROBE_TIMEOUT, PROBE_ALPN, ProbeAccept, Server, confirm_records, confirm_socket,
};
pub use record::{
    Alpn, Index, MAX_ALPN_LEN, RecordPayload, RecordPayloadV1, SignedRecord, VerifyError,
};
pub use store::{Limits, PublishError, Store};
pub use udp::{MAX_DGRAM, ResolveResult, UdpClient, UdpError, UdpHandle};

pub use blake3::Hash;
pub use iroh::{EndpointAddr, EndpointId};

/// Mainline infohash for a BLAKE3 content hash: `SHA-1` of the [`Hash`].
pub fn infohash_from_blake3(hash: &Hash) -> [u8; 20] {
    sha1_smol::Sha1::from(hash.as_bytes().as_slice())
        .digest()
        .bytes()
}

/// Parse a 40-char infohash hex or a 64-char BLAKE3 hex (the latter is hashed).
pub fn parse_infohash(s: &str) -> Result<[u8; 20], HashParseError> {
    let s = s.trim();
    match s.len() {
        40 => decode_hex20(s),
        64 => {
            let hash = Hash::from_hex(s).map_err(|_| HashParseError::InvalidHex)?;
            Ok(infohash_from_blake3(&hash))
        }
        _ => Err(HashParseError::InvalidLength),
    }
}

/// Format 20-byte infohash as lowercase hex.
pub fn infohash_hex(id: &[u8; 20]) -> String {
    let mut s = String::with_capacity(40);
    for b in id {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Error parsing an infohash or BLAKE3 hex string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum HashParseError {
    /// Neither 40 nor 64 hex characters.
    InvalidLength,
    /// Non-hex character.
    InvalidHex,
}

impl std::fmt::Display for HashParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLength => {
                write!(f, "expected 40-char infohash hex or 64-char BLAKE3 hex")
            }
            Self::InvalidHex => write!(f, "invalid hex"),
        }
    }
}

impl std::error::Error for HashParseError {}

fn decode_hex20(s: &str) -> Result<[u8; 20], HashParseError> {
    let mut out = [0u8; 20];
    decode_hex_into(s, &mut out)?;
    Ok(out)
}

fn decode_hex_into(s: &str, out: &mut [u8]) -> Result<(), HashParseError> {
    if s.len() != out.len() * 2 {
        return Err(HashParseError::InvalidLength);
    }
    for (i, &[hi, lo]) in s.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        out[i] = (from_hex(hi)? << 4) | from_hex(lo)?;
    }
    Ok(())
}

fn from_hex(b: u8) -> Result<u8, HashParseError> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(HashParseError::InvalidHex),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_of_32_zeros() {
        let a = infohash_from_blake3(&Hash::from_bytes([0u8; 32]));
        assert_eq!(infohash_hex(&a), "de8a847bff8c343d69b853a215e6ee775ef2ef96");
        assert_eq!(
            parse_infohash("0000000000000000000000000000000000000000000000000000000000000000")
                .unwrap(),
            a
        );
        assert_eq!(
            parse_infohash("de8a847bff8c343d69b853a215e6ee775ef2ef96").unwrap(),
            a
        );
    }

    #[test]
    fn rejects_bad_len() {
        assert_eq!(parse_infohash("abcd"), Err(HashParseError::InvalidLength));
    }
}
