//! Signed announcement: this eid is listening on these `host:port`s using these ALPNs.

use std::{
    net::SocketAddrV4,
    ops::Deref,
    time::{SystemTime, UNIX_EPOCH},
};

use iroh::{EndpointAddr, EndpointId, SecretKey, Signature};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::store::Limits;

/// TLS ALPN protocol names are 1..=255 octets.
pub const MAX_ALPN_LEN: usize = 255;

/// ALPN bytes, stored inline up to the common 32-byte size.
pub type Alpn = SmallVec<[u8; 32]>;

/// What a replica may index from this announcement.
///
/// This crate's store is reverse (`addr` → eid) and only accepts rows that
/// list [`Index::Reverse`]. The other variants are for other indexers; they
/// are signed here so the publisher can consent without a second packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Index {
    /// EndpointId → addrs.
    Forward,
    /// addrs → EndpointId.
    Reverse,
    /// ALPN → eids that announced that ALPN.
    AlpnPeers,
}

/// Versioned announcement payload covered by a [`SignedRecord`]'s signature.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum RecordPayload {
    /// Initial payload format.
    V1(RecordPayloadV1),
}

impl RecordPayload {
    /// Access the V1 payload.
    pub fn v1(&self) -> &RecordPayloadV1 {
        match self {
            Self::V1(payload) => payload,
        }
    }

    /// Mutably access the V1 payload.
    pub fn v1_mut(&mut self) -> &mut RecordPayloadV1 {
        match self {
            Self::V1(payload) => payload,
        }
    }
}

/// Initial signed announcement payload.
///
/// `addrs` must include at least the Mainline-published IPv4 `ip:port`.
/// At least one ALPN must be listed; the protocol names are application-defined.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordPayloadV1 {
    /// Direct sockets, including the DHT-seen compact mapping.
    pub addrs: SmallVec<[SocketAddrV4; 1]>,
    /// ALPNs currently accepted on `addrs` (at least one).
    pub alpns: SmallVec<[Alpn; 1]>,
    /// Index kinds this publisher allows replicas to build from the row.
    pub index: SmallVec<[Index; 3]>,
    /// Unix seconds when signed.
    pub ts: u64,
}

/// Signed mapping from an endpoint's addresses to its [`EndpointId`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedRecord {
    /// Endpoint whose public key verifies `sig`.
    pub eid: EndpointId,
    /// Announcement fields covered by `sig`.
    pub payload: RecordPayload,
    /// Ed25519 signature over the publish payload.
    pub sig: Signature,
}

impl Deref for SignedRecord {
    type Target = RecordPayloadV1;

    fn deref(&self) -> &Self::Target {
        self.payload.v1()
    }
}

/// Signature or field-level rejection of a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum VerifyError {
    /// Signature does not match `eid`.
    BadSignature,
    /// No addresses listed.
    NoAddrs,
    /// Too many addresses.
    TooManyAddrs,
    /// Unspecified, multicast, broadcast, or port 0.
    InvalidAddr,
    /// No ALPNs listed.
    NoAlpns,
    /// Too many ALPNs.
    TooManyAlpns,
    /// No index kinds permitted.
    NoIndex,
    /// Empty ALPN or longer than [`MAX_ALPN_LEN`].
    InvalidAlpn,
    /// `ts` is too far in the future.
    NotYetValid,
}

impl std::fmt::Display for VerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadSignature => write!(f, "bad signature"),
            Self::NoAddrs => write!(f, "record lists no addresses"),
            Self::TooManyAddrs => write!(f, "too many addresses"),
            Self::InvalidAddr => write!(f, "invalid socket address"),
            Self::NoAlpns => write!(f, "record lists no ALPNs"),
            Self::TooManyAlpns => write!(f, "too many ALPNs"),
            Self::NoIndex => write!(f, "record permits no indexes"),
            Self::InvalidAlpn => write!(f, "invalid ALPN"),
            Self::NotYetValid => write!(f, "record timestamp is in the future"),
        }
    }
}

impl std::error::Error for VerifyError {}

impl SignedRecord {
    /// Sign a record at the current time.
    pub fn sign(
        secret: &SecretKey,
        addrs: impl Into<Vec<SocketAddrV4>>,
        alpns: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> Self {
        let ts = unix_secs();
        Self::sign_at(secret, addrs, alpns, [Index::Reverse], ts)
    }

    /// [`Self::sign`] with an explicit index list.
    pub fn sign_index(
        secret: &SecretKey,
        addrs: impl Into<Vec<SocketAddrV4>>,
        alpns: impl IntoIterator<Item = impl AsRef<[u8]>>,
        index: impl IntoIterator<Item = Index>,
    ) -> Self {
        let ts = unix_secs();
        Self::sign_at(secret, addrs, alpns, index, ts)
    }

    /// Sign a record with explicit timestamps (for tests).
    pub fn sign_at(
        secret: &SecretKey,
        addrs: impl Into<Vec<SocketAddrV4>>,
        alpns: impl IntoIterator<Item = impl AsRef<[u8]>>,
        index: impl IntoIterator<Item = Index>,
        ts: u64,
    ) -> Self {
        let eid = secret.public();
        let addrs = normalize_addrs(addrs.into());
        let alpns = normalize_alpns(alpns);
        let index = normalize_index(index);
        let payload = RecordPayload::V1(RecordPayloadV1 {
            addrs,
            alpns,
            index,
            ts,
        });
        let sig = secret.sign(&payload_bytes(&payload));
        Self { eid, payload, sig }
    }

    /// Verify the signature and basic field shape.
    pub fn verify_sig(&self) -> Result<(), VerifyError> {
        if self.addrs.is_empty() {
            return Err(VerifyError::NoAddrs);
        }
        if self.alpns.is_empty() {
            return Err(VerifyError::NoAlpns);
        }
        if self.index.is_empty() {
            return Err(VerifyError::NoIndex);
        }
        if self.addrs.iter().any(|a| !is_usable_addr(*a)) {
            return Err(VerifyError::InvalidAddr);
        }
        if self.alpns.iter().any(|a| !is_usable_alpn(a)) {
            return Err(VerifyError::InvalidAlpn);
        }
        let payload = payload_bytes(&self.payload);
        self.eid
            .verify(&payload, &self.sig)
            .map_err(|_| VerifyError::BadSignature)
    }

    /// [`Self::verify_sig`] plus timestamp and replica-cap checks.
    pub fn verify_at(&self, now: u64, limits: &Limits) -> Result<(), VerifyError> {
        self.verify_sig()?;
        if self.addrs.len() > limits.max_addrs {
            return Err(VerifyError::TooManyAddrs);
        }
        if self.alpns.len() > limits.max_alpns {
            return Err(VerifyError::TooManyAlpns);
        }
        if self.ts > now.saturating_add(limits.clock_skew_secs) {
            return Err(VerifyError::NotYetValid);
        }
        Ok(())
    }

    /// Whether this record lists `addr` (canonical form).
    pub fn covers_addr(&self, addr: SocketAddrV4) -> bool {
        self.addrs.contains(&addr)
    }

    /// Addressing info a finder can pass to [`iroh::Endpoint::connect`].
    pub fn endpoint_addr(&self) -> EndpointAddr {
        let mut addr = EndpointAddr::new(self.eid);
        for sa in &self.addrs {
            addr = addr.with_ip_addr((*sa).into());
        }
        addr
    }
}

fn is_usable_addr(addr: SocketAddrV4) -> bool {
    if addr.port() == 0 {
        return false;
    }
    let ip = addr.ip();
    !(ip.is_unspecified() || ip.is_multicast() || ip.is_broadcast())
}

fn normalize_addrs(addrs: impl IntoIterator<Item = SocketAddrV4>) -> SmallVec<[SocketAddrV4; 1]> {
    let mut out = SmallVec::new();
    for addr in addrs {
        if !is_usable_addr(addr) {
            continue;
        }
        if !out.contains(&addr) {
            out.push(addr);
        }
    }
    out
}

fn normalize_index(index: impl IntoIterator<Item = Index>) -> SmallVec<[Index; 3]> {
    let mut out = SmallVec::new();
    for kind in index {
        if !out.contains(&kind) {
            out.push(kind);
        }
    }
    out
}

fn normalize_alpns(alpns: impl IntoIterator<Item = impl AsRef<[u8]>>) -> SmallVec<[Alpn; 1]> {
    let mut out = SmallVec::new();
    for alpn in alpns {
        let alpn = Alpn::from_slice(alpn.as_ref());
        if !out.contains(&alpn) {
            out.push(alpn);
        }
    }
    out
}

fn is_usable_alpn(alpn: &[u8]) -> bool {
    !alpn.is_empty() && alpn.len() <= MAX_ALPN_LEN
}

fn payload_bytes(payload: &RecordPayload) -> Vec<u8> {
    postcard::to_stdvec(payload).expect("publish payload")
}

pub(crate) fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_ALPN: &[u8] = b"test/0";

    fn sign(sk: &SecretKey, addr: SocketAddrV4) -> SignedRecord {
        SignedRecord::sign(sk, vec![addr], [TEST_ALPN])
    }

    #[test]
    fn sign_verify_roundtrip() {
        let sk = SecretKey::generate();
        let addr: SocketAddrV4 = "1.2.3.4:6881".parse().unwrap();
        let rec = sign(&sk, addr);
        rec.verify_sig().unwrap();
        rec.verify_at(rec.ts, &Limits::default()).unwrap();
        assert!(rec.covers_addr(addr));
        assert_eq!(rec.eid, sk.public());
        assert_eq!(rec.alpns[0].as_slice(), TEST_ALPN);
        assert_eq!(rec.index.as_slice(), [Index::Reverse]);
    }

    #[test]
    fn rejects_empty_index() {
        let rec = SignedRecord::sign_index(
            &SecretKey::generate(),
            vec!["1.2.3.4:6881".parse().unwrap()],
            [TEST_ALPN],
            [],
        );
        assert_eq!(rec.verify_sig(), Err(VerifyError::NoIndex));
    }

    #[test]
    fn rejects_tampered_index() {
        let mut rec = sign(&SecretKey::generate(), "1.2.3.4:6881".parse().unwrap());
        rec.payload.v1_mut().index.push(Index::Forward);
        assert_eq!(rec.verify_sig(), Err(VerifyError::BadSignature));
    }

    #[test]
    fn rejects_unspecified_and_zero_port() {
        assert!(!is_usable_addr("0.0.0.0:80".parse().unwrap()));
        assert!(!is_usable_addr("1.2.3.4:0".parse().unwrap()));
        assert!(is_usable_addr("1.2.3.4:80".parse().unwrap()));
    }

    #[test]
    fn sign_drops_unspecified_addrs() {
        let rec = SignedRecord::sign(
            &SecretKey::generate(),
            vec![
                "0.0.0.0:56354".parse().unwrap(),
                "86.123.229.92:56354".parse().unwrap(),
            ],
            [TEST_ALPN],
        );
        rec.verify_sig().unwrap();
        assert_eq!(
            rec.addrs.as_slice(),
            ["86.123.229.92:56354".parse().unwrap()]
        );
    }

    #[test]
    fn rejects_tampered_addr_or_alpn() {
        let sk = SecretKey::generate();
        let mut rec = sign(&sk, "1.2.3.4:6881".parse().unwrap());
        rec.payload.v1_mut().addrs[0] = "8.8.8.8:80".parse().unwrap();
        assert_eq!(rec.verify_sig(), Err(VerifyError::BadSignature));
        let mut rec = sign(&sk, "1.2.3.4:6881".parse().unwrap());
        rec.payload.v1_mut().alpns[0] = Alpn::from_slice(b"other/1");
        assert_eq!(rec.verify_sig(), Err(VerifyError::BadSignature));
    }

    #[test]
    fn rejects_empty_alpns() {
        let rec = SignedRecord::sign(
            &SecretKey::generate(),
            vec!["1.2.3.4:6881".parse().unwrap()],
            std::iter::empty::<&[u8]>(),
        );
        assert_eq!(rec.verify_sig(), Err(VerifyError::NoAlpns));
    }

    #[test]
    fn rejects_empty_alpn_name() {
        let rec = SignedRecord::sign(
            &SecretKey::generate(),
            vec!["1.2.3.4:6881".parse().unwrap()],
            [b"".as_slice()],
        );
        assert_eq!(rec.verify_sig(), Err(VerifyError::InvalidAlpn));
    }

    #[test]
    fn rejects_too_many_alpns() {
        let alpns: Vec<Vec<u8>> = (0..3).map(|i| format!("p/{i}").into_bytes()).collect();
        let rec = SignedRecord::sign(
            &SecretKey::generate(),
            vec!["1.2.3.4:6881".parse().unwrap()],
            alpns,
        );
        rec.verify_sig().unwrap();
        let limits = Limits {
            max_alpns: 2,
            ..Limits::for_tests()
        };
        assert_eq!(
            rec.verify_at(rec.ts, &limits),
            Err(VerifyError::TooManyAlpns)
        );
    }

    #[test]
    fn rejects_foreign_key() {
        let rec = sign(&SecretKey::generate(), "1.2.3.4:6881".parse().unwrap());
        let mut stolen = rec.clone();
        stolen.eid = SecretKey::generate().public();
        assert_eq!(stolen.verify_sig(), Err(VerifyError::BadSignature));
    }
}
