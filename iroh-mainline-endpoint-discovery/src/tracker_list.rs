//! Signed BEP44 bootstrap lists for address-index replicas.

use std::net::{Ipv4Addr, SocketAddrV4};

use n0_mainline::{MutableItem, SigningKey};

/// BEP44 salt separating tracker lists from other records signed by the same key.
pub const TRACKER_LIST_SALT: &[u8] = b"iroh-addr-index replicas v1";

/// A versioned list of at most two tracker sockets.
///
/// The BEP44 byte-string value is one version byte (`1`) followed by six bytes
/// per address: four IPv4 octets and a big-endian UDP port. An empty list is valid.
/// At 13 bytes maximum, this fits comfortably within BEP44's value limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackerList(Vec<SocketAddrV4>);

impl TrackerList {
    /// Build a list, rejecting invalid sockets and more than two entries.
    pub fn new(addresses: Vec<SocketAddrV4>) -> anyhow::Result<Self> {
        anyhow::ensure!(addresses.len() <= 2, "at most two trackers are allowed");
        anyhow::ensure!(
            addresses.iter().all(valid_address),
            "invalid tracker socket"
        );
        let mut unique = Vec::new();
        for address in addresses {
            if !unique.contains(&address) {
                unique.push(address);
            }
        }
        Ok(Self(unique))
    }

    /// Tracker sockets endorsed by the signer.
    pub fn addresses(&self) -> &[SocketAddrV4] {
        &self.0
    }

    /// Encode the value to sign and store in BEP44.
    pub fn encode(&self) -> Vec<u8> {
        let mut value = vec![1];
        for addr in &self.0 {
            value.extend_from_slice(&addr.ip().octets());
            value.extend_from_slice(&addr.port().to_be_bytes());
        }
        value
    }

    /// Decode a list value. Signature verification is performed by the DHT client.
    pub fn decode(value: &[u8]) -> Option<Self> {
        let (&version, bytes) = value.split_first()?;
        if version != 1 || bytes.len() > 2 * 6 || bytes.len() % 6 != 0 {
            return None;
        }
        let addresses = bytes
            .as_chunks::<6>()
            .0
            .iter()
            .map(|b| {
                SocketAddrV4::new(
                    Ipv4Addr::new(b[0], b[1], b[2], b[3]),
                    u16::from_be_bytes([b[4], b[5]]),
                )
            })
            .collect();
        Self::new(addresses).ok()
    }

    /// Sign a list for publication with [`n0_mainline::Dht::put_mutable`].
    ///
    /// Increase the sequence number whenever the list changes. Republish the
    /// same item periodically to keep it available in the DHT.
    pub fn sign(&self, key: &SigningKey, sequence: i64) -> anyhow::Result<MutableItem> {
        anyhow::ensure!(sequence >= 0, "sequence must be nonnegative");
        Ok(MutableItem::new(
            key,
            &self.encode(),
            sequence,
            Some(TRACKER_LIST_SALT),
        ))
    }
}

fn valid_address(addr: &SocketAddrV4) -> bool {
    addr.port() != 0
        && !addr.ip().is_unspecified()
        && !addr.ip().is_multicast()
        && !addr.ip().is_broadcast()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding_is_bounded_and_strict() {
        let addr = "203.0.113.1:1234".parse().unwrap();
        let list = TrackerList::new(vec![addr, addr]).unwrap();
        assert_eq!(list.encode(), [1, 203, 0, 113, 1, 4, 210]);
        assert_eq!(TrackerList::decode(&list.encode()), Some(list));
        assert!(TrackerList::decode(&[1]).is_some());
        for bytes in [
            vec![],
            vec![2],
            vec![1, 1],
            vec![1; 199],
            vec![1, 0, 0, 0, 0, 0, 1],
        ] {
            assert!(TrackerList::decode(&bytes).is_none());
        }
        assert!(TrackerList::new(vec![addr; 3]).is_err());
        assert!(TrackerList::new(vec!["203.0.113.1:0".parse().unwrap()]).is_err());
    }
}
