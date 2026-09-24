//! Signed BEP44 bootstrap lists for address-index servers.

use std::net::{Ipv4Addr, SocketAddrV4};

use n0_mainline::{MutableItem, SigningKey};

/// BEP44 salt separating server lists from other records signed by the same key.
pub const SERVER_LIST_SALT: &[u8] = b"iroh-addr-index servers v1";

/// A versioned list of at most two address index server sockets.
///
/// The BEP44 byte-string value is one version byte (`1`) followed by six bytes
/// per address: four IPv4 octets and a big-endian UDP port. An empty list is valid.
/// At 13 bytes, the longest value is far below BEP44's 1000-byte limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerList(Vec<SocketAddrV4>);

impl ServerList {
    /// Builds a list, rejecting invalid sockets and more than two entries.
    pub fn new(addresses: Vec<SocketAddrV4>) -> n0_error::Result<Self> {
        n0_error::ensure_any!(
            addresses.len() <= 2,
            "at most two index servers are allowed"
        );
        n0_error::ensure_any!(
            addresses.iter().all(valid_address),
            "invalid index server socket"
        );
        let mut unique = Vec::new();
        for address in addresses {
            if !unique.contains(&address) {
                unique.push(address);
            }
        }
        Ok(Self(unique))
    }

    /// Returns the server sockets endorsed by the signer.
    pub fn addresses(&self) -> &[SocketAddrV4] {
        &self.0
    }

    /// Encodes the value to sign and store in BEP44.
    pub fn encode(&self) -> Vec<u8> {
        let mut value = vec![1];
        for addr in &self.0 {
            value.extend_from_slice(&addr.ip().octets());
            value.extend_from_slice(&addr.port().to_be_bytes());
        }
        value
    }

    /// Decodes a list value.
    ///
    /// Signature verification is performed by the DHT client.
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

    /// Signs a list for publication with [`n0_mainline::Dht::put_mutable`].
    ///
    /// Increase the sequence number whenever the list changes. Republish the
    /// same item periodically to keep it available in the DHT.
    pub fn sign(&self, key: &SigningKey, sequence: i64) -> n0_error::Result<MutableItem> {
        n0_error::ensure_any!(sequence >= 0, "sequence must be nonnegative");
        Ok(MutableItem::new(
            key,
            &self.encode(),
            sequence,
            Some(SERVER_LIST_SALT),
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
        let list = ServerList::new(vec![addr, addr]).unwrap();
        assert_eq!(list.encode(), [1, 203, 0, 113, 1, 4, 210]);
        assert_eq!(ServerList::decode(&list.encode()), Some(list));
        assert!(ServerList::decode(&[1]).is_some());
        for bytes in [
            vec![],
            vec![2],
            vec![1, 1],
            vec![1; 199],
            vec![1, 0, 0, 0, 0, 0, 1],
        ] {
            assert!(ServerList::decode(&bytes).is_none());
        }
        assert!(ServerList::new(vec![addr; 3]).is_err());
        assert!(ServerList::new(vec!["203.0.113.1:0".parse().unwrap()]).is_err());
    }
}
