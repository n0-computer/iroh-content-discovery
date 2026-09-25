//! Curated address-index servers in signed Pkarr DNS packets.

use std::net::SocketAddrV4;

use n0_mainline::{MutableItem, SigningKey};
use simple_dns::{
    CLASS, Name, Packet, ResourceRecord,
    rdata::{RData, TXT},
};

/// A list of at most two address index server sockets.
///
/// Each socket is an apex IN TXT record containing `IPv4:port` in a Pkarr
/// packet. The apex is the signing key encoded as z-base-32. An empty TXT
/// record represents an empty list. Other record types and owners are ignored.
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

    /// Encodes apex TXT records as a Pkarr DNS packet for this public key.
    pub fn encode(&self, public_key: &[u8; 32]) -> Vec<u8> {
        let owner = crate::pkarr_name(public_key);
        let name = Name::new(&owner).expect("z-base-32 DNS name");
        let mut packet = Packet::new_reply(0);
        let values = if self.0.is_empty() {
            vec![String::new()]
        } else {
            self.0.iter().map(ToString::to_string).collect()
        };
        for value in &values {
            packet.answers.push(ResourceRecord::new(
                name.clone(),
                CLASS::IN,
                300,
                RData::TXT(TXT::try_from(value.as_str()).expect("short socket string")),
            ));
        }
        packet
            .build_bytes_vec_compressed()
            .expect("bounded DNS packet")
    }

    /// Decodes apex TXT sockets from a Pkarr packet signed by `public_key`.
    ///
    /// Signature verification is performed by the DHT client. Rejects malformed
    /// apex entries, invalid sockets, and lists exceeding two entries. A packet
    /// without apex TXT records yields an empty list, allowing rendezvous fallback.
    pub fn decode(value: &[u8], public_key: &[u8; 32]) -> Option<Self> {
        if value.len() > 1000 {
            return None;
        }
        let packet = Packet::parse(value).ok()?;
        let owner = crate::pkarr_name(public_key);
        let name = Name::new(&owner).ok()?;
        let mut addresses = Vec::new();
        for record in packet.answers {
            if record.class != CLASS::IN || record.name != name {
                continue;
            }
            let RData::TXT(txt) = record.rdata else {
                continue;
            };
            let text = String::try_from(txt).ok()?;
            if text.is_empty() {
                continue;
            }
            addresses.push(text.parse().ok()?);
        }
        Self::new(addresses).ok()
    }

    /// Signs a Pkarr packet for publication with [`n0_mainline::Dht::put_mutable`].
    ///
    /// Pkarr uses an unsalted BEP44 item and a microsecond Unix timestamp as its
    /// sequence. Increase it whenever the list changes and republish periodically.
    pub fn sign(&self, key: &SigningKey, sequence: i64) -> n0_error::Result<MutableItem> {
        n0_error::ensure_any!(sequence >= 0, "sequence must be nonnegative");
        Ok(MutableItem::new(
            key,
            &self.encode(key.verifying_key().as_bytes()),
            sequence,
            None,
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
    const KEY: [u8; 32] = [42; 32];

    #[test]
    fn pkarr_roundtrip_and_withdrawal() {
        for addresses in [
            vec![],
            vec![
                "203.0.113.1:11223".parse().unwrap(),
                "198.51.100.2:33445".parse().unwrap(),
            ],
        ] {
            let list = ServerList::new(addresses).unwrap();
            let bytes = list.encode(&KEY);
            assert!(bytes.len() <= 1000);
            assert_eq!(ServerList::decode(&bytes, &KEY), Some(list));
        }
        let key = SigningKey::from_bytes(&KEY);
        let list = ServerList::new(vec![]).unwrap();
        let item = list.sign(&key, 1234).unwrap();
        assert_eq!(item.salt(), None);
        assert_eq!(ServerList::decode(item.value(), item.key()), Some(list));
        assert!(ServerList::decode(&[1, 203, 0, 113, 1, 4, 210], &KEY).is_none());
    }

    fn packet(entries: &[(&str, &str)]) -> Vec<u8> {
        let mut packet = Packet::new_reply(0);
        for (owner, value) in entries {
            packet.answers.push(ResourceRecord::new(
                Name::new(owner).unwrap(),
                CLASS::IN,
                300,
                RData::TXT(TXT::try_from(*value).unwrap()),
            ));
        }
        packet.build_bytes_vec_compressed().unwrap()
    }

    #[test]
    fn only_apex_records_are_candidates() {
        let owner = crate::pkarr_name(&KEY);
        let subdomain = format!("_index.{owner}");
        let bytes = packet(&[
            (&subdomain, "203.0.113.2:22"),
            ("other", "not a socket"),
            (&owner, "203.0.113.1:11223"),
        ]);
        assert_eq!(
            ServerList::decode(&bytes, &KEY).unwrap().addresses(),
            &["203.0.113.1:11223".parse::<SocketAddrV4>().unwrap()]
        );
    }

    #[test]
    fn malformed_apex_records_are_rejected() {
        let owner = crate::pkarr_name(&KEY);
        for text in [
            "garbage",
            "[::1]:1234",
            "203.0.113.1:0",
            "0.0.0.0:1234",
            "224.0.0.1:1234",
            "255.255.255.255:1234",
        ] {
            assert!(ServerList::decode(&packet(&[(&owner, text)]), &KEY).is_none());
        }
        assert!(
            ServerList::decode(&packet(&[(owner.as_str(), "203.0.113.1:1234"); 3]), &KEY).is_none()
        );
    }
}
