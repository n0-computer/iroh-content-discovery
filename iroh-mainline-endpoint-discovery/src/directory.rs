//! Convenience wrapper around the UDP directory client.

use std::net::SocketAddrV4;

use n0_mainline::Dht;

use crate::{SignedRecord, UdpClient, UdpError};

/// UDP client for one or more directory replicas.
#[derive(Debug, Clone)]
pub struct Directory(UdpClient);

impl Directory {
    /// Attach to a Mainline node's UDP socket and add one replica.
    pub async fn udp(dht: Dht, replica: SocketAddrV4) -> Result<Self, UdpError> {
        let client = UdpClient::attach(dht).await?;
        client.add_replica(replica).await?;
        Ok(Self(client))
    }

    /// Wrap an existing UDP client.
    pub fn from_udp(client: UdpClient) -> Self {
        Self(client)
    }

    /// Publish a signed endpoint record to all responsive replicas.
    ///
    /// Returns the public UDP sockets under which replicas stored it.
    pub async fn publish(
        &self,
        record: &SignedRecord,
    ) -> Result<Vec<SocketAddrV4>, DirectoryError> {
        let value = record.encode().map_err(|_| DirectoryError::Encoding)?;
        self.0.publish(value).await.map_err(Into::into)
    }

    /// Lookup the endpoint that listed `addr`.
    pub async fn lookup(&self, addr: SocketAddrV4) -> Result<Vec<SignedRecord>, DirectoryError> {
        let result = self.0.resolve(addr).await?;
        Ok(result
            .values
            .into_iter()
            .filter_map(|value| SignedRecord::decode(&value))
            .collect())
    }
}

impl From<UdpClient> for Directory {
    fn from(value: UdpClient) -> Self {
        Self(value)
    }
}

/// Error from [`Directory::publish`] or [`Directory::lookup`].
#[derive(Debug)]
pub enum DirectoryError {
    /// UDP transport failed.
    Udp(UdpError),
    /// The signed record could not be encoded.
    Encoding,
}

impl std::fmt::Display for DirectoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Udp(err) => err.fmt(f),
            Self::Encoding => write!(f, "could not encode signed endpoint record"),
        }
    }
}

impl std::error::Error for DirectoryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Udp(err) => Some(err),
            Self::Encoding => None,
        }
    }
}

impl From<UdpError> for DirectoryError {
    fn from(value: UdpError) -> Self {
        Self::Udp(value)
    }
}
