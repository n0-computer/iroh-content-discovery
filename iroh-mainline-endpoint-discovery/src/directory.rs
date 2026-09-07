//! Convenience wrapper around the UDP directory client.

use std::net::{SocketAddr, SocketAddrV4};

use iroh_addr_index_proto::SignedRecord;

use crate::{UdpClient, UdpError};

/// UDP client for one or more directory replicas.
#[derive(Debug, Clone)]
pub struct Directory(UdpClient);

impl Directory {
    /// Bind an ephemeral UDP socket and add one replica.
    pub async fn udp(replica: SocketAddr) -> Result<Self, UdpError> {
        let client = UdpClient::bind().await?;
        client.add_replica(replica).await?;
        Ok(Self(client))
    }

    /// Wrap an existing UDP client.
    pub fn from_udp(client: UdpClient) -> Self {
        Self(client)
    }

    /// Publish `record` to all configured replicas.
    pub async fn publish(&self, record: SignedRecord) -> Result<(), DirectoryError> {
        self.0.publish(record).await.map_err(Into::into)
    }

    /// Lookup the endpoint that listed `addr`.
    pub async fn lookup(&self, addr: SocketAddrV4) -> Result<Vec<SignedRecord>, DirectoryError> {
        let res = self.0.resolve(addr).await?;
        if res.truncated {
            tracing::debug!(%addr, "udp lookup truncated");
        }
        Ok(res.records)
    }
}

impl From<UdpClient> for Directory {
    fn from(value: UdpClient) -> Self {
        Self(value)
    }
}

/// Error from [`Directory::publish`] or [`Directory::lookup`].
#[derive(Debug)]
pub struct DirectoryError(UdpError);

impl std::fmt::Display for DirectoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for DirectoryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl From<UdpError> for DirectoryError {
    fn from(value: UdpError) -> Self {
        Self(value)
    }
}
