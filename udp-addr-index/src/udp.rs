//! Address-index service sharing a Mainline node's UDP socket.

use crate::{Server, unix_secs};
use anyhow::{Context, Result, bail};
use n0_mainline::{ActorShutdown, Dht, Id};
use std::{
    net::{SocketAddr, SocketAddrV4},
    time::Duration,
};
use tokio::{
    sync::{mpsc, mpsc::error::TrySendError},
    task::JoinHandle,
};
use tracing::{debug, trace};
use udp_addr_index_proto::{
    MAGIC, MAX_DGRAM, Proto, RENDEZVOUS_INFOHASH, Request, RequestV1, Response, ResponseV1,
};

/// A running address-index service.
///
/// Drop it to detach from the shared DHT socket.
#[derive(Debug)]
pub struct UdpHandle {
    local_addr: SocketAddr,
    task: JoinHandle<Result<(), ActorShutdown>>,
    announcement: Option<JoinHandle<()>>,
}

impl UdpHandle {
    /// Returns the local DHT socket address, whose bind IP may be unspecified.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops serving and renewing server announcements.
    pub fn abort(&self) {
        self.task.abort();
        if let Some(announcement) = &self.announcement {
            announcement.abort();
        }
    }

    /// Waits for either owned task to stop, reporting an unexpected exit.
    pub async fn terminated(&mut self) -> Result<()> {
        tokio::select! {
            result = &mut self.task => {
                result.context("address-index service task failed")?
                    .context("address-index transport stopped")?;
                bail!("address-index service task stopped unexpectedly")
            }
            result = async {
                match &mut self.announcement {
                    Some(announcement) => announcement.await,
                    None => std::future::pending().await,
                }
            } => {
                result.context("server announcement task failed")?;
                bail!("server announcement task stopped unexpectedly")
            }
        }
    }
}

impl Drop for UdpHandle {
    fn drop(&mut self) {
        self.abort();
    }
}

impl Server {
    /// Serves on an existing Mainline socket and announces as a server.
    ///
    /// Announcements use the observed source port and are renewed every ten
    /// minutes. Failed announcements retry after thirty seconds. Dropping the
    /// handle stops serving and renewal; existing DHT announcements expire naturally.
    pub async fn attach(&self, dht: Dht) -> Result<UdpHandle, ActorShutdown> {
        self.attach_with_rendezvous(dht, Some(RENDEZVOUS_INFOHASH))
            .await
    }

    /// Serves and optionally announces under a configured rendezvous hash.
    ///
    /// `None` serves index requests without publishing a Mainline announcement.
    pub async fn attach_with_rendezvous(
        &self,
        dht: Dht,
        rendezvous_hash: Option<[u8; 20]>,
    ) -> Result<UdpHandle, ActorShutdown> {
        let local_addr = dht.info().await?.local_addr().into();
        let (tx, mut rx) = mpsc::channel::<(Box<[u8]>, SocketAddrV4)>(256);
        let metrics = self.metrics();
        dht.set_datagram_hook(Some(n0_mainline::DatagramHook::new(move |bytes, from| {
            if !bytes.starts_with(MAGIC) {
                return false;
            }
            if bytes.len() > MAX_DGRAM {
                return true;
            }
            match tx.try_send((bytes.into(), from)) {
                Ok(()) => true,
                Err(TrySendError::Full(_)) => {
                    metrics.queue_drops.inc();
                    true
                }
                Err(TrySendError::Closed(_)) => false,
            }
        })))
        .await?;
        let server = self.clone();
        let transport = dht.clone();
        let task = tokio::spawn(async move {
            while let Some((bytes, from)) = rx.recv().await {
                server.handle_packet(&bytes, &transport, from).await?;
            }
            Ok(())
        });
        let announcement = rendezvous_hash.map(|rendezvous_hash| {
            tokio::spawn(async move {
                loop {
                    let result = async {
                        let hash = Id::from(rendezvous_hash);
                        dht.get_closest_nodes(hash).await?;
                        dht.announce_peer(hash, None).await
                    }
                    .await;
                    let delay = match result {
                        Ok(_) => Duration::from_secs(600),
                        Err(err) => {
                            debug!(%err, "server announcement failed");
                            Duration::from_secs(30)
                        }
                    };
                    tokio::time::sleep(delay).await;
                }
            })
        });
        Ok(UdpHandle {
            local_addr,
            task,
            announcement,
        })
    }

    async fn handle_packet(
        &self,
        data: &[u8],
        dht: &Dht,
        from: SocketAddrV4,
    ) -> Result<(), ActorShutdown> {
        trace!(%from, bytes = data.len(), "UDP packet");
        if data.is_empty() || data.len() > MAX_DGRAM {
            return Ok(());
        }
        let Some(Proto::Request(Request::V1(request))) = Proto::decode(data) else {
            return Ok(());
        };
        let now = unix_secs();
        let response = match request {
            RequestV1::Prepare { tx, padding: _ } => {
                if !self.allow_request((*from.ip()).into()) {
                    self.metrics().rate_limited.inc();
                    return Ok(());
                }
                self.metrics().prepares.inc();
                Some(Response::V1(ResponseV1::Prepared {
                    tx,
                    addr: from,
                    token: self.issue_token(from, now),
                }))
            }
            RequestV1::Put { tx, token, value } => {
                if !self.verify_token(from, &token, now) {
                    self.metrics().invalid_tokens.inc();
                    trace!(%from, "invalid put token");
                    return Ok(());
                }
                if !self.allow_request((*from.ip()).into()) {
                    self.metrics().rate_limited.inc();
                    return Ok(());
                }
                if let Err(err) = self.put_local(from, value) {
                    self.metrics().rejected_puts.inc();
                    debug!(%from, %err, "put rejected");
                    return Ok(());
                }
                self.metrics().puts.inc();
                Some(Response::V1(ResponseV1::Stored { tx, addr: from }))
            }
            RequestV1::Get { tx, addr } => {
                if !self.allow_request((*from.ip()).into()) {
                    self.metrics().rate_limited.inc();
                    return Ok(());
                }
                self.metrics().gets.inc();
                let value = self.get_local(addr);
                if value.is_some() {
                    self.metrics().get_hits.inc();
                }
                Some(Response::V1(ResponseV1::Value { tx, addr, value }))
            }
        };
        if let Some(response) = response {
            let mut out = [0; MAX_DGRAM];
            let bytes = Proto::Response(response)
                .encode(&mut out)
                .expect("bounded response");
            dht.send_datagram(bytes.to_vec(), from).await?;
        }
        Ok(())
    }
}
