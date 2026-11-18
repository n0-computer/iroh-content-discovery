//! Asks HTTP Pkarr relays for a packet Mainline did not answer with.
//!
//! A relay is only a transport: what it returns must carry a valid signature
//! for the requested key, as on Mainline.

use std::{sync::Arc, time::Duration};

use iroh_mainline_endpoint_discovery::decode_signed_packet;
use n0_error::{Result, StdResultExt};
use n0_mainline::MutableItem;
use tracing::{debug, info};

/// How long one relay gets to answer.
const RELAY_TIMEOUT: Duration = Duration::from_secs(5);
/// Largest relay answer: signature, sequence, and a BEP 44 value of 1000 bytes.
const MAX_ANSWER: usize = 64 + 8 + 1000;

/// HTTP Pkarr relays and the client that asks them.
#[derive(Debug, Clone)]
pub(crate) struct Relays {
    client: reqwest::Client,
    urls: Vec<String>,
}

impl Relays {
    /// Returns `None` for an empty list, so no client is built.
    pub(crate) fn new(urls: Vec<String>) -> Result<Option<Self>> {
        if urls.is_empty() {
            return Ok(None);
        }
        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .std_context("TLS configuration")?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let client = reqwest::Client::builder()
            .tls_backend_preconfigured(tls)
            .timeout(RELAY_TIMEOUT)
            .build()
            .std_context("HTTP client")?;
        let urls = urls
            .into_iter()
            .map(|url| url.trim_end_matches('/').to_owned())
            .collect();
        Ok(Some(Self { client, urls }))
    }

    /// Returns the newest verified packet any relay has for `key`.
    pub(crate) async fn resolve(&self, key: &[u8; 32]) -> Option<MutableItem> {
        let name = z32::encode(key);
        let mut newest: Option<MutableItem> = None;
        for url in &self.urls {
            match self.fetch(&format!("{url}/{name}"), key).await {
                Ok(Some(item)) => {
                    debug!(relay = %url, sequence = item.seq(), "Pkarr relay answered");
                    if newest
                        .as_ref()
                        .is_none_or(|newest| item.seq() > newest.seq())
                    {
                        newest = Some(item);
                    }
                }
                Ok(None) => debug!(relay = %url, "Pkarr relay has no packet"),
                Err(error) => info!(relay = %url, %error, "Pkarr relay failed"),
            }
        }
        newest
    }

    async fn fetch(&self, url: &str, key: &[u8; 32]) -> Result<Option<MutableItem>> {
        let mut response = self.client.get(url).send().await.std_context("request")?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        response.error_for_status_ref().std_context("status")?;
        let mut body = key.to_vec();
        while let Some(chunk) = response.chunk().await.std_context("body")? {
            n0_error::ensure_any!(
                body.len() + chunk.len() <= 32 + MAX_ANSWER,
                "answer too large"
            );
            body.extend_from_slice(&chunk);
        }
        // A relay answers without the key, which the signature covers.
        decode_signed_packet(&body)
            .map(Some)
            .ok_or_else(|| n0_error::anyerr!("packet does not verify"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh_mainline_endpoint_discovery::encode_signed_packet;

    /// Serves `body` once over plain HTTP and returns the base URL.
    async fn serve_once(status: &'static str, body: Vec<u8>) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            let _ = socket.read(&mut request).await.unwrap();
            let head = format!(
                "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn relay_answers_must_verify() {
        let key = n0_mainline::SigningKey::from_bytes(&[5; 32]);
        let item = MutableItem::new(&key, b"packet", 7, None);
        let public = *item.key();
        // Relays answer without the leading key.
        let answer = encode_signed_packet(&item).unwrap()[32..].to_vec();
        let mut forged = answer.clone();
        *forged.last_mut().unwrap() ^= 1;
        let relays = |url| Relays::new(vec![url]).unwrap().unwrap();

        let url = serve_once("200 OK", answer).await;
        assert_eq!(relays(url).resolve(&public).await, Some(item));
        let url = serve_once("200 OK", forged).await;
        assert_eq!(relays(url).resolve(&public).await, None);
        let url = serve_once("404 Not Found", Vec::new()).await;
        assert_eq!(relays(url).resolve(&public).await, None);
        assert!(Relays::new(Vec::new()).unwrap().is_none());
    }
}
