//! Runs the iroh link gateway inside the Android app.
//!
//! The app loads `blake3.net` and `pkarr.net` links from
//! `http://<label>.<blake3|pkarr>.localhost:<port>/`, served by an
//! [`iroh_link_gateway::Gateway`] in this process. The gateway listens as soon
//! as [`start`] returns, and requests made before index server discovery
//! finishes wait for it.
//!
//! The JNI entry points belong to `computer.iroh.link.NativeGateway`.

use std::{
    net::Ipv4Addr,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::Duration,
};

use axum::{
    Router,
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header},
    response::{Html, IntoResponse, Response},
};
use iroh::endpoint::presets;
use iroh_link_gateway::Gateway;
use iroh_mainline_endpoint_discovery::{
    AddrIndex, Resolver, decode_signed_packet, encode_signed_packet,
};
use n0_mainline::{Dht, MutableItem};
use tokio::{net::TcpListener, sync::watch};
use tower::ServiceExt;
use tracing::{info, warn};

mod jni_api;
mod logcat;

/// How long a request waits for the gateway to become ready.
const READY_WAIT: Duration = Duration::from_secs(45);
/// Pause between attempts to discover index servers or bind the endpoint.
const RETRY_DELAY: Duration = Duration::from_secs(10);
/// Signed index list from the last successful discovery.
///
/// Used when the list's Pkarr record does not resolve, as the desktop
/// gateway does with its state directory.
const INDEX_LIST_FILE: &str = "index-list.pkarr";

/// Shown while the gateway is still discovering index servers.
const STARTING_PAGE: &str = r#"<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<meta http-equiv="refresh" content="3"><title>Connecting</title>
<style>body{font-family:sans-serif;margin:2em;color:#444}</style></head>
<body><h2>Connecting to the network</h2>
<p>The gateway is still looking for index servers. This page reloads automatically.</p>
</body></html>"#;

/// The process-wide gateway state, set once by [`start`].
static GATEWAY: OnceLock<Running> = OnceLock::new();

/// A started gateway.
struct Running {
    /// Runs the gateway for the life of the process.
    runtime: tokio::runtime::Runtime,
    status: Mutex<String>,
}

/// Starts the gateway on `127.0.0.1:port`, unless it is already running.
///
/// Binds the listener before returning, so an occupied port fails here.
/// Discovery continues in the background; see [`status`].
///
/// # Errors
///
/// Returns a message if the runtime cannot be created or the port cannot be
/// bound.
pub fn start(port: u16, state_dir: PathBuf) -> Result<(), String> {
    static START: Mutex<()> = Mutex::new(());
    let _guard = START.lock().expect("poisoned");
    if GATEWAY.get().is_some() {
        return Ok(());
    }
    logcat::init();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("iroh-link")
        .build()
        .map_err(|error| format!("cannot start runtime: {error}"))?;
    let listener = runtime
        .block_on(TcpListener::bind((Ipv4Addr::LOCALHOST, port)))
        .map_err(|error| format!("cannot listen on port {port}: {error}"))?;
    info!(port, state_dir = %state_dir.display(), "gateway listening");
    let running = GATEWAY.get_or_init(|| Running {
        runtime,
        status: Mutex::new("starting".to_owned()),
    });
    let (ready, ready_rx) = watch::channel(None);
    let proxy = Router::new().fallback(proxy).with_state(ready_rx);
    running.runtime.spawn(async move {
        if let Err(error) = axum::serve(listener, proxy).await {
            warn!(%error, "gateway HTTP server stopped");
            set_status(format!("stopped: {error}"));
        }
    });
    running.runtime.spawn(run(state_dir, ready));
    Ok(())
}

/// Returns a short description of the gateway state.
///
/// One of `not started`, `starting`, `discovering index servers`, `ready`,
/// or a message describing a failure.
pub fn status() -> String {
    match GATEWAY.get() {
        Some(running) => running.status.lock().expect("poisoned").clone(),
        None => "not started".to_owned(),
    }
}

fn set_status(status: impl Into<String>) {
    if let Some(running) = GATEWAY.get() {
        *running.status.lock().expect("poisoned") = status.into();
    }
}

/// Forwards a request to the gateway once it is ready.
async fn proxy(
    State(mut ready): State<watch::Receiver<Option<Router>>>,
    request: Request,
) -> Response {
    let router = match tokio::time::timeout(READY_WAIT, ready.wait_for(Option::is_some)).await {
        Ok(Ok(router)) => router.clone().expect("checked by wait_for"),
        _ => {
            let mut response =
                (StatusCode::SERVICE_UNAVAILABLE, Html(STARTING_PAGE)).into_response();
            response
                .headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            return response;
        }
    };
    match router.oneshot(request).await {
        Ok(response) => response,
        Err(never) => match never {},
    }
}

/// Binds the endpoint, discovers index servers, and publishes the gateway.
///
/// Retries each step until it succeeds, since the phone may be offline when
/// the app starts.
async fn run(state_dir: PathBuf, ready: watch::Sender<Option<Router>>) {
    let endpoint = loop {
        match iroh::Endpoint::bind(presets::N0).await {
            Ok(endpoint) => break endpoint,
            Err(error) => {
                warn!(%error, "cannot bind iroh endpoint; retrying");
                set_status(format!("cannot bind iroh endpoint: {error}"));
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    };
    info!(endpoint = %endpoint.id(), "iroh endpoint bound");
    let dht = loop {
        match Dht::builder().port(0).build() {
            Ok(dht) => break dht,
            Err(error) => {
                warn!(%error, "cannot start Mainline node; retrying");
                set_status(format!("cannot start Mainline node: {error}"));
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    };
    set_status("discovering index servers");
    let mut builder = AddrIndex::builder(dht.clone()).n0_defaults();
    if let Some(item) = load_index_list(&state_dir) {
        builder = builder.fallback_list(item);
    }
    let index = loop {
        match builder.clone().build().await {
            Ok(index) => break index,
            Err(error) => {
                warn!(%error, "index discovery failed; retrying");
                set_status(format!("index discovery failed, retrying: {error}"));
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    };
    if let Some(item) = index.signed_list().await
        && let Err(error) = store_index_list(&state_dir, &item)
    {
        warn!(%error, "cannot store the signed index list");
    }
    let gateway = Gateway::new(endpoint, Resolver::new(dht, index));
    ready.send_replace(Some(gateway.router()));
    set_status("ready");
    info!("gateway ready");
}

fn load_index_list(state: &Path) -> Option<MutableItem> {
    let bytes = std::fs::read(state.join(INDEX_LIST_FILE)).ok()?;
    let item = decode_signed_packet(&bytes);
    if item.is_none() {
        warn!("ignoring stored index list that does not verify");
    }
    item
}

fn store_index_list(state: &Path, item: &MutableItem) -> std::io::Result<()> {
    // Write beside the file and rename, so a crash never leaves half a list.
    let path = state.join(INDEX_LIST_FILE);
    let temporary = path.with_extension("tmp");
    let bytes = encode_signed_packet(item)
        .ok_or_else(|| std::io::Error::other("index list is not a Pkarr packet"))?;
    std::fs::create_dir_all(state)?;
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(temporary, path)
}
