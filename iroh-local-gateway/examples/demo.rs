//! One-command Pkarr and blob demo using public Mainline by default.

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::PathBuf,
    time::Duration,
};

use clap::Parser;
use iroh::{Endpoint, address_lookup::memory::MemoryLookup, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, store::fs::FsStore};
use iroh_local_gateway::Gateway;
use iroh_mainline_endpoint_discovery::{
    AddrIndex, BLAKE3_DOMAIN, DiscoveryConfig, PKARR_DOMAIN, PkarrPublisher, Publisher, Resolver,
    infohash_from_blake3, pkarr_name,
};
use n0_error::{Result, StackResultExt, StdResultExt};
use n0_mainline::{Dht, SigningKey, Testnet};
use udp_addr_index::{Limits, Server};

#[derive(Parser)]
#[command(about = "Serve a file via Pkarr and a local gateway, using public Mainline")]
struct Args {
    /// File to serve, for example an MP4. Without a file, serve a text greeting.
    file: Option<PathBuf>,
    /// Local HTTP port; configure the same port in the browser extension.
    #[arg(long, default_value_t = 8080)]
    port: u16,
    /// Use an isolated local DHT, server, and iroh discovery instead of public services.
    #[arg(long)]
    local_testnet: bool,
    /// Explicit public index server IPv4:port; otherwise discover index servers through Mainline.
    #[arg(long, env = "IROH_ADDR_INDEX", conflicts_with = "local_testnet")]
    index_server: Option<SocketAddrV4>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    // Bind first so an occupied port fails before importing a potentially large file.
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, args.port))
        .await
        .with_std_context(|_| format!("failed to bind port {}", args.port))?;
    let http_addr = listener.local_addr().anyerr()?;
    let temporary = tempfile::tempdir().anyerr()?;
    let store = FsStore::load(temporary.path()).await?;
    let tag = match args.file {
        Some(path) => {
            println!("Importing {}...", path.display());
            store
                .blobs()
                .add_path(
                    path.canonicalize()
                        .with_std_context(|_| format!("cannot read {}", path.display()))?,
                )
                .await?
        }
        None => {
            store
                .blobs()
                .add_bytes(b"Hello through a Pkarr name, a hash name, and an iroh peer!\n".to_vec())
                .await?
        }
    };
    let hash = tag.hash;
    let encoded = z32::encode(hash.as_bytes());
    let infohash = infohash_from_blake3(&blake3::Hash::from_bytes(*hash.as_bytes())).into();
    let network = if args.local_testnet {
        println!("Using an isolated local Mainline testnet.");
        Some(Testnet::new(3).await.anyerr()?)
    } else {
        println!(
            "Using public Mainline; discovering index servers and publishing may take a minute."
        );
        None
    };
    let node = || {
        let mut builder = Dht::builder();
        builder.port(0);
        if let Some(network) = &network {
            builder.bootstrap(&network.bootstrap);
        }
        builder.build()
    };
    let server = if args.local_testnet {
        Some(Server::new(Limits::for_tests()).attach(node()?).await?)
    } else {
        None
    };
    let server_addr = server
        .as_ref()
        .map(|server| SocketAddrV4::new(Ipv4Addr::LOCALHOST, server.local_addr().port()))
        .or(args.index_server);
    let config = DiscoveryConfig {
        server: server_addr,
        ..Default::default()
    };
    let provider_dht = node()?;
    let index = AddrIndex::discover_with_config(provider_dht.clone(), config.clone())
        .await
        .context(
            "index server discovery failed; supply --index server IP:PORT for an available public index server",
        )?;
    let provider = if args.local_testnet {
        Endpoint::builder(presets::Minimal)
            .bind_addr("127.0.0.1:0".parse::<SocketAddr>().anyerr()?)?
            .bind()
            .await?
    } else {
        Endpoint::bind(presets::N0).await?
    };
    let router = Router::builder(provider.clone())
        .accept(iroh_blobs::ALPN, BlobsProtocol::new(&store, None))
        .spawn();
    tracing::debug!(endpoint = %provider.id(), %hash, "demo blob provider started");
    let publisher = Publisher::new(provider.secret_key().clone(), provider_dht.clone(), index);
    publisher.add_infohash(infohash);
    let key = SigningKey::from_bytes(&rand::random());
    let target = format!("{encoded}.{BLAKE3_DOMAIN}");
    let pkarr = PkarrPublisher::new(provider_dht.clone());
    pkarr.set_blake3(&key, hash.as_bytes())?;
    let public_key = pkarr_name(&key.verifying_key().to_bytes());
    let client = if args.local_testnet {
        Endpoint::builder(presets::Minimal)
            .address_lookup(MemoryLookup::from_endpoint_info([provider.addr()]))
            .bind()
            .await?
    } else {
        Endpoint::bind(presets::N0).await?
    };
    let gateway_dht = node()?;
    let index = AddrIndex::discover_with_config(gateway_dht.clone(), config)
        .await
        .context("gateway index server discovery failed")?;
    let gateway = Gateway::new(client.clone(), Resolver::bind(gateway_dht, index).await?);
    let serve = async {
        tokio::time::timeout(Duration::from_secs(120), publisher.wait_published())
            .await
            .std_context("content publication timed out")?;
        tokio::time::timeout(Duration::from_secs(60), pkarr.publish_all())
            .await
            .std_context("Pkarr publication timed out")??;
        tracing::debug!(%public_key, %target, "demo Pkarr record published");
        println!("\nExtension port: {}", http_addr.port());
        println!("Open:         https://{public_key}.{PKARR_DOMAIN}/");
        println!("Serves:       https://{encoded}.{BLAKE3_DOMAIN}/");
        println!("Pkarr route:  http://{http_addr}/pkarr/{public_key}/");
        println!("Blob route:   http://{http_addr}/blake3/{encoded}");
        println!("\nLeave this running. Press Ctrl-C to stop.");
        // The Pkarr publisher keeps republishing in its own task.
        gateway
            .serve(listener, async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await?;
        Ok(())
    };
    // The publishers keep running in their own tasks until they are dropped.
    let result = tokio::select! {
        result = serve => result,
        result = tokio::signal::ctrl_c() => { result.anyerr()?; Ok(()) },
    };
    client.close().await;
    router.shutdown().await.anyerr()?;
    // Router shutdown also shuts down the blob store before TempDir cleanup.
    result
}
