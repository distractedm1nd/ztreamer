//! `ztreamerd` process lifecycle: embedded Zakura, historical indexing, head following, and servers.

use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use tokio::sync::watch;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;
use zakurad::{components::metrics::MetricsEndpoint, config::ZakuradConfig, node};
use ztreamer_indexer::{
    head::HeadSyncError, index::Index, pipeline::PipelineConfig, source::ZakuraSource,
};
use ztreamer_protocol::proto::compact_tx_streamer_server::CompactTxStreamerServer;
use ztreamer_service::{CompactService, HeadFollowerConfig, p2p::P2pCompactService};

/// Must be large enough to hold full mainnet index, which is 21 GiB at the time of writing
const DEFAULT_MAP_SIZE: usize = 64 * 1024 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(version, about = "CompactTxStreamer daemon backed by embedded Zakura")]
struct Cli {
    /// Zakura TOML configuration. Zakura defaults and ZAKURA_* variables apply when omitted.
    #[arg(long, value_name = "FILE")]
    zakura_config: Option<PathBuf>,

    /// Directory containing the LMDB compact index.
    #[arg(long, default_value = "ztreamer-index", value_name = "DIR")]
    index_dir: PathBuf,

    /// Maximum LMDB map size in bytes.
    #[arg(long, default_value_t = NonZeroUsize::new(DEFAULT_MAP_SIZE).unwrap())]
    index_map_size: NonZeroUsize,

    /// Historical fetch/parse workers. Defaults to the pipeline setting.
    #[arg(long = "fetch-workers")]
    workers: Option<NonZeroUsize>,

    /// Consecutive blocks scanned by each historical fetch worker.
    #[arg(long)]
    source_segment_blocks: Option<NonZeroU32>,

    /// Maximum bytes awaiting ordered historical ingestion.
    #[arg(long)]
    max_pending_bytes: Option<NonZeroUsize>,

    /// Maximum compact-index write batch size in bytes.
    #[arg(long)]
    max_batch_bytes: Option<NonZeroUsize>,

    /// CompactTxStreamer gRPC listener.
    #[arg(long, default_value = "127.0.0.1:9067")]
    grpc_listen: SocketAddr,

    /// Prometheus metrics listener.
    #[arg(long, default_value = "127.0.0.1:9999")]
    metrics_listen: SocketAddr,

    /// Exit after historical indexing instead of starting servers and the head follower.
    #[arg(long)]
    index_only: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let mut config = ZakuradConfig::load(cli.zakura_config).context("load Zakura configuration")?;
    config.metrics.endpoint_addr = Some(cli.metrics_listen);
    let _metrics = MetricsEndpoint::new(&config.metrics).context("start Prometheus endpoint")?;
    let network = config.network.network.clone();
    let index = Arc::new(Index::open(
        &cli.index_dir,
        cli.index_map_size.get(),
        &network.to_string(),
        network.genesis_hash().0,
    )?);

    let p2p = Arc::new(P2pCompactService::pending());
    info!(network = %network, "starting embedded Zakura");
    let node = node::spawn_with_services(config, vec![p2p.registration()])
        .await
        .map_err(|error| anyhow!("start embedded Zakura: {error}"))?;
    let client = node.client();
    let signal = tokio::signal::ctrl_c();
    tokio::pin!(signal);

    info!("waiting for Zakura to sync near the chain tip");
    tokio::select! {
        result = client.wait_until_close_to_tip() => {
            result.map_err(|error| anyhow!("wait for Zakura sync: {error}"))?;
        }
        result = &mut signal => {
            result.context("install Ctrl-C handler")?;
            info!("shutting down");
            return node
                .shutdown()
                .await
                .map_err(|error| anyhow!("shut down embedded Zakura: {error}"));
        }
    }
    info!(tip = ?client.tip(), "Zakura is near the chain tip");

    while client.database().tip().is_none() {
        info!("waiting for Zakura's first finalized block");
        tokio::select! {
            result = &mut signal => {
                result.context("install Ctrl-C handler")?;
                info!("shutting down");
                return node
                    .shutdown()
                    .await
                    .map_err(|error| anyhow!("shut down embedded Zakura: {error}"));
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
        }
    }

    let mut pipeline = PipelineConfig::default();
    if let Some(workers) = cli.workers {
        pipeline.workers = workers.get();
    }
    if let Some(source_segment_blocks) = cli.source_segment_blocks {
        pipeline.source_segment_blocks = source_segment_blocks.get();
    }
    if let Some(max_pending_bytes) = cli.max_pending_bytes {
        pipeline.max_pending_bytes = max_pending_bytes.get();
    }
    if let Some(max_batch_bytes) = cli.max_batch_bytes {
        pipeline.max_batch_bytes = max_batch_bytes.get();
    }
    let source = ZakuraSource::new(client.database());
    let sync_index = Arc::clone(&index);
    info!("syncing historical compact index");
    let historical_started = Instant::now();
    let mut historical = tokio::task::spawn_blocking(move || source.sync(&sync_index, pipeline));
    let state = tokio::select! {
        result = &mut historical => result.context("historical index task panicked")??,
        result = &mut signal => {
            result.context("install Ctrl-C handler")?;
            info!("waiting for the historical index writer before shutdown");
            let outcome = historical
                .await
                .context("historical index task panicked")
                .and_then(|result| result.map_err(Into::into));
            let node_result = node
                .shutdown()
                .await
                .map_err(|error| anyhow!("shut down embedded Zakura: {error}"));
            outcome?;
            node_result?;
            return Ok(());
        }
    };
    info!(
        height = state.durable_tip().map(|tip| tip.height),
        elapsed_seconds = historical_started.elapsed().as_secs_f64(),
        "historical compact index complete"
    );

    if cli.index_only {
        drop(client);
        drop(p2p);
        drop(index);
        return node
            .shutdown()
            .await
            .map_err(|error| anyhow!("shut down embedded Zakura: {error}"));
    }

    let compact =
        CompactService::with_node(index, state, network.bip70_network_name(), client.clone());
    let mut startup_client = client.clone();
    match compact.sync_head(&mut startup_client, pipeline).await {
        Err(HeadSyncError::DeepReorg { .. }) => {
            compact
                .recover_deep_reorg(&mut startup_client, pipeline)
                .await
        }
        result => result,
    }
    .context("reconcile compact index with Zakura's canonical head")?;
    p2p.install(compact.clone())
        .map_err(|error| anyhow!(error))?;
    drop(p2p);

    let (shutdown, _) = watch::channel(false);
    let mut follower = tokio::spawn({
        let compact = compact.clone();
        let receiver = shutdown.subscribe();
        async move {
            compact
                .follow_head(client, pipeline, HeadFollowerConfig::default(), receiver)
                .await
                .context("head follower failed")
        }
    });
    let mut grpc = tokio::spawn({
        let receiver = shutdown.subscribe();
        async move {
            info!(address = %cli.grpc_listen, "serving CompactTxStreamer gRPC");
            Server::builder()
                .add_service(CompactTxStreamerServer::new(compact))
                .serve_with_shutdown(cli.grpc_listen, shutdown_requested(receiver))
                .await
                .context("gRPC server failed")
        }
    });

    let mut signal_result = None;
    let mut grpc_result = None;
    let mut follower_result = None;
    tokio::select! {
        result = &mut signal => signal_result = Some(result),
        result = &mut grpc => grpc_result = Some(result),
        result = &mut follower => follower_result = Some(result),
    }
    let _ = shutdown.send(true);

    let outcome = async {
        let signaled = signal_result.is_some();
        if let Some(result) = signal_result {
            result.context("install Ctrl-C handler")?;
            info!("shutting down");
        }
        if grpc_result.is_none() {
            grpc_result = Some(grpc.await);
        }
        if follower_result.is_none() {
            follower_result = Some(follower.await);
        }
        task_result(grpc_result.unwrap(), "gRPC")?;
        task_result(follower_result.unwrap(), "head follower")?;
        if !signaled {
            return Err(anyhow!("daemon task stopped unexpectedly"));
        }
        Ok(())
    }
    .await;

    let node_result = node
        .shutdown()
        .await
        .map_err(|error| anyhow!("shut down embedded Zakura: {error}"));
    outcome?;
    node_result
}

async fn shutdown_requested(mut receiver: watch::Receiver<bool>) {
    while !*receiver.borrow() && receiver.changed().await.is_ok() {}
}

fn task_result(result: Result<Result<()>, tokio::task::JoinError>, name: &str) -> Result<()> {
    result.with_context(|| format!("{name} task panicked"))?
}
