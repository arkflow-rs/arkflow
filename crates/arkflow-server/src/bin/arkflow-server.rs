use arkflow_server::{
    hub::{Hub, HubConfig},
    serve_hub,
    storage::{ControlPlaneStore, StorageActor},
    ServerConfig,
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cancellation = CancellationToken::new();
    let shutdown = cancellation.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown.cancel();
    });
    let config = ServerConfig {
        address: std::env::var("ARKFLOW_HUB_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8080".into()),
        node_token: std::env::var("ARKFLOW_NODE_TOKEN").ok(),
        ..ServerConfig::default()
    };
    let hub_config = HubConfig {
        operator_token: std::env::var("ARKFLOW_OPERATOR_TOKEN").ok(),
        node_token: config.node_token.clone(),
        lease_ttl_ms: config.lease_ttl_ms,
        poll_interval_ms: config.poll_interval_ms,
    };
    let hub = if let Ok(path) = std::env::var("ARKFLOW_HUB_STORAGE") {
        let store = ControlPlaneStore::open(path)?;
        Hub::with_storage(hub_config, StorageActor::start(store, 128))
    } else {
        Hub::new(hub_config)
    };
    serve_hub(hub, config, cancellation).await
}
