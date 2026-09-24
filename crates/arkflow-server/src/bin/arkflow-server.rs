use arkflow_server::{
    hub::{Hub, HubConfig},
    oidc::OidcFederation,
    serve_hub,
    storage::{ControlPlaneBackend, ControlPlaneStore, StorageActor},
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
        insecure_local: std::env::var("ARKFLOW_HUB_INSECURE_LOCAL")
            .ok()
            .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes")),
        hub_storage: std::env::var("ARKFLOW_HUB_STORAGE").ok(),
        ..ServerConfig::default()
    };
    let hub_config = HubConfig {
        operator_token: std::env::var("ARKFLOW_OPERATOR_TOKEN").ok(),
        node_token: config.node_token.clone(),
        insecure_local: config.insecure_local,
        lease_ttl_ms: config.lease_ttl_ms,
        poll_interval_ms: config.poll_interval_ms,
        session_ttl_ms: config.session_ttl_ms,
    };
    let mut hub = if let Some(spec) = config.hub_storage.as_deref() {
        // `postgres://`/`postgresql://` URLs select the PostgreSQL backend;
        // anything else remains a path to the SQLite database file.
        let backend = if spec.starts_with("postgres://") || spec.starts_with("postgresql://") {
            ControlPlaneBackend::Postgres(arkflow_server::pg_store::PgStore::open(spec).await?)
        } else {
            ControlPlaneBackend::Sqlite(ControlPlaneStore::open(spec)?)
        };
        Hub::with_storage(hub_config, StorageActor::start(backend, 128))
    } else {
        Hub::new(hub_config)
    };
    if let Some(oidc) = OidcFederation::from_env().await {
        hub = hub.with_oidc(oidc);
    }
    serve_hub(hub, config, cancellation).await
}
