use arkflow_server::{
    hub::{Hub, HubConfig},
    oidc::OidcFederation,
    serve_hub,
    storage::{migrate_tool, ControlPlaneStore, StorageActor},
    ServerConfig,
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // `arkflow-server migrate --from sqlite:<path> --to postgres:<url>`:
    // one-shot offline storage migration (see the hub-ha deployment docs).
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() == Some("migrate") {
        let mut from: Option<String> = None;
        let mut to: Option<String> = None;
        let mut current: Option<&mut Option<String>> = None;
        for arg in args {
            match arg.as_str() {
                "--from" => current = Some(&mut from),
                "--to" => current = Some(&mut to),
                _ => {
                    if let Some(slot) = current.as_deref_mut() {
                        *slot = Some(arg);
                    }
                }
            }
        }
        let (Some(from), Some(to)) = (from, to) else {
            eprintln!("usage: arkflow-server migrate --from sqlite:<path> --to postgres:<url>");
            std::process::exit(2);
        };
        let Some(sqlite_path) = from.strip_prefix("sqlite:") else {
            eprintln!("--from must start with sqlite: (got {from})");
            std::process::exit(2);
        };
        if !(to.starts_with("postgres://") || to.starts_with("postgresql://")) {
            eprintln!("--to must start with postgres:// or postgresql:// (got {to})");
            std::process::exit(2);
        }
        let postgres_url = to;
        let report = migrate_tool::migrate_sqlite_to_postgres(sqlite_path, &postgres_url)
            .await
            .map_err(|error| {
                eprintln!("migration failed: {error}");
                error
            })?;
        for (table, rows) in &report.rows_per_table {
            eprintln!("{table}: {rows} rows");
        }
        eprintln!("migration complete: {} rows total", report.total_rows());
        return Ok(());
    }

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
    let mut hub = if let Some(path) = config.hub_storage.as_deref() {
        let store = ControlPlaneStore::open(path).await?;
        Hub::with_storage(hub_config, StorageActor::start(store, 128))
    } else {
        Hub::new(hub_config)
    };
    if let Some(oidc) = OidcFederation::from_env().await {
        hub = hub.with_oidc(oidc);
    }
    serve_hub(hub, config, cancellation).await
}
