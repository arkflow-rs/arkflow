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
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() == Some("migrate") {
        let rest: Vec<String> = args.collect();
        return run_migrate(rest).await;
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

/// One-shot SQLite -> PostgreSQL control-plane data migration (Hub HA phase 1).
/// Usage: arkflow-server migrate --from sqlite:<path> --to postgres:<url>
/// Stop the Hub before migrating; the target schema is created idempotently.
async fn run_migrate(
    raw: Vec<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The caller passes flag-style arguments: --from <spec> --to <spec>.
    let mut from: Option<String> = None;
    let mut to: Option<String> = None;
    let mut iter = raw.iter();
    while let Some(flag) = iter.next() {
        match flag.as_str() {
            "--from" => from = iter.next().cloned(),
            "--to" => to = iter.next().cloned(),
            other => {
                eprintln!("unknown migrate argument: {other}");
                std::process::exit(2);
            }
        }
    }
    let (from, to) = match (from, to) {
        (Some(from), Some(to)) => (from, to),
        _ => {
            eprintln!("usage: arkflow-server migrate --from sqlite:<path> --to postgres:<url>");
            std::process::exit(2);
        }
    };
    if !from.starts_with("sqlite:") || !to.starts_with("postgres") {
        eprintln!("migrate requires --from sqlite:<path> and --to postgres:<url>");
        std::process::exit(2);
    }
    let sqlite_path = &from["sqlite:".len()..];
    let source = arkflow_server::storage::ControlPlaneStore::open(sqlite_path)?;
    let target = arkflow_server::pg_store::PgStore::open(&to).await?;

    let mut counts: Vec<(&'static str, i64, i64)> = Vec::new();
    for table in arkflow_server::pg_store::MIGRATION_TABLES {
        let (columns, types, rows) = source.export_table(table)?;
        target.import_table(table, &columns, &types, &rows).await?;
        target.reset_identity_sequences().await?;
        let source_count = source.table_row_count(table)?;
        let target_count = target.table_row_count(table).await?;
        if source_count != target_count {
            eprintln!(
                "migration mismatch on {table}: source {source_count} rows, target {target_count} rows"
            );
            std::process::exit(1);
        }
        counts.push((table, source_count, target_count));
    }
    println!("{:>24}  {:>10}  {:>10}", "table", "source", "target");
    for (table, source_count, target_count) in &counts {
        println!("{:>24}  {source_count:>10}  {target_count:>10}", table);
    }
    println!("migration complete: {} tables reconciled", counts.len());
    Ok(())
}
