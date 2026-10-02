use arkflow_server::bootstrap;
use arkflow_server::hub::{HubConfig, HubHaConfig};
use arkflow_server::ServerConfig;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() == Some("migrate") {
        let rest: Vec<String> = args.collect();
        let mut stderr = |message: String| eprintln!("{message}");
        let code = bootstrap::run_migrate(&rest, &mut stderr).await?;
        if code != 0 {
            std::process::exit(code);
        }
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
        tls_cert: std::env::var("ARKFLOW_HUB_TLS_CERT").ok(),
        tls_key: std::env::var("ARKFLOW_HUB_TLS_KEY").ok(),
        ..ServerConfig::default()
    };
    let ha = HubHaConfig {
        enabled: std::env::var("ARKFLOW_HUB_HA_ENABLED")
            .ok()
            .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes")),
        lease_ttl_ms: std::env::var("ARKFLOW_HUB_HA_LEASE_TTL_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|ttl| *ttl >= 1_000)
            .unwrap_or(15_000),
        holder_id: std::env::var("ARKFLOW_HUB_HA_HOLDER_ID").ok(),
        advertise_url: std::env::var("ARKFLOW_HUB_HA_ADVERTISE_URL")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty()),
    };
    bootstrap::validate_ha_config(&ha, &config).map_err(|message| {
        eprintln!("{message}");
        message
    })?;
    let hub_config = HubConfig {
        operator_token: std::env::var("ARKFLOW_OPERATOR_TOKEN").ok(),
        node_token: config.node_token.clone(),
        insecure_local: config.insecure_local,
        lease_ttl_ms: config.lease_ttl_ms,
        poll_interval_ms: config.poll_interval_ms,
        session_ttl_ms: config.session_ttl_ms,
    };
    bootstrap::serve_from_config(hub_config, config, ha, cancellation).await
}
