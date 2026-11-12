use arkflow_server::bootstrap;
use arkflow_server::hub::{HubConfig, HubHaConfig};
use arkflow_server::ServerConfig;
use tokio_util::sync::CancellationToken;

/// Rate-limit overrides for the Hub plane; unset variables fall back to the
/// defaults in `arkflow_core::config::RateLimitConfig`. Unrecognized boolean
/// spellings and unparseable numbers warn on stderr instead of silently
/// flipping a default: a typo in `ARKFLOW_RATE_LIMIT_ENABLED` must not turn
/// a protection off unnoticed.
fn env_rate_limit_config() -> arkflow_core::config::RateLimitConfig {
    use arkflow_core::config::{
        default_node_burst, default_node_rps, default_principal_burst, default_principal_rps,
        default_rate_limit_enabled, RateLimitConfig,
    };
    let warn = |name: &str, value: &str, expected: &str| {
        eprintln!("warning: {name}='{value}' is not {expected}; using the default");
    };
    let flag = |name: &str| -> Option<bool> {
        std::env::var(name).ok().map(|value| match value.as_str() {
            "1" | "true" | "yes" => true,
            "0" | "false" | "no" => false,
            _ => {
                warn(name, &value, "one of 1/true/yes/0/false/no");
                true
            }
        })
    };
    let number = |name: &str| -> Option<f64> {
        match std::env::var(name) {
            Ok(value) => match value.parse::<f64>() {
                Ok(parsed) => Some(parsed),
                Err(_) => {
                    warn(name, &value, "a number");
                    None
                }
            },
            Err(_) => None,
        }
    };
    RateLimitConfig {
        enabled: flag("ARKFLOW_RATE_LIMIT_ENABLED").unwrap_or_else(default_rate_limit_enabled),
        principal_rps: number("ARKFLOW_RATE_LIMIT_PRINCIPAL_RPS")
            .unwrap_or_else(default_principal_rps),
        principal_burst: number("ARKFLOW_RATE_LIMIT_PRINCIPAL_BURST")
            .unwrap_or_else(default_principal_burst),
        node_rps: number("ARKFLOW_RATE_LIMIT_NODE_RPS").unwrap_or_else(default_node_rps),
        node_burst: number("ARKFLOW_RATE_LIMIT_NODE_BURST").unwrap_or_else(default_node_burst),
    }
}

const USAGE: &str = "\
arkflow-server — ArkFlow control-plane Hub

Usage:
  arkflow-server
      Run the Hub (configured through ARKFLOW_* environment variables).
  arkflow-server migrate --from sqlite:<path> --to postgres:<url>
      One-shot offline store migration (Hub stopped; see the deploy docs).
  arkflow-server backup --from <store> --to <snapshot-file>
      Snapshot the control-plane store to a new SQLite-format file.
      <store> is a SQLite path or a postgres:// URL; the snapshot file
      must not exist.
  arkflow-server restore --from <snapshot-file> --to <store>
      Restore a snapshot into an EMPTY control-plane store. Stop every
      Hub instance against the target first.
  arkflow-server --help
      Show this help.
";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let subcommand = args.next();
    match subcommand.as_deref() {
        Some("migrate" | "backup" | "restore") => {
            let rest: Vec<String> = args.collect();
            let mut stderr = |message: String| eprintln!("{message}");
            let code = match subcommand.as_deref() {
                Some("migrate") => bootstrap::run_migrate(&rest, &mut stderr).await?,
                Some("backup") => bootstrap::run_backup(&rest, &mut stderr).await?,
                _ => bootstrap::run_restore(&rest, &mut stderr).await?,
            };
            if code != 0 {
                std::process::exit(code);
            }
            return Ok(());
        }
        Some("--help" | "-h" | "help") => {
            print!("{USAGE}");
            return Ok(());
        }
        _ => {}
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
        approval_required: std::env::var("ARKFLOW_APPROVAL_REQUIRED")
            .ok()
            .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes")),
        approval_ttl_ms: std::env::var("ARKFLOW_APPROVAL_TTL_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|ttl| *ttl >= 1_000)
            .unwrap_or_else(arkflow_server::hub::default_approval_ttl_ms),
        rate_limit: env_rate_limit_config(),
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
        approval_required: config.approval_required,
        approval_ttl_ms: config.approval_ttl_ms,
    };
    bootstrap::serve_from_config(hub_config, config, ha, cancellation).await
}
