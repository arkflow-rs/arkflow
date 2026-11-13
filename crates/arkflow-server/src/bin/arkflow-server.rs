use arkflow_server::bootstrap;
use arkflow_server::hub::{HubConfig, HubHaConfig};
use arkflow_server::{apply_env_overrides, load_startup_config, ServerConfig};
use tokio_util::sync::CancellationToken;

const USAGE: &str = "\
arkflow-server — ArkFlow control-plane Hub

Usage:
  arkflow-server [--config <FILE>]
      Run the Hub. Startup configuration comes from environment variables
      (ARKFLOW_*) plus, with --config <FILE>, a full configuration file in
      YAML/JSON/TOML. Variables that are explicitly set override the file;
      everything else falls back to the file and then built-in defaults.
      Secret references (${env:VAR}, ${file:PATH}, ${secret:NAME}) are
      resolved while loading the file. See the CLI reference for the file
      schema and the precedence rules.
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

Usage errors (an unknown flag or stray argument, a missing --config value,
or an unreadable/unparseable config file) print a message to stderr and
exit with code 2.
";

/// Parses serve-path arguments: only `--config <FILE>` is recognized; any
/// unknown flag or stray positional argument is a usage error (exit 2)
/// instead of silently starting the Hub. `first` is the already-consumed
/// first argument (None when there were no arguments).
fn parse_serve_args(
    first: Option<String>,
    mut rest: impl Iterator<Item = String>,
) -> Option<String> {
    match first.as_deref() {
        None => None,
        Some("--config") => {
            let path = rest.next();
            if path.is_none() {
                eprintln!("error: --config requires a file path\n\n{USAGE}");
                std::process::exit(2);
            }
            if let Some(extra) = rest.next() {
                eprintln!("error: unexpected argument '{extra}' after --config <FILE>\n\n{USAGE}");
                std::process::exit(2);
            }
            path
        }
        Some(arg) => {
            eprintln!("error: unrecognized argument '{arg}'\n\n{USAGE}");
            std::process::exit(2);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let result = run(std::env::args().skip(1).collect()).await;
    // Flush buffered OTel spans on every terminal path (serve return or
    // early error); a no-op when tracing was never enabled.
    arkflow_core::cli::shutdown_otel_tracing();
    result
}

async fn run(args: Vec<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut iter = args.into_iter();
    let subcommand = iter.next();
    match subcommand.as_deref() {
        Some("migrate" | "backup" | "restore") => {
            let rest: Vec<String> = iter.collect();
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
    serve_with_config(parse_serve_args(subcommand, iter)).await
}

async fn serve_with_config(
    config_path: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut startup = match config_path.as_deref() {
        Some(path) => match load_startup_config(path) {
            Ok(config) => config,
            Err(error) => {
                eprintln!("error: {error}");
                std::process::exit(2);
            }
        },
        None => arkflow_server::HubStartupConfig::default(),
    };
    apply_env_overrides(&mut startup, &|name| std::env::var(name).ok());
    arkflow_core::cli::init_logging_from(
        &startup.server.logging,
        &startup.server.observability.tracing,
    );

    let mut config: ServerConfig = startup.server;
    let ha: HubHaConfig = startup.ha;
    // The standalone Hub always serves when invoked; the documented
    // `enabled` field is ignored (serve_hub treats false as a silent no-op,
    // which must not happen from an explicit binary invocation).
    config.enabled = true;
    bootstrap::validate_ha_config(&ha, &config).map_err(|message| {
        eprintln!("{message}");
        message
    })?;
    let hub_config = HubConfig {
        operator_token: startup.hub.operator_token,
        node_token: config.node_token.clone(),
        insecure_local: config.insecure_local,
        lease_ttl_ms: config.lease_ttl_ms,
        poll_interval_ms: config.poll_interval_ms,
        session_ttl_ms: config.session_ttl_ms,
        approval_required: config.approval_required,
        approval_ttl_ms: config.approval_ttl_ms,
    };
    let cancellation = CancellationToken::new();
    let shutdown = cancellation.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown.cancel();
    });
    bootstrap::serve_from_config(hub_config, config, ha, cancellation).await
}
