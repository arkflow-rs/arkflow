//! Testable wiring for the `arkflow-server` binary entry point.
//!
//! The binary keeps only the environment/argv plumbing; the migrate
//! subcommand and the hub bootstrap live here so they can be exercised
//! in-process by integration tests.

use crate::hub::{Hub, HubConfig, HubHaConfig};
use crate::oidc::OidcFederation;
use crate::storage::{ControlPlaneStore, StorageActor};
use crate::{serve_hub, ServerConfig};
use std::error::Error;
use tokio_util::sync::CancellationToken;

/// `arkflow-server migrate --from sqlite:<path> --to postgres:<url>`:
/// one-shot offline storage migration (see the hub-ha deployment docs).
///
/// Returns the process exit code: `0` on success, `2` on a usage error.
pub async fn run_migrate(
    args: &[String],
    error_sink: &mut dyn FnMut(String),
) -> Result<i32, Box<dyn Error + Send + Sync>> {
    let mut args = args.iter();
    let mut from: Option<String> = None;
    let mut to: Option<String> = None;
    let mut current: Option<&mut Option<String>> = None;
    for arg in &mut args {
        match arg.as_str() {
            "--from" => current = Some(&mut from),
            "--to" => current = Some(&mut to),
            _ => {
                if let Some(slot) = current.as_deref_mut() {
                    *slot = Some(arg.clone());
                }
            }
        }
    }
    let (Some(from), Some(to)) = (from, to) else {
        error_sink("usage: arkflow-server migrate --from sqlite:<path> --to postgres:<url>".into());
        return Ok(2);
    };
    let Some(sqlite_path) = from.strip_prefix("sqlite:") else {
        error_sink(format!("--from must start with sqlite: (got {from})"));
        return Ok(2);
    };
    if !(to.starts_with("postgres://") || to.starts_with("postgresql://")) {
        error_sink(format!(
            "--to must start with postgres:// or postgresql:// (got {to})"
        ));
        return Ok(2);
    }
    let postgres_url = to;
    let report = crate::storage::migrate_tool::migrate_sqlite_to_postgres(sqlite_path, &postgres_url)
        .await
        .map_err(|error| {
            error_sink(format!("migration failed: {error}"));
            error
        })?;
    for (table, rows) in &report.rows_per_table {
        error_sink(format!("{table}: {rows} rows"));
    }
    error_sink(format!("migration complete: {} rows total", report.total_rows()));
    Ok(0)
}

/// Validate the HA configuration against the selected storage backend.
///
/// HA requires external storage; a SQLite-backed HA deployment is allowed
/// but logs a development-only warning.
pub fn validate_ha_config(ha: &HubHaConfig, config: &ServerConfig) -> Result<(), String> {
    if !ha.enabled {
        return Ok(());
    }
    if let Some(url) = ha.advertise_url.as_deref() {
        let parsed = url::Url::parse(url)
            .map_err(|error| format!("ARKFLOW_HUB_HA_ADVERTISE_URL is not a valid URL ({url}): {error}"))?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return Err(format!(
                "ARKFLOW_HUB_HA_ADVERTISE_URL must be an absolute http(s) URL with a host: {url}"
            ));
        }
    }
    if config.hub_storage.is_none() {
        return Err(
            "ARKFLOW_HUB_HA_ENABLED requires ARKFLOW_HUB_STORAGE (use a PostgreSQL URL for multi-instance HA)"
                .into(),
        );
    }
    let is_postgres = config
        .hub_storage
        .as_deref()
        .is_some_and(|value| value.starts_with("postgres://") || value.starts_with("postgresql://"));
    if !is_postgres {
        tracing::warn!(
            "HA election is enabled on a SQLite store: multi-instance HA requires the \
             PostgreSQL backend; this configuration is for development and testing only"
        );
    }
    Ok(())
}

/// Build the hub from explicit config and serve it until cancellation.
pub async fn serve_from_config(
    hub_config: HubConfig,
    config: ServerConfig,
    ha: HubHaConfig,
    cancellation: CancellationToken,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut hub = if let Some(path) = config.hub_storage.as_deref() {
        let store = ControlPlaneStore::open(path).await?;
        Hub::with_storage(hub_config, StorageActor::start(store, 128)).with_ha(ha)
    } else {
        Hub::new(hub_config).with_ha(ha)
    };
    if let Some(oidc) = OidcFederation::from_env().await {
        hub = hub.with_oidc(oidc);
    }
    serve_hub(hub, config, cancellation).await
}
