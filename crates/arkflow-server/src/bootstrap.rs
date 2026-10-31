//! Testable wiring for the `arkflow-server` binary entry point.
//!
//! The binary keeps only the environment/argv plumbing; the migrate
//! subcommand and the hub bootstrap live here so they can be exercised
//! in-process by integration tests.

use crate::hub::{Hub, HubConfig, HubHaConfig};
use crate::oidc::OidcFederation;
use crate::storage::table_copier;
use crate::storage::{ControlPlaneStore, StorageActor};
use crate::{serve_hub, ServerConfig};
use std::error::Error;
use tokio_util::sync::CancellationToken;

/// Whether a storage value selects the PostgreSQL backend (the normal
/// scheme dispatch: `postgres://`/`postgresql://` URLs; everything else is
/// a SQLite file path).
fn is_postgres_url(value: &str) -> bool {
    value.starts_with("postgres://") || value.starts_with("postgresql://")
}

/// Parse repeated `--flag value` pairs. A value without a pending flag is
/// dropped (so a stray argument cannot silently become an endpoint).
fn take_flags(args: &[String], names: &[&str]) -> Vec<Option<String>> {
    let mut slots: Vec<Option<String>> = vec![None; names.len()];
    let mut current: Option<usize> = None;
    for arg in args {
        match names.iter().position(|name| *name == arg.as_str()) {
            Some(index) => current = Some(index),
            None => {
                if let Some(index) = current.take() {
                    slots[index] = Some(arg.clone());
                }
            }
        }
    }
    slots
}

/// `arkflow-server migrate --from sqlite:<path> --to postgres:<url>`:
/// one-shot offline storage migration (see the hub-ha deployment docs).
///
/// Returns the process exit code: `0` on success, `2` on a usage error.
pub async fn run_migrate(
    args: &[String],
    error_sink: &mut dyn FnMut(String),
) -> Result<i32, Box<dyn Error + Send + Sync>> {
    let flags = take_flags(args, &["--from", "--to"]);
    let (Some(from), Some(to)) = (flags[0].clone(), flags[1].clone()) else {
        error_sink("usage: arkflow-server migrate --from sqlite:<path> --to postgres:<url>".into());
        return Ok(2);
    };
    let Some(sqlite_path) = from.strip_prefix("sqlite:") else {
        error_sink(format!("--from must start with sqlite: (got {from})"));
        return Ok(2);
    };
    if !is_postgres_url(&to) {
        error_sink(format!(
            "--to must start with postgres:// or postgresql:// (got {to})"
        ));
        return Ok(2);
    }
    let postgres_url = to;
    let report =
        crate::storage::migrate_tool::migrate_sqlite_to_postgres(sqlite_path, &postgres_url)
            .await
            .map_err(|error| {
                error_sink(format!("migration failed: {error}"));
                error
            })?;
    for (table, rows) in &report.rows_per_table {
        error_sink(format!("{table}: {rows} rows"));
    }
    error_sink(format!(
        "migration complete: {} rows total",
        report.total_rows()
    ));
    Ok(0)
}

/// `arkflow-server backup --from <store> --to <snapshot-file>`: produce a
/// consistent SQLite-format snapshot of the control-plane store. The store
/// is selected by the normal scheme dispatch (`postgres://` URL or a SQLite
/// path); the snapshot file must not exist.
///
/// Returns the process exit code: `0` on success, `2` on a usage error.
pub async fn run_backup(
    args: &[String],
    error_sink: &mut dyn FnMut(String),
) -> Result<i32, Box<dyn Error + Send + Sync>> {
    let flags = take_flags(args, &["--from", "--to"]);
    let (Some(store), Some(to)) = (flags[0].clone(), flags[1].clone()) else {
        error_sink("usage: arkflow-server backup --from <store> --to <snapshot-file>".into());
        return Ok(2);
    };
    if std::path::Path::new(&to).exists() {
        error_sink(format!(
            "backup failed: snapshot file already exists (refusing to overwrite): {to}"
        ));
        return Ok(2);
    }
    let report = if is_postgres_url(&store) {
        let backend = match ControlPlaneStore::open(&store).await {
            Ok(ControlPlaneStore::Postgres(backend)) => backend,
            Ok(ControlPlaneStore::Sqlite(_)) => {
                error_sink(format!(
                    "backup failed: {store} selected the SQLite backend despite its scheme"
                ));
                return Ok(2);
            }
            Err(error) => {
                error_sink(format!("backup failed: {error}"));
                return Err(error.into());
            }
        };
        match table_copier::backup_postgres_to_sqlite(backend.pool(), &to).await {
            Ok(report) => report,
            Err(error) => {
                // Fail closed: a partial snapshot must not linger where a
                // retry would refuse to overwrite it.
                let _ = std::fs::remove_file(&to);
                error_sink(format!("backup failed: {error}"));
                return Err(error.into());
            }
        }
    } else {
        // Opening a nonexistent path would silently create an empty store
        // and "back it up" — refuse instead.
        if !std::path::Path::new(&store).exists() {
            error_sink(format!(
                "backup failed: source store file does not exist: {store}"
            ));
            return Ok(2);
        }
        if let Some(message) = sqlite_control_plane_file_guard(&store) {
            error_sink(format!("backup failed: {message}"));
            return Ok(2);
        }
        let store = match ControlPlaneStore::open(&store).await {
            Ok(store) => store,
            Err(error) => {
                error_sink(format!("backup failed: {error}"));
                return Err(error.into());
            }
        };
        let actor = StorageActor::start(store, 128);
        match actor.backup_sqlite(&to).await {
            Ok(report) => report,
            Err(error) => {
                error_sink(format!("backup failed: {error}"));
                return Err(error.into());
            }
        }
    };
    for (table, rows) in &report.rows_per_table {
        error_sink(format!("{table}: {rows} rows"));
    }
    error_sink(format!(
        "backup complete: {} rows total -> {to}",
        report.total_rows()
    ));
    Ok(0)
}

/// `arkflow-server restore --from <snapshot-file> --to <store>`: import a
/// backup snapshot into an EMPTY control-plane store (scheme-dispatched
/// like `--from` of `backup`). Every Hub instance against the target must
/// be stopped; the empty check refuses any store that already holds rows.
///
/// Returns the process exit code: `0` on success, `2` on a usage error.
pub async fn run_restore(
    args: &[String],
    error_sink: &mut dyn FnMut(String),
) -> Result<i32, Box<dyn Error + Send + Sync>> {
    let flags = take_flags(args, &["--from", "--to"]);
    let (Some(from), Some(to)) = (flags[0].clone(), flags[1].clone()) else {
        error_sink("usage: arkflow-server restore --from <snapshot-file> --to <store>".into());
        return Ok(2);
    };
    if is_postgres_url(&from) {
        error_sink(format!(
            "restore failed: --from must be a SQLite snapshot file (got {from})"
        ));
        return Ok(2);
    }
    if !std::path::Path::new(&from).exists() {
        error_sink(format!(
            "restore failed: snapshot file does not exist: {from}"
        ));
        return Ok(2);
    }
    if let Some(message) = sqlite_control_plane_file_guard(&from) {
        error_sink(format!("restore failed: {message}"));
        return Ok(2);
    }
    // Open a WORKING COPY of the snapshot, never the archive itself: the
    // normal backend open flips the file's journal mode and may run schema
    // migration, and the archived backup must stay byte-identical (this also
    // lets restore read a snapshot from read-only media). The copy is
    // removed when the restore finishes, success or failure.
    let work_dir = std::env::temp_dir().join(format!(
        "arkflow-restore-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis())
            .unwrap_or_default()
    ));
    let work_copy = work_dir.join("snapshot.sqlite");
    if let Err(error) =
        std::fs::create_dir_all(&work_dir).and_then(|()| std::fs::copy(&from, &work_copy))
    {
        error_sink(format!(
            "restore failed: could not stage a working copy of the snapshot: {error}"
        ));
        return Ok(2);
    }
    let outcome =
        run_restore_from_snapshot(work_copy.to_string_lossy().as_ref(), &to, error_sink).await;
    let _ = std::fs::remove_dir_all(&work_dir);
    outcome
}

/// Restore body after the snapshot has been staged as a writable working
/// copy: version check via the normal open path, scheme-dispatched target,
/// empty check, copy, verify, sequences.
async fn run_restore_from_snapshot(
    snapshot_path: &str,
    target: &str,
    error_sink: &mut dyn FnMut(String),
) -> Result<i32, Box<dyn Error + Send + Sync>> {
    let from = snapshot_path;
    let to = target;
    // Opening the snapshot runs the normal version check: a snapshot newer
    // than this binary fails here, before any target is touched; an older
    // one is upgraded through the existing open-time migration path.
    let snapshot = match ControlPlaneStore::open(from).await {
        Ok(ControlPlaneStore::Sqlite(backend)) => backend,
        Ok(ControlPlaneStore::Postgres(_)) => {
            error_sink(format!(
                "restore failed: {from} opened as PostgreSQL although it is not a postgres:// URL"
            ));
            return Ok(2);
        }
        Err(error) => {
            error_sink(format!("restore failed: {error}"));
            return Err(error.into());
        }
    };
    let report = if is_postgres_url(to) {
        let backend = match ControlPlaneStore::open(&to).await {
            Ok(ControlPlaneStore::Postgres(backend)) => backend,
            Ok(ControlPlaneStore::Sqlite(_)) => {
                error_sink(format!(
                    "restore failed: {to} selected the SQLite backend despite its scheme"
                ));
                return Ok(2);
            }
            Err(error) => {
                error_sink(format!("restore failed: {error}"));
                return Err(error.into());
            }
        };
        match table_copier::restore_postgres_target(&snapshot, backend.pool()).await {
            Ok(report) => report,
            Err(error) => {
                error_sink(format!("restore failed: {error}"));
                return Err(error.into());
            }
        }
    } else {
        if let Some(message) = sqlite_control_plane_file_guard(to) {
            error_sink(format!("restore failed: {message}"));
            return Ok(2);
        }
        match table_copier::restore_sqlite_target(&snapshot, to).await {
            Ok(report) => report,
            Err(error) => {
                error_sink(format!("restore failed: {error}"));
                return Err(error.into());
            }
        }
    };
    for (table, rows) in &report.rows_per_table {
        error_sink(format!("{table}: {rows} rows"));
    }
    error_sink(format!(
        "restore complete: {} rows total -> {to}",
        report.total_rows()
    ));
    Ok(0)
}

/// A SQLite file handed to `backup --from` (the source store), `restore
/// --from` (the snapshot), or `restore --to` (the target) must be a
/// control-plane database. A nonexistent or zero-length file passes (the
/// real open path creates it); an existing non-empty file is only accepted
/// when it already looks like a control-plane database — silently DDL-ing
/// `cp_*` tables into an unrelated SQLite file would destroy its contents,
/// and treating an unrelated file as a "snapshot" would restore nothing
/// while looking successful.
fn sqlite_control_plane_file_guard(path: &str) -> Option<String> {
    let metadata = std::fs::metadata(path).ok()?;
    if metadata.len() == 0 {
        return None;
    }
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .ok()?;
    let has_control_plane_tables: Option<bool> = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name LIKE 'cp%')",
            [],
            |row| row.get(0),
        )
        .ok();
    match has_control_plane_tables {
        // Not readable as SQLite at all: let the real open path produce the
        // actionable error.
        None => None,
        Some(true) => None,
        Some(false) => Some(format!(
            "refusing to use {path}: it is not a control-plane database"
        )),
    }
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
        let parsed = url::Url::parse(url).map_err(|error| {
            format!("ARKFLOW_HUB_HA_ADVERTISE_URL is not a valid URL ({url}): {error}")
        })?;
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
    let is_postgres = config.hub_storage.as_deref().is_some_and(|value| {
        value.starts_with("postgres://") || value.starts_with("postgresql://")
    });
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
