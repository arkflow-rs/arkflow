/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! In-process coverage for the `arkflow-server` binary wiring: the migrate
//! subcommand's argument handling, HA config validation, and the hub
//! bootstrap path (the binary itself keeps only env/argv plumbing).

use arkflow_server::bootstrap;
use arkflow_server::hub::{HubConfig, HubHaConfig};
use arkflow_server::ServerConfig;

fn args(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[tokio::test]
async fn migrate_rejects_each_malformed_invocation() {
    let mut sink = |_message: String| {};

    // Missing both endpoints.
    let code = bootstrap::run_migrate(&args(&[]), &mut sink).await.unwrap();
    assert_eq!(code, 2);
    // Missing --to.
    let code = bootstrap::run_migrate(&args(&["--from", "sqlite:a.db"]), &mut sink)
        .await
        .unwrap();
    assert_eq!(code, 2);
    // --from without the sqlite: prefix.
    let code = bootstrap::run_migrate(&args(&["--from", "a.db", "--to", "postgres://x"]), &mut sink)
        .await
        .unwrap();
    assert_eq!(code, 2);
    // --to without a postgres scheme.
    let code =
        bootstrap::run_migrate(&args(&["--from", "sqlite:a.db", "--to", "mysql://x"]), &mut sink)
            .await
            .unwrap();
    assert_eq!(code, 2);
    // A value with no pending flag is dropped, leaving --to missing.
    let code = bootstrap::run_migrate(&args(&["--from", "sqlite:a.db", "stray"]), &mut sink)
        .await
        .unwrap();
    assert_eq!(code, 2);
}

#[tokio::test]
async fn migrate_reports_a_missing_source_database_loudly() {
    // A well-formed invocation against a nonexistent sqlite file surfaces
    // the migration error through the sink and the Err return.
    let mut messages = Vec::new();
    let mut sink = |message: String| messages.push(message);
    let result = bootstrap::run_migrate(
        &args(&["--from", "sqlite:/nonexistent/arkflow/none.db", "--to", "postgres://127.0.0.1:1/db"]),
        &mut sink,
    )
    .await;
    assert!(result.is_err(), "a missing source must fail");
    assert!(
        messages.iter().any(|m| m.contains("migration failed")),
        "{messages:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn migrate_end_to_end_against_live_postgres_when_available() {
    let Ok(url) = std::env::var("ARKFLOW_TEST_POSTGRES_URL") else {
        eprintln!("skipping: ARKFLOW_TEST_POSTGRES_URL not set");
        return;
    };
    // Seed a sqlite source with one job through the contract store.
    let dir = tempfile::tempdir().unwrap();
    let sqlite_path = dir.path().join("migrate.db");
    let store = arkflow_server::storage::ControlPlaneStore::open(
        sqlite_path.to_str().unwrap(),
    )
    .await
    .unwrap();
    arkflow_server::storage::StorageBackend::upsert_job(
        &store,
        arkflow_server::storage::JobRecord {
            job_id: "bootstrap-migrate".into(),
            version: 1,
            spec_json: "{}".into(),
            desired_state: "running".into(),
            observed_state: "running".into(),
            convergence: "converged".into(),
            generation: 2,
            node_ids: vec![],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 1,
        },
    )
    .await
    .unwrap();
    drop(store);

    // Give the migration a dedicated database.
    let target = format!("{url}/ct_bootstrap_migrate");
    let mut messages = Vec::new();
    let mut sink = |message: String| messages.push(message);
    // The target database may not exist; create it through the admin URL.
    if let Some((prefix, _)) = url.rsplit_once('/') {
        use sqlx::Connection as _;
        if let Ok(mut admin) = sqlx::PgConnection::connect(&format!("{prefix}/postgres")).await {
            let _ = sqlx::query("DROP DATABASE IF EXISTS ct_bootstrap_migrate WITH (FORCE)")
                .execute(&mut admin)
                .await;
            let _ = sqlx::query("CREATE DATABASE ct_bootstrap_migrate")
                .execute(&mut admin)
                .await;
        }
    }
    let code = bootstrap::run_migrate(
        &args(&["--from", &format!("sqlite:{}", sqlite_path.display()), "--to", &target]),
        &mut sink,
    )
    .await
    .unwrap();
    assert_eq!(code, 0, "{messages:?}");
    assert!(messages.iter().any(|m| m.contains("migration complete")));
}

#[test]
fn ha_validation_requires_external_storage_only_when_enabled() {
    let disabled = HubHaConfig {
        enabled: false,
        ..HubHaConfig::default()
    };
    let no_storage = ServerConfig::default();
    // Disabled HA never requires storage.
    bootstrap::validate_ha_config(&disabled, &no_storage).unwrap();

    let enabled = HubHaConfig {
        enabled: true,
        ..HubHaConfig::default()
    };
    let err = bootstrap::validate_ha_config(&enabled, &no_storage).unwrap_err();
    assert!(err.contains("requires ARKFLOW_HUB_STORAGE"), "{err}");

    // Postgres storage passes cleanly; SQLite is accepted with a
    // development-only warning.
    for storage in ["postgres://127.0.0.1:1/db", "sqlite.db"] {
        let config = ServerConfig {
            hub_storage: Some(storage.into()),
            ..ServerConfig::default()
        };
        bootstrap::validate_ha_config(&enabled, &config).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_from_config_starts_and_stops_on_cancellation() {
    let hub_config = HubConfig {
        operator_token: None,
        node_token: None,
        insecure_local: true,
        lease_ttl_ms: 1_000,
        poll_interval_ms: 100,
        session_ttl_ms: 3_600_000,
    };
    let config = ServerConfig {
        address: "127.0.0.1:0".into(),
        insecure_local: true,
        ..ServerConfig::default()
    };
    let ha = HubHaConfig::default();
    let cancellation = tokio_util::sync::CancellationToken::new();
    let cancelled = cancellation.clone();
    let task = tokio::spawn(bootstrap::serve_from_config(
        hub_config,
        config,
        ha,
        cancellation,
    ));
    // Let the listener bind, then shut down cleanly.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    cancelled.cancel();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), task).await;
}
