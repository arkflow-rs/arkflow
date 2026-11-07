//! One-shot SQLite → PostgreSQL migration for the Hub control-plane store.
//!
//! The copy mechanics live in [`super::table_copier`] and are shared with
//! `backup`/`restore`; this module keeps the migrate entry point (open both
//! endpoints, copy everything, reset identity sequences) and its
//! integration test. The Hub must be stopped while migrating (documented);
//! fresh PostgreSQL deployments converge schema via startup DDL and never
//! need this tool.
use super::postgres::PostgresBackend;
use super::sqlite::SqliteBackend;
use super::table_copier::{copy_sqlite_to_postgres, CopyReport};
use super::StorageError;

pub use super::table_copier::TABLE_ORDER;

/// Outcome of a successful migration: per-table copied row counts.
pub type MigrationReport = CopyReport;

/// Migrate a SQLite store file into a PostgreSQL database. The target is
/// opened through the normal backend path (connectivity probe + idempotent
/// DDL), tables are copied in `TABLE_ORDER` in 1000-row transactions, and any
/// per-table row-count divergence fails with a table-specific error.
pub async fn migrate_sqlite_to_postgres(
    sqlite_path: impl AsRef<std::path::Path>,
    postgres_url: &str,
) -> Result<MigrationReport, StorageError> {
    let source = SqliteBackend::open(sqlite_path)?;
    let target = PostgresBackend::open(postgres_url).await?;
    copy_sqlite_to_postgres(&source, target.pool()).await
}

#[cfg(test)]
mod tests {
    /// Gated live migration test: populates an in-memory SQLite store through
    /// the contract, migrates, and verifies the report and round-trip counts.
    /// The fixture includes a webhook subscription (the `cp_webhooks` table
    /// was missing from the copy order once, silently dropping subscriptions)
    /// and a node with NULL bigint columns (typed-NULL binds).
    #[tokio::test]
    async fn migrates_sqlite_data_into_postgres() {
        if std::env::var("ARKFLOW_TEST_POSTGRES_URL").is_err() {
            eprintln!("skipping: ARKFLOW_TEST_POSTGRES_URL not set");
            return;
        }
        let url = super::super::contract_database_url("pg_migrate_tool").await;
        use super::super::{
            ControlPlaneStore, NodeMutation, StorageBackend, WebhookSetMutation,
            WebhookSubscription,
        };
        let path = std::env::temp_dir().join(format!(
            "arkflow-migrate-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let sqlite = ControlPlaneStore::open(path.to_str().unwrap())
            .await
            .unwrap();
        StorageBackend::upsert_job(
            &sqlite,
            super::super::JobRecord {
                job_id: "migrate-job".into(),
                version: 1,
                spec_json: "{}".into(),
                desired_state: "running".into(),
                observed_state: "running".into(),
                convergence: "converged".into(),
                generation: 3,
                node_ids: vec!["node-1".into()],
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 7,

                pause_phase: None,
                pause_artifact_id: None,
                pause_prev_checkpoint_id: None,
                pause_retry_count: 0,
                paused_at_ms: None,
                paused_by: None,
            },
        )
        .await
        .unwrap();
        // NULL bigint and text columns on a node row: the copy must bind
        // typed NULLs or PostgreSQL rejects the insert (SQLSTATE 42804).
        StorageBackend::upsert_node(
            &sqlite,
            NodeMutation {
                node_id: "node-1".into(),
                version: "test".into(),
                state: "online".into(),
                capabilities_json: "[]".into(),
                boot_id: None,
                report_seq: None,
                last_seen_at_ms: 5,
                lease_expires_at_ms: 0,
                maintenance_state: None,
                maintenance_updated_at_ms: None,
            },
        )
        .await
        .unwrap();
        StorageBackend::replace_webhooks(
            &sqlite,
            WebhookSetMutation {
                subscriptions: vec![WebhookSubscription {
                    id: "hook-1".into(),
                    url: "https://example.test/hook".into(),
                    event_types: vec!["job.".into()],
                    enabled: true,
                    secret: Some("hmac-secret".into()),
                    created_by: Some("operator".into()),
                    created_at_ms: 9,
                }],
                actor: None,
                correlation_id: None,
            },
            9,
        )
        .await
        .unwrap();
        // Close the source connection so the WAL is checkpointed into the main
        // database file before the migration tool reopens it by path, and keep
        // the file until the migration has read it.
        drop(sqlite);

        let report = super::migrate_sqlite_to_postgres(&path, &url)
            .await
            .unwrap();
        assert!(report.total_rows() >= 1);
        let jobs = report
            .rows_per_table
            .iter()
            .find(|(table, _)| table == "cp_jobs")
            .unwrap();
        assert!(jobs.1 >= 1);
        let webhooks = report
            .rows_per_table
            .iter()
            .find(|(table, _)| table == "cp_webhooks")
            .expect("cp_webhooks participates in the migration copy order");
        assert_eq!(webhooks.1, 1);

        let target = ControlPlaneStore::open(&url).await.unwrap();
        let job = StorageBackend::get_job(&target, "migrate-job")
            .await
            .unwrap()
            .expect("migrated job present");
        assert_eq!(job.generation, 3);
        let hooks = StorageBackend::list_webhooks(&target).await.unwrap();
        assert_eq!(hooks.len(), 1, "subscriptions survive the migration");
        assert_eq!(hooks[0].id, "hook-1");
        assert_eq!(hooks[0].secret.as_deref(), Some("hmac-secret"));
        assert!(hooks[0].enabled);
        let _ = std::fs::remove_file(&path);
    }
}
