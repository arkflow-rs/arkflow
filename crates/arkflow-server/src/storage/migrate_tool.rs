//! One-shot SQLite → PostgreSQL migration for the Hub control-plane store.
//!
//! Copies every `cp_*` table in foreign-key order in bounded chunks, resets
//! IDENTITY sequences above the migrated max ids, and verifies per-table row
//! counts. The Hub must be stopped while migrating (documented); fresh
//! PostgreSQL deployments converge schema via startup DDL and never need this
//! tool.
use super::postgres::{PgVal, PostgresBackend};
use super::sqlite::SqliteBackend;
use super::StorageError;
use rusqlite::types::ValueRef;

/// Foreign-key-safe copy order (referenced tables first).
pub const TABLE_ORDER: [&str; 16] = [
    "cp_nodes",
    "cp_jobs",
    "cp_job_versions",
    "cp_job_tasks",
    "cp_job_checkpoints",
    "cp_config_versions",
    "cp_intents",
    "cp_stream_desired",
    "cp_stream_observed",
    "cp_attempts",
    "cp_events",
    "cp_audit_events",
    "cp_rollouts",
    "cp_rollout_targets",
    "cp_operations",
    "cp_outbox",
];

/// Identity (autoincrement) columns that need sequence resets after copying.
const IDENTITY_COLUMNS: [(&str, &str); 3] = [
    ("cp_events", "event_id"),
    ("cp_audit_events", "event_id"),
    ("cp_outbox", "outbox_id"),
];

const CHUNK: usize = 1000;

fn value_to_pg(value: ValueRef<'_>) -> PgVal {
    match value {
        ValueRef::Null => PgVal::Null,
        ValueRef::Integer(v) => PgVal::Int(v),
        ValueRef::Real(v) => PgVal::Real(v),
        ValueRef::Text(v) => PgVal::Text(String::from_utf8_lossy(v).into_owned()),
        ValueRef::Blob(v) => PgVal::Bytes(v.to_vec()),
    }
}

/// Outcome of a successful migration: per-table copied row counts.
pub struct MigrationReport {
    pub rows_per_table: Vec<(String, usize)>,
}

impl MigrationReport {
    pub fn total_rows(&self) -> usize {
        self.rows_per_table.iter().map(|(_, count)| *count).sum()
    }
}

fn sqlite_table_columns(
    source: &SqliteBackend,
    table: &str,
) -> Result<Vec<String>, StorageError> {
    source.with_connection(|connection| {
        let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(columns)
    })
}

fn sqlite_read_table(
    source: &SqliteBackend,
    table: &str,
    columns: &[String],
) -> Result<Vec<Vec<PgVal>>, StorageError> {
    let column_list = columns.join(", ");
    let sql = format!("SELECT {column_list} FROM {table}");
    source.with_connection(|connection| {
        let mut statement = connection.prepare(&sql)?;
        let mut rows = statement.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let mut values = Vec::with_capacity(columns.len());
            for index in 0..columns.len() {
                values.push(value_to_pg(row.get_ref(index)?));
            }
            out.push(values);
        }
        Ok(out)
    })
}

fn sqlite_count_rows(source: &SqliteBackend, table: &str) -> Result<usize, StorageError> {
    source.with_connection(|connection| {
        let count: i64 = connection.query_row(
            &format!("SELECT COUNT(*) FROM {table}"),
            [],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    })
}

async fn pg_count(pool: &sqlx::PgPool, table: &str) -> Result<usize, StorageError> {
    let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(pool)
        .await?;
    Ok(count as usize)
}

async fn pg_insert_chunk(
    pool: &sqlx::PgPool,
    table: &str,
    columns: &[String],
    rows: &[Vec<PgVal>],
) -> Result<(), StorageError> {
    let mut transaction = pool.begin().await?;
    let column_list = columns.join(", ");
    for values in rows {
        let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${i}")).collect();
        let sql = format!(
            "INSERT INTO {table} ({column_list}) VALUES ({}) ON CONFLICT DO NOTHING",
            placeholders.join(", ")
        );
        let mut query = sqlx::query(&sql);
        for value in values {
            query = match value {
                PgVal::Null => query.bind(Option::<String>::None),
                PgVal::Text(v) => query.bind(v.clone()),
                PgVal::Int(v) => query.bind(*v),
                PgVal::Real(v) => query.bind(*v),
                PgVal::Bool(v) => query.bind(*v),
                PgVal::Bytes(v) => query.bind(v.clone()),
            };
        }
        query.execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
    Ok(())
}

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
    let pool = target.pool();

    let mut report = MigrationReport {
        rows_per_table: Vec::new(),
    };
    for table in TABLE_ORDER {
        let columns = sqlite_table_columns(&source, table)?;
        if columns.is_empty() {
            return Err(StorageError::Unsupported(
                "source database is missing a cp_* table",
            ));
        }
        let rows = sqlite_read_table(&source, table, &columns)?;
        for chunk in rows.chunks(CHUNK) {
            pg_insert_chunk(pool, table, &columns, chunk).await?;
        }
        let expected = sqlite_count_rows(&source, table)?;
        let actual = pg_count(pool, table).await?;
        if expected != actual {
            return Err(StorageError::Unsupported(
                "row count mismatch after migration",
            ));
        }
        report.rows_per_table.push((table.to_string(), expected));
    }

    for (table, column) in IDENTITY_COLUMNS {
        sqlx::query(&format!(
            "SELECT setval(pg_get_serial_sequence('{table}', '{column}'), COALESCE((SELECT MAX({column}) FROM {table}), 0) + 1, false)"
        ))
        .execute(pool)
        .await?;
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    #[test]
    fn table_order_puts_referenced_tables_first() {
        let order = super::TABLE_ORDER;
        let nodes = order.iter().position(|t| *t == "cp_nodes").unwrap();
        let jobs = order.iter().position(|t| *t == "cp_jobs").unwrap();
        let config = order
            .iter()
            .position(|t| *t == "cp_config_versions")
            .unwrap();
        let rollouts = order.iter().position(|t| *t == "cp_rollouts").unwrap();
        let targets = order
            .iter()
            .position(|t| *t == "cp_rollout_targets")
            .unwrap();
        assert!(nodes < jobs);
        assert!(config < rollouts);
        assert!(rollouts < targets);
    }

    /// Gated live migration test: populates an in-memory SQLite store through
    /// the contract, migrates, and verifies the report and round-trip counts.
    #[tokio::test]
    async fn migrates_sqlite_data_into_postgres() {
        let url = match std::env::var("ARKFLOW_TEST_POSTGRES_URL") {
            Ok(url) => url,
            Err(_) => {
                eprintln!("skipping: ARKFLOW_TEST_POSTGRES_URL not set");
                return;
            }
        };
        use super::super::{ControlPlaneStore, StorageBackend};
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
            },
        )
        .await
        .unwrap();
        let _ = std::fs::remove_file(&path);

        let report = super::migrate_sqlite_to_postgres(&path, &url).await.unwrap();
        assert!(report.total_rows() >= 1);
        let jobs = report
            .rows_per_table
            .iter()
            .find(|(table, _)| table == "cp_jobs")
            .unwrap();
        assert!(jobs.1 >= 1);

        let target = ControlPlaneStore::open(&url).await.unwrap();
        let job = StorageBackend::get_job(&target, "migrate-job")
            .await
            .unwrap()
            .expect("migrated job present");
        assert_eq!(job.generation, 3);
    }
}
