//! PostgreSQL backend for the Hub control-plane store (sqlx).
//!
//! The implementation deliberately mirrors the rusqlite call shapes of the
//! SQLite backend (query_row/execute/query_all + row.get(idx)) so both
//! backends share the same SQL text verbatim: `q()` rewrites the SQLite
//! `?N` placeholder style to PostgreSQL `$N` and `INSERT OR IGNORE` to
//! `ON CONFLICT DO NOTHING` at runtime. The FIFO actor serializes calls,
//! so per-statement semantics match SQLite's single-writer behavior.
use super::*;
use sqlx::postgres::{PgArguments, PgPool, PgPoolOptions, PgRow};
use sqlx::postgres::PgQueryResult;
use sqlx::Arguments;
use sqlx::{Postgres, Row as SqlxRow};
use std::time::Duration;

/// Runtime SQLite-dialect to PostgreSQL-dialect SQL rewrite: `?N` -> `$N`,
/// `INSERT OR IGNORE INTO` -> `INSERT INTO ... ON CONFLICT DO NOTHING`.
pub(crate) fn q(sql: &str) -> String {
    if let Some(rest) = sql.strip_prefix("INSERT OR IGNORE INTO") {
        return format!("INSERT INTO{} ON CONFLICT DO NOTHING", q(rest));
    }
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'?' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            out.push('$');
        } else {
            out.push(bytes[i] as char);
        }
        i += 1;
    }
    out
}

/// A bindable value. `u64` values are stored as BIGINT via `i64` casts
/// (PostgreSQL has no unsigned integers); reads cast back.
#[derive(Clone)]
pub(crate) enum PgVal {
    /// Typed NULL: bound as text. Every NULL produced from an `Option`
    /// numeric field must use `NullInt` instead — a text-typed parameter
    /// sent to a bigint column fails PostgreSQL's strict prepared-statement
    /// type check (SQLSTATE 42804).
    Null,
    /// Typed NULL bound as bigint, for `Option<i64/u64/u32>` fields.
    NullInt,
    Text(String),
    Int(i64),
    Real(f64),
    Bool(bool),
    Bytes(Vec<u8>),
}

impl From<String> for PgVal {
    fn from(value: String) -> Self {
        PgVal::Text(value)
    }
}
impl From<&String> for PgVal {
    fn from(value: &String) -> Self {
        PgVal::Text(value.clone())
    }
}
impl From<&str> for PgVal {
    fn from(value: &str) -> Self {
        PgVal::Text(value.to_owned())
    }
}
impl From<Option<String>> for PgVal {
    fn from(value: Option<String>) -> Self {
        value.map(PgVal::Text).unwrap_or(PgVal::Null)
    }
}
impl From<Option<&str>> for PgVal {
    fn from(value: Option<&str>) -> Self {
        value.map(|v| PgVal::Text(v.to_owned())).unwrap_or(PgVal::Null)
    }
}
impl From<&Option<String>> for PgVal {
    fn from(value: &Option<String>) -> Self {
        value.clone().map(PgVal::Text).unwrap_or(PgVal::Null)
    }
}
impl From<&Option<&str>> for PgVal {
    fn from(value: &Option<&str>) -> Self {
        value.map(|v| PgVal::Text((*v).to_owned())).unwrap_or(PgVal::Null)
    }
}
impl From<&Option<u64>> for PgVal {
    fn from(value: &Option<u64>) -> Self {
        value.map(|v| PgVal::Int(v as i64)).unwrap_or(PgVal::NullInt)
    }
}
impl From<&Option<i64>> for PgVal {
    fn from(value: &Option<i64>) -> Self {
        value.map(PgVal::Int).unwrap_or(PgVal::NullInt)
    }
}
impl From<&Option<u32>> for PgVal {
    fn from(value: &Option<u32>) -> Self {
        value.map(|v| PgVal::Int(v as i64)).unwrap_or(PgVal::NullInt)
    }
}
impl From<&u64> for PgVal {
    fn from(value: &u64) -> Self {
        PgVal::Int(*value as i64)
    }
}
impl From<&i64> for PgVal {
    fn from(value: &i64) -> Self {
        PgVal::Int(*value)
    }
}
impl From<&u32> for PgVal {
    fn from(value: &u32) -> Self {
        PgVal::Int(*value as i64)
    }
}
impl From<&i32> for PgVal {
    fn from(value: &i32) -> Self {
        PgVal::Int(*value as i64)
    }
}
impl From<&usize> for PgVal {
    fn from(value: &usize) -> Self {
        PgVal::Int(*value as i64)
    }
}
impl From<&f64> for PgVal {
    fn from(value: &f64) -> Self {
        PgVal::Real(*value)
    }
}
impl From<&bool> for PgVal {
    fn from(value: &bool) -> Self {
        PgVal::Bool(*value)
    }
}
impl From<&Vec<u8>> for PgVal {
    fn from(value: &Vec<u8>) -> Self {
        PgVal::Bytes(value.clone())
    }
}
impl From<&&str> for PgVal {
    fn from(value: &&str) -> Self {
        PgVal::Text((*value).to_owned())
    }
}
impl From<&&String> for PgVal {
    fn from(value: &&String) -> Self {
        PgVal::Text((*value).clone())
    }
}
impl From<&&Option<String>> for PgVal {
    fn from(value: &&Option<String>) -> Self {
        value.as_ref().map(|v| PgVal::Text(v.clone())).unwrap_or(PgVal::Null)
    }
}
impl From<&Option<Option<String>>> for PgVal {
    fn from(value: &Option<Option<String>>) -> Self {
        match value {
            Some(Some(v)) => PgVal::Text(v.clone()),
            _ => PgVal::Null,
        }
    }
}
impl From<i64> for PgVal {
    fn from(value: i64) -> Self {
        PgVal::Int(value)
    }
}
impl From<u64> for PgVal {
    fn from(value: u64) -> Self {
        PgVal::Int(value as i64)
    }
}
impl From<u32> for PgVal {
    fn from(value: u32) -> Self {
        PgVal::Int(value as i64)
    }
}
impl From<i32> for PgVal {
    fn from(value: i32) -> Self {
        PgVal::Int(value as i64)
    }
}
impl From<usize> for PgVal {
    fn from(value: usize) -> Self {
        PgVal::Int(value as i64)
    }
}
impl From<f64> for PgVal {
    fn from(value: f64) -> Self {
        PgVal::Real(value)
    }
}
impl From<bool> for PgVal {
    fn from(value: bool) -> Self {
        PgVal::Bool(value)
    }
}
impl From<Vec<u8>> for PgVal {
    fn from(value: Vec<u8>) -> Self {
        PgVal::Bytes(value)
    }
}

macro_rules! binds {
    ($($e:expr),* $(,)?) => {
        &[$(PgVal::from(&$e)),*]
    };
}

fn push_arguments(vals: &[PgVal]) -> PgArguments {
    let mut args = PgArguments::default();
    for val in vals {
        let _ = match val {
            PgVal::Null => args.add(Option::<String>::None),
            PgVal::NullInt => args.add(Option::<i64>::None),
            PgVal::Text(s) => args.add(s.clone()),
            PgVal::Int(v) => args.add(*v),
            PgVal::Real(v) => args.add(*v),
            PgVal::Bool(v) => args.add(*v),
            PgVal::Bytes(v) => args.add(v.clone()),
        };
    }
    args
}

/// Decoded column access mirroring `rusqlite::Row::get` by index.
pub(crate) trait PgGet: Sized {
    fn pg_get(row: &PgRow, idx: usize) -> Result<Self, StorageError>;
}

macro_rules! pg_get {
    ($($t:ty),* $(,)?) => {
        $(impl PgGet for $t {
            fn pg_get(row: &PgRow, idx: usize) -> Result<Self, StorageError> {
                row.try_get::<$t, _>(idx).map_err(StorageError::from)
            }
        })*
    };
}
pg_get!(String, i64, f64, bool, Vec<u8>);
impl PgGet for u64 {
    fn pg_get(row: &PgRow, idx: usize) -> Result<Self, StorageError> {
        Ok(row.try_get::<i64, _>(idx).map_err(StorageError::from)? as u64)
    }
}
impl PgGet for u32 {
    fn pg_get(row: &PgRow, idx: usize) -> Result<Self, StorageError> {
        Ok(row.try_get::<i64, _>(idx).map_err(StorageError::from)? as u32)
    }
}
impl PgGet for Option<String> {
    fn pg_get(row: &PgRow, idx: usize) -> Result<Self, StorageError> {
        row.try_get::<Option<String>, _>(idx).map_err(StorageError::from)
    }
}
impl PgGet for Option<i64> {
    fn pg_get(row: &PgRow, idx: usize) -> Result<Self, StorageError> {
        row.try_get::<Option<i64>, _>(idx).map_err(StorageError::from)
    }
}
impl PgGet for Option<u64> {
    fn pg_get(row: &PgRow, idx: usize) -> Result<Self, StorageError> {
        Ok(row
            .try_get::<Option<i64>, _>(idx)
            .map_err(StorageError::from)?
            .map(|v| v as u64))
    }
}

/// Row wrapper exposing `get(idx)` in the rusqlite shape.
pub(crate) struct Row<'a>(pub &'a PgRow);

impl Row<'_> {
    pub fn get<T: PgGet>(&self, idx: usize) -> Result<T, StorageError> {
        T::pg_get(self.0, idx)
    }
}

pub(crate) fn job_upgrade_from_row(row: &Row<'_>) -> Result<JobUpgradeRecord, StorageError> {
    Ok(JobUpgradeRecord {
        upgrade_id: row.get(0)?,
        job_id: row.get(1)?,
        from_version: row.get(2)?,
        to_version: row.get(3)?,
        phase: row.get(4)?,
        savepoint_id: row.get(5)?,
        target_spec_json: row.get(6)?,
        phase_deadline_at_ms: row.get(7)?,
        savepoint_retries: row.get(8)?,
        verify_timeout_ms: row.get(9)?,
        actor: row.get(10)?,
        correlation_id: row.get(11)?,
        last_error: row.get(12)?,
        paused_from: row.get(13)?,
        created_at_ms: row.get(14)?,
        updated_at_ms: row.get(15)?,
    })
}

/// Result of `query_row`: distinguish "no row" (optional) from errors.
pub(crate) struct RowResult<T>(Result<Option<T>, StorageError>);

impl<T> RowResult<T> {
    pub fn optional(self) -> Result<Option<T>, StorageError> {
        self.0
    }

    pub fn required(self) -> Result<T, StorageError> {
        match self.0 {
            Ok(Some(value)) => Ok(value),
            Ok(None) => Err(StorageError::Unsupported(
                "query returned no row where one was required",
            )),
            Err(error) => Err(error),
        }
    }
}

/// Connection wrapper mirroring the rusqlite call surface used by the
/// storage bodies: `execute`, `query_row`, and `query_all` (prepare +
/// query_map + collect). Owns its pooled connection.
pub(crate) struct PgConn {
    connection: sqlx::pool::PoolConnection<Postgres>,
}

impl PgConn {
    pub async fn execute(&mut self, sql: &str, vals: &[PgVal]) -> Result<u64, StorageError> {
        let args = push_arguments(vals);
        let result: PgQueryResult =
            sqlx::query_with(&q(sql), args).execute(&mut *self.connection).await?;
        Ok(result.rows_affected())
    }

    pub async fn query_row<T>(
        &mut self,
        sql: &str,
        vals: &[PgVal],
        map: impl Fn(&Row<'_>) -> Result<T, StorageError>,
    ) -> RowResult<T> {
        let args = push_arguments(vals);
        match sqlx::query_with(&q(sql), args).fetch_one(&mut *self.connection).await {
            Ok(row) => match map(&Row(&row)) {
                Ok(value) => RowResult(Ok(Some(value))),
                Err(error) => RowResult(Err(error)),
            },
            Err(sqlx::Error::RowNotFound) => RowResult(Ok(None)),
            Err(error) => RowResult(Err(error.into())),
        }
    }

    pub async fn query_all<T>(
        &mut self,
        sql: &str,
        vals: &[PgVal],
        map: impl Fn(&Row<'_>) -> Result<T, StorageError>,
    ) -> Result<Vec<T>, StorageError> {
        let args = push_arguments(vals);
        let rows = sqlx::query_with(&q(sql), args)
            .fetch_all(&mut *self.connection)
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(map(&Row(row))?);
        }
        Ok(out)
    }
}

#[derive(Clone)]
pub struct PostgresBackend {
    pool: PgPool,
    /// The open write-fence transaction (a `FOR SHARE` lock on the lease
    /// row), held for the duration of a fenced mutation. Shared across
    /// clones: the FIFO actor runs at most one fence at a time.
    fence: std::sync::Arc<tokio::sync::Mutex<Option<sqlx::Transaction<'static, Postgres>>>>,
}

/// Idempotent PostgreSQL DDL: one table per SQLite counterpart, BIGINT for
/// epoch-ms/counters/generations, IDENTITY for autoincrement keys, and
/// `COLLATE "C"` on TEXT primary/unique columns so uniqueness matches
/// SQLite's binary comparison byte for byte.
const PG_DDL: &str = r#"
            CREATE TABLE IF NOT EXISTS cp_nodes (
                node_id TEXT COLLATE "C" PRIMARY KEY,
                role TEXT NOT NULL DEFAULT 'compute',
                protocol_version TEXT NOT NULL DEFAULT 'v1',
                node_version TEXT NOT NULL DEFAULT 'unknown',
                state TEXT NOT NULL DEFAULT 'offline',
                capabilities_json TEXT NOT NULL DEFAULT '[]',
                boot_id TEXT,
                last_report_seq BIGINT,
                last_seen_at_ms BIGINT NOT NULL DEFAULT 0,
                lease_expires_at_ms BIGINT NOT NULL DEFAULT 0,
                maintenance_state TEXT NOT NULL DEFAULT 'active',
                maintenance_updated_at_ms BIGINT,
                created_at_ms BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cp_jobs (
                job_id TEXT COLLATE "C" PRIMARY KEY,
                version BIGINT NOT NULL,
                spec_json TEXT NOT NULL,
                desired_state TEXT NOT NULL DEFAULT 'stopped',
                observed_state TEXT NOT NULL DEFAULT 'draft',
                convergence TEXT NOT NULL DEFAULT 'unknown',
                generation BIGINT NOT NULL DEFAULT 0,
                node_ids_json TEXT NOT NULL DEFAULT '[]',
                checkpoint_id TEXT,
                last_error TEXT,
                updated_at_ms BIGINT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS cp_jobs_updated
                ON cp_jobs(updated_at_ms DESC, job_id);

            CREATE TABLE IF NOT EXISTS cp_job_versions (
                job_id TEXT COLLATE "C" NOT NULL, version BIGINT NOT NULL, spec_json TEXT NOT NULL,
                plan_json TEXT NOT NULL, created_at_ms BIGINT NOT NULL,
                PRIMARY KEY (job_id, version)
            );
            CREATE TABLE IF NOT EXISTS cp_job_tasks (
                job_id TEXT COLLATE "C" NOT NULL, generation BIGINT NOT NULL, task_id TEXT COLLATE "C" NOT NULL,
                node_id TEXT COLLATE "C" NOT NULL, attempt_id TEXT COLLATE "C" NOT NULL, state TEXT NOT NULL,
                updated_at_ms BIGINT NOT NULL, PRIMARY KEY (job_id, generation, task_id)
            );
            CREATE TABLE IF NOT EXISTS cp_job_checkpoints (
                job_id TEXT COLLATE "C" NOT NULL, job_version BIGINT NOT NULL DEFAULT 0,
                checkpoint_id TEXT COLLATE "C" NOT NULL, kind TEXT NOT NULL,
                status TEXT NOT NULL, manifest_uri TEXT, format_version BIGINT NOT NULL,
                created_at_ms BIGINT NOT NULL, updated_at_ms BIGINT NOT NULL,
                PRIMARY KEY (job_id, checkpoint_id)
            );

            CREATE TABLE IF NOT EXISTS cp_stream_desired (
                node_id TEXT COLLATE "C" NOT NULL,
                stream_id TEXT COLLATE "C" NOT NULL,
                generation BIGINT NOT NULL,
                desired_state TEXT NOT NULL,
                config_version_id TEXT COLLATE "C",
                desired_action_id TEXT COLLATE "C",
                paused BIGINT NOT NULL DEFAULT 0,
                updated_at_ms BIGINT NOT NULL,
                updated_by TEXT,
                correlation_id TEXT,
                PRIMARY KEY (node_id, stream_id)
            );

            CREATE TABLE IF NOT EXISTS cp_stream_observed (
                node_id TEXT COLLATE "C" NOT NULL,
                stream_id TEXT COLLATE "C" NOT NULL,
                boot_id TEXT,
                report_seq BIGINT,
                observed_generation BIGINT,
                observed_state TEXT NOT NULL,
                applied_config_version TEXT COLLATE "C",
                last_action_id TEXT COLLATE "C",
                active_operation_id TEXT COLLATE "C",
                last_error_code TEXT,
                last_error_message TEXT,
                snapshot_json TEXT NOT NULL DEFAULT '{}',
                observed_at_ms BIGINT NOT NULL,
                PRIMARY KEY (node_id, stream_id)
            );

            CREATE TABLE IF NOT EXISTS cp_config_versions (
                config_version_id TEXT COLLATE "C" PRIMARY KEY,
                parent_version_id TEXT COLLATE "C",
                content_digest TEXT NOT NULL,
                content_ref TEXT NOT NULL,
                format TEXT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                created_by TEXT,
                correlation_id TEXT,
                FOREIGN KEY (parent_version_id)
                    REFERENCES cp_config_versions(config_version_id)
            );

            CREATE TABLE IF NOT EXISTS cp_intents (
                intent_id TEXT COLLATE "C" PRIMARY KEY,
                node_id TEXT COLLATE "C" NOT NULL,
                stream_id TEXT COLLATE "C" NOT NULL,
                generation BIGINT NOT NULL,
                intent_type TEXT NOT NULL,
                desired_state TEXT,
                config_version_id TEXT COLLATE "C",
                action_id TEXT COLLATE "C",
                payload_json TEXT,
                state TEXT NOT NULL,
                convergence_state TEXT NOT NULL,
                retry_count BIGINT NOT NULL DEFAULT 0,
                next_retry_at_ms BIGINT,
                last_failure_class TEXT,
                last_failure_code TEXT,
                last_failure_message TEXT,
                superseded_by_intent_id TEXT COLLATE "C",
                created_at_ms BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL,
                converged_at_ms BIGINT,
                actor TEXT,
                correlation_id TEXT,
                idempotency_key TEXT COLLATE "C",
                UNIQUE (node_id, stream_id, generation),
                FOREIGN KEY (superseded_by_intent_id)
                    REFERENCES cp_intents(intent_id)
            );

            CREATE TABLE IF NOT EXISTS cp_attempts (
                attempt_id TEXT COLLATE "C" PRIMARY KEY,
                intent_id TEXT COLLATE "C" NOT NULL,
                command_id TEXT COLLATE "C" NOT NULL UNIQUE,
                node_id TEXT COLLATE "C" NOT NULL,
                stream_id TEXT COLLATE "C" NOT NULL,
                generation BIGINT NOT NULL,
                operation TEXT NOT NULL,
                state TEXT NOT NULL,
                failure_class TEXT,
                dispatched_at_ms BIGINT,
                acknowledged_at_ms BIGINT,
                started_at_ms BIGINT,
                finished_at_ms BIGINT,
                expires_at_ms BIGINT,
                error_code TEXT,
                error_message TEXT,
                created_at_ms BIGINT NOT NULL,
                FOREIGN KEY (intent_id) REFERENCES cp_intents(intent_id)
            );

            CREATE UNIQUE INDEX IF NOT EXISTS cp_one_active_attempt
                ON cp_attempts(node_id, stream_id, generation)
                WHERE state IN ('queued', 'dispatched', 'acknowledged', 'running');

            CREATE TABLE IF NOT EXISTS cp_events (
                event_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                node_id TEXT COLLATE "C",
                stream_id TEXT COLLATE "C",
                intent_id TEXT COLLATE "C",
                attempt_id TEXT COLLATE "C",
                event_type TEXT NOT NULL,
                outcome TEXT NOT NULL,
                failure_class TEXT,
                message TEXT,
                generation BIGINT,
                correlation_id TEXT,
                actor TEXT,
                occurred_at_ms BIGINT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cp_audit_events (
                event_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                actor TEXT,
                action TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                resource_id TEXT COLLATE "C",
                node_id TEXT COLLATE "C",
                stream_id TEXT COLLATE "C",
                correlation_id TEXT,
                outcome TEXT NOT NULL,
                failure_code TEXT,
                message TEXT,
                occurred_at_ms BIGINT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cp_rollouts (
                rollout_id TEXT COLLATE "C" PRIMARY KEY,
                config_version_id TEXT COLLATE "C" NOT NULL,
                state TEXT NOT NULL,
                batch_size BIGINT NOT NULL,
                current_batch BIGINT NOT NULL DEFAULT 0,
                total_targets BIGINT NOT NULL,
                actor TEXT,
                correlation_id TEXT,
                created_at_ms BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL,
                FOREIGN KEY (config_version_id)
                    REFERENCES cp_config_versions(config_version_id)
            );

            CREATE TABLE IF NOT EXISTS cp_rollout_targets (
                rollout_id TEXT COLLATE "C" NOT NULL,
                node_id TEXT COLLATE "C" NOT NULL,
                ordinal BIGINT NOT NULL,
                state TEXT NOT NULL,
                attempt_id TEXT COLLATE "C",
                error TEXT,
                observed_config_version TEXT COLLATE "C",
                updated_at_ms BIGINT NOT NULL,
                PRIMARY KEY (rollout_id, node_id),
                FOREIGN KEY (rollout_id)
                    REFERENCES cp_rollouts(rollout_id)
            );

            CREATE TABLE IF NOT EXISTS cp_job_upgrades (
                upgrade_id TEXT COLLATE "C" PRIMARY KEY,
                job_id TEXT COLLATE "C" NOT NULL,
                from_version BIGINT NOT NULL,
                to_version BIGINT NOT NULL,
                phase TEXT NOT NULL,
                savepoint_id TEXT COLLATE "C",
                target_spec_json TEXT NOT NULL,
                phase_deadline_at_ms BIGINT NOT NULL,
                savepoint_retries BIGINT NOT NULL DEFAULT 0,
                verify_timeout_ms BIGINT NOT NULL DEFAULT 0,
                actor TEXT,
                correlation_id TEXT,
                last_error TEXT,
                paused_from TEXT,
                created_at_ms BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS cp_job_upgrades_phase
                ON cp_job_upgrades(phase, created_at_ms);
            CREATE INDEX IF NOT EXISTS cp_job_upgrades_job
                ON cp_job_upgrades(job_id, created_at_ms);

            CREATE TABLE IF NOT EXISTS cp_operations (
                operation_id TEXT COLLATE "C" PRIMARY KEY,
                node_id TEXT COLLATE "C" NOT NULL,
                resource_id TEXT COLLATE "C" NOT NULL,
                operation TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL,
                operation_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cp_outbox (
                outbox_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                event_key TEXT COLLATE "C" NOT NULL UNIQUE,
                event_type TEXT NOT NULL,
                node_id TEXT COLLATE "C" NOT NULL,
                stream_id TEXT COLLATE "C",
                intent_id TEXT COLLATE "C",
                available_at_ms BIGINT NOT NULL,
                claimed_at_ms BIGINT,
                worker_id TEXT COLLATE "C",
                processed_at_ms BIGINT,
                created_at_ms BIGINT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cp_hub_lease (
                id BIGINT PRIMARY KEY CHECK (id = 1),
                holder TEXT NOT NULL DEFAULT '',
                epoch BIGINT NOT NULL DEFAULT 0,
                expires_at_ms BIGINT NOT NULL DEFAULT 0,
                updated_at_ms BIGINT NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS cp_intents_due
                ON cp_intents(state, next_retry_at_ms);
            CREATE INDEX IF NOT EXISTS cp_attempts_pending
                ON cp_attempts(state, expires_at_ms);
            CREATE INDEX IF NOT EXISTS cp_events_resource
                ON cp_events(node_id, stream_id, occurred_at_ms);
            CREATE INDEX IF NOT EXISTS cp_audit_resource
                ON cp_audit_events(resource_id, occurred_at_ms);
            CREATE INDEX IF NOT EXISTS cp_rollout_targets_state
                ON cp_rollout_targets(rollout_id, state, ordinal);
            CREATE INDEX IF NOT EXISTS cp_operations_node_created
                ON cp_operations(node_id, created_at_ms DESC);
            CREATE INDEX IF NOT EXISTS cp_outbox_ready
                ON cp_outbox(processed_at_ms, available_at_ms);
            CREATE UNIQUE INDEX IF NOT EXISTS cp_intents_idempotency ON cp_intents(node_id, stream_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
"#;

impl PostgresBackend {
    pub(crate) fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Open a pool with a startup connectivity probe (SELECT 1) so a bad URL
    /// fails Hub startup instead of surfacing on the first command.
    pub(crate) async fn open(url: &str) -> Result<Self, StorageError> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await?;
        sqlx::query("SELECT 1").execute(&pool).await?;
        sqlx::raw_sql(PG_DDL).execute(&pool).await?;
        Ok(Self {
            pool,
            fence: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
        })
    }

    async fn lease(&self) -> Result<PgConn, StorageError> {
        Ok(PgConn {
            connection: self.pool.acquire().await?,
        })
    }

    async fn begin(&self) -> Result<PgTx, StorageError> {
        Ok(PgTx {
            transaction: self.pool.begin().await?,
        })
    }
}

/// Transaction wrapper with the same call surface as `PgConn`, mirroring
/// the SQLite backend's `immediate_transaction` bodies.
pub(crate) struct PgTx {
    transaction: sqlx::Transaction<'static, Postgres>,
}

impl PgTx {
    pub async fn commit(self) -> Result<(), StorageError> {
        self.transaction.commit().await?;
        Ok(())
    }

    pub async fn execute(&mut self, sql: &str, vals: &[PgVal]) -> Result<u64, StorageError> {
        let args = push_arguments(vals);
        let result: PgQueryResult =
            sqlx::query_with(&q(sql), args).execute(&mut *self.transaction).await?;
        Ok(result.rows_affected())
    }

    pub async fn query_row<T>(
        &mut self,
        sql: &str,
        vals: &[PgVal],
        map: impl Fn(&Row<'_>) -> Result<T, StorageError>,
    ) -> RowResult<T> {
        let args = push_arguments(vals);
        match sqlx::query_with(&q(sql), args).fetch_one(&mut *self.transaction).await {
            Ok(row) => match map(&Row(&row)) {
                Ok(value) => RowResult(Ok(Some(value))),
                Err(error) => RowResult(Err(error)),
            },
            Err(sqlx::Error::RowNotFound) => RowResult(Ok(None)),
            Err(error) => RowResult(Err(error.into())),
        }
    }

    pub async fn query_all<T>(
        &mut self,
        sql: &str,
        vals: &[PgVal],
        map: impl Fn(&Row<'_>) -> Result<T, StorageError>,
    ) -> Result<Vec<T>, StorageError> {
        let args = push_arguments(vals);
        let rows = sqlx::query_with(&q(sql), args)
            .fetch_all(&mut *self.transaction)
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(map(&Row(row))?);
        }
        Ok(out)
    }
}

include!("postgres_methods.rs");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_placeholders_and_insert_or_ignore() {
        assert_eq!(
            q("SELECT generation FROM cp_jobs WHERE job_id = ?1 AND version > ?2"),
            "SELECT generation FROM cp_jobs WHERE job_id = $1 AND version > $2"
        );
        assert_eq!(
            q("INSERT OR IGNORE INTO cp_config_versions (config_version_id) VALUES (?1)"),
            "INSERT INTO cp_config_versions (config_version_id) VALUES ($1) ON CONFLICT DO NOTHING"
        );
        // Non-placeholder question marks (e.g. inside literals) are untouched.
        assert_eq!(q("SELECT 'a?b'"), "SELECT 'a?b'");
    }

    /// The bind surface pins its typed-NULL contracts: every Option numeric
    /// must bind as a bigint-typed NULL (NullInt), never a text-typed one —
    /// PostgreSQL's prepared-statement type check rejects a text-typed
    /// parameter against a BIGINT column (SQLSTATE 42804).
    #[test]
    fn pg_val_from_impls_pin_the_typed_null_contracts() {
        let text = String::from("text");
        let optional_text = Some(String::from("opt"));
        // Text shapes, owned and borrowed.
        assert!(matches!(PgVal::from(&text), PgVal::Text(_)));
        assert!(matches!(PgVal::from(text.clone()), PgVal::Text(_)));
        assert!(matches!(PgVal::from(&text[..]), PgVal::Text(_)));
        assert!(matches!(PgVal::from(&optional_text), PgVal::Text(_)));
        assert!(matches!(PgVal::from(optional_text.clone()), PgVal::Text(_)));
        let text_reference = &text;
        assert!(matches!(PgVal::from(&text_reference), PgVal::Text(_)));
        let slice_reference = &text[..];
        assert!(matches!(PgVal::from(&slice_reference), PgVal::Text(_)));
        let optional_reference = &optional_text;
        assert!(matches!(PgVal::from(&optional_reference), PgVal::Text(_)));
        // Untyped NULLs for text columns.
        assert!(matches!(PgVal::from(None::<String>), PgVal::Null));
        assert!(matches!(PgVal::from(None::<&str>), PgVal::Null));
        assert!(matches!(PgVal::from(&None::<String>), PgVal::Null));
        assert!(matches!(PgVal::from(&None::<&str>), PgVal::Null));
        let nested = Some(Some(String::from("nested")));
        let inner_none = Some(None);
        let outer_none: Option<Option<String>> = None;
        assert!(matches!(PgVal::from(&nested), PgVal::Text(_)));
        assert!(matches!(PgVal::from(&inner_none), PgVal::Null));
        assert!(matches!(PgVal::from(&outer_none), PgVal::Null));
        // Typed NULLs for numeric columns.
        let none_u64: Option<u64> = None;
        let none_i64: Option<i64> = None;
        let none_u32: Option<u32> = None;
        assert!(matches!(PgVal::from(&none_u64), PgVal::NullInt));
        assert!(matches!(PgVal::from(&none_i64), PgVal::NullInt));
        assert!(matches!(PgVal::from(&none_u32), PgVal::NullInt));
        // Integer shapes (u64 values travel as BIGINT via i64 casts).
        assert!(matches!(PgVal::from(&7u64), PgVal::Int(7)));
        assert!(matches!(PgVal::from(&7i64), PgVal::Int(7)));
        assert!(matches!(PgVal::from(&7u32), PgVal::Int(7)));
        assert!(matches!(PgVal::from(&7i32), PgVal::Int(7)));
        assert!(matches!(PgVal::from(&7usize), PgVal::Int(7)));
        assert!(matches!(PgVal::from(7u64), PgVal::Int(7)));
        assert!(matches!(PgVal::from(7i64), PgVal::Int(7)));
        assert!(matches!(PgVal::from(7u32), PgVal::Int(7)));
        assert!(matches!(PgVal::from(7i32), PgVal::Int(7)));
        assert!(matches!(PgVal::from(7usize), PgVal::Int(7)));
        // Real, boolean and byte shapes.
        assert!(matches!(PgVal::from(&1.5f64), PgVal::Real(value) if value == 1.5));
        assert!(matches!(PgVal::from(1.5f64), PgVal::Real(value) if value == 1.5));
        assert!(matches!(PgVal::from(&true), PgVal::Bool(true)));
        assert!(matches!(PgVal::from(false), PgVal::Bool(false)));
        let bytes = vec![1u8, 2];
        assert!(matches!(PgVal::from(&bytes), PgVal::Bytes(_)));
        assert!(matches!(PgVal::from(bytes.clone()), PgVal::Bytes(_)));
    }

    /// `RowResult::required` distinguishes a missing row (a contract
    /// violation worth surfacing) from a propagated storage failure, while
    /// `optional` passes both apart.
    #[test]
    fn row_result_required_distinguishes_missing_from_failure() {
        let missing: RowResult<i64> = RowResult(Ok(None));
        assert!(matches!(
            missing.required(),
            Err(StorageError::Unsupported(_))
        ));
        let absent: RowResult<i64> = RowResult(Ok(None));
        assert_eq!(absent.optional().unwrap(), None);
        let present: RowResult<i64> = RowResult(Ok(Some(3)));
        assert_eq!(present.required().unwrap(), 3);
        let failed: RowResult<i64> = RowResult(Err(StorageError::Unsupported("probe")));
        assert!(failed.required().is_err());
        let failed: RowResult<i64> = RowResult(Err(StorageError::Unsupported("probe")));
        assert!(failed.optional().is_err());
    }

    /// Decode battery: exercise every column shape the row accessors support
    /// (text, bigint, double, boolean, bytea, narrowed u32/u64 reads, and
    /// typed NULLs) through a real PostgreSQL round trip.
    #[tokio::test]
    async fn pg_row_decode_covers_the_column_type_surface() {
        if std::env::var("ARKFLOW_TEST_POSTGRES_URL").is_err() {
            eprintln!("skipping: ARKFLOW_TEST_POSTGRES_URL not set");
            return;
        }
        let url = super::contract_database_url("pg_row_decode_types").await;
        let backend = PostgresBackend::open(&url).await.unwrap();
        let mut connection = backend.lease().await.unwrap();
        let (text, bigint, real, flag, bytes): (String, i64, f64, bool, Vec<u8>) = connection
            .query_row(
                "SELECT ?1::text, ?2::bigint, ?3::double precision, ?4::boolean, ?5::bytea",
                &[
                    PgVal::from("txt"),
                    PgVal::from(42i64),
                    PgVal::from(1.5f64),
                    PgVal::from(true),
                    PgVal::from(vec![1u8, 2, 3]),
                ],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .await
            .required()
            .unwrap();
        assert_eq!(
            (text.as_str(), bigint, flag, bytes.as_slice()),
            ("txt", 42, true, &[1u8, 2, 3][..])
        );
        assert!((real - 1.5).abs() < f64::EPSILON);
        let (narrow, wide, opt_text, opt_bigint, opt_wide): (
            u32,
            u64,
            Option<String>,
            Option<i64>,
            Option<u64>,
        ) = connection
            .query_row(
                "SELECT ?1::bigint, ?2::bigint, ?3::text, ?4::bigint, ?5::bigint",
                &[
                    PgVal::from(7i32),
                    PgVal::from(9u64),
                    PgVal::Null,
                    PgVal::NullInt,
                    PgVal::NullInt,
                ],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .await
            .required()
            .unwrap();
        assert_eq!((narrow, wide), (7, 9));
        assert_eq!((opt_text, opt_bigint, opt_wide), (None, None, None));
    }

    /// The contract battery every backend must satisfy. The SQLite suite in
    /// `mod.rs` runs the full 29-test surface offline; this battery covers the
    /// cross-backend invariants called out by the hub-ha spec (idempotency-key
    /// reuse, job round-trip, outbox claim, audit append, aggregates) so a
    /// PostgreSQL instance passing it demonstrates contract parity.
    #[tokio::test]
    async fn postgres_contract_suite() {
        if std::env::var("ARKFLOW_TEST_POSTGRES_URL").is_err() {
            eprintln!("skipping: ARKFLOW_TEST_POSTGRES_URL not set");
            return;
        }
        let url = super::contract_database_url("pg_contract_suite").await;
        let store = ControlPlaneStore::open(&url).await.unwrap();
        let storage = super::super::StorageActor::start(store, 8);

        // Intent idempotency: same key + same payload replays the record.
        let mutation = DesiredMutation {
            node_id: "pg-node".into(),
            stream_id: "pg-stream".into(),
            desired_state: "running".into(),
            idempotency_key: Some("pg-key-1".into()),
            ..Default::default()
        };
        let first = storage.set_desired(mutation.clone()).await.unwrap();
        let second = storage.set_desired(mutation).await.unwrap();
        assert_eq!(first.intent_id, second.intent_id);

        // Same key + different payload is the domain reuse error.
        let conflict = DesiredMutation {
            node_id: "pg-node".into(),
            stream_id: "pg-stream".into(),
            desired_state: "stopped".into(),
            idempotency_key: Some("pg-key-1".into()),
            ..Default::default()
        };
        let conflict_error = storage.set_desired(conflict).await.unwrap_err();
        assert!(
            matches!(conflict_error, StorageError::IdempotencyKeyReused),
            "{conflict_error:?}"
        );



        // Job round-trip preserves the record and bumps generation.
        let job = storage
            .upsert_job(JobRecord {
                job_id: "pg-job".into(),
                version: 1,
                spec_json: "{}".into(),
                desired_state: "running".into(),
                observed_state: "draft".into(),
                convergence: "unknown".into(),
                generation: 0,
                node_ids: vec!["pg-node".into()],
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 1,
            })
            .await
            .unwrap();
        // A brand-new job starts at generation 1 (max(passed, 1)) in both backends.
        assert_eq!(job.generation, 1);
        let loaded = storage.get_job("pg-job").await.unwrap().unwrap();
        assert_eq!(loaded.node_ids, vec!["pg-node".to_string()]);

        // Optimistic concurrency: matching expectation bumps the generation,
        // a stale one conflicts.
        let mut updated = loaded.clone();
        updated.generation += 1;
        let bumped = storage
            .update_job_with_expected_generation(updated, loaded.generation)
            .await
            .unwrap();
        assert_eq!(bumped.generation, loaded.generation + 1);
        let stale = storage
            .update_job_with_expected_generation(bumped.clone(), loaded.generation)
            .await
            .unwrap_err();
        assert!(
            matches!(stale, StorageError::GenerationConflict { .. }),
            "{stale:?}"
        );

        // Audit append allocates identity ids.
        let audit_id = storage
            .record_audit(AuditRecord {
                event_id: 0,
                actor: Some("operator".into()),
                action: "job.start".into(),
                resource_type: "job".into(),
                resource_id: Some("pg-job".into()),
                node_id: None,
                stream_id: None,
                correlation_id: None,
                outcome: "accepted".into(),
                failure_code: None,
                message: None,
                occurred_at_ms: 42,
            })
            .await
            .unwrap();
        assert!(audit_id > 0);

        // Outbox claim is exclusive and observable through aggregates.
        let claimed = storage.claim_outbox("worker-pg", 4_102_444_800_000).await.unwrap();
        assert!(claimed.is_some());
        let aggregates = storage.operational_aggregates(4_102_444_800_000).await.unwrap();
        assert!(aggregates.outbox_pending >= 1);

        // Hub-lease contract parity with the SQLite suite. Epochs are
        // asserted relative to the first acquired value so the battery also
        // passes against a database that retains earlier lease takeovers.
        let run = format!("pg-hub-{}", now_ms());
        let now = now_ms();
        let ttl = 60_000u64;
        let acquired = storage.try_acquire_hub_lease(&run, ttl, now).await.unwrap();
        assert!(matches!(acquired, HubLeaseAcquire::Acquired { .. }), "{acquired:?}");
        let HubLeaseAcquire::Acquired { epoch } = acquired else { unreachable!() };
        assert_eq!(
            storage.try_acquire_hub_lease("pg-other", ttl, now + 1).await.unwrap(),
            HubLeaseAcquire::HeldByOther(HubLeaseSnapshot {
                holder: run.clone(),
                epoch,
                expires_at_ms: now + ttl,
            })
        );
        assert_eq!(
            storage.renew_hub_lease(&run, ttl, now + 2).await.unwrap(),
            HubLeaseRenew::Renewed { epoch }
        );
        assert_eq!(
            storage.renew_hub_lease("pg-other", ttl, now + 2).await.unwrap(),
            HubLeaseRenew::Lost
        );
        assert_eq!(
            storage.try_acquire_hub_lease(&run, ttl, now + 3).await.unwrap(),
            HubLeaseAcquire::Acquired { epoch }
        );
        assert!(storage.release_hub_lease(&run, now + 4).await.unwrap());
        assert!(!storage.release_hub_lease(&run, now + 5).await.unwrap());
        assert_eq!(
            storage.try_acquire_hub_lease("pg-other", ttl, now + 6).await.unwrap(),
            HubLeaseAcquire::Acquired { epoch: epoch + 1 }
        );
        assert!(storage.release_hub_lease("pg-other", now + 7).await.unwrap());
    }
}
