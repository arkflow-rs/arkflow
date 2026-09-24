//! PostgreSQL backend for the control-plane storage (Hub HA phase 1).
//!
//! Mirrors [`crate::storage::ControlPlaneStore`]'s synchronous rusqlite
//! surface over `sqlx`. The schema is created idempotently at startup; the
//! storage actor still serialises commands, so ordering semantics match the
//! SQLite backend exactly.

use std::sync::atomic::Ordering;
use std::time::Duration;

use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::Row;

use crate::storage::{
    AuditRecord, AttemptRecord, DesiredMutation, DesiredRecord, IntentRecord, JobCheckpointRecord,
    JobRecord, JobVersionRecord, NodeMaintenanceMutation, NodeMutation, ObservedMutation,
    OperationalAggregates, OutboxRecord, PersistedOperation, RolloutRecord, RolloutTargetRecord,
    RolloutTargetUpdate, StorageError, StoredEvent,
};

/// Idempotent schema, transliterated 1:1 from the SQLite DDL:
/// `INTEGER PRIMARY KEY AUTOINCREMENT` becomes identity columns and the
/// conditional unique index carries over unchanged.
pub const PG_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS cp_nodes (
    node_id TEXT PRIMARY KEY,
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
    job_id TEXT PRIMARY KEY,
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
CREATE INDEX IF NOT EXISTS cp_jobs_updated ON cp_jobs(updated_at_ms DESC, job_id);
CREATE TABLE IF NOT EXISTS cp_job_versions (
    job_id TEXT NOT NULL, version BIGINT NOT NULL, spec_json TEXT NOT NULL,
    plan_json TEXT NOT NULL, created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (job_id, version)
);
CREATE TABLE IF NOT EXISTS cp_job_tasks (
    job_id TEXT NOT NULL, generation BIGINT NOT NULL, task_id TEXT NOT NULL,
    node_id TEXT NOT NULL, attempt_id TEXT NOT NULL, state TEXT NOT NULL,
    updated_at_ms BIGINT NOT NULL, PRIMARY KEY (job_id, generation, task_id)
);
CREATE TABLE IF NOT EXISTS cp_job_checkpoints (
    job_id TEXT NOT NULL, job_version BIGINT NOT NULL DEFAULT 0,
    checkpoint_id TEXT NOT NULL, kind TEXT NOT NULL,
    status TEXT NOT NULL, manifest_uri TEXT, format_version BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL, updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (job_id, checkpoint_id)
);
CREATE TABLE IF NOT EXISTS cp_stream_desired (
    node_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    generation BIGINT NOT NULL,
    desired_state TEXT NOT NULL,
    config_version_id TEXT,
    desired_action_id TEXT,
    paused BIGINT NOT NULL DEFAULT 0,
    updated_at_ms BIGINT NOT NULL,
    updated_by TEXT,
    correlation_id TEXT,
    PRIMARY KEY (node_id, stream_id)
);
CREATE TABLE IF NOT EXISTS cp_stream_observed (
    node_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    boot_id TEXT,
    report_seq BIGINT,
    observed_generation BIGINT,
    observed_state TEXT NOT NULL,
    applied_config_version TEXT,
    last_action_id TEXT,
    active_operation_id TEXT,
    last_error_code TEXT,
    last_error_message TEXT,
    snapshot_json TEXT NOT NULL DEFAULT '{}',
    observed_at_ms BIGINT NOT NULL,
    PRIMARY KEY (node_id, stream_id)
);
CREATE TABLE IF NOT EXISTS cp_config_versions (
    config_version_id TEXT PRIMARY KEY,
    parent_version_id TEXT,
    content_digest TEXT NOT NULL,
    content_ref TEXT NOT NULL,
    format TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    created_by TEXT,
    correlation_id TEXT,
    FOREIGN KEY (parent_version_id) REFERENCES cp_config_versions(config_version_id)
);
CREATE TABLE IF NOT EXISTS cp_intents (
    intent_id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    generation BIGINT NOT NULL,
    intent_type TEXT NOT NULL,
    desired_state TEXT,
    config_version_id TEXT,
    action_id TEXT,
    payload_json TEXT,
    state TEXT NOT NULL,
    convergence_state TEXT NOT NULL,
    retry_count BIGINT NOT NULL DEFAULT 0,
    next_retry_at_ms BIGINT,
    last_failure_class TEXT,
    last_failure_code TEXT,
    last_failure_message TEXT,
    superseded_by_intent_id TEXT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    converged_at_ms BIGINT,
    actor TEXT,
    correlation_id TEXT,
    idempotency_key TEXT,
    UNIQUE (node_id, stream_id, generation),
    FOREIGN KEY (superseded_by_intent_id) REFERENCES cp_intents(intent_id)
);
CREATE TABLE IF NOT EXISTS cp_attempts (
    attempt_id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL,
    command_id TEXT NOT NULL UNIQUE,
    node_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
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
    node_id TEXT,
    stream_id TEXT,
    intent_id TEXT,
    attempt_id TEXT,
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
    resource_id TEXT,
    node_id TEXT,
    stream_id TEXT,
    correlation_id TEXT,
    outcome TEXT NOT NULL,
    failure_code TEXT,
    message TEXT,
    occurred_at_ms BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS cp_rollouts (
    rollout_id TEXT PRIMARY KEY,
    config_version_id TEXT NOT NULL,
    state TEXT NOT NULL,
    batch_size BIGINT NOT NULL,
    current_batch BIGINT NOT NULL DEFAULT 0,
    total_targets BIGINT NOT NULL,
    actor TEXT,
    correlation_id TEXT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    FOREIGN KEY (config_version_id) REFERENCES cp_config_versions(config_version_id)
);
CREATE TABLE IF NOT EXISTS cp_rollout_targets (
    rollout_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    ordinal BIGINT NOT NULL,
    state TEXT NOT NULL,
    attempt_id TEXT,
    error TEXT,
    observed_config_version TEXT,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (rollout_id, node_id),
    FOREIGN KEY (rollout_id) REFERENCES cp_rollouts(rollout_id)
);
CREATE TABLE IF NOT EXISTS cp_operations (
    operation_id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    operation_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cp_outbox (
    outbox_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    event_key TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    node_id TEXT NOT NULL,
    stream_id TEXT,
    intent_id TEXT,
    available_at_ms BIGINT NOT NULL,
    claimed_at_ms BIGINT,
    worker_id TEXT,
    processed_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS cp_intents_due ON cp_intents(state, next_retry_at_ms);
CREATE INDEX IF NOT EXISTS cp_attempts_pending ON cp_attempts(state, expires_at_ms);
CREATE INDEX IF NOT EXISTS cp_events_resource ON cp_events(node_id, stream_id, occurred_at_ms);
CREATE INDEX IF NOT EXISTS cp_audit_resource ON cp_audit_events(resource_id, occurred_at_ms);
CREATE INDEX IF NOT EXISTS cp_rollout_targets_state ON cp_rollout_targets(rollout_id, state, ordinal);
"#;

/// PostgreSQL control-plane store.
pub struct PgStore {
    pool: sqlx::PgPool,
}


fn pg_row_to_job(row: &PgRow) -> Result<JobRecord, StorageError> {
    let node_ids_json: String = row.try_get(7)?;
    let node_ids = serde_json::from_str(&node_ids_json)
        .map_err(|error| StorageError::Unsupported(format!("node_ids_json decode failed: {error}")))?;
    Ok(JobRecord {
        job_id: row.try_get(0)?,
        version: row.try_get::<i64, usize>(1)? as u64,
        spec_json: row.try_get(2)?,
        desired_state: row.try_get(3)?,
        observed_state: row.try_get(4)?,
        convergence: row.try_get(5)?,
        generation: row.try_get::<i64, usize>(6)? as u64,
        node_ids,
        checkpoint_id: row.try_get(8)?,
        last_error: row.try_get(9)?,
        updated_at_ms: row.try_get::<i64, usize>(10)? as u64,
    })
}

const JOB_COLUMNS: &str = "SELECT job_id, version, spec_json, desired_state, observed_state, convergence, generation, node_ids_json, checkpoint_id, last_error, updated_at_ms FROM cp_jobs";

async fn pg_fetch_job(conn: &mut sqlx::PgConnection, job_id: &str) -> Result<Option<JobRecord>, StorageError> {
    let row = sqlx::query(&format!("{JOB_COLUMNS} WHERE job_id = $1"))
        .bind(job_id)
        .fetch_optional(conn)
        .await?;
    Ok(row.map(|row| pg_row_to_job(&row)).transpose()?)
}

async fn pg_job_generation(conn: &mut sqlx::PgConnection, job_id: &str) -> Result<Option<u64>, StorageError> {
    let row: Option<(i64,)> = sqlx::query_as("SELECT generation FROM cp_jobs WHERE job_id = $1")
        .bind(job_id)
        .fetch_optional(conn)
        .await?;
    Ok(row.map(|(generation,)| generation as u64))
}



#[allow(clippy::type_complexity)]
fn pg_row_intent_with_observed(
    row: &PgRow,
) -> Result<(IntentRecord, Option<String>, Option<String>), StorageError> {
    let record = IntentRecord {
        intent_id: row.try_get(0)?,
        node_id: row.try_get(1)?,
        stream_id: row.try_get(2)?,
        generation: row.try_get::<i64, usize>(3)? as u64,
        state: row.try_get(4)?,
        desired_state: row.try_get(5)?,
        config_version_id: row.try_get(6)?,
        action_id: row.try_get(7)?,
        convergence_state: row.try_get(8)?,
        retry_count: row.try_get::<i64, usize>(11)? as u32,
        next_retry_at_ms: row.try_get::<Option<i64>, usize>(12)?.map(|v| v as u64),
        failure_class: row.try_get(13)?,
        superseded_by_intent_id: row.try_get(14)?,
        superseded_generation: row.try_get::<Option<i64>, usize>(19)?.map(|v| v as u64),
        created_at_ms: row.try_get::<i64, usize>(15)? as u64,
        updated_at_ms: row.try_get::<i64, usize>(16)? as u64,
        observed_generation: row.try_get::<Option<i64>, usize>(17)?.map(|v| v as u64),
        observed_state: row.try_get(18)?,
    };
    Ok((record, row.try_get(9)?, row.try_get(10)?))
}



async fn pg_get_intent(conn: &mut sqlx::PgConnection, intent_id: &str) -> Result<Option<IntentRecord>, StorageError> {
    let row = sqlx::query(
        "SELECT i.intent_id, i.node_id, i.stream_id, i.generation, i.state, i.desired_state, i.config_version_id, i.action_id, i.convergence_state, i.retry_count, i.next_retry_at_ms, i.last_failure_class, i.superseded_by_intent_id, i.created_at_ms, i.updated_at_ms, o.observed_generation, o.observed_state, (SELECT generation FROM cp_intents s WHERE s.intent_id = i.superseded_by_intent_id) FROM cp_intents i LEFT JOIN cp_stream_observed o ON o.node_id = i.node_id AND o.stream_id = i.stream_id WHERE i.intent_id = $1",
    )
    .bind(intent_id)
    .fetch_optional(conn)
    .await?;
    Ok(row
        .map(|row| -> Result<IntentRecord, StorageError> {
            Ok(IntentRecord {
                intent_id: row.try_get(0)?,
                node_id: row.try_get(1)?,
                stream_id: row.try_get(2)?,
                generation: row.try_get::<i64, usize>(3)? as u64,
                state: row.try_get(4)?,
                desired_state: row.try_get(5)?,
                config_version_id: row.try_get(6)?,
                action_id: row.try_get(7)?,
                convergence_state: row.try_get(8)?,
                retry_count: row.try_get::<i64, usize>(9)? as u32,
                next_retry_at_ms: row.try_get::<Option<i64>, usize>(10)?.map(|v| v as u64),
                failure_class: row.try_get(11)?,
                superseded_by_intent_id: row.try_get(12)?,
                superseded_generation: row.try_get::<Option<i64>, usize>(17)?.map(|v| v as u64),
                created_at_ms: row.try_get::<i64, usize>(13)? as u64,
                updated_at_ms: row.try_get::<i64, usize>(14)? as u64,
                observed_generation: row.try_get::<Option<i64>, usize>(15)?.map(|v| v as u64),
                observed_state: row.try_get(16)?,
            })
        })
        .transpose()?)
}



#[allow(clippy::type_complexity)]
fn pg_row_attempt(row: &PgRow) -> Result<AttemptRecord, StorageError> {
    Ok(AttemptRecord {
        attempt_id: row.try_get(0)?,
        intent_id: row.try_get(1)?,
        command_id: row.try_get(2)?,
        state: row.try_get(3)?,
        failure_class: row.try_get(4)?,
        node_id: row.try_get(5)?,
        stream_id: row.try_get(6)?,
        generation: row.try_get::<i64, usize>(7)? as u64,
        operation: row.try_get(8)?,
        action_id: row.try_get(9)?,
        config_version_id: row.try_get(10)?,
        payload_json: row.try_get(12)?,
    })
}


impl PgStore {
    /// Connects, probes the server and creates the schema idempotently.
    /// A failure here fails Hub startup fast: a control plane without its
    /// storage is not partially usable.
    pub async fn open(url: &str) -> Result<Self, StorageError> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await?;
        sqlx::raw_sql(PG_DDL)
            .execute(&pool)
            .await?;
        Ok(Self { pool })
    }


    pub async fn set_desired(&self, mutation: DesiredMutation) -> Result<IntentRecord, StorageError> {
        let now = crate::storage::now_ms();
        let mut tx = self.pool.begin().await?;
        let intent_type = mutation.intent_type.as_deref().unwrap_or_else(|| {
            if mutation.action_id.is_some() { "restart" } else { "set_state" }
        });
        if let Some(idempotency_key) = mutation.idempotency_key.as_deref() {
            let existing = sqlx::query(
                "SELECT i.intent_id, i.node_id, i.stream_id, i.generation, i.state, i.desired_state, i.config_version_id, i.action_id, i.convergence_state, i.intent_type, i.payload_json, i.retry_count, i.next_retry_at_ms, i.last_failure_class, i.superseded_by_intent_id, i.created_at_ms, i.updated_at_ms, o.observed_generation, o.observed_state, (SELECT generation FROM cp_intents s WHERE s.intent_id = i.superseded_by_intent_id) FROM cp_intents i LEFT JOIN cp_stream_observed o ON o.node_id = i.node_id AND o.stream_id = i.stream_id WHERE i.node_id = $1 AND i.stream_id = $2 AND i.idempotency_key = $3",
            )
            .bind(&mutation.node_id)
            .bind(&mutation.stream_id)
            .bind(idempotency_key)
            .fetch_optional(&mut *tx)
            .await?
            .map(|row| pg_row_intent_with_observed(&row))
            .transpose()?;
            if let Some((existing, stored_intent_type, payload_json)) = existing {
                let requested_intent_type = Some(intent_type.to_owned());
                if existing.desired_state != mutation.desired_state
                    || existing.config_version_id != mutation.config_version_id
                    || existing.action_id != mutation.action_id
                    || stored_intent_type != requested_intent_type
                    || payload_json != mutation.payload_json
                {
                    return Err(StorageError::IdempotencyKeyReused);
                }
                tx.commit().await?;
                return Ok(existing);
            }
        }
        let current: Option<i64> = sqlx::query_scalar(
            "SELECT generation FROM cp_stream_desired WHERE node_id = $1 AND stream_id = $2",
        )
        .bind(&mutation.node_id)
        .bind(&mutation.stream_id)
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
        let current = current.unwrap_or(0) as u64;
        if let Some(expected) = mutation.expected_generation {
            if expected != current {
                return Err(StorageError::GenerationConflict { expected, current });
            }
        }
        let generation = current + 1;
        let intent_id = format!("intent-{generation}-{}", crate::storage::NEXT_ID.fetch_add(1, Ordering::Relaxed));
        sqlx::query(
            "INSERT INTO cp_stream_desired (node_id, stream_id, generation, desired_state, config_version_id, desired_action_id, updated_at_ms, updated_by, correlation_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT(node_id, stream_id) DO UPDATE SET generation = excluded.generation, desired_state = excluded.desired_state, config_version_id = excluded.config_version_id, desired_action_id = excluded.desired_action_id, updated_at_ms = excluded.updated_at_ms, updated_by = excluded.updated_by, correlation_id = excluded.correlation_id",
        )
        .bind(&mutation.node_id)
        .bind(&mutation.stream_id)
        .bind(generation as i64)
        .bind(&mutation.desired_state)
        .bind(&mutation.config_version_id)
        .bind(&mutation.action_id)
        .bind(now as i64)
        .bind(&mutation.actor)
        .bind(&mutation.correlation_id)
        .execute(&mut *tx)
        .await?;
        if let (Some(config_version_id), Some(payload_json)) = (
            mutation.config_version_id.as_deref(),
            mutation.payload_json.as_deref(),
        ) {
            sqlx::query(
                "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms, created_by, correlation_id) VALUES ($1, 'inline-json', $2, 'json', $3, $4, $5) ON CONFLICT DO NOTHING",
            )
            .bind(config_version_id)
            .bind(payload_json)
            .bind(now as i64)
            .bind(&mutation.actor)
            .bind(&mutation.correlation_id)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(
            "INSERT INTO cp_intents (intent_id, node_id, stream_id, generation, intent_type, desired_state, config_version_id, action_id, payload_json, state, convergence_state, created_at_ms, updated_at_ms, actor, correlation_id, idempotency_key) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'accepted', 'pending', $10, $10, $11, $12, $13)",
        )
        .bind(&intent_id)
        .bind(&mutation.node_id)
        .bind(&mutation.stream_id)
        .bind(generation as i64)
        .bind(intent_type)
        .bind(&mutation.desired_state)
        .bind(&mutation.config_version_id)
        .bind(&mutation.action_id)
        .bind(&mutation.payload_json)
        .bind(now as i64)
        .bind(&mutation.actor)
        .bind(&mutation.correlation_id)
        .bind(&mutation.idempotency_key)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE cp_intents SET state = 'superseded', convergence_state = 'pending', superseded_by_intent_id = $1, updated_at_ms = $2 WHERE node_id = $3 AND stream_id = $4 AND state IN ('accepted', 'converging', 'retrying') AND generation < $5",
        )
        .bind(&intent_id)
        .bind(now as i64)
        .bind(&mutation.node_id)
        .bind(&mutation.stream_id)
        .bind(generation as i64)
        .execute(&mut *tx)
        .await?;
        let event_key = format!("reconcile:{intent_id}:{generation}");
        sqlx::query(
            "INSERT INTO cp_outbox (event_key, event_type, node_id, stream_id, intent_id, available_at_ms, created_at_ms) VALUES ($1, 'reconcile_intent', $2, $3, $4, $5, $5)",
        )
        .bind(&event_key)
        .bind(&mutation.node_id)
        .bind(&mutation.stream_id)
        .bind(&intent_id)
        .bind(now as i64)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO cp_events (node_id, stream_id, intent_id, event_type, outcome, generation, correlation_id, occurred_at_ms) VALUES ($1, $2, $3, 'intent_created', 'accepted', $4, $5, $6)",
        )
        .bind(&mutation.node_id)
        .bind(&mutation.stream_id)
        .bind(&intent_id)
        .bind(generation as i64)
        .bind(&mutation.correlation_id)
        .bind(now as i64)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO cp_audit_events (actor, action, resource_type, resource_id, node_id, stream_id, correlation_id, outcome, occurred_at_ms) VALUES ($1, $2, 'stream', $3, $4, $5, $6, 'accepted', $7)",
        )
        .bind(&mutation.actor)
        .bind(intent_type)
        .bind(format!("{}:{}", mutation.node_id, mutation.stream_id))
        .bind(&mutation.node_id)
        .bind(&mutation.stream_id)
        .bind(&mutation.correlation_id)
        .bind(now as i64)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(IntentRecord {
            intent_id,
            node_id: mutation.node_id,
            stream_id: mutation.stream_id,
            generation,
            state: "accepted".into(),
            desired_state: mutation.desired_state,
            config_version_id: mutation.config_version_id,
            action_id: mutation.action_id,
            convergence_state: "pending".into(),
            retry_count: 0,
            next_retry_at_ms: None,
            failure_class: None,
            superseded_by_intent_id: None,
            superseded_generation: None,
            created_at_ms: now,
            updated_at_ms: now,
            observed_generation: None,
            observed_state: None,
        })
    }

    pub async fn upsert_node(&self, mutation: NodeMutation) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO cp_nodes (node_id, role, protocol_version, node_version, state, capabilities_json, boot_id, last_report_seq, last_seen_at_ms, lease_expires_at_ms, maintenance_state, maintenance_updated_at_ms, created_at_ms, updated_at_ms) VALUES ($1, 'compute', 'v1', $2, $3, $4, $5, $6, $7, $8, COALESCE($9, 'active'), $10, $7, $7) ON CONFLICT(node_id) DO UPDATE SET node_version = excluded.node_version, state = excluded.state, capabilities_json = excluded.capabilities_json, boot_id = excluded.boot_id, last_report_seq = excluded.last_report_seq, last_seen_at_ms = excluded.last_seen_at_ms, lease_expires_at_ms = excluded.lease_expires_at_ms, updated_at_ms = excluded.updated_at_ms",
        )
        .bind(&mutation.node_id)
        .bind(&mutation.version)
        .bind(&mutation.state)
        .bind(&mutation.capabilities_json)
        .bind(&mutation.boot_id)
        .bind(mutation.report_seq.map(|value| value as i64))
        .bind(mutation.last_seen_at_ms as i64)
        .bind(mutation.lease_expires_at_ms as i64)
        .bind(&mutation.maintenance_state)
        .bind(mutation.maintenance_updated_at_ms.map(|value| value as i64))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn reset_observed_cursors(&self, node_id: &str) -> Result<(), StorageError> {
        sqlx::query("UPDATE cp_stream_observed SET report_seq = 0 WHERE node_id = $1")
            .bind(node_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn set_node_maintenance(&self, mutation: NodeMaintenanceMutation, now_ms: u64) -> Result<bool, StorageError> {
        let state = mutation.state.as_str();
        if !matches!(state, "active" | "draining" | "maintenance") {
            return Ok(false);
        }
        let mut tx = self.pool.begin().await?;
        let previous: Option<(String,)> = sqlx::query_as(
            "SELECT COALESCE(maintenance_state, 'active') FROM cp_nodes WHERE node_id = $1",
        )
        .bind(&mutation.node_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(previous) = previous else { return Ok(false) };
        let previous = previous.0;
        if previous != state {
            sqlx::query(
                "UPDATE cp_nodes SET maintenance_state = $1, maintenance_updated_at_ms = $2, updated_at_ms = $2 WHERE node_id = $3",
            )
            .bind(state)
            .bind(now_ms as i64)
            .bind(&mutation.node_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO cp_events (node_id, event_type, outcome, message, correlation_id, actor, occurred_at_ms) VALUES ($1, 'node_maintenance_changed', 'succeeded', $2, $3, $4, $5)",
            )
            .bind(&mutation.node_id)
            .bind(format!("{previous}->{state}"))
            .bind(&mutation.correlation_id)
            .bind(&mutation.actor)
            .bind(now_ms as i64)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO cp_audit_events (actor, action, resource_type, resource_id, node_id, correlation_id, outcome, message, occurred_at_ms) VALUES ($1, 'node.maintenance', 'node', $2, $2, $3, 'accepted', $4, $5)",
            )
            .bind(&mutation.actor)
            .bind(&mutation.node_id)
            .bind(&mutation.correlation_id)
            .bind(format!("{previous}->{state}"))
            .bind(now_ms as i64)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(true)
    }

    pub async fn get_node_maintenance(&self, node_id: &str) -> Result<Option<String>, StorageError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT COALESCE(maintenance_state, 'active') FROM cp_nodes WHERE node_id = $1")
                .bind(node_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(state,)| state))
    }

    pub async fn operational_aggregates(&self, now_ms: u64) -> Result<OperationalAggregates, StorageError> {
        async fn grouped(
            pool: &sqlx::PgPool,
            sql: &str,
        ) -> Result<Vec<(String, u64)>, StorageError> {
            let rows: Vec<(String, i64)> = sqlx::query_as(sql).fetch_all(pool).await?;
            Ok(rows.into_iter().map(|(name, count)| (name, count as u64)).collect())
        }
        async fn scalar(pool: &sqlx::PgPool, sql: &str) -> Result<u64, StorageError> {
            let (count,): (i64,) = sqlx::query_as(sql).fetch_one(pool).await?;
            Ok(count as u64)
        }
        let oldest: Option<(Option<i64>,)> = sqlx::query_as(
            "SELECT MIN(created_at_ms) FROM cp_outbox WHERE processed_at_ms IS NULL",
        )
        .fetch_optional(&self.pool)
        .await?;
        let oldest = oldest.and_then(|(created,)| created);
        let stale_nodes: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM cp_nodes WHERE state = 'stale' OR lease_expires_at_ms <= $1",
        )
        .bind(now_ms as i64)
        .fetch_one(&self.pool)
        .await?;
        Ok(OperationalAggregates {
            node_states: grouped(&self.pool, "SELECT state, COUNT(*) FROM cp_nodes GROUP BY state").await?,
            maintenance_states: grouped(&self.pool, "SELECT COALESCE(maintenance_state, 'active'), COUNT(*) FROM cp_nodes GROUP BY COALESCE(maintenance_state, 'active')").await?,
            intent_states: grouped(&self.pool, "SELECT state, COUNT(*) FROM cp_intents GROUP BY state").await?,
            convergence_states: grouped(&self.pool, "SELECT convergence_state, COUNT(*) FROM cp_intents GROUP BY convergence_state").await?,
            attempt_states: grouped(&self.pool, "SELECT state, COUNT(*) FROM cp_attempts GROUP BY state").await?,
            failure_classes: grouped(&self.pool, "SELECT COALESCE(last_failure_class, 'none'), COUNT(*) FROM cp_intents GROUP BY COALESCE(last_failure_class, 'none')").await?,
            outbox_pending: scalar(&self.pool, "SELECT COUNT(*) FROM cp_outbox WHERE processed_at_ms IS NULL").await?,
            outbox_claimed: scalar(&self.pool, "SELECT COUNT(*) FROM cp_outbox WHERE processed_at_ms IS NULL AND claimed_at_ms IS NOT NULL").await?,
            stale_nodes: stale_nodes.0 as u64,
            active_attempts: scalar(&self.pool, "SELECT COUNT(*) FROM cp_attempts WHERE state IN ('queued','dispatched','acknowledged','running')").await?,
            non_terminal_intents: scalar(&self.pool, "SELECT COUNT(*) FROM cp_intents WHERE state IN ('accepted','converging','retrying')").await?,
            oldest_pending_age_seconds: oldest
                .map(|created| now_ms.saturating_sub(created as u64) / 1000),
        })
    }

    pub async fn claim_outbox(&self, worker_id: &str, now_ms: u64) -> Result<Option<OutboxRecord>, StorageError> {
        let _ = (&worker_id, &now_ms);
        Err(StorageError::Unsupported(format!(
            "postgres backend: claim_outbox is not implemented yet"
        )))
    }

    pub async fn get_desired(&self, node_id: &str, stream_id: &str) -> Result<Option<DesiredRecord>, StorageError> {
        let row = sqlx::query(
            "SELECT node_id, stream_id, generation, desired_state, config_version_id, desired_action_id, correlation_id FROM cp_stream_desired WHERE node_id = $1 AND stream_id = $2",
        )
        .bind(node_id)
        .bind(stream_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row
            .map(|row| -> Result<DesiredRecord, StorageError> {
                Ok(DesiredRecord {
                    node_id: row.try_get(0)?,
                    stream_id: row.try_get(1)?,
                    generation: row.try_get::<i64, usize>(2)? as u64,
                    desired_state: row.try_get(3)?,
                    config_version_id: row.try_get(4)?,
                    action_id: row.try_get(5)?,
                    correlation_id: row.try_get(6)?,
                })
            })
            .transpose()?)
    }

    pub async fn get_intent(&self, intent_id: &str) -> Result<Option<IntentRecord>, StorageError> {
        let mut tx = self.pool.begin().await?;
        let intent = pg_get_intent(&mut tx, intent_id).await?;
        tx.commit().await?;
        Ok(intent)
    }

    pub async fn list_intents(&self, node_id: Option<&str>) -> Result<Vec<IntentRecord>, StorageError> {
        let ids: Vec<(String,)> = sqlx::query_as(
            "SELECT intent_id FROM cp_intents WHERE ($1 IS NULL OR node_id = $1) ORDER BY created_at_ms DESC, intent_id DESC LIMIT 4096",
        )
        .bind(node_id)
        .fetch_all(&self.pool)
        .await?;
        let mut tx = self.pool.begin().await?;
        let mut intents = Vec::with_capacity(ids.len());
        for (id,) in ids {
            if let Some(intent) = pg_get_intent(&mut tx, &id).await? {
                intents.push(intent);
            }
        }
        tx.commit().await?;
        Ok(intents)
    }

    pub async fn recover_reconciliation(&self, now_ms: u64) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO cp_outbox (event_key, event_type, node_id, stream_id, intent_id, available_at_ms, created_at_ms) SELECT 'reconcile:recovery:' || i.intent_id || ':' || $1, 'reconcile_intent', i.node_id, i.stream_id, i.intent_id, $1, $1 FROM cp_intents i WHERE i.state IN ('accepted', 'converging', 'retrying') AND (i.last_failure_class IS NULL OR i.last_failure_class <> 'ambiguous') AND NOT EXISTS (SELECT 1 FROM cp_outbox o WHERE o.intent_id = i.intent_id AND o.processed_at_ms IS NULL) ON CONFLICT DO NOTHING",
        )
        .bind(now_ms as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn wake_node(&self, node_id: &str, now_ms: u64) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO cp_outbox (event_key, event_type, node_id, stream_id, intent_id, available_at_ms, created_at_ms) SELECT 'reconcile:register:' || i.intent_id, 'reconcile_intent', i.node_id, i.stream_id, i.intent_id, $1, $1 FROM cp_intents i WHERE i.node_id = $2 AND i.state IN ('accepted', 'converging', 'retrying') AND (i.last_failure_class IS NULL OR i.last_failure_class <> 'ambiguous') AND NOT EXISTS (SELECT 1 FROM cp_outbox o WHERE o.intent_id = i.intent_id AND o.processed_at_ms IS NULL) ON CONFLICT DO NOTHING",
        )
        .bind(now_ms as i64)
        .bind(node_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_events(&self, node_id: Option<&str>) -> Result<Vec<StoredEvent>, StorageError> {
        let _ = (&node_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: list_events is not implemented yet"
        )))
    }

    pub async fn prune_events(&self, retain: usize) -> Result<usize, StorageError> {
        let _ = (&retain);
        Err(StorageError::Unsupported(format!(
            "postgres backend: prune_events is not implemented yet"
        )))
    }

    pub async fn prune_operation_history(&self, older_than_ms: i64, max_retained: i64) -> Result<usize, StorageError> {
        let _ = (&older_than_ms, &max_retained);
        Err(StorageError::Unsupported(format!(
            "postgres backend: prune_operation_history is not implemented yet"
        )))
    }

    pub async fn prune_job_checkpoint_records(&self, older_than_ms: i64) -> Result<usize, StorageError> {
        let _ = (&older_than_ms);
        Err(StorageError::Unsupported(format!(
            "postgres backend: prune_job_checkpoint_records is not implemented yet"
        )))
    }

    pub async fn prune_audit_events(&self, older_than_ms: i64, max_retained: i64) -> Result<usize, StorageError> {
        let _ = (&older_than_ms, &max_retained);
        Err(StorageError::Unsupported(format!(
            "postgres backend: prune_audit_events is not implemented yet"
        )))
    }

    pub async fn prune_processed_outbox(&self, older_than_ms: i64, max_retained: i64) -> Result<usize, StorageError> {
        let _ = (&older_than_ms, &max_retained);
        Err(StorageError::Unsupported(format!(
            "postgres backend: prune_processed_outbox is not implemented yet"
        )))
    }

    pub async fn prune_terminal_attempts(&self, older_than_ms: i64, max_retained: i64) -> Result<usize, StorageError> {
        let mut tx = self.pool.begin().await?;
        let mut deleted = sqlx::query(
            "DELETE FROM cp_attempts WHERE state NOT IN ('queued', 'dispatched', 'acknowledged', 'running') AND COALESCE(finished_at_ms, created_at_ms) < $1",
        )
        .bind(older_than_ms)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        deleted += sqlx::query(
            "DELETE FROM cp_attempts WHERE state NOT IN ('queued', 'dispatched', 'acknowledged', 'running') AND attempt_id NOT IN (SELECT attempt_id FROM cp_attempts WHERE state NOT IN ('queued', 'dispatched', 'acknowledged', 'running') ORDER BY COALESCE(finished_at_ms, created_at_ms) DESC, attempt_id DESC LIMIT $1)",
        )
        .bind(max_retained)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        Ok(deleted as usize)
    }

    pub async fn claim_attempt(&self, intent_id: &str) -> Result<Option<AttemptRecord>, StorageError> {
        let mut tx = self.pool.begin().await?;
        let existing = sqlx::query(
            "SELECT a.attempt_id, a.intent_id, a.command_id, a.state, a.failure_class, a.node_id, a.stream_id, a.generation, a.operation, i.action_id, i.config_version_id, i.intent_type, COALESCE(i.payload_json, cv.content_ref) FROM cp_attempts a JOIN cp_intents i ON i.intent_id = a.intent_id LEFT JOIN cp_config_versions cv ON cv.config_version_id = i.config_version_id WHERE a.intent_id = $1 AND a.state IN ('queued', 'dispatched', 'acknowledged', 'running') ORDER BY a.created_at_ms DESC LIMIT 1",
        )
        .bind(intent_id)
        .fetch_optional(&mut *tx)
        .await?
        .map(|row| pg_row_attempt(&row));
        if let Some(attempt) = existing {
            tx.commit().await?;
            return Ok(Some(attempt?));
        }
        let target: Option<(
            String,
            String,
            i64,
            String,
            Option<String>,
            Option<String>,
            String,
            Option<String>,
        )> = sqlx::query_as(
            "SELECT i.node_id, i.stream_id, i.generation, COALESCE(i.desired_state, ''), i.action_id, i.config_version_id, i.intent_type, COALESCE(i.payload_json, cv.content_ref) FROM cp_intents i LEFT JOIN cp_config_versions cv ON cv.config_version_id = i.config_version_id WHERE i.intent_id = $1 AND i.state IN ('accepted', 'converging', 'retrying')",
        )
        .bind(intent_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((
            node_id,
            stream_id,
            generation,
            desired_state,
            action_id,
            config_version_id,
            intent_type,
            payload_json,
        )) = target
        else {
            tx.commit().await?;
            return Ok(None);
        };
        let generation = generation as u64;
        let operation = if intent_type == "apply_configuration" {
            "apply_configuration"
        } else if action_id.is_some() {
            "restart"
        } else if desired_state == "running" {
            "start"
        } else {
            "stop"
        };
        let suffix = crate::storage::NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let attempt_id = format!("attempt-{suffix}");
        let command_id = format!("cmd-{suffix}");
        let now = crate::storage::now_ms();
        sqlx::query(
            "INSERT INTO cp_attempts (attempt_id, intent_id, command_id, node_id, stream_id, generation, operation, state, created_at_ms) VALUES ($1, $2, $3, $4, $5, $6, $7, 'queued', $8)",
        )
        .bind(&attempt_id)
        .bind(intent_id)
        .bind(&command_id)
        .bind(&node_id)
        .bind(&stream_id)
        .bind(generation as i64)
        .bind(operation)
        .bind(now as i64)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Some(AttemptRecord {
            attempt_id,
            intent_id: intent_id.to_string(),
            command_id,
            state: "queued".to_string(),
            failure_class: None,
            node_id,
            stream_id,
            generation,
            operation: operation.to_string(),
            action_id,
            config_version_id,
            payload_json,
        }))
    }

    pub async fn complete_attempt(&self, attempt_id: &str, state: &str, failure_class: Option<&str>) -> Result<(), StorageError> {
        let mut tx = self.pool.begin().await?;
        let attempt: Option<(String, String, String, i64)> = sqlx::query_as(
            "SELECT intent_id, node_id, stream_id, generation FROM cp_attempts WHERE attempt_id = $1",
        )
        .bind(attempt_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((intent_id, node_id, stream_id, generation)) = attempt else {
            return Ok(());
        };
        let generation = generation as u64;
        let ambiguous = state == "ambiguous" || failure_class == Some("ambiguous");
        let terminal = ambiguous
            || matches!(
                state,
                "succeeded" | "failed" | "timed_out" | "node_unavailable" | "cancelled" | "superseded"
            );
        sqlx::query(
            "UPDATE cp_attempts SET state = $1, failure_class = $2, finished_at_ms = CASE WHEN $3 THEN $4 ELSE finished_at_ms END WHERE attempt_id = $5",
        )
        .bind(state)
        .bind(failure_class)
        .bind(terminal)
        .bind(crate::storage::now_ms() as i64)
        .bind(attempt_id)
        .execute(&mut *tx)
        .await?;
        if terminal {
            match failure_class {
                Some("temporary_execution") | Some("transport") | Some("node_unavailable") => {
                    let retry_at = crate::storage::now_ms() + 1_000;
                    sqlx::query(
                        "UPDATE cp_intents SET state = 'retrying', convergence_state = 'degraded', retry_count = retry_count + 1, next_retry_at_ms = $1, last_failure_class = $2, updated_at_ms = $1 WHERE intent_id = $3 AND state IN ('accepted', 'converging', 'retrying')",
                    )
                    .bind(retry_at as i64)
                    .bind(failure_class)
                    .bind(&intent_id)
                    .execute(&mut *tx)
                    .await?;
                    let event_key = format!("reconcile:retry:{attempt_id}");
                    sqlx::query(
                        "INSERT INTO cp_outbox (event_key, event_type, node_id, stream_id, intent_id, available_at_ms, created_at_ms) VALUES ($1, 'retry_intent', $2, $3, $4, $5, $5) ON CONFLICT DO NOTHING",
                    )
                    .bind(&event_key)
                    .bind(&node_id)
                    .bind(&stream_id)
                    .bind(&intent_id)
                    .bind(retry_at as i64)
                    .execute(&mut *tx)
                    .await?;
                }
                Some("stale_generation") => {
                    sqlx::query(
                        "UPDATE cp_intents SET state = 'superseded', convergence_state = 'degraded', last_failure_class = $1, updated_at_ms = $2 WHERE intent_id = $3 AND state IN ('accepted', 'converging', 'retrying')",
                    )
                    .bind(failure_class)
                    .bind(crate::storage::now_ms() as i64)
                    .bind(&intent_id)
                    .execute(&mut *tx)
                    .await?;
                }
                Some("ambiguous") => {
                    sqlx::query(
                        "UPDATE cp_intents SET state = 'converging', convergence_state = 'degraded', next_retry_at_ms = NULL, last_failure_class = $1, updated_at_ms = $2 WHERE intent_id = $3 AND state IN ('accepted', 'converging', 'retrying')",
                    )
                    .bind(failure_class)
                    .bind(crate::storage::now_ms() as i64)
                    .bind(&intent_id)
                    .execute(&mut *tx)
                    .await?;
                }
                Some(_) if state != "succeeded" => {
                    sqlx::query(
                        "UPDATE cp_intents SET state = 'blocked', convergence_state = 'blocked', last_failure_class = $1, updated_at_ms = $2 WHERE intent_id = $3 AND state IN ('accepted', 'converging', 'retrying')",
                    )
                    .bind(failure_class)
                    .bind(crate::storage::now_ms() as i64)
                    .bind(&intent_id)
                    .execute(&mut *tx)
                    .await?;
                }
                None if state != "succeeded" => {
                    sqlx::query(
                        "UPDATE cp_intents SET state = 'blocked', convergence_state = 'blocked', updated_at_ms = $1 WHERE intent_id = $2 AND state IN ('accepted', 'converging', 'retrying')",
                    )
                    .bind(crate::storage::now_ms() as i64)
                    .bind(&intent_id)
                    .execute(&mut *tx)
                    .await?;
                }
                _ => {}
            }
        }
        sqlx::query(
            "INSERT INTO cp_events (node_id, stream_id, intent_id, attempt_id, event_type, outcome, failure_class, generation, occurred_at_ms) VALUES ($1, $2, $3, $4, 'attempt_completed', $5, $6, $7, $8)",
        )
        .bind(&node_id)
        .bind(&stream_id)
        .bind(&intent_id)
        .bind(attempt_id)
        .bind(state)
        .bind(failure_class)
        .bind(generation as i64)
        .bind(crate::storage::now_ms() as i64)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn mark_attempt_dispatched(&self, attempt_id: &str, expires_at_ms: u64) -> Result<(), StorageError> {
        sqlx::query(
            "UPDATE cp_attempts SET state = 'dispatched', dispatched_at_ms = $1, expires_at_ms = $2 WHERE attempt_id = $3 AND state = 'queued'",
        )
        .bind(crate::storage::now_ms() as i64)
        .bind(expires_at_ms as i64)
        .bind(attempt_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn expire_attempts(&self, now_ms: u64) -> Result<usize, StorageError> {
        let mut tx = self.pool.begin().await?;
        let expired: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT attempt_id, intent_id, node_id, stream_id FROM cp_attempts WHERE state IN ('queued', 'dispatched', 'acknowledged', 'running') AND expires_at_ms IS NOT NULL AND expires_at_ms <= $1",
        )
        .bind(now_ms as i64)
        .fetch_all(&mut *tx)
        .await?;
        for (attempt_id, intent_id, node_id, stream_id) in &expired {
            sqlx::query(
                "UPDATE cp_attempts SET state = 'ambiguous', failure_class = 'ambiguous', finished_at_ms = $1 WHERE attempt_id = $2 AND state IN ('queued', 'dispatched', 'acknowledged', 'running')",
            )
            .bind(now_ms as i64)
            .bind(attempt_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE cp_intents SET state = 'converging', convergence_state = 'degraded', next_retry_at_ms = NULL, last_failure_class = 'ambiguous', updated_at_ms = $1 WHERE intent_id = $2 AND state IN ('accepted', 'converging', 'retrying')",
            )
            .bind(now_ms as i64)
            .bind(intent_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO cp_events (node_id, stream_id, intent_id, event_type, outcome, failure_class, message, occurred_at_ms) VALUES ($1, $2, $3, 'attempt_expired', 'ambiguous', 'ambiguous', 'Attempt lease expired; waiting for a fresh observed report', $4)",
            )
            .bind(node_id)
            .bind(stream_id)
            .bind(intent_id)
            .bind(now_ms as i64)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(expired.len())
    }

    pub async fn record_observed(&self, mutation: ObservedMutation) -> Result<(), StorageError> {
        let mut tx = self.pool.begin().await?;
        let current: Option<(Option<String>, i64)> = sqlx::query_as(
            "SELECT boot_id, COALESCE(report_seq, 0) FROM cp_stream_observed WHERE node_id = $1 AND stream_id = $2",
        )
        .bind(&mutation.node_id)
        .bind(&mutation.stream_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((boot_id, report_seq)) = current {
            let report_seq = report_seq as u64;
            match (boot_id.as_deref(), mutation.boot_id.as_deref()) {
                // Same fencible session: the sequence cursor rejects replays.
                (Some(stored), Some(incoming)) if stored == incoming => {
                    if mutation.report_seq <= report_seq {
                        return Ok(());
                    }
                }
                // Boot-less agents report seq 0 forever: accept the report and
                // let convergence run on its content.
                (None, None) => {}
                // Session identity changed: the incoming report supersedes.
                _ => {}
            }
        }
        let now = crate::storage::now_ms();
        sqlx::query(
            "INSERT INTO cp_stream_observed (node_id, stream_id, boot_id, report_seq, observed_generation, observed_state, applied_config_version, last_action_id, last_error_code, last_error_message, snapshot_json, observed_at_ms) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ON CONFLICT(node_id, stream_id) DO UPDATE SET boot_id = excluded.boot_id, report_seq = excluded.report_seq, observed_generation = excluded.observed_generation, observed_state = excluded.observed_state, applied_config_version = excluded.applied_config_version, last_action_id = excluded.last_action_id, last_error_code = excluded.last_error_code, last_error_message = excluded.last_error_message, snapshot_json = excluded.snapshot_json, observed_at_ms = excluded.observed_at_ms",
        )
        .bind(&mutation.node_id)
        .bind(&mutation.stream_id)
        .bind(&mutation.boot_id)
        .bind(mutation.report_seq as i64)
        .bind(mutation.observed_generation.map(|value| value as i64))
        .bind(&mutation.observed_state)
        .bind(&mutation.config_version_id)
        .bind(&mutation.action_id)
        .bind(&mutation.last_error_code)
        .bind(&mutation.last_error_message)
        .bind(&mutation.snapshot_json)
        .bind(now as i64)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO cp_events (node_id, stream_id, event_type, outcome, message, generation, occurred_at_ms) VALUES ($1, $2, 'observed_report', $3, $4, $5, $6)",
        )
        .bind(&mutation.node_id)
        .bind(&mutation.stream_id)
        .bind(&mutation.observed_state)
        .bind(&mutation.last_error_message)
        .bind(mutation.observed_generation.map(|value| value as i64))
        .bind(now as i64)
        .execute(&mut *tx)
        .await?;
        let desired: Option<(i64, String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT generation, desired_state, config_version_id, desired_action_id FROM cp_stream_desired WHERE node_id = $1 AND stream_id = $2",
        )
        .bind(&mutation.node_id)
        .bind(&mutation.stream_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((generation, desired_state, desired_config, desired_action_id)) = desired {
            let generation = generation as u64;
            let config_matches = desired_config
                .as_deref()
                .is_none_or(|version| Some(version) == mutation.config_version_id.as_deref());
            let action_matches = desired_action_id
                .as_deref()
                .is_none_or(|action_id| Some(action_id) == mutation.action_id.as_deref());
            let affected_streams_converged = if mutation.stream_id == "__configuration__" {
                let blockers: (i64,) = sqlx::query_as(
                    "SELECT COUNT(*) FROM cp_stream_desired d LEFT JOIN cp_stream_observed o ON o.node_id = d.node_id AND o.stream_id = d.stream_id WHERE d.node_id = $1 AND d.stream_id <> '__configuration__' AND (o.stream_id IS NULL OR o.observed_generation <> d.generation OR o.observed_state <> d.desired_state OR o.applied_config_version IS NULL OR o.applied_config_version <> $2)",
                )
                .bind(&mutation.node_id)
                .bind(&mutation.config_version_id)
                .fetch_one(&mut *tx)
                .await?;
                blockers.0 == 0
            } else {
                true
            };
            if mutation.observed_generation == Some(generation)
                && desired_state == mutation.observed_state
                && config_matches
                && action_matches
                && affected_streams_converged
            {
                sqlx::query(
                    "UPDATE cp_intents SET state = 'converged', convergence_state = 'in_sync', converged_at_ms = $1, updated_at_ms = $1 WHERE node_id = $2 AND stream_id = $3 AND generation = $4 AND state IN ('accepted', 'converging', 'retrying')",
                )
                .bind(now as i64)
                .bind(&mutation.node_id)
                .bind(&mutation.stream_id)
                .bind(generation as i64)
                .execute(&mut *tx)
                .await?;
                sqlx::query(
                    "UPDATE cp_attempts SET state = 'succeeded', finished_at_ms = $1 WHERE node_id = $2 AND stream_id = $3 AND generation = $4 AND state IN ('queued', 'dispatched', 'acknowledged', 'running')",
                )
                .bind(now as i64)
                .bind(&mutation.node_id)
                .bind(&mutation.stream_id)
                .bind(generation as i64)
                .execute(&mut *tx)
                .await?;
                let intent_id: Option<(String,)> = sqlx::query_as(
                    "SELECT intent_id FROM cp_intents WHERE node_id = $1 AND stream_id = $2 AND generation = $3 ORDER BY created_at_ms DESC LIMIT 1",
                )
                .bind(&mutation.node_id)
                .bind(&mutation.stream_id)
                .bind(generation as i64)
                .fetch_optional(&mut *tx)
                .await?;
                let intent_id = intent_id.map(|(id,)| id);
                sqlx::query(
                    "INSERT INTO cp_events (node_id, stream_id, intent_id, event_type, outcome, generation, occurred_at_ms) VALUES ($1, $2, $3, 'intent_converged', 'converged', $4, $5)",
                )
                .bind(&mutation.node_id)
                .bind(&mutation.stream_id)
                .bind(&intent_id)
                .bind(generation as i64)
                .bind(now as i64)
                .execute(&mut *tx)
                .await?;
            } else if mutation.stream_id == "__configuration__"
                && mutation.observed_generation == Some(generation)
                && desired_state == mutation.observed_state
                && config_matches
                && action_matches
            {
                sqlx::query(
                    "UPDATE cp_intents SET state = 'converging', convergence_state = 'applying', updated_at_ms = $1 WHERE node_id = $2 AND stream_id = $3 AND generation = $4 AND state IN ('accepted', 'converging', 'retrying')",
                )
                .bind(now as i64)
                .bind(&mutation.node_id)
                .bind(&mutation.stream_id)
                .bind(generation as i64)
                .execute(&mut *tx)
                .await?;
            }
        }
        let wake_key = format!(
            "reconcile:observed:{}:{}:{}:{}",
            mutation.node_id,
            mutation.stream_id,
            mutation.boot_id.as_deref().unwrap_or("unknown"),
            mutation.report_seq
        );
        sqlx::query(
            "INSERT INTO cp_outbox (event_key, event_type, node_id, stream_id, intent_id, available_at_ms, created_at_ms) SELECT $1, 'reconcile_intent', d.node_id, d.stream_id, i.intent_id, $2, $2 FROM cp_stream_desired d JOIN cp_intents i ON i.node_id = d.node_id AND i.stream_id = d.stream_id AND i.generation = d.generation WHERE d.node_id = $3 AND d.stream_id = $4 AND i.state IN ('accepted', 'converging', 'retrying') AND NOT EXISTS (SELECT 1 FROM cp_outbox o WHERE o.intent_id = i.intent_id AND o.processed_at_ms IS NULL) ON CONFLICT DO NOTHING",
        )
        .bind(&wake_key)
        .bind(now as i64)
        .bind(&mutation.node_id)
        .bind(&mutation.stream_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn mark_outbox_processed(&self, outbox_id: i64, now_ms: u64) -> Result<(), StorageError> {
        let _ = (&outbox_id, &now_ms);
        Err(StorageError::Unsupported(format!(
            "postgres backend: mark_outbox_processed is not implemented yet"
        )))
    }

    pub async fn record_audit(&self, record: AuditRecord) -> Result<i64, StorageError> {
        let _ = (&record);
        Err(StorageError::Unsupported(format!(
            "postgres backend: record_audit is not implemented yet"
        )))
    }

    pub async fn list_audit(&self, resource_id: Option<&str>) -> Result<Vec<AuditRecord>, StorageError> {
        let _ = (&resource_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: list_audit is not implemented yet"
        )))
    }

    pub async fn create_rollout(&self, rollout: RolloutRecord, targets: Vec<RolloutTargetRecord>) -> Result<(), StorageError> {
        let _ = (&rollout, &targets);
        Err(StorageError::Unsupported(format!(
            "postgres backend: create_rollout is not implemented yet"
        )))
    }

    pub async fn create_rollout_with_content(&self, rollout: RolloutRecord, targets: Vec<RolloutTargetRecord>, content: &str, created_by: Option<&str>) -> Result<(), StorageError> {
        let _ = (&rollout, &targets, &content, &created_by);
        Err(StorageError::Unsupported(format!(
            "postgres backend: create_rollout_with_content is not implemented yet"
        )))
    }

    pub async fn get_rollout(&self, rollout_id: &str) -> Result<Option<RolloutRecord>, StorageError> {
        let _ = (&rollout_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: get_rollout is not implemented yet"
        )))
    }

    pub async fn list_rollout_targets(&self, rollout_id: &str) -> Result<Vec<RolloutTargetRecord>, StorageError> {
        let _ = (&rollout_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: list_rollout_targets is not implemented yet"
        )))
    }

    pub async fn update_rollout(&self, rollout_id: &str, state: &str, current_batch: u32, updated_at_ms: u64) -> Result<(), StorageError> {
        let _ = (&rollout_id, &state, &current_batch, &updated_at_ms);
        Err(StorageError::Unsupported(format!(
            "postgres backend: update_rollout is not implemented yet"
        )))
    }

    pub async fn update_rollout_target(&self, update: RolloutTargetUpdate) -> Result<(), StorageError> {
        let _ = (&update);
        Err(StorageError::Unsupported(format!(
            "postgres backend: update_rollout_target is not implemented yet"
        )))
    }

    pub async fn get_config_version_content(&self, config_version_id: &str) -> Result<Option<String>, StorageError> {
        let _ = (&config_version_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: get_config_version_content is not implemented yet"
        )))
    }

    pub async fn recover_rollouts(&self, ) -> Result<Vec<RolloutRecord>, StorageError> {
        Err(StorageError::Unsupported(format!(
            "postgres backend: recover_rollouts is not implemented yet"
        )))
    }

    pub async fn list_rollouts(&self, ) -> Result<Vec<RolloutRecord>, StorageError> {
        Err(StorageError::Unsupported(format!(
            "postgres backend: list_rollouts is not implemented yet"
        )))
    }

    pub async fn upsert_operation(&self, operation: PersistedOperation) -> Result<(), StorageError> {
        let _ = (&operation);
        Err(StorageError::Unsupported(format!(
            "postgres backend: upsert_operation is not implemented yet"
        )))
    }

    pub async fn get_operation(&self, operation_id: &str) -> Result<Option<PersistedOperation>, StorageError> {
        let _ = (&operation_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: get_operation is not implemented yet"
        )))
    }

    pub async fn list_operations(&self, node_id: Option<&str>) -> Result<Vec<PersistedOperation>, StorageError> {
        let _ = (&node_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: list_operations is not implemented yet"
        )))
    }

    pub async fn list_job_start_operations(&self, resource_id: &str) -> Result<Vec<PersistedOperation>, StorageError> {
        let _ = (&resource_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: list_job_start_operations is not implemented yet"
        )))
    }

    pub async fn upsert_job(&self, mut job: JobRecord) -> Result<JobRecord, StorageError> {
        let mut tx = self.pool.begin().await?;
        let current_generation = pg_job_generation(&mut tx, &job.job_id).await?;
        job.generation = current_generation
            .map(|generation| generation.saturating_add(1))
            .unwrap_or_else(|| job.generation.max(1));
        let node_ids = serde_json::to_string(&job.node_ids)
            .map_err(|error| StorageError::Unsupported(format!("node_ids encode failed: {error}")))?;
        sqlx::query(
            "INSERT INTO cp_jobs (job_id, version, spec_json, desired_state, observed_state, convergence, generation, node_ids_json, checkpoint_id, last_error, updated_at_ms) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT(job_id) DO UPDATE SET version=excluded.version, spec_json=excluded.spec_json, desired_state=excluded.desired_state, observed_state=excluded.observed_state, convergence=excluded.convergence, generation=excluded.generation, node_ids_json=excluded.node_ids_json, checkpoint_id=excluded.checkpoint_id, last_error=excluded.last_error, updated_at_ms=excluded.updated_at_ms",
        )
        .bind(&job.job_id)
        .bind(job.version as i64)
        .bind(&job.spec_json)
        .bind(&job.desired_state)
        .bind(&job.observed_state)
        .bind(&job.convergence)
        .bind(job.generation as i64)
        .bind(&node_ids)
        .bind(&job.checkpoint_id)
        .bind(&job.last_error)
        .bind(job.updated_at_ms as i64)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(job)
    }

    pub async fn update_job_with_expected_generation(&self, mut job: JobRecord, expected_generation: u64) -> Result<JobRecord, StorageError> {
        let mut tx = self.pool.begin().await?;
        job.generation = expected_generation.saturating_add(1);
        let node_ids = serde_json::to_string(&job.node_ids)
            .map_err(|error| StorageError::Unsupported(format!("node_ids encode failed: {error}")))?;
        // The recovery pointer (checkpoint_id) is deliberately NOT in the SET
        // list: it is owned by the checkpoint path and preserving it cannot
        // regress recovery (same rationale as the SQLite implementation).
        let changed = sqlx::query(
            "UPDATE cp_jobs SET version=$1, spec_json=$2, desired_state=$3, observed_state=$4, convergence=$5, generation=$6, node_ids_json=$7, last_error=$8, updated_at_ms=$9 WHERE job_id=$10 AND generation=$11",
        )
        .bind(job.version as i64)
        .bind(&job.spec_json)
        .bind(&job.desired_state)
        .bind(&job.observed_state)
        .bind(&job.convergence)
        .bind(job.generation as i64)
        .bind(&node_ids)
        .bind(&job.last_error)
        .bind(job.updated_at_ms as i64)
        .bind(&job.job_id)
        .bind(expected_generation as i64)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if changed == 0 {
            let current = pg_job_generation(&mut tx, &job.job_id).await?;
            return Err(StorageError::GenerationConflict {
                expected: expected_generation,
                current: current.unwrap_or(0),
            });
        }
        let stored = pg_fetch_job(&mut tx, &job.job_id).await?;
        tx.commit().await?;
        Ok(stored.unwrap_or(job))
    }

    pub async fn get_job(&self, job_id: &str) -> Result<Option<JobRecord>, StorageError> {
        let mut tx = self.pool.begin().await?;
        let job = pg_fetch_job(&mut tx, job_id).await?;
        tx.commit().await?;
        Ok(job)
    }

    pub async fn upsert_job_version(&self, record: JobVersionRecord) -> Result<(), StorageError> {
        let _ = (&record);
        Err(StorageError::Unsupported(format!(
            "postgres backend: upsert_job_version is not implemented yet"
        )))
    }

    pub async fn list_job_versions(&self, job_id: &str) -> Result<Vec<JobVersionRecord>, StorageError> {
        let _ = (&job_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: list_job_versions is not implemented yet"
        )))
    }

    pub async fn list_jobs(&self, ) -> Result<Vec<JobRecord>, StorageError> {
        let rows = sqlx::query(&format!("{JOB_COLUMNS} ORDER BY updated_at_ms DESC, job_id LIMIT 4096"))
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| pg_row_to_job(&row))
            .collect::<Result<Vec<_>, StorageError>>()?)
    }

    pub async fn update_job(&self, job_id: &str, desired_state: Option<&str>, observed_state: Option<&str>, convergence: Option<&str>, generation: Option<u64>, checkpoint_id: Option<&str>, last_error: Option<&str>) -> Result<Option<JobRecord>, StorageError> {
        let _ = (&job_id, &desired_state, &observed_state, &convergence, &generation, &checkpoint_id, &last_error);
        Err(StorageError::Unsupported(format!(
            "postgres backend: update_job is not implemented yet"
        )))
    }

    pub async fn update_job_observation(&self, job_id: &str, observed_state: &str, convergence: &str, generation: u64, expected_generation: u64, checkpoint_id: Option<&str>, last_error: Option<&str>) -> Result<Option<JobRecord>, StorageError> {
        let _ = (&job_id, &observed_state, &convergence, &generation, &expected_generation, &checkpoint_id, &last_error);
        Err(StorageError::Unsupported(format!(
            "postgres backend: update_job_observation is not implemented yet"
        )))
    }

    pub async fn update_job_desired_state(&self, job_id: &str, desired_state: &str, expected_generation: u64) -> Result<Option<JobRecord>, StorageError> {
        let _ = (&job_id, &desired_state, &expected_generation);
        Err(StorageError::Unsupported(format!(
            "postgres backend: update_job_desired_state is not implemented yet"
        )))
    }

    pub async fn upsert_job_checkpoint(&self, record: JobCheckpointRecord) -> Result<(), StorageError> {
        let _ = (&record);
        Err(StorageError::Unsupported(format!(
            "postgres backend: upsert_job_checkpoint is not implemented yet"
        )))
    }

    pub async fn list_job_checkpoints(&self, job_id: &str) -> Result<Vec<JobCheckpointRecord>, StorageError> {
        let _ = (&job_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: list_job_checkpoints is not implemented yet"
        )))
    }

    pub async fn delete_job_checkpoint(&self, job_id: &str, checkpoint_id: &str) -> Result<(), StorageError> {
        let _ = (&job_id, &checkpoint_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: delete_job_checkpoint is not implemented yet"
        )))
    }
}

#[cfg(test)]
mod live_tests {
    use super::*;

    fn pg_url() -> Option<String> {
        std::env::var("ARKFLOW_TEST_POSTGRES_URL").ok()
    }

    async fn open_or_skip() -> Option<PgStore> {
        let url = pg_url()?;
        Some(PgStore::open(&url).await.expect("pg open"))
    }

    /// The live container keeps data across runs; the smoke owns it, so start
    /// from a clean slate. NEXT_ID is process-local and would otherwise
    /// collide with intents persisted by a previous run.
    async fn clean_slate(store: &PgStore) {
        for table in [
            "cp_outbox",
            "cp_attempts",
            "cp_events",
            "cp_audit_events",
            "cp_rollout_targets",
            "cp_rollouts",
            "cp_operations",
            "cp_intents",
            "cp_stream_observed",
            "cp_stream_desired",
            "cp_config_versions",
            "cp_job_checkpoints",
            "cp_job_tasks",
            "cp_job_versions",
            "cp_jobs",
            "cp_nodes",
        ] {
            sqlx::query(&format!("DELETE FROM {table}"))
                .execute(&store.pool)
                .await
                .expect("clean slate");
        }
    }

    fn sample_job(job_id: &str) -> JobRecord {
        JobRecord {
            job_id: job_id.to_string(),
            version: 3,
            spec_json: "{}".to_string(),
            desired_state: "running".to_string(),
            observed_state: "starting".to_string(),
            convergence: "converging".to_string(),
            generation: 0,
            node_ids: vec!["node-a".to_string()],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 1_000,
        }
    }

    #[tokio::test]
    async fn jobs_group_smoke() {
        let Some(store) = open_or_skip().await else {
            eprintln!("skipping: ARKFLOW_TEST_POSTGRES_URL not set");
            return;
        };
        clean_slate(&store).await;
        // Unique per run: the live database keeps data across runs.
        let job_id = format!("smoke-job-{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
        let job = sample_job(&job_id);
        let stored = store.upsert_job(job).await.expect("upsert");
        assert_eq!(stored.generation, 1, "first write bumps generation to 1");
        assert_eq!(stored.node_ids, vec!["node-a".to_string()]);

        // Second upsert bumps generation again (reads 1, writes 2).
        let stored = store
            .upsert_job(sample_job(&job_id))
            .await
            .expect("upsert 2");
        assert_eq!(stored.generation, 2);

        let fetched = store.get_job(&job_id).await.expect("get").expect("exists");
        assert_eq!(fetched.generation, 2);
        assert_eq!(fetched.desired_state, "running");

        let listed = store.list_jobs().await.expect("list");
        assert!(listed.iter().any(|job| job.job_id == job_id));

        // CAS update with the correct expected generation succeeds and bumps.
        let mut cas = stored.clone();
        cas.desired_state = "stopped".to_string();
        let updated = store
            .update_job_with_expected_generation(cas, 2)
            .await
            .expect("cas update");
        assert_eq!(updated.generation, 3);
        assert_eq!(updated.desired_state, "stopped");

        // CAS with a stale generation conflicts.
        let mut stale = stored;
        stale.desired_state = "running".to_string();
        let error = store
            .update_job_with_expected_generation(stale, 2)
            .await
            .expect_err("stale generation must conflict");
        assert!(error.to_string().contains("generation conflict"), "{error}");

        // ---- nodes / streams group ----
        let node_id = format!(
            "node-{}",
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        );
        let stream_id = format!(
            "demo-{}",
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        );
        let mutation = DesiredMutation {
            node_id: node_id.clone(),
            stream_id: stream_id.clone(),
            desired_state: "running".to_string(),
            config_version_id: Some("cfg-1".to_string()),
            action_id: Some("apply-1".to_string()),
            payload_json: Some("{}".to_string()),
            expected_generation: None,
            intent_type: None,
            idempotency_key: Some("idem-1".to_string()),
            actor: Some("tester".to_string()),
            correlation_id: Some("corr-1".to_string()),
        };
        let intent = store.set_desired(mutation.clone()).await.expect("set_desired");
        assert_eq!(intent.generation, 1);
        assert_eq!(intent.state, "accepted");

        // Idempotent replay returns the same intent.
        let replay = store.set_desired(mutation.clone()).await.expect("replay");
        assert_eq!(replay.intent_id, intent.intent_id);

        let desired = store.get_desired(&node_id, &stream_id).await.expect("get").expect("exists");
        assert_eq!(desired.generation, 1);
        assert_eq!(desired.desired_state, "running");

        // An observed report matching the desired state converges the intent.
        store.record_observed(ObservedMutation {
            node_id: node_id.clone(),
            stream_id: stream_id.clone(),
            boot_id: Some("boot-1".to_string()),
            report_seq: 1,
            observed_generation: Some(1),
            observed_state: "running".to_string(),
            config_version_id: Some("cfg-1".to_string()),
            action_id: Some("apply-1".to_string()),
            last_error_code: None,
            last_error_message: None,
            snapshot_json: "{}".to_string(),
        }).await.expect("record_observed");

        let converged = store.get_intent(&intent.intent_id).await.expect("get").expect("exists");
        assert_eq!(converged.state, "converged");

        store.upsert_node(NodeMutation {
            node_id: node_id.clone(),
            version: "test".to_string(),
            state: "online".to_string(),
            capabilities_json: "[]".to_string(),
            boot_id: Some("boot-1".to_string()),
            report_seq: Some(1),
            last_seen_at_ms: 1_000,
            lease_expires_at_ms: 2_000,
            maintenance_state: None,
            maintenance_updated_at_ms: None,
        }).await.expect("upsert_node");

        let maintenance = store
            .set_node_maintenance(
                NodeMaintenanceMutation {
                    node_id: node_id.clone(),
                    state: "draining".to_string(),
                    actor: Some("tester".to_string()),
                    correlation_id: Some("corr-m".to_string()),
                },
                5_000,
            )
            .await
            .expect("maintenance");
        assert!(maintenance, "first transition applies");
        let state = store.get_node_maintenance(&node_id).await.expect("get").expect("exists");
        assert_eq!(state, "draining");

        let aggregates = store.operational_aggregates(10_000).await.expect("aggregates");
        assert!(aggregates.node_states.iter().any(|(state, _)| state == "online"));

        store.reset_observed_cursors(&node_id).await.expect("reset");
        store.wake_node(&node_id, 20_000).await.expect("wake");

        // ---- attempts group ----
        let stream_id2 = format!(
            "att-{}",
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        );
        let intent = store
            .set_desired(DesiredMutation {
                node_id: "att-node".to_string(),
                stream_id: stream_id2.clone(),
                desired_state: "running".to_string(),
                config_version_id: None,
                action_id: None,
                payload_json: None,
                expected_generation: None,
                intent_type: None,
                idempotency_key: None,
                actor: None,
                correlation_id: None,
            })
            .await
            .expect("set_desired");
        let attempt = store
            .claim_attempt(&intent.intent_id)
            .await
            .expect("claim")
            .expect("attempt created");
        assert_eq!(attempt.state, "queued");
        assert_eq!(attempt.operation, "start");
        // Re-claim returns the same active attempt.
        let again = store
            .claim_attempt(&intent.intent_id)
            .await
            .expect("re-claim")
            .expect("existing attempt");
        assert_eq!(again.attempt_id, attempt.attempt_id);

        store
            .mark_attempt_dispatched(&attempt.attempt_id, 10_000)
            .await
            .expect("dispatch");

        store
            .complete_attempt(&attempt.attempt_id, "succeeded", None)
            .await
            .expect("complete");
        // Completing the attempt terminates it; the intent itself converges
        // through record_observed only - until that report arrives, a re-claim
        // legitimately creates a fresh attempt (at-least-once control loop).
        let after = store
            .get_intent(&intent.intent_id)
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(
            after.state, "accepted",
            "a succeeded attempt alone does not converge the intent"
        );

        // Expired attempt becomes ambiguous and degrades the intent.
        let intent2 = store
            .set_desired(DesiredMutation {
                node_id: "att-node".to_string(),
                stream_id: format!("{stream_id2}-2"),
                desired_state: "running".to_string(),
                config_version_id: None,
                action_id: None,
                payload_json: None,
                expected_generation: None,
                intent_type: None,
                idempotency_key: None,
                actor: None,
                correlation_id: None,
            })
            .await
            .expect("set_desired 2");
        let attempt2 = store
            .claim_attempt(&intent2.intent_id)
            .await
            .expect("claim")
            .expect("attempt created");
        store
            .mark_attempt_dispatched(&attempt2.attempt_id, 1)
            .await
            .expect("dispatch 2");
        let expired = store.expire_attempts(2).await.expect("expire");
        assert!(expired >= 1, "expiry must catch the dispatched attempt");
        let degraded = store
            .get_intent(&intent2.intent_id)
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(degraded.state, "converging");
        assert_eq!(degraded.convergence_state, "degraded");
    }
}
