//! SQLite backend for the Hub control-plane store.
//!
//! Bodies moved verbatim from the former `storage.rs` monolith; the
//! `StorageBackend` delegation below is a mechanical async shim so the FIFO
//! actor can drive either backend through one contract.
use super::*;
use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior};
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct SqliteBackend {
    connection: Arc<Mutex<Connection>>,
}

fn row_to_job(row: &Row<'_>) -> rusqlite::Result<JobRecord> {
    let node_ids_json: String = row.get(7)?;
    let node_ids = serde_json::from_str(&node_ids_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(7, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(JobRecord {
        job_id: row.get(0)?,
        version: row.get(1)?,
        spec_json: row.get(2)?,
        desired_state: row.get(3)?,
        observed_state: row.get(4)?,
        convergence: row.get(5)?,
        generation: row.get(6)?,
        node_ids,
        checkpoint_id: row.get(8)?,
        last_error: row.get(9)?,
        updated_at_ms: row.get(10)?,
    })
}

impl SqliteBackend {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        let store = Self {
            connection: Arc::new(Mutex::new(connection)),
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn in_memory() -> Result<Self, StorageError> {
        Self::open(":memory:")
    }

    pub fn with_connection<T>(
        &self,
        operation: impl FnOnce(&Connection) -> Result<T, rusqlite::Error>,
    ) -> Result<T, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(operation(&connection)?)
    }

    /// Execute one short state transition under SQLite's single-writer lock.
    /// Callers must not perform network or other awaitable work in `operation`.
    pub fn immediate_transaction<T>(
        &self,
        operation: impl FnOnce(&Transaction<'_>) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let mut connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = operation(&transaction)?;
        transaction.commit()?;
        Ok(result)
    }

    /// Persist one desired-state mutation and its reconciliation wake-up as a
    /// single transaction. Dispatch is intentionally left to the reconciler.
    pub fn set_desired(&self, mutation: DesiredMutation) -> Result<IntentRecord, StorageError> {
        let now = now_ms();
        self.immediate_transaction(|transaction| {
            let intent_type = mutation.intent_type.as_deref().unwrap_or_else(|| {
                if mutation.action_id.is_some() {
                    "restart"
                } else {
                    "set_state"
                }
            });
            if let Some(idempotency_key) = mutation.idempotency_key.as_deref() {
                let existing: Option<(IntentRecord, Option<String>, Option<String>)> = transaction
                    .query_row(
                        "SELECT i.intent_id, i.node_id, i.stream_id, i.generation, i.state, i.desired_state, i.config_version_id, i.action_id, i.convergence_state, i.intent_type, i.payload_json, i.retry_count, i.next_retry_at_ms, i.last_failure_class, i.superseded_by_intent_id, i.created_at_ms, i.updated_at_ms, o.observed_generation, o.observed_state, (SELECT generation FROM cp_intents s WHERE s.intent_id = i.superseded_by_intent_id) FROM cp_intents i LEFT JOIN cp_stream_observed o ON o.node_id = i.node_id AND o.stream_id = i.stream_id WHERE i.node_id = ?1 AND i.stream_id = ?2 AND i.idempotency_key = ?3",
                        (&mutation.node_id, &mutation.stream_id, idempotency_key),
                        |row| {
                            Ok((
                                IntentRecord {
                                    intent_id: row.get(0)?,
                                    node_id: row.get(1)?,
                                    stream_id: row.get(2)?,
                                    generation: row.get(3)?,
                                    state: row.get(4)?,
                                    desired_state: row.get(5)?,
                                    config_version_id: row.get(6)?,
                                    action_id: row.get(7)?,
                                    convergence_state: row.get(8)?,
                                    retry_count: row.get(11)?,
                                    next_retry_at_ms: row.get(12)?,
                                    failure_class: row.get(13)?,
                                    superseded_by_intent_id: row.get(14)?,
                                    superseded_generation: row.get(19)?,
                                    created_at_ms: row.get(15)?,
                                    updated_at_ms: row.get(16)?,
                                    observed_generation: row.get(17)?,
                                    observed_state: row.get(18)?,
                                },
                                row.get(9)?,
                                row.get(10)?,
                            ))
                        },
                    )
                    .optional()?;
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
                    return Ok(existing);
                }
            }
            let current: u64 = transaction
                .query_row(
                    "SELECT generation FROM cp_stream_desired WHERE node_id = ?1 AND stream_id = ?2",
                    (&mutation.node_id, &mutation.stream_id),
                    |row| row.get(0),
                )
                .optional()?
                .unwrap_or(0);
            if let Some(expected) = mutation.expected_generation {
                if expected != current {
                    return Err(StorageError::GenerationConflict { expected, current });
                }
            }
            let generation = current + 1;
            let intent_id = format!("intent-{generation}-{}", NEXT_ID.fetch_add(1, Ordering::Relaxed));
            transaction.execute(
                "INSERT INTO cp_stream_desired (node_id, stream_id, generation, desired_state, config_version_id, desired_action_id, updated_at_ms, updated_by, correlation_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) ON CONFLICT(node_id, stream_id) DO UPDATE SET generation = excluded.generation, desired_state = excluded.desired_state, config_version_id = excluded.config_version_id, desired_action_id = excluded.desired_action_id, updated_at_ms = excluded.updated_at_ms, updated_by = excluded.updated_by, correlation_id = excluded.correlation_id",
                rusqlite::params![
                    mutation.node_id,
                    mutation.stream_id,
                    generation,
                    mutation.desired_state,
                    mutation.config_version_id,
                    mutation.action_id,
                    now,
                    mutation.actor,
                    mutation.correlation_id,
                ],
            )?;
            if let (Some(config_version_id), Some(payload_json)) = (
                mutation.config_version_id.as_deref(),
                mutation.payload_json.as_deref(),
            ) {
                transaction.execute(
                    "INSERT OR IGNORE INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms, created_by, correlation_id) VALUES (?1, 'inline-json', ?2, 'json', ?3, ?4, ?5)",
                    rusqlite::params![
                        config_version_id,
                        payload_json,
                        now,
                        mutation.actor,
                        mutation.correlation_id,
                    ],
                )?;
            }
            transaction.execute(
                "INSERT INTO cp_intents (intent_id, node_id, stream_id, generation, intent_type, desired_state, config_version_id, action_id, payload_json, state, convergence_state, created_at_ms, updated_at_ms, actor, correlation_id, idempotency_key) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'accepted', 'pending', ?10, ?10, ?11, ?12, ?13)",
                rusqlite::params![
                    intent_id,
                    mutation.node_id,
                    mutation.stream_id,
                    generation,
                    intent_type,
                    mutation.desired_state,
                    mutation.config_version_id,
                    mutation.action_id,
                    mutation.payload_json,
                    now,
                    mutation.actor,
                    mutation.correlation_id,
                    mutation.idempotency_key,
                ],
            )?;
            transaction.execute(
                "UPDATE cp_intents SET state = 'superseded', convergence_state = 'pending', superseded_by_intent_id = ?1, updated_at_ms = ?2 WHERE node_id = ?3 AND stream_id = ?4 AND state IN ('accepted', 'converging', 'retrying') AND generation < ?5",
                (&intent_id, now, &mutation.node_id, &mutation.stream_id, generation),
            )?;
            let event_key = format!("reconcile:{intent_id}:{generation}");
            transaction.execute(
                "INSERT INTO cp_outbox (event_key, event_type, node_id, stream_id, intent_id, available_at_ms, created_at_ms) VALUES (?1, 'reconcile_intent', ?2, ?3, ?4, ?5, ?5)",
                (&event_key, &mutation.node_id, &mutation.stream_id, &intent_id, now),
            )?;
            transaction.execute(
                "INSERT INTO cp_events (node_id, stream_id, intent_id, event_type, outcome, generation, correlation_id, occurred_at_ms) VALUES (?1, ?2, ?3, 'intent_created', 'accepted', ?4, ?5, ?6)",
                (&mutation.node_id, &mutation.stream_id, &intent_id, generation, &mutation.correlation_id, now),
            )?;
            transaction.execute(
                "INSERT INTO cp_audit_events (actor, action, resource_type, resource_id, node_id, stream_id, correlation_id, outcome, occurred_at_ms) VALUES (?1, ?2, 'stream', ?3, ?4, ?5, ?6, 'accepted', ?7)",
                rusqlite::params![
                    mutation.actor,
                    intent_type,
                    format!("{}:{}", mutation.node_id, mutation.stream_id),
                    mutation.node_id,
                    mutation.stream_id,
                    mutation.correlation_id,
                    now,
                ],
            )?;
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
        })
    }

    pub fn upsert_node(&self, mutation: NodeMutation) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "INSERT INTO cp_nodes (node_id, role, protocol_version, node_version, state, capabilities_json, boot_id, last_report_seq, last_seen_at_ms, lease_expires_at_ms, maintenance_state, maintenance_updated_at_ms, created_at_ms, updated_at_ms) VALUES (?1, 'compute', 'v1', ?2, ?3, ?4, ?5, ?6, ?7, ?8, COALESCE(?9, 'active'), ?10, ?7, ?7) ON CONFLICT(node_id) DO UPDATE SET node_version = excluded.node_version, state = excluded.state, capabilities_json = excluded.capabilities_json, boot_id = excluded.boot_id, last_report_seq = excluded.last_report_seq, last_seen_at_ms = excluded.last_seen_at_ms, lease_expires_at_ms = excluded.lease_expires_at_ms, updated_at_ms = excluded.updated_at_ms",
                rusqlite::params![
                    mutation.node_id,
                    mutation.version,
                    mutation.state,
                    mutation.capabilities_json,
                    mutation.boot_id,
                    mutation.report_seq,
                    mutation.last_seen_at_ms,
                    mutation.lease_expires_at_ms,
                    mutation.maintenance_state,
                    mutation.maintenance_updated_at_ms,
                ],
            )?;
            Ok(())
        })
    }

    /// Reset every per-stream report cursor for a node. The Agent restarts
    /// `report_seq` from 1 on each session rebuild (register), so a cursor
    /// left at the previous session's high-water mark would silently drop
    /// every new report until the node caught up to it — blinding stream
    /// convergence, configuration rollout, and reconcile for the whole
    /// previous session's duration.
    pub fn reset_observed_cursors(&self, node_id: &str) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "UPDATE cp_stream_observed SET report_seq = 0 WHERE node_id = ?1",
                rusqlite::params![node_id],
            )?;
            Ok(())
        })
    }

    pub fn set_node_maintenance(
        &self,
        mutation: NodeMaintenanceMutation,
        now_ms: u64,
    ) -> Result<bool, StorageError> {
        let state = mutation.state.as_str();
        if !matches!(state, "active" | "draining" | "maintenance") {
            return Ok(false);
        }
        self.immediate_transaction(|transaction| {
            let previous: Option<String> = transaction
                .query_row(
                    "SELECT COALESCE(maintenance_state, 'active') FROM cp_nodes WHERE node_id = ?1",
                    [&mutation.node_id],
                    |row| row.get(0),
                )
                .optional()?;
            let Some(previous) = previous else { return Ok(false) };
            if previous != state {
                transaction.execute(
                    "UPDATE cp_nodes SET maintenance_state = ?1, maintenance_updated_at_ms = ?2, updated_at_ms = ?2 WHERE node_id = ?3",
                    rusqlite::params![state, now_ms, mutation.node_id],
                )?;
                transaction.execute(
                    "INSERT INTO cp_events (node_id, event_type, outcome, message, correlation_id, actor, occurred_at_ms) VALUES (?1, 'node_maintenance_changed', 'succeeded', ?2, ?3, ?4, ?5)",
                    rusqlite::params![mutation.node_id, format!("{previous}->{state}"), mutation.correlation_id, mutation.actor, now_ms],
                )?;
                transaction.execute(
                    "INSERT INTO cp_audit_events (actor, action, resource_type, resource_id, node_id, correlation_id, outcome, message, occurred_at_ms) VALUES (?1, 'node.maintenance', 'node', ?2, ?2, ?3, 'accepted', ?4, ?5)",
                    rusqlite::params![mutation.actor, mutation.node_id, mutation.correlation_id, format!("{previous}->{state}"), now_ms],
                )?;
            }
            Ok(true)
        })
    }

    pub fn get_node_maintenance(&self, node_id: &str) -> Result<Option<String>, StorageError> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT COALESCE(maintenance_state, 'active') FROM cp_nodes WHERE node_id = ?1",
                    [node_id],
                    |row| row.get(0),
                )
                .optional()
        })
    }

    pub fn operational_aggregates(
        &self,
        now_ms: u64,
    ) -> Result<OperationalAggregates, StorageError> {
        self.with_connection(|connection| {
            let grouped = |sql: &str| -> Result<Vec<(String, u64)>, rusqlite::Error> {
                let mut statement = connection.prepare(sql)?;
                let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u64)))?;
                rows.collect()
            };
            let scalar = |sql: &str| -> Result<u64, rusqlite::Error> {
                connection.query_row(sql, [], |row| row.get::<_, i64>(0)).map(|v| v as u64)
            };
            let oldest: Option<i64> = connection.query_row(
                "SELECT MIN(created_at_ms) FROM cp_outbox WHERE processed_at_ms IS NULL", [], |row| row.get(0)
            ).optional()?.flatten();
            Ok(OperationalAggregates {
                node_states: grouped("SELECT state, COUNT(*) FROM cp_nodes GROUP BY state")?,
                maintenance_states: grouped("SELECT COALESCE(maintenance_state, 'active'), COUNT(*) FROM cp_nodes GROUP BY COALESCE(maintenance_state, 'active')")?,
                intent_states: grouped("SELECT state, COUNT(*) FROM cp_intents GROUP BY state")?,
                convergence_states: grouped("SELECT convergence_state, COUNT(*) FROM cp_intents GROUP BY convergence_state")?,
                attempt_states: grouped("SELECT state, COUNT(*) FROM cp_attempts GROUP BY state")?,
                failure_classes: grouped("SELECT COALESCE(last_failure_class, 'none'), COUNT(*) FROM cp_intents GROUP BY COALESCE(last_failure_class, 'none')")?,
                outbox_pending: scalar("SELECT COUNT(*) FROM cp_outbox WHERE processed_at_ms IS NULL")?,
                outbox_claimed: scalar("SELECT COUNT(*) FROM cp_outbox WHERE processed_at_ms IS NULL AND claimed_at_ms IS NOT NULL")?,
                stale_nodes: connection.query_row("SELECT COUNT(*) FROM cp_nodes WHERE state = 'stale' OR lease_expires_at_ms <= ?1", [now_ms], |row| row.get::<_, i64>(0)).map(|v| v as u64)?,
                active_attempts: scalar("SELECT COUNT(*) FROM cp_attempts WHERE state IN ('queued','dispatched','acknowledged','running')")?,
                non_terminal_intents: scalar("SELECT COUNT(*) FROM cp_intents WHERE state IN ('accepted','converging','retrying')")?,
                oldest_pending_age_seconds: oldest.map(|created| now_ms.saturating_sub(created as u64) / 1000),
            })
        })
    }

    #[allow(clippy::type_complexity)]
    pub fn claim_outbox(
        &self,
        worker_id: &str,
        now_ms: u64,
    ) -> Result<Option<OutboxRecord>, StorageError> {
        const CLAIM_LEASE_MS: u64 = 30_000;
        self.immediate_transaction(|transaction| {
            let candidate: Option<(i64, String, String, String, Option<String>, Option<String>)> =
                transaction
                    .query_row(
                        "SELECT outbox_id, event_key, event_type, node_id, stream_id, intent_id FROM cp_outbox WHERE processed_at_ms IS NULL AND available_at_ms <= ?1 AND (claimed_at_ms IS NULL OR claimed_at_ms < ?2) ORDER BY outbox_id LIMIT 1",
                        rusqlite::params![now_ms, now_ms.saturating_sub(CLAIM_LEASE_MS)],
                        |row| {
                            Ok((
                                row.get(0)?,
                                row.get(1)?,
                                row.get(2)?,
                                row.get(3)?,
                                row.get(4)?,
                                row.get(5)?,
                            ))
                        },
                    )
                    .optional()?;
            let Some((outbox_id, event_key, event_type, node_id, stream_id, intent_id)) = candidate
            else {
                return Ok(None);
            };
            let updated = transaction.execute(
                "UPDATE cp_outbox SET claimed_at_ms = ?1, worker_id = ?2 WHERE outbox_id = ?3 AND processed_at_ms IS NULL AND (claimed_at_ms IS NULL OR claimed_at_ms < ?4)",
                rusqlite::params![now_ms, worker_id, outbox_id, now_ms.saturating_sub(CLAIM_LEASE_MS)],
            )?;
            if updated != 1 {
                return Ok(None);
            }
            Ok(Some(OutboxRecord {
                outbox_id,
                event_key,
                event_type,
                node_id,
                stream_id,
                intent_id,
            }))
        })
    }

    pub fn get_desired(
        &self,
        node_id: &str,
        stream_id: &str,
    ) -> Result<Option<DesiredRecord>, StorageError> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT node_id, stream_id, generation, desired_state, config_version_id, desired_action_id, correlation_id FROM cp_stream_desired WHERE node_id = ?1 AND stream_id = ?2",
                    (node_id, stream_id),
                    |row| {
                        Ok(DesiredRecord {
                            node_id: row.get(0)?,
                            stream_id: row.get(1)?,
                            generation: row.get(2)?,
                            desired_state: row.get(3)?,
                            config_version_id: row.get(4)?,
                            action_id: row.get(5)?,
                            correlation_id: row.get(6)?,
                        })
                    },
                )
                .optional()
        })
    }

    pub fn get_intent(&self, intent_id: &str) -> Result<Option<IntentRecord>, StorageError> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT i.intent_id, i.node_id, i.stream_id, i.generation, i.state, i.desired_state, i.config_version_id, i.action_id, i.convergence_state, i.retry_count, i.next_retry_at_ms, i.last_failure_class, i.superseded_by_intent_id, i.created_at_ms, i.updated_at_ms, o.observed_generation, o.observed_state, (SELECT generation FROM cp_intents s WHERE s.intent_id = i.superseded_by_intent_id) FROM cp_intents i LEFT JOIN cp_stream_observed o ON o.node_id = i.node_id AND o.stream_id = i.stream_id WHERE i.intent_id = ?1",
                    [intent_id],
                    |row| {
                        Ok(IntentRecord {
                            intent_id: row.get(0)?,
                            node_id: row.get(1)?,
                            stream_id: row.get(2)?,
                            generation: row.get(3)?,
                            state: row.get(4)?,
                            desired_state: row.get(5)?,
                            config_version_id: row.get(6)?,
                            action_id: row.get(7)?,
                            convergence_state: row.get(8)?,
                            retry_count: row.get(9)?,
                            next_retry_at_ms: row.get(10)?,
                            failure_class: row.get(11)?,
                            superseded_by_intent_id: row.get(12)?,
                            superseded_generation: row.get(17)?,
                            created_at_ms: row.get(13)?,
                            updated_at_ms: row.get(14)?,
                            observed_generation: row.get(15)?,
                            observed_state: row.get(16)?,
                        })
                    },
                )
                .optional()
        })
    }

    pub fn list_intents(&self, node_id: Option<&str>) -> Result<Vec<IntentRecord>, StorageError> {
        let ids = self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT intent_id FROM cp_intents WHERE (?1 IS NULL OR node_id = ?1) ORDER BY created_at_ms DESC, intent_id DESC LIMIT 4096",
            )?;
            let rows = statement.query_map([node_id], |row| row.get::<_, String>(0))?;
            rows.collect::<Result<Vec<_>, _>>()
        })?;
        let mut intents = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(intent) = self.get_intent(&id)? {
                intents.push(intent);
            }
        }
        Ok(intents)
    }

    pub fn recover_reconciliation(&self, now_ms: u64) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "INSERT INTO cp_outbox (event_key, event_type, node_id, stream_id, intent_id, available_at_ms, created_at_ms) SELECT 'reconcile:recovery:' || i.intent_id || ':' || ?1, 'reconcile_intent', i.node_id, i.stream_id, i.intent_id, ?1, ?1 FROM cp_intents i WHERE i.state IN ('accepted', 'converging', 'retrying') AND (i.last_failure_class IS NULL OR i.last_failure_class <> 'ambiguous') AND NOT EXISTS (SELECT 1 FROM cp_outbox o WHERE o.intent_id = i.intent_id AND o.processed_at_ms IS NULL)",
                [now_ms],
            )?;
            Ok(())
        })
    }

    pub fn wake_node(&self, node_id: &str, now_ms: u64) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "INSERT OR IGNORE INTO cp_outbox (event_key, event_type, node_id, stream_id, intent_id, available_at_ms, created_at_ms) SELECT 'reconcile:register:' || i.intent_id, 'reconcile_intent', i.node_id, i.stream_id, i.intent_id, ?1, ?1 FROM cp_intents i WHERE i.node_id = ?2 AND i.state IN ('accepted', 'converging', 'retrying') AND (i.last_failure_class IS NULL OR i.last_failure_class <> 'ambiguous') AND NOT EXISTS (SELECT 1 FROM cp_outbox o WHERE o.intent_id = i.intent_id AND o.processed_at_ms IS NULL)",
                rusqlite::params![now_ms, node_id],
            )?;
            Ok(())
        })
    }

    pub fn list_events(&self, node_id: Option<&str>) -> Result<Vec<StoredEvent>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT event_id, node_id, stream_id, intent_id, attempt_id, event_type, outcome, failure_class, message, generation, correlation_id, occurred_at_ms, actor FROM cp_events WHERE (?1 IS NULL OR node_id = ?1) ORDER BY event_id DESC LIMIT 2048",
            )?;
            let rows = statement.query_map([node_id], |row| {
                Ok(StoredEvent {
                    event_id: row.get(0)?,
                    node_id: row.get(1)?,
                    stream_id: row.get(2)?,
                    intent_id: row.get(3)?,
                    attempt_id: row.get(4)?,
                    event_type: row.get(5)?,
                    outcome: row.get(6)?,
                    failure_class: row.get(7)?,
                    message: row.get(8)?,
                    generation: row.get(9)?,
                    correlation_id: row.get(10)?,
                    occurred_at_ms: row.get(11)?,
                    actor: row.get(12)?,
                })
            })?;
            rows.collect()
        })
    }

    pub fn prune_events(&self, retain: usize) -> Result<usize, StorageError> {
        self.immediate_transaction(|transaction| {
            let deleted = transaction.execute(
                "DELETE FROM cp_events WHERE event_id NOT IN (SELECT event_id FROM cp_events ORDER BY event_id DESC LIMIT ?1)",
                [retain as i64],
            )?;
            Ok(deleted)
        })
    }

    pub fn prune_operation_history(
        &self,
        older_than_ms: i64,
        max_retained: i64,
    ) -> Result<usize, StorageError> {
        self.immediate_transaction(|transaction| {
            // Keep the newest durable start fact for each Job.  A successful
            // start can later be rewritten to NodeUnavailable with the
            // recovery_required failure class when an Agent process is
            // replaced; both forms are recovery evidence and must survive
            // operation-history retention.
            let protected = "operation = 'job_start' AND (state = 'succeeded' OR operation_json LIKE '%\"failure_class\":\"recovery_required\"%') AND NOT EXISTS (SELECT 1 FROM cp_operations newer WHERE newer.resource_id = cp_operations.resource_id AND newer.operation = 'job_start' AND (newer.state = 'succeeded' OR newer.operation_json LIKE '%\"failure_class\":\"recovery_required\"%') AND (newer.updated_at_ms > cp_operations.updated_at_ms OR (newer.updated_at_ms = cp_operations.updated_at_ms AND newer.operation_id > cp_operations.operation_id)))";
            let mut deleted = transaction.execute(
                &format!(
                    "DELETE FROM cp_operations WHERE state NOT IN ('queued', 'dispatched', 'acknowledged', 'running') AND updated_at_ms < ?1 AND NOT ({protected})"
                ),
                [older_than_ms],
            )?;
            // Count bound: keep the newest `max_retained` terminal rows when
            // long-lived deployments accumulate faster than the age window
            // reclaims them.
            deleted += transaction.execute(
                &format!(
                    "DELETE FROM cp_operations WHERE state NOT IN ('queued', 'dispatched', 'acknowledged', 'running') AND operation_id NOT IN (SELECT operation_id FROM cp_operations WHERE state NOT IN ('queued', 'dispatched', 'acknowledged', 'running') ORDER BY updated_at_ms DESC, operation_id DESC LIMIT ?1) AND NOT ({protected})"
                ),
                [max_retained],
            )?;
            Ok(deleted)
        })
    }

    pub fn prune_job_checkpoint_records(&self, older_than_ms: i64) -> Result<usize, StorageError> {
        self.immediate_transaction(|transaction| {
            let deleted = transaction.execute(
                "DELETE FROM cp_job_checkpoints WHERE status IN ('pending', 'failed') AND updated_at_ms < ?1",
                [older_than_ms],
            )?;
            Ok(deleted)
        })
    }

    /// Reclaim audit history by age and count bound. Recent records within
    /// both bounds always survive; the trail stays queryable but bounded.
    pub fn prune_audit_events(
        &self,
        older_than_ms: i64,
        max_retained: i64,
    ) -> Result<usize, StorageError> {
        self.immediate_transaction(|transaction| {
            let mut deleted = transaction.execute(
                "DELETE FROM cp_audit_events WHERE occurred_at_ms < ?1",
                [older_than_ms],
            )?;
            deleted += transaction.execute(
                "DELETE FROM cp_audit_events WHERE event_id NOT IN (SELECT event_id FROM cp_audit_events ORDER BY event_id DESC LIMIT ?1)",
                [max_retained],
            )?;
            Ok(deleted)
        })
    }

    /// Reclaim processed outbox rows by age and count bound. Rows still
    /// awaiting processing (pending or claimed) are never reclaimed.
    pub fn prune_processed_outbox(
        &self,
        older_than_ms: i64,
        max_retained: i64,
    ) -> Result<usize, StorageError> {
        self.immediate_transaction(|transaction| {
            let mut deleted = transaction.execute(
                "DELETE FROM cp_outbox WHERE processed_at_ms IS NOT NULL AND processed_at_ms < ?1",
                [older_than_ms],
            )?;
            deleted += transaction.execute(
                "DELETE FROM cp_outbox WHERE processed_at_ms IS NOT NULL AND outbox_id NOT IN (SELECT outbox_id FROM cp_outbox WHERE processed_at_ms IS NOT NULL ORDER BY processed_at_ms DESC, outbox_id DESC LIMIT ?1)",
                [max_retained],
            )?;
            Ok(deleted)
        })
    }

    /// Reclaim terminal Attempt records by age and count bound. Active
    /// attempts are protected by both the state predicate and the
    /// `cp_one_active_attempt` unique index.
    pub fn prune_terminal_attempts(
        &self,
        older_than_ms: i64,
        max_retained: i64,
    ) -> Result<usize, StorageError> {
        self.immediate_transaction(|transaction| {
            let mut deleted = transaction.execute(
                "DELETE FROM cp_attempts WHERE state NOT IN ('queued', 'dispatched', 'acknowledged', 'running') AND COALESCE(finished_at_ms, created_at_ms) < ?1",
                [older_than_ms],
            )?;
            deleted += transaction.execute(
                "DELETE FROM cp_attempts WHERE state NOT IN ('queued', 'dispatched', 'acknowledged', 'running') AND attempt_id NOT IN (SELECT attempt_id FROM cp_attempts WHERE state NOT IN ('queued', 'dispatched', 'acknowledged', 'running') ORDER BY COALESCE(finished_at_ms, created_at_ms) DESC, attempt_id DESC LIMIT ?1)",
                [max_retained],
            )?;
            Ok(deleted)
        })
    }

    #[allow(clippy::type_complexity)]
    pub fn claim_attempt(&self, intent_id: &str) -> Result<Option<AttemptRecord>, StorageError> {
        self.immediate_transaction(|transaction| {
            if let Some(attempt) = transaction
                .query_row(
                    "SELECT a.attempt_id, a.intent_id, a.command_id, a.state, a.failure_class, a.node_id, a.stream_id, a.generation, a.operation, i.action_id, i.config_version_id, i.intent_type, COALESCE(i.payload_json, cv.content_ref) FROM cp_attempts a JOIN cp_intents i ON i.intent_id = a.intent_id LEFT JOIN cp_config_versions cv ON cv.config_version_id = i.config_version_id WHERE a.intent_id = ?1 AND a.state IN ('queued', 'dispatched', 'acknowledged', 'running') ORDER BY a.created_at_ms DESC LIMIT 1",
                    [intent_id],
                    |row| {
                        Ok(AttemptRecord {
                            attempt_id: row.get(0)?,
                            intent_id: row.get(1)?,
                            command_id: row.get(2)?,
                            state: row.get(3)?,
                            failure_class: row.get(4)?,
                            node_id: row.get(5)?,
                            stream_id: row.get(6)?,
                            generation: row.get(7)?,
                            operation: row.get(8)?,
                            action_id: row.get(9)?,
                            config_version_id: row.get(10)?,
                            payload_json: row.get(12)?,
                        })
                    },
                )
                .optional()?
            {
                return Ok(Some(attempt));
            }
            let target: Option<(
                String,
                String,
                u64,
                String,
                Option<String>,
                Option<String>,
                String,
                Option<String>,
            )> = transaction
                .query_row(
                    "SELECT i.node_id, i.stream_id, i.generation, COALESCE(i.desired_state, ''), i.action_id, i.config_version_id, i.intent_type, COALESCE(i.payload_json, cv.content_ref) FROM cp_intents i LEFT JOIN cp_config_versions cv ON cv.config_version_id = i.config_version_id WHERE i.intent_id = ?1 AND i.state IN ('accepted', 'converging', 'retrying')",
                    [intent_id],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                            row.get(6)?,
                            row.get(7)?,
                        ))
                    },
                )
                .optional()?;
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
                return Ok(None);
            };
            let operation = if intent_type == "apply_configuration" {
                "apply_configuration"
            } else if action_id.is_some() {
                "restart"
            } else if desired_state == "running" {
                "start"
            } else {
                "stop"
            };
            let suffix = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let attempt_id = format!("attempt-{suffix}");
            let command_id = format!("cmd-{suffix}");
            let now = now_ms();
            transaction.execute(
                "INSERT INTO cp_attempts (attempt_id, intent_id, command_id, node_id, stream_id, generation, operation, state, created_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'queued', ?8)",
                rusqlite::params![
                    attempt_id,
                    intent_id,
                    command_id,
                    node_id,
                    stream_id,
                    generation,
                    operation,
                    now,
                ],
            )?;
            Ok(Some(AttemptRecord {
                attempt_id,
                intent_id: intent_id.into(),
                command_id,
                state: "queued".into(),
                failure_class: None,
                node_id,
                stream_id,
                generation,
                operation: operation.into(),
                action_id,
                config_version_id,
                payload_json,
            }))
        })
    }

    pub fn complete_attempt(
        &self,
        attempt_id: &str,
        state: &str,
        failure_class: Option<&str>,
    ) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            let attempt: Option<(String, String, String, u64)> = transaction
                .query_row(
                    "SELECT intent_id, node_id, stream_id, generation FROM cp_attempts WHERE attempt_id = ?1",
                    [attempt_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            let Some((intent_id, node_id, stream_id, _generation)) = attempt else {
                return Ok(());
            };
            let ambiguous = state == "ambiguous" || failure_class == Some("ambiguous");
            let terminal = ambiguous || matches!(
                state,
                "succeeded"
                    | "failed"
                    | "timed_out"
                    | "node_unavailable"
                    | "cancelled"
                    | "superseded"
            );
            transaction.execute(
                "UPDATE cp_attempts SET state = ?1, failure_class = ?2, finished_at_ms = CASE WHEN ?3 THEN ?4 ELSE finished_at_ms END WHERE attempt_id = ?5",
                rusqlite::params![state, failure_class, terminal, now_ms(), attempt_id],
            )?;
            if terminal {
                match failure_class {
                    Some("temporary_execution") | Some("transport") | Some("node_unavailable") => {
                        let retry_at = now_ms() + 1_000;
                        transaction.execute(
                            "UPDATE cp_intents SET state = 'retrying', convergence_state = 'degraded', retry_count = retry_count + 1, next_retry_at_ms = ?1, last_failure_class = ?2, updated_at_ms = ?1 WHERE intent_id = ?3 AND state IN ('accepted', 'converging', 'retrying')",
                            rusqlite::params![retry_at, failure_class, intent_id],
                        )?;
                        let event_key = format!("reconcile:retry:{attempt_id}");
                        transaction.execute(
                            "INSERT OR IGNORE INTO cp_outbox (event_key, event_type, node_id, stream_id, intent_id, available_at_ms, created_at_ms) VALUES (?1, 'retry_intent', ?2, ?3, ?4, ?5, ?5)",
                            rusqlite::params![event_key, node_id, stream_id, intent_id, retry_at],
                        )?;
                    }
                    Some("stale_generation") => {
                        transaction.execute(
                            "UPDATE cp_intents SET state = 'superseded', convergence_state = 'degraded', last_failure_class = ?1, updated_at_ms = ?2 WHERE intent_id = ?3 AND state IN ('accepted', 'converging', 'retrying')",
                            rusqlite::params![failure_class, now_ms(), intent_id],
                        )?;
                    }
                    Some("ambiguous") => {
                        transaction.execute(
                            "UPDATE cp_intents SET state = 'converging', convergence_state = 'degraded', next_retry_at_ms = NULL, last_failure_class = ?1, updated_at_ms = ?2 WHERE intent_id = ?3 AND state IN ('accepted', 'converging', 'retrying')",
                            rusqlite::params![failure_class, now_ms(), intent_id],
                        )?;
                    }
                    Some(_) if state != "succeeded" => {
                        transaction.execute(
                            "UPDATE cp_intents SET state = 'blocked', convergence_state = 'blocked', last_failure_class = ?1, updated_at_ms = ?2 WHERE intent_id = ?3 AND state IN ('accepted', 'converging', 'retrying')",
                            rusqlite::params![failure_class, now_ms(), intent_id],
                        )?;
                    }
                    None if state != "succeeded" => {
                        transaction.execute(
                            "UPDATE cp_intents SET state = 'blocked', convergence_state = 'blocked', updated_at_ms = ?1 WHERE intent_id = ?2 AND state IN ('accepted', 'converging', 'retrying')",
                            rusqlite::params![now_ms(), intent_id],
                        )?;
                    }
                    _ => {}
                }
            }
            transaction.execute(
                "INSERT INTO cp_events (node_id, stream_id, intent_id, attempt_id, event_type, outcome, failure_class, generation, occurred_at_ms) VALUES (?1, ?2, ?3, ?4, 'attempt_completed', ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    node_id,
                    stream_id,
                    intent_id,
                    attempt_id,
                    state,
                    failure_class,
                    _generation,
                    now_ms(),
                ],
            )?;
            Ok(())
        })
    }

    pub fn mark_attempt_dispatched(
        &self,
        attempt_id: &str,
        expires_at_ms: u64,
    ) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "UPDATE cp_attempts SET state = 'dispatched', dispatched_at_ms = ?1, expires_at_ms = ?2 WHERE attempt_id = ?3 AND state = 'queued'",
                rusqlite::params![now_ms(), expires_at_ms, attempt_id],
            )?;
            Ok(())
        })
    }

    pub fn expire_attempts(&self, now_ms: u64) -> Result<usize, StorageError> {
        self.immediate_transaction(|transaction| {
            let expired: Vec<(String, String, String, String)> = transaction
                .prepare(
                    "SELECT attempt_id, intent_id, node_id, stream_id FROM cp_attempts WHERE state IN ('queued', 'dispatched', 'acknowledged', 'running') AND expires_at_ms IS NOT NULL AND expires_at_ms <= ?1",
                )?
                .query_map([now_ms], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            for (attempt_id, intent_id, node_id, stream_id) in &expired {
                transaction.execute(
                    "UPDATE cp_attempts SET state = 'ambiguous', failure_class = 'ambiguous', finished_at_ms = ?1 WHERE attempt_id = ?2 AND state IN ('queued', 'dispatched', 'acknowledged', 'running')",
                    rusqlite::params![now_ms, attempt_id],
                )?;
                transaction.execute(
                    "UPDATE cp_intents SET state = 'converging', convergence_state = 'degraded', next_retry_at_ms = NULL, last_failure_class = 'ambiguous', updated_at_ms = ?1 WHERE intent_id = ?2 AND state IN ('accepted', 'converging', 'retrying')",
                    rusqlite::params![now_ms, intent_id],
                )?;
                transaction.execute(
                    "INSERT INTO cp_events (node_id, stream_id, intent_id, event_type, outcome, failure_class, message, occurred_at_ms) VALUES (?1, ?2, ?3, 'attempt_expired', 'ambiguous', 'ambiguous', 'Attempt lease expired; waiting for a fresh observed report', ?4)",
                    rusqlite::params![node_id, stream_id, intent_id, now_ms],
                )?;
            }
            Ok(expired.len())
        })
    }

    pub fn record_observed(&self, mutation: ObservedMutation) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            let current: Option<(Option<String>, u64)> = transaction
                .query_row(
                    "SELECT boot_id, COALESCE(report_seq, 0) FROM cp_stream_observed WHERE node_id = ?1 AND stream_id = ?2",
                    (&mutation.node_id, &mutation.stream_id),
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            if let Some((boot_id, report_seq)) = current {
                match (boot_id.as_deref(), mutation.boot_id.as_deref()) {
                    // Same fencible session: the sequence cursor rejects
                    // replays (every report of a session carries a strictly
                    // increasing seq).
                    (Some(stored), Some(incoming)) if stored == incoming => {
                        if mutation.report_seq <= report_seq {
                            return Ok(());
                        }
                    }
                    // No fencible identity on either side: boot-less agents
                    // report seq 0 forever, so a seq gate here would freeze
                    // the observed state at the first report. Accept the
                    // report; convergence runs on its content.
                    (None, None) => {}
                    // The session identity changed (an agent re-registered
                    // with or without a boot id): the incoming report
                    // supersedes the stored session.
                    _ => {}
                }
            }
            let now = now_ms();
            transaction.execute(
                "INSERT INTO cp_stream_observed (node_id, stream_id, boot_id, report_seq, observed_generation, observed_state, applied_config_version, last_action_id, last_error_code, last_error_message, snapshot_json, observed_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12) ON CONFLICT(node_id, stream_id) DO UPDATE SET boot_id = excluded.boot_id, report_seq = excluded.report_seq, observed_generation = excluded.observed_generation, observed_state = excluded.observed_state, applied_config_version = excluded.applied_config_version, last_action_id = excluded.last_action_id, last_error_code = excluded.last_error_code, last_error_message = excluded.last_error_message, snapshot_json = excluded.snapshot_json, observed_at_ms = excluded.observed_at_ms",
                rusqlite::params![
                    mutation.node_id,
                    mutation.stream_id,
                    mutation.boot_id,
                    mutation.report_seq,
                    mutation.observed_generation,
                    mutation.observed_state,
                    mutation.config_version_id,
                    mutation.action_id,
                    mutation.last_error_code,
                    mutation.last_error_message,
                    mutation.snapshot_json,
                    now,
                ],
            )?;
            transaction.execute(
                "INSERT INTO cp_events (node_id, stream_id, event_type, outcome, message, generation, occurred_at_ms) VALUES (?1, ?2, 'observed_report', ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    mutation.node_id,
                    mutation.stream_id,
                    mutation.observed_state,
                    mutation.last_error_message,
                    mutation.observed_generation,
                    now,
                ],
            )?;
            let desired: Option<(u64, String, Option<String>, Option<String>)> = transaction
                .query_row(
                    "SELECT generation, desired_state, config_version_id, desired_action_id FROM cp_stream_desired WHERE node_id = ?1 AND stream_id = ?2",
                    (&mutation.node_id, &mutation.stream_id),
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            if let Some((generation, desired_state, desired_config, desired_action_id)) = desired {
                let config_matches = desired_config
                    .as_deref()
                    .is_none_or(|version| Some(version) == mutation.config_version_id.as_deref());
                let action_matches = desired_action_id
                    .as_deref()
                    .is_none_or(|action_id| Some(action_id) == mutation.action_id.as_deref());
                let affected_streams_converged = if mutation.stream_id == "__configuration__" {
                    let blockers: i64 = transaction.query_row(
                        "SELECT COUNT(*) FROM cp_stream_desired d LEFT JOIN cp_stream_observed o ON o.node_id = d.node_id AND o.stream_id = d.stream_id WHERE d.node_id = ?1 AND d.stream_id <> '__configuration__' AND (o.stream_id IS NULL OR o.observed_generation <> d.generation OR o.observed_state <> d.desired_state OR o.applied_config_version IS NULL OR o.applied_config_version <> ?2)",
                        rusqlite::params![mutation.node_id, mutation.config_version_id],
                        |row| row.get(0),
                    )?;
                    blockers == 0
                } else {
                    true
                };
                if mutation.observed_generation == Some(generation)
                    && desired_state == mutation.observed_state
                    && config_matches
                    && action_matches
                    && affected_streams_converged
                {
                    transaction.execute(
                        "UPDATE cp_intents SET state = 'converged', convergence_state = 'in_sync', converged_at_ms = ?1, updated_at_ms = ?1 WHERE node_id = ?2 AND stream_id = ?3 AND generation = ?4 AND state IN ('accepted', 'converging', 'retrying')",
                        rusqlite::params![now, mutation.node_id, mutation.stream_id, generation],
                    )?;
                    transaction.execute(
                        "UPDATE cp_attempts SET state = 'succeeded', finished_at_ms = ?1 WHERE node_id = ?2 AND stream_id = ?3 AND generation = ?4 AND state IN ('queued', 'dispatched', 'acknowledged', 'running')",
                        rusqlite::params![now, mutation.node_id, mutation.stream_id, generation],
                    )?;
                    let intent_id: Option<String> = transaction
                        .query_row(
                            "SELECT intent_id FROM cp_intents WHERE node_id = ?1 AND stream_id = ?2 AND generation = ?3 ORDER BY created_at_ms DESC LIMIT 1",
                            rusqlite::params![mutation.node_id, mutation.stream_id, generation],
                            |row| row.get(0),
                        )
                        .optional()?;
                    transaction.execute(
                        "INSERT INTO cp_events (node_id, stream_id, intent_id, event_type, outcome, generation, occurred_at_ms) VALUES (?1, ?2, ?3, 'intent_converged', 'converged', ?4, ?5)",
                        rusqlite::params![mutation.node_id, mutation.stream_id, intent_id, generation, now],
                    )?;
                } else if mutation.stream_id == "__configuration__"
                    && mutation.observed_generation == Some(generation)
                    && desired_state == mutation.observed_state
                    && config_matches
                    && action_matches
                {
                    transaction.execute(
                        "UPDATE cp_intents SET state = 'converging', convergence_state = 'applying', updated_at_ms = ?1 WHERE node_id = ?2 AND stream_id = ?3 AND generation = ?4 AND state IN ('accepted', 'converging', 'retrying')",
                        rusqlite::params![now, mutation.node_id, mutation.stream_id, generation],
                    )?;
                }
            }
            let wake_key = format!(
                "reconcile:observed:{}:{}:{}:{}",
                mutation.node_id,
                mutation.stream_id,
                mutation.boot_id.as_deref().unwrap_or("unknown"),
                mutation.report_seq
            );
            transaction.execute(
                "INSERT OR IGNORE INTO cp_outbox (event_key, event_type, node_id, stream_id, intent_id, available_at_ms, created_at_ms) SELECT ?1, 'reconcile_intent', d.node_id, d.stream_id, i.intent_id, ?2, ?2 FROM cp_stream_desired d JOIN cp_intents i ON i.node_id = d.node_id AND i.stream_id = d.stream_id AND i.generation = d.generation WHERE d.node_id = ?3 AND d.stream_id = ?4 AND i.state IN ('accepted', 'converging', 'retrying') AND NOT EXISTS (SELECT 1 FROM cp_outbox o WHERE o.intent_id = i.intent_id AND o.processed_at_ms IS NULL)",
                rusqlite::params![wake_key, now, mutation.node_id, mutation.stream_id],
            )?;
            Ok(())
        })
    }

    pub fn mark_outbox_processed(&self, outbox_id: i64, now_ms: u64) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "UPDATE cp_outbox SET processed_at_ms = ?1 WHERE outbox_id = ?2 AND processed_at_ms IS NULL",
                rusqlite::params![now_ms, outbox_id],
            )?;
            Ok(())
        })
    }

    pub fn record_audit(&self, record: AuditRecord) -> Result<i64, StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "INSERT INTO cp_audit_events (actor, action, resource_type, resource_id, node_id, stream_id, correlation_id, outcome, failure_code, message, occurred_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                rusqlite::params![
                    record.actor,
                    record.action,
                    record.resource_type,
                    record.resource_id,
                    record.node_id,
                    record.stream_id,
                    record.correlation_id,
                    record.outcome,
                    record.failure_code,
                    record.message,
                    record.occurred_at_ms,
                ],
            )?;
            Ok(transaction.last_insert_rowid())
        })
    }

    pub fn list_audit(&self, resource_id: Option<&str>) -> Result<Vec<AuditRecord>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT event_id, actor, action, resource_type, resource_id, node_id, stream_id, correlation_id, outcome, failure_code, message, occurred_at_ms FROM cp_audit_events WHERE (?1 IS NULL OR resource_id = ?1) ORDER BY event_id DESC LIMIT 1024",
            )?;
            let rows = statement.query_map([resource_id], |row| {
                Ok(AuditRecord {
                    event_id: row.get(0)?,
                    actor: row.get(1)?,
                    action: row.get(2)?,
                    resource_type: row.get(3)?,
                    resource_id: row.get(4)?,
                    node_id: row.get(5)?,
                    stream_id: row.get(6)?,
                    correlation_id: row.get(7)?,
                    outcome: row.get(8)?,
                    failure_code: row.get(9)?,
                    message: row.get(10)?,
                    occurred_at_ms: row.get(11)?,
                })
            })?;
            rows.collect()
        })
    }

    pub fn create_rollout(
        &self,
        rollout: RolloutRecord,
        targets: Vec<RolloutTargetRecord>,
    ) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "INSERT INTO cp_rollouts (rollout_id, config_version_id, state, batch_size, current_batch, total_targets, actor, correlation_id, created_at_ms, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                rusqlite::params![
                    rollout.rollout_id,
                    rollout.config_version_id,
                    rollout.state,
                    rollout.batch_size,
                    rollout.current_batch,
                    rollout.total_targets,
                    rollout.actor,
                    rollout.correlation_id,
                    rollout.created_at_ms,
                    rollout.updated_at_ms,
                ],
            )?;
            for target in targets {
                transaction.execute(
                    "INSERT INTO cp_rollout_targets (rollout_id, node_id, ordinal, state, attempt_id, error, observed_config_version, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    rusqlite::params![
                        target.rollout_id,
                        target.node_id,
                        target.ordinal,
                        target.state,
                        target.attempt_id,
                        target.error,
                        target.observed_config_version,
                        target.updated_at_ms,
                    ],
                )?;
            }
            Ok(())
        })
    }

    pub fn create_rollout_with_content(
        &self,
        rollout: RolloutRecord,
        targets: Vec<RolloutTargetRecord>,
        content: &str,
        created_by: Option<&str>,
    ) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "INSERT OR IGNORE INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms, created_by) VALUES (?1, 'inline-json', ?2, 'json', ?3, ?4)",
                rusqlite::params![
                    rollout.config_version_id,
                    content,
                    rollout.created_at_ms,
                    created_by,
                ],
            )?;
            transaction.execute(
                "INSERT INTO cp_rollouts (rollout_id, config_version_id, state, batch_size, current_batch, total_targets, actor, correlation_id, created_at_ms, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                rusqlite::params![
                    rollout.rollout_id,
                    rollout.config_version_id,
                    rollout.state,
                    rollout.batch_size,
                    rollout.current_batch,
                    rollout.total_targets,
                    rollout.actor,
                    rollout.correlation_id,
                    rollout.created_at_ms,
                    rollout.updated_at_ms,
                ],
            )?;
            for target in targets {
                transaction.execute(
                    "INSERT INTO cp_rollout_targets (rollout_id, node_id, ordinal, state, attempt_id, error, observed_config_version, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    rusqlite::params![
                        target.rollout_id,
                        target.node_id,
                        target.ordinal,
                        target.state,
                        target.attempt_id,
                        target.error,
                        target.observed_config_version,
                        target.updated_at_ms,
                    ],
                )?;
            }
            Ok(())
        })
    }

    pub fn get_rollout(&self, rollout_id: &str) -> Result<Option<RolloutRecord>, StorageError> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT rollout_id, config_version_id, state, batch_size, current_batch, total_targets, actor, correlation_id, created_at_ms, updated_at_ms FROM cp_rollouts WHERE rollout_id = ?1",
                    [rollout_id],
                    |row| {
                        Ok(RolloutRecord {
                            rollout_id: row.get(0)?,
                            config_version_id: row.get(1)?,
                            state: row.get(2)?,
                            batch_size: row.get(3)?,
                            current_batch: row.get(4)?,
                            total_targets: row.get(5)?,
                            actor: row.get(6)?,
                            correlation_id: row.get(7)?,
                            created_at_ms: row.get(8)?,
                            updated_at_ms: row.get(9)?,
                        })
                    },
                )
                .optional()
        })
    }

    pub fn list_rollout_targets(
        &self,
        rollout_id: &str,
    ) -> Result<Vec<RolloutTargetRecord>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT rollout_id, node_id, ordinal, state, attempt_id, error, observed_config_version, updated_at_ms FROM cp_rollout_targets WHERE rollout_id = ?1 ORDER BY ordinal, node_id",
            )?;
            let rows = statement.query_map([rollout_id], |row| {
                Ok(RolloutTargetRecord {
                    rollout_id: row.get(0)?,
                    node_id: row.get(1)?,
                    ordinal: row.get(2)?,
                    state: row.get(3)?,
                    attempt_id: row.get(4)?,
                    error: row.get(5)?,
                    observed_config_version: row.get(6)?,
                    updated_at_ms: row.get(7)?,
                })
            })?;
            rows.collect()
        })
    }

    pub fn update_rollout(
        &self,
        rollout_id: &str,
        state: &str,
        current_batch: u32,
        updated_at_ms: u64,
    ) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "UPDATE cp_rollouts SET state = ?1, current_batch = ?2, updated_at_ms = ?3 WHERE rollout_id = ?4",
                rusqlite::params![state, current_batch, updated_at_ms, rollout_id],
            )?;
            Ok(())
        })
    }

    pub fn update_rollout_target(&self, update: RolloutTargetUpdate) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "UPDATE cp_rollout_targets SET state = ?1, attempt_id = ?2, error = ?3, observed_config_version = ?4, updated_at_ms = ?5 WHERE rollout_id = ?6 AND node_id = ?7",
                rusqlite::params![
                    update.state,
                    update.attempt_id,
                    update.error,
                    update.observed_config_version,
                    update.updated_at_ms,
                    update.rollout_id,
                    update.node_id,
                ],
            )?;
            Ok(())
        })
    }

    pub fn get_config_version_content(
        &self,
        config_version_id: &str,
    ) -> Result<Option<String>, StorageError> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT content_ref FROM cp_config_versions WHERE config_version_id = ?1",
                    [config_version_id],
                    |row| row.get(0),
                )
                .optional()
        })
    }

    pub fn recover_rollouts(&self) -> Result<Vec<RolloutRecord>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT rollout_id, config_version_id, state, batch_size, current_batch, total_targets, actor, correlation_id, created_at_ms, updated_at_ms FROM cp_rollouts WHERE state NOT IN ('converged', 'cancelled', 'rolled_back') ORDER BY created_at_ms",
            )?;
            let rows = statement.query_map([], |row| {
                Ok(RolloutRecord {
                    rollout_id: row.get(0)?,
                    config_version_id: row.get(1)?,
                    state: row.get(2)?,
                    batch_size: row.get(3)?,
                    current_batch: row.get(4)?,
                    total_targets: row.get(5)?,
                    actor: row.get(6)?,
                    correlation_id: row.get(7)?,
                    created_at_ms: row.get(8)?,
                    updated_at_ms: row.get(9)?,
                })
            })?;
            rows.collect()
        })
    }

    pub fn list_rollouts(&self) -> Result<Vec<RolloutRecord>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT rollout_id, config_version_id, state, batch_size, current_batch, total_targets, actor, correlation_id, created_at_ms, updated_at_ms FROM cp_rollouts ORDER BY created_at_ms DESC, rollout_id DESC LIMIT 1024",
            )?;
            let rows = statement.query_map([], |row| {
                Ok(RolloutRecord {
                    rollout_id: row.get(0)?,
                    config_version_id: row.get(1)?,
                    state: row.get(2)?,
                    batch_size: row.get(3)?,
                    current_batch: row.get(4)?,
                    total_targets: row.get(5)?,
                    actor: row.get(6)?,
                    correlation_id: row.get(7)?,
                    created_at_ms: row.get(8)?,
                    updated_at_ms: row.get(9)?,
                })
            })?;
            rows.collect()
        })
    }

    pub fn upsert_operation(&self, operation: PersistedOperation) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "INSERT INTO cp_operations (operation_id, node_id, resource_id, operation, state, created_at_ms, updated_at_ms, operation_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ON CONFLICT(operation_id) DO UPDATE SET state = excluded.state, updated_at_ms = excluded.updated_at_ms, operation_json = excluded.operation_json",
                rusqlite::params![
                    operation.operation_id,
                    operation.node_id,
                    operation.resource_id,
                    operation.operation,
                    operation.state,
                    operation.created_at_ms,
                    operation.updated_at_ms,
                    operation.operation_json,
                ],
            )?;
            Ok(())
        })
    }

    pub fn get_operation(
        &self,
        operation_id: &str,
    ) -> Result<Option<PersistedOperation>, StorageError> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT operation_id, node_id, resource_id, operation, state, created_at_ms, updated_at_ms, operation_json FROM cp_operations WHERE operation_id = ?1",
                    [operation_id],
                    |row| {
                        Ok(PersistedOperation {
                            operation_id: row.get(0)?,
                            node_id: row.get(1)?,
                            resource_id: row.get(2)?,
                            operation: row.get(3)?,
                            state: row.get(4)?,
                            created_at_ms: row.get(5)?,
                            updated_at_ms: row.get(6)?,
                            operation_json: row.get(7)?,
                        })
                    },
                )
                .optional()
        })
    }

    pub fn list_operations(
        &self,
        node_id: Option<&str>,
    ) -> Result<Vec<PersistedOperation>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT operation_id, node_id, resource_id, operation, state, created_at_ms, updated_at_ms, operation_json FROM cp_operations WHERE (?1 IS NULL OR node_id = ?1) ORDER BY created_at_ms DESC, operation_id DESC LIMIT 1024",
            )?;
            let rows = statement.query_map([node_id], |row| {
                Ok(PersistedOperation {
                    operation_id: row.get(0)?,
                    node_id: row.get(1)?,
                    resource_id: row.get(2)?,
                    operation: row.get(3)?,
                    state: row.get(4)?,
                    created_at_ms: row.get(5)?,
                    updated_at_ms: row.get(6)?,
                    operation_json: row.get(7)?,
                })
            })?;
            rows.collect()
        })
    }

    pub fn list_job_start_operations(
        &self,
        resource_id: &str,
    ) -> Result<Vec<PersistedOperation>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT operation_id, node_id, resource_id, operation, state, created_at_ms, updated_at_ms, operation_json FROM cp_operations WHERE resource_id = ?1 AND operation = 'job_start' AND (state = 'succeeded' OR operation_json LIKE '%\"failure_class\":\"recovery_required\"%') ORDER BY updated_at_ms DESC, operation_id DESC LIMIT 4096",
            )?;
            let rows = statement.query_map([resource_id], |row| {
                Ok(PersistedOperation {
                    operation_id: row.get(0)?,
                    node_id: row.get(1)?,
                    resource_id: row.get(2)?,
                    operation: row.get(3)?,
                    state: row.get(4)?,
                    created_at_ms: row.get(5)?,
                    updated_at_ms: row.get(6)?,
                    operation_json: row.get(7)?,
                })
            })?;
            rows.collect()
        })
    }

    pub fn upsert_job(&self, mut job: JobRecord) -> Result<JobRecord, StorageError> {
        self.immediate_transaction(|connection| {
            let current_generation = connection
                .query_row(
                    "SELECT generation FROM cp_jobs WHERE job_id = ?1",
                    [&job.job_id],
                    |row| row.get::<_, u64>(0),
                )
                .optional()?;
            job.generation = current_generation
                .map(|generation| generation.saturating_add(1))
                .unwrap_or_else(|| job.generation.max(1));
            let node_ids = serde_json::to_string(&job.node_ids).map_err(|error| {
                StorageError::Sqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
            })?;
            connection.execute(
                "INSERT INTO cp_jobs (job_id, version, spec_json, desired_state, observed_state, convergence, generation, node_ids_json, checkpoint_id, last_error, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) ON CONFLICT(job_id) DO UPDATE SET version=excluded.version, spec_json=excluded.spec_json, desired_state=excluded.desired_state, observed_state=excluded.observed_state, convergence=excluded.convergence, generation=excluded.generation, node_ids_json=excluded.node_ids_json, checkpoint_id=excluded.checkpoint_id, last_error=excluded.last_error, updated_at_ms=excluded.updated_at_ms",
                rusqlite::params![
                    job.job_id,
                    job.version,
                    job.spec_json,
                    job.desired_state,
                    job.observed_state,
                    job.convergence,
                    job.generation,
                    node_ids,
                    job.checkpoint_id,
                    job.last_error,
                    job.updated_at_ms,
                ],
            )?;
            Ok(job)
        })
    }

    /// Replace a Job record only when its stored generation still matches
    /// `expected_generation`, bumping the generation on success. The
    /// upgrade/rollback handlers read the Job, await several round trips and
    /// then write: without this CAS a concurrent desired-state change that
    /// bumps the generation can be silently overwritten by the older read.
    ///
    /// The recovery pointer is not written: it belongs to the checkpoint path,
    /// which moves it without bumping the generation, so a rollback copying its
    /// earlier read back would regress recovery to a checkpoint retention may
    /// already have deleted.
    pub fn update_job_with_expected_generation(
        &self,
        mut job: JobRecord,
        expected_generation: u64,
    ) -> Result<JobRecord, StorageError> {
        self.immediate_transaction(|connection| {
            job.generation = expected_generation.saturating_add(1);
            let node_ids = serde_json::to_string(&job.node_ids).map_err(|error| {
                StorageError::Sqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
            })?;
            // The recovery pointer is fenced by PRESERVATION: a checkpoint
            // that lands between the caller's read and this write updated the
            // `checkpoint_id` is deliberately NOT in the SET list: the
            // recovery pointer is owned by the checkpoint path, which updates
            // it without bumping the generation (see `update_job`). A
            // conditional write that copied the caller's older read back would
            // regress recovery to a pointer retention may already have deleted.
            // The version/spec change from a rollback does not need to re-point
            // recovery: selection filters artifacts by job version and state
            // format, so preserving the newest pointer is both safe and the
            // only choice that cannot regress.
            let changed = connection.execute(
                "UPDATE cp_jobs SET version=?, spec_json=?, desired_state=?, observed_state=?, convergence=?, generation=?, node_ids_json=?, last_error=?, updated_at_ms=? WHERE job_id=? AND generation=?",
                rusqlite::params![
                    job.version,
                    job.spec_json,
                    job.desired_state,
                    job.observed_state,
                    job.convergence,
                    job.generation,
                    node_ids,
                    job.last_error,
                    job.updated_at_ms,
                    job.job_id,
                    expected_generation,
                ],
            )?;
            if changed == 0 {
                let current = connection
                    .query_row(
                        "SELECT generation FROM cp_jobs WHERE job_id = ?1",
                        [&job.job_id],
                        |row| row.get::<_, u64>(0),
                    )
                    .optional()?;
                return Err(StorageError::GenerationConflict {
                    expected: expected_generation,
                    current: current.unwrap_or(0),
                });
            }
            // Report the row as stored: the recovery pointer may have been
            // preserved from a concurrent checkpoint rather than taken from the
            // request, and the caller caches this record.
            let stored = connection
                .query_row(
                    "SELECT job_id, version, spec_json, desired_state, observed_state, convergence, generation, node_ids_json, checkpoint_id, last_error, updated_at_ms FROM cp_jobs WHERE job_id = ?1",
                    [&job.job_id],
                    row_to_job,
                )
                .optional()?;
            Ok(stored.unwrap_or(job))
        })
    }

    pub fn get_job(&self, job_id: &str) -> Result<Option<JobRecord>, StorageError> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT job_id, version, spec_json, desired_state, observed_state, convergence, generation, node_ids_json, checkpoint_id, last_error, updated_at_ms FROM cp_jobs WHERE job_id = ?1",
                    [job_id],
                    row_to_job,
                )
                .optional()
        })
    }

    pub fn upsert_job_version(&self, record: JobVersionRecord) -> Result<(), StorageError> {
        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO cp_job_versions (job_id, version, spec_json, plan_json, created_at_ms) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(job_id, version) DO UPDATE SET spec_json=excluded.spec_json, plan_json=excluded.plan_json",
                rusqlite::params![
                    record.job_id,
                    record.version,
                    record.spec_json,
                    record.plan_json,
                    record.created_at_ms,
                ],
            )?;
            Ok(())
        })
    }

    pub fn list_job_versions(&self, job_id: &str) -> Result<Vec<JobVersionRecord>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT job_id, version, spec_json, plan_json, created_at_ms FROM cp_job_versions WHERE job_id = ?1 ORDER BY version DESC",
            )?;
            let rows = statement.query_map([job_id], |row| {
                Ok(JobVersionRecord {
                    job_id: row.get(0)?,
                    version: row.get(1)?,
                    spec_json: row.get(2)?,
                    plan_json: row.get(3)?,
                    created_at_ms: row.get(4)?,
                })
            })?;
            rows.collect()
        })
    }

    pub fn list_jobs(&self) -> Result<Vec<JobRecord>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT job_id, version, spec_json, desired_state, observed_state, convergence, generation, node_ids_json, checkpoint_id, last_error, updated_at_ms FROM cp_jobs ORDER BY updated_at_ms DESC, job_id LIMIT 4096",
            )?;
            let rows = statement.query_map([], row_to_job)?;
            rows.collect()
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_job(
        &self,
        job_id: &str,
        desired_state: Option<&str>,
        observed_state: Option<&str>,
        convergence: Option<&str>,
        generation: Option<u64>,
        checkpoint_id: Option<&str>,
        last_error: Option<&str>,
    ) -> Result<Option<JobRecord>, StorageError> {
        self.with_connection(|connection| {
            connection.execute(
                "UPDATE cp_jobs SET desired_state=COALESCE(?2, desired_state), observed_state=COALESCE(?3, observed_state), convergence=COALESCE(?4, convergence), generation=COALESCE(?5, generation), checkpoint_id=COALESCE(?6, checkpoint_id), last_error=COALESCE(?7, last_error), updated_at_ms=?8 WHERE job_id=?1",
                rusqlite::params![job_id, desired_state, observed_state, convergence, generation, checkpoint_id, last_error, now_ms()],
            )?;
            connection
                .query_row(
                    "SELECT job_id, version, spec_json, desired_state, observed_state, convergence, generation, node_ids_json, checkpoint_id, last_error, updated_at_ms FROM cp_jobs WHERE job_id = ?1",
                    [job_id],
                    row_to_job,
                )
                .optional()
        })
    }

    /// CAS observation update: applies the observed state only while the
    /// Job's generation still equals `expected_generation`. A concurrent
    /// desired-state change or placement move must never be rolled back by a
    /// stale report — that would fence every newer observation and pin the
    /// Job in a reconciling loop.
    pub fn update_job_observation(
        &self,
        job_id: &str,
        observed_state: &str,
        convergence: &str,
        generation: u64,
        expected_generation: u64,
        checkpoint_id: Option<&str>,
        last_error: Option<&str>,
    ) -> Result<Option<JobRecord>, StorageError> {
        self.immediate_transaction(|connection| {
            let changed = connection.execute(
                "UPDATE cp_jobs SET observed_state=?2, convergence=?3, generation=?4, checkpoint_id=COALESCE(?5, checkpoint_id), last_error=?6, updated_at_ms=?7 WHERE job_id=?1 AND generation=?8",
                rusqlite::params![
                    job_id,
                    observed_state,
                    convergence,
                    generation,
                    checkpoint_id,
                    last_error,
                    now_ms(),
                    expected_generation,
                ],
            )?;
            if changed == 0 {
                let current = connection
                    .query_row(
                        "SELECT generation FROM cp_jobs WHERE job_id = ?1",
                        [job_id],
                        |row| row.get::<_, u64>(0),
                    )
                    .optional()?;
                return match current {
                    Some(current) => Err(StorageError::GenerationConflict {
                        expected: expected_generation,
                        current,
                    }),
                    None => Ok(None),
                };
            }
            Ok(connection
                .query_row(
                    "SELECT job_id, version, spec_json, desired_state, observed_state, convergence, generation, node_ids_json, checkpoint_id, last_error, updated_at_ms FROM cp_jobs WHERE job_id = ?1",
                    [job_id],
                    row_to_job,
                )
                .optional()?)
        })
    }

    pub fn update_job_desired_state(
        &self,
        job_id: &str,
        desired_state: &str,
        expected_generation: u64,
    ) -> Result<Option<JobRecord>, StorageError> {
        self.immediate_transaction(|connection| {
            let changed = connection.execute(
                "UPDATE cp_jobs SET desired_state=?2, convergence='reconciling', generation=?3, updated_at_ms=?4 WHERE job_id=?1 AND generation=?5",
                rusqlite::params![
                    job_id,
                    desired_state,
                    expected_generation.saturating_add(1),
                    now_ms(),
                    expected_generation,
                ],
            )?;
            if changed == 0 {
                let current = connection
                    .query_row(
                        "SELECT generation FROM cp_jobs WHERE job_id = ?1",
                        [job_id],
                        |row| row.get::<_, u64>(0),
                    )
                    .optional()?;
                return match current {
                    Some(current) => Err(StorageError::GenerationConflict {
                        expected: expected_generation,
                        current,
                    }),
                    None => Ok(None),
                };
            }
            Ok(connection
                .query_row(
                    "SELECT job_id, version, spec_json, desired_state, observed_state, convergence, generation, node_ids_json, checkpoint_id, last_error, updated_at_ms FROM cp_jobs WHERE job_id = ?1",
                    [job_id],
                    row_to_job,
                )
                .optional()?)
        })
    }

    pub fn upsert_job_checkpoint(&self, record: JobCheckpointRecord) -> Result<(), StorageError> {
        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO cp_job_checkpoints (job_id, job_version, checkpoint_id, kind, status, manifest_uri, format_version, created_at_ms, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) ON CONFLICT(job_id, checkpoint_id) DO UPDATE SET job_version=excluded.job_version, status=excluded.status, manifest_uri=excluded.manifest_uri, format_version=excluded.format_version, updated_at_ms=excluded.updated_at_ms",
                rusqlite::params![
                    record.job_id,
                    record.job_version,
                    record.checkpoint_id,
                    record.kind,
                    record.status,
                    record.manifest_uri,
                    record.format_version,
                    record.created_at_ms,
                    record.updated_at_ms,
                ],
            )?;
            Ok(())
        })
    }

    pub fn list_job_checkpoints(
        &self,
        job_id: &str,
    ) -> Result<Vec<JobCheckpointRecord>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT job_id, job_version, checkpoint_id, kind, status, manifest_uri, format_version, created_at_ms, updated_at_ms FROM cp_job_checkpoints WHERE job_id = ?1 ORDER BY created_at_ms DESC, checkpoint_id DESC",
            )?;
            let rows = statement.query_map([job_id], |row| {
                Ok(JobCheckpointRecord {
                    job_id: row.get(0)?,
                    job_version: row.get(1)?,
                    checkpoint_id: row.get(2)?,
                    kind: row.get(3)?,
                    status: row.get(4)?,
                    manifest_uri: row.get(5)?,
                    format_version: row.get(6)?,
                    created_at_ms: row.get(7)?,
                    updated_at_ms: row.get(8)?,
                })
            })?;
            rows.collect::<Result<Vec<_>, _>>()
        })
    }

    pub fn delete_job_checkpoint(
        &self,
        job_id: &str,
        checkpoint_id: &str,
    ) -> Result<(), StorageError> {
        self.with_connection(|connection| {
            connection.execute(
                "DELETE FROM cp_job_checkpoints WHERE job_id = ?1 AND checkpoint_id = ?2",
                rusqlite::params![job_id, checkpoint_id],
            )?;
            Ok(())
        })
    }

    fn migrate(&self) -> Result<(), StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS cp_nodes (
                node_id TEXT PRIMARY KEY,
                role TEXT NOT NULL DEFAULT 'compute',
                protocol_version TEXT NOT NULL DEFAULT 'v1',
                node_version TEXT NOT NULL DEFAULT 'unknown',
                state TEXT NOT NULL DEFAULT 'offline',
                capabilities_json TEXT NOT NULL DEFAULT '[]',
                boot_id TEXT,
                last_report_seq INTEGER,
                last_seen_at_ms INTEGER NOT NULL DEFAULT 0,
                lease_expires_at_ms INTEGER NOT NULL DEFAULT 0,
                maintenance_state TEXT NOT NULL DEFAULT 'active',
                maintenance_updated_at_ms INTEGER,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cp_jobs (
                job_id TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                spec_json TEXT NOT NULL,
                desired_state TEXT NOT NULL DEFAULT 'stopped',
                observed_state TEXT NOT NULL DEFAULT 'draft',
                convergence TEXT NOT NULL DEFAULT 'unknown',
                generation INTEGER NOT NULL DEFAULT 0,
                node_ids_json TEXT NOT NULL DEFAULT '[]',
                checkpoint_id TEXT,
                last_error TEXT,
                updated_at_ms INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS cp_jobs_updated
                ON cp_jobs(updated_at_ms DESC, job_id);

            CREATE TABLE IF NOT EXISTS cp_job_versions (
                job_id TEXT NOT NULL, version INTEGER NOT NULL, spec_json TEXT NOT NULL,
                plan_json TEXT NOT NULL, created_at_ms INTEGER NOT NULL,
                PRIMARY KEY (job_id, version)
            );
            CREATE TABLE IF NOT EXISTS cp_job_tasks (
                job_id TEXT NOT NULL, generation INTEGER NOT NULL, task_id TEXT NOT NULL,
                node_id TEXT NOT NULL, attempt_id TEXT NOT NULL, state TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL, PRIMARY KEY (job_id, generation, task_id)
            );
            CREATE TABLE IF NOT EXISTS cp_job_checkpoints (
                job_id TEXT NOT NULL, job_version INTEGER NOT NULL DEFAULT 0,
                checkpoint_id TEXT NOT NULL, kind TEXT NOT NULL,
                status TEXT NOT NULL, manifest_uri TEXT, format_version INTEGER NOT NULL,
                created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (job_id, checkpoint_id)
            );

            CREATE TABLE IF NOT EXISTS cp_stream_desired (
                node_id TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                desired_state TEXT NOT NULL,
                config_version_id TEXT,
                desired_action_id TEXT,
                paused INTEGER NOT NULL DEFAULT 0,
                updated_at_ms INTEGER NOT NULL,
                updated_by TEXT,
                correlation_id TEXT,
                PRIMARY KEY (node_id, stream_id)
            );

            CREATE TABLE IF NOT EXISTS cp_stream_observed (
                node_id TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                boot_id TEXT,
                report_seq INTEGER,
                observed_generation INTEGER,
                observed_state TEXT NOT NULL,
                applied_config_version TEXT,
                last_action_id TEXT,
                active_operation_id TEXT,
                last_error_code TEXT,
                last_error_message TEXT,
                snapshot_json TEXT NOT NULL DEFAULT '{}',
                observed_at_ms INTEGER NOT NULL,
                PRIMARY KEY (node_id, stream_id)
            );

            CREATE TABLE IF NOT EXISTS cp_config_versions (
                config_version_id TEXT PRIMARY KEY,
                parent_version_id TEXT,
                content_digest TEXT NOT NULL,
                content_ref TEXT NOT NULL,
                format TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                created_by TEXT,
                correlation_id TEXT,
                FOREIGN KEY (parent_version_id)
                    REFERENCES cp_config_versions(config_version_id)
            );

            CREATE TABLE IF NOT EXISTS cp_intents (
                intent_id TEXT PRIMARY KEY,
                node_id TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                intent_type TEXT NOT NULL,
                desired_state TEXT,
                config_version_id TEXT,
                action_id TEXT,
                payload_json TEXT,
                state TEXT NOT NULL,
                convergence_state TEXT NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                next_retry_at_ms INTEGER,
                last_failure_class TEXT,
                last_failure_code TEXT,
                last_failure_message TEXT,
                superseded_by_intent_id TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                converged_at_ms INTEGER,
                actor TEXT,
                correlation_id TEXT,
                idempotency_key TEXT,
                UNIQUE (node_id, stream_id, generation),
                FOREIGN KEY (superseded_by_intent_id)
                    REFERENCES cp_intents(intent_id)
            );

            CREATE TABLE IF NOT EXISTS cp_attempts (
                attempt_id TEXT PRIMARY KEY,
                intent_id TEXT NOT NULL,
                command_id TEXT NOT NULL UNIQUE,
                node_id TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                operation TEXT NOT NULL,
                state TEXT NOT NULL,
                failure_class TEXT,
                dispatched_at_ms INTEGER,
                acknowledged_at_ms INTEGER,
                started_at_ms INTEGER,
                finished_at_ms INTEGER,
                expires_at_ms INTEGER,
                error_code TEXT,
                error_message TEXT,
                created_at_ms INTEGER NOT NULL,
                FOREIGN KEY (intent_id) REFERENCES cp_intents(intent_id)
            );

            CREATE UNIQUE INDEX IF NOT EXISTS cp_one_active_attempt
                ON cp_attempts(node_id, stream_id, generation)
                WHERE state IN ('queued', 'dispatched', 'acknowledged', 'running');

            CREATE TABLE IF NOT EXISTS cp_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT,
                stream_id TEXT,
                intent_id TEXT,
                attempt_id TEXT,
                event_type TEXT NOT NULL,
                outcome TEXT NOT NULL,
                failure_class TEXT,
                message TEXT,
                generation INTEGER,
                correlation_id TEXT,
                actor TEXT,
                occurred_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cp_audit_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                occurred_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cp_rollouts (
                rollout_id TEXT PRIMARY KEY,
                config_version_id TEXT NOT NULL,
                state TEXT NOT NULL,
                batch_size INTEGER NOT NULL,
                current_batch INTEGER NOT NULL DEFAULT 0,
                total_targets INTEGER NOT NULL,
                actor TEXT,
                correlation_id TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                FOREIGN KEY (config_version_id)
                    REFERENCES cp_config_versions(config_version_id)
            );

            CREATE TABLE IF NOT EXISTS cp_rollout_targets (
                rollout_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                state TEXT NOT NULL,
                attempt_id TEXT,
                error TEXT,
                observed_config_version TEXT,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (rollout_id, node_id),
                FOREIGN KEY (rollout_id)
                    REFERENCES cp_rollouts(rollout_id)
            );

            CREATE TABLE IF NOT EXISTS cp_operations (
                operation_id TEXT PRIMARY KEY,
                node_id TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                operation TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                operation_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cp_outbox (
                outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_key TEXT NOT NULL UNIQUE,
                event_type TEXT NOT NULL,
                node_id TEXT NOT NULL,
                stream_id TEXT,
                intent_id TEXT,
                available_at_ms INTEGER NOT NULL,
                claimed_at_ms INTEGER,
                worker_id TEXT,
                processed_at_ms INTEGER,
                created_at_ms INTEGER NOT NULL
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
            "#,
        )?;
        let has_idempotency_key: bool = connection
            .prepare("PRAGMA table_info(cp_intents)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .any(|name| name == "idempotency_key");
        if !has_idempotency_key {
            connection.execute("ALTER TABLE cp_intents ADD COLUMN idempotency_key TEXT", [])?;
        }
        let has_payload_json: bool = connection
            .prepare("PRAGMA table_info(cp_intents)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .any(|name| name == "payload_json");
        if !has_payload_json {
            connection.execute("ALTER TABLE cp_intents ADD COLUMN payload_json TEXT", [])?;
        }
        let has_node_version: bool = connection
            .prepare("PRAGMA table_info(cp_nodes)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .any(|name| name == "node_version");
        if !has_node_version {
            connection.execute(
                "ALTER TABLE cp_nodes ADD COLUMN node_version TEXT NOT NULL DEFAULT 'unknown'",
                [],
            )?;
        }
        let node_columns: Vec<String> = connection
            .prepare("PRAGMA table_info(cp_nodes)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        if !node_columns.iter().any(|name| name == "maintenance_state") {
            connection.execute(
                "ALTER TABLE cp_nodes ADD COLUMN maintenance_state TEXT NOT NULL DEFAULT 'active'",
                [],
            )?;
        }
        if !node_columns
            .iter()
            .any(|name| name == "maintenance_updated_at_ms")
        {
            connection.execute(
                "ALTER TABLE cp_nodes ADD COLUMN maintenance_updated_at_ms INTEGER",
                [],
            )?;
        }
        let event_columns: Vec<String> = connection
            .prepare("PRAGMA table_info(cp_events)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        if !event_columns.iter().any(|name| name == "actor") {
            connection.execute("ALTER TABLE cp_events ADD COLUMN actor TEXT", [])?;
        }
        let checkpoint_columns: Vec<String> = connection
            .prepare("PRAGMA table_info(cp_job_checkpoints)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        if !checkpoint_columns.iter().any(|name| name == "job_version") {
            // Existing checkpoints have no durable producer version. Mark them
            // incompatible (0) so they are never auto-selected for recovery.
            connection.execute(
                "ALTER TABLE cp_job_checkpoints ADD COLUMN job_version INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS cp_intents_idempotency ON cp_intents(node_id, stream_id, idempotency_key) WHERE idempotency_key IS NOT NULL",
            [],
        )?;
        Ok(())
    }

    pub fn table_exists(&self, name: &str) -> Result<bool, StorageError> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
                    [name],
                    |_| Ok(()),
                )
                .optional()
                .map(|value| value.is_some())
        })
    }

    pub fn index_exists(&self, name: &str) -> Result<bool, StorageError> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?1",
                    [name],
                    |_| Ok(()),
                )
                .optional()
                .map(|value| value.is_some())
        })
    }
}

#[async_trait]
impl StorageBackend for SqliteBackend {
    async fn set_desired(&self, mutation: DesiredMutation) -> Result<IntentRecord, StorageError> {
        self.set_desired(mutation)
    }
    async fn upsert_node(&self, mutation: NodeMutation) -> Result<(), StorageError> {
        self.upsert_node(mutation)
    }
    async fn reset_observed_cursors(&self, node_id: &str) -> Result<(), StorageError> {
        self.reset_observed_cursors(node_id)
    }
    async fn set_node_maintenance(
&self,
mutation: NodeMaintenanceMutation,
now_ms: u64,
) -> Result<bool, StorageError> {
        self.set_node_maintenance(mutation, now_ms)
    }
    async fn get_node_maintenance(&self, node_id: &str) -> Result<Option<String>, StorageError> {
        self.get_node_maintenance(node_id)
    }
    async fn operational_aggregates(
&self,
now_ms: u64,
) -> Result<OperationalAggregates, StorageError> {
        self.operational_aggregates(now_ms)
    }
    async fn claim_outbox(
&self,
worker_id: &str,
now_ms: u64,
) -> Result<Option<OutboxRecord>, StorageError> {
        self.claim_outbox(worker_id, now_ms)
    }
    async fn get_desired(
&self,
node_id: &str,
stream_id: &str,
) -> Result<Option<DesiredRecord>, StorageError> {
        self.get_desired(node_id, stream_id)
    }
    async fn get_intent(&self, intent_id: &str) -> Result<Option<IntentRecord>, StorageError> {
        self.get_intent(intent_id)
    }
    async fn list_intents(&self, node_id: Option<&str>) -> Result<Vec<IntentRecord>, StorageError> {
        self.list_intents(node_id)
    }
    async fn recover_reconciliation(&self, now_ms: u64) -> Result<(), StorageError> {
        self.recover_reconciliation(now_ms)
    }
    async fn wake_node(&self, node_id: &str, now_ms: u64) -> Result<(), StorageError> {
        self.wake_node(node_id, now_ms)
    }
    async fn list_events(&self, node_id: Option<&str>) -> Result<Vec<StoredEvent>, StorageError> {
        self.list_events(node_id)
    }
    async fn prune_events(&self, retain: usize) -> Result<usize, StorageError> {
        self.prune_events(retain)
    }
    async fn prune_operation_history(
&self,
older_than_ms: i64,
max_retained: i64,
) -> Result<usize, StorageError> {
        self.prune_operation_history(older_than_ms, max_retained)
    }
    async fn prune_job_checkpoint_records(&self, older_than_ms: i64) -> Result<usize, StorageError> {
        self.prune_job_checkpoint_records(older_than_ms)
    }
    async fn prune_audit_events(
&self,
older_than_ms: i64,
max_retained: i64,
) -> Result<usize, StorageError> {
        self.prune_audit_events(older_than_ms, max_retained)
    }
    async fn prune_processed_outbox(
&self,
older_than_ms: i64,
max_retained: i64,
) -> Result<usize, StorageError> {
        self.prune_processed_outbox(older_than_ms, max_retained)
    }
    async fn prune_terminal_attempts(
&self,
older_than_ms: i64,
max_retained: i64,
) -> Result<usize, StorageError> {
        self.prune_terminal_attempts(older_than_ms, max_retained)
    }
    async fn claim_attempt(&self, intent_id: &str) -> Result<Option<AttemptRecord>, StorageError> {
        self.claim_attempt(intent_id)
    }
    async fn complete_attempt(
&self,
attempt_id: &str,
state: &str,
failure_class: Option<&str>,
) -> Result<(), StorageError> {
        self.complete_attempt(attempt_id, state, failure_class)
    }
    async fn mark_attempt_dispatched(
&self,
attempt_id: &str,
expires_at_ms: u64,
) -> Result<(), StorageError> {
        self.mark_attempt_dispatched(attempt_id, expires_at_ms)
    }
    async fn expire_attempts(&self, now_ms: u64) -> Result<usize, StorageError> {
        self.expire_attempts(now_ms)
    }
    async fn record_observed(&self, mutation: ObservedMutation) -> Result<(), StorageError> {
        self.record_observed(mutation)
    }
    async fn mark_outbox_processed(&self, outbox_id: i64, now_ms: u64) -> Result<(), StorageError> {
        self.mark_outbox_processed(outbox_id, now_ms)
    }
    async fn record_audit(&self, record: AuditRecord) -> Result<i64, StorageError> {
        self.record_audit(record)
    }
    async fn list_audit(&self, resource_id: Option<&str>) -> Result<Vec<AuditRecord>, StorageError> {
        self.list_audit(resource_id)
    }
    async fn create_rollout(
&self,
rollout: RolloutRecord,
targets: Vec<RolloutTargetRecord>,
) -> Result<(), StorageError> {
        self.create_rollout(rollout, targets)
    }
    async fn create_rollout_with_content(
&self,
rollout: RolloutRecord,
targets: Vec<RolloutTargetRecord>,
content: &str,
created_by: Option<&str>,
) -> Result<(), StorageError> {
        self.create_rollout_with_content(rollout, targets, content, created_by)
    }
    async fn get_rollout(&self, rollout_id: &str) -> Result<Option<RolloutRecord>, StorageError> {
        self.get_rollout(rollout_id)
    }
    async fn list_rollout_targets(
&self,
rollout_id: &str,
) -> Result<Vec<RolloutTargetRecord>, StorageError> {
        self.list_rollout_targets(rollout_id)
    }
    async fn update_rollout(
&self,
rollout_id: &str,
state: &str,
current_batch: u32,
updated_at_ms: u64,
) -> Result<(), StorageError> {
        self.update_rollout(rollout_id, state, current_batch, updated_at_ms)
    }
    async fn update_rollout_target(&self, update: RolloutTargetUpdate) -> Result<(), StorageError> {
        self.update_rollout_target(update)
    }
    async fn get_config_version_content(
&self,
config_version_id: &str,
) -> Result<Option<String>, StorageError> {
        self.get_config_version_content(config_version_id)
    }
    async fn recover_rollouts(&self) -> Result<Vec<RolloutRecord>, StorageError> {
        self.recover_rollouts()
    }
    async fn list_rollouts(&self) -> Result<Vec<RolloutRecord>, StorageError> {
        self.list_rollouts()
    }
    async fn upsert_operation(&self, operation: PersistedOperation) -> Result<(), StorageError> {
        self.upsert_operation(operation)
    }
    async fn get_operation(
&self,
operation_id: &str,
) -> Result<Option<PersistedOperation>, StorageError> {
        self.get_operation(operation_id)
    }
    async fn list_operations(
&self,
node_id: Option<&str>,
) -> Result<Vec<PersistedOperation>, StorageError> {
        self.list_operations(node_id)
    }
    async fn list_job_start_operations(
&self,
resource_id: &str,
) -> Result<Vec<PersistedOperation>, StorageError> {
        self.list_job_start_operations(resource_id)
    }
    async fn upsert_job(&self, mut job: JobRecord) -> Result<JobRecord, StorageError> {
        self.upsert_job(job)
    }
    async fn update_job_with_expected_generation(
&self,
mut job: JobRecord,
expected_generation: u64,
) -> Result<JobRecord, StorageError> {
        self.update_job_with_expected_generation(job, expected_generation)
    }
    async fn get_job(&self, job_id: &str) -> Result<Option<JobRecord>, StorageError> {
        self.get_job(job_id)
    }
    async fn upsert_job_version(&self, record: JobVersionRecord) -> Result<(), StorageError> {
        self.upsert_job_version(record)
    }
    async fn list_job_versions(&self, job_id: &str) -> Result<Vec<JobVersionRecord>, StorageError> {
        self.list_job_versions(job_id)
    }
    async fn list_jobs(&self) -> Result<Vec<JobRecord>, StorageError> {
        self.list_jobs()
    }
    async fn update_job(
&self,
job_id: &str,
desired_state: Option<&str>,
observed_state: Option<&str>,
convergence: Option<&str>,
generation: Option<u64>,
checkpoint_id: Option<&str>,
last_error: Option<&str>,
) -> Result<Option<JobRecord>, StorageError> {
        self.update_job(job_id, desired_state, observed_state, convergence, generation, checkpoint_id, last_error)
    }
    async fn update_job_observation(
&self,
job_id: &str,
observed_state: &str,
convergence: &str,
generation: u64,
expected_generation: u64,
checkpoint_id: Option<&str>,
last_error: Option<&str>,
) -> Result<Option<JobRecord>, StorageError> {
        self.update_job_observation(job_id, observed_state, convergence, generation, expected_generation, checkpoint_id, last_error)
    }
    async fn update_job_desired_state(
&self,
job_id: &str,
desired_state: &str,
expected_generation: u64,
) -> Result<Option<JobRecord>, StorageError> {
        self.update_job_desired_state(job_id, desired_state, expected_generation)
    }
    async fn upsert_job_checkpoint(&self, record: JobCheckpointRecord) -> Result<(), StorageError> {
        self.upsert_job_checkpoint(record)
    }
    async fn list_job_checkpoints(
&self,
job_id: &str,
) -> Result<Vec<JobCheckpointRecord>, StorageError> {
        self.list_job_checkpoints(job_id)
    }
    async fn delete_job_checkpoint(
&self,
job_id: &str,
checkpoint_id: &str,
) -> Result<(), StorageError> {
        self.delete_job_checkpoint(job_id, checkpoint_id)
    }
}
