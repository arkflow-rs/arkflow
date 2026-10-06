//! Job lifecycle bookkeeping: observation, recovery, operational status.

use super::error::HubError;
use super::wire::{
    AgentOperation, CommandResult, HubOperation, HubOperationState, JobObservationRequest,
};
use super::{now_ms, persist_operation, Hub, MAX_OPERATIONS};
use crate::storage::{DesiredMutation, IntentRecord, JobRecord, StorageError};
use arkflow_core::control::{OperationalStatus, ReconciliationHealth};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use subtle::ConstantTimeEq;

impl Hub {
    pub async fn observe_job(
        &self,
        job_id: &str,
        generation: u64,
        observed_state: &str,
        checkpoint_id: Option<&str>,
        last_error: Option<&str>,
    ) -> Result<Option<JobRecord>, HubError> {
        let Some(current) = self.job(job_id).await? else {
            return Ok(None);
        };
        if generation != current.generation {
            return Ok(Some(current));
        }
        let convergence =
            if generation == current.generation && current.desired_state == observed_state {
                "converged"
            } else {
                "reconciling"
            };
        let updated = if let Some(storage) = &self.storage {
            // The observation is a compare-and-set on the generation the
            // caller read. A concurrent desired-state change or placement
            // move that bumped the generation must not be rolled back by a
            // stale report — that would fence every newer observation and
            // pin the Job in a reconciling loop.
            match storage
                .update_job_observation(
                    job_id,
                    observed_state,
                    convergence,
                    generation,
                    generation,
                    checkpoint_id.map(str::to_owned),
                    last_error.map(str::to_owned),
                )
                .await
            {
                Ok(updated) => updated,
                Err(StorageError::GenerationConflict { .. }) => {
                    return self.job(job_id).await;
                }
                Err(error) => return Err(HubError::from(error)),
            }
        } else {
            let mut jobs = self.jobs.write().await;
            let Some(job) = jobs.get_mut(job_id) else {
                return Ok(None);
            };
            if job.generation != generation {
                // Stale report under the same lock: a newer generation is
                // already recorded and must not be rolled back.
                return Ok(Some(job.clone()));
            }
            job.observed_state = observed_state.into();
            job.convergence = convergence.into();
            job.generation = generation;
            job.checkpoint_id = checkpoint_id
                .map(str::to_owned)
                .or_else(|| job.checkpoint_id.clone());
            job.last_error = last_error.map(str::to_owned);
            job.updated_at_ms = now_ms();
            Some(job.clone())
        };
        if let Some(job) = &updated {
            self.jobs
                .write()
                .await
                .insert(job.job_id.clone(), job.clone());
        }
        Ok(updated)
    }

    pub async fn report_job_observation(
        &self,
        request: JobObservationRequest,
    ) -> Result<Option<JobRecord>, HubError> {
        let nodes = self.nodes.read().await;
        let node = nodes
            .get(&request.auth.node_id)
            .ok_or(HubError::Unauthorized)?;
        if !bool::from(
            request
                .auth
                .session_token
                .as_bytes()
                .ct_eq(node.session_token.as_bytes()),
        ) || now_ms() > node.session_expires_at_ms
        {
            return Err(HubError::Unauthorized);
        }
        drop(nodes);
        let observed = self
            .observe_job(
                &request.job_id,
                request.generation,
                &request.state,
                None,
                request.error.as_deref(),
            )
            .await;
        // A failed observation means this node's kernel for the Job ended
        // (for example after a remote edge exhausted its reconnect budget).
        // Its Succeeded start would otherwise keep satisfying the
        // dispatch-skip forever and the dead tasks never restart at this
        // generation. Mirror the boot-change invalidation: settle the start
        // as retriable so the next reconcile re-dispatches with recovery.
        if request.state == "failed" {
            self.invalidate_succeeded_start_on_runtime_failure(
                &request.auth.node_id,
                &request.job_id,
                request.generation,
            )
            .await;
        }
        observed
    }

    /// Settle this node's Succeeded job_start at `generation` as TimedOut
    /// with the `runtime_failed` class so reconciliation re-dispatches it.
    async fn invalidate_succeeded_start_on_runtime_failure(
        &self,
        node_id: &str,
        job_id: &str,
        generation: u64,
    ) {
        let now = now_ms();
        let mutated: Vec<HubOperation> = {
            let mut operations = self.operations.write().await;
            operations
                .values_mut()
                .filter(|operation| {
                    operation.node_id == node_id
                        && operation.resource_id == job_id
                        && operation.operation == AgentOperation::JobStart
                        && operation.generation == generation
                        && operation.state == HubOperationState::Succeeded
                })
                .map(|operation| {
                    operation.state = HubOperationState::TimedOut;
                    // `recovery_required` (not a generic runtime class): the
                    // boot-change invalidation established that this class
                    // preserves the "a durable start succeeded once and must
                    // restore from a checkpoint" fact — the reconcile's
                    // recovery gating keys on it, so the re-dispatch carries
                    // a recovery artifact for durable Jobs.
                    operation.failure_class = Some("recovery_required".into());
                    operation.finished_at_ms = Some(now);
                    operation.error = Some(
                        "successful Job start invalidated by a failed runtime observation".into(),
                    );
                    operation.clone()
                })
                .collect()
        };
        if let Some(storage) = self.storage.as_ref() {
            for operation in &mutated {
                if let Err(error) = persist_operation(storage, operation).await {
                    tracing::warn!(
                        operation_id = %operation.id,
                        %error,
                        "failed to persist runtime-failure start invalidation"
                    );
                }
            }
        }
    }

    /// Restore recently persisted operations into the in-memory map so the
    /// terminal-state dispatch-skip memory and the `/operations` read API
    /// survive a restart. A restored non-terminal operation's command died
    /// with the old Hub process (commands are memory-only), so it can never
    /// complete as-is: it is settled as timed out — `job_start`/`job_stop`
    /// flow back through the existing reconcile retry path, checkpoint
    /// triggers are re-fired by the periodic scheduler — instead of lingering
    /// as a ghost pending record. Unparsable rows are skipped fail-open.
    pub async fn restore_persisted_operations(&self) -> Result<usize, HubError> {
        let Some(storage) = self.storage.as_ref() else {
            return Ok(0);
        };
        let persisted = storage
            .list_operations(None::<String>)
            .await
            .map_err(HubError::from)?;
        let mut restored = 0usize;
        let mut unsettled: Vec<HubOperation> = Vec::new();
        {
            let mut operations = self.operations.write().await;
            for record in persisted {
                if operations.len() >= MAX_OPERATIONS {
                    break;
                }
                if operations.contains_key(&record.operation_id) {
                    continue;
                }
                let Ok(mut operation) =
                    serde_json::from_str::<HubOperation>(&record.operation_json)
                else {
                    tracing::warn!(
                        operation_id = %record.operation_id,
                        "skipping unparsable persisted operation during recovery"
                    );
                    continue;
                };
                if matches!(
                    operation.state,
                    HubOperationState::Queued
                        | HubOperationState::Dispatched
                        | HubOperationState::Acknowledged
                        | HubOperationState::Running
                ) {
                    operation.state = HubOperationState::TimedOut;
                    operation.finished_at_ms = Some(now_ms());
                    operation.error = Some("Hub restarted before the command completed".into());
                    unsettled.push(operation.clone());
                }
                operations.insert(operation.id.clone(), operation);
                restored += 1;
            }
        }
        for operation in &unsettled {
            persist_operation(storage, operation)
                .await
                .map_err(HubError::from)?;
        }
        Ok(restored)
    }

    pub async fn recover_persisted_state(&self) -> Result<(), HubError> {
        if let Some(storage) = self.storage.as_ref() {
            storage
                .recover_reconciliation(now_ms())
                .await
                .map_err(HubError::from)?;
            // Operations are part of durable reconciliation state. Restore
            // them before the recovered Hub becomes visible, otherwise the
            // first reconciliation cannot tell an existing assignment from a
            // missing one and may dispatch duplicate starts.
            let recovered_operations = storage
                .list_operations(None::<String>)
                .await
                .map_err(HubError::from)?;
            let mut operations = self.operations.write().await;
            for persisted in recovered_operations {
                let operation = serde_json::from_str::<HubOperation>(&persisted.operation_json)
                    .map_err(|error| {
                        HubError::Invalid(format!(
                            "invalid persisted operation '{}': {error}",
                            persisted.operation_id
                        ))
                    })?;
                operations.insert(operation.id.clone(), operation);
            }
            drop(operations);
            let recovered = storage.recover_rollouts().await.map_err(HubError::from)?;
            let mut rollouts = self.rollouts.write().await;
            for rollout in recovered {
                rollouts.insert(rollout.rollout_id.clone(), rollout);
            }
            drop(rollouts);
            // Upgrade orchestrations re-enter their phases on the next
            // reconcile tick; recovering the cache first restores the
            // reconciler fence and the retention pin before the tick runs.
            self.recover_job_upgrade_cache().await?;
            let recovered_jobs = storage.list_jobs().await.map_err(HubError::from)?;
            let mut jobs = self.jobs.write().await;
            for job in recovered_jobs {
                jobs.insert(job.job_id.clone(), job);
            }
        }
        self.lifecycle.write().await.recovered = true;
        Ok(())
    }

    pub async fn record_reconcile_result(
        &self,
        started_at_ms: u64,
        result: &Result<Option<HubOperation>, HubError>,
    ) {
        let mut lifecycle = self.lifecycle.write().await;
        lifecycle.runs_total += 1;
        lifecycle.last_duration_ms = Some(now_ms().saturating_sub(started_at_ms));
        match result {
            Ok(_) => {
                lifecycle.last_success_at_ms = Some(now_ms());
                lifecycle.last_failure_class = None;
            }
            Err(error) => {
                lifecycle.failures_total += 1;
                lifecycle.last_error_at_ms = Some(now_ms());
                lifecycle.last_failure_class = Some(error.failure_class().into());
            }
        }
    }

    pub async fn operational_status(&self) -> Result<OperationalStatus, HubError> {
        let lifecycle = self.lifecycle.read().await.clone();
        let aggregates = self
            .storage
            .as_ref()
            .ok_or(HubError::StorageUnavailable)?
            .operational_aggregates(now_ms())
            .await
            .map_err(HubError::from)?;
        let map = |items: Vec<(String, u64)>| items.into_iter().collect();
        let degraded = lifecycle.failures_total > 0 || aggregates.stale_nodes > 0;
        Ok(OperationalStatus {
            status: if degraded { "degraded" } else { "healthy" }.into(),
            ready: lifecycle.recovered,
            recovered: lifecycle.recovered,
            storage_ready: true,
            reconciliation: ReconciliationHealth {
                state: if lifecycle.failures_total > 0 {
                    "degraded"
                } else {
                    "healthy"
                }
                .into(),
                runs_total: lifecycle.runs_total,
                failures_total: lifecycle.failures_total,
                last_success_at_ms: lifecycle.last_success_at_ms,
                last_error_at_ms: lifecycle.last_error_at_ms,
                last_duration_ms: lifecycle.last_duration_ms,
                last_failure_class: lifecycle.last_failure_class,
            },
            node_states: map(aggregates.node_states),
            maintenance_states: map(aggregates.maintenance_states),
            intent_states: map(aggregates.intent_states),
            convergence_states: map(aggregates.convergence_states),
            attempt_states: map(aggregates.attempt_states),
            failure_classes: map(aggregates.failure_classes),
            outbox_pending: aggregates.outbox_pending,
            outbox_claimed: aggregates.outbox_claimed,
            stale_nodes: aggregates.stale_nodes,
            active_attempts: aggregates.active_attempts,
            non_terminal_intents: aggregates.non_terminal_intents,
            oldest_pending_age_seconds: aggregates.oldest_pending_age_seconds,
        })
    }

    pub async fn set_desired_state(
        &self,
        mutation: DesiredMutation,
    ) -> Result<IntentRecord, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage.set_desired(mutation).await.map_err(HubError::from)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn restart_state(
        &self,
        node_id: String,
        stream_id: String,
        action_id: String,
        expected_generation: Option<u64>,
        actor: Option<String>,
        correlation_id: Option<String>,
        idempotency_key: Option<String>,
    ) -> Result<IntentRecord, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        let desired_state = storage
            .get_desired(node_id.clone(), stream_id.clone())
            .await
            .map_err(HubError::from)?
            .map(|desired| desired.desired_state)
            .unwrap_or_else(|| "running".into());
        storage
            .set_desired(DesiredMutation {
                node_id,
                stream_id,
                desired_state,
                config_version_id: None,
                action_id: Some(action_id),
                expected_generation,
                actor,
                correlation_id,
                idempotency_key,
                intent_type: None,
                payload_json: None,
            })
            .await
            .map_err(HubError::from)
    }

    /// Aggregate the Job-level observed state across every assignment
    /// operation of the same (resource, generation, action). Returns:
    /// * `Some("running" | "stopped")` when every expected assignment
    ///   succeeded (the terminal success form for the action);
    /// * `Some("failed")` when the COMPLETE set has been evaluated and an
    ///   assignment reports a non-retryable execution failure;
    /// * `None` while any assignment is still queued/dispatched/running or
    ///   degraded-but-retryable — the current observed snapshot stays.
    pub(crate) async fn aggregate_job_observed_state(
        &self,
        updated: &HubOperation,
        result: &CommandResult,
    ) -> Result<Option<String>, HubError> {
        let peers = self
            .operations
            .read()
            .await
            .values()
            .filter(|operation| {
                operation.resource_id == updated.resource_id
                    && operation.operation == updated.operation
                    && operation.generation == updated.generation
            })
            .cloned()
            .collect::<Vec<_>>();
        // A failed/expired attempt can be followed by a retry for the same
        // node and generation.  Aggregate only the newest operation per
        // assignment; otherwise the old terminal failure would continue to
        // make the whole Job look failed after the replacement succeeds.
        let mut latest_by_node = BTreeMap::<String, HubOperation>::new();
        for peer in peers {
            latest_by_node
                .entry(peer.node_id.clone())
                .and_modify(|current| {
                    if (peer.created_at_ms, peer.id.as_str())
                        > (current.created_at_ms, current.id.as_str())
                    {
                        *current = peer.clone();
                    }
                })
                .or_insert(peer);
        }
        let peers = latest_by_node.into_values().collect::<Vec<_>>();
        let fallback_nodes = peers
            .iter()
            .map(|peer| peer.node_id.clone())
            .collect::<BTreeSet<_>>();
        let (expected_nodes, _) = self.checkpoint_scope(updated, &fallback_nodes).await?;
        let expected_nodes = if expected_nodes.is_empty() {
            fallback_nodes
        } else {
            expected_nodes
        };
        let observed_nodes = peers
            .iter()
            .map(|peer| peer.node_id.clone())
            .collect::<BTreeSet<_>>();
        // A command result is only one assignment's result. Missing planned
        // nodes (including offline nodes filtered before dispatch) keep the
        // Job converging instead of making the first successful peer look
        // like a fully running Job.
        if observed_nodes != expected_nodes {
            return Ok(None);
        }
        let terminal_success = updated.operation == AgentOperation::JobStop;
        let succeeded = |state: &HubOperationState| {
            matches!(
                state,
                HubOperationState::Succeeded | HubOperationState::Running
            )
        };
        let permanently_failed = |state: &HubOperationState| {
            matches!(
                state,
                HubOperationState::Failed | HubOperationState::Superseded
            )
        };
        // Single-assignment operations keep the direct derivation.
        if peers.len() <= 1 {
            return Ok(Some(if succeeded(&result.state) {
                if terminal_success {
                    "stopped".to_string()
                } else {
                    "running".to_string()
                }
            } else if permanently_failed(&result.state) {
                "failed".to_string()
            } else {
                // A retryable single-node outcome stays observed-neutral.
                return Ok(None);
            }));
        }
        if peers.iter().all(|peer| succeeded(&peer.state)) {
            return Ok(Some(
                if terminal_success {
                    "stopped"
                } else {
                    "running"
                }
                .to_string(),
            ));
        }
        if peers
            .iter()
            .all(|peer| succeeded(&peer.state) || permanently_failed(&peer.state))
            && peers.iter().any(|peer| permanently_failed(&peer.state))
        {
            return Ok(Some("failed".to_string()));
        }
        // Pending, running, or degraded-retryable peers: keep observing.
        Ok(None)
    }
}
