//! Command queue: intent/attempt enqueue, results, operation listing.

use super::command_metrics::CommandMetrics;
use super::error::HubError;
use super::nodes::required_capabilities;
use super::wire::{
    AgentAuth, AgentCommand, CommandResult, HubOperation, HubOperationState, NodeConnectionState,
};
use super::{now_ms, persist_operation, Hub, HUB_SEQUENCE, MAX_COMMANDS_PER_NODE, MAX_OPERATIONS};
use crate::storage::{AttemptRecord, IntentRecord};
use arkflow_core::control::NodeMaintenanceState;
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use subtle::ConstantTimeEq;

/// An operation whose (Job, generation, operation) key has been retried this
/// many times by the expiry sweep reaches a terminal failed state and is no
/// longer re-enqueued.
pub(crate) const MAX_JOB_OPERATION_RETRIES: u32 = 3;

fn apply_intent_metadata(operation: &mut HubOperation, intent: IntentRecord) {
    operation.intent_id = Some(intent.intent_id);
    operation.generation = intent.generation;
    operation.config_version_id = intent.config_version_id;
    operation.intent_state = Some(intent.state);
    operation.convergence_state = Some(intent.convergence_state);
    operation.retry_count = intent.retry_count;
    operation.next_retry_at_ms = intent.next_retry_at_ms;
    operation.failure_class = intent.failure_class;
    operation.superseded_by_intent_id = intent.superseded_by_intent_id;
    operation.superseded_generation = intent.superseded_generation;
    operation.created_at_ms = intent.created_at_ms;
    operation.observed_generation = intent.observed_generation;
    operation.observed_state = intent.observed_state;
    if operation.intent_state.as_deref() == Some("converged") {
        operation.state = HubOperationState::Succeeded;
        operation.progress = 100;
    } else if operation.intent_state.as_deref() == Some("blocked") {
        operation.state = HubOperationState::Failed;
    } else if operation.intent_state.as_deref() == Some("superseded") {
        operation.state = HubOperationState::Superseded;
    }
}

fn operation_from_intent(intent: IntentRecord) -> HubOperation {
    let intent_id = intent.intent_id.clone();
    let state = match intent.state.as_str() {
        "converged" => HubOperationState::Succeeded,
        "blocked" => HubOperationState::Failed,
        "superseded" => HubOperationState::Superseded,
        _ => HubOperationState::Queued,
    };
    HubOperation {
        id: intent_id.clone(),
        intent_id: Some(intent_id.clone()),
        command_id: format!("intent:{intent_id}"),
        node_id: intent.node_id,
        operation: "reconcile".into(),
        resource_id: intent.stream_id,
        checkpoint_id: None,
        generation: intent.generation,
        attempt_id: None,
        config_version_id: intent.config_version_id,
        expires_at_ms: None,
        state,
        progress: if state == HubOperationState::Succeeded {
            100
        } else {
            0
        },
        created_at_ms: intent.created_at_ms,
        dispatched_at_ms: None,
        acknowledged_at_ms: None,
        finished_at_ms: None,
        correlation_id: None,
        error: None,
        failure_class: intent.failure_class,
        intent_state: Some(intent.state),
        convergence_state: Some(intent.convergence_state),
        retry_count: intent.retry_count,
        next_retry_at_ms: intent.next_retry_at_ms,
        superseded_by_intent_id: intent.superseded_by_intent_id,
        superseded_generation: intent.superseded_generation,
        observed_generation: intent.observed_generation,
        observed_state: intent.observed_state,
        result: None,
    }
}

pub(crate) fn is_durable_job_start(operation: &HubOperation) -> bool {
    operation.operation == "job_start"
        && (operation.state == HubOperationState::Succeeded
            || operation.failure_class.as_deref() == Some("recovery_required"))
}

impl Hub {
    pub async fn enqueue(
        &self,
        node_id: String,
        operation: String,
        resource_id: String,
        correlation_id: Option<String>,
    ) -> Result<HubOperation, HubError> {
        self.enqueue_with_payload(node_id, operation, resource_id, correlation_id, None)
            .await
    }

    pub async fn enqueue_with_payload(
        &self,
        node_id: String,
        operation: String,
        resource_id: String,
        correlation_id: Option<String>,
        payload: Option<serde_json::Value>,
    ) -> Result<HubOperation, HubError> {
        self.enqueue_with_metadata(
            node_id,
            operation,
            resource_id,
            correlation_id,
            payload,
            0,
            None,
            None,
            None,
            None,
            None,
        )
        .await
    }

    pub async fn enqueue_intent(
        &self,
        node_id: String,
        operation: String,
        resource_id: String,
        generation: u64,
        action_id: Option<String>,
        correlation_id: Option<String>,
    ) -> Result<HubOperation, HubError> {
        self.enqueue_with_metadata(
            node_id,
            operation,
            resource_id,
            correlation_id,
            None,
            generation,
            action_id,
            None,
            None,
            None,
            None,
        )
        .await
    }

    pub async fn enqueue_attempt(&self, attempt: AttemptRecord) -> Result<HubOperation, HubError> {
        let intent_id = attempt.intent_id.clone();
        let payload = attempt
            .payload_json
            .as_deref()
            .and_then(|value| serde_json::from_str(value).ok());
        let mut operation = self
            .enqueue_with_metadata(
                attempt.node_id,
                attempt.operation,
                attempt.stream_id,
                None,
                payload,
                attempt.generation,
                attempt.action_id,
                attempt.config_version_id,
                Some(attempt.intent_id),
                Some(attempt.command_id),
                Some(attempt.attempt_id),
            )
            .await?;
        if let Some(storage) = self.storage.as_ref() {
            if let Some(intent) = storage
                .get_intent(&intent_id)
                .await
                .map_err(HubError::from)?
            {
                apply_intent_metadata(&mut operation, intent);
                self.operations
                    .write()
                    .await
                    .insert(operation.id.clone(), operation.clone());
                persist_operation(storage, &operation)
                    .await
                    .map_err(HubError::from)?;
            }
        }
        Ok(operation)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn enqueue_with_metadata(
        &self,
        node_id: String,
        operation: String,
        resource_id: String,
        correlation_id: Option<String>,
        payload: Option<serde_json::Value>,
        generation: u64,
        action_id: Option<String>,
        config_version_id: Option<String>,
        operation_id_override: Option<String>,
        command_id_override: Option<String>,
        attempt_id: Option<String>,
    ) -> Result<HubOperation, HubError> {
        let now = now_ms();
        let mut nodes = self.nodes.write().await;
        if !nodes.contains_key(&node_id) {
            drop(nodes);
            self.reject_enqueue(
                &operation,
                &resource_id,
                &node_id,
                correlation_id.as_deref(),
                generation,
                "node_unavailable",
            )
            .await;
            return Err(HubError::NodeUnavailable);
        }
        let node = nodes
            .get_mut(&node_id)
            .expect("node presence checked above");
        if node.resource.state != NodeConnectionState::Online
            || node.resource.lease_expires_at_ms <= now
            || node.resource.maintenance_state != NodeMaintenanceState::Active
        {
            drop(nodes);
            self.reject_enqueue(
                &operation,
                &resource_id,
                &node_id,
                correlation_id.as_deref(),
                generation,
                "node_unavailable",
            )
            .await;
            return Err(HubError::NodeUnavailable);
        }
        let required_capabilities = required_capabilities(&operation);
        let rollout_id = if operation == "apply_configuration" && resource_id == "__configuration__"
        {
            self.rollouts
                .read()
                .await
                .values()
                .find(|rollout| {
                    config_version_id
                        .as_deref()
                        .is_some_and(|id| rollout.config_version_id == id)
                        && !matches!(
                            rollout.state.as_str(),
                            "converged" | "cancelled" | "rolled_back"
                        )
                })
                .map(|rollout| rollout.rollout_id.clone())
        } else {
            None
        };
        if !node.resource.capabilities.is_empty()
            && required_capabilities.iter().any(|required| {
                !node
                    .resource
                    .capabilities
                    .iter()
                    .any(|capability| capability == required)
            })
        {
            let message = format!("node lacks capability for {operation}");
            drop(nodes);
            self.command_metrics.record_outcome(&operation, "rejected");
            if Self::job_audit_action(&operation).is_some() {
                self.record_job_operation_audit(
                    &operation,
                    &resource_id,
                    Some(&node_id),
                    correlation_id.as_deref(),
                    "rejected",
                    Some("incompatible_capability"),
                    message.clone(),
                )
                .await;
            } else {
                let _ = self
                    .record_audit_event(crate::storage::AuditRecord {
                        event_id: 0,
                        actor: None,
                        action: "command.dispatch".into(),
                        resource_type: "stream".into(),
                        resource_id: Some(resource_id),
                        node_id: Some(node_id),
                        stream_id: None,
                        correlation_id,
                        outcome: "rejected".into(),
                        failure_code: Some("incompatible_capability".into()),
                        message: Some(message.clone()),
                        occurred_at_ms: now,
                    })
                    .await;
            }
            return Err(HubError::Invalid(message));
        }
        let mut operations = self.operations.write().await;
        let requested_checkpoint_id = payload
            .as_ref()
            .and_then(|payload| payload.get("checkpoint_id"))
            .and_then(serde_json::Value::as_str);
        if let Some(operation_id) = operation_id_override.as_deref() {
            if let Some(existing) = operations.get(operation_id) {
                if existing.generation == generation {
                    // Idempotent replay: an operation still in flight — or one
                    // that terminally SUCCEEDED — returns the existing record.
                    // A terminal FAILURE must not wedge the intent: the
                    // reconciler enqueues retry attempts with fresh command
                    // ids, and the replacement operation below supersedes the
                    // failed record under the same intent id.
                    if matches!(
                        existing.state,
                        HubOperationState::Queued
                            | HubOperationState::Dispatched
                            | HubOperationState::Acknowledged
                            | HubOperationState::Running
                            | HubOperationState::Succeeded
                    ) {
                        return Ok(existing.clone());
                    }
                } else {
                    return Err(HubError::IdempotencyKeyReused);
                }
            }
        } else if let Some(existing) = operations.values().find(|item| {
            item.node_id == node_id
                && item.resource_id == resource_id
                && item.operation == operation
                && item.generation == generation
                && (item.checkpoint_id.as_deref() == requested_checkpoint_id
                    || (!operation.starts_with("job_checkpoint")
                        && !operation.starts_with("job_savepoint")))
                && matches!(
                    item.state,
                    HubOperationState::Queued
                        | HubOperationState::Dispatched
                        | HubOperationState::Acknowledged
                        | HubOperationState::Running
                )
        }) {
            return Ok(existing.clone());
        }
        let intent_id = operation_id_override.clone();
        let id = operation_id_override
            .unwrap_or_else(|| format!("hop-{}", HUB_SEQUENCE.fetch_add(1, Ordering::Relaxed)));
        let command_id = command_id_override
            .unwrap_or_else(|| format!("cmd-{}", HUB_SEQUENCE.fetch_add(1, Ordering::Relaxed)));
        if node.commands.len() + node.leased_commands.len() >= MAX_COMMANDS_PER_NODE {
            drop(nodes);
            drop(operations);
            self.reject_enqueue(
                &operation,
                &resource_id,
                &node_id,
                correlation_id.as_deref(),
                generation,
                "capacity",
            )
            .await;
            return Err(HubError::Capacity);
        }
        // Retry memory across re-enqueues: a replacement for the same
        // (node, resource, operation, generation) inherits the retry count
        // accumulated by its expired predecessors, so the sweep's cap bounds
        // the lifecycle of the logical command, not of one row. Job-scoped by
        // design: stream attempts keep their own reconciler retry semantics
        // and must never hit this cap.
        let inherited_retry_count = if operation.starts_with("job_") {
            operations
                .values()
                .filter(|item| {
                    item.node_id == node_id
                        && item.resource_id == resource_id
                        && item.operation == operation
                        && item.generation == generation
                })
                .map(|item| item.retry_count)
                .max()
                .unwrap_or(0)
        } else {
            0
        };
        if inherited_retry_count >= MAX_JOB_OPERATION_RETRIES {
            drop(nodes);
            drop(operations);
            self.reject_enqueue(
                &operation,
                &resource_id,
                &node_id,
                correlation_id.as_deref(),
                generation,
                "expired",
            )
            .await;
            return Err(HubError::Invalid(format!(
                "operation {operation} for {resource_id} exhausted its retry budget at generation {generation}"
            )));
        }
        let operation_record = HubOperation {
            id,
            intent_id,
            command_id: command_id.clone(),
            node_id: node_id.clone(),
            operation: operation.clone(),
            resource_id: resource_id.clone(),
            checkpoint_id: payload
                .as_ref()
                .and_then(|payload| payload.get("checkpoint_id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            generation,
            attempt_id: attempt_id.clone(),
            config_version_id: config_version_id.clone(),
            state: HubOperationState::Queued,
            progress: 0,
            created_at_ms: now,
            expires_at_ms: Some(now + self.config.lease_ttl_ms),
            dispatched_at_ms: None,
            acknowledged_at_ms: None,
            finished_at_ms: None,
            correlation_id: correlation_id.clone(),
            error: None,
            failure_class: None,
            intent_state: None,
            convergence_state: None,
            retry_count: inherited_retry_count,
            next_retry_at_ms: None,
            superseded_by_intent_id: None,
            superseded_generation: None,
            observed_generation: None,
            observed_state: None,
            result: None,
        };
        let command = AgentCommand {
            id: command_id.clone(),
            operation_id: operation_record.id.clone(),
            node_id,
            operation,
            resource_id,
            expires_at_ms: now + self.config.lease_ttl_ms,
            generation,
            action_id,
            config_version_id,
            attempt_id: attempt_id.clone(),
            rollout_id,
            correlation_id,
            payload,
            required_capabilities,
        };
        node.commands.push_back(command);
        if operations.len() >= MAX_OPERATIONS {
            // Evict the oldest TERMINAL operation when one exists; the map is
            // keyed by id (not insertion order), so lexicographically-first
            // is not oldest, and evicting an in-flight operation would make
            // the Agent's eventual command result miss with a 404 — which the
            // Agent treats as a fatal session error.
            let terminal = |operation: &HubOperation| {
                matches!(
                    operation.state,
                    HubOperationState::Succeeded
                        | HubOperationState::Failed
                        | HubOperationState::TimedOut
                        | HubOperationState::NodeUnavailable
                        | HubOperationState::Cancelled
                        | HubOperationState::Superseded
                )
            };
            let eviction = operations
                .values()
                .filter(|operation| terminal(operation))
                .min_by(|a, b| a.created_at_ms.cmp(&b.created_at_ms).then(a.id.cmp(&b.id)))
                .or_else(|| {
                    operations
                        .values()
                        .min_by(|a, b| a.created_at_ms.cmp(&b.created_at_ms).then(a.id.cmp(&b.id)))
                })
                .map(|operation| operation.id.clone());
            if let Some(oldest) = eviction {
                operations.remove(&oldest);
            }
        }
        // Audit the logical mutation once: reconciler re-dispatches of the
        // same (resource, operation, generation) are mechanics, not new
        // mutations, and would otherwise write one audit row per tick for a
        // persistently failing Job. Dispatch metrics count every attempt.
        let is_first_dispatch = !operations.values().any(|item| {
            item.resource_id == operation_record.resource_id
                && item.operation == operation_record.operation
                && item.generation == operation_record.generation
        });
        operations.insert(operation_record.id.clone(), operation_record.clone());
        drop(operations);
        drop(nodes);
        if let Some(storage) = self.storage.as_ref() {
            persist_operation(storage, &operation_record)
                .await
                .map_err(HubError::from)?;
        }
        self.command_metrics
            .record_outcome(&operation_record.operation, "enqueued");
        // Accepted-mutation audits cover the desired-state lifecycle only:
        // checkpoint/savepoint dispatches also flow through this funnel for
        // both operator triggers AND the periodic scheduler, so auditing
        // them here would log scheduler mechanics as operator mutations —
        // the HTTP trigger handler records those instead. Rejections of any
        // Job operation (below) stay audited at dispatch time.
        if is_first_dispatch
            && matches!(
                operation_record.operation.as_str(),
                "job_start" | "job_stop"
            )
        {
            self.record_job_operation_audit(
                &operation_record.operation,
                &operation_record.resource_id,
                Some(&operation_record.node_id),
                operation_record.correlation_id.as_deref(),
                "accepted",
                None,
                format!(
                    "{} generation={} queued for {}",
                    operation_record.operation,
                    operation_record.generation,
                    operation_record.node_id
                ),
            )
            .await;
        }
        Ok(operation_record)
    }

    /// Account and audit an enqueue that never reached the node's command
    /// queue. Metrics record every command class; the audit trail records
    /// Job lifecycle operations only, per the Actor-aware audit scope.
    async fn reject_enqueue(
        &self,
        operation: &str,
        resource_id: &str,
        node_id: &str,
        correlation_id: Option<&str>,
        generation: u64,
        failure_code: &str,
    ) {
        self.command_metrics.record_outcome(operation, failure_code);
        self.record_job_operation_audit(
            operation,
            resource_id,
            Some(node_id),
            correlation_id,
            "rejected",
            Some(failure_code),
            format!("{operation} generation={generation} rejected: {failure_code}"),
        )
        .await;
    }

    pub async fn command_result(
        &self,
        auth: AgentAuth,
        result: CommandResult,
    ) -> Result<HubOperation, HubError> {
        let mut nodes = self.nodes.write().await;
        let node = nodes.get(&auth.node_id).ok_or(HubError::Unauthorized)?;
        if !bool::from(
            auth.session_token
                .as_bytes()
                .ct_eq(node.session_token.as_bytes()),
        ) || now_ms() > node.session_expires_at_ms
        {
            return Err(HubError::Unauthorized);
        }
        // A terminal result settles the command lease as well as the
        // operation.  If the result is a duplicate, removing an already
        // absent lease is intentionally idempotent.
        if let Some(node) = nodes.get_mut(&auth.node_id) {
            node.leased_commands.remove(&result.command_id);
        }
        let mut operations = self.operations.write().await;
        let operation = operations
            .values_mut()
            .find(|item| item.command_id == result.command_id)
            .ok_or(HubError::NotFound)?;
        if operation.node_id != auth.node_id {
            return Err(HubError::Unauthorized);
        }
        if operation.id != result.operation_id {
            return Ok(operation.clone());
        }
        if operation.generation != result.generation {
            return Ok(operation.clone());
        }
        if matches!(
            operation.state,
            HubOperationState::Succeeded
                | HubOperationState::Failed
                | HubOperationState::TimedOut
                | HubOperationState::NodeUnavailable
                | HubOperationState::Cancelled
                | HubOperationState::Superseded
        ) {
            // A late result from an in-flight Agent must not resurrect or
            // otherwise rewrite a terminal operation. Returning the stored
            // record keeps duplicate result delivery idempotent.
            return Ok(operation.clone());
        }
        operation.state = result.state;
        operation.progress = result.progress;
        operation.error = result.error.clone();
        operation.failure_class = result.failure_class.clone();
        // Read-only command reports (validation/diff) ride the terminal
        // result; mutations never set the field.
        if result.result.is_some() {
            operation.result = result.result.clone();
        }
        if matches!(
            result.state,
            HubOperationState::Succeeded
                | HubOperationState::Failed
                | HubOperationState::TimedOut
                | HubOperationState::NodeUnavailable
                | HubOperationState::Cancelled
                | HubOperationState::Superseded
        ) {
            operation.finished_at_ms = Some(now_ms());
        }
        if matches!(result.state, HubOperationState::Acknowledged) {
            let now = now_ms();
            operation.acknowledged_at_ms = Some(now);
            // Enqueue-to-acknowledgement latency, the metric the spec
            // commits to; terminal outcomes settle the outcome counters.
            self.command_metrics.record_latency(
                &operation.operation,
                now.saturating_sub(operation.created_at_ms),
            );
            self.command_metrics
                .record_outcome(&operation.operation, "acknowledged");
        }
        if let Some(outcome) = CommandMetrics::outcome_label(result.state) {
            self.command_metrics
                .record_outcome(&operation.operation, outcome);
        }
        let updated = operation.clone();
        let attempt_id = operation.attempt_id.clone();
        drop(operations);
        drop(nodes);
        if let (Some(storage), Some(attempt_id)) = (self.storage.as_ref(), attempt_id) {
            let state = serde_json::to_value(result.state)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .unwrap_or_else(|| "failed".into());
            storage
                .complete_attempt(&attempt_id, &state, result.failure_class.clone())
                .await
                .map_err(HubError::from)?;
        }
        if let Some(storage) = self.storage.as_ref() {
            persist_operation(storage, &updated)
                .await
                .map_err(HubError::from)?;
        }
        if updated.operation.starts_with("job_") {
            if matches!(
                updated.operation.as_str(),
                "job_checkpoint" | "job_savepoint"
            ) {
                let checkpoint_id = result.observed_checkpoint_id.as_deref();
                let checkpoint_operations = self
                    .operations
                    .read()
                    .await
                    .values()
                    .filter(|operation| {
                        operation.resource_id == updated.resource_id
                            && operation.operation == updated.operation
                            && operation.generation == updated.generation
                            && operation.checkpoint_id.as_deref() == checkpoint_id
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let fallback_nodes = checkpoint_operations
                    .iter()
                    .map(|operation| operation.node_id.clone())
                    .collect::<BTreeSet<_>>();
                let (expected_nodes, planned_task_ids) =
                    self.checkpoint_scope(&updated, &fallback_nodes).await?;
                let succeeded_nodes = checkpoint_operations
                    .iter()
                    .filter(|operation| operation.state == HubOperationState::Succeeded)
                    .map(|operation| operation.node_id.clone())
                    .collect::<BTreeSet<_>>();
                let all_nodes_succeeded = if result.state == HubOperationState::Succeeded
                    && checkpoint_id.is_some()
                    && !expected_nodes.is_empty()
                {
                    expected_nodes == succeeded_nodes
                } else {
                    false
                };
                if all_nodes_succeeded {
                    let completed_operations = checkpoint_operations
                        .iter()
                        .filter(|operation| {
                            operation.state == HubOperationState::Succeeded
                                && expected_nodes.contains(&operation.node_id)
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    let commit_operation = if updated.operation == "job_savepoint" {
                        "job_savepoint_commit"
                    } else {
                        "job_checkpoint_commit"
                    };
                    let commit_exists = self.operations.read().await.values().any(|operation| {
                        operation.resource_id == updated.resource_id
                            && operation.operation == commit_operation
                            && operation.generation == updated.generation
                            && operation.checkpoint_id.as_deref() == checkpoint_id
                            && !matches!(
                                operation.state,
                                HubOperationState::Failed
                                    | HubOperationState::TimedOut
                                    | HubOperationState::NodeUnavailable
                                    | HubOperationState::Cancelled
                                    | HubOperationState::Superseded
                            )
                    });
                    if !commit_exists {
                        let coordinator = completed_operations.first().ok_or_else(|| {
                            HubError::Invalid("checkpoint has no successful agent".into())
                        })?;
                        self.enqueue_with_metadata(
                            coordinator.node_id.clone(),
                            commit_operation.into(),
                            updated.resource_id.clone(),
                            updated.correlation_id.clone(),
                            Some(serde_json::json!({
                                "checkpoint_id": checkpoint_id.unwrap_or_default(),
                                "manifest_nodes": completed_operations
                                    .iter()
                                    .map(|operation| operation.node_id.clone())
                                    .collect::<Vec<_>>(),
                                "planned_task_ids": planned_task_ids,
                            })),
                            updated.generation,
                            None,
                            updated.config_version_id.clone(),
                            None,
                            None,
                            None,
                        )
                        .await?;
                    }
                } else if result.state != HubOperationState::Succeeded {
                    self.complete_job_checkpoint(
                        &updated.resource_id,
                        checkpoint_id.unwrap_or("unknown"),
                        "failed",
                        result.checkpoint_manifest_uri.clone(),
                    )
                    .await?;
                }
            } else if matches!(
                updated.operation.as_str(),
                "job_checkpoint_commit" | "job_savepoint_commit"
            ) && result.observed_checkpoint_id.is_some()
            {
                self.complete_job_checkpoint(
                    &updated.resource_id,
                    result
                        .observed_checkpoint_id
                        .as_deref()
                        .unwrap_or("unknown"),
                    if result.state == HubOperationState::Succeeded {
                        "completed"
                    } else {
                        "failed"
                    },
                    result.checkpoint_manifest_uri.clone(),
                )
                .await?;
            }
            if matches!(updated.operation.as_str(), "job_start" | "job_stop") {
                // Task 7.1: derive the Job-level observed state from the
                // AGGREGATE of every planned assignment operation for the
                // same generation and action. One peer's acknowledgement or
                // transient failure never overwrites healthy peers, and the
                // Job reports running/stopped only after EVERY expected
                // assignment succeeded.
                match self.aggregate_job_observed_state(&updated, &result).await? {
                    Some(observed_state) => {
                        let _ = self
                            .observe_job(
                                &updated.resource_id,
                                updated.generation,
                                &observed_state,
                                None,
                                result.error.as_deref(),
                            )
                            .await?;
                    }
                    None => {
                        // The aggregate is still converging (pending
                        // assignments or retryable degradation): keep the
                        // current observed state and convergence label.
                    }
                }
            }
        }
        Ok(updated)
    }

    pub async fn operations(&self, node_id: Option<&str>) -> Vec<HubOperation> {
        let mut operations = self
            .operations
            .read()
            .await
            .values()
            .filter(|operation| node_id.is_none_or(|id| operation.node_id == id))
            .cloned()
            .collect::<Vec<_>>();
        if let Some(storage) = self.storage.as_ref() {
            if let Ok(intents) = storage.list_intents(node_id.map(str::to_owned)).await {
                let known = operations
                    .iter()
                    .filter_map(|operation| operation.intent_id.clone())
                    .collect::<std::collections::BTreeSet<_>>();
                operations.extend(
                    intents
                        .into_iter()
                        .filter(|intent| !known.contains(&intent.intent_id))
                        .map(operation_from_intent),
                );
            }
            if let Ok(persisted) = storage.list_operations(node_id.map(str::to_owned)).await {
                let known = operations
                    .iter()
                    .map(|operation| operation.id.clone())
                    .collect::<std::collections::BTreeSet<_>>();
                operations.extend(persisted.into_iter().filter_map(|stored| {
                    if known.contains(&stored.operation_id) {
                        None
                    } else {
                        serde_json::from_str(&stored.operation_json).ok()
                    }
                }));
            }
        }
        operations.sort_by_key(|operation| std::cmp::Reverse(operation.created_at_ms));
        operations.truncate(MAX_OPERATIONS);
        operations
    }

    pub async fn operation(&self, id: &str) -> Option<HubOperation> {
        if let Some(mut operation) = self.operations.read().await.get(id).cloned() {
            if let (Some(storage), Some(intent_id)) =
                (self.storage.as_ref(), operation.intent_id.as_deref())
            {
                if let Ok(Some(intent)) = storage.get_intent(intent_id).await {
                    apply_intent_metadata(&mut operation, intent);
                }
            }
            return Some(operation);
        }
        let storage = self.storage.as_ref()?;
        if let Ok(Some(intent)) = storage.get_intent(id.to_owned()).await {
            return Some(operation_from_intent(intent));
        }
        storage
            .get_operation(id.to_owned())
            .await
            .ok()
            .flatten()
            .and_then(|stored| serde_json::from_str(&stored.operation_json).ok())
    }

    pub async fn cancel_operation(&self, id: &str) -> Option<HubOperation> {
        let mut operations = self.operations.write().await;
        let operation = operations.get_mut(id)?;
        if matches!(
            operation.state,
            HubOperationState::Succeeded
                | HubOperationState::Failed
                | HubOperationState::TimedOut
                | HubOperationState::NodeUnavailable
                | HubOperationState::Cancelled
                | HubOperationState::Superseded
        ) {
            return Some(operation.clone());
        }
        operation.state = HubOperationState::Cancelled;
        operation.finished_at_ms = Some(now_ms());
        let node_id = operation.node_id.clone();
        let command_id = operation.command_id.clone();
        let cancelled = operation.clone();
        drop(operations);

        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(&node_id) {
            node.commands.retain(|command| command.id != command_id);
            node.leased_commands.remove(&command_id);
        }
        drop(nodes);
        if let Some(storage) = self.storage.as_ref() {
            let _ = persist_operation(storage, &cancelled).await;
        }
        Some(cancelled)
    }
}
