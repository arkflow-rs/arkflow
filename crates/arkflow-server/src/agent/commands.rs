//! Hub command execution: the command dispatch loop and the Job operation
//! handlers. The string command protocol is unchanged.
use super::checkpoint::parse_recovery_payload;
use super::config::NodeAgentConfig;
use super::kernel::{JobRuntime, SplitPlacementPayload};
use super::now_ms;
use crate::hub::{AgentAuth, AgentCommand, AgentOperation, CommandResult, HubOperationState};
use arkflow_core::control::OperationState;
use arkflow_core::control_plane::ControlPlane;
use arkflow_core::job::{JobPlan, TaskAttempt};
use reqwest::Client;
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::warn;

/// How often to renew a command's execution lease: half of the Hub lease,
/// so the lease is renewed before it can expire. Older Hubs that do not
/// report `lease_ttl_ms` fall back to half of the time left on the command.
/// Clamped to a sane floor/ceiling.
pub(super) fn command_progress_interval(expires_at_ms: u64, lease_ttl_ms: u64) -> Duration {
    let lease_ms = if lease_ttl_ms > 0 {
        lease_ttl_ms
    } else {
        expires_at_ms.saturating_sub(now_ms())
    };
    Duration::from_millis((lease_ms / 2).clamp(500, 30_000))
}

/// The local deadline after a renewal: the Hub extends its lease by the full
/// `lease_ttl_ms` from each report, so the agent's own copy must advance by
/// the same amount (or, without a reported lease, by two report intervals).
pub(super) fn renewed_deadline_ms(lease_ttl_ms: u64, interval: Duration, now_ms: u64) -> u64 {
    let renewal_ms = if lease_ttl_ms > 0 {
        lease_ttl_ms
    } else {
        2 * u64::try_from(interval.as_millis()).unwrap_or(u64::MAX)
    };
    now_ms.saturating_add(renewal_ms)
}

/// Keeps a "running" progress report ticking while a command executes. The
/// Hub renews the command lease on Acknowledged/Running results, so an
/// operation that outlives `lease_ttl_ms` (a configuration apply, a Job
/// start) is not swept as TimedOut and dispatched a second time. Dropping
/// the guard stops the reports; every early-return path is covered.
pub(super) struct ProgressReports {
    cancel: tokio_util::sync::CancellationToken,
    handle: tokio::task::JoinHandle<()>,
    /// The locally tracked command deadline, advanced on every successful
    /// renewal report. The lifecycle watcher reads it instead of the
    /// command's original (stale) `expires_at_ms`.
    deadline_ms: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl ProgressReports {
    pub(super) fn deadline_ms(&self) -> u64 {
        self.deadline_ms.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Advance the local deadline after a CONFIRMED renewal (a report the
    /// Hub answered while still holding the operation as non-terminal).
    pub(super) fn mark_renewed(&self, lease_ttl_ms: u64, interval: Duration) {
        self.deadline_ms.store(
            renewed_deadline_ms(lease_ttl_ms, interval, now_ms()),
            std::sync::atomic::Ordering::Release,
        );
    }
}

impl Drop for ProgressReports {
    fn drop(&mut self) {
        self.cancel.cancel();
        // A report in flight is harmless: the Hub ignores non-terminal
        // updates once the operation is terminal.
        self.handle.abort();
    }
}

pub(super) fn spawn_progress_reports(
    client: &Client,
    config: &NodeAgentConfig,
    auth: &AgentAuth,
    template: &CommandResult,
    interval: Duration,
    lease_ttl_ms: u64,
    expires_at_ms: u64,
) -> ProgressReports {
    let cancel = tokio_util::sync::CancellationToken::new();
    let task_cancel = cancel.clone();
    let client = client.clone();
    let config = config.clone();
    let auth = auth.clone();
    let deadline_ms = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(expires_at_ms));
    let task_deadline = deadline_ms.clone();
    let mut update = template.clone();
    update.state = HubOperationState::Running;
    let handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick fires immediately: renew the lease up front so a
        // lease shorter than the interval cannot expire before the first
        // report.
        loop {
            tokio::select! {
                _ = task_cancel.cancelled() => break,
                _ = tick.tick() => {
                    match send_progress_report(&client, &config, &auth, update.clone()).await {
                        // The Hub renewed its lease: advance the local copy
                        // so the lifecycle watcher does not time out a command
                        // the Hub still considers alive.
                        Ok(true) => task_deadline.store(
                            renewed_deadline_ms(lease_ttl_ms, interval, now_ms()),
                            std::sync::atomic::Ordering::Release,
                        ),
                        // The Hub already settled the command (its lease
                        // sweep timed it out): stop reporting.
                        Ok(false) => break,
                        // Transport failure: retry on the next tick; the
                        // Hub's lease sweep remains the fallback.
                        Err(_) => {}
                    }
                }
            }
        }
    });
    ProgressReports {
        cancel,
        handle,
        deadline_ms,
    }
}

pub(super) async fn execute_command(
    client: &Client,
    cp: &ControlPlane,
    config: &NodeAgentConfig,
    auth: &AgentAuth,
    command: &AgentCommand,
    job_runtime: &JobRuntime,
    lease_ttl_ms: u64,
) -> Result<CommandResult, Box<dyn std::error::Error + Send + Sync>> {
    let mut result = CommandResult {
        command_id: command.id.clone(),
        operation_id: command.operation_id.clone(),
        state: HubOperationState::Acknowledged,
        progress: 5,
        error: None,
        correlation_id: command.correlation_id.clone(),
        generation: command.generation,
        observed_generation: None,
        action_id: command.action_id.clone(),
        failure_class: None,
        config_version_id: command.config_version_id.clone(),
        rollout_id: command.rollout_id.clone(),
        observed_checkpoint_id: None,
        checkpoint_manifest_uri: None,
        result: None,
    };
    if command_expired(command.expires_at_ms, now_ms()) {
        result.state = HubOperationState::TimedOut;
        result.error = Some("Command expired before execution".into());
        result.failure_class = Some("temporary_execution".into());
        return deliver_result(client, config, auth, result).await;
    }
    if command.operation.is_job() {
        let latest_generation = job_runtime.generation(&command.resource_id).await;
        if command_is_stale(command.generation, latest_generation) {
            result.state = HubOperationState::Superseded;
            result.error = Some("Job command generation is stale".into());
            result.observed_generation = latest_generation;
            result.failure_class = Some("stale_generation".into());
            return deliver_result(client, config, auth, result).await;
        }
        if matches!(
            command.operation,
            AgentOperation::JobCheckpoint
                | AgentOperation::JobSavepoint
                | AgentOperation::JobCheckpointCommit
                | AgentOperation::JobSavepointCommit
        ) {
            result.observed_checkpoint_id = command
                .payload
                .as_ref()
                .and_then(|payload| payload.get("checkpoint_id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
        }
        // Long Job operations keep the command lease alive while they run.
        let _progress = spawn_progress_reports(
            client,
            config,
            auth,
            &result,
            command_progress_interval(command.expires_at_ms, lease_ttl_ms),
            lease_ttl_ms,
            command.expires_at_ms,
        );
        // Confirm the Hub still holds the command before starting the Job
        // operation: if its lease sweep already settled it, a replacement may
        // be running and this one must not start.
        let mut confirmation = result.clone();
        confirmation.state = HubOperationState::Running;
        if !send_progress_report(client, config, auth, confirmation).await? {
            result.state = HubOperationState::TimedOut;
            result.progress = 100;
            result.error = Some("command was settled by the Hub before execution".into());
            result.failure_class = Some("temporary_execution".into());
            return deliver_result(client, config, auth, result).await;
        }
        let operation = execute_job_operation(command, config, job_runtime).await;
        if let Ok(Some(manifest_uri)) = &operation {
            result.checkpoint_manifest_uri = Some(manifest_uri.clone());
        }
        let outcome = operation.map(|_| ());
        result.state = if outcome.is_ok() {
            HubOperationState::Succeeded
        } else {
            HubOperationState::Failed
        };
        result.progress = 100;
        result.error = outcome.err();
        result.observed_generation = Some(command.generation);
        result.failure_class = result.error.as_ref().map(|_| "permanent_execution".into());
        return deliver_result(client, config, auth, result).await;
    }
    let latest_generation = cp
        .runtime_manager()
        .snapshots()
        .await
        .into_iter()
        .find(|stream| stream.id == command.resource_id)
        .map(|stream| stream.desired_generation);
    if command_is_stale(command.generation, latest_generation) {
        result.state = HubOperationState::Superseded;
        result.error = Some("Command generation is older than the local desired generation".into());
        result.observed_generation = latest_generation;
        result.failure_class = Some("stale_generation".into());
        return deliver_result(client, config, auth, result).await;
    }
    // Configuration/lifecycle execution can outlive the lease; report
    // progress until the terminal result is delivered.
    let progress_interval = command_progress_interval(command.expires_at_ms, lease_ttl_ms);
    let progress = spawn_progress_reports(
        client,
        config,
        auth,
        &result,
        progress_interval,
        lease_ttl_ms,
        command.expires_at_ms,
    );
    // The acknowledgement is itself a lease renewal: advance the local
    // deadline when the Hub still holds the operation as non-terminal. A
    // terminal response means the Hub already settled this command (its
    // lease sweep timed it out and may have dispatched a replacement):
    // starting the operation now would run it twice.
    if !send_progress_report(client, config, auth, result.clone()).await? {
        result.state = HubOperationState::TimedOut;
        result.progress = 100;
        result.error = Some("command was settled by the Hub before execution".into());
        result.failure_class = Some("temporary_execution".into());
        return deliver_result(client, config, auth, result).await;
    }
    progress.mark_renewed(lease_ttl_ms, progress_interval);
    match &command.operation {
        AgentOperation::ValidateConfiguration | AgentOperation::DiffConfiguration => {
            // Read-only reports dispatched by the Hub for console clients.
            // Execution success is distinct from the report's own verdict: an
            // invalid candidate still validates successfully, so the report
            // rides the result payload instead of the error channel.
            let outcome: Result<serde_json::Value, String> =
                if matches!(command.operation, AgentOperation::ValidateConfiguration) {
                    command
                        .payload
                        .clone()
                        .ok_or_else(|| "missing configuration payload".to_string())
                        .and_then(|payload| {
                            serde_json::from_value::<arkflow_core::configuration::ConfigCandidate>(
                                payload,
                            )
                            .map_err(|error| error.to_string())
                        })
                        .map(|candidate| {
                            serde_json::to_value(cp.validate_configuration(&candidate))
                                .unwrap_or_default()
                        })
                } else {
                    // Errors here are part of the command's outcome (reported to the
                    // Hub as a Failed result), never a transport-level failure:
                    // returning them from execute_command would abort the session
                    // and trigger a redelivery loop for what is a client input error.
                    (|| -> Result<serde_json::Value, String> {
                        let from = command
                            .payload
                            .as_ref()
                            .and_then(|payload| payload.get("from"))
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| "missing configuration version".to_string())?;
                        let to = command
                            .payload
                            .as_ref()
                            .and_then(|payload| payload.get("to"))
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| "missing configuration version".to_string())?;
                        let from_candidate = cp
                            .version_store()
                            .load(from)
                            .map_err(|error| error.to_string())?;
                        let to_candidate = cp
                            .version_store()
                            .load(to)
                            .map_err(|error| error.to_string())?;
                        Ok(serde_json::json!({
                            "from": from,
                            "to": to,
                            "changed": from_candidate.content != to_candidate.content,
                            "from_format": from_candidate.format,
                            "to_format": to_candidate.format,
                        }))
                    })()
                };
            let (state, report, error, failure_class) = match outcome {
                Ok(report) => (
                    HubOperationState::Succeeded,
                    Some(report),
                    None,
                    None::<String>,
                ),
                Err(error) => (
                    HubOperationState::Failed,
                    None,
                    Some(error),
                    Some("permanent_execution".into()),
                ),
            };
            return deliver_result(
                client,
                config,
                auth,
                CommandResult {
                    command_id: command.id.clone(),
                    operation_id: command.operation_id.clone(),
                    state,
                    progress: 100,
                    error,
                    correlation_id: command.correlation_id.clone(),
                    generation: command.generation,
                    observed_generation: None,
                    action_id: command.action_id.clone(),
                    failure_class,
                    config_version_id: command.config_version_id.clone(),
                    rollout_id: command.rollout_id.clone(),
                    observed_checkpoint_id: None,
                    checkpoint_manifest_uri: None,
                    result: report,
                },
            )
            .await;
        }
        AgentOperation::ApplyConfiguration | AgentOperation::RollbackConfiguration => {
            let outcome: Result<(), String> =
                if matches!(command.operation, AgentOperation::ApplyConfiguration) {
                    let candidate = command
                        .payload
                        .clone()
                        .ok_or_else(|| "missing configuration payload".to_string())
                        .and_then(|payload| {
                            serde_json::from_value::<arkflow_core::configuration::ConfigCandidate>(
                                payload,
                            )
                            .map_err(|error| error.to_string())
                        });
                    match candidate {
                        Ok(candidate) => cp
                            .apply_configuration(&candidate)
                            .await
                            .map(|_| ())
                            .map_err(|error| error.to_string()),
                        Err(error) => Err(error),
                    }
                } else {
                    let version = command
                        .payload
                        .as_ref()
                        .and_then(|payload| payload.get("id"))
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| "missing configuration version".to_string());
                    match version {
                        Ok(version) => cp
                            .rollback_configuration(version)
                            .await
                            .map(|_| ())
                            .map_err(|error| error.to_string()),
                        Err(error) => Err(error),
                    }
                };
            if outcome.is_ok() {
                if let Some(version) = command.config_version_id.clone() {
                    cp.runtime_manager()
                        .set_observed_config_version(version)
                        .await;
                }
            }
            let succeeded = outcome.is_ok();
            let failure_class = if succeeded {
                None
            } else {
                Some("permanent_execution".into())
            };
            let error = outcome.err();
            return deliver_result(
                client,
                config,
                auth,
                CommandResult {
                    command_id: command.id.clone(),
                    operation_id: command.operation_id.clone(),
                    state: if succeeded {
                        HubOperationState::Succeeded
                    } else {
                        HubOperationState::Failed
                    },
                    progress: 100,
                    error,
                    correlation_id: command.correlation_id.clone(),
                    generation: command.generation,
                    observed_generation: None,
                    action_id: command.action_id.clone(),
                    failure_class,
                    config_version_id: command.config_version_id.clone(),
                    rollout_id: command.rollout_id.clone(),
                    observed_checkpoint_id: None,
                    checkpoint_manifest_uri: None,
                    result: None,
                },
            )
            .await;
        }
        AgentOperation::Start
        | AgentOperation::Stop
        | AgentOperation::Restart
        | AgentOperation::Reconcile
        | AgentOperation::Unknown(_) => {
            let operation = match cp
                .lifecycle(
                    &command.resource_id,
                    command.operation.as_str(),
                    command.correlation_id.clone(),
                )
                .await
            {
                Ok(operation) => operation,
                Err(error) => {
                    return deliver_result(
                        client,
                        config,
                        auth,
                        CommandResult {
                            command_id: command.id.clone(),
                            operation_id: command.operation_id.clone(),
                            state: HubOperationState::Failed,
                            progress: 100,
                            error: Some(error.to_string()),
                            correlation_id: command.correlation_id.clone(),
                            generation: command.generation,
                            observed_generation: None,
                            action_id: command.action_id.clone(),
                            failure_class: Some("permanent_execution".into()),
                            config_version_id: command.config_version_id.clone(),
                            rollout_id: command.rollout_id.clone(),
                            observed_checkpoint_id: None,
                            checkpoint_manifest_uri: None,
                            result: None,
                        },
                    )
                    .await;
                }
            };
            if !send_progress_report(
                client,
                config,
                auth,
                CommandResult {
                    command_id: command.id.clone(),
                    // The Hub matches results against ITS operation id
                    // (`hop-*` / intent ids); the local runtime operation id
                    // (`op-*`) is agent-internal and would be silently
                    // rejected, leaving the Hub operation non-terminal.
                    operation_id: command.operation_id.clone(),
                    state: HubOperationState::Running,
                    progress: 10,
                    error: None,
                    correlation_id: command.correlation_id.clone(),
                    generation: command.generation,
                    observed_generation: None,
                    action_id: command.action_id.clone(),
                    failure_class: None,
                    config_version_id: command.config_version_id.clone(),
                    rollout_id: command.rollout_id.clone(),
                    observed_checkpoint_id: None,
                    checkpoint_manifest_uri: None,
                    result: None,
                },
            )
            .await?
            {
                // The Hub already settled the command: stop watching and let
                // its reconciler own the replacement instead of reporting a
                // result the Hub ignores.
                result.state = HubOperationState::TimedOut;
                result.progress = 100;
                result.error = Some(
                    "command was settled by the Hub before its lifecycle operation completed"
                        .into(),
                );
                result.failure_class = Some("temporary_execution".into());
                return deliver_result(client, config, auth, result).await;
            }
            progress.mark_renewed(lease_ttl_ms, progress_interval);
            loop {
                let current = cp.operation(&operation.id).await;
                match lifecycle_wait_decision(
                    current.as_ref().map(|record| record.state),
                    progress.deadline_ms(),
                    now_ms(),
                ) {
                    LifecycleWait::Terminal(state) => {
                        if let Some(action_id) = command.action_id.clone() {
                            cp.runtime_manager()
                                .set_last_completed_action(&command.resource_id, action_id)
                                .await;
                        }
                        let state = match state {
                            OperationState::Succeeded => HubOperationState::Succeeded,
                            OperationState::Cancelled => HubOperationState::Cancelled,
                            OperationState::TimedOut => HubOperationState::TimedOut,
                            _ => HubOperationState::Failed,
                        };
                        return deliver_result(
                            client,
                            config,
                            auth,
                            CommandResult {
                                command_id: command.id.clone(),
                                operation_id: command.operation_id.clone(),
                                state,
                                progress: 100,
                                error: current.and_then(|record| record.error),
                                correlation_id: command.correlation_id.clone(),
                                generation: command.generation,
                                observed_generation: None,
                                action_id: command.action_id.clone(),
                                failure_class: match state {
                                    HubOperationState::TimedOut => {
                                        Some("temporary_execution".into())
                                    }
                                    HubOperationState::Failed => Some("permanent_execution".into()),
                                    _ => None,
                                },
                                config_version_id: command.config_version_id.clone(),
                                rollout_id: command.rollout_id.clone(),
                                observed_checkpoint_id: None,
                                checkpoint_manifest_uri: None,
                                result: None,
                            },
                        )
                        .await;
                    }
                    LifecycleWait::RecordGone => {
                        // The local operation record is gone (e.g. evicted from
                        // the bounded operation store), so its outcome can no
                        // longer be observed. The execution itself keeps
                        // running; report an ambiguous temporary failure so the
                        // Hub settles the command through its retry path
                        // instead of this watcher spinning forever.
                        return deliver_result(
                            client,
                            config,
                            auth,
                            CommandResult {
                                command_id: command.id.clone(),
                                operation_id: command.operation_id.clone(),
                                state: HubOperationState::Failed,
                                progress: 100,
                                error: Some(format!(
                                    "operation record {} is no longer observable on the agent",
                                    operation.id
                                )),
                                correlation_id: command.correlation_id.clone(),
                                generation: command.generation,
                                observed_generation: None,
                                action_id: command.action_id.clone(),
                                failure_class: Some("temporary_execution".into()),
                                config_version_id: command.config_version_id.clone(),
                                rollout_id: command.rollout_id.clone(),
                                observed_checkpoint_id: None,
                                checkpoint_manifest_uri: None,
                                result: None,
                            },
                        )
                        .await;
                    }
                    LifecycleWait::DeadlinePassed => {
                        // The (renewed) command deadline passed without a
                        // terminal observation; the Hub has stopped waiting.
                        return deliver_result(
                            client,
                            config,
                            auth,
                            CommandResult {
                                command_id: command.id.clone(),
                                operation_id: command.operation_id.clone(),
                                state: HubOperationState::TimedOut,
                                progress: 100,
                                error: Some(
                                    "operation did not reach a terminal state before the command deadline"
                                        .into(),
                                ),
                                correlation_id: command.correlation_id.clone(),
                                generation: command.generation,
                                observed_generation: None,
                                action_id: command.action_id.clone(),
                                failure_class: Some("temporary_execution".into()),
                                config_version_id: command.config_version_id.clone(),
                                rollout_id: command.rollout_id.clone(),
                                observed_checkpoint_id: None,
                                checkpoint_manifest_uri: None,
                                result: None,
                            },
                        )
                        .await;
                    }
                    LifecycleWait::Continue => {}
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        // Job-plane commands returned through the dedicated path above the
        // staleness/ack prefix; listing their variants keeps this dispatch
        // exhaustive so a new AgentOperation variant fails compilation here
        // until it is routed explicitly.
        AgentOperation::JobStart
        | AgentOperation::JobRestart
        | AgentOperation::JobStop
        | AgentOperation::JobCheckpoint
        | AgentOperation::JobSavepoint
        | AgentOperation::JobCheckpointCommit
        | AgentOperation::JobSavepointCommit => unreachable!("job-plane commands dispatch above"),
    }
}

/// One lifecycle-watcher iteration's decision, factored out so the "the
/// operation is still running when the deadline passes" behavior can be
/// unit-tested with a fixed clock.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum LifecycleWait {
    Terminal(OperationState),
    RecordGone,
    DeadlinePassed,
    Continue,
}

pub(super) fn lifecycle_wait_decision(
    current: Option<OperationState>,
    deadline_ms: u64,
    now_ms: u64,
) -> LifecycleWait {
    match current {
        Some(state)
            if matches!(
                state,
                OperationState::Succeeded
                    | OperationState::Failed
                    | OperationState::Cancelled
                    | OperationState::TimedOut
            ) =>
        {
            LifecycleWait::Terminal(state)
        }
        None => LifecycleWait::RecordGone,
        Some(_) if command_expired(deadline_ms, now_ms) => LifecycleWait::DeadlinePassed,
        Some(_) => LifecycleWait::Continue,
    }
}

/// Execute a Job command without allowing an execution failure to escape the
/// command-result path. In particular, checkpoint and aggregation failures
/// must become terminal `Failed` results so the Hub can settle the command and
/// the Agent session remains available for subsequent work.
pub(super) async fn execute_job_operation(
    command: &AgentCommand,
    config: &NodeAgentConfig,
    job_runtime: &JobRuntime,
) -> Result<Option<String>, String> {
    match &command.operation {
        AgentOperation::JobStart | AgentOperation::JobRestart => {
            let payload = command
                .payload
                .as_ref()
                .ok_or_else(|| "missing Job plan payload".to_string())?;
            let parsed = JobStartCommandPayload::from_payload(payload)?;
            if matches!(command.operation, AgentOperation::JobRestart) {
                job_runtime
                    .stop(&command.resource_id, command.generation)
                    .await?;
            }
            job_runtime
                .start(
                    parsed.plan,
                    parsed.assignments,
                    command.generation,
                    parsed.recovery_id,
                    parsed.recovery_savepoint,
                    &config.node_id,
                    &parsed.split,
                )
                .await?;
            Ok(None)
        }
        AgentOperation::JobStop => {
            job_runtime
                .stop(&command.resource_id, command.generation)
                .await?;
            Ok(None)
        }
        AgentOperation::JobCheckpoint | AgentOperation::JobSavepoint => {
            let payload = command
                .payload
                .as_ref()
                .ok_or_else(|| "missing checkpoint payload".to_string())?;
            let parsed = CheckpointCommandPayload::from_payload(payload)?;
            let manifest_uri = job_runtime
                .checkpoint(
                    &command.resource_id,
                    &parsed.checkpoint_id,
                    command.generation,
                    matches!(command.operation, AgentOperation::JobSavepoint),
                    &config.node_id,
                )
                .await?;
            Ok(Some(manifest_uri))
        }
        AgentOperation::JobCheckpointCommit | AgentOperation::JobSavepointCommit => {
            let payload = command
                .payload
                .as_ref()
                .ok_or_else(|| "missing checkpoint aggregation payload".to_string())?;
            let parsed = CheckpointCommitCommandPayload::from_payload(payload)?;
            let manifest_uri = job_runtime
                .aggregate_checkpoint(
                    &command.resource_id,
                    &parsed.checkpoint_id,
                    command.generation,
                    matches!(command.operation, AgentOperation::JobSavepointCommit),
                    &parsed.manifest_nodes,
                    &parsed.planned_task_ids,
                )
                .await?;
            Ok(Some(manifest_uri))
        }
        AgentOperation::Unknown(operation) => Err(format!("unknown Job operation {operation}")),
        // Non-Job operations never reach here (execute_command routes only
        // the Job plane to this function); refusing keeps the boundary
        // explicit instead of panicking on a routing mistake.
        AgentOperation::Start
        | AgentOperation::Stop
        | AgentOperation::Restart
        | AgentOperation::ValidateConfiguration
        | AgentOperation::DiffConfiguration
        | AgentOperation::ApplyConfiguration
        | AgentOperation::RollbackConfiguration
        | AgentOperation::Reconcile => Err(format!(
            "operation {} is not a Job operation",
            command.operation
        )),
    }
}

/// `job_start` / `job_restart` payload: plan, per-node task assignments,
/// recovery facts, and optional split-placement routing. Parsed once at
/// dispatch; the handler consumes typed fields only. Error strings are the
/// historical wire-facing messages — agents and tests assert them.
struct JobStartCommandPayload {
    plan: JobPlan,
    assignments: Vec<TaskAttempt>,
    recovery_id: Option<String>,
    recovery_savepoint: bool,
    split: SplitPlacementPayload,
}

impl JobStartCommandPayload {
    fn from_payload(payload: &serde_json::Value) -> Result<Self, String> {
        let plan = serde_json::from_value::<JobPlan>(
            payload
                .get("plan")
                .cloned()
                .ok_or_else(|| "missing Job plan payload".to_string())?,
        )
        .map_err(|error| error.to_string())?;
        let assignments = serde_json::from_value::<Vec<TaskAttempt>>(
            payload
                .get("assignments")
                .cloned()
                .ok_or_else(|| "missing Job task assignments".to_string())?,
        )
        .map_err(|error| error.to_string())?;
        let (recovery_id, recovery_savepoint, recovery_required) = parse_recovery_payload(payload)?;
        let split = SplitPlacementPayload {
            task_nodes: payload.get("task_nodes").and_then(|nodes| {
                serde_json::from_value::<BTreeMap<String, String>>(nodes.clone()).ok()
            }),
            node_data_ports: payload
                .get("node_data_ports")
                .and_then(|ports| {
                    serde_json::from_value::<BTreeMap<String, String>>(ports.clone()).ok()
                })
                .unwrap_or_default(),
            recovery_required,
        };
        Ok(Self {
            plan,
            assignments,
            recovery_id,
            recovery_savepoint,
            split,
        })
    }
}

/// `job_checkpoint` / `job_savepoint` payload.
struct CheckpointCommandPayload {
    checkpoint_id: String,
}

impl CheckpointCommandPayload {
    fn from_payload(payload: &serde_json::Value) -> Result<Self, String> {
        let checkpoint_id = payload
            .get("checkpoint_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "missing checkpoint_id".to_string())?
            .to_owned();
        Ok(Self { checkpoint_id })
    }
}

/// `job_checkpoint_commit` / `job_savepoint_commit` aggregation payload.
struct CheckpointCommitCommandPayload {
    checkpoint_id: String,
    manifest_nodes: Vec<String>,
    planned_task_ids: Vec<String>,
}

impl CheckpointCommitCommandPayload {
    fn from_payload(payload: &serde_json::Value) -> Result<Self, String> {
        let checkpoint_id = payload
            .get("checkpoint_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "missing checkpoint_id".to_string())?
            .to_owned();
        let manifest_nodes = serde_json::from_value::<Vec<String>>(
            payload
                .get("manifest_nodes")
                .cloned()
                .ok_or_else(|| "missing checkpoint manifest nodes".to_string())?,
        )
        .map_err(|error| error.to_string())?;
        let planned_task_ids = serde_json::from_value::<Vec<String>>(
            payload
                .get("planned_task_ids")
                .cloned()
                .ok_or_else(|| "missing planned checkpoint task ids".to_string())?,
        )
        .map_err(|error| error.to_string())?;
        Ok(Self {
            checkpoint_id,
            manifest_nodes,
            planned_task_ids,
        })
    }
}

async fn deliver_result(
    client: &Client,
    config: &NodeAgentConfig,
    auth: &AgentAuth,
    result: CommandResult,
) -> Result<CommandResult, Box<dyn std::error::Error + Send + Sync>> {
    if let Err(error) = send_result(client, config, auth, result.clone()).await {
        // The terminal result exists and MUST survive the session. Returning
        // Ok lets the completed-command cache remember it, so after the (now
        // certainly failing) session re-registers, the Hub's redelivery of
        // the leased command replays the cached terminal result exactly once.
        // Propagating the error would drop the result: the Hub operation
        // would expire, retry, and lose again until its retry budget wedged.
        warn!(
            command_id = %result.command_id,
            %error,
            "terminal result delivery failed; cached for replay after re-registration"
        );
    }
    Ok(result)
}

pub(super) async fn send_result(
    client: &Client,
    config: &NodeAgentConfig,
    auth: &AgentAuth,
    result: CommandResult,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let query = agent_auth_query(&auth.node_id);
    bearer_auth(
        client.post(format!(
            "{}{}/agent/commands/{}/result?{}",
            config.hub_url, config.api_prefix, result.command_id, query
        )),
        &auth.session_token,
    )
    .json(&result)
    .send()
    .await?
    .error_for_status()?;
    Ok(())
}

/// POST an `acknowledged`/`running` report and return whether the Hub still
/// holds the operation as NON-terminal. A 200 that carries a terminal record
/// means the Hub already settled the command (its lease sweep timed it out):
/// that is not a renewal and must not extend the local deadline. Transport
/// errors stay errors so the session still tears down; an unrecognized body
/// is treated as a renewal for older Hubs.
async fn send_progress_report(
    client: &Client,
    config: &NodeAgentConfig,
    auth: &AgentAuth,
    result: CommandResult,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let query = agent_auth_query(&auth.node_id);
    let response = bearer_auth(
        client.post(format!(
            "{}{}/agent/commands/{}/result?{}",
            config.hub_url, config.api_prefix, result.command_id, query
        )),
        &auth.session_token,
    )
    .json(&result)
    .send()
    .await?
    .error_for_status()?;
    let body: serde_json::Value = response.json().await.unwrap_or_default();
    let terminal = body
        .get("state")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|state| {
            matches!(
                state,
                "succeeded"
                    | "failed"
                    | "timed_out"
                    | "node_unavailable"
                    | "cancelled"
                    | "superseded"
            )
        });
    Ok(!terminal)
}
pub(super) async fn post_json<T: Serialize>(
    client: &Client,
    url: String,
    session_token: &str,
    body: &T,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The session credential rides the Bearer header as well as the body:
    // the Hub rate limiter keys agent routes on the presented credential, so
    // body-only requests would all share one anonymous bucket. Older Hubs
    // ignore the header and keep authenticating from the body.
    bearer_auth(client.post(url), session_token)
        .json(body)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}
/// Build the agent command query string.
///
/// The session credential travels only in the `Authorization: Bearer` header:
/// a query string leaks into reverse-proxy and access logs, and an Agent from
/// this release therefore requires a Hub from the same release or later.
pub(super) fn agent_auth_query(node_id: &str) -> String {
    url::form_urlencoded::Serializer::new(String::new())
        .append_pair("node_id", node_id)
        .finish()
}

/// Attach the session credential as a Bearer header: tokens in URL query
/// strings leak into reverse-proxy and access logs.
pub(super) fn bearer_auth(
    builder: reqwest::RequestBuilder,
    session_token: &str,
) -> reqwest::RequestBuilder {
    builder.header(
        reqwest::header::AUTHORIZATION,
        format!("Bearer {session_token}"),
    )
}

pub(super) fn command_expired(expires_at_ms: u64, now: u64) -> bool {
    expires_at_ms <= now
}

pub(super) fn command_is_stale(command_generation: u64, latest_generation: Option<u64>) -> bool {
    latest_generation.is_some_and(|latest| command_generation < latest)
}
