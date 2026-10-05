//! Hub command execution: the command dispatch loop and the Job operation
//! handlers. The string command protocol is unchanged.
use super::checkpoint::parse_recovery_payload;
use super::config::NodeAgentConfig;
use super::kernel::{JobRuntime, SplitPlacementPayload};
use super::now_ms;
use crate::hub::{AgentAuth, AgentCommand, CommandResult, HubOperationState};
use arkflow_core::control::OperationState;
use arkflow_core::control_plane::ControlPlane;
use arkflow_core::job::{JobPlan, TaskAttempt};
use reqwest::Client;
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::warn;

pub(super) async fn execute_command(
    client: &Client,
    cp: &ControlPlane,
    config: &NodeAgentConfig,
    auth: &AgentAuth,
    command: &AgentCommand,
    job_runtime: &JobRuntime,
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
    if command.operation.starts_with("job_") {
        let latest_generation = job_runtime.generation(&command.resource_id).await;
        if command_is_stale(command.generation, latest_generation) {
            result.state = HubOperationState::Superseded;
            result.error = Some("Job command generation is stale".into());
            result.observed_generation = latest_generation;
            result.failure_class = Some("stale_generation".into());
            return deliver_result(client, config, auth, result).await;
        }
        if matches!(
            command.operation.as_str(),
            "job_checkpoint" | "job_savepoint" | "job_checkpoint_commit" | "job_savepoint_commit"
        ) {
            result.observed_checkpoint_id = command
                .payload
                .as_ref()
                .and_then(|payload| payload.get("checkpoint_id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
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
    send_result(client, config, auth, result).await?;
    if matches!(
        command.operation.as_str(),
        "validate_configuration" | "diff_configuration"
    ) {
        // Read-only reports dispatched by the Hub for console clients.
        // Execution success is distinct from the report's own verdict: an
        // invalid candidate still validates successfully, so the report
        // rides the result payload instead of the error channel.
        let outcome: Result<serde_json::Value, String> = if command.operation
            == "validate_configuration"
        {
            command
                .payload
                .clone()
                .ok_or_else(|| "missing configuration payload".to_string())
                .and_then(|payload| {
                    serde_json::from_value::<arkflow_core::configuration::ConfigCandidate>(payload)
                        .map_err(|error| error.to_string())
                })
                .map(|candidate| {
                    serde_json::to_value(cp.validate_configuration(&candidate)).unwrap_or_default()
                })
        } else {
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
    if matches!(
        command.operation.as_str(),
        "apply_configuration" | "rollback_configuration"
    ) {
        let outcome: Result<(), String> = if command.operation == "apply_configuration" {
            let candidate = command
                .payload
                .clone()
                .ok_or_else(|| "missing configuration payload".to_string())
                .and_then(|payload| {
                    serde_json::from_value::<arkflow_core::configuration::ConfigCandidate>(payload)
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
    let operation = match cp
        .lifecycle(
            &command.resource_id,
            &command.operation,
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
    send_result(
        client,
        config,
        auth,
        CommandResult {
            command_id: command.id.clone(),
            operation_id: operation.id.clone(),
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
    .await?;
    loop {
        if let Some(current) = cp.operation(&operation.id).await {
            if matches!(
                current.state,
                OperationState::Succeeded
                    | OperationState::Failed
                    | OperationState::Cancelled
                    | OperationState::TimedOut
            ) {
                if let Some(action_id) = command.action_id.clone() {
                    cp.runtime_manager()
                        .set_last_completed_action(&command.resource_id, action_id)
                        .await;
                }
                let state = match current.state {
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
                        operation_id: operation.id,
                        state,
                        progress: 100,
                        error: current.error,
                        correlation_id: command.correlation_id.clone(),
                        generation: command.generation,
                        observed_generation: None,
                        action_id: command.action_id.clone(),
                        failure_class: match state {
                            HubOperationState::TimedOut => Some("temporary_execution".into()),
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
        } else {
            // The local operation record is gone (e.g. evicted from the bounded
            // operation store), so its outcome can no longer be observed. The
            // execution itself keeps running; report an ambiguous temporary
            // failure so the Hub settles the command through its retry path
            // instead of this watcher spinning forever.
            return deliver_result(
                client,
                config,
                auth,
                CommandResult {
                    command_id: command.id.clone(),
                    operation_id: operation.id,
                    state: HubOperationState::Failed,
                    progress: 100,
                    error: Some(format!(
                        "operation record {} is no longer observable on the agent",
                        command.operation_id
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
        if command_expired(command.expires_at_ms, now_ms()) {
            // The command deadline passed without a terminal observation; the
            // Hub has already stopped waiting for this command.
            return deliver_result(
                client,
                config,
                auth,
                CommandResult {
                    command_id: command.id.clone(),
                    operation_id: operation.id,
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
        tokio::time::sleep(Duration::from_millis(100)).await;
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
    match command.operation.as_str() {
        "job_start" | "job_restart" => {
            let payload = command
                .payload
                .as_ref()
                .ok_or_else(|| "missing Job plan payload".to_string())?;
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
            let (recovery_id, recovery_savepoint, recovery_required) =
                parse_recovery_payload(payload)?;
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
            if command.operation == "job_restart" {
                job_runtime
                    .stop(&command.resource_id, command.generation)
                    .await?;
            }
            job_runtime
                .start(
                    plan,
                    assignments,
                    command.generation,
                    recovery_id,
                    recovery_savepoint,
                    &config.node_id,
                    &split,
                )
                .await?;
            Ok(None)
        }
        "job_stop" => {
            job_runtime
                .stop(&command.resource_id, command.generation)
                .await?;
            Ok(None)
        }
        "job_checkpoint" | "job_savepoint" => {
            let payload = command
                .payload
                .as_ref()
                .ok_or_else(|| "missing checkpoint payload".to_string())?;
            let checkpoint_id = payload
                .get("checkpoint_id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "missing checkpoint_id".to_string())?;
            let manifest_uri = job_runtime
                .checkpoint(
                    &command.resource_id,
                    checkpoint_id,
                    command.generation,
                    command.operation == "job_savepoint",
                    &config.node_id,
                )
                .await?;
            Ok(Some(manifest_uri))
        }
        "job_checkpoint_commit" | "job_savepoint_commit" => {
            let payload = command
                .payload
                .as_ref()
                .ok_or_else(|| "missing checkpoint aggregation payload".to_string())?;
            let checkpoint_id = payload
                .get("checkpoint_id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "missing checkpoint_id".to_string())?;
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
            let manifest_uri = job_runtime
                .aggregate_checkpoint(
                    &command.resource_id,
                    checkpoint_id,
                    command.generation,
                    command.operation == "job_savepoint_commit",
                    &manifest_nodes,
                    &planned_task_ids,
                )
                .await?;
            Ok(Some(manifest_uri))
        }
        _ => Err(format!("unknown Job operation {}", command.operation)),
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
pub(super) async fn post_json<T: Serialize>(
    client: &Client,
    url: String,
    body: &T,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    client
        .post(url)
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
