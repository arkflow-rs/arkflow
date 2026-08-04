//! Compute-node Agent client for the Hub pull protocol.

use crate::hub::{
    AgentAuth, AgentCommand, CommandResult, HeartbeatRequest, HubOperationState, NodeReport,
    RegisterRequest, RegisterResponse,
};
use arkflow_core::configuration::redacted_config;
use arkflow_core::control::OperationState;
use arkflow_core::control_plane::ControlPlane;
use reqwest::Client;
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct NodeAgentConfig {
    pub hub_url: String,
    pub api_prefix: String,
    pub node_id: String,
    pub node_token: String,
    pub boot_id: String,
    pub heartbeat_interval: Duration,
    pub report_interval: Duration,
    pub poll_interval: Duration,
}

impl NodeAgentConfig {
    pub fn from_engine(config: &arkflow_core::config::EngineConfig) -> Option<Self> {
        let hub_url = config.health_check.hub_url.clone()?;
        let node_id = config
            .health_check
            .node_id
            .clone()
            .or_else(|| std::env::var("ARKFLOW_NODE_ID").ok())?;
        let node_token = config
            .health_check
            .node_token
            .clone()
            .or_else(|| std::env::var("ARKFLOW_NODE_TOKEN").ok())
            .unwrap_or_default();
        let ttl = config.health_check.agent_lease_ttl_ms.max(3_000);
        Some(Self {
            hub_url: hub_url.trim_end_matches('/').into(),
            api_prefix: config.health_check.api_prefix.trim_end_matches('/').into(),
            node_id,
            node_token,
            boot_id: format!("boot-{}", std::process::id()),
            heartbeat_interval: Duration::from_millis(ttl / 3),
            report_interval: Duration::from_secs(2),
            poll_interval: Duration::from_secs(1),
        })
    }
}

pub async fn run(
    cp: ControlPlane,
    config: NodeAgentConfig,
    cancellation: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::new();
    let mut backoff = Duration::from_millis(250);
    let mut completed_commands = HashMap::<String, CommandResult>::new();
    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        match register(&client, &config).await {
            Ok(session) => {
                info!(node_id = %config.node_id, hub = %config.hub_url, "Compute node registered with control-plane Hub");
                backoff = Duration::from_millis(250);
                if let Err(error) = run_session(
                    &client,
                    &cp,
                    &config,
                    session,
                    cancellation.clone(),
                    &mut completed_commands,
                )
                .await
                {
                    warn!(node_id = %config.node_id, error = %error, "Hub Agent session ended; reconnecting");
                }
            }
            Err(error) => {
                warn!(node_id = %config.node_id, error = %error, "Hub Agent registration failed")
            }
        }
        tokio::select! { _ = cancellation.cancelled() => return Ok(()), _ = tokio::time::sleep(backoff) => {} }
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
}

async fn register(
    client: &Client,
    config: &NodeAgentConfig,
) -> Result<RegisterResponse, reqwest::Error> {
    client
        .post(format!(
            "{}{}{}",
            config.hub_url, config.api_prefix, "/agent/register"
        ))
        .json(&RegisterRequest {
            node_id: config.node_id.clone(),
            node_token: config.node_token.clone(),
            protocol_version: "v1".into(),
            capabilities: vec![
                "stream_lifecycle".into(),
                "configuration".into(),
                "metrics".into(),
            ],
        })
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
}

async fn run_session(
    client: &Client,
    cp: &ControlPlane,
    config: &NodeAgentConfig,
    session: RegisterResponse,
    cancellation: CancellationToken,
    completed_commands: &mut HashMap<String, CommandResult>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let auth = AgentAuth {
        node_id: config.node_id.clone(),
        session_token: session.session_token,
    };
    let mut heartbeat = tokio::time::interval(config.heartbeat_interval);
    let mut report_tick = tokio::time::interval(config.report_interval);
    let mut poll = tokio::time::interval(config.poll_interval);
    let mut report_seq = 0_u64;
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => { let _ = post_json(client, format!("{}{}{}", config.hub_url, config.api_prefix, "/agent/heartbeat"), &HeartbeatRequest { auth: auth.clone(), state: "draining".into(), protocol_version: Some("v1".into()), software_version: Some(env!("CARGO_PKG_VERSION").into()), capabilities: vec!["stream_lifecycle".into(), "configuration".into(), "metrics".into()], rollout_id: None }).await; return Ok(()) },
            _ = heartbeat.tick() => { post_json(client, format!("{}{}{}", config.hub_url, config.api_prefix, "/agent/heartbeat"), &HeartbeatRequest { auth: auth.clone(), state: if cp.health().is_running() { "online".into() } else { "starting".into() }, protocol_version: Some("v1".into()), software_version: Some(env!("CARGO_PKG_VERSION").into()), capabilities: vec!["stream_lifecycle".into(), "configuration".into(), "metrics".into()], rollout_id: None }).await?; }
            _ = report_tick.tick() => { report_seq = report_seq.saturating_add(1); post_json(client, format!("{}{}{}", config.hub_url, config.api_prefix, "/agent/report"), &report(cp, &auth, &config.boot_id, report_seq).await).await?; }
            _ = poll.tick() => { let query = url::form_urlencoded::Serializer::new(String::new()).append_pair("node_id", &auth.node_id).append_pair("session_token", &auth.session_token).finish(); let commands: Vec<AgentCommand> = client.get(format!("{}{}{}?{}", config.hub_url, config.api_prefix, "/agent/commands", query)).send().await?.error_for_status()?.json().await?; for command in commands { if let Some(result) = replay_cached_command(completed_commands, &command.id) { send_result(client, config, &auth, result).await?; continue; } let result = execute_command(client, cp, config, &auth, &command).await?; remember_completed_command(completed_commands, command.id, result); } }
        }
    }
}

async fn report(cp: &ControlPlane, auth: &AgentAuth, boot_id: &str, report_seq: u64) -> NodeReport {
    let streams = cp.runtime_manager().snapshots().await;
    let configuration_version = cp
        .runtime_manager()
        .observed_config_version()
        .await
        .or_else(|| {
            streams
                .iter()
                .find_map(|stream| stream.observed_config_version.clone())
        });
    let mut metrics = std::collections::BTreeMap::new();
    for stream in &streams {
        let values = [
            ("input_batches", stream.metrics.input_batches),
            ("input_messages", stream.metrics.input_messages),
            ("processing_errors", stream.metrics.processing_errors),
            ("output_batches", stream.metrics.output_batches),
            ("output_messages", stream.metrics.output_messages),
            ("input_errors", stream.metrics.input_errors),
            ("input_reconnects", stream.metrics.input_reconnects),
            ("output_errors", stream.metrics.output_errors),
            ("restarts", stream.metrics.restarts),
        ];
        for (name, value) in values {
            *metrics.entry(name.into()).or_insert(0.0) += value as f64;
        }
    }
    metrics.insert("streams_total".into(), streams.len() as f64);
    metrics.insert(
        "streams_running".into(),
        streams
            .iter()
            .filter(|stream| stream.state == arkflow_core::control::StreamState::Running)
            .count() as f64,
    );
    NodeReport {
        auth: auth.clone(),
        version: env!("CARGO_PKG_VERSION").into(),
        state: if cp.health().is_running() {
            "online".into()
        } else {
            "starting".into()
        },
        capabilities: vec![
            "stream_lifecycle".into(),
            "configuration".into(),
            "metrics".into(),
        ],
        streams,
        operations: cp.operations().await,
        events: cp.events().await,
        metrics,
        configuration: redacted_config(&cp.configuration().await).ok(),
        configuration_version,
        boot_id: Some(boot_id.into()),
        report_seq,
    }
}

async fn execute_command(
    client: &Client,
    cp: &ControlPlane,
    config: &NodeAgentConfig,
    auth: &AgentAuth,
    command: &AgentCommand,
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
    };
    if command_expired(command.expires_at_ms, now_ms()) {
        result.state = HubOperationState::TimedOut;
        result.error = Some("Command expired before execution".into());
        result.failure_class = Some("temporary_execution".into());
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
                    },
                )
                .await;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn deliver_result(
    client: &Client,
    config: &NodeAgentConfig,
    auth: &AgentAuth,
    result: CommandResult,
) -> Result<CommandResult, Box<dyn std::error::Error + Send + Sync>> {
    send_result(client, config, auth, result.clone()).await?;
    Ok(result)
}

fn replay_cached_command(
    cache: &HashMap<String, CommandResult>,
    command_id: &str,
) -> Option<CommandResult> {
    cache.get(command_id).cloned()
}

fn remember_completed_command(
    cache: &mut HashMap<String, CommandResult>,
    command_id: String,
    result: CommandResult,
) {
    if cache.len() >= 1024 {
        cache.clear();
    }
    cache.insert(command_id, result);
}

async fn send_result(
    client: &Client,
    config: &NodeAgentConfig,
    auth: &AgentAuth,
    result: CommandResult,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    post_json(
        client,
        format!(
            "{}{}/agent/commands/{}/result?{}",
            config.hub_url,
            config.api_prefix,
            result.command_id,
            url::form_urlencoded::Serializer::new(String::new())
                .append_pair("node_id", &auth.node_id)
                .append_pair("session_token", &auth.session_token)
                .finish()
        ),
        &result,
    )
    .await
}
async fn post_json<T: Serialize>(
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
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn command_expired(expires_at_ms: u64, now: u64) -> bool {
    expires_at_ms <= now
}

fn command_is_stale(command_generation: u64, latest_generation: Option<u64>) -> bool {
    latest_generation.is_some_and(|latest| command_generation < latest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::config::{EngineConfig, HealthCheckConfig, LoggingConfig};

    #[test]
    fn agent_mode_requires_hub_and_stable_identity() {
        let health = HealthCheckConfig {
            hub_url: Some("http://hub".into()),
            node_id: Some("node-a".into()),
            ..Default::default()
        };
        let config = EngineConfig {
            streams: vec![],
            logging: LoggingConfig::default(),
            health_check: health,
        };
        let agent = NodeAgentConfig::from_engine(&config).unwrap();
        assert_eq!(agent.node_id, "node-a");
        assert_eq!(agent.api_prefix, "/api/v1");
    }

    #[test]
    fn expired_commands_are_rejected_by_time_boundary() {
        assert!(command_expired(10, 10));
        assert!(!command_expired(11, 10));
    }

    #[test]
    fn older_command_generation_is_stale() {
        assert!(command_is_stale(41, Some(42)));
        assert!(!command_is_stale(42, Some(42)));
        assert!(!command_is_stale(42, None));
    }

    #[test]
    fn duplicate_command_replays_the_terminal_result() {
        let mut cache = HashMap::new();
        let result = CommandResult {
            command_id: "cmd-1".into(),
            operation_id: "op-1".into(),
            state: HubOperationState::Succeeded,
            progress: 100,
            error: None,
            correlation_id: None,
            generation: 4,
            observed_generation: Some(4),
            action_id: Some("restart-1".into()),
            failure_class: None,
            config_version_id: None,
            rollout_id: None,
        };
        assert!(replay_cached_command(&cache, "cmd-1").is_none());
        remember_completed_command(&mut cache, "cmd-1".into(), result.clone());
        let replay = replay_cached_command(&cache, "cmd-1").unwrap();
        assert_eq!(replay.command_id, result.command_id);
        assert_eq!(replay.state, result.state);
        assert_eq!(replay.action_id, result.action_id);
    }
}
