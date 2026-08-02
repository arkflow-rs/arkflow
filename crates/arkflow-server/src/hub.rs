//! Hub-side node registry and command broker.
//!
//! The Hub owns fleet state; compute nodes own execution. This module contains
//! the transport-neutral state machine used by the HTTP handlers and Agent
//! client protocol.

use crate::storage::{
    AttemptRecord, DesiredMutation, IntentRecord, NodeMutation, ObservedMutation, StorageActor,
    StorageError,
};
use arkflow_core::control::{
    ControlEvent, NodeMaintenanceState, OperationRecord, OperationalStatus, ReconciliationHealth,
    StreamStatus,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;
use tokio::sync::RwLock;

const MAX_NODES: usize = 256;
const MAX_COMMANDS_PER_NODE: usize = 128;
const MAX_OPERATIONS: usize = 1024;
const MAX_EVENTS: usize = 2048;

#[derive(Debug, Clone)]
pub struct HubConfig {
    pub operator_token: Option<String>,
    pub node_token: Option<String>,
    pub lease_ttl_ms: u64,
    pub poll_interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub node_id: String,
    pub node_token: String,
    #[serde(default = "default_protocol_version")]
    pub protocol_version: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterResponse {
    pub node_id: String,
    pub session_token: String,
    pub lease_ttl_ms: u64,
    pub poll_interval_ms: u64,
    pub protocol_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentAuth {
    pub node_id: String,
    pub session_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    #[serde(flatten)]
    pub auth: AgentAuth,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeReport {
    #[serde(flatten)]
    pub auth: AgentAuth,
    pub version: String,
    pub state: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub streams: Vec<StreamStatus>,
    #[serde(default)]
    pub operations: Vec<OperationRecord>,
    #[serde(default)]
    pub events: Vec<ControlEvent>,
    #[serde(default)]
    pub metrics: BTreeMap<String, f64>,
    #[serde(default)]
    pub configuration: Option<serde_json::Value>,
    #[serde(default)]
    pub configuration_version: Option<String>,
    #[serde(default)]
    pub boot_id: Option<String>,
    #[serde(default)]
    pub report_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HubNode {
    pub id: String,
    pub version: String,
    pub state: NodeConnectionState,
    pub capabilities: Vec<String>,
    pub last_seen_at_ms: u64,
    pub lease_expires_at_ms: u64,
    pub streams_total: usize,
    pub streams_running: usize,
    pub streams_failed: usize,
    #[serde(default)]
    pub maintenance_state: NodeMaintenanceState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HubEvent {
    pub node_id: String,
    #[serde(flatten)]
    pub event: ControlEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HubNodeMetrics {
    pub node_id: String,
    pub metrics: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeConnectionState {
    Online,
    Stale,
    Offline,
    Draining,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCommand {
    pub id: String,
    pub operation_id: String,
    pub node_id: String,
    pub operation: String,
    pub resource_id: String,
    pub expires_at_ms: u64,
    #[serde(default)]
    pub generation: u64,
    #[serde(default)]
    pub action_id: Option<String>,
    #[serde(default)]
    pub config_version_id: Option<String>,
    #[serde(default)]
    pub attempt_id: Option<String>,
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResult {
    pub command_id: String,
    pub operation_id: String,
    pub state: HubOperationState,
    pub progress: u8,
    pub error: Option<String>,
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub generation: u64,
    #[serde(default)]
    pub observed_generation: Option<u64>,
    #[serde(default)]
    pub action_id: Option<String>,
    #[serde(default)]
    pub failure_class: Option<String>,
    #[serde(default)]
    pub config_version_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HubOperationState {
    Queued,
    Dispatched,
    Acknowledged,
    Running,
    Succeeded,
    Failed,
    TimedOut,
    NodeUnavailable,
    Cancelled,
    Superseded,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HubOperation {
    pub id: String,
    #[serde(default)]
    pub intent_id: Option<String>,
    pub command_id: String,
    pub node_id: String,
    pub operation: String,
    pub resource_id: String,
    #[serde(default)]
    pub generation: u64,
    #[serde(default)]
    pub attempt_id: Option<String>,
    #[serde(default)]
    pub config_version_id: Option<String>,
    pub state: HubOperationState,
    pub progress: u8,
    pub created_at_ms: u64,
    pub dispatched_at_ms: Option<u64>,
    pub acknowledged_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    pub correlation_id: Option<String>,
    pub error: Option<String>,
    #[serde(default)]
    pub failure_class: Option<String>,
    #[serde(default)]
    pub intent_state: Option<String>,
    #[serde(default)]
    pub convergence_state: Option<String>,
    #[serde(default)]
    pub retry_count: u32,
    #[serde(default)]
    pub next_retry_at_ms: Option<u64>,
    #[serde(default)]
    pub superseded_by_intent_id: Option<String>,
    #[serde(default)]
    pub superseded_generation: Option<u64>,
    #[serde(default)]
    pub observed_generation: Option<u64>,
    #[serde(default)]
    pub observed_state: Option<String>,
}

#[derive(Debug, Clone)]
struct NodeRecord {
    resource: HubNode,
    session_token: String,
    boot_id: Option<String>,
    report_seq: u64,
    commands: VecDeque<AgentCommand>,
    streams: Vec<StreamStatus>,
    operations: Vec<OperationRecord>,
    events: Vec<ControlEvent>,
    metrics: BTreeMap<String, f64>,
    configuration: Option<serde_json::Value>,
}

#[derive(Clone)]
pub struct Hub {
    config: Arc<HubConfig>,
    nodes: Arc<RwLock<BTreeMap<String, NodeRecord>>>,
    operations: Arc<RwLock<BTreeMap<String, HubOperation>>>,
    events: Arc<RwLock<VecDeque<HubEvent>>>,
    storage: Option<StorageActor>,
    lifecycle: Arc<RwLock<HubLifecycle>>,
}

#[derive(Debug, Clone, Default)]
struct HubLifecycle {
    recovered: bool,
    runs_total: u64,
    failures_total: u64,
    last_success_at_ms: Option<u64>,
    last_error_at_ms: Option<u64>,
    last_duration_ms: Option<u64>,
    last_failure_class: Option<String>,
}

impl Hub {
    pub fn new(config: HubConfig) -> Self {
        Self {
            config: Arc::new(config),
            nodes: Arc::new(RwLock::new(BTreeMap::new())),
            operations: Arc::new(RwLock::new(BTreeMap::new())),
            events: Arc::new(RwLock::new(VecDeque::new())),
            storage: None,
            lifecycle: Arc::new(RwLock::new(HubLifecycle::default())),
        }
    }

    pub fn with_storage(config: HubConfig, storage: StorageActor) -> Self {
        let mut hub = Self::new(config);
        hub.storage = Some(storage);
        hub
    }

    pub fn has_storage(&self) -> bool {
        self.storage.is_some()
    }

    pub async fn recover_persisted_state(&self) -> Result<(), HubError> {
        if let Some(storage) = self.storage.as_ref() {
            storage
                .recover_reconciliation(now_ms())
                .await
                .map_err(HubError::from)?;
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

    pub async fn expire_attempts(&self) -> Result<usize, HubError> {
        let Some(storage) = self.storage.as_ref() else {
            return Ok(0);
        };
        storage
            .expire_attempts(now_ms())
            .await
            .map_err(HubError::from)
    }

    pub async fn set_desired_state(
        &self,
        mutation: DesiredMutation,
    ) -> Result<IntentRecord, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage.set_desired(mutation).await.map_err(HubError::from)
    }

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

    /// Consume one durable reconciliation wake-up. If the target node is
    /// offline the outbox row remains unprocessed and its claim lease expires
    /// for a later retry.
    pub async fn reconcile_once(&self, worker_id: &str) -> Result<Option<HubOperation>, HubError> {
        let Some(storage) = self.storage.as_ref() else {
            return Ok(None);
        };
        let Some(outbox) = storage
            .claim_outbox(worker_id, now_ms())
            .await
            .map_err(HubError::from)?
        else {
            return Ok(None);
        };
        let Some(stream_id) = outbox.stream_id.clone() else {
            storage
                .mark_outbox_processed(outbox.outbox_id, now_ms())
                .await
                .map_err(HubError::from)?;
            return Ok(None);
        };
        let Some(desired) = storage
            .get_desired(&outbox.node_id, &stream_id)
            .await
            .map_err(HubError::from)?
        else {
            storage
                .mark_outbox_processed(outbox.outbox_id, now_ms())
                .await
                .map_err(HubError::from)?;
            return Ok(None);
        };
        let online = {
            let nodes = self.nodes.read().await;
            nodes.get(&desired.node_id).is_some_and(|node| {
                node.resource.state == NodeConnectionState::Online
                    && node.resource.lease_expires_at_ms > now_ms()
            })
        };
        if !online {
            return Ok(None);
        }
        let Some(attempt) = storage
            .claim_attempt(&outbox.intent_id.clone().unwrap_or_default())
            .await
            .map_err(HubError::from)?
        else {
            return Ok(None);
        };
        let operation = self.enqueue_attempt(attempt).await?;
        storage
            .mark_outbox_processed(outbox.outbox_id, now_ms())
            .await
            .map_err(HubError::from)?;
        Ok(Some(operation))
    }

    pub fn operator_authorized(&self, supplied: Option<&str>) -> bool {
        let Some(expected) = self.config.operator_token.as_deref() else {
            return true;
        };
        supplied.is_some_and(|value| value.as_bytes().ct_eq(expected.as_bytes()).into())
    }

    pub async fn register(&self, request: RegisterRequest) -> Result<RegisterResponse, HubError> {
        if let Some(expected) = self.config.node_token.as_deref() {
            if !bool::from(request.node_token.as_bytes().ct_eq(expected.as_bytes())) {
                return Err(HubError::Unauthorized);
            }
        }
        if request.node_id.trim().is_empty() {
            return Err(HubError::Invalid("node_id must not be empty".into()));
        }
        let now = now_ms();
        let session_token = format!(
            "node-session-{}",
            SESSION_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        let resource = HubNode {
            id: request.node_id.clone(),
            version: "unknown".into(),
            state: NodeConnectionState::Online,
            capabilities: request.capabilities.clone(),
            last_seen_at_ms: now,
            lease_expires_at_ms: now + self.config.lease_ttl_ms,
            streams_total: 0,
            streams_running: 0,
            streams_failed: 0,
            maintenance_state: NodeMaintenanceState::Active,
        };
        let mut nodes = self.nodes.write().await;
        if nodes.len() >= MAX_NODES && !nodes.contains_key(&request.node_id) {
            return Err(HubError::Capacity);
        }
        let old = nodes.remove(&request.node_id);
        nodes.insert(
            request.node_id.clone(),
            NodeRecord {
                resource,
                session_token: session_token.clone(),
                boot_id: None,
                report_seq: 0,
                commands: old
                    .as_ref()
                    .map(|record| record.commands.clone())
                    .unwrap_or_default(),
                streams: old
                    .as_ref()
                    .map(|record| record.streams.clone())
                    .unwrap_or_default(),
                operations: old
                    .as_ref()
                    .map(|record| record.operations.clone())
                    .unwrap_or_default(),
                events: old
                    .as_ref()
                    .map(|record| record.events.clone())
                    .unwrap_or_default(),
                configuration: old.as_ref().and_then(|record| record.configuration.clone()),
                metrics: old.map(|record| record.metrics).unwrap_or_default(),
            },
        );
        drop(nodes);
        if let Some(storage) = self.storage.as_ref() {
            storage
                .upsert_node(NodeMutation {
                    node_id: request.node_id.clone(),
                    version: "unknown".into(),
                    state: "online".into(),
                    capabilities_json: serde_json::to_string(&request.capabilities)
                        .unwrap_or_else(|_| "[]".into()),
                    boot_id: None,
                    report_seq: None,
                    last_seen_at_ms: now,
                    lease_expires_at_ms: now + self.config.lease_ttl_ms,
                    maintenance_state: None,
                    maintenance_updated_at_ms: None,
                })
                .await
                .map_err(HubError::from)?;
            storage
                .wake_node(&request.node_id, now)
                .await
                .map_err(HubError::from)?;
            if let Some(state) = storage
                .get_node_maintenance(&request.node_id)
                .await
                .map_err(HubError::from)?
            {
                let maintenance_state = match state.as_str() {
                    "draining" => NodeMaintenanceState::Draining,
                    "maintenance" => NodeMaintenanceState::Maintenance,
                    _ => NodeMaintenanceState::Active,
                };
                if let Some(node) = self.nodes.write().await.get_mut(&request.node_id) {
                    node.resource.maintenance_state = maintenance_state;
                }
            }
        }
        Ok(RegisterResponse {
            node_id: request.node_id,
            session_token,
            lease_ttl_ms: self.config.lease_ttl_ms,
            poll_interval_ms: self.config.poll_interval_ms,
            protocol_version: default_protocol_version(),
        })
    }

    pub async fn heartbeat(&self, request: HeartbeatRequest) -> Result<(), HubError> {
        let mut nodes = self.nodes.write().await;
        let node = authenticated_node(&mut nodes, &request.auth)?;
        let now = now_ms();
        node.resource.last_seen_at_ms = now;
        node.resource.lease_expires_at_ms = now + self.config.lease_ttl_ms;
        node.resource.state = match request.state.as_str() {
            "draining" => NodeConnectionState::Draining,
            _ => NodeConnectionState::Online,
        };
        Ok(())
    }

    pub async fn report(&self, report: NodeReport) -> Result<(), HubError> {
        let reported_streams = report.streams.clone();
        let reported_configuration = report.configuration.clone();
        let mut nodes = self.nodes.write().await;
        let node = authenticated_node(&mut nodes, &report.auth)?;
        if let Some(boot_id) = report.boot_id.as_deref() {
            if node.boot_id.as_deref() == Some(boot_id) && report.report_seq <= node.report_seq {
                return Ok(());
            }
            node.boot_id = Some(boot_id.to_owned());
            node.report_seq = report.report_seq;
        }
        let now = now_ms();
        node.resource.last_seen_at_ms = now;
        node.resource.lease_expires_at_ms = now + self.config.lease_ttl_ms;
        node.resource.state = if report.state == "draining" {
            NodeConnectionState::Draining
        } else {
            NodeConnectionState::Online
        };
        node.resource.version = report.version;
        node.resource.capabilities = report.capabilities;
        node.resource.streams_total = report.streams.len();
        node.resource.streams_running = report
            .streams
            .iter()
            .filter(|stream| stream.state == arkflow_core::control::StreamState::Running)
            .count();
        node.resource.streams_failed = report
            .streams
            .iter()
            .filter(|stream| stream.state == arkflow_core::control::StreamState::Failed)
            .count();
        node.streams = report.streams;
        node.operations = report.operations;
        node.events = report.events.clone();
        node.metrics = report.metrics;
        node.configuration = report.configuration;
        let persisted_version = node.resource.version.clone();
        let persisted_state = format!("{:?}", node.resource.state).to_lowercase();
        let persisted_capabilities =
            serde_json::to_string(&node.resource.capabilities).unwrap_or_else(|_| "[]".into());
        let persisted_boot_id = node.boot_id.clone();
        let persisted_report_seq = Some(node.report_seq);
        let persisted_lease = node.resource.lease_expires_at_ms;
        drop(nodes);
        if let Some(storage) = self.storage.as_ref() {
            storage
                .upsert_node(NodeMutation {
                    node_id: report.auth.node_id.clone(),
                    version: persisted_version,
                    state: persisted_state,
                    capabilities_json: persisted_capabilities,
                    boot_id: persisted_boot_id,
                    report_seq: persisted_report_seq,
                    last_seen_at_ms: now,
                    lease_expires_at_ms: persisted_lease,
                    maintenance_state: None,
                    maintenance_updated_at_ms: None,
                })
                .await
                .map_err(HubError::from)?;
        }
        let mut events = self.events.write().await;
        for event in report.events {
            if events.len() >= MAX_EVENTS {
                events.pop_front();
            }
            events.push_back(HubEvent {
                node_id: report.auth.node_id.clone(),
                event,
            });
        }
        if let Some(storage) = self.storage.as_ref() {
            for stream in &reported_streams {
                let observed_state = serde_json::to_value(stream.state)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .unwrap_or_else(|| "unknown".into());
                let last_error_code = stream.last_error.as_ref().map(|error| error.stage.clone());
                let last_error_message = stream
                    .last_error
                    .as_ref()
                    .map(|error| error.message.clone());
                storage
                    .record_observed(ObservedMutation {
                        node_id: report.auth.node_id.clone(),
                        stream_id: stream.id.clone(),
                        boot_id: report.boot_id.clone(),
                        report_seq: report.report_seq,
                        observed_generation: stream.observed_generation,
                        observed_state,
                        config_version_id: stream.observed_config_version.clone(),
                        action_id: stream.last_completed_action_id.clone(),
                        snapshot_json: serde_json::to_string(&stream)
                            .unwrap_or_else(|_| "{}".into()),
                        last_error_code,
                        last_error_message,
                    })
                    .await
                    .map_err(HubError::from)?;
            }
            if let Some(config_target) = storage
                .get_desired(&report.auth.node_id, "__configuration__")
                .await
                .map_err(HubError::from)?
            {
                let observed_version = report.configuration_version.clone().or_else(|| {
                    reported_streams
                        .iter()
                        .find_map(|stream| stream.observed_config_version.clone())
                });
                if observed_version.is_some() {
                    storage
                        .record_observed(ObservedMutation {
                            node_id: report.auth.node_id.clone(),
                            stream_id: "__configuration__".into(),
                            boot_id: report.boot_id.clone(),
                            report_seq: report.report_seq,
                            observed_generation: Some(config_target.generation),
                            observed_state: "configured".into(),
                            config_version_id: observed_version,
                            action_id: None,
                            snapshot_json: serde_json::to_string(&reported_configuration)
                                .unwrap_or_else(|_| "null".into()),
                            last_error_code: None,
                            last_error_message: None,
                        })
                        .await
                        .map_err(HubError::from)?;
                }
            }
        }
        Ok(())
    }

    pub async fn commands(&self, auth: AgentAuth) -> Result<Vec<AgentCommand>, HubError> {
        let mut nodes = self.nodes.write().await;
        let node = authenticated_node(&mut nodes, &auth)?;
        let now = now_ms();
        let mut commands = Vec::new();
        while let Some(command) = node.commands.pop_front() {
            if command.expires_at_ms > now {
                commands.push(command);
            }
        }
        drop(nodes);
        if !commands.is_empty() {
            let mut operations = self.operations.write().await;
            for command in &commands {
                if let Some(operation) = operations.get_mut(&command.operation_id) {
                    operation.state = HubOperationState::Dispatched;
                    operation.dispatched_at_ms = Some(now);
                }
            }
        }
        if let Some(storage) = self.storage.as_ref() {
            for command in &commands {
                if let Some(attempt_id) = command.attempt_id.as_deref() {
                    storage
                        .mark_attempt_dispatched(attempt_id, command.expires_at_ms)
                        .await
                        .map_err(HubError::from)?;
                }
            }
        }
        Ok(commands)
    }

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
            }
        }
        Ok(operation)
    }

    async fn enqueue_with_metadata(
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
        let node = nodes.get_mut(&node_id).ok_or(HubError::NodeUnavailable)?;
        if node.resource.state != NodeConnectionState::Online
            || node.resource.lease_expires_at_ms <= now
            || node.resource.maintenance_state != NodeMaintenanceState::Active
        {
            return Err(HubError::NodeUnavailable);
        }
        let mut operations = self.operations.write().await;
        if let Some(operation_id) = operation_id_override.as_deref() {
            if let Some(existing) = operations.get(operation_id) {
                return Ok(existing.clone());
            }
        } else if let Some(existing) = operations.values().find(|item| {
            item.node_id == node_id
                && item.resource_id == resource_id
                && item.operation == operation
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
        if node.commands.len() >= MAX_COMMANDS_PER_NODE {
            return Err(HubError::Capacity);
        }
        let operation_record = HubOperation {
            id,
            intent_id,
            command_id: command_id.clone(),
            node_id: node_id.clone(),
            operation: operation.clone(),
            resource_id: resource_id.clone(),
            generation,
            attempt_id: attempt_id.clone(),
            config_version_id: config_version_id.clone(),
            state: HubOperationState::Queued,
            progress: 0,
            created_at_ms: now,
            dispatched_at_ms: None,
            acknowledged_at_ms: None,
            finished_at_ms: None,
            correlation_id: correlation_id.clone(),
            error: None,
            failure_class: None,
            intent_state: None,
            convergence_state: None,
            retry_count: 0,
            next_retry_at_ms: None,
            superseded_by_intent_id: None,
            superseded_generation: None,
            observed_generation: None,
            observed_state: None,
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
            correlation_id,
            payload,
        };
        node.commands.push_back(command);
        if operations.len() >= MAX_OPERATIONS {
            if let Some(oldest) = operations.keys().next().cloned() {
                operations.remove(&oldest);
            }
        }
        operations.insert(operation_record.id.clone(), operation_record.clone());
        Ok(operation_record)
    }

    pub async fn command_result(
        &self,
        auth: AgentAuth,
        result: CommandResult,
    ) -> Result<HubOperation, HubError> {
        let nodes = self.nodes.read().await;
        let node = nodes.get(&auth.node_id).ok_or(HubError::Unauthorized)?;
        if !bool::from(
            auth.session_token
                .as_bytes()
                .ct_eq(node.session_token.as_bytes()),
        ) {
            return Err(HubError::Unauthorized);
        }
        let mut operations = self.operations.write().await;
        let operation = operations
            .values_mut()
            .find(|item| item.command_id == result.command_id)
            .ok_or(HubError::NotFound)?;
        if operation.node_id != auth.node_id {
            return Err(HubError::Unauthorized);
        }
        operation.state = result.state;
        operation.progress = result.progress;
        operation.error = result.error;
        operation.failure_class = result.failure_class.clone();
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
            operation.acknowledged_at_ms = Some(now_ms());
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
        Ok(updated)
    }

    pub async fn nodes(&self) -> Vec<HubNode> {
        self.nodes
            .read()
            .await
            .values()
            .map(|node| node.resource.clone())
            .collect()
    }

    pub async fn set_node_maintenance(
        &self,
        node_id: &str,
        state: NodeMaintenanceState,
        actor: Option<String>,
        correlation_id: Option<String>,
    ) -> Result<HubNode, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        let state_name = match state {
            NodeMaintenanceState::Active => "active",
            NodeMaintenanceState::Draining => "draining",
            NodeMaintenanceState::Maintenance => "maintenance",
        };
        if !storage
            .set_node_maintenance(
                crate::storage::NodeMaintenanceMutation {
                    node_id: node_id.into(),
                    state: state_name.into(),
                    actor,
                    correlation_id,
                },
                now_ms(),
            )
            .await
            .map_err(HubError::from)?
        {
            return Err(HubError::NotFound);
        }
        let mut nodes = self.nodes.write().await;
        let node = nodes.get_mut(node_id).ok_or(HubError::NotFound)?;
        node.resource.maintenance_state = state;
        Ok(node.resource.clone())
    }
    pub async fn streams(&self, node_id: Option<&str>) -> Vec<(String, StreamStatus)> {
        self.nodes
            .read()
            .await
            .values()
            .filter(|node| node_id.is_none_or(|id| node.resource.id == id))
            .flat_map(|node| {
                node.streams
                    .iter()
                    .cloned()
                    .map(|stream| (node.resource.id.clone(), stream))
            })
            .collect()
    }

    pub async fn stream_resource(
        &self,
        node_id: &str,
        stream_id: &str,
    ) -> Result<Option<serde_json::Value>, HubError> {
        let observed = self
            .nodes
            .read()
            .await
            .get(node_id)
            .and_then(|node| node.streams.iter().find(|stream| stream.id == stream_id))
            .cloned();
        let desired = if let Some(storage) = self.storage.as_ref() {
            storage
                .get_desired(node_id, stream_id)
                .await
                .map_err(HubError::from)?
        } else {
            None
        };
        if observed.is_none() && desired.is_none() {
            return Ok(None);
        }
        let mut resource = observed
            .map(|stream| serde_json::to_value(stream).unwrap_or_default())
            .unwrap_or_else(|| {
                serde_json::json!({
                    "id": stream_id,
                    "state": "unknown",
                    "convergence": "unknown"
                })
            });
        if let Some(object) = resource.as_object_mut() {
            object.insert("node_id".into(), serde_json::Value::String(node_id.into()));
            if let Some(desired) = desired {
                object.insert(
                    "desired".into(),
                    serde_json::json!({
                        "state": desired.desired_state,
                        "generation": desired.generation,
                        "config_version": desired.config_version_id,
                        "action_id": desired.action_id
                    }),
                );
                object.insert(
                    "generation".into(),
                    serde_json::Value::Number(desired.generation.into()),
                );
            }
        }
        Ok(Some(resource))
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
        let intent = storage.get_intent(id.to_owned()).await.ok()??;
        Some(operation_from_intent(intent))
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
        }
        Some(cancelled)
    }

    pub async fn events(&self, node_id: Option<&str>) -> Vec<HubEvent> {
        let mut events = self
            .events
            .read()
            .await
            .iter()
            .filter(|event| node_id.is_none_or(|id| event.node_id == id))
            .cloned()
            .collect::<Vec<_>>();
        if let Some(storage) = self.storage.as_ref() {
            if let Ok(stored) = storage.list_events(node_id.map(str::to_owned)).await {
                events.extend(stored.into_iter().filter_map(|event| {
                    let node_id = event.node_id?;
                    Some(HubEvent {
                        node_id,
                        event: ControlEvent {
                            occurred_at_ms: event.occurred_at_ms,
                            event_type: event.event_type,
                            stream_id: event.stream_id,
                            outcome: event.outcome,
                            message: event.message,
                            operation_id: event.intent_id.or(event.attempt_id),
                            correlation_id: event.correlation_id,
                            actor: event.actor,
                        },
                    })
                }));
            }
        }
        events.sort_by_key(|event| std::cmp::Reverse(event.event.occurred_at_ms));
        events.truncate(MAX_EVENTS);
        events
    }

    pub async fn metrics(&self, node_id: Option<&str>) -> BTreeMap<String, f64> {
        let nodes = self.nodes.read().await;
        let mut aggregate = BTreeMap::new();
        for node in nodes
            .values()
            .filter(|node| node_id.is_none_or(|id| node.resource.id == id))
        {
            for (key, value) in &node.metrics {
                *aggregate.entry(key.clone()).or_insert(0.0) += value;
            }
        }
        aggregate
    }

    pub async fn metrics_by_node(&self, node_id: Option<&str>) -> Vec<HubNodeMetrics> {
        self.nodes
            .read()
            .await
            .values()
            .filter(|node| node_id.is_none_or(|id| node.resource.id == id))
            .map(|node| HubNodeMetrics {
                node_id: node.resource.id.clone(),
                metrics: node.metrics.clone(),
            })
            .collect()
    }

    pub async fn configuration(&self, node_id: &str) -> Option<serde_json::Value> {
        self.nodes
            .read()
            .await
            .get(node_id)
            .and_then(|node| node.configuration.clone())
    }

    pub async fn mark_stale(&self) {
        let now = now_ms();
        let mut nodes = self.nodes.write().await;
        let stale_ids: Vec<String> = nodes
            .values_mut()
            .filter_map(|node| {
                if node.resource.state == NodeConnectionState::Online
                    && node.resource.lease_expires_at_ms <= now
                {
                    node.resource.state = NodeConnectionState::Stale;
                    Some(node.resource.id.clone())
                } else {
                    None
                }
            })
            .collect();
        if stale_ids.is_empty() {
            return;
        }
        let mut operations = self.operations.write().await;
        for operation in operations.values_mut() {
            if stale_ids.iter().any(|id| id == &operation.node_id)
                && matches!(
                    operation.state,
                    HubOperationState::Queued
                        | HubOperationState::Dispatched
                        | HubOperationState::Acknowledged
                        | HubOperationState::Running
                )
            {
                operation.state = HubOperationState::NodeUnavailable;
                operation.finished_at_ms = Some(now);
                operation.error = Some("Node lease expired".into());
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HubError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("node unavailable")]
    NodeUnavailable,
    #[error("resource not found")]
    NotFound,
    #[error("hub capacity exceeded")]
    Capacity,
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("durable storage is unavailable")]
    StorageUnavailable,
    #[error("desired state generation conflict: expected {expected}, current {current}")]
    GenerationConflict { expected: u64, current: u64 },
    #[error("idempotency key was already used for a different mutation")]
    IdempotencyKeyReused,
    #[error("storage error: {0}")]
    Storage(String),
}

impl HubError {
    pub fn failure_class(&self) -> &'static str {
        match self {
            Self::Unauthorized => "authorization",
            Self::NodeUnavailable => "node_unavailable",
            Self::StorageUnavailable | Self::Storage(_) => "repository",
            Self::NotFound => "not_found",
            _ => "invalid",
        }
    }
}

impl From<StorageError> for HubError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::GenerationConflict { expected, current } => {
                Self::GenerationConflict { expected, current }
            }
            StorageError::IdempotencyKeyReused => Self::IdempotencyKeyReused,
            StorageError::ActorClosed => Self::StorageUnavailable,
            other => Self::Storage(other.to_string()),
        }
    }
}

fn authenticated_node<'a>(
    nodes: &'a mut BTreeMap<String, NodeRecord>,
    auth: &AgentAuth,
) -> Result<&'a mut NodeRecord, HubError> {
    let node = nodes.get_mut(&auth.node_id).ok_or(HubError::Unauthorized)?;
    if !bool::from(
        auth.session_token
            .as_bytes()
            .ct_eq(node.session_token.as_bytes()),
    ) {
        return Err(HubError::Unauthorized);
    }
    Ok(node)
}

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
        generation: intent.generation,
        attempt_id: None,
        config_version_id: intent.config_version_id,
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
    }
}

fn default_protocol_version() -> String {
    "v1".into()
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

pub fn now_ms_for_metrics() -> u64 {
    now_ms()
}
static SESSION_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static HUB_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::control::{ConvergenceState, StreamMetricsSnapshot, StreamState};

    fn config() -> HubConfig {
        HubConfig {
            operator_token: Some("operator".into()),
            node_token: Some("node-secret".into()),
            lease_ttl_ms: 1000,
            poll_interval_ms: 10,
        }
    }

    #[test]
    fn agent_wire_contract_round_trips_reconciliation_fields() {
        let command = AgentCommand {
            id: "cmd-1".into(),
            operation_id: "intent-1".into(),
            node_id: "node-a".into(),
            operation: "restart".into(),
            resource_id: "orders".into(),
            expires_at_ms: 123,
            generation: 7,
            action_id: Some("restart-7".into()),
            config_version_id: Some("cfg-7".into()),
            attempt_id: Some("attempt-7".into()),
            correlation_id: Some("corr-7".into()),
            payload: None,
        };
        let encoded = serde_json::to_vec(&command).unwrap();
        let decoded: AgentCommand = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded.generation, 7);
        assert_eq!(decoded.action_id.as_deref(), Some("restart-7"));
        assert_eq!(decoded.config_version_id.as_deref(), Some("cfg-7"));
        assert_eq!(decoded.attempt_id.as_deref(), Some("attempt-7"));
        assert_eq!(decoded.expires_at_ms, 123);

        let report = NodeReport {
            auth: AgentAuth {
                node_id: "node-a".into(),
                session_token: "session".into(),
            },
            version: "test".into(),
            state: "online".into(),
            capabilities: vec![],
            streams: vec![],
            operations: vec![],
            events: vec![],
            metrics: BTreeMap::new(),
            configuration: None,
            configuration_version: Some("cfg-7".into()),
            boot_id: Some("boot-7".into()),
            report_seq: 9,
        };
        let decoded: NodeReport =
            serde_json::from_value(serde_json::to_value(report).unwrap()).unwrap();
        assert_eq!(decoded.boot_id.as_deref(), Some("boot-7"));
        assert_eq!(decoded.report_seq, 9);
        assert_eq!(decoded.configuration_version.as_deref(), Some("cfg-7"));
    }

    fn stopped_report(stream_id: &str, generation: Option<u64>) -> StreamStatus {
        StreamStatus {
            id: stream_id.into(),
            state: StreamState::Stopped,
            desired_state: None,
            desired_generation: 0,
            desired_config_version: None,
            observed_generation: generation,
            observed_config_version: None,
            convergence: ConvergenceState::Unknown,
            intent_id: None,
            attempt_id: None,
            last_completed_action_id: None,
            retry_count: 0,
            next_retry_at_ms: None,
            transition_started_at_ms: None,
            active_operation_id: None,
            node_id: Some("node-a".into()),
            started_at_ms: None,
            last_error: None,
            metrics: StreamMetricsSnapshot::default(),
        }
    }

    #[tokio::test]
    async fn persisted_intent_survives_hub_restart_before_dispatch() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        let storage = StorageActor::start(store, 8);
        let hub1 = Hub::with_storage(config(), storage.clone());
        let intent = hub1
            .set_desired_state(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                expected_generation: Some(0),
                ..Default::default()
            })
            .await
            .unwrap();
        drop(hub1);

        let hub2 = Hub::with_storage(config(), storage);
        hub2.register(RegisterRequest {
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["stream_lifecycle".into()],
        })
        .await
        .unwrap();
        let operation = hub2.reconcile_once("after-restart").await.unwrap();
        assert_eq!(operation.as_ref().map(|value| value.generation), Some(1));
        assert_eq!(
            operation.as_ref().map(|value| value.id.as_str()),
            Some(intent.intent_id.as_str())
        );
        assert!(hub2
            .operations(None)
            .await
            .iter()
            .any(|value| value.intent_id.as_deref() == Some(intent.intent_id.as_str())));
    }

    #[tokio::test]
    async fn dispatched_attempt_waits_for_fresh_report_after_hub_restart() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        let storage = StorageActor::start(store, 8);
        let hub1 = Hub::with_storage(config(), storage.clone());
        let session1 = hub1
            .register(RegisterRequest {
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["stream_lifecycle".into()],
            })
            .await
            .unwrap();
        let intent = hub1
            .set_desired_state(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                expected_generation: Some(0),
                ..Default::default()
            })
            .await
            .unwrap();
        hub1.reconcile_once("dispatch").await.unwrap().unwrap();
        hub1.commands(AgentAuth {
            node_id: "node-a".into(),
            session_token: session1.session_token,
        })
        .await
        .unwrap();
        storage
            .expire_attempts(now_ms() + config().lease_ttl_ms + 1)
            .await
            .unwrap();
        drop(hub1);

        let hub2 = Hub::with_storage(config(), storage);
        let session2 = hub2
            .register(RegisterRequest {
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["stream_lifecycle".into()],
            })
            .await
            .unwrap();
        hub2.recover_persisted_state().await.unwrap();
        assert!(hub2
            .reconcile_once("without-report")
            .await
            .unwrap()
            .is_none());
        hub2.report(NodeReport {
            auth: AgentAuth {
                node_id: "node-a".into(),
                session_token: session2.session_token,
            },
            version: "test".into(),
            state: "online".into(),
            capabilities: vec!["stream_lifecycle".into()],
            streams: vec![stopped_report("orders", Some(0))],
            operations: vec![],
            events: vec![],
            metrics: BTreeMap::new(),
            configuration: None,
            configuration_version: None,
            boot_id: Some("boot-2".into()),
            report_seq: 1,
        })
        .await
        .unwrap();
        let operation = hub2.reconcile_once("after-report").await.unwrap();
        assert_eq!(
            operation.as_ref().map(|value| value.id.as_str()),
            Some(intent.intent_id.as_str())
        );
    }

    #[tokio::test]
    async fn registers_reports_and_dispatches_targeted_commands() {
        let hub = Hub::new(config());
        assert!(matches!(
            hub.register(RegisterRequest {
                node_id: "n1".into(),
                node_token: "bad".into(),
                protocol_version: "v1".into(),
                capabilities: vec![]
            })
            .await,
            Err(HubError::Unauthorized)
        ));
        let session = hub
            .register(RegisterRequest {
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["stream_lifecycle".into()],
            })
            .await
            .unwrap();
        hub.report(NodeReport {
            auth: AgentAuth {
                node_id: "n1".into(),
                session_token: session.session_token.clone(),
            },
            version: "test".into(),
            state: "online".into(),
            capabilities: vec!["stream_lifecycle".into()],
            streams: vec![],
            operations: vec![],
            events: vec![],
            metrics: BTreeMap::new(),
            configuration: None,
            configuration_version: None,
            boot_id: None,
            report_seq: 0,
        })
        .await
        .unwrap();
        let first = hub
            .enqueue(
                "n1".into(),
                "start".into(),
                "orders".into(),
                Some("corr".into()),
            )
            .await
            .unwrap();
        let second = hub
            .enqueue(
                "n1".into(),
                "start".into(),
                "orders".into(),
                Some("corr".into()),
            )
            .await
            .unwrap();
        assert_eq!(first.id, second.id);
        let commands = hub
            .commands(AgentAuth {
                node_id: "n1".into(),
                session_token: session.session_token.clone(),
            })
            .await
            .unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].operation_id, first.id);
        let result = hub
            .command_result(
                AgentAuth {
                    node_id: "n1".into(),
                    session_token: session.session_token,
                },
                CommandResult {
                    command_id: commands[0].id.clone(),
                    operation_id: "local-op".into(),
                    state: HubOperationState::Succeeded,
                    progress: 100,
                    error: None,
                    correlation_id: Some("corr".into()),
                    generation: 0,
                    observed_generation: None,
                    action_id: None,
                    failure_class: None,
                    config_version_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(result.state, HubOperationState::Succeeded);
    }

    #[tokio::test]
    async fn expired_lease_is_not_commandable() {
        let hub = Hub::new(HubConfig {
            lease_ttl_ms: 1,
            ..config()
        });
        let session = hub
            .register(RegisterRequest {
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec![],
            })
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(3)).await;
        hub.mark_stale().await;
        assert!(matches!(
            hub.enqueue("n1".into(), "start".into(), "orders".into(), None)
                .await,
            Err(HubError::NodeUnavailable)
        ));
        assert_eq!(hub.nodes().await[0].state, NodeConnectionState::Stale);
        assert!(!session.session_token.is_empty());
    }

    #[tokio::test]
    async fn ignores_replayed_reports_from_the_same_boot() {
        let hub = Hub::new(config());
        let session = hub
            .register(RegisterRequest {
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec![],
            })
            .await
            .unwrap();
        let stream = |state| {
            serde_json::from_value(serde_json::json!({
                "id": "orders",
                "state": state,
                "metrics": {
                    "input_batches": 0,
                    "input_messages": 0,
                    "processing_errors": 0,
                    "output_batches": 0,
                    "output_messages": 0,
                    "input_errors": 0,
                    "input_reconnects": 0,
                    "output_errors": 0,
                    "restarts": 0
                }
            }))
            .unwrap()
        };
        let report = |report_seq, state| NodeReport {
            auth: AgentAuth {
                node_id: "n1".into(),
                session_token: session.session_token.clone(),
            },
            version: "test".into(),
            state: "online".into(),
            capabilities: vec![],
            streams: vec![stream(state)],
            operations: vec![],
            events: vec![],
            metrics: BTreeMap::new(),
            configuration: None,
            configuration_version: None,
            boot_id: Some("boot-1".into()),
            report_seq,
        };
        hub.report(report(2, "running")).await.unwrap();
        hub.report(report(1, "stopped")).await.unwrap();
        let streams = hub.streams(Some("n1")).await;
        assert_eq!(
            streams[0].1.state,
            arkflow_core::control::StreamState::Running
        );
    }

    #[tokio::test]
    async fn reconciler_dispatches_persisted_intent_with_generation() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        let hub = Hub::with_storage(config(), crate::storage::StorageActor::start(store, 8));
        let session = hub
            .register(RegisterRequest {
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec![],
            })
            .await
            .unwrap();
        let intent = hub
            .set_desired_state(crate::storage::DesiredMutation {
                node_id: "n1".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                config_version_id: None,
                action_id: None,
                expected_generation: Some(0),
                actor: Some("operator".into()),
                correlation_id: None,
                idempotency_key: None,
                intent_type: None,
                payload_json: None,
            })
            .await
            .unwrap();
        assert_eq!(intent.generation, 1);
        let operation = hub
            .reconcile_once("test-reconciler")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(operation.operation, "start");
        let commands = hub
            .commands(AgentAuth {
                node_id: "n1".into(),
                session_token: session.session_token.clone(),
            })
            .await
            .unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].generation, 1);
        assert!(commands[0].attempt_id.is_some());

        hub.set_desired_state(crate::storage::DesiredMutation {
            node_id: "n1".into(),
            stream_id: "orders".into(),
            desired_state: "running".into(),
            config_version_id: None,
            action_id: Some("restart-action-1".into()),
            expected_generation: Some(1),
            actor: Some("operator".into()),
            correlation_id: None,
            idempotency_key: Some("restart-1".into()),
            ..Default::default()
        })
        .await
        .unwrap();
        let restart = hub
            .reconcile_once("test-reconciler")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restart.operation, "restart");
        let commands = hub
            .commands(AgentAuth {
                node_id: "n1".into(),
                session_token: session.session_token,
            })
            .await
            .unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].action_id.as_deref(), Some("restart-action-1"));
    }

    #[tokio::test]
    async fn reconnect_replaces_session_but_preserves_node_resources() {
        let hub = Hub::new(config());
        let first = hub
            .register(RegisterRequest {
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["first".into()],
            })
            .await
            .unwrap();
        let second = hub
            .register(RegisterRequest {
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["second".into()],
            })
            .await
            .unwrap();
        assert_ne!(first.session_token, second.session_token);
        assert!(matches!(
            hub.heartbeat(HeartbeatRequest {
                auth: AgentAuth {
                    node_id: "n1".into(),
                    session_token: first.session_token
                },
                state: "online".into(),
            })
            .await,
            Err(HubError::Unauthorized)
        ));
        hub.heartbeat(HeartbeatRequest {
            auth: AgentAuth {
                node_id: "n1".into(),
                session_token: second.session_token,
            },
            state: "online".into(),
        })
        .await
        .unwrap();
        assert_eq!(hub.nodes().await.len(), 1);
    }

    #[tokio::test]
    async fn command_queues_are_bounded_and_isolated_per_node() {
        let hub = Hub::new(config());
        hub.register(RegisterRequest {
            node_id: "n1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec![],
        })
        .await
        .unwrap();
        let n2_session = hub
            .register(RegisterRequest {
                node_id: "n2".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec![],
            })
            .await
            .unwrap()
            .session_token;
        for index in 0..128 {
            hub.enqueue("n1".into(), "start".into(), format!("stream-{index}"), None)
                .await
                .unwrap();
        }
        assert!(matches!(
            hub.enqueue("n1".into(), "start".into(), "overflow".into(), None)
                .await,
            Err(HubError::Capacity)
        ));
        let n2 = hub
            .commands(AgentAuth {
                node_id: "n2".into(),
                session_token: n2_session,
            })
            .await
            .unwrap();
        assert!(n2.is_empty());
    }
}
