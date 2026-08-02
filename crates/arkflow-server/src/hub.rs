//! Hub-side node registry and command broker.
//!
//! The Hub owns fleet state; compute nodes own execution. This module contains
//! the transport-neutral state machine used by the HTTP handlers and Agent
//! client protocol.

use arkflow_core::control::{ControlEvent, OperationRecord, StreamStatus};
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HubOperation {
    pub id: String,
    pub command_id: String,
    pub node_id: String,
    pub operation: String,
    pub resource_id: String,
    pub state: HubOperationState,
    pub progress: u8,
    pub created_at_ms: u64,
    pub dispatched_at_ms: Option<u64>,
    pub acknowledged_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    pub correlation_id: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
struct NodeRecord {
    resource: HubNode,
    session_token: String,
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
}

impl Hub {
    pub fn new(config: HubConfig) -> Self {
        Self {
            config: Arc::new(config),
            nodes: Arc::new(RwLock::new(BTreeMap::new())),
            operations: Arc::new(RwLock::new(BTreeMap::new())),
            events: Arc::new(RwLock::new(VecDeque::new())),
        }
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
        let mut nodes = self.nodes.write().await;
        let node = authenticated_node(&mut nodes, &report.auth)?;
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
        drop(nodes);
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
        let now = now_ms();
        let mut nodes = self.nodes.write().await;
        let node = nodes.get_mut(&node_id).ok_or(HubError::NodeUnavailable)?;
        if node.resource.state != NodeConnectionState::Online
            || node.resource.lease_expires_at_ms <= now
        {
            return Err(HubError::NodeUnavailable);
        }
        let mut operations = self.operations.write().await;
        if let Some(existing) = operations.values().find(|item| {
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
        let id = format!("hop-{}", HUB_SEQUENCE.fetch_add(1, Ordering::Relaxed));
        let command_id = format!("cmd-{}", HUB_SEQUENCE.fetch_add(1, Ordering::Relaxed));
        if node.commands.len() >= MAX_COMMANDS_PER_NODE {
            return Err(HubError::Capacity);
        }
        let operation_record = HubOperation {
            id,
            command_id: command_id.clone(),
            node_id: node_id.clone(),
            operation: operation.clone(),
            resource_id: resource_id.clone(),
            state: HubOperationState::Queued,
            progress: 0,
            created_at_ms: now,
            dispatched_at_ms: None,
            acknowledged_at_ms: None,
            finished_at_ms: None,
            correlation_id: correlation_id.clone(),
            error: None,
        };
        let command = AgentCommand {
            id: command_id.clone(),
            operation_id: operation_record.id.clone(),
            node_id,
            operation,
            resource_id,
            expires_at_ms: now + self.config.lease_ttl_ms,
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
        if matches!(
            result.state,
            HubOperationState::Succeeded
                | HubOperationState::Failed
                | HubOperationState::TimedOut
                | HubOperationState::NodeUnavailable
                | HubOperationState::Cancelled
        ) {
            operation.finished_at_ms = Some(now_ms());
        }
        if matches!(result.state, HubOperationState::Acknowledged) {
            operation.acknowledged_at_ms = Some(now_ms());
        }
        Ok(operation.clone())
    }

    pub async fn nodes(&self) -> Vec<HubNode> {
        self.nodes
            .read()
            .await
            .values()
            .map(|node| node.resource.clone())
            .collect()
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
    pub async fn operations(&self, node_id: Option<&str>) -> Vec<HubOperation> {
        self.operations
            .read()
            .await
            .values()
            .filter(|operation| node_id.is_none_or(|id| operation.node_id == id))
            .cloned()
            .collect()
    }

    pub async fn operation(&self, id: &str) -> Option<HubOperation> {
        self.operations.read().await.get(id).cloned()
    }
    pub async fn events(&self, node_id: Option<&str>) -> Vec<HubEvent> {
        self.events
            .read()
            .await
            .iter()
            .filter(|event| node_id.is_none_or(|id| event.node_id == id))
            .cloned()
            .collect()
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

fn default_protocol_version() -> String {
    "v1".into()
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}
static SESSION_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static HUB_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> HubConfig {
        HubConfig {
            operator_token: Some("operator".into()),
            node_token: Some("node-secret".into()),
            lease_ttl_ms: 1000,
            poll_interval_ms: 10,
        }
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
