//! Hub<->Agent wire types: registration, heartbeat, report, commands, results.

use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub node_id: String,
    pub node_token: String,
    #[serde(default = "default_protocol_version")]
    pub protocol_version: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Stable identity of the Agent process. It is independent from the
    /// per-registration session token, so a reconnect in the same process
    /// does not look like a process restart to report/reconciliation logic.
    #[serde(default)]
    pub boot_id: Option<String>,
    /// Advertised cross-node data-plane address ("host:port"). Present only
    /// on nodes running the network shuffle data plane.
    #[serde(default)]
    pub data_address: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterResponse {
    pub node_id: String,
    pub session_token: String,
    #[serde(default)]
    pub session_ttl_ms: u64,
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
pub struct JobObservationRequest {
    #[serde(flatten)]
    pub auth: AgentAuth,
    pub job_id: String,
    pub generation: u64,
    pub state: String,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    #[serde(flatten)]
    pub auth: AgentAuth,
    pub state: String,
    #[serde(default)]
    pub protocol_version: Option<String>,
    #[serde(default)]
    pub software_version: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub rollout_id: Option<String>,
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
    /// Per-Job kernel metric snapshots for the data-plane Prometheus export.
    /// Older Agents omit the field (empty map = no data-plane series).
    #[serde(default)]
    pub jobs: BTreeMap<String, arkflow_core::executor::metrics::KernelMetricsSnapshot>,
    #[serde(default)]
    pub configuration: Option<serde_json::Value>,
    #[serde(default)]
    pub configuration_version: Option<String>,
    /// Known configuration versions on the node (id/format metadata only,
    /// never content) so Hub consumers can list and diff without a second
    /// channel. Older Agents omit the field (empty list).
    #[serde(default)]
    pub config_versions: Vec<arkflow_core::configuration::ConfigVersion>,
    /// Task ids each Job kernel is currently executing on the node, keyed by
    /// job id. Presence of a task id is the node's observation that the task
    /// is running there. Older Agents omit the field.
    #[serde(default)]
    pub job_tasks: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub boot_id: Option<String>,
    #[serde(default)]
    pub report_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HubNode {
    pub id: String,
    pub protocol_version: String,
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
    /// Advertised cross-node data-plane address ("host:port").
    #[serde(default)]
    pub data_address: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HubEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<i64>,
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
    #[serde(default)]
    pub rollout_id: Option<String>,
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub payload: Option<serde_json::Value>,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
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
    #[serde(default)]
    pub rollout_id: Option<String>,
    #[serde(default)]
    pub observed_checkpoint_id: Option<String>,
    #[serde(default)]
    pub checkpoint_manifest_uri: Option<String>,
    /// Read-only command report (e.g. a configuration validation or diff)
    /// delivered alongside the terminal state. Absent for mutations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
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
    pub checkpoint_id: Option<String>,
    #[serde(default)]
    pub generation: u64,
    #[serde(default)]
    pub attempt_id: Option<String>,
    #[serde(default)]
    pub config_version_id: Option<String>,
    pub state: HubOperationState,
    pub progress: u8,
    pub created_at_ms: u64,
    /// Delivery window after which the expiry sweep stops treating the
    /// operation as in flight. Absent on rows persisted before this field
    /// existed; those rows never expire, which preserves their old behavior.
    #[serde(default)]
    pub expires_at_ms: Option<u64>,
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
    /// Report payload of a read-only command (validation/diff), carried from
    /// the agent result so polling clients can read the outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
}

pub(crate) fn default_protocol_version() -> String {
    "v1".into()
}
