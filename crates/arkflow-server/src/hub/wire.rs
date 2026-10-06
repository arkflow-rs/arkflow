//! Hub<->Agent wire types: registration, heartbeat, report, commands, results.

use arkflow_core::control::{ControlEvent, NodeMaintenanceState, OperationRecord, StreamStatus};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
    /// Base URL of the Hub this report was sent to (hub-ha stage 3). Older
    /// Agents omit the field.
    #[serde(default)]
    pub connected_hub: Option<String>,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AgentOperation {
    Start,
    Stop,
    Restart,
    ValidateConfiguration,
    DiffConfiguration,
    ApplyConfiguration,
    RollbackConfiguration,
    JobStart,
    JobRestart,
    JobStop,
    JobCheckpoint,
    JobSavepoint,
    JobCheckpointCommit,
    JobSavepointCommit,
    Reconcile,
    Unknown(String),
}

impl AgentOperation {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Restart => "restart",
            Self::ValidateConfiguration => "validate_configuration",
            Self::DiffConfiguration => "diff_configuration",
            Self::ApplyConfiguration => "apply_configuration",
            Self::RollbackConfiguration => "rollback_configuration",
            Self::JobStart => "job_start",
            Self::JobRestart => "job_restart",
            Self::JobStop => "job_stop",
            Self::JobCheckpoint => "job_checkpoint",
            Self::JobSavepoint => "job_savepoint",
            Self::JobCheckpointCommit => "job_checkpoint_commit",
            Self::JobSavepointCommit => "job_savepoint_commit",
            Self::Reconcile => "reconcile",
            Self::Unknown(raw) => raw,
        }
    }

    /// Parse a persisted or received operation name. Names outside the
    /// closed set keep their original string instead of failing: dispatch
    /// rejects them (fail closed) while persistence and audit records
    /// round-trip losslessly.
    pub fn parse(value: &str) -> Self {
        match value {
            "start" => Self::Start,
            "stop" => Self::Stop,
            "restart" => Self::Restart,
            "validate_configuration" => Self::ValidateConfiguration,
            "diff_configuration" => Self::DiffConfiguration,
            "apply_configuration" => Self::ApplyConfiguration,
            "rollback_configuration" => Self::RollbackConfiguration,
            "job_start" => Self::JobStart,
            "job_restart" => Self::JobRestart,
            "job_stop" => Self::JobStop,
            "job_checkpoint" => Self::JobCheckpoint,
            "job_savepoint" => Self::JobSavepoint,
            "job_checkpoint_commit" => Self::JobCheckpointCommit,
            "job_savepoint_commit" => Self::JobSavepointCommit,
            "reconcile" => Self::Reconcile,
            other => Self::Unknown(other.to_owned()),
        }
    }

    /// Whether this is a Job-plane operation (`job_*`): dispatched to the
    /// local Job runtime instead of the stream lifecycle and configuration
    /// paths. An unknown operation whose wire name carries the `job_`
    /// prefix stays on the Job plane so dispatch fails closed inside the
    /// Job path with the historical error wording.
    pub fn is_job(&self) -> bool {
        match self {
            Self::JobStart
            | Self::JobRestart
            | Self::JobStop
            | Self::JobCheckpoint
            | Self::JobSavepoint
            | Self::JobCheckpointCommit
            | Self::JobSavepointCommit => true,
            Self::Unknown(raw) => raw.starts_with("job_"),
            _ => false,
        }
    }
}

impl Serialize for AgentOperation {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for AgentOperation {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::parse(&String::deserialize(deserializer)?))
    }
}

impl std::fmt::Display for AgentOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCommand {
    pub id: String,
    pub operation_id: String,
    pub node_id: String,
    pub operation: AgentOperation,
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
    pub operation: AgentOperation,
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

#[cfg(test)]
mod operation_tests {
    use super::AgentOperation;

    /// The wire contract is the historical free-form string: every known
    /// variant MUST serialize to exactly the literal Agents and the storage
    /// layer have always used, in both directions, or a mixed-version fleet
    /// breaks.
    #[test]
    fn known_operations_round_trip_through_the_historical_strings() {
        let cases = [
            (AgentOperation::Start, "start"),
            (AgentOperation::Stop, "stop"),
            (AgentOperation::Restart, "restart"),
            (
                AgentOperation::ValidateConfiguration,
                "validate_configuration",
            ),
            (AgentOperation::DiffConfiguration, "diff_configuration"),
            (AgentOperation::ApplyConfiguration, "apply_configuration"),
            (
                AgentOperation::RollbackConfiguration,
                "rollback_configuration",
            ),
            (AgentOperation::JobStart, "job_start"),
            (AgentOperation::JobRestart, "job_restart"),
            (AgentOperation::JobStop, "job_stop"),
            (AgentOperation::JobCheckpoint, "job_checkpoint"),
            (AgentOperation::JobSavepoint, "job_savepoint"),
            (AgentOperation::JobCheckpointCommit, "job_checkpoint_commit"),
            (AgentOperation::JobSavepointCommit, "job_savepoint_commit"),
            (AgentOperation::Reconcile, "reconcile"),
        ];
        for (operation, literal) in cases {
            assert_eq!(operation.as_str(), literal, "{operation:?}");
            let json = serde_json::to_value(&operation).unwrap();
            assert_eq!(json, serde_json::json!(literal), "{operation:?}");
            let parsed: AgentOperation = serde_json::from_value(json).unwrap();
            assert_eq!(parsed, operation);
            assert_eq!(AgentOperation::parse(literal), operation);
        }
    }

    /// Unknown names (e.g. a newer Hub) deserialize losslessly and
    /// round-trip back to the exact original string.
    #[test]
    fn unknown_operations_preserve_their_original_string() {
        let json = serde_json::json!("job_teleport");
        let parsed: AgentOperation = serde_json::from_value(json).unwrap();
        assert_eq!(parsed, AgentOperation::Unknown("job_teleport".into()));
        assert_eq!(parsed.as_str(), "job_teleport");
        let reserialized = serde_json::to_value(&parsed).unwrap();
        assert_eq!(reserialized, serde_json::json!("job_teleport"));
        // A job_-prefixed unknown stays on the Job plane (fail closed inside
        // the Job dispatch path); any other unknown does not.
        assert!(parsed.is_job());
        let other: AgentOperation = serde_json::from_value(serde_json::json!("future_op")).unwrap();
        assert!(!other.is_job());
    }

    #[test]
    fn job_plane_operations_are_recognized() {
        assert!(AgentOperation::JobStart.is_job());
        assert!(AgentOperation::JobSavepointCommit.is_job());
        assert!(!AgentOperation::Start.is_job());
        assert!(!AgentOperation::ApplyConfiguration.is_job());
    }
}
