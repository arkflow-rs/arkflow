//! Hub-side node registry and command broker.
//!
//! The Hub owns fleet state; compute nodes own execution. This module contains
//! the transport-neutral state machine used by the HTTP handlers and Agent
//! client protocol.

use crate::agent::{delete_checkpoint_artifact, recovery_record_is_valid};
use crate::api_contract::{OperatorAction, OperatorPrincipal, OperatorRole, ResourceScope};
use crate::storage::{
    AttemptRecord, DesiredMutation, IntentRecord, JobCheckpointRecord, JobRecord, JobVersionRecord,
    NodeMutation, ObservedMutation, PersistedOperation, RolloutRecord, RolloutTargetRecord,
    RolloutTargetUpdate, StorageActor, StorageError,
};
use arkflow_core::control::{
    ControlEvent, NodeMaintenanceState, OperationRecord, OperationalStatus, ReconciliationHealth,
    StreamStatus,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;
use tokio::sync::{broadcast, RwLock};

const MAX_NODES: usize = 256;
const MAX_COMMANDS_PER_NODE: usize = 128;
const MAX_OPERATIONS: usize = 1024;
const MAX_EVENTS: usize = 2048;
const MAX_JOB_RECONCILIATIONS_PER_TICK: usize = 256;
const SUPPORTED_PROTOCOL_VERSION: &str = "v1";
const ALLOWED_NODE_METRICS: &[&str] = &[
    "input_batches",
    "input_messages",
    "processing_errors",
    "output_batches",
    "output_messages",
    "input_errors",
    "input_reconnects",
    "output_errors",
    "restarts",
    "streams_total",
    "streams_running",
    "kernel_batches_in",
    "kernel_batches_out",
    "kernel_rows",
    "kernel_errors",
    "in_flight",
    "mean_latency_us",
    "checkpoint_duration_ms",
    "checkpoint_failures",
    "watermark_lag_ms",
    "late_events",
    "jobs_total",
    "jobs_running",
    "jobs_ephemeral_state",
    "jobs_recovery_required",
    // Host resource gauges sampled by the Agent (see agent.rs
    // ResourceSampler): ephemeral registry state, exported as-is.
    "node_cpu_usage_percent",
    "node_memory_used_bytes",
    "node_memory_total_bytes",
    "node_memory_available_bytes",
];

#[derive(Debug, Clone)]
pub struct HubConfig {
    pub operator_token: Option<String>,
    pub node_token: Option<String>,
    /// Explicitly permit the legacy unauthenticated behavior for an
    /// in-process/local-development Hub. Production startup validates this
    /// flag against the loopback bind address before serving.
    pub insecure_local: bool,
    pub lease_ttl_ms: u64,
    pub poll_interval_ms: u64,
    /// Hard lifetime of an issued agent session credential. Credentials are
    /// never renewed: after expiry every agent request is rejected and the
    /// Agent transparently re-registers through its normal reconnect loop.
    pub session_ttl_ms: u64,
}

pub fn default_session_ttl_ms() -> u64 {
    3_600_000
}

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
}

#[derive(Debug, Clone)]
struct NodeRecord {
    resource: HubNode,
    session_token: String,
    /// Wall-clock instant after which the session token stops authenticating.
    /// Credentials are never renewed; only re-registration mints a new one.
    session_expires_at_ms: u64,
    boot_id: Option<String>,
    report_seq: u64,
    commands: VecDeque<AgentCommand>,
    /// Commands returned by the poll endpoint remain leased until a terminal
    /// result arrives. Keeping the lease separately from the queue lets the
    /// Hub recover a command whose HTTP response was lost after it was popped
    /// but before the Agent executed it.
    leased_commands: BTreeMap<String, AgentCommand>,
    streams: Vec<StreamStatus>,
    operations: Vec<OperationRecord>,
    events: Vec<ControlEvent>,
    metrics: BTreeMap<String, f64>,
    /// Last report that carried the metrics above (0 = never reported).
    /// Heartbeats refresh `last_seen_at_ms` but not this, so gauge freshness
    /// stays honest for placement ranking and pressure detection.
    last_report_at_ms: u64,
    /// Consecutive reports over the fleet pressure predicate; reset by any
    /// under-threshold (or gauge-less) report. Drives opt-in rebalancing.
    pressure_streak: u32,
    /// Most recent per-Job kernel snapshots reported by the Agent.
    jobs: BTreeMap<String, arkflow_core::executor::metrics::KernelMetricsSnapshot>,
    configuration: Option<serde_json::Value>,
}

#[derive(Clone)]
pub struct Hub {
    config: Arc<HubConfig>,
    nodes: Arc<RwLock<BTreeMap<String, NodeRecord>>>,
    operations: Arc<RwLock<BTreeMap<String, HubOperation>>>,
    rollouts: Arc<RwLock<BTreeMap<String, RolloutRecord>>>,
    events: Arc<RwLock<VecDeque<HubEvent>>>,
    updates: broadcast::Sender<HubEvent>,
    storage: Option<StorageActor>,
    lifecycle: Arc<RwLock<HubLifecycle>>,
    jobs: Arc<RwLock<BTreeMap<String, JobRecord>>>,
    job_versions: Arc<RwLock<BTreeMap<String, Vec<JobVersionRecord>>>>,
    job_checkpoints: Arc<RwLock<BTreeMap<String, Vec<JobCheckpointRecord>>>>,
    /// The node order each Job's placement was actually dispatched in, so a
    /// retained placement re-dispatches with the identical task→node mapping
    /// (split round-robin and multi-component co-location are order
    /// sensitive). In-memory by design: after a Hub restart the order falls
    /// back to node-id order until the next ranked placement, which is a
    /// legal re-placement (state restores per task attempt).
    placement_order: Arc<RwLock<BTreeMap<String, Vec<String>>>>,
    command_metrics: Arc<CommandMetrics>,
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

/// Fixed latency buckets (milliseconds) for command dispatch accounting.
const COMMAND_METRIC_BUCKETS_MS: &[u64] = &[5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000];

/// An operation whose (Job, generation, operation) key has been retried this
/// many times by the expiry sweep reaches a terminal failed state and is no
/// longer re-enqueued.
const MAX_JOB_OPERATION_RETRIES: u32 = 3;

/// Low-cardinality command dispatch accounting: fixed command-type labels,
/// fixed outcome classes, fixed latency buckets. Counters live in process
/// memory and reset on Hub restart, in line with Prometheus counter
/// semantics.
#[derive(Default)]
pub struct CommandMetrics {
    latencies: std::sync::Mutex<BTreeMap<String, CommandLatency>>,
    outcomes: std::sync::Mutex<BTreeMap<(String, String), u64>>,
}

#[derive(Default)]
struct CommandLatency {
    buckets: Vec<u64>,
    sum_ms: u64,
    count: u64,
}

impl CommandMetrics {
    /// Bound the label vocabulary: recognized command names pass through;
    /// anything else collapses into `other` so dynamic operation strings
    /// cannot grow the series space.
    fn command_label(operation: &str) -> String {
        const KNOWN: &[&str] = &[
            "job_start",
            "job_stop",
            "job_checkpoint",
            "job_savepoint",
            "job_checkpoint_commit",
            "job_savepoint_commit",
            "start",
            "stop",
            "restart",
            "apply_configuration",
        ];
        if KNOWN.contains(&operation) {
            operation.to_owned()
        } else {
            "other".to_owned()
        }
    }

    fn outcome_label(state: HubOperationState) -> Option<&'static str> {
        match state {
            HubOperationState::Succeeded => Some("succeeded"),
            HubOperationState::Failed => Some("failed"),
            HubOperationState::TimedOut => Some("timed_out"),
            HubOperationState::NodeUnavailable => Some("node_unavailable"),
            HubOperationState::Cancelled => Some("cancelled"),
            HubOperationState::Superseded => Some("superseded"),
            HubOperationState::Queued
            | HubOperationState::Dispatched
            | HubOperationState::Acknowledged
            | HubOperationState::Running => None,
        }
    }

    fn record_outcome(&self, operation: &str, outcome: &str) {
        let label = Self::command_label(operation);
        if let Ok(mut outcomes) = self.outcomes.lock() {
            *outcomes.entry((label, outcome.to_owned())).or_default() += 1;
        }
    }

    fn record_latency(&self, operation: &str, duration_ms: u64) {
        let label = Self::command_label(operation);
        let Ok(mut latencies) = self.latencies.lock() else {
            return;
        };
        let latency = latencies.entry(label).or_default();
        if latency.buckets.is_empty() {
            latency.buckets = vec![0; COMMAND_METRIC_BUCKETS_MS.len() + 1];
        }
        let index = COMMAND_METRIC_BUCKETS_MS.partition_point(|bound| *bound < duration_ms);
        latency.buckets[index] += 1;
        latency.sum_ms += duration_ms;
        latency.count += 1;
    }

    /// Render the Prometheus text exposition for command dispatch metrics.
    /// Series are bounded by the fixed command-type and outcome-class
    /// enumerations; resource IDs and error texts never appear.
    pub fn render(&self) -> String {
        let mut body = String::new();
        if let Ok(latencies) = self.latencies.lock() {
            for (command, latency) in latencies.iter() {
                for (index, bound) in COMMAND_METRIC_BUCKETS_MS.iter().enumerate() {
                    body.push_str(&format!(
                        "arkflow_command_duration_bucket{{command=\"{command}\",le=\"{bound}\"}} {}\n",
                        latency.buckets[index]
                    ));
                }
                body.push_str(&format!(
                    "arkflow_command_duration_bucket{{command=\"{command}\",le=\"+Inf\"}} {}\n",
                    latency.count
                ));
                body.push_str(&format!(
                    "arkflow_command_duration_count{{command=\"{command}\"}} {}\n",
                    latency.count
                ));
                body.push_str(&format!(
                    "arkflow_command_duration_sum{{command=\"{command}\"}} {}\n",
                    latency.sum_ms
                ));
            }
        }
        if let Ok(outcomes) = self.outcomes.lock() {
            for ((command, outcome), total) in outcomes.iter() {
                body.push_str(&format!(
                    "arkflow_command_total{{command=\"{command}\",outcome=\"{outcome}\"}} {total}\n"
                ));
            }
        }
        body
    }
}

impl Hub {
    pub fn new(config: HubConfig) -> Self {
        let (updates, _) = broadcast::channel(256);
        Self {
            config: Arc::new(config),
            nodes: Arc::new(RwLock::new(BTreeMap::new())),
            operations: Arc::new(RwLock::new(BTreeMap::new())),
            rollouts: Arc::new(RwLock::new(BTreeMap::new())),
            events: Arc::new(RwLock::new(VecDeque::new())),
            updates,
            storage: None,
            lifecycle: Arc::new(RwLock::new(HubLifecycle::default())),
            jobs: Arc::new(RwLock::new(BTreeMap::new())),
            job_versions: Arc::new(RwLock::new(BTreeMap::new())),
            job_checkpoints: Arc::new(RwLock::new(BTreeMap::new())),
            placement_order: Arc::new(RwLock::new(BTreeMap::new())),
            command_metrics: Arc::new(CommandMetrics::default()),
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

    /// Startup diagnostics for the fail-open token defaults: an unset token
    /// means the corresponding route class accepts unauthenticated callers.
    pub fn operator_token_is_set(&self) -> bool {
        self.config
            .operator_token
            .as_deref()
            .is_some_and(|token| !token.trim().is_empty())
    }

    pub fn node_token_is_set(&self) -> bool {
        self.config
            .node_token
            .as_deref()
            .is_some_and(|token| !token.trim().is_empty())
    }

    pub async fn jobs(&self) -> Result<Vec<JobRecord>, HubError> {
        if let Some(storage) = &self.storage {
            return storage.list_jobs().await.map_err(HubError::from);
        }
        Ok(self.jobs.read().await.values().cloned().collect())
    }

    pub async fn job(&self, job_id: &str) -> Result<Option<JobRecord>, HubError> {
        if let Some(storage) = &self.storage {
            return storage.get_job(job_id).await.map_err(HubError::from);
        }
        Ok(self.jobs.read().await.get(job_id).cloned())
    }

    pub async fn upsert_job(&self, mut job: JobRecord) -> Result<JobRecord, HubError> {
        // A pinned placement has nothing to relocate to: rebalancing is a
        // scheduler decision over unpinned Jobs, so an explicit pin combined
        // with the auto policy is a configuration contradiction, not a
        // silent no-op.
        if let Ok(spec) = serde_json::from_str::<arkflow_core::job::JobSpec>(&job.spec_json) {
            if spec.rebalance.is_some_and(|policy| {
                policy.mode == arkflow_core::job::RebalanceMode::Auto
            }) && !job.node_ids.is_empty()
            {
                return Err(HubError::Invalid(
                    "rebalance policy 'auto' requires an unpinned placement: remove node_ids"
                        .into(),
                ));
            }
        }
        let version_record = serde_json::from_str::<arkflow_core::job::JobSpec>(&job.spec_json)
            .ok()
            .and_then(|spec| {
                arkflow_core::job::JobPlan::compile(spec)
                    .ok()
                    .and_then(|plan| serde_json::to_string(&plan).ok())
                    .map(|plan_json| JobVersionRecord {
                        job_id: job.job_id.clone(),
                        version: job.version,
                        spec_json: job.spec_json.clone(),
                        plan_json,
                        created_at_ms: now_ms(),
                    })
            });
        if let Some(storage) = &self.storage {
            job = storage.upsert_job(job).await.map_err(HubError::from)?;
            if let Some(record) = version_record.clone() {
                storage
                    .upsert_job_version(record)
                    .await
                    .map_err(HubError::from)?;
            }
        } else {
            let mut jobs = self.jobs.write().await;
            job.generation = jobs
                .get(&job.job_id)
                .map(|current| current.generation.saturating_add(1))
                .unwrap_or_else(|| job.generation.max(1));
            jobs.insert(job.job_id.clone(), job.clone());
        }
        if let Some(record) = version_record {
            let mut versions = self.job_versions.write().await;
            let entries = versions.entry(record.job_id.clone()).or_default();
            entries.retain(|existing| existing.version != record.version);
            entries.push(record);
            entries.sort_by_key(|entry| std::cmp::Reverse(entry.version));
        }
        if self.storage.is_some() {
            self.jobs
                .write()
                .await
                .insert(job.job_id.clone(), job.clone());
        }
        if job.desired_state != "stopped" {
            self.reconcile_job(&job).await?;
        }
        Ok(job)
    }

    /// Generation-fenced Job record replacement for upgrade and rollback.
    /// The handlers read the Job, await several round trips and then write;
    /// the fence makes a concurrent desired-state change (or reconciler
    /// write) that bumped the generation surface as a conflict instead of
    /// being silently overwritten by the older read.
    pub async fn update_job_with_expected_generation(
        &self,
        job: JobRecord,
        expected_generation: u64,
    ) -> Result<JobRecord, HubError> {
        let version_record = serde_json::from_str::<arkflow_core::job::JobSpec>(&job.spec_json)
            .ok()
            .and_then(|spec| {
                arkflow_core::job::JobPlan::compile(spec)
                    .ok()
                    .and_then(|plan| serde_json::to_string(&plan).ok())
                    .map(|plan_json| JobVersionRecord {
                        job_id: job.job_id.clone(),
                        version: job.version,
                        spec_json: job.spec_json.clone(),
                        plan_json,
                        created_at_ms: now_ms(),
                    })
            });
        let updated = if let Some(storage) = &self.storage {
            let updated = storage
                .update_job_with_expected_generation(job.clone(), expected_generation)
                .await
                .map_err(HubError::from)?;
            if let Some(record) = version_record.clone() {
                storage
                    .upsert_job_version(record)
                    .await
                    .map_err(HubError::from)?;
            }
            self.jobs
                .write()
                .await
                .insert(updated.job_id.clone(), updated.clone());
            updated
        } else {
            let mut jobs = self.jobs.write().await;
            match jobs.get(&job.job_id) {
                Some(current) if current.generation == expected_generation => {
                    // Same rule as the storage backend: the recovery pointer
                    // belongs to the checkpoint path, which moves it without
                    // bumping the generation, so this write must not copy the
                    // caller's earlier read back over it.
                    let stored_checkpoint = current.checkpoint_id.clone();
                    let mut updated = job;
                    updated.generation = expected_generation.saturating_add(1);
                    if stored_checkpoint.is_some() {
                        updated.checkpoint_id = stored_checkpoint;
                    }
                    jobs.insert(updated.job_id.clone(), updated.clone());
                    updated
                }
                Some(current) => {
                    return Err(HubError::from(StorageError::GenerationConflict {
                        expected: expected_generation,
                        current: current.generation,
                    }));
                }
                None => {
                    return Err(HubError::from(StorageError::GenerationConflict {
                        expected: expected_generation,
                        current: 0,
                    }));
                }
            }
        };
        if let Some(record) = version_record {
            let mut versions = self.job_versions.write().await;
            let entries = versions.entry(record.job_id.clone()).or_default();
            entries.retain(|existing| existing.version != record.version);
            entries.push(record);
            entries.sort_by_key(|entry| std::cmp::Reverse(entry.version));
        }
        Ok(updated)
    }

    pub async fn job_versions(&self, job_id: &str) -> Result<Vec<JobVersionRecord>, HubError> {
        if let Some(storage) = &self.storage {
            let versions = storage
                .list_job_versions(job_id)
                .await
                .map_err(HubError::from)?;
            if !versions.is_empty() {
                return Ok(versions);
            }
        }
        Ok(self
            .job_versions
            .read()
            .await
            .get(job_id)
            .cloned()
            .unwrap_or_default())
    }

    /// Reconcile a bounded set of durable Jobs so Agent failures and Hub
    /// recovery converge without waiting for a new lifecycle request.
    /// The retained placement set, in the order its placement was actually
    /// dispatched in (remembered at ranked-dispatch time), so a retained
    /// re-dispatch reproduces the identical task→node mapping. Nodes the
    /// memory does not know (e.g. after a Hub restart) are appended in id
    /// order.
    async fn retained_targets_in_dispatch_order(
        &self,
        job_id: &str,
        set: &BTreeSet<String>,
    ) -> Vec<String> {
        let remembered = self
            .placement_order
            .read()
            .await
            .get(job_id)
            .cloned()
            .unwrap_or_default();
        let mut ordered: Vec<String> = remembered
            .into_iter()
            .filter(|node_id| set.contains(node_id))
            .collect();
        for node_id in set {
            if !ordered.contains(node_id) {
                ordered.push(node_id.clone());
            }
        }
        ordered
    }

    /// Nodes in `targets` whose sustained-pressure streak trips the Job's
    /// opt-in rebalance policy, subject to its cooldown and rollout
    /// ownership. Empty unless rebalancing may proceed this tick; the caller
    /// only evicts while at least one target remains (never into nothing).
    async fn rebalance_evictions(
        &self,
        job: &JobRecord,
        spec: &arkflow_core::job::JobSpec,
        targets: &[String],
    ) -> BTreeSet<String> {
        let Some(policy) = &spec.rebalance else {
            return BTreeSet::new();
        };
        if policy.mode != arkflow_core::job::RebalanceMode::Auto || !job.node_ids.is_empty() {
            return BTreeSet::new();
        }
        if targets.is_empty() {
            return BTreeSet::new();
        }
        let now = now_ms();
        {
            let operations = self.operations.read().await;
            let mut latest_start_ms: Option<u64> = None;
            for operation_record in operations.values() {
                if operation_record.resource_id != job.job_id
                    || operation_record.operation != "job_start"
                    || operation_record.generation != job.generation
                {
                    continue;
                }
                latest_start_ms = Some(
                    latest_start_ms
                        .unwrap_or(0)
                        .max(operation_record.created_at_ms),
                );
            }
            // Hysteresis: a placement dispatched inside the cooldown window
            // — the initial placement included — is not moved again yet.
            if latest_start_ms.is_some_and(|latest| now.saturating_sub(latest) < policy.cooldown_ms)
            {
                return BTreeSet::new();
            }
        }
        let mut evicted = BTreeSet::new();
        {
            let nodes = self.nodes.read().await;
            for node_id in targets {
                if nodes
                    .get(node_id)
                    .is_some_and(|node| node.pressure_streak >= policy.pressure_streak.max(1))
                {
                    evicted.insert(node_id.clone());
                }
            }
        }
        if evicted.len() >= targets.len() {
            // Nowhere to relocate to (single-node fleet, or every target is
            // pressuring): keep running where it is and retry next tick.
            return BTreeSet::new();
        }
        evicted
    }

    pub async fn reconcile_jobs(&self) -> Result<usize, HubError> {
        let jobs = self.jobs().await?;
        let mut dispatched = 0;
        for job in jobs.into_iter().take(MAX_JOB_RECONCILIATIONS_PER_TICK) {
            match self.reconcile_job(&job).await {
                Ok(count) => dispatched += count,
                // One Job whose target is at capacity, whose node expired
                // between the online pre-check and the enqueue, or whose
                // persisted spec no longer compiles must not stall the tick
                // for every other Job; the same ordering failure would repeat
                // each tick.
                Err(
                    error @ (HubError::Capacity | HubError::Invalid(_) | HubError::NodeUnavailable),
                ) => {
                    tracing::warn!(
                        job_id = %job.job_id,
                        error = %error,
                        "skipping Job reconciliation this tick"
                    );
                }
                Err(error) => return Err(error),
            }
        }
        Ok(dispatched)
    }

    pub async fn reconcile_job(&self, job: &JobRecord) -> Result<usize, HubError> {
        let spec: arkflow_core::job::JobSpec = serde_json::from_str(&job.spec_json)
            .map_err(|error| HubError::Invalid(format!("invalid persisted Job spec: {error}")))?;
        let plan = arkflow_core::job::JobPlan::compile(spec.clone())
            .map_err(|error| HubError::Invalid(error.to_string()))?;
        let operation = match job.desired_state.as_str() {
            "running" => "job_start",
            "stopped" => "job_stop",
            _ => return Ok(0),
        };
        // A durable state directory is not a disposable cache.  Once a Job
        // has successfully run, a restart or a version/generation move must
        // restore a compatible completed checkpoint before any source is
        // started again.  The first start of a brand-new Job is exempt: no
        // prior committed state exists yet.
        let persisted_job_starts = if operation == "job_start"
            && spec.state.as_ref().is_some_and(|state| {
                state.durability == arkflow_core::job::StateDurability::Durable
            })
            && spec.requires_state()
            && spec.checkpoint.is_some()
        {
            let records = match &self.storage {
                Some(storage) => storage
                    .list_job_start_operations(job.job_id.clone())
                    .await
                    .map_err(HubError::from)?,
                None => Vec::new(),
            };
            records
                .into_iter()
                .filter_map(|record| {
                    serde_json::from_str::<HubOperation>(&record.operation_json).ok()
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let recovery_required = if operation == "job_start"
            && spec.state.as_ref().is_some_and(|state| {
                state.durability == arkflow_core::job::StateDurability::Durable
            })
            && spec.requires_state()
            && spec.checkpoint.is_some()
        {
            let operations = self.operations.read().await;
            // A successful start is a durable fact even after the Hub fences
            // that operation because the Agent process was replaced.  The
            // `recovery_required` failure class preserves that fact across
            // the in-memory and SQLite operation histories; a plain
            // NodeUnavailable/TimedOut record is not sufficient because it
            // may represent a command that never reached an Agent.
            let previous_generation_started =
                operations.values().any(|operation_record| {
                    operation_record.resource_id == job.job_id
                        && is_durable_job_start(operation_record)
                        && operation_record.generation < job.generation
                }) || persisted_job_starts.iter().any(|operation_record| {
                    is_durable_job_start(operation_record)
                        && operation_record.generation < job.generation
                });
            let current_generation_requires_recovery =
                operations.values().any(|operation_record| {
                    operation_record.resource_id == job.job_id
                        && operation_record.operation == "job_start"
                        && operation_record.generation == job.generation
                        && (operation_record.failure_class.as_deref() == Some("recovery_required")
                            || (operation_record.state == HubOperationState::Succeeded
                                && matches!(job.observed_state.as_str(), "failed" | "stopped")))
                }) || persisted_job_starts.iter().any(|operation_record| {
                    operation_record.resource_id == job.job_id
                        && operation_record.generation == job.generation
                        && operation_record.failure_class.as_deref() == Some("recovery_required")
                });
            // A checkpoint/savepoint pointer is an explicit recovery request.
            // It must never be silently ignored just because the lifecycle
            // operation at the current generation is still marked Succeeded.
            job.checkpoint_id.is_some()
                || previous_generation_started
                || current_generation_requires_recovery
        } else {
            false
        };
        let candidates = if job.node_ids.is_empty() {
            self.nodes
                .read()
                .await
                .iter()
                .filter(|(_, node)| {
                    node.resource.state == NodeConnectionState::Online
                        && node.resource.lease_expires_at_ms > now_ms()
                })
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>()
        } else {
            job.node_ids.clone()
        };
        let targets = {
            let nodes = self.nodes.read().await;
            candidates
                .into_iter()
                .filter(|node_id| {
                    nodes.get(node_id).is_some_and(|node| {
                        node.resource.state == NodeConnectionState::Online
                            && node.resource.lease_expires_at_ms > now_ms()
                            && node.resource.maintenance_state == NodeMaintenanceState::Active
                    })
                })
                .collect::<Vec<_>>()
        };
        // Resource-aware ordering for unpinned placements: highest headroom
        // first, so a colocated Job lands on the freshest node and split
        // round-robin spreads from the best-ranked set. Pinned node_ids pass
        // through verbatim. The dispatched order is remembered further below
        // (only when this ranked order actually drives the dispatch).
        let mut targets = targets;
        if job.node_ids.is_empty() {
            let nodes = self.nodes.read().await;
            targets = rank_candidates(targets, &nodes, now_ms());
        }
        // Opt-in pressure rebalance: exclude nodes whose sustained-pressure
        // streak trips the Job's policy. The abandoned-placement fencing
        // below then supersedes their starts and dispatches their stops.
        let evictions = if operation == "job_start" {
            self.rebalance_evictions(job, &spec, &targets).await
        } else {
            BTreeSet::new()
        };
        if !evictions.is_empty() {
            targets.retain(|node_id| !evictions.contains(node_id));
        }
        let historical_nodes = self
            .operations
            .read()
            .await
            .values()
            .filter(|operation_record| {
                operation_record.resource_id == job.job_id
                    && operation_record.operation == "job_start"
                    // Keep starts from older generations in the placement
                    // history: a generation change may move a Job to another
                    // node, and the old node must receive a stop command.
                    && operation_record.generation <= job.generation
            })
            .map(|operation_record| operation_record.node_id.clone())
            .collect::<BTreeSet<_>>();
        // A successful/current operation is used to retain an automatic
        // placement. Historical failed or expired starts are intentionally
        // excluded here: they may never have reached an Agent and must not
        // pin a newly reconciled Job to a dead node. They remain in
        // `historical_nodes` so a partially executed command can still be
        // fenced with a best-effort stop below.
        let previous_nodes = self
            .operations
            .read()
            .await
            .values()
            .filter(|operation_record| {
                operation_record.resource_id == job.job_id
                    && operation_record.operation == "job_start"
                    && operation_record.generation <= job.generation
                    && !matches!(
                        operation_record.state,
                        HubOperationState::Failed
                            | HubOperationState::TimedOut
                            | HubOperationState::NodeUnavailable
                            | HubOperationState::Cancelled
                            | HubOperationState::Superseded
                    )
            })
            .map(|operation_record| operation_record.node_id.clone())
            .collect::<BTreeSet<_>>();
        let mut previous_nodes_all_online = !previous_nodes.is_empty();
        for node_id in &previous_nodes {
            let online = self.nodes.read().await.get(node_id).is_some_and(|node| {
                node.resource.state == NodeConnectionState::Online
                    && node.resource.lease_expires_at_ms > now_ms()
                    && node.resource.maintenance_state == NodeMaintenanceState::Active
            });
            if !online {
                previous_nodes_all_online = false;
                break;
            }
        }
        let retention_won =
            operation == "job_start" && job.node_ids.is_empty() && previous_nodes_all_online
                && evictions.is_empty();
        let targets = if retention_won {
            // Re-dispatch in the placement's original node order: split
            // round-robin and multi-component co-location are order
            // sensitive, and the mapping for a retained placement must not
            // drift between dispatches.
            self.retained_targets_in_dispatch_order(&job.job_id, &previous_nodes)
                .await
        } else if operation == "job_stop" {
                // A stopped Job must reach every node that may still host an
                // older generation. Such a node is not necessarily part of
                // the current explicit placement (for example after a move
                // from A to B), so filtering the already-derived current
                // targets would silently omit A. Include both the durable
                // start history and the current placement, then retain only
                // nodes that can accept a command now.
                let mut target_ids = historical_nodes.clone();
                target_ids.extend(targets.iter().cloned());
                let nodes = self.nodes.read().await;
                target_ids
                    .into_iter()
                    .filter(|node_id| {
                        nodes.get(node_id).is_some_and(|node| {
                            node.resource.state == NodeConnectionState::Online
                                && node.resource.lease_expires_at_ms > now_ms()
                                && node.resource.maintenance_state == NodeMaintenanceState::Active
                        })
                    })
                    .collect::<Vec<_>>()
            } else {
                targets
            };
        // A ranked dispatch actually happened: remember its node order so
        // later retained re-dispatches reproduce the identical mapping.
        if operation == "job_start" && job.node_ids.is_empty() && !retention_won {
            self.placement_order
                .write()
                .await
                .insert(job.job_id.clone(), targets.clone());
        }
        let target_ids = targets.iter().cloned().collect::<BTreeSet<_>>();
        if operation == "job_start" {
            // Auto-placement fencing: when the reconciler re-places a Job
            // (its previous placement lost its lease or was partitioned), the
            // abandoned node's Succeeded start at THIS generation still
            // claims the assignment. Without invalidating it, the node is
            // deduped back into the sticky target set on its return and both
            // nodes run the same Job forever. Mark those starts Superseded so
            // the placement history stops claiming them and the nodes receive
            // a stop command when they reappear.
            let abandoned: Vec<HubOperation> = {
                let operations = self.operations.read().await;
                operations
                    .values()
                    .filter(|operation_record| {
                        operation_record.resource_id == job.job_id
                            && operation_record.operation == "job_start"
                            && operation_record.generation == job.generation
                            && operation_record.state == HubOperationState::Succeeded
                            && !target_ids.contains(&operation_record.node_id)
                    })
                    .cloned()
                    .collect()
            };
            if !abandoned.is_empty() {
                // Apply the in-memory transition under the write lock, then
                // persist OUTSIDE it: the storage round-trips are async and
                // holding the operations write lock across them would block
                // every agent poll, report, and command result for the
                // duration of the I/O.
                let mut superseded = Vec::with_capacity(abandoned.len());
                {
                    let mut operations = self.operations.write().await;
                    for mut record in abandoned {
                        record.state = HubOperationState::Superseded;
                        record.superseded_generation = Some(job.generation);
                        operations.insert(record.id.clone(), record.clone());
                        superseded.push(record);
                    }
                }
                if let Some(storage) = self.storage.as_ref() {
                    for record in &superseded {
                        persist_operation(storage, record)
                            .await
                            .map_err(HubError::from)?;
                    }
                }
            }
            // A target that is still valid for the new generation does not
            // need a stop/start bounce. Every historical placement outside
            // the desired set is stale and must be fenced, including starts
            // recorded under an older generation.
            let nodes_to_stop = historical_nodes.difference(&target_ids).collect::<Vec<_>>();
            for node_id in nodes_to_stop {
                // The same terminal-state memory as the dispatch loop: a
                // Succeeded stop for this (node, job, generation) already
                // fenced the abandoned placement; re-enqueuing it every tick
                // would churn persistent operation rows.
                let stop_settled = self
                    .operations
                    .read()
                    .await
                    .values()
                    .any(|operation_record| {
                        operation_record.node_id == *node_id
                            && operation_record.resource_id == job.job_id
                            && operation_record.operation == "job_stop"
                            && operation_record.generation == job.generation
                            && operation_record.state == HubOperationState::Succeeded
                    });
                if stop_settled {
                    continue;
                }
                let is_online = self.nodes.read().await.get(node_id).is_some_and(|node| {
                    node.resource.state == NodeConnectionState::Online
                        && node.resource.lease_expires_at_ms > now_ms()
                        && node.resource.maintenance_state == NodeMaintenanceState::Active
                });
                if is_online {
                    self.enqueue_with_metadata(
                        node_id.clone(),
                        "job_stop".into(),
                        job.job_id.clone(),
                        None,
                        Some(serde_json::json!({"job_id": job.job_id})),
                        job.generation,
                        None,
                        None,
                        None,
                        None,
                        None,
                    )
                    .await?;
                }
            }
        }
        let assignments = plan
            .assignments_for_nodes(&targets, job.generation)
            .map_err(|error| HubError::Invalid(error.to_string()))?;
        // Re-check the complete task→node map at the dispatch boundary.  The
        // planner already validates it, but keeping this guard here prevents a
        // future assignment source or persistence replay from bypassing the
        // side-edge co-location contract.
        plan.validate_side_edge_assignments(&assignments)
            .map_err(|error| HubError::Invalid(error.to_string()))?;
        // Split placement: validate that every target node runs the data
        // plane, then attach the full task→node map and peer data addresses
        // so each node's graph build can wire its remote edges without any
        // further lookup.
        let mut split_payload = None;
        if spec.placement == arkflow_core::job::PlacementStrategy::Split {
            let nodes = self.nodes.read().await;
            let mut node_data_ports = BTreeMap::new();
            for node_id in &targets {
                let Some(record) = nodes.get(node_id) else {
                    return Err(HubError::Invalid(format!(
                        "split placement target node '{node_id}' is not registered"
                    )));
                };
                if !record
                    .resource
                    .capabilities
                    .iter()
                    .any(|capability| capability == "network_shuffle")
                {
                    return Err(HubError::Invalid(format!(
                        "split placement requires node '{node_id}' with the network_shuffle capability"
                    )));
                }
                let Some(address) = &record.resource.data_address else {
                    return Err(HubError::Invalid(format!(
                        "split placement requires node '{node_id}' to advertise a data address"
                    )));
                };
                node_data_ports.insert(node_id.clone(), address.clone());
            }
            drop(nodes);
            let mut task_nodes = BTreeMap::new();
            for assignment in &assignments {
                task_nodes.insert(assignment.task_id.clone(), assignment.node_id.clone());
            }
            split_payload = Some(serde_json::json!({
                "task_nodes": task_nodes,
                "node_data_ports": node_data_ports,
            }));
        }
        let explicit_recovery_id = job.checkpoint_id.clone();
        let mut recovery_candidates = self
            .job_checkpoints(&job.job_id)
            .await?
            .into_iter()
            .filter(|record| record.status == "completed")
            .filter(|record| match explicit_recovery_id.as_deref() {
                Some(requested) => {
                    record.checkpoint_id == requested
                        && recovery_record_is_compatible(&spec, record)
                }
                None => recovery_record_is_compatible(&spec, record),
            })
            .filter(|record| match spec.recovery {
                arkflow_core::job::RecoveryPolicy::LatestCheckpoint => record.kind == "checkpoint",
                arkflow_core::job::RecoveryPolicy::LatestSavepoint => record.kind == "savepoint",
                arkflow_core::job::RecoveryPolicy::Fail => false,
            })
            .collect::<Vec<_>>();
        recovery_candidates.sort_by(|left, right| {
            right
                .created_at_ms
                .cmp(&left.created_at_ms)
                .then_with(|| right.checkpoint_id.cmp(&left.checkpoint_id))
        });
        let recovery = recovery_candidates
            .into_iter()
            .find(|record| recovery_record_is_valid(&spec, record))
            .map(|record| {
                serde_json::json!({
                    "checkpoint_id": record.checkpoint_id,
                    "savepoint": record.kind == "savepoint",
                })
            });
        if recovery_required && recovery.is_none() {
            return Err(HubError::Invalid(format!(
                "durable Job '{}' requires recovery, but no compatible completed checkpoint is available",
                job.job_id
            )));
        }
        let mut dispatched = 0;
        for node_id in targets {
            // Terminal-state memory: a Succeeded lifecycle operation for THIS
            // (node, job, generation) already satisfies the desired state.
            // Without this skip, every reconcile tick re-enqueued the command
            // and a fresh persistent operation row — an unbounded churn loop
            // for stopped Jobs (and for fenced placements) with no retention
            // able to keep up. A generation bump or desired-state change
            // re-dispatches naturally.
            let already_terminal = self
                .operations
                .read()
                .await
                .values()
                .any(|operation_record| {
                    operation_record.node_id == node_id
                        && operation_record.resource_id == job.job_id
                        && operation_record.operation == operation
                        && operation_record.generation == job.generation
                        && operation_record.state == HubOperationState::Succeeded
                });
            if already_terminal {
                continue;
            }
            let node_assignments = assignments
                .iter()
                .filter(|assignment| assignment.node_id == node_id)
                .cloned()
                .collect::<Vec<_>>();
            if operation == "job_start" && node_assignments.is_empty() {
                continue;
            }
            let mut payload_value = serde_json::json!({
                "job_id": job.job_id,
                "spec": spec,
                "plan": plan,
                "assignments": node_assignments,
                "generation": job.generation,
                "recovery": recovery,
                "recovery_required": recovery_required,
            });
            if let (Some(base), Some(extra)) = (
                payload_value.as_object_mut(),
                split_payload.as_ref().and_then(|extra| extra.as_object()),
            ) {
                for (key, value) in extra {
                    base.insert(key.clone(), value.clone());
                }
            }
            let payload = Some(payload_value);
            self.enqueue_with_metadata(
                node_id,
                operation.into(),
                job.job_id.clone(),
                None,
                payload.clone(),
                job.generation,
                None,
                None,
                None,
                None,
                None,
            )
            .await?;
            dispatched += 1;
        }
        Ok(dispatched)
    }

    pub async fn update_job(
        &self,
        job_id: &str,
        desired_state: Option<&str>,
        generation: Option<u64>,
    ) -> Result<Option<JobRecord>, HubError> {
        let updated = if let Some(storage) = &self.storage {
            storage
                .update_job(
                    job_id,
                    desired_state.map(str::to_owned),
                    None,
                    None,
                    generation,
                    None,
                    None,
                )
                .await
                .map_err(HubError::from)?
        } else {
            let mut jobs = self.jobs.write().await;
            let Some(job) = jobs.get_mut(job_id) else {
                return Ok(None);
            };
            if let Some(desired_state) = desired_state {
                job.desired_state = desired_state.into();
            }
            if let Some(generation) = generation {
                job.generation = generation;
            }
            job.updated_at_ms = now_ms();
            Some(job.clone())
        };
        if let Some(job) = &updated {
            self.jobs
                .write()
                .await
                .insert(job.job_id.clone(), job.clone());
            self.reconcile_job(job).await?;
        }
        Ok(updated)
    }

    pub async fn update_job_desired_state(
        &self,
        job_id: &str,
        desired_state: &str,
        expected_generation: u64,
    ) -> Result<Option<JobRecord>, HubError> {
        let updated = if let Some(storage) = &self.storage {
            storage
                .update_job_desired_state(job_id, desired_state, expected_generation)
                .await
                .map_err(HubError::from)?
        } else {
            let mut jobs = self.jobs.write().await;
            let Some(job) = jobs.get_mut(job_id) else {
                return Ok(None);
            };
            if job.generation != expected_generation {
                return Err(HubError::GenerationConflict {
                    expected: expected_generation,
                    current: job.generation,
                });
            }
            job.desired_state = desired_state.into();
            job.convergence = "reconciling".into();
            job.generation = expected_generation.saturating_add(1);
            job.updated_at_ms = now_ms();
            Some(job.clone())
        };
        if let Some(job) = &updated {
            self.jobs
                .write()
                .await
                .insert(job.job_id.clone(), job.clone());
            self.reconcile_job(job).await?;
        }
        Ok(updated)
    }

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
        self.observe_job(
            &request.job_id,
            request.generation,
            &request.state,
            None,
            request.error.as_deref(),
        )
        .await
    }

    pub async fn record_job_checkpoint(
        &self,
        record: JobCheckpointRecord,
    ) -> Result<Option<JobRecord>, HubError> {
        let record_for_dispatch = record.clone();
        // A checkpoint produced by a different Job deployment (version) must
        // never repoint the live record's recovery pointer: the artifact row
        // is kept for audit, but recovery keeps its current selection.
        let version_matches = self
            .job(&record.job_id)
            .await
            .map(|job| job.is_some_and(|job| job.version == record.job_version))?;
        if let Some(storage) = &self.storage {
            storage
                .upsert_job_checkpoint(record.clone())
                .await
                .map_err(HubError::from)?;
            let job = if version_matches {
                storage
                    .update_job(
                        &record.job_id,
                        None,
                        None,
                        None,
                        None,
                        Some(record.checkpoint_id.clone()),
                        None,
                    )
                    .await
                    .map_err(HubError::from)?
            } else {
                self.job(&record.job_id).await.map_err(HubError::from)?
            };
            if let Some(job) = &job {
                self.jobs
                    .write()
                    .await
                    .insert(job.job_id.clone(), job.clone());
                self.job_checkpoints
                    .write()
                    .await
                    .entry(record.job_id.clone())
                    .or_default()
                    .retain(|existing| existing.checkpoint_id != record.checkpoint_id);
                self.job_checkpoints
                    .write()
                    .await
                    .entry(record.job_id.clone())
                    .or_default()
                    .push(record.clone());
                self.dispatch_job_artifact(job, &record_for_dispatch)
                    .await?;
                self.enforce_checkpoint_retention_for_job(&record.job_id)
                    .await?;
            }
            return Ok(job);
        }
        let mut jobs = self.jobs.write().await;
        let Some(job) = jobs.get_mut(&record.job_id) else {
            return Ok(None);
        };
        if version_matches {
            job.checkpoint_id = Some(record.checkpoint_id.clone());
        }
        job.updated_at_ms = now_ms();
        let result = job.clone();
        drop(jobs);
        self.job_checkpoints
            .write()
            .await
            .entry(record.job_id.clone())
            .or_default()
            .retain(|existing| existing.checkpoint_id != record.checkpoint_id);
        self.job_checkpoints
            .write()
            .await
            .entry(record.job_id.clone())
            .or_default()
            .push(record);
        self.dispatch_job_artifact(&result, &record_for_dispatch)
            .await?;
        self.enforce_checkpoint_retention_for_job(&record_for_dispatch.job_id)
            .await?;
        Ok(Some(result))
    }

    async fn dispatch_job_artifact(
        &self,
        job: &JobRecord,
        record: &JobCheckpointRecord,
    ) -> Result<usize, HubError> {
        let spec: arkflow_core::job::JobSpec = serde_json::from_str(&job.spec_json)
            .map_err(|error| HubError::Invalid(format!("invalid persisted Job spec: {error}")))?;
        let plan = arkflow_core::job::JobPlan::compile(spec.clone())
            .map_err(|error| HubError::Invalid(error.to_string()))?;
        let candidates = if job.node_ids.is_empty() {
            self.operations
                .read()
                .await
                .values()
                .filter(|operation| {
                    operation.resource_id == job.job_id
                        && operation.operation == "job_start"
                        && operation.generation == job.generation
                        && matches!(
                            operation.state,
                            HubOperationState::Queued
                                | HubOperationState::Dispatched
                                | HubOperationState::Acknowledged
                                | HubOperationState::Running
                                | HubOperationState::Succeeded
                        )
                })
                .map(|operation| operation.node_id.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
        } else {
            job.node_ids.clone()
        };
        let nodes = self.nodes.read().await;
        let targets = candidates
            .into_iter()
            .filter(|node_id| {
                nodes.get(node_id).is_some_and(|node| {
                    node.resource.state == NodeConnectionState::Online
                        && node.resource.lease_expires_at_ms > now_ms()
                })
            })
            .collect::<Vec<_>>();
        drop(nodes);
        let assignments = plan
            .assignments_for_nodes(&targets, job.generation)
            .map_err(|error| HubError::Invalid(error.to_string()))?;
        let operation = if record.kind == "savepoint" {
            "job_savepoint"
        } else {
            "job_checkpoint"
        };
        let mut dispatched = 0;
        for node_id in targets {
            let node_assignments = assignments
                .iter()
                .filter(|assignment| assignment.node_id == node_id)
                .cloned()
                .collect::<Vec<_>>();
            if node_assignments.is_empty() {
                continue;
            }
            self.enqueue_with_metadata(
                node_id,
                operation.into(),
                job.job_id.clone(),
                None,
                Some(serde_json::json!({
                    "job_id": job.job_id,
                    "plan": plan,
                    "assignments": node_assignments,
                    "checkpoint_id": record.checkpoint_id,
                })),
                job.generation,
                None,
                None,
                None,
                None,
                None,
            )
            .await?;
            dispatched += 1;
        }
        Ok(dispatched)
    }

    pub async fn complete_job_checkpoint(
        &self,
        job_id: &str,
        checkpoint_id: &str,
        status: &str,
        manifest_uri: Option<String>,
    ) -> Result<(), HubError> {
        let kind = if checkpoint_id.starts_with("savepoint-") {
            "savepoint"
        } else {
            "checkpoint"
        };
        let mut record = self
            .job_checkpoints
            .read()
            .await
            .get(job_id)
            .and_then(|records| {
                records
                    .iter()
                    .find(|record| record.checkpoint_id == checkpoint_id)
                    .cloned()
            });
        if record.is_none() {
            if let Some(storage) = &self.storage {
                record = storage
                    .list_job_checkpoints(job_id)
                    .await
                    .map_err(HubError::from)?
                    .into_iter()
                    .find(|record| record.checkpoint_id == checkpoint_id);
            }
        }
        let record = if let Some(mut record) = record {
            record.status = status.into();
            record.manifest_uri = manifest_uri;
            record.updated_at_ms = now_ms();
            record
        } else {
            let job = self.job(job_id).await?;
            let (job_version, format_version) = job
                .as_ref()
                .and_then(|job| {
                    serde_json::from_str::<arkflow_core::job::JobSpec>(&job.spec_json)
                        .ok()
                        .map(|spec| (spec.version.0, job_state_format_version(&spec)))
                })
                .unwrap_or((0, 1));
            JobCheckpointRecord {
                job_id: job_id.into(),
                job_version,
                checkpoint_id: checkpoint_id.into(),
                kind: kind.into(),
                status: status.into(),
                manifest_uri,
                format_version,
                created_at_ms: now_ms(),
                updated_at_ms: now_ms(),
            }
        };
        if let Some(storage) = &self.storage {
            storage
                .upsert_job_checkpoint(record.clone())
                .await
                .map_err(HubError::from)?;
        }
        let mut records = self.job_checkpoints.write().await;
        records
            .entry(job_id.into())
            .or_default()
            .retain(|existing| existing.checkpoint_id != checkpoint_id);
        records.entry(job_id.into()).or_default().push(record);
        drop(records);
        self.enforce_checkpoint_retention_for_job(job_id).await?;
        Ok(())
    }

    pub async fn job_checkpoints(
        &self,
        job_id: &str,
    ) -> Result<Vec<JobCheckpointRecord>, HubError> {
        let Some(storage) = &self.storage else {
            return Ok(self
                .job_checkpoints
                .read()
                .await
                .get(job_id)
                .cloned()
                .unwrap_or_default());
        };
        storage
            .list_job_checkpoints(job_id)
            .await
            .map_err(HubError::from)
    }

    /// Enqueue periodic checkpoints for running Jobs whose configured interval
    /// has elapsed. Scheduling lives in the Hub so the normal checkpoint
    /// aggregation and fencing path is used for every Agent.
    pub async fn schedule_periodic_checkpoints(&self) -> Result<usize, HubError> {
        let now = now_ms();
        let mut scheduled = 0;
        for job in self.jobs().await? {
            if job.desired_state != "running" {
                continue;
            }
            let spec: arkflow_core::job::JobSpec =
                serde_json::from_str(&job.spec_json).map_err(|error| {
                    HubError::Invalid(format!("invalid persisted Job spec: {error}"))
                })?;
            let Some(checkpoint) = spec.checkpoint.as_ref() else {
                continue;
            };
            if checkpoint.interval_ms == 0 {
                continue;
            }
            let records = self.job_checkpoints(&job.job_id).await?;
            let last_attempt = records.iter().map(|record| record.created_at_ms).max();
            if last_attempt
                .is_some_and(|created| now.saturating_sub(created) < checkpoint.interval_ms)
            {
                continue;
            }
            let checkpoint_id = format!("checkpoint-{}-{}-{}", job.job_id, job.generation, now);
            let record = JobCheckpointRecord {
                job_id: job.job_id.clone(),
                job_version: spec.version.0,
                checkpoint_id,
                kind: "checkpoint".into(),
                status: "pending".into(),
                manifest_uri: None,
                format_version: job_state_format_version(&spec),
                created_at_ms: now,
                updated_at_ms: now,
            };
            self.record_job_checkpoint(record).await?;
            scheduled += 1;
        }
        Ok(scheduled)
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

    /// Prometheus accounting for command dispatch. Counters reset on Hub
    /// restart in line with counter semantics.
    pub fn command_metrics(&self) -> &CommandMetrics {
        &self.command_metrics
    }

    /// Expire queued/dispatched `job_start`/`job_stop` operations whose
    /// delivery window passed. Each expiry increments the retry count; once
    /// the count reaches `MAX_JOB_OPERATION_RETRIES` the operation settles
    /// as terminal `failed`/`expired` and reconciliation stops re-enqueueing
    /// it. The undeliverable command is dropped so a late Agent poll cannot
    /// execute it, and the transition is persisted so a Hub restart sees the
    /// same state. Expiry marks the operation terminal-but-retriable: the
    /// next `reconcile_jobs` tick re-enqueues it with the inherited retry
    /// count. Checkpoint/savepoint triggers are deliberately out of scope:
    /// their retry path is the poll handler's expired-command re-enqueue,
    /// which replays the original payload — expiring them here would drop
    /// the trigger instead.
    pub async fn expire_stale_job_operations(&self) -> Result<usize, HubError> {
        let now = now_ms();
        let expired: Vec<String> = {
            let operations = self.operations.read().await;
            operations
                .values()
                .filter(|record| {
                    matches!(record.operation.as_str(), "job_start" | "job_stop")
                        && matches!(
                            record.state,
                            HubOperationState::Queued | HubOperationState::Dispatched
                        )
                        && record.expires_at_ms.is_some_and(|expires| expires <= now)
                })
                .map(|record| record.id.clone())
                .collect()
        };
        if expired.is_empty() {
            return Ok(0);
        }
        let mut mutated: Vec<HubOperation> = Vec::with_capacity(expired.len());
        {
            let mut operations = self.operations.write().await;
            for id in expired {
                let Some(record) = operations.get_mut(&id) else {
                    continue;
                };
                // Re-check under the write lock: a command result may have
                // settled the operation between the scan and this transition.
                if !matches!(
                    record.state,
                    HubOperationState::Queued | HubOperationState::Dispatched
                ) || !record.expires_at_ms.is_some_and(|expires| expires <= now)
                {
                    continue;
                }
                record.retry_count += 1;
                record.failure_class = Some("expired".into());
                record.next_retry_at_ms = Some(now);
                if record.retry_count >= MAX_JOB_OPERATION_RETRIES {
                    record.state = HubOperationState::Failed;
                } else {
                    record.state = HubOperationState::TimedOut;
                }
                mutated.push(record.clone());
            }
        }
        if mutated.is_empty() {
            return Ok(0);
        }
        // Drop the undeliverable commands. The operations lock must be
        // released first: enqueue takes the nodes lock before the operations
        // lock, so the reverse order here could deadlock against it.
        {
            let mut nodes = self.nodes.write().await;
            for record in &mutated {
                if let Some(node) = nodes.get_mut(&record.node_id) {
                    node.commands
                        .retain(|command| command.operation_id != record.id);
                    node.leased_commands.remove(&record.command_id);
                }
            }
        }
        if let Some(storage) = self.storage.as_ref() {
            for record in &mutated {
                persist_operation(storage, record)
                    .await
                    .map_err(HubError::from)?;
            }
        }
        for record in &mutated {
            self.command_metrics.record_outcome(
                &record.operation,
                CommandMetrics::outcome_label(record.state).unwrap_or("failed"),
            );
        }
        Ok(mutated.len())
    }

    /// Reclaim audit history past the retention window and count bound so
    /// the trail stays bounded without losing recent records.
    pub async fn prune_audit_history(&self) -> Result<(), HubError> {
        const RETENTION_MS: i64 = 30 * 24 * 60 * 60 * 1000;
        const RETENTION_MAX: i64 = 100_000;
        if let Some(storage) = &self.storage {
            storage
                .prune_audit_events(now_ms() as i64 - RETENTION_MS, RETENTION_MAX)
                .await
                .map_err(HubError::from)?;
        }
        Ok(())
    }

    /// Bounded retention for the operation history. The reconciler's
    /// terminal-state memory reads Succeeded/terminal rows, so records are
    /// kept for a grace window and a count bound instead of living forever:
    /// without this, every reconcile tick of a stopped Job (or a fenced
    /// placement) appended a fresh persistent row with no reclaim. Pruning a
    /// terminal record past the window can cause one idempotent
    /// re-dispatch, which is bounded and safe.
    pub async fn prune_operation_history(&self) -> Result<(), HubError> {
        const RETENTION_MS: i64 = 24 * 60 * 60 * 1000;
        const RETENTION_MAX: i64 = 4096;
        let cutoff = now_ms() as i64 - RETENTION_MS;
        if let Some(storage) = &self.storage {
            storage
                .prune_operation_history(cutoff, RETENTION_MAX)
                .await
                .map_err(HubError::from)?;
        }
        let mut operations = self.operations.write().await;
        let terminal = |state: &HubOperationState| {
            matches!(
                state,
                HubOperationState::Succeeded
                    | HubOperationState::Failed
                    | HubOperationState::TimedOut
                    | HubOperationState::NodeUnavailable
                    | HubOperationState::Cancelled
                    | HubOperationState::Superseded
            )
        };
        let mut protected_starts = BTreeMap::<String, (u64, u64, String)>::new();
        for (id, record) in operations.iter() {
            if !is_durable_job_start(record) {
                continue;
            }
            let candidate = (record.generation, record.created_at_ms, id.clone());
            let replace = protected_starts
                .get(&record.resource_id)
                .is_none_or(|current| candidate > *current);
            if replace {
                protected_starts.insert(record.resource_id.clone(), candidate);
            }
        }
        let protected_ids = protected_starts
            .into_values()
            .map(|(_, _, id)| id)
            .collect::<BTreeSet<_>>();
        let stale: Vec<String> = operations
            .iter()
            .filter(|(_, record)| {
                terminal(&record.state)
                    && !protected_ids.contains(&record.id)
                    && ((record.finished_at_ms.unwrap_or(record.created_at_ms)) as i64) < cutoff
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in stale {
            operations.remove(&id);
        }
        let mut terminal_ids: Vec<(i64, String)> = operations
            .iter()
            .filter(|(_, record)| terminal(&record.state) && !protected_ids.contains(&record.id))
            .map(|(id, record)| {
                (
                    (record.finished_at_ms.unwrap_or(record.created_at_ms)) as i64,
                    id.clone(),
                )
            })
            .collect();
        terminal_ids.sort();
        let excess = terminal_ids.len().saturating_sub(RETENTION_MAX as usize);
        for (_, id) in terminal_ids.into_iter().take(excess) {
            operations.remove(&id);
        }
        Ok(())
    }

    /// Reclaim processed reconciliation outbox rows so the durable outbox
    /// stays bounded under steady reconcile churn. Unprocessed rows — the
    /// outstanding work queue — are never reclaimed, and the
    /// `outbox_pending`/`outbox_claimed` status counters are unaffected.
    pub async fn prune_outbox_history(&self) -> Result<(), HubError> {
        const RETENTION_MS: i64 = 24 * 60 * 60 * 1000;
        const RETENTION_MAX: i64 = 4096;
        if let Some(storage) = &self.storage {
            storage
                .prune_processed_outbox(now_ms() as i64 - RETENTION_MS, RETENTION_MAX)
                .await
                .map_err(HubError::from)?;
        }
        Ok(())
    }

    /// Reclaim terminal Attempt records so the durable attempt store stays
    /// bounded under steady dispatch churn. Active attempts are never
    /// reclaimed.
    pub async fn prune_attempt_history(&self) -> Result<(), HubError> {
        const RETENTION_MS: i64 = 24 * 60 * 60 * 1000;
        const RETENTION_MAX: i64 = 4096;
        if let Some(storage) = &self.storage {
            storage
                .prune_terminal_attempts(now_ms() as i64 - RETENTION_MS, RETENTION_MAX)
                .await
                .map_err(HubError::from)?;
        }
        Ok(())
    }

    /// Reclaim pending/failed checkpoint attempt records older than the
    /// retention window. Completed records are governed by the per-Job
    /// checkpoint retention policy; pending/failed rows used to accumulate
    /// forever whenever an Agent could not finish a round.
    pub async fn prune_stale_checkpoint_records(&self) -> Result<(), HubError> {
        const RETENTION_MS: i64 = 24 * 60 * 60 * 1000;
        let cutoff = now_ms() as i64 - RETENTION_MS;
        if let Some(storage) = &self.storage {
            storage
                .prune_job_checkpoint_records(cutoff)
                .await
                .map_err(HubError::from)?;
        }
        let stale: Vec<(String, String)> = self
            .job_checkpoints
            .read()
            .await
            .iter()
            .flat_map(|(job_id, records)| {
                records
                    .iter()
                    .filter(|record| {
                        (record.updated_at_ms as i64) < cutoff
                            && matches!(record.status.as_str(), "pending" | "failed")
                    })
                    .map(|record| (job_id.clone(), record.checkpoint_id.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        if stale.is_empty() {
            return Ok(());
        }
        let mut checkpoints = self.job_checkpoints.write().await;
        for (job_id, checkpoint_id) in stale {
            if let Some(records) = checkpoints.get_mut(&job_id) {
                records.retain(|record| record.checkpoint_id != checkpoint_id);
            }
        }
        Ok(())
    }

    async fn enforce_checkpoint_retention(
        &self,
        job: &JobRecord,
        spec: &arkflow_core::job::JobSpec,
    ) -> Result<(), HubError> {
        let retention = spec
            .checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.retention as usize)
            .unwrap_or(0);
        if retention == 0 {
            return Ok(());
        }
        let mut completed = self
            .job_checkpoints(&job.job_id)
            .await?
            .into_iter()
            .filter(|record| record.kind == "checkpoint" && record.status == "completed")
            .collect::<Vec<_>>();
        completed.sort_by(|left, right| {
            right
                .created_at_ms
                .cmp(&left.created_at_ms)
                .then_with(|| right.checkpoint_id.cmp(&left.checkpoint_id))
        });
        for record in completed.into_iter().skip(retention) {
            let artifact = arkflow_core::checkpoint::RecoveryArtifact {
                id: record.checkpoint_id.clone(),
                kind: arkflow_core::checkpoint::RecoveryArtifactKind::Checkpoint,
                manifest_key: arkflow_core::checkpoint::recovery_manifest_key(
                    arkflow_core::checkpoint::RecoveryArtifactKind::Checkpoint,
                    &record.checkpoint_id,
                ),
                job_version: spec.version,
                format_version: record.format_version,
                created_at_ms: record.created_at_ms,
                status: arkflow_core::checkpoint::CheckpointStatus::Completed,
            };
            // The artifact delete performs blocking object-store I/O; keep it
            // off the async runtime's worker threads (this runs inside
            // reconciliation and request handling).
            let spec_for_delete = spec.clone();
            tokio::task::spawn_blocking(move || {
                delete_checkpoint_artifact(&spec_for_delete, &artifact).map_err(HubError::Invalid)
            })
            .await
            .map_err(|error| {
                HubError::Invalid(format!("checkpoint retention task failed: {error}"))
            })??;
            if let Some(storage) = &self.storage {
                storage
                    .delete_job_checkpoint(&record.job_id, &record.checkpoint_id)
                    .await
                    .map_err(HubError::from)?;
            } else {
                self.job_checkpoints
                    .write()
                    .await
                    .entry(record.job_id.clone())
                    .or_default()
                    .retain(|candidate| candidate.checkpoint_id != record.checkpoint_id);
            }
        }
        Ok(())
    }

    async fn enforce_checkpoint_retention_for_job(&self, job_id: &str) -> Result<(), HubError> {
        let Some(job) = self.job(job_id).await? else {
            return Ok(());
        };
        let spec: arkflow_core::job::JobSpec = serde_json::from_str(&job.spec_json)
            .map_err(|error| HubError::Invalid(format!("invalid persisted Job spec: {error}")))?;
        self.enforce_checkpoint_retention(&job, &spec).await
    }

    pub fn subscribe(&self) -> broadcast::Receiver<HubEvent> {
        self.updates.subscribe()
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
        self.operator_principal(supplied).is_some()
    }

    pub fn operator_principal(&self, supplied: Option<&str>) -> Option<OperatorPrincipal> {
        let Some(expected) = self.config.operator_token.as_deref() else {
            return self
                .config
                .insecure_local
                .then(OperatorPrincipal::legacy_operator);
        };
        if expected.trim().is_empty() {
            return None;
        }
        let (id, role, secret, scopes) = parse_operator_credential(expected);
        let supplied = supplied?;
        if !bool::from(supplied.as_bytes().ct_eq(secret.as_bytes())) {
            return None;
        }
        Some(OperatorPrincipal {
            id: id.to_owned(),
            roles: vec![role],
            scopes,
        })
    }

    pub fn operator_can(&self, supplied: Option<&str>, action: OperatorAction) -> bool {
        self.operator_principal(supplied)
            .is_some_and(|principal| principal.can(action))
    }

    pub fn operator_can_scope(
        &self,
        supplied: Option<&str>,
        action: OperatorAction,
        resource_type: &str,
        resource_id: Option<&str>,
    ) -> bool {
        self.operator_principal(supplied)
            .is_some_and(|principal| principal.can_scope(action, resource_type, resource_id))
    }

    pub async fn register(&self, request: RegisterRequest) -> Result<RegisterResponse, HubError> {
        let Some(expected) = self.config.node_token.as_deref() else {
            if !self.config.insecure_local {
                return Err(HubError::Unauthorized);
            }
            // Explicit loopback development mode may omit the node token.
            // The standalone server refuses to expose this mode externally.
            let _ = &request.node_token;
            return self.register_after_auth(request).await;
        };
        if expected.trim().is_empty() {
            return Err(HubError::Unauthorized);
        }
        if !bool::from(request.node_token.as_bytes().ct_eq(expected.as_bytes())) {
            return Err(HubError::Unauthorized);
        }
        self.register_after_auth(request).await
    }

    async fn register_after_auth(
        &self,
        request: RegisterRequest,
    ) -> Result<RegisterResponse, HubError> {
        if request.node_id.trim().is_empty() {
            return Err(HubError::Invalid("node_id must not be empty".into()));
        }
        if request.protocol_version != SUPPORTED_PROTOCOL_VERSION {
            let message = format!("unsupported protocol version: {}", request.protocol_version);
            let _ = self
                .record_audit_event(crate::storage::AuditRecord {
                    event_id: 0,
                    actor: None,
                    action: "agent.register".into(),
                    resource_type: "node".into(),
                    resource_id: Some(request.node_id.clone()),
                    node_id: Some(request.node_id.clone()),
                    stream_id: None,
                    correlation_id: None,
                    outcome: "rejected".into(),
                    failure_code: Some("incompatible_protocol".into()),
                    message: Some(message.clone()),
                    occurred_at_ms: now_ms(),
                })
                .await;
            return Err(HubError::Invalid(message));
        }
        let now = now_ms();
        // Session tokens authenticate every agent request after registration,
        // so they MUST come from a CSPRNG: a sequential counter would be
        // enumerable by anyone who can reach the Hub and defeat the
        // constant-time comparisons downstream.
        let session_token: String = {
            use rand::TryRngCore;
            let mut bytes = [0u8; 32];
            rand::rngs::OsRng
                .try_fill_bytes(&mut bytes)
                .expect("OS RNG cannot fail");
            bytes.iter().map(|byte| format!("{byte:02x}")).collect()
        };
        // Older clients do not send a process identity. Keep them compatible
        // by treating the fresh session token as their boot identity; the
        // built-in Agent sends its stable `NodeAgentConfig::boot_id`.
        let registered_boot_id = request
            .boot_id
            .clone()
            .filter(|boot_id| !boot_id.trim().is_empty())
            .unwrap_or_else(|| session_token.clone());
        let resource = HubNode {
            id: request.node_id.clone(),
            protocol_version: request.protocol_version.clone(),
            version: "unknown".into(),
            state: NodeConnectionState::Online,
            capabilities: sanitize_capabilities(request.capabilities.clone()),
            last_seen_at_ms: now,
            lease_expires_at_ms: now + self.config.lease_ttl_ms,
            streams_total: 0,
            streams_running: 0,
            streams_failed: 0,
            maintenance_state: NodeMaintenanceState::Active,
            data_address: request
                .data_address
                .clone()
                .filter(|address| !address.trim().is_empty()),
        };
        let mut nodes = self.nodes.write().await;
        if nodes.len() >= MAX_NODES && !nodes.contains_key(&request.node_id) {
            return Err(HubError::Capacity);
        }
        let old = nodes.remove(&request.node_id);
        let boot_changed = old
            .as_ref()
            .is_some_and(|record| record.boot_id.as_deref() != Some(registered_boot_id.as_str()));
        nodes.insert(
            request.node_id.clone(),
            NodeRecord {
                resource,
                session_token: session_token.clone(),
                session_expires_at_ms: now.saturating_add(self.config.session_ttl_ms),
                boot_id: Some(registered_boot_id.clone()),
                report_seq: 0,
                last_report_at_ms: 0,
                // Pressure history is deliberately not carried across
                // registrations: a reconnecting node re-earns its streak
                // within a few report intervals (bounded rebalance delay).
                pressure_streak: 0,
                // Commands queued for an old process belong to a runtime that
                // no longer exists. Reconciliation below will enqueue the
                // desired state for the new boot.
                commands: if boot_changed {
                    VecDeque::new()
                } else {
                    old.as_ref()
                        .map(|record| record.commands.clone())
                        .unwrap_or_default()
                },
                leased_commands: if boot_changed {
                    BTreeMap::new()
                } else {
                    old.as_ref()
                        .map(|record| record.leased_commands.clone())
                        .unwrap_or_default()
                },
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
                // A boot change invalidates the previous process's local Jobs
                // (their start operations are marked unavailable below), so
                // their metric snapshots must not survive the re-registration.
                jobs: if boot_changed {
                    BTreeMap::new()
                } else {
                    old.as_ref()
                        .map(|record| record.jobs.clone())
                        .unwrap_or_default()
                },
                metrics: old.map(|record| record.metrics).unwrap_or_default(),
            },
        );
        drop(nodes);
        let invalidated_job_starts = self
            .invalidate_job_starts_on_boot_change(&request.node_id, boot_changed, now)
            .await;
        if let Some(storage) = self.storage.as_ref() {
            storage
                .upsert_node(NodeMutation {
                    node_id: request.node_id.clone(),
                    version: "unknown".into(),
                    state: "online".into(),
                    capabilities_json: serde_json::to_string(&sanitize_capabilities(
                        request.capabilities,
                    ))
                    .unwrap_or_else(|_| "[]".into()),
                    boot_id: Some(registered_boot_id.clone()),
                    report_seq: Some(0),
                    last_seen_at_ms: now,
                    lease_expires_at_ms: now + self.config.lease_ttl_ms,
                    maintenance_state: None,
                    maintenance_updated_at_ms: None,
                })
                .await
                .map_err(HubError::from)?;
            // The Agent restarts report_seq from 1 on every session rebuild,
            // so the previous session's stored per-stream cursors would
            // silently drop every new observation. Reset them together with
            // the in-memory cursor above.
            storage
                .reset_observed_cursors(request.node_id.clone())
                .await
                .map_err(HubError::from)?;
            for operation in &invalidated_job_starts {
                persist_operation(storage, operation)
                    .await
                    .map_err(HubError::from)?;
            }
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
        for job in self.jobs().await? {
            if job.desired_state != "stopped"
                && (job.node_ids.is_empty() || job.node_ids.iter().any(|id| id == &request.node_id))
            {
                self.reconcile_job(&job).await?;
            }
        }
        Ok(RegisterResponse {
            node_id: request.node_id,
            session_token,
            session_ttl_ms: self.config.session_ttl_ms,
            lease_ttl_ms: self.config.lease_ttl_ms,
            poll_interval_ms: self.config.poll_interval_ms,
            protocol_version: default_protocol_version(),
        })
    }

    pub async fn heartbeat(&self, request: HeartbeatRequest) -> Result<(), HubError> {
        if let Some(protocol_version) = request.protocol_version.as_deref() {
            if protocol_version != SUPPORTED_PROTOCOL_VERSION {
                return Err(HubError::Invalid(format!(
                    "unsupported protocol version: {protocol_version}"
                )));
            }
        }
        let mut nodes = self.nodes.write().await;
        let node = authenticated_node(&mut nodes, &request.auth)?;
        let now = now_ms();
        node.resource.last_seen_at_ms = now;
        node.resource.lease_expires_at_ms = now + self.config.lease_ttl_ms;
        node.resource.state = match request.state.as_str() {
            "draining" => NodeConnectionState::Draining,
            _ => NodeConnectionState::Online,
        };
        if let Some(version) = request.software_version {
            node.resource.version = version;
        }
        if !request.capabilities.is_empty() {
            node.resource.capabilities = sanitize_capabilities(request.capabilities);
        }
        Ok(())
    }

    /// A fresh Agent process starts with an empty local JobRuntime. Mark every
    /// previous start attempt for this node unavailable before reconciliation,
    /// otherwise a persisted successful operation would suppress the new start
    /// command even though no local Job exists. Shared by registration and
    /// report so the invalidation semantics cannot drift between them.
    async fn invalidate_job_starts_on_boot_change(
        &self,
        node_id: &str,
        boot_changed: bool,
        now: u64,
    ) -> Vec<HubOperation> {
        if !boot_changed {
            return Vec::new();
        }
        let mut operations = self.operations.write().await;
        operations
            .values_mut()
            .filter(|operation| {
                operation.node_id == node_id
                    && operation.operation == "job_start"
                    && !matches!(
                        operation.state,
                        HubOperationState::Failed
                            | HubOperationState::TimedOut
                            | HubOperationState::NodeUnavailable
                            | HubOperationState::Cancelled
                            | HubOperationState::Superseded
                    )
            })
            .map(|operation| {
                let was_succeeded = operation.state == HubOperationState::Succeeded;
                operation.state = HubOperationState::NodeUnavailable;
                operation.finished_at_ms = Some(now);
                if was_succeeded {
                    operation.failure_class = Some("recovery_required".into());
                    operation.error = Some(
                        "previous successful Job start invalidated by a new Agent process boot"
                            .into(),
                    );
                } else {
                    operation.error =
                        Some("in-flight Job start invalidated by a new Agent process boot".into());
                }
                operation.clone()
            })
            .collect()
    }

    pub async fn report(&self, report: NodeReport) -> Result<(), HubError> {
        let reported_streams = report.streams.clone();
        let reported_configuration = report.configuration.clone();
        let mut nodes = self.nodes.write().await;
        let node = authenticated_node(&mut nodes, &report.auth)?;
        let boot_changed = report
            .boot_id
            .as_deref()
            .is_some_and(|boot_id| node.boot_id.as_deref() != Some(boot_id));
        if let Some(boot_id) = report.boot_id.as_deref() {
            match node.boot_id.as_deref() {
                Some(current) if current == boot_id => {
                    // Same session: the sequence cursor rejects replays.
                    if report.report_seq <= node.report_seq {
                        return Ok(());
                    }
                    node.report_seq = report.report_seq;
                }
                Some(_) => {
                    // A delayed report from an older session (the node has
                    // re-registered since): acknowledge without regressing
                    // the new session's observed state.
                    return Ok(());
                }
                None => {
                    node.boot_id = Some(boot_id.to_owned());
                    node.report_seq = report.report_seq;
                }
            }
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
        node.resource.capabilities = sanitize_capabilities(report.capabilities);
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
        node.metrics = sanitize_metrics(report.metrics);
        node.last_report_at_ms = now;
        node.pressure_streak = if node_under_pressure(&node.metrics) {
            node.pressure_streak.saturating_add(1)
        } else {
            0
        };
        node.jobs = bounded_job_snapshots(report.jobs);
        node.configuration = report.configuration;
        let persisted_version = node.resource.version.clone();
        let persisted_state = format!("{:?}", node.resource.state).to_lowercase();
        let persisted_capabilities =
            serde_json::to_string(&node.resource.capabilities).unwrap_or_else(|_| "[]".into());
        let persisted_boot_id = node.boot_id.clone();
        let persisted_report_seq = Some(node.report_seq);
        let persisted_lease = node.resource.lease_expires_at_ms;
        drop(nodes);
        let invalidated_job_starts = self
            .invalidate_job_starts_on_boot_change(&report.auth.node_id, boot_changed, now)
            .await;
        if let Some(storage) = self.storage.as_ref() {
            for operation in &invalidated_job_starts {
                persist_operation(storage, operation)
                    .await
                    .map_err(HubError::from)?;
            }
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
        for mut event in report.events {
            if events.len() >= MAX_EVENTS {
                events.pop_front();
            }
            event.message = event.message.map(|message| bounded_text(&message, 512));
            events.push_back(HubEvent {
                event_id: None,
                node_id: report.auth.node_id.clone(),
                event,
            });
            if let Some(event) = events.back().cloned() {
                let _ = self.updates.send(event);
            }
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
        if boot_changed {
            for job in self.jobs().await? {
                if job.desired_state != "stopped"
                    && (job.node_ids.is_empty()
                        || job.node_ids.iter().any(|id| id == &report.auth.node_id))
                {
                    self.reconcile_job(&job).await?;
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
        let mut expired = Vec::new();
        while let Some(command) = node.commands.pop_front() {
            if command.expires_at_ms > now {
                node.leased_commands
                    .insert(command.id.clone(), command.clone());
                commands.push(command);
            } else {
                expired.push(command);
            }
        }
        let expired_leases = node
            .leased_commands
            .iter()
            .filter(|(_, command)| command.expires_at_ms <= now)
            .map(|(command_id, _)| command_id.clone())
            .collect::<Vec<_>>();
        for command_id in expired_leases {
            if let Some(command) = node.leased_commands.remove(&command_id) {
                expired.push(command);
            }
        }
        drop(nodes);
        // A queued command is leased even before an Agent receives it. Do not
        // silently discard an expired lease while leaving its operation in an
        // active deduplication state: mark it retryable/terminal so the normal
        // Job reconciler can enqueue a fresh command.  Non-Job commands do
        // not have a desired-state reconciler, so retain enough of the
        // expired command to enqueue a replacement below.
        let mut expired_operations = Vec::new();
        let mut expired_job_ids = BTreeSet::new();
        let mut expired_retries = Vec::new();
        if !expired.is_empty() {
            let mut operations = self.operations.write().await;
            for command in &expired {
                if let Some(operation) = operations.get_mut(&command.operation_id) {
                    if matches!(
                        operation.state,
                        HubOperationState::Queued
                            | HubOperationState::Dispatched
                            | HubOperationState::Acknowledged
                            | HubOperationState::Running
                    ) {
                        operation.state = HubOperationState::TimedOut;
                        operation.finished_at_ms = Some(now);
                        operation.next_retry_at_ms = Some(now);
                        operation.error = Some("command lease expired before execution".into());
                        if matches!(operation.operation.as_str(), "job_start" | "job_stop") {
                            expired_job_ids.insert(operation.resource_id.clone());
                        } else {
                            expired_retries.push(command.clone());
                        }
                        expired_operations.push(operation.clone());
                    }
                }
            }
        }
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
            for operation in &expired_operations {
                persist_operation(storage, operation)
                    .await
                    .map_err(HubError::from)?;
            }
            for command in &commands {
                if let Some(attempt_id) = command.attempt_id.as_deref() {
                    storage
                        .mark_attempt_dispatched(attempt_id, command.expires_at_ms)
                        .await
                        .map_err(HubError::from)?;
                }
            }
        }
        // A command can expire after being queued, or after an Agent polled it
        // and lost the response before execution.  Marking its old operation
        // terminal is not enough for operations without a desired-state
        // reconciler: the old active record would otherwise be the next
        // deduplication hit forever.  Re-enqueue a fresh command with the same
        // payload and generation.  Job start/stop uses the canonical
        // reconciliation path below so a changed Job spec/placement is
        // rebuilt instead of replaying stale command data.
        for command in expired_retries {
            if command.operation.starts_with("job_") {
                let Some(job) = self.job(&command.resource_id).await? else {
                    continue;
                };
                if job.generation != command.generation {
                    continue;
                }
            }
            if let Err(error) = self
                .enqueue_with_metadata(
                    command.node_id.clone(),
                    command.operation.clone(),
                    command.resource_id.clone(),
                    command.correlation_id.clone(),
                    command.payload.clone(),
                    command.generation,
                    command.action_id.clone(),
                    command.config_version_id.clone(),
                    None,
                    None,
                    command.attempt_id.clone(),
                )
                .await
            {
                // The node may have gone offline while the expired command
                // was being requeued.  The next heartbeat/reconciliation can
                // retry it; polling the current command queue should still
                // succeed and return the non-expired commands.
                tracing::warn!(
                    node_id = %command.node_id,
                    operation = %command.operation,
                    resource_id = %command.resource_id,
                    %error,
                    "failed to requeue expired Hub command"
                );
            }
        }
        for job_id in expired_job_ids {
            if let Some(job) = self.job(&job_id).await? {
                self.reconcile_job(&job).await?;
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
                persist_operation(storage, &operation)
                    .await
                    .map_err(HubError::from)?;
            }
        }
        Ok(operation)
    }

    #[allow(clippy::too_many_arguments)]
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

    /// Resolve the complete node/task scope for one checkpoint round. The
    /// checkpoint operation list contains only nodes that were online when
    /// dispatch ran, so it cannot be used as the expected set by itself: an
    /// offline node would disappear and a partial artifact could be sealed.
    /// Prefer the Job's explicit placement, otherwise retain the nodes from
    /// the generation's active start assignments.
    async fn checkpoint_scope(
        &self,
        operation: &HubOperation,
        fallback_nodes: &BTreeSet<String>,
    ) -> Result<(BTreeSet<String>, Vec<String>), HubError> {
        let Some(job) = self.job(&operation.resource_id).await? else {
            return Ok((fallback_nodes.clone(), Vec::new()));
        };
        let spec: arkflow_core::job::JobSpec = serde_json::from_str(&job.spec_json)
            .map_err(|error| HubError::Invalid(format!("invalid persisted Job spec: {error}")))?;
        let plan = arkflow_core::job::JobPlan::compile(spec)
            .map_err(|error| HubError::Invalid(error.to_string()))?;
        let candidates = if job.node_ids.is_empty() {
            let nodes = self
                .operations
                .read()
                .await
                .values()
                .filter(|candidate| {
                    candidate.resource_id == operation.resource_id
                        && candidate.operation == "job_start"
                        && candidate.generation == operation.generation
                        && !matches!(
                            candidate.state,
                            HubOperationState::Failed
                                | HubOperationState::TimedOut
                                | HubOperationState::NodeUnavailable
                                | HubOperationState::Cancelled
                                | HubOperationState::Superseded
                        )
                })
                .map(|candidate| candidate.node_id.clone())
                .collect::<BTreeSet<_>>();
            if nodes.is_empty() {
                fallback_nodes.iter().cloned().collect::<Vec<_>>()
            } else {
                nodes.into_iter().collect::<Vec<_>>()
            }
        } else {
            job.node_ids.clone()
        };
        let assignments = plan
            .assignments_for_nodes(&candidates, operation.generation)
            .map_err(|error| HubError::Invalid(error.to_string()))?;
        let expected_nodes = assignments
            .iter()
            .map(|assignment| assignment.node_id.clone())
            .collect::<BTreeSet<_>>();
        let planned_task_ids = plan
            .tasks
            .iter()
            .map(|task| task.id.clone())
            .collect::<Vec<_>>();
        Ok((expected_nodes, planned_task_ids))
    }

    /// Aggregate the Job-level observed state across every assignment
    /// operation of the same (resource, generation, action). Returns:
    /// * `Some("running" | "stopped")` when every expected assignment
    ///   succeeded (the terminal success form for the action);
    /// * `Some("failed")` when the COMPLETE set has been evaluated and an
    ///   assignment reports a non-retryable execution failure;
    /// * `None` while any assignment is still queued/dispatched/running or
    ///   degraded-but-retryable — the current observed snapshot stays.
    async fn aggregate_job_observed_state(
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
        let terminal_success = updated.operation == "job_stop";
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
                        event_id: Some(event.event_id),
                        node_id,
                        event: ControlEvent {
                            occurred_at_ms: event.occurred_at_ms,
                            event_type: event.event_type,
                            stream_id: event.stream_id,
                            outcome: event.outcome,
                            message: event.message.map(|message| bounded_text(&message, 512)),
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

    pub async fn audit(
        &self,
        resource_id: Option<&str>,
    ) -> Result<Vec<crate::storage::AuditRecord>, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage
            .list_audit(resource_id.map(str::to_owned))
            .await
            .map_err(HubError::from)
    }

    pub async fn record_audit_event(
        &self,
        record: crate::storage::AuditRecord,
    ) -> Result<i64, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage.record_audit(record).await.map_err(HubError::from)
    }

    /// Audit action name for a dispatched operation: `job_start` becomes
    /// `job.start`, mirroring the dotted style of the existing audit
    /// vocabulary. Non-Job operations have no Job audit action.
    fn job_audit_action(operation: &str) -> Option<String> {
        operation
            .strip_prefix("job_")
            .map(|verb| format!("job.{verb}"))
    }

    /// Record the acceptance or rejection of a Job lifecycle operation.
    /// The message carries scalar operation metadata only — never the Job
    /// spec or configuration body (`control-plane-identity` MUST NOT).
    /// Best effort: audit failures never fail the mutation itself.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn record_job_operation_audit(
        &self,
        operation: &str,
        resource_id: &str,
        node_id: Option<&str>,
        correlation_id: Option<&str>,
        outcome: &str,
        failure_code: Option<&str>,
        message: String,
    ) {
        let Some(action) = Self::job_audit_action(operation) else {
            return;
        };
        let record = crate::storage::AuditRecord {
            event_id: 0,
            actor: Some("operator".into()),
            action,
            resource_type: "job".into(),
            resource_id: Some(resource_id.to_owned()),
            node_id: node_id.map(str::to_owned),
            stream_id: None,
            correlation_id: correlation_id.map(str::to_owned),
            outcome: outcome.to_owned(),
            failure_code: failure_code.map(str::to_owned),
            message: Some(message),
            occurred_at_ms: now_ms(),
        };
        let _ = self.record_audit_event(record).await;
    }

    pub async fn prune_events(&self, retain: usize) -> Result<usize, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage.prune_events(retain).await.map_err(HubError::from)
    }

    pub async fn create_rollout(
        &self,
        config_version_id: String,
        node_ids: Vec<String>,
        batch_size: u32,
        actor: Option<String>,
        correlation_id: Option<String>,
    ) -> Result<RolloutRecord, HubError> {
        let batch_size = batch_size.clamp(1, 256);
        if node_ids.is_empty() || node_ids.iter().any(|id| id.trim().is_empty()) {
            return Err(HubError::Invalid("rollout requires nodes".into()));
        }
        let mut unique = std::collections::BTreeSet::new();
        if node_ids.iter().any(|id| !unique.insert(id.clone())) {
            return Err(HubError::Invalid("rollout contains duplicate nodes".into()));
        }
        let now = now_ms();
        let rollout = RolloutRecord {
            rollout_id: format!("rollout-{}", HUB_SEQUENCE.fetch_add(1, Ordering::Relaxed)),
            config_version_id,
            state: "pending".into(),
            batch_size,
            current_batch: 0,
            total_targets: node_ids.len() as u32,
            actor: actor.clone(),
            correlation_id: correlation_id.clone(),
            created_at_ms: now,
            updated_at_ms: now,
        };
        let targets = node_ids
            .into_iter()
            .enumerate()
            .map(|(ordinal, node_id)| RolloutTargetRecord {
                rollout_id: rollout.rollout_id.clone(),
                node_id,
                ordinal: ordinal as u32,
                state: "pending".into(),
                attempt_id: None,
                error: None,
                observed_config_version: None,
                updated_at_ms: now,
            })
            .collect::<Vec<_>>();
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        if storage
            .get_config_version_content(rollout.config_version_id.clone())
            .await
            .map_err(HubError::from)?
            .is_none()
        {
            return Err(HubError::Invalid("configuration version not found".into()));
        }
        storage
            .create_rollout(rollout.clone(), targets)
            .await
            .map_err(HubError::from)?;
        storage
            .record_audit(crate::storage::AuditRecord {
                event_id: 0,
                actor,
                action: "rollout.create".into(),
                resource_type: "rollout".into(),
                resource_id: Some(rollout.rollout_id.clone()),
                node_id: None,
                stream_id: None,
                correlation_id,
                outcome: "accepted".into(),
                failure_code: None,
                message: None,
                occurred_at_ms: now,
            })
            .await
            .map_err(HubError::from)?;
        self.rollouts
            .write()
            .await
            .insert(rollout.rollout_id.clone(), rollout.clone());
        Ok(rollout)
    }

    pub async fn create_rollout_with_content(
        &self,
        config_version_id: String,
        content: String,
        node_ids: Vec<String>,
        batch_size: u32,
        actor: Option<String>,
        correlation_id: Option<String>,
    ) -> Result<RolloutRecord, HubError> {
        let batch_size = batch_size.clamp(1, 256);
        if node_ids.len() != 1 || node_ids.iter().any(|id| id.trim().is_empty()) {
            return Err(HubError::Invalid(
                "single-node rollout requires exactly one node".into(),
            ));
        }
        let now = now_ms();
        let rollout = RolloutRecord {
            rollout_id: format!("rollout-{}", HUB_SEQUENCE.fetch_add(1, Ordering::Relaxed)),
            config_version_id,
            state: "pending".into(),
            batch_size,
            current_batch: 0,
            total_targets: 1,
            actor: actor.clone(),
            correlation_id: correlation_id.clone(),
            created_at_ms: now,
            updated_at_ms: now,
        };
        let targets = vec![RolloutTargetRecord {
            rollout_id: rollout.rollout_id.clone(),
            node_id: node_ids[0].clone(),
            ordinal: 0,
            state: "pending".into(),
            attempt_id: None,
            error: None,
            observed_config_version: None,
            updated_at_ms: now,
        }];
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage
            .create_rollout_with_content(rollout.clone(), targets, content, actor.clone())
            .await
            .map_err(HubError::from)?;
        storage
            .record_audit(crate::storage::AuditRecord {
                event_id: 0,
                actor,
                action: "rollout.create".into(),
                resource_type: "rollout".into(),
                resource_id: Some(rollout.rollout_id.clone()),
                node_id: node_ids.into_iter().next(),
                stream_id: None,
                correlation_id,
                outcome: "accepted".into(),
                failure_code: None,
                message: None,
                occurred_at_ms: now,
            })
            .await
            .map_err(HubError::from)?;
        self.rollouts
            .write()
            .await
            .insert(rollout.rollout_id.clone(), rollout.clone());
        Ok(rollout)
    }

    pub async fn rollout(
        &self,
        rollout_id: &str,
    ) -> Result<Option<(RolloutRecord, Vec<RolloutTargetRecord>)>, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        let Some(rollout) = storage
            .get_rollout(rollout_id.to_owned())
            .await
            .map_err(HubError::from)?
        else {
            return Ok(None);
        };
        let targets = storage
            .list_rollout_targets(rollout_id.to_owned())
            .await
            .map_err(HubError::from)?;
        Ok(Some((rollout, targets)))
    }

    pub async fn rollouts(&self) -> Result<Vec<RolloutRecord>, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage.list_rollouts().await.map_err(HubError::from)
    }

    pub async fn act_rollout(
        &self,
        rollout_id: &str,
        action: &str,
        rollback_config_version: Option<String>,
        actor: Option<String>,
        correlation_id: Option<String>,
    ) -> Result<RolloutRecord, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        let Some(rollout) = storage
            .get_rollout(rollout_id.to_owned())
            .await
            .map_err(HubError::from)?
        else {
            return Err(HubError::Invalid("rollout not found".into()));
        };
        let terminal = matches!(
            rollout.state.as_str(),
            "converged" | "cancelled" | "rolled_back"
        );
        if terminal {
            return Err(HubError::Invalid("rollout is already terminal".into()));
        }
        let now = now_ms();
        match action {
            "pause" => {
                storage
                    .update_rollout(rollout_id, "paused", rollout.current_batch, now)
                    .await
                    .map_err(HubError::from)?;
                let targets = storage
                    .list_rollout_targets(rollout_id.to_owned())
                    .await
                    .map_err(HubError::from)?;
                for target in targets
                    .into_iter()
                    .filter(|target| target.state == "pending")
                {
                    storage
                        .update_rollout_target(RolloutTargetUpdate {
                            rollout_id: rollout_id.to_owned(),
                            node_id: target.node_id,
                            state: "paused".into(),
                            attempt_id: target.attempt_id,
                            error: target.error,
                            observed_config_version: target.observed_config_version,
                            updated_at_ms: now,
                        })
                        .await
                        .map_err(HubError::from)?;
                }
                self.record_rollout_audit(
                    &rollout,
                    "rollout.pause",
                    actor,
                    correlation_id,
                    "accepted",
                    None,
                )
                .await?;
                Ok(RolloutRecord {
                    state: "paused".into(),
                    updated_at_ms: now,
                    ..rollout
                })
            }
            "resume" => {
                if rollout.state != "paused" {
                    return Err(HubError::Invalid("only a paused rollout can resume".into()));
                }
                storage
                    .update_rollout(rollout_id, "applying", rollout.current_batch, now)
                    .await
                    .map_err(HubError::from)?;
                let targets = storage
                    .list_rollout_targets(rollout_id.to_owned())
                    .await
                    .map_err(HubError::from)?;
                for target in targets
                    .into_iter()
                    .filter(|target| target.state == "paused")
                {
                    storage
                        .update_rollout_target(RolloutTargetUpdate {
                            rollout_id: rollout_id.to_owned(),
                            node_id: target.node_id,
                            state: "pending".into(),
                            attempt_id: target.attempt_id,
                            error: target.error,
                            observed_config_version: target.observed_config_version,
                            updated_at_ms: now,
                        })
                        .await
                        .map_err(HubError::from)?;
                }
                self.record_rollout_audit(
                    &rollout,
                    "rollout.resume",
                    actor,
                    correlation_id,
                    "accepted",
                    None,
                )
                .await?;
                Ok(RolloutRecord {
                    state: "applying".into(),
                    updated_at_ms: now,
                    ..rollout
                })
            }
            "cancel" => {
                storage
                    .update_rollout(rollout_id, "cancelled", rollout.current_batch, now)
                    .await
                    .map_err(HubError::from)?;
                let targets = storage
                    .list_rollout_targets(rollout_id.to_owned())
                    .await
                    .map_err(HubError::from)?;
                for target in targets.into_iter().filter(|target| {
                    matches!(target.state.as_str(), "pending" | "paused" | "applying")
                }) {
                    storage
                        .update_rollout_target(RolloutTargetUpdate {
                            rollout_id: rollout_id.to_owned(),
                            node_id: target.node_id,
                            state: "cancelled".into(),
                            attempt_id: target.attempt_id,
                            error: Some("rollout cancelled by operator".into()),
                            observed_config_version: target.observed_config_version,
                            updated_at_ms: now,
                        })
                        .await
                        .map_err(HubError::from)?;
                }
                self.record_rollout_audit(
                    &rollout,
                    "rollout.cancel",
                    actor,
                    correlation_id,
                    "accepted",
                    None,
                )
                .await?;
                Ok(RolloutRecord {
                    state: "cancelled".into(),
                    updated_at_ms: now,
                    ..rollout
                })
            }
            "rollback" => {
                let Some(config_version_id) = rollback_config_version else {
                    return Err(HubError::Invalid("rollback requires config_version".into()));
                };
                let targets = storage
                    .list_rollout_targets(rollout_id.to_owned())
                    .await
                    .map_err(HubError::from)?;
                let rollback = self
                    .create_rollout(
                        config_version_id,
                        targets.into_iter().map(|target| target.node_id).collect(),
                        rollout.batch_size,
                        actor.clone(),
                        correlation_id.clone(),
                    )
                    .await?;
                storage
                    .update_rollout(rollout_id, "rolled_back", rollout.current_batch, now)
                    .await
                    .map_err(HubError::from)?;
                self.record_rollout_audit(
                    &rollout,
                    "rollout.rollback",
                    actor,
                    correlation_id,
                    "accepted",
                    Some(format!("created rollout {}", rollback.rollout_id)),
                )
                .await?;
                Ok(rollback)
            }
            _ => Err(HubError::Invalid(
                "action must be pause, resume, cancel, or rollback".into(),
            )),
        }
    }

    async fn record_rollout_audit(
        &self,
        rollout: &RolloutRecord,
        action: &str,
        actor: Option<String>,
        correlation_id: Option<String>,
        outcome: &str,
        message: Option<String>,
    ) -> Result<(), HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage
            .record_audit(crate::storage::AuditRecord {
                event_id: 0,
                actor,
                action: action.into(),
                resource_type: "rollout".into(),
                resource_id: Some(rollout.rollout_id.clone()),
                node_id: None,
                stream_id: None,
                correlation_id,
                outcome: outcome.into(),
                failure_code: None,
                message: message.map(|value| value.chars().take(256).collect()),
                occurred_at_ms: now_ms(),
            })
            .await
            .map(|_| ())
            .map_err(HubError::from)
    }

    pub async fn reconcile_rollouts(&self) -> Result<usize, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        let active = storage.recover_rollouts().await.map_err(HubError::from)?;
        let mut changes = 0;
        for rollout in active {
            if rollout.state == "paused" {
                continue;
            }
            let targets = storage
                .list_rollout_targets(rollout.rollout_id.clone())
                .await
                .map_err(HubError::from)?;
            let batch_start = rollout.current_batch * rollout.batch_size;
            let batch_end = batch_start + rollout.batch_size;
            let mut batch_failed = false;
            for target in targets
                .iter()
                .filter(|target| target.ordinal >= batch_start && target.ordinal < batch_end)
            {
                match target.state.as_str() {
                    "pending" => {
                        let online =
                            self.nodes
                                .read()
                                .await
                                .get(&target.node_id)
                                .is_some_and(|node| {
                                    node.resource.state == NodeConnectionState::Online
                                        && node.resource.lease_expires_at_ms > now_ms()
                                        && node.resource.maintenance_state
                                            == NodeMaintenanceState::Active
                                });
                        if !online {
                            continue;
                        }
                        let Some(payload_json) = storage
                            .get_config_version_content(rollout.config_version_id.clone())
                            .await
                            .map_err(HubError::from)?
                        else {
                            storage
                                .update_rollout_target(RolloutTargetUpdate {
                                    rollout_id: rollout.rollout_id.clone(),
                                    node_id: target.node_id.clone(),
                                    state: "failed".into(),
                                    attempt_id: None,
                                    error: Some("configuration version content is missing".into()),
                                    observed_config_version: None,
                                    updated_at_ms: now_ms(),
                                })
                                .await
                                .map_err(HubError::from)?;
                            batch_failed = true;
                            changes += 1;
                            continue;
                        };
                        let expected_generation = storage
                            .get_desired(target.node_id.clone(), "__configuration__")
                            .await
                            .map_err(HubError::from)?
                            .map(|desired| desired.generation)
                            .unwrap_or(0);
                        let intent = self
                            .set_desired_state(DesiredMutation {
                                node_id: target.node_id.clone(),
                                stream_id: "__configuration__".into(),
                                desired_state: "configured".into(),
                                config_version_id: Some(rollout.config_version_id.clone()),
                                action_id: None,
                                expected_generation: Some(expected_generation),
                                actor: rollout.actor.clone(),
                                correlation_id: rollout.correlation_id.clone(),
                                idempotency_key: Some(format!(
                                    "{}:{}",
                                    rollout.rollout_id, target.node_id
                                )),
                                intent_type: Some("apply_configuration".into()),
                                payload_json: Some(payload_json),
                            })
                            .await?;
                        storage
                            .update_rollout_target(RolloutTargetUpdate {
                                rollout_id: rollout.rollout_id.clone(),
                                node_id: target.node_id.clone(),
                                state: "applying".into(),
                                attempt_id: Some(intent.intent_id),
                                error: None,
                                observed_config_version: None,
                                updated_at_ms: now_ms(),
                            })
                            .await
                            .map_err(HubError::from)?;
                        changes += 1;
                    }
                    "applying" => {
                        let Some(intent_id) = target.attempt_id.as_deref() else {
                            continue;
                        };
                        let Some(intent) = storage
                            .get_intent(intent_id.to_owned())
                            .await
                            .map_err(HubError::from)?
                        else {
                            continue;
                        };
                        if intent.state == "converged" {
                            storage
                                .update_rollout_target(RolloutTargetUpdate {
                                    rollout_id: rollout.rollout_id.clone(),
                                    node_id: target.node_id.clone(),
                                    state: "succeeded".into(),
                                    attempt_id: target.attempt_id.clone(),
                                    error: None,
                                    observed_config_version: intent.config_version_id.clone(),
                                    updated_at_ms: now_ms(),
                                })
                                .await
                                .map_err(HubError::from)?;
                            changes += 1;
                        } else if matches!(intent.state.as_str(), "blocked" | "superseded") {
                            storage
                                .update_rollout_target(RolloutTargetUpdate {
                                    rollout_id: rollout.rollout_id.clone(),
                                    node_id: target.node_id.clone(),
                                    state: "failed".into(),
                                    attempt_id: target.attempt_id.clone(),
                                    error: intent.failure_class.clone(),
                                    observed_config_version: intent.config_version_id.clone(),
                                    updated_at_ms: now_ms(),
                                })
                                .await
                                .map_err(HubError::from)?;
                            batch_failed = true;
                            changes += 1;
                        }
                    }
                    "failed" => batch_failed = true,
                    _ => {}
                }
            }
            let refreshed = storage
                .list_rollout_targets(rollout.rollout_id.clone())
                .await
                .map_err(HubError::from)?;
            let current_batch = refreshed
                .iter()
                .filter(|target| target.ordinal >= batch_start && target.ordinal < batch_end)
                .collect::<Vec<_>>();
            if batch_failed {
                storage
                    .update_rollout(
                        &rollout.rollout_id,
                        "paused",
                        rollout.current_batch,
                        now_ms(),
                    )
                    .await
                    .map_err(HubError::from)?;
            } else if !current_batch.is_empty()
                && current_batch
                    .iter()
                    .all(|target| target.state == "succeeded")
            {
                let next_batch = rollout.current_batch + 1;
                let complete = next_batch * rollout.batch_size >= rollout.total_targets;
                storage
                    .update_rollout(
                        &rollout.rollout_id,
                        if complete { "converged" } else { "applying" },
                        next_batch,
                        now_ms(),
                    )
                    .await
                    .map_err(HubError::from)?;
                changes += 1;
            } else if rollout.state == "pending" {
                storage
                    .update_rollout(
                        &rollout.rollout_id,
                        "applying",
                        rollout.current_batch,
                        now_ms(),
                    )
                    .await
                    .map_err(HubError::from)?;
                changes += 1;
            }
        }
        Ok(changes)
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

    /// Per-node, per-Job kernel metric snapshots for the data-plane Prometheus
    /// export. Only Agents with an unexpired lease are included, so an Agent
    /// that stops reporting (expired lease or deregistration) stops being
    /// exported.
    pub async fn job_metrics(
        &self,
    ) -> Vec<(
        String,
        BTreeMap<String, arkflow_core::executor::metrics::KernelMetricsSnapshot>,
    )> {
        let now = now_ms();
        self.nodes
            .read()
            .await
            .values()
            .filter(|node| node.resource.lease_expires_at_ms > now)
            .filter(|node| !node.jobs.is_empty())
            .map(|node| (node.resource.id.clone(), node.jobs.clone()))
            .collect()
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
    ) || now_ms() > node.session_expires_at_ms
    {
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
    }
}

fn default_protocol_version() -> String {
    "v1".into()
}

fn sanitize_metrics(metrics: BTreeMap<String, f64>) -> BTreeMap<String, f64> {
    metrics
        .into_iter()
        .filter(|(key, value)| {
            ALLOWED_NODE_METRICS.contains(&key.as_str()) && value.is_finite() && *value >= 0.0
        })
        .collect()
}

/// Fleet-level "node pressuring" judgment: memory used ratio or CPU above
/// the threshold. Evaluated against the LATEST report's gauges; a node
/// without usable gauges is never pressuring (fail-safe: no data, no move).
const PRESSURE_MEMORY_USED_RATIO: f64 = 0.9;
const PRESSURE_CPU_PERCENT: f64 = 90.0;

fn node_under_pressure(metrics: &BTreeMap<String, f64>) -> bool {
    let memory_pressuring = match (
        metrics.get("node_memory_used_bytes"),
        metrics.get("node_memory_total_bytes"),
    ) {
        (Some(used), Some(total))
            if total.is_finite() && *total > 0.0 && used.is_finite() =>
        {
            used / total >= PRESSURE_MEMORY_USED_RATIO
        }
        _ => false,
    };
    let cpu_pressuring = metrics
        .get("node_cpu_usage_percent")
        .is_some_and(|cpu| cpu.is_finite() && *cpu >= PRESSURE_CPU_PERCENT);
    memory_pressuring || cpu_pressuring
}

/// How long a node's gauges stay eligible for headroom ranking after its
/// last report — twice the Agent sampling interval, mirroring the
/// Agent-side freshness window.
const RESOURCE_GAUGE_FRESH_MS: u64 = 10_000;

/// Headroom ordering key for placement ranking: fresh-gauged nodes rank
/// (1, memory-available ratio, CPU headroom), gauge-less nodes rank
/// (0, 0, 0) and land after every gauged node, in id order. Larger is
/// better in every component.
fn headroom_key(record: Option<&NodeRecord>, now: u64) -> (u8, f64, f64) {
    let Some(record) = record else {
        return (0, 0.0, 0.0);
    };
    if record.last_report_at_ms == 0
        || now.saturating_sub(record.last_report_at_ms) > RESOURCE_GAUGE_FRESH_MS
    {
        return (0, 0.0, 0.0);
    }
    let (Some(used), Some(total)) = (
        record.metrics.get("node_memory_used_bytes"),
        record.metrics.get("node_memory_total_bytes"),
    ) else {
        return (0, 0.0, 0.0);
    };
    if !total.is_finite() || *total <= 0.0 || !used.is_finite() {
        return (0, 0.0, 0.0);
    }
    let memory_available_ratio = (1.0 - used / total).clamp(0.0, 1.0);
    let cpu_headroom = record
        .metrics
        .get("node_cpu_usage_percent")
        .filter(|cpu| cpu.is_finite())
        .map(|cpu| (100.0 - cpu).clamp(0.0, 100.0))
        .unwrap_or(0.0);
    (1, memory_available_ratio, cpu_headroom)
}

/// Rank eligible placement candidates by resource headroom. Pure and
/// deterministic: the ordered output feeds the unchanged assignment logic,
/// so `split-placement`'s "same input, same mapping" contract holds by
/// construction. Nodes with equal headroom (and all gauge-less nodes) tie
/// on node id.
fn rank_candidates(
    candidates: Vec<String>,
    nodes: &BTreeMap<String, NodeRecord>,
    now: u64,
) -> Vec<String> {
    let mut ranked = candidates;
    ranked.sort_by(|left, right| {
        let left_key = headroom_key(nodes.get(left), now);
        let right_key = headroom_key(nodes.get(right), now);
        right_key
            .0
            .cmp(&left_key.0)
            .then_with(|| right_key.1.total_cmp(&left_key.1))
            .then_with(|| right_key.2.total_cmp(&left_key.2))
            .then_with(|| left.cmp(right))
    });
    ranked
}

/// Upper bound on per-Job snapshots kept for one node. Jobs are
/// operator-configured so this is generous; a misbehaving Agent cannot grow
/// Hub memory without bound.
const MAX_REPORTED_JOBS_PER_NODE: usize = 256;

/// Keep the bounded set of reported Job snapshots, discarding finite values
/// only: series cardinality stays at O(jobs x chains) per node.
fn bounded_job_snapshots(
    jobs: BTreeMap<String, arkflow_core::executor::metrics::KernelMetricsSnapshot>,
) -> BTreeMap<String, arkflow_core::executor::metrics::KernelMetricsSnapshot> {
    jobs.into_iter().take(MAX_REPORTED_JOBS_PER_NODE).collect()
}

fn sanitize_capabilities(capabilities: Vec<String>) -> Vec<String> {
    capabilities
        .into_iter()
        .filter(|capability| {
            !capability.is_empty()
                && capability.len() <= 64
                && capability
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || "._-".contains(character))
        })
        .take(32)
        .collect()
}

fn bounded_text(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

fn is_durable_job_start(operation: &HubOperation) -> bool {
    operation.operation == "job_start"
        && (operation.state == HubOperationState::Succeeded
            || operation.failure_class.as_deref() == Some("recovery_required"))
}

fn parse_operator_credential(configured: &str) -> (&str, OperatorRole, &str, Vec<ResourceScope>) {
    let mut fields = configured.splitn(4, '|');
    let Some(id) = fields.next() else {
        return ("operator", OperatorRole::Admin, configured, Vec::new());
    };
    let Some(role) = fields.next() else {
        return ("operator", OperatorRole::Admin, configured, Vec::new());
    };
    let Some(secret) = fields.next() else {
        return ("operator", OperatorRole::Admin, configured, Vec::new());
    };
    let role = match role {
        "admin" => OperatorRole::Admin,
        "operator" => OperatorRole::Operator,
        "viewer" => OperatorRole::Viewer,
        _ => return ("operator", OperatorRole::Admin, configured, Vec::new()),
    };
    if id.trim().is_empty() || secret.is_empty() {
        ("operator", OperatorRole::Admin, configured, Vec::new())
    } else {
        let scopes = fields
            .next()
            .into_iter()
            .flat_map(|value| value.split(','))
            .filter_map(parse_resource_scope)
            .collect();
        (id, role, secret, scopes)
    }
}

fn parse_resource_scope(value: &str) -> Option<ResourceScope> {
    let (resource_type, resource_id) = value.split_once('=')?;
    if resource_type.trim().is_empty() {
        return None;
    }
    Some(ResourceScope {
        resource_type: resource_type.to_owned(),
        resource_id: (!resource_id.is_empty()).then(|| resource_id.to_owned()),
    })
}

fn required_capabilities(operation: &str) -> Vec<String> {
    match operation {
        "start" | "stop" | "restart" => vec!["stream_lifecycle".into()],
        "job_start"
        | "job_stop"
        | "job_restart"
        | "job_checkpoint"
        | "job_savepoint"
        | "job_checkpoint_commit"
        | "job_savepoint_commit" => {
            vec!["job_runtime".into(), "state_backend".into()]
        }
        "apply_configuration" | "rollback_configuration" => vec!["configuration".into()],
        _ => Vec::new(),
    }
}

async fn persist_operation(
    storage: &StorageActor,
    operation: &HubOperation,
) -> Result<(), StorageError> {
    storage
        .upsert_operation(PersistedOperation {
            operation_id: operation.id.clone(),
            node_id: operation.node_id.clone(),
            resource_id: operation.resource_id.clone(),
            operation: operation.operation.clone(),
            state: serde_json::to_value(operation.state)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .unwrap_or_else(|| "unknown".into()),
            created_at_ms: operation.created_at_ms,
            updated_at_ms: now_ms(),
            operation_json: serde_json::to_string(operation)
                .map_err(|_| StorageError::ActorClosed)?,
        })
        .await
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

fn recovery_record_is_compatible(
    spec: &arkflow_core::job::JobSpec,
    record: &JobCheckpointRecord,
) -> bool {
    // The same version-direction rule the shared recovery evaluator applies
    // for the Agent and the repository: an equal state format permits a
    // TARGET VERSION UPGRADE (a savepoint written by an older Job version
    // restoring into the new one); downgrades and format changes have no
    // migration path and stay rejected on both sides.
    record.format_version == job_state_format_version(spec) && record.job_version <= spec.version.0
}

fn job_state_format_version(spec: &arkflow_core::job::JobSpec) -> u32 {
    spec.state
        .as_ref()
        .map(|state| state.format_version)
        .unwrap_or(1)
}
static HUB_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::control::{ConvergenceState, StreamMetricsSnapshot, StreamState};

    #[test]
    fn recovery_selection_requires_matching_job_and_state_versions() {
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(serde_json::json!({
            "id": "orders",
            "version": 2,
            "operators": [],
            "sources": [],
            "sinks": [],
            "state": {"backend": "embedded_kv", "format_version": 3}
        }))
        .unwrap();
        assert_eq!(job_state_format_version(&spec), 3);
        let compatible = JobCheckpointRecord {
            job_id: "orders".into(),
            job_version: 2,
            checkpoint_id: "checkpoint-current".into(),
            kind: "checkpoint".into(),
            status: "completed".into(),
            manifest_uri: None,
            format_version: 3,
            created_at_ms: 2,
            updated_at_ms: 2,
        };
        assert!(recovery_record_is_compatible(&spec, &compatible));
        // A savepoint written by an OLDER Job version with the same state
        // format is a compatible upgrade target (the shared evaluator's
        // version-direction rule); a NEWER artifact has no downgrade path.
        let mut upgrade = compatible.clone();
        upgrade.job_version = 1;
        assert!(
            recovery_record_is_compatible(&spec, &upgrade),
            "an equal-format older artifact restores into the newer version"
        );
        let mut downgrade = compatible.clone();
        downgrade.job_version = 3;
        assert!(
            !recovery_record_is_compatible(&spec, &downgrade),
            "downgrades have no compatibility path"
        );
        let mut old_format = compatible;
        old_format.format_version = 2;
        assert!(!recovery_record_is_compatible(&spec, &old_format));
    }

    #[tokio::test]
    async fn checkpoint_completion_after_hub_restart_preserves_metadata() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        let storage = crate::storage::StorageActor::start(store, 8);
        let hub1 = Hub::with_storage(config(), storage.clone());
        let hub2 = Hub::with_storage(config(), storage);
        let spec_json = serde_json::json!({
            "id": "orders",
            "version": 2,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}],
            "state": {"backend": "embedded_kv", "format_version": 3}
        })
        .to_string();
        hub1.upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 2,
            spec_json,
            desired_state: "stopped".into(),
            observed_state: "stopped".into(),
            convergence: "converged".into(),
            generation: 4,
            node_ids: Vec::new(),
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        hub1.record_job_checkpoint(JobCheckpointRecord {
            job_id: "orders".into(),
            job_version: 2,
            checkpoint_id: "checkpoint-4".into(),
            kind: "checkpoint".into(),
            status: "pending".into(),
            manifest_uri: None,
            format_version: 3,
            created_at_ms: 1,
            updated_at_ms: 1,
        })
        .await
        .unwrap();
        hub2.complete_job_checkpoint(
            "orders",
            "checkpoint-4",
            "completed",
            Some("s3://bucket/checkpoint-4/manifest.json".into()),
        )
        .await
        .unwrap();
        let records = hub2.job_checkpoints("orders").await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].job_version, 2);
        assert_eq!(records[0].format_version, 3);
        assert_eq!(records[0].status, "completed");
    }

    // ---------- review P1 regressions (repair-control-plane-review-defects) ----------

    /// Session credentials authenticate every agent request; they must be
    /// independent high-entropy values, not a sequential counter an attacker
    /// can enumerate.
    #[tokio::test]
    async fn session_tokens_are_random_and_unique() {
        let hub = Hub::new(config());
        let mut tokens = Vec::new();
        for node_id in ["node-a", "node-b", "node-c"] {
            let session = hub
                .register(RegisterRequest {
                    data_address: None,
                    node_id: node_id.into(),
                    node_token: "node-secret".into(),
                    protocol_version: "v1".into(),
                    capabilities: vec!["stream_lifecycle".into()],
                    boot_id: None,
                })
                .await
                .unwrap();
            assert!(
                session.session_token.len() >= 32,
                "session token must carry real entropy"
            );
            assert!(
                !session.session_token.starts_with("node-session-"),
                "session token must not be a sequential counter"
            );
            tokens.push(session.session_token);
        }
        let unique: std::collections::BTreeSet<_> = tokens.iter().collect();
        assert_eq!(
            unique.len(),
            tokens.len(),
            "every session token must be unique"
        );
        // Re-registration issues an independent token, not the next counter
        // value.
        let re_registered = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["stream_lifecycle".into()],
                boot_id: None,
            })
            .await
            .unwrap();
        assert!(!tokens.contains(&re_registered.session_token));
    }

    /// A terminal-failure operation must not wedge its intent: the retry
    /// attempt enqueued by the reconciler replaces the failed record and a
    /// fresh command reaches the node.
    #[tokio::test]
    async fn terminal_failure_intent_reenqueues_a_fresh_command_on_retry() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        let storage = StorageActor::start(store, 8);
        let hub = Hub::with_storage(config(), storage.clone());
        let session = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["stream_lifecycle".into()],
                boot_id: None,
            })
            .await
            .unwrap();
        let intent = hub
            .set_desired_state(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                expected_generation: Some(0),
                ..Default::default()
            })
            .await
            .unwrap();
        let dispatched = hub.reconcile_once("dispatch").await.unwrap().unwrap();
        assert_eq!(dispatched.id, intent.intent_id);
        let auth = AgentAuth {
            node_id: "node-a".into(),
            session_token: session.session_token.clone(),
        };
        let polled = hub.commands(auth.clone()).await.unwrap();
        assert_eq!(polled.len(), 1);
        let command_id = polled[0].id.clone();
        // The agent reports a transient execution failure: the intent must
        // move to `retrying` with a due retry row.
        hub.command_result(
            auth.clone(),
            CommandResult {
                command_id,
                operation_id: dispatched.id.clone(),
                state: HubOperationState::Failed,
                progress: 0,
                error: Some("agent worker crashed".into()),
                correlation_id: None,
                generation: dispatched.generation,
                observed_generation: None,
                action_id: None,
                failure_class: Some("temporary_execution".into()),
                config_version_id: None,
                rollout_id: None,
                observed_checkpoint_id: None,
                checkpoint_manifest_uri: None,
            },
        )
        .await
        .unwrap();
        // Wait past the 1s retry backoff, then reconcile: the retry attempt
        // must enqueue a fresh command instead of returning the terminal
        // record without queueing anything.
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        // The node's short test lease expired during the backoff; the agent
        // reconnects before the reconciler retries.
        let session = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["stream_lifecycle".into()],
                boot_id: None,
            })
            .await
            .unwrap();
        let retried = hub.reconcile_once("retry").await.unwrap();
        assert!(retried.is_some(), "retry attempt must be enqueued");
        let auth = AgentAuth {
            node_id: "node-a".into(),
            session_token: session.session_token,
        };
        let commands = hub.commands(auth).await.unwrap();
        assert!(
            !commands.is_empty(),
            "a retried intent must produce a fresh command"
        );
    }

    /// Restart recovery: persisted operations come back into the in-memory
    /// map, and a succeeded lifecycle start at the current generation is
    /// never re-dispatched — the dispatch-skip memory survives the restart.
    #[tokio::test]
    async fn restart_restores_persisted_operations_and_skips_satisfied_starts() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        let hub1 = Hub::with_storage(config(), StorageActor::start(store.clone(), 8));
        let session = hub1
            .register(RegisterRequest {
                data_address: None,
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec![],
                boot_id: Some("boot-a".into()),
            })
            .await
            .unwrap();
        hub1.upsert_job(JobRecord {
            job_id: "job-1".into(),
            version: 1,
            spec_json: job_spec_json("job-1"),
            desired_state: "running".into(),
            observed_state: "validated".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: vec!["node-a".into()],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        hub1.reconcile_jobs().await.unwrap();
        let auth = AgentAuth {
            node_id: "node-a".into(),
            session_token: session.session_token.clone(),
        };
        let commands = hub1.commands(auth.clone()).await.unwrap();
        let start_command = commands
            .iter()
            .find(|command| command.operation == "job_start")
            .expect("job_start must be dispatched for a desired-running job")
            .clone();
        hub1.command_result(
            auth,
            CommandResult {
                command_id: start_command.id.clone(),
                operation_id: start_command.operation_id.clone(),
                state: HubOperationState::Succeeded,
                progress: 100,
                error: None,
                correlation_id: start_command.correlation_id.clone(),
                generation: start_command.generation,
                observed_generation: Some(start_command.generation),
                action_id: None,
                failure_class: None,
                config_version_id: None,
                rollout_id: None,
                observed_checkpoint_id: None,
                checkpoint_manifest_uri: None,
            },
        )
        .await
        .unwrap();

        // "Restart": same durable store, fresh in-memory state. hub1 also
        // persisted its own bookkeeping rows (checkpoint triggers), so the
        // restore brings back more than the start operation — assert on the
        // semantics, not on an exact count.
        let hub2 = Hub::with_storage(config(), StorageActor::start(store.clone(), 8));
        let restored = hub2.restore_persisted_operations().await.unwrap();
        assert!(restored >= 1, "at least the succeeded start is restored");
        assert!(hub2.operations(None).await.iter().any(|operation| {
            operation.operation == "job_start" && operation.state == HubOperationState::Succeeded
        }));

        let session2 = hub2
            .register(RegisterRequest {
                data_address: None,
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec![],
                boot_id: Some("boot-a".into()),
            })
            .await
            .unwrap();
        hub2.reconcile_jobs().await.unwrap();
        let commands2 = hub2
            .commands(AgentAuth {
                node_id: "node-a".into(),
                session_token: session2.session_token.clone(),
            })
            .await
            .unwrap();
        assert!(
            !commands2
                .iter()
                .any(|command| command.operation == "job_start"),
            "a succeeded start at the current generation must not be re-dispatched after a restart"
        );
    }

    #[tokio::test]
    async fn durable_replacement_without_checkpoint_fails_closed_before_dispatch() {
        let hub = Hub::new(config());
        let session = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["job_runtime".into(), "state_backend".into()],
                boot_id: Some("boot-a".into()),
            })
            .await
            .unwrap();
        hub.upsert_job(JobRecord {
            job_id: "durable-orders".into(),
            version: 1,
            spec_json: durable_job_spec_json("durable-orders"),
            desired_state: "running".into(),
            observed_state: "validated".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: vec!["node-a".into()],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        let auth = AgentAuth {
            node_id: "node-a".into(),
            session_token: session.session_token,
        };
        let start = hub
            .commands(auth.clone())
            .await
            .unwrap()
            .into_iter()
            .find(|command| command.operation == "job_start")
            .expect("initial durable deployment is allowed to start empty");
        assert_eq!(
            start
                .payload
                .as_ref()
                .and_then(|payload| payload.get("recovery_required"))
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
        hub.command_result(
            auth,
            CommandResult {
                command_id: start.id.clone(),
                operation_id: start.operation_id.clone(),
                state: HubOperationState::Succeeded,
                progress: 100,
                error: None,
                correlation_id: start.correlation_id.clone(),
                generation: start.generation,
                observed_generation: Some(start.generation),
                action_id: None,
                failure_class: None,
                config_version_id: None,
                rollout_id: None,
                observed_checkpoint_id: None,
                checkpoint_manifest_uri: None,
            },
        )
        .await
        .unwrap();

        let mut replacement = hub.job("durable-orders").await.unwrap().unwrap();
        replacement.version = 2;
        let error = hub
            .upsert_job(replacement)
            .await
            .expect_err("a replacement without a completed checkpoint must fail closed");
        assert!(error.to_string().contains("requires recovery"));
        assert!(hub
            .nodes
            .read()
            .await
            .get("node-a")
            .is_some_and(|node| !node
                .commands
                .iter()
                .any(|command| command.operation == "job_start" && command.generation == 2)));
    }

    fn job_spec_json(id: &str) -> String {
        serde_json::json!({
            "id": id,
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        })
        .to_string()
    }

    #[tokio::test]
    async fn current_generation_recovery_required_failure_overrides_running_observation() {
        let hub = Hub::new(config());
        hub.register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["job_runtime".into(), "state_backend".into()],
            boot_id: Some("boot-a".into()),
        })
        .await
        .unwrap();
        let job = JobRecord {
            job_id: "durable-orders-restarted".into(),
            version: 1,
            spec_json: durable_job_spec_json("durable-orders-restarted"),
            desired_state: "running".into(),
            observed_state: "running".into(),
            convergence: "reconciling".into(),
            generation: 1,
            node_ids: vec!["node-a".into()],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        };
        hub.operations.write().await.insert(
            "recovery-required-start".into(),
            HubOperation {
                id: "recovery-required-start".into(),
                intent_id: None,
                command_id: "command-recovery-required".into(),
                node_id: "node-a".into(),
                operation: "job_start".into(),
                resource_id: job.job_id.clone(),
                checkpoint_id: None,
                generation: job.generation,
                attempt_id: None,
                config_version_id: None,
                state: HubOperationState::NodeUnavailable,
                progress: 100,
                created_at_ms: 1,
                expires_at_ms: None,
                dispatched_at_ms: None,
                acknowledged_at_ms: None,
                finished_at_ms: Some(2),
                correlation_id: None,
                error: Some("previous successful start invalidated by Agent reboot".into()),
                failure_class: Some("recovery_required".into()),
                intent_state: None,
                convergence_state: None,
                retry_count: 0,
                next_retry_at_ms: None,
                superseded_by_intent_id: None,
                superseded_generation: None,
                observed_generation: None,
                observed_state: None,
            },
        );

        let error = hub
            .reconcile_job(&job)
            .await
            .expect_err("a rebooted durable Job must not start from empty state");
        assert!(error.to_string().contains("requires recovery"), "{error}");
        assert!(hub.nodes.read().await.get("node-a").is_some_and(|node| node
            .commands
            .iter()
            .all(|command| command.operation != "job_start")));
    }

    fn durable_job_spec_json(id: &str) -> String {
        let mut spec = serde_json::from_str::<serde_json::Value>(&job_spec_json(id)).unwrap();
        spec["operators"] = serde_json::json!([
            {"id": "source", "kind": "source"},
            {
                "id": "aggregate",
                "kind": "aggregate",
                "stateful": true,
                "key_field": "key"
            },
            {"id": "sink", "kind": "sink"}
        ]);
        spec["edges"] = serde_json::json!([
            {"id": "source-aggregate", "from": "source", "to": "aggregate"},
            {"id": "aggregate-sink", "from": "aggregate", "to": "sink"}
        ]);
        spec["state"] = serde_json::json!({
            "backend": "embedded_kv",
            "durability": "durable",
            "format_version": 1
        });
        spec["checkpoint"] = serde_json::json!({
            "interval_ms": 1000,
            "retention": 2,
            "object_store_uri": "file:///tmp/arkflow-hub-recovery-test"
        });
        spec.to_string()
    }

    /// A stopped Job whose stop command already succeeded must not receive a
    /// fresh stop command (and a fresh persistent operation row) on every
    /// reconcile tick: that churn loop grew the durable operation store
    /// without bound.
    #[tokio::test]
    async fn a_stopped_job_is_not_recommanded_once_its_stop_succeeds() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        let storage = StorageActor::start(store, 8);
        let hub = Hub::with_storage(config(), storage);
        let session = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["job_runtime".into(), "state_backend".into()],
                boot_id: None,
            })
            .await
            .unwrap();
        hub.upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json: job_spec_json("orders"),
            desired_state: "stopped".into(),
            observed_state: "stopped".into(),
            convergence: "converged".into(),
            generation: 1,
            node_ids: vec!["node-a".into()],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        let auth = AgentAuth {
            node_id: "node-a".into(),
            session_token: session.session_token.clone(),
        };
        let job = hub.jobs().await.unwrap().remove(0);
        let first = hub.reconcile_job(&job).await.unwrap();
        assert_eq!(first, 1, "the first tick dispatches the stop");
        let polled = hub.commands(auth.clone()).await.unwrap();
        assert_eq!(polled.len(), 1);
        hub.command_result(
            auth.clone(),
            CommandResult {
                command_id: polled[0].id.clone(),
                operation_id: polled[0].operation_id.clone(),
                state: HubOperationState::Succeeded,
                progress: 100,
                error: None,
                correlation_id: None,
                generation: job.generation,
                observed_generation: None,
                action_id: None,
                failure_class: None,
                config_version_id: None,
                rollout_id: None,
                observed_checkpoint_id: None,
                checkpoint_manifest_uri: None,
            },
        )
        .await
        .unwrap();
        // The node's command queue is drained; later ticks must NOT enqueue
        // another stop for the settled (job, generation, node).
        for _ in 0..3 {
            let dispatched = hub.reconcile_job(&job).await.unwrap();
            assert_eq!(
                dispatched, 0,
                "a settled stop must not be re-dispatched by later ticks"
            );
        }
        let operations = hub.operations.read().await;
        let stops = operations
            .values()
            .filter(|record| {
                record.resource_id == "orders"
                    && record.operation == "job_stop"
                    && record.node_id == "node-a"
            })
            .count();
        assert_eq!(
            stops, 1,
            "exactly one stop operation record must exist for the settled generation"
        );
    }

    /// One Job whose dispatch fails (here: its node's command queue is at
    /// capacity) must not stall the reconcile tick for every other Job —
    /// the failure is recorded and only that Job is skipped.
    #[tokio::test]
    async fn one_jobs_dispatch_failure_does_not_stall_the_scan() {
        let hub = Hub::new(config());
        for node_id in ["node-a", "node-b"] {
            hub.register(RegisterRequest {
                data_address: None,
                node_id: node_id.into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec![
                    "stream_lifecycle".into(),
                    "job_runtime".into(),
                    "state_backend".into(),
                ],
                boot_id: None,
            })
            .await
            .unwrap();
        }
        let job = |job_id: &str, node_id: &str| JobRecord {
            job_id: job_id.into(),
            version: 1,
            spec_json: job_spec_json(job_id),
            desired_state: "stopped".into(),
            observed_state: "stopped".into(),
            convergence: "converged".into(),
            generation: 1,
            node_ids: vec![node_id.into()],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        };
        hub.upsert_job(job("job-blocked", "node-a")).await.unwrap();
        hub.upsert_job(job("job-healthy", "node-b")).await.unwrap();
        // Fill node-a's command queue to the dispatch bound.
        for index in 0..MAX_COMMANDS_PER_NODE {
            hub.enqueue_with_metadata(
                "node-a".into(),
                "start".into(),
                format!("fill-{index}"),
                None,
                None,
                1,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        }
        // Arm both Jobs for dispatch without going through upsert's eager
        // reconcile (the blocked one would fail the upsert itself).
        {
            let mut jobs = hub.jobs.write().await;
            for job_id in ["job-blocked", "job-healthy"] {
                if let Some(record) = jobs.get_mut(job_id) {
                    record.desired_state = "running".into();
                }
            }
        }
        // The scan must succeed overall: the blocked Job is skipped, the
        // healthy Job still dispatches its start.
        let dispatched = hub.reconcile_jobs().await.unwrap();
        assert!(dispatched >= 1, "the healthy Job must still dispatch");
        let operations = hub.operations.read().await;
        assert!(
            operations.values().any(
                |record| record.resource_id == "job-healthy" && record.operation == "job_start"
            ),
            "the healthy Job's start must be enqueued"
        );
        assert!(
            !operations.values().any(
                |record| record.resource_id == "job-blocked" && record.operation == "job_start"
            ),
            "the blocked Job's start must be skipped, not enqueued"
        );
    }

    /// Operation and checkpoint records must be reclaimed by a bounded
    /// retention: pending/failed checkpoint attempt rows and old terminal
    /// operation rows used to accumulate forever.
    #[tokio::test]
    async fn stale_operation_and_checkpoint_records_are_reclaimed() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        let storage = StorageActor::start(store, 8);
        let hub = Hub::with_storage(config(), storage);
        hub.upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json: job_spec_json("orders"),
            desired_state: "running".into(),
            observed_state: "running".into(),
            convergence: "in_sync".into(),
            generation: 1,
            node_ids: vec![],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        hub.record_job_checkpoint(JobCheckpointRecord {
            job_id: "orders".into(),
            job_version: 1,
            checkpoint_id: "checkpoint-stale-pending".into(),
            kind: "checkpoint".into(),
            status: "pending".into(),
            manifest_uri: None,
            format_version: 1,
            created_at_ms: 1,
            updated_at_ms: 1,
        })
        .await
        .unwrap();
        hub.record_job_checkpoint(JobCheckpointRecord {
            job_id: "orders".into(),
            job_version: 1,
            checkpoint_id: "checkpoint-stale-failed".into(),
            kind: "checkpoint".into(),
            status: "failed".into(),
            manifest_uri: None,
            format_version: 1,
            created_at_ms: 1,
            updated_at_ms: 1,
        })
        .await
        .unwrap();
        let stale_op = HubOperation {
            id: "op-stale".into(),
            intent_id: None,
            command_id: "command-stale".into(),
            node_id: "node-a".into(),
            operation: "job_stop".into(),
            resource_id: "orders".into(),
            checkpoint_id: None,
            generation: 1,
            expires_at_ms: None,
            attempt_id: None,
            config_version_id: None,
            state: HubOperationState::Succeeded,
            progress: 100,
            created_at_ms: 1,
            dispatched_at_ms: None,
            acknowledged_at_ms: None,
            finished_at_ms: Some(1),
            correlation_id: None,
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
        hub.operations
            .write()
            .await
            .insert(stale_op.id.clone(), stale_op.clone());
        hub.prune_stale_checkpoint_records().await.unwrap();
        hub.prune_operation_history().await.unwrap();
        let records = hub.job_checkpoints("orders").await.unwrap();
        assert!(
            records.is_empty(),
            "stale pending/failed checkpoint records must be reclaimed"
        );
        assert!(
            !hub.operations.read().await.contains_key("op-stale"),
            "old terminal operation records must be reclaimed"
        );
    }

    /// The observation write is a compare-and-set on the generation the
    /// caller read: a concurrent desired-state bump must not be rolled back
    /// by a stale report.
    #[tokio::test]
    async fn stale_job_observation_cannot_rollback_generation() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        let storage = StorageActor::start(store, 8);
        let spec_json = serde_json::json!({
            "id": "orders",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        })
        .to_string();
        storage
            .upsert_job(JobRecord {
                job_id: "orders".into(),
                version: 1,
                spec_json,
                desired_state: "running".into(),
                observed_state: "starting".into(),
                convergence: "reconciling".into(),
                generation: 1,
                node_ids: vec![],
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 0,
            })
            .await
            .unwrap();
        let applied = storage
            .update_job_observation("orders", "running", "converged", 1, 1, None, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(applied.generation, 1);
        // A concurrent desired-state change bumps the generation.
        let bumped = storage
            .update_job_desired_state("orders", "stopped", 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bumped.generation, 2);
        // The stale observation (still expecting generation 1) must be
        // rejected instead of writing generation 1 back.
        let conflict = storage
            .update_job_observation("orders", "running", "converged", 1, 1, None, None)
            .await;
        assert!(matches!(
            conflict,
            Err(StorageError::GenerationConflict {
                expected: 1,
                current: 2
            })
        ));
        let current = storage.get_job("orders").await.unwrap().unwrap();
        assert_eq!(current.generation, 2);
        assert_eq!(current.desired_state, "stopped");
        // A fresh report at the current generation still applies.
        let fresh = storage
            .update_job_observation("orders", "stopped", "converged", 2, 2, None, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fresh.generation, 2);
        assert_eq!(fresh.observed_state, "stopped");
    }

    /// Re-placement after a node blip must fence the abandoned node: its
    /// current-generation Succeeded start is marked Superseded and the node
    /// receives a stop when it reappears, instead of being deduped back into
    /// the target set and double-running the Job.
    #[tokio::test]
    async fn replaced_placement_supersedes_abandoned_start_and_stops_it() {
        let hub = Hub::new(config());
        hub.register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["job_runtime".into(), "state_backend".into()],
            boot_id: None,
        })
        .await
        .unwrap();
        let job = hub
            .upsert_job(JobRecord {
                job_id: "orders".into(),
                version: 1,
                spec_json: serde_json::json!({
                    "id": "orders",
                    "version": 1,
                    "operators": [
                        {"id": "source", "kind": "source"},
                        {"id": "sink", "kind": "sink"}
                    ],
                    "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
                    "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
                    "sinks": [{"operator_id": "sink", "output_type": "drop"}]
                })
                .to_string(),
                desired_state: "running".into(),
                observed_state: "stopped".into(),
                convergence: "reconciling".into(),
                generation: 1,
                node_ids: vec![],
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 0,
            })
            .await
            .unwrap();
        // Auto-place on the only online node and let its start succeed.
        hub.reconcile_job(&job).await.unwrap();
        let start_op_id = {
            let operations = hub.operations.read().await;
            operations
                .values()
                .find(|operation| {
                    operation.resource_id == "orders"
                        && operation.operation == "job_start"
                        && operation.node_id == "node-a"
                        && operation.generation == 1
                })
                .map(|operation| operation.id.clone())
                .expect("job_start dispatched to node-a")
        };
        {
            let mut operations = hub.operations.write().await;
            let operation = operations.get_mut(&start_op_id).unwrap();
            operation.state = HubOperationState::Succeeded;
        }
        // node-a loses its lease (partition); node-b joins. The reconciler
        // must move the Job to node-b and fence node-a's stale claim.
        hub.nodes
            .write()
            .await
            .get_mut("node-a")
            .unwrap()
            .resource
            .lease_expires_at_ms = now_ms();
        hub.register(RegisterRequest {
            data_address: None,
            node_id: "node-b".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["job_runtime".into(), "state_backend".into()],
            boot_id: None,
        })
        .await
        .unwrap();
        hub.reconcile_job(&job).await.unwrap();
        {
            let operations = hub.operations.read().await;
            let abandoned = operations
                .values()
                .find(|operation| operation.id == start_op_id)
                .unwrap();
            assert_eq!(
                abandoned.state,
                HubOperationState::Superseded,
                "the abandoned placement must be fenced"
            );
            assert!(operations.values().any(|operation| {
                operation.node_id == "node-b"
                    && operation.operation == "job_start"
                    && operation.generation == 1
                    && operation.state == HubOperationState::Queued
            }));
        }
        // node-b's start succeeds; node-a reappears. The reconciler must NOT
        // dedupe node-a back into the target set: it receives a stop.
        {
            let mut operations = hub.operations.write().await;
            for operation in operations.values_mut() {
                if operation.node_id == "node-b"
                    && operation.operation == "job_start"
                    && operation.generation == 1
                {
                    operation.state = HubOperationState::Succeeded;
                }
            }
        }
        hub.nodes
            .write()
            .await
            .get_mut("node-a")
            .unwrap()
            .resource
            .lease_expires_at_ms = now_ms() + config().lease_ttl_ms;
        hub.reconcile_job(&job).await.unwrap();
        let nodes = hub.nodes.read().await;
        let node_a = nodes.get("node-a").unwrap();
        assert!(
            node_a
                .commands
                .iter()
                .any(|command| command.operation == "job_stop"),
            "the abandoned node must receive a stop command"
        );
        assert_eq!(
            node_a
                .commands
                .iter()
                .filter(|command| command.operation == "job_start")
                .count(),
            1,
            "only the original start remains queued; the abandoned node must not be re-targeted"
        );
        assert!(
            !nodes
                .get("node-b")
                .unwrap()
                .commands
                .iter()
                .any(|command| command.operation == "job_stop"),
            "the live placement must keep running"
        );
    }

    fn config() -> HubConfig {
        HubConfig {
            operator_token: Some("operator".into()),
            node_token: Some("node-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 1000,
            poll_interval_ms: 10,
            session_ttl_ms: default_session_ttl_ms(),
        }
    }

    #[tokio::test]
    async fn secure_hub_fails_closed_for_missing_credentials_without_mutation() {
        let hub = Hub::new(HubConfig {
            operator_token: None,
            node_token: None,
            insecure_local: false,
            lease_ttl_ms: 1_000,
            poll_interval_ms: 10,
            session_ttl_ms: default_session_ttl_ms(),
        });
        assert!(!hub.operator_authorized(None));
        let result = hub
            .register(RegisterRequest {
                node_id: "unauthorized-node".into(),
                node_token: String::new(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: vec![],
                boot_id: None,
                data_address: None,
            })
            .await;
        assert!(matches!(result, Err(HubError::Unauthorized)));
        assert!(hub.nodes().await.is_empty());
    }

    #[test]
    fn node_metrics_accept_only_finite_whitelisted_values() {
        let metrics = sanitize_metrics(BTreeMap::from([
            ("input_messages".into(), 4.0),
            ("arbitrary_label".into(), 99.0),
            ("output_errors".into(), f64::NAN),
            ("restarts".into(), -1.0),
        ]));
        assert_eq!(metrics.get("input_messages"), Some(&4.0));
        assert!(!metrics.contains_key("arbitrary_label"));
        assert!(!metrics.contains_key("output_errors"));
        assert!(!metrics.contains_key("restarts"));
    }

    #[test]
    fn resource_gauges_pass_the_whitelist_into_the_node_view() {
        let metrics = sanitize_metrics(BTreeMap::from([
            ("node_cpu_usage_percent".into(), 37.5),
            ("node_memory_used_bytes".into(), 1_000.0),
            ("node_memory_total_bytes".into(), 8_000.0),
            ("node_memory_available_bytes".into(), 7_000.0),
            ("node_not_a_real_gauge".into(), 1.0),
        ]));
        assert_eq!(
            metrics,
            BTreeMap::from([
                ("node_cpu_usage_percent".to_string(), 37.5),
                ("node_memory_used_bytes".to_string(), 1_000.0),
                ("node_memory_total_bytes".to_string(), 8_000.0),
                ("node_memory_available_bytes".to_string(), 7_000.0),
            ])
        );
    }

    async fn register_and_report_resources(hub: &Hub, report_seq: u64) {
        let session = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: vec![],
                boot_id: None,
            })
            .await
            .unwrap();
        hub.report(NodeReport {
            auth: AgentAuth {
                node_id: "n1".into(),
                session_token: session.session_token,
            },
            version: "test".into(),
            state: "online".into(),
            capabilities: vec![],
            streams: vec![],
            operations: vec![],
            events: vec![],
            metrics: BTreeMap::from([
                ("node_cpu_usage_percent".to_string(), 12.5),
                ("node_memory_total_bytes".to_string(), 16_000.0),
            ]),
            jobs: BTreeMap::new(),
            configuration: None,
            configuration_version: None,
            boot_id: None,
            report_seq,
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn reported_resource_gauges_surface_in_the_node_metrics_view() {
        let hub = Hub::new(config());
        register_and_report_resources(&hub, 1).await;
        let view = hub
            .metrics_by_node(Some("n1"))
            .await
            .into_iter()
            .next()
            .expect("node view");
        assert_eq!(view.metrics.get("node_cpu_usage_percent"), Some(&12.5));
        assert_eq!(view.metrics.get("node_memory_total_bytes"), Some(&16_000.0));
    }

    #[tokio::test]
    async fn resource_gauges_are_ephemeral_across_a_hub_restart() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        let storage = crate::storage::StorageActor::start(store, 8);
        let hub1 = Hub::with_storage(config(), storage.clone());
        register_and_report_resources(&hub1, 1).await;
        assert!(
            hub1.metrics_by_node(Some("n1"))
                .await
                .into_iter()
                .next()
                .expect("node view")
                .metrics
                .contains_key("node_cpu_usage_percent")
        );
        // After a restart the registry starts empty and the reconnecting
        // node re-registers with an empty gauge set: gauges only reappear
        // with the node's next report. No durable gauge history exists.
        let hub2 = Hub::with_storage(config(), storage);
        assert!(hub2.metrics_by_node(Some("n1")).await.is_empty());
        let session = hub2
            .register(RegisterRequest {
                data_address: None,
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: vec![],
                boot_id: None,
            })
            .await
            .unwrap();
        let view = hub2
            .metrics_by_node(Some("n1"))
            .await
            .into_iter()
            .next()
            .expect("re-registered node view");
        assert!(!view.metrics.keys().any(|key| key.starts_with("node_")));
        register_and_report_resources(&hub2, 2).await;
        let view = hub2
            .metrics_by_node(Some("n1"))
            .await
            .into_iter()
            .next()
            .expect("reported node view");
        assert!(view.metrics.contains_key("node_cpu_usage_percent"));
        drop(session);
    }

    #[test]
    fn capability_allowlist_is_bounded_and_label_safe() {
        let long = "x".repeat(65);
        let capabilities =
            sanitize_capabilities(vec!["configuration".into(), "unsafe label".into(), long]);
        assert_eq!(capabilities, vec!["configuration"]);
    }

    #[test]
    fn compatibility_token_can_be_scoped_to_a_role() {
        let hub = Hub::new(HubConfig {
            operator_token: Some("readonly|viewer|viewer-secret".into()),
            ..config()
        });
        assert!(hub.operator_authorized(Some("viewer-secret")));
        assert!(hub.operator_can(Some("viewer-secret"), OperatorAction::Read));
        assert!(!hub.operator_can(Some("viewer-secret"), OperatorAction::Operate));
        assert!(!hub.operator_authorized(Some("operator")));
    }

    #[test]
    fn operator_credential_can_limit_resource_scope() {
        let hub = Hub::new(HubConfig {
            operator_token: Some("ops|operator|operator-secret|node=node-a,rollout=".into()),
            ..config()
        });
        assert!(hub.operator_can_scope(
            Some("operator-secret"),
            OperatorAction::Operate,
            "node",
            Some("node-a")
        ));
        assert!(!hub.operator_can_scope(
            Some("operator-secret"),
            OperatorAction::Operate,
            "node",
            Some("node-b")
        ));
        assert!(hub.operator_can_scope(
            Some("operator-secret"),
            OperatorAction::ManageRollouts,
            "rollout",
            Some("rollout-1")
        ));
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
            required_capabilities: vec!["stream_lifecycle".into()],
            rollout_id: None,
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
            jobs: BTreeMap::new(),
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
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["stream_lifecycle".into()],
            boot_id: None,
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
    async fn rollout_actions_are_durable_and_audited() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES ('cfg-current', 'digest', '{}', 'json', 1)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let storage = StorageActor::start(store, 8);
        let hub = Hub::with_storage(config(), storage);
        let rollout = hub
            .create_rollout(
                "cfg-current".into(),
                vec!["node-a".into(), "node-b".into()],
                1,
                Some("operator".into()),
                Some("corr-1".into()),
            )
            .await
            .unwrap();

        let paused = hub
            .act_rollout(
                &rollout.rollout_id,
                "pause",
                None,
                Some("operator".into()),
                Some("corr-2".into()),
            )
            .await
            .unwrap();
        assert_eq!(paused.state, "paused");
        let resumed = hub
            .act_rollout(
                &rollout.rollout_id,
                "resume",
                None,
                Some("operator".into()),
                Some("corr-3".into()),
            )
            .await
            .unwrap();
        assert_eq!(resumed.state, "applying");
        let cancelled = hub
            .act_rollout(
                &rollout.rollout_id,
                "cancel",
                None,
                Some("operator".into()),
                Some("corr-4".into()),
            )
            .await
            .unwrap();
        assert_eq!(cancelled.state, "cancelled");
        let persisted = hub.rollout(&rollout.rollout_id).await.unwrap().unwrap();
        assert_eq!(persisted.0.state, "cancelled");
        assert_eq!(
            persisted
                .1
                .iter()
                .filter(|target| target.state == "cancelled")
                .count(),
            2
        );
        assert_eq!(hub.audit(Some(&rollout.rollout_id)).await.unwrap().len(), 4);
    }

    #[tokio::test]
    async fn rollout_reconciler_dispatches_only_the_current_batch() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES ('cfg-batch', 'digest', '{}', 'json', 1)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let hub = Hub::with_storage(config(), StorageActor::start(store, 8));
        for node_id in ["node-a", "node-b"] {
            hub.register(RegisterRequest {
                data_address: None,
                node_id: node_id.into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["configuration".into()],
                boot_id: None,
            })
            .await
            .unwrap();
        }
        let rollout = hub
            .create_rollout(
                "cfg-batch".into(),
                vec!["node-a".into(), "node-b".into()],
                1,
                Some("operator".into()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(hub.reconcile_rollouts().await.unwrap(), 2);
        let (_, targets) = hub.rollout(&rollout.rollout_id).await.unwrap().unwrap();
        assert_eq!(targets[0].state, "applying");
        assert_eq!(targets[1].state, "pending");
        assert_eq!(
            hub.rollout(&rollout.rollout_id)
                .await
                .unwrap()
                .unwrap()
                .0
                .current_batch,
            0
        );
    }

    #[tokio::test]
    async fn rollout_converges_only_after_target_configuration_is_observed() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES ('cfg-health', 'digest', '{}', 'json', 1)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let hub = Hub::with_storage(config(), StorageActor::start(store, 8));
        let session = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["configuration".into()],
                boot_id: None,
            })
            .await
            .unwrap();
        let rollout = hub
            .create_rollout(
                "cfg-health".into(),
                vec!["node-a".into()],
                1,
                Some("operator".into()),
                None,
            )
            .await
            .unwrap();
        hub.reconcile_rollouts().await.unwrap();
        assert_eq!(
            hub.rollout(&rollout.rollout_id)
                .await
                .unwrap()
                .unwrap()
                .0
                .state,
            "applying"
        );
        hub.report(NodeReport {
            auth: AgentAuth {
                node_id: "node-a".into(),
                session_token: session.session_token.clone(),
            },
            version: "agent-1".into(),
            state: "online".into(),
            capabilities: vec!["configuration".into()],
            streams: vec![],
            operations: vec![],
            events: vec![],
            metrics: BTreeMap::new(),
            jobs: BTreeMap::new(),
            configuration: None,
            configuration_version: Some("cfg-health".into()),
            boot_id: Some(session.session_token.clone()),
            report_seq: 1,
        })
        .await
        .unwrap();
        hub.reconcile_rollouts().await.unwrap();
        let (rollout, targets) = hub.rollout(&rollout.rollout_id).await.unwrap().unwrap();
        assert_eq!(targets[0].state, "succeeded");
        assert_eq!(rollout.state, "converged");
    }

    #[tokio::test]
    async fn rollout_state_machine_covers_gates_drain_restart_rollback_and_cancel() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        store
            .with_connection(|connection| {
                for version in ["cfg-state-a", "cfg-state-b"] {
                    connection.execute(
                        "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES (?1, 'digest', '{}', 'json', 1)",
                        [version],
                    )?;
                }
                Ok(())
            })
            .unwrap();
        let storage = StorageActor::start(store, 8);
        let hub = Hub::with_storage(config(), storage.clone());
        let node_a = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["configuration".into()],
                boot_id: None,
            })
            .await
            .unwrap();
        hub.register(RegisterRequest {
            data_address: None,
            node_id: "node-b".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["configuration".into()],
            boot_id: None,
        })
        .await
        .unwrap();
        hub.set_node_maintenance(
            "node-b",
            NodeMaintenanceState::Draining,
            Some("operator".into()),
            Some("drain-state".into()),
        )
        .await
        .unwrap();

        let rollout = hub
            .create_rollout(
                "cfg-state-a".into(),
                vec!["node-a".into(), "node-b".into()],
                1,
                Some("operator".into()),
                Some("state-machine".into()),
            )
            .await
            .unwrap();
        hub.reconcile_rollouts().await.unwrap();
        let (_, targets) = hub.rollout(&rollout.rollout_id).await.unwrap().unwrap();
        assert_eq!(targets[0].state, "applying");
        assert_eq!(targets[1].state, "pending");

        let paused = hub
            .act_rollout(
                &rollout.rollout_id,
                "pause",
                None,
                Some("operator".into()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(paused.state, "paused");
        let resumed = hub
            .act_rollout(
                &rollout.rollout_id,
                "resume",
                None,
                Some("operator".into()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resumed.state, "applying");

        hub.report(NodeReport {
            auth: AgentAuth {
                node_id: "node-a".into(),
                session_token: node_a.session_token.clone(),
            },
            version: "agent-state".into(),
            state: "online".into(),
            capabilities: vec!["configuration".into()],
            streams: vec![],
            operations: vec![],
            events: vec![],
            metrics: BTreeMap::new(),
            jobs: BTreeMap::new(),
            configuration: None,
            configuration_version: Some("cfg-state-a".into()),
            boot_id: Some(node_a.session_token.clone()),
            report_seq: 1,
        })
        .await
        .unwrap();
        hub.reconcile_rollouts().await.unwrap();
        hub.set_node_maintenance(
            "node-b",
            NodeMaintenanceState::Active,
            Some("operator".into()),
            Some("resume-state".into()),
        )
        .await
        .unwrap();
        hub.reconcile_rollouts().await.unwrap();
        let (_, targets) = hub.rollout(&rollout.rollout_id).await.unwrap().unwrap();
        assert_eq!(targets[0].state, "succeeded");
        assert_eq!(targets[1].state, "applying");

        let rollback = hub
            .act_rollout(
                &rollout.rollout_id,
                "rollback",
                Some("cfg-state-b".into()),
                Some("operator".into()),
                Some("rollback-state".into()),
            )
            .await
            .unwrap();
        assert_eq!(rollback.total_targets, 2);
        assert_eq!(
            hub.rollout(&rollout.rollout_id)
                .await
                .unwrap()
                .unwrap()
                .0
                .state,
            "rolled_back"
        );

        let failed = hub
            .create_rollout(
                "cfg-state-a".into(),
                vec!["node-a".into()],
                1,
                Some("operator".into()),
                Some("permanent-failure".into()),
            )
            .await
            .unwrap();
        storage
            .update_rollout_target(RolloutTargetUpdate {
                rollout_id: failed.rollout_id.clone(),
                node_id: "node-a".into(),
                state: "failed".into(),
                attempt_id: None,
                error: Some("permanent_execution".into()),
                observed_config_version: None,
                updated_at_ms: now_ms(),
            })
            .await
            .unwrap();
        hub.reconcile_rollouts().await.unwrap();
        assert_eq!(
            hub.rollout(&failed.rollout_id)
                .await
                .unwrap()
                .unwrap()
                .0
                .state,
            "paused"
        );

        drop(hub);
        let recovered = Hub::with_storage(config(), storage);
        recovered.recover_persisted_state().await.unwrap();
        assert_eq!(
            recovered
                .rollout(&rollback.rollout_id)
                .await
                .unwrap()
                .unwrap()
                .0
                .state,
            "applying"
        );
        let cancelled = recovered
            .act_rollout(
                &rollback.rollout_id,
                "cancel",
                None,
                Some("operator".into()),
                Some("cancel-state".into()),
            )
            .await
            .unwrap();
        assert_eq!(cancelled.state, "cancelled");
    }

    #[tokio::test]
    async fn multiple_agent_rollout_smoke_completes_through_commands_and_reports() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES ('cfg-e2e', 'digest', '{}', 'json', 1)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let hub = Hub::with_storage(config(), StorageActor::start(store, 8));
        let mut sessions = Vec::new();
        for node_id in ["agent-a", "agent-b"] {
            let session = hub
                .register(RegisterRequest {
                    data_address: None,
                    node_id: node_id.into(),
                    node_token: "node-secret".into(),
                    protocol_version: "v1".into(),
                    capabilities: vec!["configuration".into()],
                    boot_id: None,
                })
                .await
                .unwrap();
            sessions.push((node_id.to_owned(), session.session_token));
        }
        let rollout = hub
            .create_rollout(
                "cfg-e2e".into(),
                vec!["agent-a".into(), "agent-b".into()],
                2,
                Some("operator".into()),
                Some("e2e-rollout".into()),
            )
            .await
            .unwrap();
        assert_eq!(hub.reconcile_rollouts().await.unwrap(), 3);

        for (node_id, session_token) in sessions {
            let worker_id = format!("e2e-reconcile-{node_id}");
            let operation = hub.reconcile_once(&worker_id).await.unwrap().unwrap();
            let auth = AgentAuth {
                node_id: node_id.clone(),
                session_token,
            };
            let commands = hub.commands(auth.clone()).await.unwrap();
            assert_eq!(commands.len(), 1);
            assert_eq!(
                commands[0].rollout_id.as_deref(),
                Some(rollout.rollout_id.as_str())
            );
            hub.command_result(
                auth.clone(),
                CommandResult {
                    command_id: commands[0].id.clone(),
                    operation_id: operation.id,
                    state: HubOperationState::Succeeded,
                    progress: 100,
                    error: None,
                    correlation_id: commands[0].correlation_id.clone(),
                    generation: commands[0].generation,
                    observed_generation: None,
                    action_id: commands[0].action_id.clone(),
                    failure_class: None,
                    config_version_id: Some("cfg-e2e".into()),
                    rollout_id: commands[0].rollout_id.clone(),
                    observed_checkpoint_id: None,
                    checkpoint_manifest_uri: None,
                },
            )
            .await
            .unwrap();
            hub.report(NodeReport {
                auth: auth.clone(),
                version: "agent-e2e".into(),
                state: "online".into(),
                capabilities: vec!["configuration".into()],
                streams: vec![],
                operations: vec![],
                events: vec![],
                metrics: BTreeMap::from([("streams_total".into(), 0.0)]),
                jobs: BTreeMap::new(),
                configuration: None,
                configuration_version: Some("cfg-e2e".into()),
                boot_id: Some(auth.session_token.clone()),
                report_seq: 1,
            })
            .await
            .unwrap();
        }
        hub.reconcile_rollouts().await.unwrap();
        let (rollout, targets) = hub.rollout(&rollout.rollout_id).await.unwrap().unwrap();
        assert_eq!(rollout.state, "converged");
        assert!(targets.iter().all(|target| target.state == "succeeded"));
    }

    #[tokio::test]
    async fn dispatched_attempt_waits_for_fresh_report_after_hub_restart() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        let storage = StorageActor::start(store, 8);
        let hub1 = Hub::with_storage(config(), storage.clone());
        let session1 = hub1
            .register(RegisterRequest {
                data_address: None,
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["stream_lifecycle".into()],
                boot_id: None,
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
                data_address: None,
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["stream_lifecycle".into()],
                boot_id: None,
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
                session_token: session2.session_token.clone(),
            },
            version: "test".into(),
            state: "online".into(),
            capabilities: vec!["stream_lifecycle".into()],
            streams: vec![stopped_report("orders", Some(0))],
            operations: vec![],
            events: vec![],
            metrics: BTreeMap::new(),
            jobs: BTreeMap::new(),
            configuration: None,
            configuration_version: None,
            boot_id: Some(session2.session_token.clone()),
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
                data_address: None,
                node_id: "n1".into(),
                node_token: "bad".into(),
                protocol_version: "v1".into(),
                capabilities: vec![],
                boot_id: None,
            })
            .await,
            Err(HubError::Unauthorized)
        ));
        let session = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["stream_lifecycle".into()],
                boot_id: None,
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
            jobs: BTreeMap::new(),
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
                    operation_id: first.id.clone(),
                    state: HubOperationState::Succeeded,
                    progress: 100,
                    error: None,
                    correlation_id: Some("corr".into()),
                    generation: first.generation,
                    observed_generation: None,
                    action_id: None,
                    failure_class: None,
                    config_version_id: None,
                    rollout_id: None,
                    observed_checkpoint_id: None,
                    checkpoint_manifest_uri: None,
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
                data_address: None,
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec![],
                boot_id: None,
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

    fn job_snapshot(rows: u64) -> arkflow_core::executor::metrics::KernelMetricsSnapshot {
        let mut chains = BTreeMap::new();
        chains.insert(
            "src".to_string(),
            arkflow_core::executor::metrics::ChainMetricsSnapshot {
                rows,
                ..Default::default()
            },
        );
        arkflow_core::executor::metrics::KernelMetricsSnapshot {
            chains,
            ..Default::default()
        }
    }

    /// An Agent that predates the per-Job reporting field (empty map after
    /// serde defaults) must not produce any data-plane series.
    #[tokio::test]
    async fn report_without_job_snapshots_exports_no_data_plane_series() {
        let hub = Hub::new(config());
        let session = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec![],
                boot_id: None,
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
            capabilities: vec![],
            streams: vec![],
            operations: vec![],
            events: vec![],
            metrics: BTreeMap::new(),
            jobs: BTreeMap::new(),
            configuration: None,
            configuration_version: None,
            boot_id: None,
            report_seq: 1,
        })
        .await
        .unwrap();
        assert!(hub.job_metrics().await.is_empty());
    }

    /// Two reporting Agents produce the same series vocabulary distinguished
    /// by the `node` label, per (node, job) granularity.
    #[tokio::test]
    async fn reported_job_metrics_carry_node_and_job_labels() {
        let hub = Hub::new(config());
        let mut sessions = BTreeMap::new();
        for node_id in ["node-a", "node-b"] {
            let session = hub
                .register(RegisterRequest {
                    data_address: None,
                    node_id: node_id.into(),
                    node_token: "node-secret".into(),
                    protocol_version: "v1".into(),
                    capabilities: vec![],
                    boot_id: None,
                })
                .await
                .unwrap();
            sessions.insert(node_id.to_string(), session.session_token.clone());
            hub.report(NodeReport {
                auth: AgentAuth {
                    node_id: node_id.into(),
                    session_token: session.session_token.clone(),
                },
                version: "test".into(),
                state: "online".into(),
                capabilities: vec![],
                streams: vec![],
                operations: vec![],
                events: vec![],
                metrics: BTreeMap::new(),
                jobs: BTreeMap::from([(
                    "job-1".into(),
                    job_snapshot(if node_id == "node-a" { 7 } else { 9 }),
                )]),
                configuration: None,
                configuration_version: None,
                boot_id: None,
                report_seq: 1,
            })
            .await
            .unwrap();
        }
        let exported = hub.job_metrics().await;
        assert_eq!(exported.len(), 2);
        for (node_id, jobs) in &exported {
            let snapshot = jobs.get("job-1").expect("job-1 snapshot stored");
            let expected_rows = if node_id == "node-a" { 7 } else { 9 };
            assert_eq!(snapshot.chains["src"].rows, expected_rows);
            let text = crate::metrics::encode_families(crate::metrics::kernel_job_families(
                "job-1",
                snapshot,
                &[("node", node_id.clone())],
            ));
            assert!(
                text.contains(&format!("node=\"{node_id}\"")),
                "series must carry the node label"
            );
        }
    }

    /// An Agent whose lease expires stops being exported while a live peer's
    /// series remain.
    #[tokio::test]
    async fn expired_lease_stops_data_plane_export() {
        let hub = Hub::new(config());
        let mut sessions = BTreeMap::new();
        for node_id in ["n1", "n2"] {
            let session = hub
                .register(RegisterRequest {
                    data_address: None,
                    node_id: node_id.into(),
                    node_token: "node-secret".into(),
                    protocol_version: "v1".into(),
                    capabilities: vec![],
                    boot_id: None,
                })
                .await
                .unwrap();
            sessions.insert(node_id.to_string(), session.session_token.clone());
            hub.report(NodeReport {
                auth: AgentAuth {
                    node_id: node_id.into(),
                    session_token: session.session_token.clone(),
                },
                version: "test".into(),
                state: "online".into(),
                capabilities: vec![],
                streams: vec![],
                operations: vec![],
                events: vec![],
                metrics: BTreeMap::new(),
                jobs: BTreeMap::from([(format!("{node_id}-job"), job_snapshot(1))]),
                configuration: None,
                configuration_version: None,
                boot_id: None,
                report_seq: 1,
            })
            .await
            .unwrap();
        }
        assert_eq!(hub.job_metrics().await.len(), 2);

        // n1's lease lapses deterministically; a tiny TTL would race the
        // wall clock across the registration awaits above.
        hub.nodes
            .write()
            .await
            .get_mut("n1")
            .unwrap()
            .resource
            .lease_expires_at_ms = now_ms();
        hub.heartbeat(HeartbeatRequest {
            auth: AgentAuth {
                node_id: "n2".into(),
                session_token: sessions["n2"].clone(),
            },
            state: "online".into(),
            protocol_version: Some("v1".into()),
            software_version: None,
            capabilities: vec![],
            rollout_id: None,
        })
        .await
        .unwrap();

        let exported = hub.job_metrics().await;
        assert_eq!(exported.len(), 1, "only the live node keeps exporting");
        assert_eq!(exported[0].0, "n2");
        assert!(exported[0].1.contains_key("n2-job"));
    }

    #[tokio::test]
    async fn unsupported_capability_is_rejected_before_dispatch() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        let storage = crate::storage::StorageActor::start(store, 8);
        let hub = Hub::with_storage(config(), storage.clone());
        assert!(matches!(
            hub.register(RegisterRequest {
                data_address: None,
                node_id: "incompatible".into(),
                node_token: "node-secret".into(),
                protocol_version: "v0".into(),
                capabilities: vec![],
                boot_id: None,
            })
            .await,
            Err(HubError::Invalid(message)) if message.contains("protocol")
        ));
        let protocol_audit = hub.audit(Some("incompatible")).await.unwrap();
        assert_eq!(
            protocol_audit[0].failure_code.as_deref(),
            Some("incompatible_protocol")
        );
        hub.register(RegisterRequest {
            data_address: None,
            node_id: "n1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["configuration".into()],
            boot_id: None,
        })
        .await
        .unwrap();
        assert!(matches!(
            hub.enqueue("n1".into(), "start".into(), "orders".into(), None)
                .await,
            Err(HubError::Invalid(message)) if message.contains("capability")
        ));
        let audit = hub.audit(Some("orders")).await.unwrap();
        assert_eq!(audit.len(), 1);
        assert_eq!(
            audit[0].failure_code.as_deref(),
            Some("incompatible_capability")
        );
        assert_eq!(audit[0].outcome, "rejected");
    }

    #[tokio::test]
    async fn ignores_replayed_reports_from_the_same_boot() {
        let hub = Hub::new(config());
        let session = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec![],
                boot_id: None,
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
            jobs: BTreeMap::new(),
            configuration: None,
            configuration_version: None,
            boot_id: Some(session.session_token.clone()),
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
                data_address: None,
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec![],
                boot_id: None,
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
                data_address: None,
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["first".into()],
                boot_id: None,
            })
            .await
            .unwrap();
        let second = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["second".into()],
                boot_id: None,
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
                protocol_version: None,
                software_version: None,
                capabilities: vec![],
                rollout_id: None,
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
            protocol_version: None,
            software_version: None,
            capabilities: vec![],
            rollout_id: None,
        })
        .await
        .unwrap();
        assert_eq!(hub.nodes().await.len(), 1);
    }

    #[tokio::test]
    async fn command_queues_are_bounded_and_isolated_per_node() {
        let hub = Hub::new(config());
        hub.register(RegisterRequest {
            data_address: None,
            node_id: "n1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec![],
            boot_id: None,
        })
        .await
        .unwrap();
        let n2_session = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "n2".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec![],
                boot_id: None,
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

    #[tokio::test]
    async fn job_observation_rejects_stale_generation() {
        let hub = Hub::new(config());
        let spec_json = serde_json::json!({
            "id": "orders",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{
                "operator_id": "source",
                "input_type": "memory",
                "time": {"mode": "processing_time"}
            }],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        })
        .to_string();
        hub.upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json,
            desired_state: "running".into(),
            observed_state: "starting".into(),
            convergence: "reconciling".into(),
            generation: 3,
            node_ids: vec!["n1".into()],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        let stale = hub
            .observe_job("orders", 2, "stopped", None, Some("stale"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stale.generation, 3);
        assert_eq!(stale.observed_state, "starting");
        let future = hub
            .observe_job("orders", 4, "running", Some("forged"), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(future.generation, 3);
        assert_eq!(future.checkpoint_id, None);
        let converged = hub
            .observe_job("orders", 3, "running", Some("cp-1"), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(converged.convergence, "converged");
        assert_eq!(converged.checkpoint_id.as_deref(), Some("cp-1"));
    }

    #[tokio::test]
    async fn concurrent_job_generation_updates_use_compare_and_swap() {
        let hub = Hub::new(config());
        let spec_json = serde_json::json!({
            "id": "orders",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        }).to_string();
        hub.upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json,
            desired_state: "stopped".into(),
            observed_state: "stopped".into(),
            convergence: "converged".into(),
            generation: 3,
            node_ids: Vec::new(),
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        let (first, second) = tokio::join!(
            hub.update_job_desired_state("orders", "running", 3),
            hub.update_job_desired_state("orders", "stopped", 3),
        );
        assert!(matches!(
            (first, second),
            (Ok(Some(_)), Err(HubError::GenerationConflict { .. }))
                | (Err(HubError::GenerationConflict { .. }), Ok(Some(_)))
        ));
        let current = hub.job("orders").await.unwrap().unwrap();
        assert_eq!(current.generation, 4);
        assert_eq!(current.convergence, "reconciling");
    }

    #[tokio::test]
    async fn replacing_a_job_preserves_generation_fencing() {
        let hub = Hub::new(config());
        let original = hub
            .upsert_job(JobRecord {
                job_id: "orders".into(),
                version: 1,
                spec_json: "{}".into(),
                desired_state: "stopped".into(),
                observed_state: "stopped".into(),
                convergence: "converged".into(),
                generation: 6,
                node_ids: Vec::new(),
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 1,
            })
            .await
            .unwrap();
        assert_eq!(original.generation, 6);

        let replacement = hub
            .upsert_job(JobRecord {
                job_id: "orders".into(),
                version: 2,
                spec_json: "{}".into(),
                desired_state: "stopped".into(),
                observed_state: "validated".into(),
                convergence: "pending".into(),
                generation: 1,
                node_ids: Vec::new(),
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 2,
            })
            .await
            .unwrap();
        assert_eq!(replacement.generation, 7);
        assert_eq!(hub.job("orders").await.unwrap(), Some(replacement));
    }

    #[tokio::test]
    async fn running_job_is_dispatched_to_compatible_agent() {
        let hub = Hub::new(config());
        let registration = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "compute-1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["job_runtime".into(), "state_backend".into()],
                boot_id: None,
            })
            .await
            .unwrap();
        let spec_json = serde_json::json!({
            "id": "orders",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        })
        .to_string();

        hub.upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json,
            desired_state: "running".into(),
            observed_state: "starting".into(),
            convergence: "reconciling".into(),
            generation: 7,
            node_ids: Vec::new(),
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();

        let commands = hub
            .commands(AgentAuth {
                node_id: "compute-1".into(),
                session_token: registration.session_token.clone(),
            })
            .await
            .unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].operation, "job_start");
        assert_eq!(commands[0].resource_id, "orders");
        assert_eq!(commands[0].generation, 7);
        assert_eq!(
            commands[0].required_capabilities,
            vec!["job_runtime", "state_backend"]
        );
        assert_eq!(
            commands[0]
                .payload
                .as_ref()
                .and_then(|payload| payload.get("assignments"))
                .and_then(serde_json::Value::as_array)
                .map(Vec::len),
            Some(2)
        );

        let start_command = commands[0].clone();
        hub.command_result(
            AgentAuth {
                node_id: "compute-1".into(),
                session_token: registration.session_token.clone(),
            },
            CommandResult {
                command_id: start_command.id.clone(),
                operation_id: start_command.operation_id.clone(),
                state: HubOperationState::Succeeded,
                progress: 100,
                error: None,
                correlation_id: start_command.correlation_id.clone(),
                generation: start_command.generation,
                observed_generation: Some(start_command.generation),
                action_id: None,
                failure_class: None,
                config_version_id: None,
                rollout_id: None,
                observed_checkpoint_id: None,
                checkpoint_manifest_uri: None,
            },
        )
        .await
        .unwrap();
        let job = hub.job("orders").await.unwrap().unwrap();
        hub.reconcile_job(&job).await.unwrap();
        let commands = hub
            .commands(AgentAuth {
                node_id: "compute-1".into(),
                session_token: registration.session_token.clone(),
            })
            .await
            .unwrap();
        assert!(!commands
            .iter()
            .any(|command| command.operation == "job_start"));

        hub.record_job_checkpoint(JobCheckpointRecord {
            job_id: "orders".into(),
            job_version: 1,
            checkpoint_id: "checkpoint-7".into(),
            kind: "checkpoint".into(),
            status: "pending".into(),
            manifest_uri: None,
            format_version: 1,
            created_at_ms: 0,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        let commands = hub
            .commands(AgentAuth {
                node_id: "compute-1".into(),
                session_token: registration.session_token.clone(),
            })
            .await
            .unwrap();
        assert!(commands.iter().any(|command| {
            command.operation == "job_checkpoint"
                && command
                    .payload
                    .as_ref()
                    .and_then(|payload| payload.get("checkpoint_id"))
                    .and_then(serde_json::Value::as_str)
                    == Some("checkpoint-7")
        }));
        let checkpoint_command = commands
            .iter()
            .find(|command| command.operation == "job_checkpoint")
            .unwrap();
        assert_eq!(
            hub.operation(&checkpoint_command.operation_id)
                .await
                .unwrap()
                .checkpoint_id
                .as_deref(),
            Some("checkpoint-7")
        );
        hub.command_result(
            AgentAuth {
                node_id: "compute-1".into(),
                session_token: registration.session_token.clone(),
            },
            CommandResult {
                command_id: checkpoint_command.id.clone(),
                operation_id: checkpoint_command.operation_id.clone(),
                state: HubOperationState::Succeeded,
                progress: 100,
                error: None,
                correlation_id: checkpoint_command.correlation_id.clone(),
                generation: checkpoint_command.generation,
                observed_generation: Some(checkpoint_command.generation),
                action_id: None,
                failure_class: None,
                config_version_id: None,
                rollout_id: None,
                observed_checkpoint_id: Some("checkpoint-7".into()),
                checkpoint_manifest_uri: Some("/tmp/checkpoint-7/manifest.json".into()),
            },
        )
        .await
        .unwrap();

        let commands = hub
            .commands(AgentAuth {
                node_id: "compute-1".into(),
                session_token: registration.session_token.clone(),
            })
            .await
            .unwrap();
        let commit_command = commands
            .iter()
            .find(|command| command.operation == "job_checkpoint_commit")
            .expect("checkpoint commit command");
        assert_eq!(
            commit_command
                .payload
                .as_ref()
                .and_then(|payload| payload.get("checkpoint_id"))
                .and_then(serde_json::Value::as_str),
            Some("checkpoint-7")
        );
        assert_eq!(
            commit_command
                .payload
                .as_ref()
                .and_then(|payload| payload.get("manifest_nodes"))
                .and_then(serde_json::Value::as_array)
                .map(|nodes| nodes.len()),
            Some(1)
        );
        hub.command_result(
            AgentAuth {
                node_id: "compute-1".into(),
                session_token: registration.session_token,
            },
            CommandResult {
                command_id: commit_command.id.clone(),
                operation_id: commit_command.operation_id.clone(),
                state: HubOperationState::Succeeded,
                progress: 100,
                error: None,
                correlation_id: commit_command.correlation_id.clone(),
                generation: commit_command.generation,
                observed_generation: Some(commit_command.generation),
                action_id: None,
                failure_class: None,
                config_version_id: None,
                rollout_id: None,
                observed_checkpoint_id: Some("checkpoint-7".into()),
                checkpoint_manifest_uri: Some("/tmp/final/checkpoint-7/manifest.json".into()),
            },
        )
        .await
        .unwrap();
        let records = hub.job_checkpoints("orders").await.unwrap();
        assert_eq!(records[0].status, "completed");
        assert_eq!(
            records[0].manifest_uri.as_deref(),
            Some("/tmp/final/checkpoint-7/manifest.json")
        );

        // A new process has an empty local JobRuntime. Its new boot identity
        // must invalidate the old successful start and trigger reconciliation
        // instead of treating the absent local Job as already running.
        let restarted = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "compute-1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["job_runtime".into(), "state_backend".into()],
                boot_id: Some("boot-after-process-restart".into()),
            })
            .await
            .unwrap();
        let restart_commands = hub
            .commands(AgentAuth {
                node_id: "compute-1".into(),
                session_token: restarted.session_token,
            })
            .await
            .unwrap();
        assert!(restart_commands
            .iter()
            .any(|command| command.operation == "job_start"));
    }

    /// Verification 2026-09-11 (harden-unified-streaming-runtime re-audit,
    /// WARNING 1): the Job-level observed state aggregates every planned
    /// assignment — one peer's success while another is still pending leaves
    /// the Job converging, and a retryable peer degradation never overwrites
    /// the healthy peer's observation as failed.
    #[tokio::test]
    async fn job_observed_state_waits_for_every_assignment_and_ignores_retryable_peer_degradation()
    {
        let storage =
            StorageActor::start(crate::storage::ControlPlaneStore::in_memory().unwrap(), 8);
        let hub = Hub::with_storage(config(), storage);
        let node_a = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "compute-1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["job_runtime".into(), "state_backend".into()],
                boot_id: None,
            })
            .await
            .unwrap();
        let node_b = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "compute-2".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["job_runtime".into(), "state_backend".into()],
                boot_id: None,
            })
            .await
            .unwrap();
        // Two components so each node receives one start assignment.
        let job = hub
            .upsert_job(JobRecord {
                job_id: "orders".into(),
                version: 1,
                spec_json: serde_json::json!({
                    "id": "orders",
                    "version": 1,
                    "max_parallelism": 1,
                    "parallelism": 1,
                    "operators": [
                        {"id": "source-a", "kind": "source"},
                        {"id": "sink-a", "kind": "sink"},
                        {"id": "source-b", "kind": "source"},
                        {"id": "sink-b", "kind": "sink"}
                    ],
                    "edges": [
                        {"id": "edge-a", "from": "source-a", "to": "sink-a"},
                        {"id": "edge-b", "from": "source-b", "to": "sink-b"}
                    ],
                    "sources": [
                        {"operator_id": "source-a", "input_type": "memory", "time": {"mode": "processing_time"}},
                        {"operator_id": "source-b", "input_type": "memory", "time": {"mode": "processing_time"}}
                    ],
                    "sinks": [
                        {"operator_id": "sink-a", "output_type": "drop"},
                        {"operator_id": "sink-b", "output_type": "drop"}
                    ]
                })
                .to_string(),
                desired_state: "running".into(),
                observed_state: "starting".into(),
                convergence: "reconciling".into(),
                generation: 1,
                node_ids: vec!["compute-1".into(), "compute-2".into()],
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 0,
            })
            .await
            .unwrap();

        // Peer A succeeds while peer B is still pending: the Job must stay
        // non-terminal until every planned assignment reports success.
        let command_a = hub
            .commands(AgentAuth {
                node_id: "compute-1".into(),
                session_token: node_a.session_token.clone(),
            })
            .await
            .unwrap()
            .into_iter()
            .find(|command| command.operation == "job_start")
            .expect("compute-1 receives a start assignment");
        hub.command_result(
            AgentAuth {
                node_id: "compute-1".into(),
                session_token: node_a.session_token.clone(),
            },
            CommandResult {
                command_id: command_a.id.clone(),
                operation_id: command_a.operation_id.clone(),
                state: HubOperationState::Succeeded,
                progress: 100,
                error: None,
                correlation_id: command_a.correlation_id,
                generation: command_a.generation,
                observed_generation: Some(job.generation),
                action_id: None,
                failure_class: None,
                config_version_id: None,
                rollout_id: None,
                observed_checkpoint_id: None,
                checkpoint_manifest_uri: None,
            },
        )
        .await
        .unwrap();
        let observed = hub.job("orders").await.unwrap().unwrap();
        assert_eq!(
            observed.observed_state, "starting",
            "one peer's success must not report a fully running Job"
        );
        assert_eq!(observed.convergence, "reconciling");

        // Peer B reports a retryable degradation (a TimedOut state, as the
        // Agent produces for an expired lease). The aggregation is driven
        // purely by operation states — failure_class is informational — and
        // a retryable state must keep the healthy peer's observation
        // neutral instead of overwriting it as failed.
        let command_b = hub
            .commands(AgentAuth {
                node_id: "compute-2".into(),
                session_token: node_b.session_token.clone(),
            })
            .await
            .unwrap()
            .into_iter()
            .find(|command| command.operation == "job_start")
            .expect("compute-2 receives a start assignment");
        hub.command_result(
            AgentAuth {
                node_id: "compute-2".into(),
                session_token: node_b.session_token.clone(),
            },
            CommandResult {
                command_id: command_b.id.clone(),
                operation_id: command_b.operation_id.clone(),
                state: HubOperationState::TimedOut,
                progress: 0,
                error: Some("command lease expired".into()),
                correlation_id: command_b.correlation_id,
                generation: command_b.generation,
                observed_generation: Some(job.generation),
                action_id: None,
                failure_class: Some("temporary_execution".into()),
                config_version_id: None,
                rollout_id: None,
                observed_checkpoint_id: None,
                checkpoint_manifest_uri: None,
            },
        )
        .await
        .unwrap();
        let observed = hub.job("orders").await.unwrap().unwrap();
        assert_eq!(
            observed.observed_state, "starting",
            "a retryable peer degradation must stay observed-neutral"
        );
        assert_eq!(observed.convergence, "reconciling");

        // The retry is re-enqueued by reconciliation; once it succeeds too,
        // the complete assignment set is successful and the Job reports
        // running (the terminal half of the aggregation contract).
        assert_eq!(hub.reconcile_jobs().await.unwrap(), 1);
        let retry = hub
            .commands(AgentAuth {
                node_id: "compute-2".into(),
                session_token: node_b.session_token.clone(),
            })
            .await
            .unwrap()
            .into_iter()
            .find(|command| command.operation == "job_start")
            .expect("the timed-out peer receives a replacement start command");
        hub.command_result(
            AgentAuth {
                node_id: "compute-2".into(),
                session_token: node_b.session_token.clone(),
            },
            CommandResult {
                command_id: retry.id.clone(),
                operation_id: retry.operation_id.clone(),
                state: HubOperationState::Succeeded,
                progress: 100,
                error: None,
                correlation_id: retry.correlation_id,
                generation: retry.generation,
                observed_generation: Some(job.generation),
                action_id: None,
                failure_class: None,
                config_version_id: None,
                rollout_id: None,
                observed_checkpoint_id: None,
                checkpoint_manifest_uri: None,
            },
        )
        .await
        .unwrap();
        let observed = hub.job("orders").await.unwrap().unwrap();
        assert_eq!(
            observed.observed_state, "running",
            "every planned assignment succeeded, so the Job reports running"
        );
        assert_eq!(observed.convergence, "converged");
    }

    /// Verification (repair-kernel-review-findings task 2.3): the terminal
    /// failure half of the aggregation — once the complete assignment set is
    /// evaluated and any assignment reports a permanent execution failure,
    /// the Job reports failed even though a peer succeeded.
    #[tokio::test]
    async fn job_observed_state_reports_failed_when_a_peer_permanently_fails() {
        let storage =
            StorageActor::start(crate::storage::ControlPlaneStore::in_memory().unwrap(), 8);
        let hub = Hub::with_storage(config(), storage);
        let node_a = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "compute-1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["job_runtime".into(), "state_backend".into()],
                boot_id: None,
            })
            .await
            .unwrap();
        let node_b = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "compute-2".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["job_runtime".into(), "state_backend".into()],
                boot_id: None,
            })
            .await
            .unwrap();
        let job = hub
            .upsert_job(JobRecord {
                job_id: "orders".into(),
                version: 1,
                spec_json: serde_json::json!({
                    "id": "orders",
                    "version": 1,
                    "max_parallelism": 1,
                    "parallelism": 1,
                    "operators": [
                        {"id": "source-a", "kind": "source"},
                        {"id": "sink-a", "kind": "sink"},
                        {"id": "source-b", "kind": "source"},
                        {"id": "sink-b", "kind": "sink"}
                    ],
                    "edges": [
                        {"id": "edge-a", "from": "source-a", "to": "sink-a"},
                        {"id": "edge-b", "from": "source-b", "to": "sink-b"}
                    ],
                    "sources": [
                        {"operator_id": "source-a", "input_type": "memory", "time": {"mode": "processing_time"}},
                        {"operator_id": "source-b", "input_type": "memory", "time": {"mode": "processing_time"}}
                    ],
                    "sinks": [
                        {"operator_id": "sink-a", "output_type": "drop"},
                        {"operator_id": "sink-b", "output_type": "drop"}
                    ]
                })
                .to_string(),
                desired_state: "running".into(),
                observed_state: "starting".into(),
                convergence: "reconciling".into(),
                generation: 1,
                node_ids: vec!["compute-1".into(), "compute-2".into()],
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 0,
            })
            .await
            .unwrap();

        for (node_id, token, state, error) in [
            (
                "compute-1",
                node_a.session_token.clone(),
                HubOperationState::Succeeded,
                None,
            ),
            (
                "compute-2",
                node_b.session_token.clone(),
                HubOperationState::Failed,
                Some("runner process exited".into()),
            ),
        ] {
            let command = hub
                .commands(AgentAuth {
                    node_id: node_id.into(),
                    session_token: token.clone(),
                })
                .await
                .unwrap()
                .into_iter()
                .find(|command| command.operation == "job_start")
                .expect("each peer receives a start assignment");
            hub.command_result(
                AgentAuth {
                    node_id: node_id.into(),
                    session_token: token,
                },
                CommandResult {
                    command_id: command.id.clone(),
                    operation_id: command.operation_id.clone(),
                    state,
                    progress: 100,
                    error,
                    correlation_id: command.correlation_id,
                    generation: command.generation,
                    observed_generation: Some(job.generation),
                    action_id: None,
                    failure_class: None,
                    config_version_id: None,
                    rollout_id: None,
                    observed_checkpoint_id: None,
                    checkpoint_manifest_uri: None,
                },
            )
            .await
            .unwrap();
        }

        let observed = hub.job("orders").await.unwrap().unwrap();
        assert_eq!(
            observed.observed_state, "failed",
            "a complete set with a permanent failure aggregates to failed"
        );
    }

    #[tokio::test]
    async fn periodic_job_reconciliation_retries_a_failed_runtime() {
        let storage =
            StorageActor::start(crate::storage::ControlPlaneStore::in_memory().unwrap(), 8);
        let hub = Hub::with_storage(config(), storage);
        let registration = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "compute-1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["job_runtime".into(), "state_backend".into()],
                boot_id: None,
            })
            .await
            .unwrap();
        let job = hub
            .upsert_job(JobRecord {
                job_id: "orders".into(),
                version: 1,
                spec_json: serde_json::json!({
                    "id": "orders",
                    "version": 1,
                    "operators": [{"id": "source", "kind": "source"}, {"id": "sink", "kind": "sink"}],
                    "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
                    "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
                    "sinks": [{"operator_id": "sink", "output_type": "drop"}]
                })
                .to_string(),
                desired_state: "running".into(),
                observed_state: "starting".into(),
                convergence: "reconciling".into(),
                generation: 1,
                node_ids: vec!["compute-1".into()],
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 0,
            })
            .await
            .unwrap();
        let first = hub
            .commands(AgentAuth {
                node_id: "compute-1".into(),
                session_token: registration.session_token.clone(),
            })
            .await
            .unwrap()
            .pop()
            .unwrap();
        hub.command_result(
            AgentAuth {
                node_id: "compute-1".into(),
                session_token: registration.session_token.clone(),
            },
            CommandResult {
                command_id: first.id.clone(),
                operation_id: first.operation_id.clone(),
                state: HubOperationState::Failed,
                progress: 100,
                error: Some("runner failed".into()),
                correlation_id: first.correlation_id,
                generation: job.generation,
                observed_generation: Some(job.generation),
                action_id: None,
                failure_class: Some("runtime".into()),
                config_version_id: None,
                rollout_id: None,
                observed_checkpoint_id: None,
                checkpoint_manifest_uri: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            hub.job("orders").await.unwrap().unwrap().observed_state,
            "failed"
        );
        assert_eq!(hub.reconcile_jobs().await.unwrap(), 1);
        let commands = hub
            .commands(AgentAuth {
                node_id: "compute-1".into(),
                session_token: registration.session_token,
            })
            .await
            .unwrap();
        assert!(commands.iter().any(|command| {
            command.operation == "job_start"
                && command.generation == job.generation
                && command.id != first.id
        }));
    }

    #[tokio::test]
    async fn periodic_job_reconciliation_stops_persisted_divergence_after_recovery() {
        let storage =
            StorageActor::start(crate::storage::ControlPlaneStore::in_memory().unwrap(), 8);
        let hub1 = Hub::with_storage(config(), storage.clone());
        hub1.register(RegisterRequest {
            data_address: None,
            node_id: "compute-1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["job_runtime".into(), "state_backend".into()],
            boot_id: None,
        })
        .await
        .unwrap();
        let job = hub1
            .upsert_job(JobRecord {
                job_id: "orders".into(),
                version: 1,
                spec_json: serde_json::json!({
                    "id": "orders",
                    "version": 1,
                    "operators": [{"id": "source", "kind": "source"}, {"id": "sink", "kind": "sink"}],
                    "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
                    "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
                    "sinks": [{"operator_id": "sink", "output_type": "drop"}]
                })
                .to_string(),
                desired_state: "stopped".into(),
                observed_state: "running".into(),
                convergence: "reconciling".into(),
                generation: 3,
                node_ids: vec!["compute-1".into()],
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 0,
            })
            .await
            .unwrap();
        drop(hub1);

        let hub2 = Hub::with_storage(config(), storage);
        hub2.recover_persisted_state().await.unwrap();
        let registration = hub2
            .register(RegisterRequest {
                data_address: None,
                node_id: "compute-1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["job_runtime".into(), "state_backend".into()],
                boot_id: None,
            })
            .await
            .unwrap();

        assert_eq!(hub2.reconcile_jobs().await.unwrap(), 1);
        let commands = hub2
            .commands(AgentAuth {
                node_id: "compute-1".into(),
                session_token: registration.session_token,
            })
            .await
            .unwrap();
        assert!(commands.iter().any(|command| {
            command.operation == "job_stop"
                && command.resource_id == "orders"
                && command.generation == job.generation
        }));
    }

    // ===== Job operation audit + command metrics + expiry
    // (add-hub-job-audit-and-command-metrics) =====

    /// A storage-backed Hub with one online node and one Job whose spec
    /// carries an injected `connection_string` so tests can prove audit
    /// records never echo configuration bodies.
    async fn audited_job_hub(secret_marker: &str) -> (Hub, crate::hub::RegisterResponse) {
        let storage =
            StorageActor::start(crate::storage::ControlPlaneStore::in_memory().unwrap(), 8);
        let hub = Hub::with_storage(config(), storage);
        let session = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "compute-1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["job_runtime".into(), "state_backend".into()],
                boot_id: None,
            })
            .await
            .unwrap();
        let spec = serde_json::json!({
            "id": "orders",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}],
            "state": {"backend": "embedded_kv", "format_version": 3},
            "connection_string": format!("password={secret_marker}")
        });
        hub.upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json: spec.to_string(),
            desired_state: "running".into(),
            observed_state: "stopped".into(),
            convergence: "reconciling".into(),
            generation: 1,
            node_ids: vec!["compute-1".into()],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        (hub, session)
    }

    #[tokio::test]
    async fn job_start_is_audited_without_configuration_bodies() {
        let (hub, _session) = audited_job_hub("super-secret-42").await;
        let audits = hub.audit(Some("orders")).await.unwrap();
        let starts: Vec<&crate::storage::AuditRecord> = audits
            .iter()
            .filter(|record| record.action == "job.start")
            .collect();
        assert_eq!(
            starts.len(),
            1,
            "the accepted start is audited exactly once"
        );
        assert_eq!(starts[0].resource_type, "job");
        assert_eq!(starts[0].outcome, "accepted");
        assert_eq!(starts[0].node_id.as_deref(), Some("compute-1"));
        // The audit trail identifies the operation without echoing the
        // configuration: the injected secret must not surface anywhere in
        // the record, and neither must the spec field that carried it.
        let rendered = format!(
            "{} {}",
            starts[0].message.as_deref().unwrap_or_default(),
            serde_json::to_string(starts[0]).unwrap()
        );
        assert!(!rendered.contains("super-secret-42"));
        assert!(!rendered.contains("connection_string"));
    }

    #[tokio::test]
    async fn job_stop_rejection_for_unknown_node_is_audited() {
        let (hub, _session) = audited_job_hub("irrelevant").await;
        let error = hub
            .enqueue("ghost".into(), "job_stop".into(), "orders".into(), None)
            .await
            .unwrap_err();
        assert!(matches!(error, HubError::NodeUnavailable));
        let audits = hub.audit(Some("orders")).await.unwrap();
        assert!(audits.iter().any(|record| {
            record.action == "job.stop"
                && record.outcome == "rejected"
                && record.failure_code.as_deref() == Some("node_unavailable")
        }));
    }

    #[tokio::test]
    async fn command_metrics_track_enqueues_latency_and_rejections() {
        let (hub, session) = audited_job_hub("irrelevant").await;
        // The start dispatch inside the setup counted one enqueued job_start.
        assert!(hub
            .command_metrics()
            .render()
            .contains("arkflow_command_total{command=\"job_start\",outcome=\"enqueued\"} 1"));
        // Acknowledgement records the enqueue→ack latency into the buckets.
        let command = hub
            .commands(AgentAuth {
                node_id: "compute-1".into(),
                session_token: session.session_token.clone(),
            })
            .await
            .unwrap()
            .into_iter()
            .find(|command| command.operation == "job_start")
            .expect("compute-1 receives the start command");
        hub.command_result(
            AgentAuth {
                node_id: "compute-1".into(),
                session_token: session.session_token.clone(),
            },
            CommandResult {
                command_id: command.id.clone(),
                operation_id: command.operation_id.clone(),
                state: HubOperationState::Acknowledged,
                progress: 10,
                error: None,
                correlation_id: command.correlation_id.clone(),
                generation: command.generation,
                observed_generation: None,
                action_id: None,
                failure_class: None,
                config_version_id: None,
                rollout_id: None,
                observed_checkpoint_id: None,
                checkpoint_manifest_uri: None,
            },
        )
        .await
        .unwrap();
        let rendered = hub.command_metrics().render();
        assert!(rendered
            .contains("arkflow_command_duration_bucket{command=\"job_start\",le=\"+Inf\"} 1"));
        assert!(rendered.contains("arkflow_command_duration_count{command=\"job_start\"} 1"));
        assert!(rendered
            .contains("arkflow_command_total{command=\"job_start\",outcome=\"acknowledged\"} 1"));
        // A dispatch to an unknown node counts the fixed outcome class.
        let _ = hub
            .enqueue("ghost".into(), "job_stop".into(), "orders".into(), None)
            .await;
        assert!(hub.command_metrics().render().contains(
            "arkflow_command_total{command=\"job_stop\",outcome=\"node_unavailable\"} 1"
        ));
        // Dynamic operation names collapse into the `other` label so the
        // series space stays bounded by the fixed enumeration.
        let _ = hub
            .enqueue(
                "compute-1".into(),
                "exotic-operation".into(),
                "orders".into(),
                None,
            )
            .await
            .unwrap();
        assert!(hub
            .command_metrics()
            .render()
            .contains("arkflow_command_total{command=\"other\",outcome=\"enqueued\"} 1"));
    }

    #[tokio::test]
    async fn expired_job_operations_retry_then_reach_the_terminal_cap() {
        let (hub, session) = audited_job_hub("irrelevant").await;
        let rewind_expiry = |operations: &mut BTreeMap<String, HubOperation>, id: &str| {
            if let Some(record) = operations.get_mut(id) {
                record.expires_at_ms = Some(1);
            }
        };
        let mut current_id = {
            let commands = hub
                .commands(AgentAuth {
                    node_id: "compute-1".into(),
                    session_token: session.session_token.clone(),
                })
                .await
                .unwrap();
            let start = commands
                .iter()
                .find(|command| command.operation == "job_start")
                .expect("the start command is queued");
            let operations = hub.operations.read().await;
            let queued = operations
                .values()
                .find(|record| record.command_id == start.id)
                .expect("the queued operation exists")
                .id
                .clone();
            drop(operations);
            {
                let mut operations = hub.operations.write().await;
                rewind_expiry(&mut operations, &queued);
            }
            queued
        };
        // Two expiry sweeps retry the operation (TimedOut, retry_count 1
        // then 2); each re-enqueue inherits the accumulated count.
        for expected_retry in [1, 2] {
            assert_eq!(hub.expire_stale_job_operations().await.unwrap(), 1);
            let operations = hub.operations.read().await;
            let expired = operations.get(&current_id).unwrap();
            assert_eq!(expired.state, HubOperationState::TimedOut);
            assert_eq!(expired.retry_count, expected_retry);
            assert_eq!(expired.failure_class.as_deref(), Some("expired"));
            drop(operations);
            // Re-enqueue at the SAME generation the expired start used (the
            // reconciler always passes job.generation) so the retry count
            // inheritance lookup matches.
            let replacement = hub
                .enqueue_with_metadata(
                    "compute-1".into(),
                    "job_start".into(),
                    "orders".into(),
                    None,
                    None,
                    1,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
            assert_eq!(replacement.retry_count, expected_retry);
            // The retry is reconciler mechanics, not a new mutation: the
            // audit trail must still hold exactly one accepted job.start.
            let starts = hub
                .audit(Some("orders"))
                .await
                .unwrap()
                .iter()
                .filter(|record| record.action == "job.start" && record.outcome == "accepted")
                .count();
            assert_eq!(
                starts, 1,
                "reconciler re-dispatches must not add audit rows"
            );
            current_id = replacement.id.clone();
            let mut operations = hub.operations.write().await;
            rewind_expiry(&mut operations, &current_id);
        }
        // The third expiry exhausts the budget: terminal failed/expired.
        assert_eq!(hub.expire_stale_job_operations().await.unwrap(), 1);
        {
            let operations = hub.operations.read().await;
            let failed = operations.get(&current_id).unwrap();
            assert_eq!(failed.state, HubOperationState::Failed);
            assert_eq!(failed.retry_count, 3);
            assert_eq!(failed.failure_class.as_deref(), Some("expired"));
        }
        // And the exhausted budget refuses further re-enqueue attempts.
        let error = hub
            .enqueue_with_metadata(
                "compute-1".into(),
                "job_start".into(),
                "orders".into(),
                None,
                None,
                1,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, HubError::Invalid(_)));
    }

    #[tokio::test]
    async fn periodic_checkpoint_scheduling_writes_no_audit_rows() {
        // The periodic scheduler funnels through the same dispatch path as
        // operator triggers; only the latter is a mutation and may appear in
        // the audit trail.
        let storage =
            StorageActor::start(crate::storage::ControlPlaneStore::in_memory().unwrap(), 8);
        let hub = Hub::with_storage(config(), storage);
        hub.register(RegisterRequest {
            data_address: None,
            node_id: "compute-1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["job_runtime".into(), "state_backend".into()],
            boot_id: None,
        })
        .await
        .unwrap();
        hub.upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json: serde_json::json!({
                "id": "orders",
                "version": 1,
                "operators": [
                    {"id": "source", "kind": "source"},
                    {"id": "sink", "kind": "sink"}
                ],
                "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
                "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
                "sinks": [{"operator_id": "sink", "output_type": "drop"}],
                "state": {"backend": "embedded_kv", "format_version": 3},
                "checkpoint": {"interval_ms": 1, "object_store_uri": "file:///tmp/checkpoints"}
            })
            .to_string(),
            desired_state: "running".into(),
            observed_state: "running".into(),
            convergence: "converged".into(),
            generation: 1,
            node_ids: vec!["compute-1".into()],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        // One scheduling round dispatches the auto checkpoint; the second is
        // deduplicated by the pending record's created_at gate.
        let scheduled = hub.schedule_periodic_checkpoints().await.unwrap();
        assert_eq!(scheduled, 1, "the auto checkpoint was scheduled");
        let audits = hub.audit(Some("orders")).await.unwrap();
        assert!(
            !audits
                .iter()
                .any(|record| record.action == "job.checkpoint"),
            "scheduler mechanics must not appear in the audit trail"
        );
        // The dispatch itself is still metriced like any command.
        assert!(hub
            .command_metrics()
            .render()
            .contains("command=\"job_checkpoint\""));
    }

    #[tokio::test]
    async fn audit_history_prunes_old_records_but_keeps_recent() {
        let storage =
            StorageActor::start(crate::storage::ControlPlaneStore::in_memory().unwrap(), 8);
        let hub = Hub::with_storage(config(), storage);
        let now = now_ms() as i64;
        let day_ms = 24 * 60 * 60 * 1000;
        for (action, occurred_at_ms) in [
            ("job.start", now - 31 * day_ms),
            ("job.stop", now - 60 * 60 * 1000),
        ] {
            hub.record_audit_event(crate::storage::AuditRecord {
                event_id: 0,
                actor: Some("operator".into()),
                action: action.into(),
                resource_type: "job".into(),
                resource_id: Some("orders".into()),
                node_id: None,
                stream_id: None,
                correlation_id: None,
                outcome: "accepted".into(),
                failure_code: None,
                message: None,
                occurred_at_ms: occurred_at_ms as u64,
            })
            .await
            .unwrap();
        }
        hub.prune_audit_history().await.unwrap();
        let remaining = hub.audit(None).await.unwrap();
        assert_eq!(remaining.len(), 1, "only the recent record survives");
        assert_eq!(remaining[0].action, "job.stop");
    }
}

#[cfg(test)]
mod session_report_tests {
    use super::*;
    use std::time::Duration;

    fn config() -> HubConfig {
        HubConfig {
            operator_token: Some("operator".into()),
            node_token: Some("node-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 1000,
            poll_interval_ms: 1000,
            session_ttl_ms: default_session_ttl_ms(),
        }
    }

    fn stream(state: &str) -> arkflow_core::control::StreamStatus {
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
                "restarts": 0,
                "kernel_chains": {},
                "in_flight": 0,
                "mean_latency_us": 0,
                "checkpoint_duration_ms": 0,
                "checkpoint_failures": 0,
                "watermark_lag_ms": 0,
                "late_events": 0
            }
        }))
        .unwrap()
    }

    async fn registered_hub() -> (Hub, crate::hub::RegisterResponse) {
        let hub = Hub::with_storage(
            config(),
            crate::storage::StorageActor::start(
                crate::storage::ControlPlaneStore::in_memory().unwrap(),
                4,
            ),
        );
        let session = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec![],
                boot_id: None,
            })
            .await
            .unwrap();
        (hub, session)
    }

    /// Task 7.3: a newly registered session receives a fresh report cursor —
    /// sequence 1 of the new session identity is accepted even though the
    /// previous session had already reported higher sequences.
    #[tokio::test]
    async fn new_session_resets_the_report_cursor() {
        let (hub, first) = registered_hub().await;
        let report = |session: &crate::hub::RegisterResponse, seq: u64, state: &str| NodeReport {
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
            jobs: BTreeMap::new(),
            configuration: None,
            configuration_version: None,
            boot_id: Some(session.session_token.clone()),
            report_seq: seq,
        };
        hub.report(report(&first, 7, "running")).await.unwrap();
        // Re-register: a fresh session identity with a fresh cursor.
        let second = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec![],
                boot_id: None,
            })
            .await
            .unwrap();
        // Sequence 1 of the new session is accepted (not stale under the
        // previous session's cursor of 7).
        hub.report(report(&second, 1, "failed")).await.unwrap();
        let streams = hub.streams(Some("n1")).await;
        assert_eq!(streams.len(), 1);
        assert_eq!(
            streams[0].1.state,
            arkflow_core::control::StreamState::Failed
        );
    }

    // --- Session credential lifetime (harden-agent-session-credentials) ---

    fn short_session_config() -> HubConfig {
        HubConfig {
            session_ttl_ms: 80,
            ..config()
        }
    }

    fn heartbeat_request(session_token: &str) -> HeartbeatRequest {
        HeartbeatRequest {
            auth: AgentAuth {
                node_id: "n1".into(),
                session_token: session_token.to_owned(),
            },
            state: "online".into(),
            protocol_version: Some("v1".into()),
            software_version: None,
            capabilities: vec![],
            rollout_id: None,
        }
    }

    async fn register_with_boot(hub: &Hub, boot_id: &str) -> RegisterResponse {
        hub.register(RegisterRequest {
            data_address: None,
            node_id: "n1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec![],
            boot_id: Some(boot_id.to_owned()),
        })
        .await
        .unwrap()
    }

    /// An expired session credential stops authenticating agent requests, and
    /// the rejection must not mutate the node registry.
    #[tokio::test]
    async fn expired_session_is_rejected_without_registry_mutation() {
        let hub = Hub::new(short_session_config());
        let session = register_with_boot(&hub, "boot-1").await;
        // The registration response advertises the configured session TTL.
        assert_eq!(session.session_ttl_ms, 80);

        hub.heartbeat(heartbeat_request(&session.session_token))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(140)).await;
        assert!(matches!(
            hub.heartbeat(heartbeat_request(&session.session_token))
                .await,
            Err(HubError::Unauthorized)
        ));

        // Registry unchanged: the node is still present, online, untouched.
        let nodes = hub.nodes().await;
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, "n1");
        assert_eq!(nodes[0].state, NodeConnectionState::Online);
    }

    /// Re-registration mints a fresh credential and kills the previous one;
    /// resources reported by the old session survive when the boot identity
    /// is stable.
    #[tokio::test]
    async fn re_registration_rotates_the_credential_and_preserves_state() {
        let hub = Hub::new(config());
        let first = register_with_boot(&hub, "boot-1").await;
        let report = |session: &RegisterResponse, seq: u64| NodeReport {
            auth: AgentAuth {
                node_id: "n1".into(),
                session_token: session.session_token.clone(),
            },
            version: "test".into(),
            state: "online".into(),
            capabilities: vec![],
            streams: vec![stream("running")],
            operations: vec![],
            events: vec![],
            metrics: BTreeMap::new(),
            jobs: BTreeMap::new(),
            configuration: None,
            configuration_version: None,
            boot_id: Some("boot-1".into()),
            report_seq: seq,
        };
        hub.report(report(&first, 1)).await.unwrap();

        let second = register_with_boot(&hub, "boot-1").await;
        assert_ne!(first.session_token, second.session_token);
        assert!(matches!(
            hub.heartbeat(heartbeat_request(&first.session_token)).await,
            Err(HubError::Unauthorized)
        ));
        hub.heartbeat(heartbeat_request(&second.session_token))
            .await
            .unwrap();
        let streams = hub.streams(Some("n1")).await;
        assert_eq!(streams.len(), 1);
    }

    /// Agents built before `session_ttl_ms` existed must keep parsing
    /// registration responses from a Hub that does not send it yet.
    #[test]
    fn register_response_without_session_ttl_is_accepted() {
        let legacy: RegisterResponse = serde_json::from_value(serde_json::json!({
            "node_id": "n1",
            "session_token": "credential",
            "lease_ttl_ms": 1,
            "poll_interval_ms": 1,
            "protocol_version": "v1"
        }))
        .unwrap();
        assert_eq!(legacy.session_ttl_ms, 0);
    }

    /// The session TTL elapsing while a command executes must not lose the
    /// terminal result: the Agent re-registers, the command-lease replay path
    /// re-enqueues the command, and the Hub settles exactly one terminal
    /// outcome through it.
    #[tokio::test]
    async fn expired_session_mid_command_still_settles_one_terminal_result() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES ('cfg-e2e', 'digest', '{}', 'json', 1)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let mut hub_config = config();
        hub_config.lease_ttl_ms = 1_000; // command lease duration
        hub_config.session_ttl_ms = 80; // expires long before the command lease
        let hub = Hub::with_storage(hub_config, StorageActor::start(store, 8));
        let registration = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "agent-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["configuration".into()],
                boot_id: Some("boot-1".into()),
            })
            .await
            .unwrap();
        let auth_a = AgentAuth {
            node_id: "agent-a".into(),
            session_token: registration.session_token.clone(),
        };

        hub.create_rollout(
            "cfg-e2e".into(),
            vec!["agent-a".into()],
            1,
            Some("operator".into()),
            Some("e2e-session-expiry".into()),
        )
        .await
        .unwrap();
        hub.reconcile_rollouts().await.unwrap();
        let operation = hub
            .reconcile_once("e2e-session-expiry")
            .await
            .unwrap()
            .unwrap();
        let commands = hub.commands(auth_a.clone()).await.unwrap();
        assert_eq!(commands.len(), 1);
        let command = commands[0].clone();

        // The session expires while the Agent executes the command.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let result = hub
            .command_result(
                auth_a.clone(),
                CommandResult {
                    command_id: command.id.clone(),
                    operation_id: command.operation_id.clone(),
                    state: HubOperationState::Succeeded,
                    progress: 100,
                    error: None,
                    correlation_id: command.correlation_id.clone(),
                    generation: command.generation,
                    observed_generation: Some(command.generation),
                    action_id: command.action_id.clone(),
                    failure_class: None,
                    config_version_id: Some("cfg-e2e".into()),
                    rollout_id: command.rollout_id.clone(),
                    observed_checkpoint_id: None,
                    checkpoint_manifest_uri: None,
                },
            )
            .await;
        assert!(matches!(result, Err(HubError::Unauthorized)));

        // The Agent re-registers with its stable boot identity: leased command
        // state survives because the boot did not change.
        let reauth = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "agent-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["configuration".into()],
                boot_id: Some("boot-1".into()),
            })
            .await
            .unwrap();
        let auth_b = AgentAuth {
            node_id: "agent-a".into(),
            session_token: reauth.session_token.clone(),
        };
        // The command lease is still valid, so nothing is redelivered yet.
        assert!(hub.commands(auth_b.clone()).await.unwrap().is_empty());

        // Let the command lease expire. Session B's own TTL also elapses
        // during the wait — with a hard TTL every long gap ends in another
        // re-registration, exactly like the real Agent reconnect loop.
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        let reauth2 = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "agent-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["configuration".into()],
                boot_id: Some("boot-1".into()),
            })
            .await
            .unwrap();
        let auth_c = AgentAuth {
            node_id: "agent-a".into(),
            session_token: reauth2.session_token.clone(),
        };
        // Refresh the node lease before polling so the replay re-enqueue
        // finds the node online.
        hub.heartbeat(HeartbeatRequest {
            auth: auth_c.clone(),
            state: "online".into(),
            protocol_version: Some("v1".into()),
            software_version: None,
            capabilities: vec!["configuration".into()],
            rollout_id: None,
        })
        .await
        .unwrap();
        // The first poll triggers the lease-expiry sweep that re-enqueues the
        // command; the replacement lands in the queue after the pop loop, so
        // the next poll hands it back.
        let _ = hub.commands(auth_c.clone()).await.unwrap();
        let redelivered = hub.commands(auth_c.clone()).await.unwrap();
        assert_eq!(redelivered.len(), 1);

        hub.command_result(
            auth_c.clone(),
            CommandResult {
                command_id: redelivered[0].id.clone(),
                operation_id: redelivered[0].operation_id.clone(),
                state: HubOperationState::Succeeded,
                progress: 100,
                error: None,
                correlation_id: redelivered[0].correlation_id.clone(),
                generation: redelivered[0].generation,
                observed_generation: Some(redelivered[0].generation),
                action_id: redelivered[0].action_id.clone(),
                failure_class: None,
                config_version_id: Some("cfg-e2e".into()),
                rollout_id: redelivered[0].rollout_id.clone(),
                observed_checkpoint_id: None,
                checkpoint_manifest_uri: None,
            },
        )
        .await
        .unwrap();
        let settled = hub.operation(&redelivered[0].operation_id).await.unwrap();
        assert_eq!(settled.state, HubOperationState::Succeeded);
        // The pre-expiry operation was timed out by the lease expiry — the
        // result rejected with 401 never settled it. Exactly one terminal
        // outcome exists per operation record.
        let original = hub.operation(&operation.id).await.unwrap();
        assert_eq!(original.state, HubOperationState::TimedOut);
    }

    /// The outbox and attempt retention wrappers converge their tables
    /// through the storage actor while unprocessed outbox rows and active
    /// attempts survive, and the status counters stay meaningful.
    #[tokio::test]
    async fn outbox_and_attempt_history_prunes_converge_through_the_hub() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        let hub = Hub::with_storage(config(), StorageActor::start(store.clone(), 8));
        let now = now_ms() as i64;
        store
            .immediate_transaction(|transaction| -> Result<(), crate::storage::StorageError> {
                transaction.execute(
                    "INSERT INTO cp_intents (intent_id, node_id, stream_id, generation, intent_type, state, convergence_state, created_at_ms, updated_at_ms) VALUES ('intent-1', 'n1', 'orders', 1, 'stream_lifecycle', 'converged', 'converged', 1, 1)",
                    [],
                )?;
                transaction.execute(
                    "INSERT INTO cp_outbox (event_key, event_type, node_id, available_at_ms, created_at_ms, processed_at_ms) VALUES ('stale-processed', 'reconcile_intent', 'n1', 1, 1, 100)",
                    [],
                )?;
                transaction.execute(
                    "INSERT INTO cp_outbox (event_key, event_type, node_id, available_at_ms, created_at_ms, processed_at_ms) VALUES ('fresh-processed', 'reconcile_intent', 'n1', 1, 1, ?1)",
                    [now],
                )?;
                transaction.execute(
                    "INSERT INTO cp_attempts (attempt_id, intent_id, command_id, node_id, stream_id, generation, operation, state, finished_at_ms, created_at_ms) VALUES ('old-terminal', 'intent-1', 'cmd-old', 'n1', 'orders', 1, 'apply_configuration', 'succeeded', 100, 1)",
                    [],
                )?;
                transaction.execute(
                    "INSERT INTO cp_attempts (attempt_id, intent_id, command_id, node_id, stream_id, generation, operation, state, finished_at_ms, created_at_ms) VALUES ('live-active', 'intent-1', 'cmd-live', 'n1', 'orders', 1, 'apply_configuration', 'running', NULL, 1)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        hub.prune_outbox_history().await.unwrap();
        hub.prune_attempt_history().await.unwrap();
        let counts = store
            .immediate_transaction(|transaction| {
                let outbox = transaction.query_row(
                    "SELECT COUNT(*) FROM cp_outbox WHERE event_key IN ('stale-processed', 'fresh-processed')",
                    [],
                    |row| row.get::<_, i64>(0),
                )?;
                let attempts = transaction.query_row(
                    "SELECT COUNT(*) FROM cp_attempts WHERE attempt_id IN ('old-terminal', 'live-active')",
                    [],
                    |row| row.get::<_, i64>(0),
                )?;
                Ok((outbox, attempts))
            })
            .unwrap();
        assert_eq!(
            counts,
            (1, 1),
            "stale processed outbox and terminal attempt rows are reclaimed; fresh and active rows are kept"
        );
    }

    /// Task 7.3: a delayed report from an older session arrives after the new
    /// session registered — the Hub acknowledges it without changing the new
    /// session's observed state.
    #[tokio::test]
    async fn delayed_report_from_an_old_session_is_ignored() {
        let (hub, first) = registered_hub().await;
        let second = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec![],
                boot_id: None,
            })
            .await
            .unwrap();
        // The new session reports a healthy stream.
        hub.report(NodeReport {
            auth: AgentAuth {
                node_id: "n1".into(),
                session_token: second.session_token.clone(),
            },
            version: "test".into(),
            state: "online".into(),
            capabilities: vec![],
            streams: vec![stream("running")],
            operations: vec![],
            events: vec![],
            metrics: BTreeMap::new(),
            jobs: BTreeMap::new(),
            configuration: None,
            configuration_version: None,
            boot_id: Some(second.session_token.clone()),
            report_seq: 1,
        })
        .await
        .unwrap();
        // A delayed report from the OLD session (stale boot identity) claims
        // a failure: the Hub must not regress the observed snapshot.
        // The old session token is no longer authenticated after
        // re-registration, so this surfaces as Unauthorized.
        let stale = hub
            .report(NodeReport {
                auth: AgentAuth {
                    node_id: "n1".into(),
                    session_token: first.session_token.clone(),
                },
                version: "test".into(),
                state: "online".into(),
                capabilities: vec![],
                streams: vec![stream("failed")],
                operations: vec![],
                events: vec![],
                metrics: BTreeMap::new(),
                jobs: BTreeMap::new(),
                configuration: None,
                configuration_version: None,
                boot_id: Some(first.session_token.clone()),
                report_seq: 99,
            })
            .await;
        assert!(stale.is_err(), "the old session token is revoked");
        let streams = hub.streams(Some("n1")).await;
        assert_eq!(
            streams[0].1.state,
            arkflow_core::control::StreamState::Running
        );
    }

    // ----- resource-aware placement and opt-in rebalancing -----

    fn job_spec_json(id: &str) -> String {
        serde_json::json!({
            "id": id,
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        })
        .to_string()
    }

    fn resource_metrics(used_ratio: f64, cpu: f64) -> BTreeMap<String, f64> {
        BTreeMap::from([
            ("node_memory_total_bytes".to_string(), 16_000.0),
            ("node_memory_used_bytes".to_string(), 16_000.0 * used_ratio),
            ("node_cpu_usage_percent".to_string(), cpu),
        ])
    }

    fn rebalance_job_spec_json(id: &str) -> String {
        let mut value: serde_json::Value = serde_json::from_str(&job_spec_json(id)).unwrap();
        value["rebalance"] =
            serde_json::json!({"mode": "auto", "pressure_streak": 2, "cooldown_ms": 0});
        value.to_string()
    }

    async fn report_resources(
        hub: &Hub,
        auth: &AgentAuth,
        used_ratio: f64,
        cpu: f64,
        report_seq: u64,
    ) {
        hub.report(NodeReport {
            auth: auth.clone(),
            version: "test".into(),
            state: "online".into(),
            capabilities: vec![],
            streams: vec![],
            operations: vec![],
            events: vec![],
            metrics: resource_metrics(used_ratio, cpu),
            jobs: BTreeMap::new(),
            configuration: None,
            configuration_version: None,
            boot_id: None,
            report_seq,
        })
        .await
        .unwrap();
    }

    async fn start_operations(hub: &Hub, job_id: &str) -> Vec<HubOperation> {
        hub.operations(None)
            .await
            .into_iter()
            .filter(|operation_record| {
                operation_record.resource_id == job_id
                    && operation_record.operation == "job_start"
            })
            .collect()
    }

    async fn complete_start_commands(hub: &Hub, node_id: &str, session_token: &str) {
        let auth = AgentAuth {
            node_id: node_id.into(),
            session_token: session_token.into(),
        };
        for command in hub.commands(auth.clone()).await.unwrap() {
            hub.command_result(
                auth.clone(),
                CommandResult {
                    command_id: command.id,
                    operation_id: command.operation_id,
                    state: HubOperationState::Succeeded,
                    progress: 100,
                    error: None,
                    correlation_id: command.correlation_id,
                    generation: command.generation,
                    observed_generation: None,
                    action_id: None,
                    failure_class: None,
                    config_version_id: command.config_version_id,
                    rollout_id: command.rollout_id,
                    observed_checkpoint_id: None,
                    checkpoint_manifest_uri: None,
                },
            )
            .await
            .unwrap();
        }
    }

    #[test]
    fn rank_candidates_is_deterministic_and_prefers_headroom() {
        let now = now_ms();
        let mut nodes = BTreeMap::new();
        for (id, used_ratio, cpu, reported) in [
            ("n-busy", 0.95, 90.0, true),
            ("n-free", 0.10, 5.0, true),
            ("n-mid", 0.50, 50.0, true),
            ("n-blind", 0.0, 0.0, false),
        ] {
            let mut record = NodeRecord {
                resource: HubNode {
                    id: id.into(),
                    protocol_version: "v1".into(),
                    version: "test".into(),
                    state: NodeConnectionState::Online,
                    capabilities: vec![],
                    last_seen_at_ms: now,
                    lease_expires_at_ms: now + 1_000,
                    streams_total: 0,
                    streams_running: 0,
                    streams_failed: 0,
                    maintenance_state: NodeMaintenanceState::Active,
                    data_address: None,
                },
                session_token: String::new(),
                session_expires_at_ms: now + 1_000,
                boot_id: Some("boot".into()),
                report_seq: 0,
                commands: VecDeque::new(),
                leased_commands: BTreeMap::new(),
                streams: vec![],
                operations: vec![],
                events: vec![],
                metrics: BTreeMap::new(),
                last_report_at_ms: if reported { now } else { 0 },
                pressure_streak: 0,
                jobs: BTreeMap::new(),
                configuration: None,
            };
            if reported {
                record.metrics = resource_metrics(used_ratio, cpu);
            }
            nodes.insert(id.to_string(), record);
        }
        let rank = |candidates: Vec<String>| {
            rank_candidates(candidates, &nodes, now)
                .into_iter()
                .collect::<Vec<_>>()
        };
        let expected = vec![
            "n-free".to_string(),
            "n-mid".to_string(),
            "n-busy".to_string(),
            "n-blind".to_string(),
        ];
        assert_eq!(rank(vec!["n-blind".into(), "n-busy".into(), "n-free".into(), "n-mid".into()]), expected);
        // Deterministic: the same input produces the same order.
        assert_eq!(
            rank(vec!["n-mid".into(), "n-blind".into(), "n-free".into(), "n-busy".into()]),
            expected,
            "ranking must be a pure function of (candidates, gauges, ids)"
        );
    }

    #[tokio::test]
    async fn first_placement_lands_on_the_higher_headroom_node() {
        let hub = Hub::new(config());
        let session_a = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: vec![],
                boot_id: Some("boot".into()),
            })
            .await
            .unwrap();
        let session_b = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "node-b".into(),
                node_token: "node-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: vec![],
                boot_id: Some("boot".into()),
            })
            .await
            .unwrap();
        let _ = (&session_a, &session_b);
        // node-a reports a fuller node; node-b has headroom and must win the
        // first placement even though node-a sorts first by id.
        report_resources(&hub, &AgentAuth { node_id: "node-a".into(), session_token: session_a.session_token.clone() }, 0.9, 10.0, 1).await;
        report_resources(&hub, &AgentAuth { node_id: "node-b".into(), session_token: session_b.session_token.clone() }, 0.1, 10.0, 1).await;
        hub.upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json: job_spec_json("orders"),
            desired_state: "running".into(),
            observed_state: "validated".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: vec![],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        let starts = start_operations(&hub, "orders").await;
        assert_eq!(starts.len(), 1);
        assert_eq!(starts[0].node_id, "node-b");
    }

    #[tokio::test]
    async fn gauge_less_fleet_keeps_id_order_placement() {
        let hub = Hub::new(config());
        hub.register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot".into()),
        })
        .await
        .unwrap();
        hub.register(RegisterRequest {
            data_address: None,
            node_id: "node-b".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot".into()),
        })
        .await
        .unwrap();
        hub.upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json: job_spec_json("orders"),
            desired_state: "running".into(),
            observed_state: "validated".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: vec![],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        let starts = start_operations(&hub, "orders").await;
        assert_eq!(starts.len(), 1);
        assert_eq!(starts[0].node_id, "node-a", "no gauges: today's id order");
    }

    #[tokio::test]
    async fn pressure_streak_counts_consecutive_pressuring_reports() {
        let hub = Hub::new(config());
        let session = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "n1".into(),
                node_token: "node-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: vec![],
                boot_id: Some("boot".into()),
            })
            .await
            .unwrap();
        let auth = AgentAuth {
            node_id: "n1".into(),
            session_token: session.session_token.clone(),
        };
        let streak = || async {
            hub.nodes
                .read()
                .await
                .get("n1")
                .expect("registered node")
                .pressure_streak
        };
        assert_eq!(streak().await, 0);
        report_resources(&hub, &auth, 0.95, 5.0, 1).await;
        assert_eq!(streak().await, 1, "one pressuring report");
        report_resources(&hub, &auth, 0.95, 50.0, 2).await;
        assert_eq!(streak().await, 2, "consecutive pressuring reports accrue");
        report_resources(&hub, &auth, 0.10, 5.0, 3).await;
        assert_eq!(streak().await, 0, "an under-threshold report resets");
        // A report without usable gauges also resets: no data, no pressure.
        hub.report(NodeReport {
            auth: auth.clone(),
            version: "test".into(),
            state: "online".into(),
            capabilities: vec![],
            streams: vec![],
            operations: vec![],
            events: vec![],
            metrics: resource_metrics(0.95, 5.0)
                .into_iter()
                .filter(|(key, _)| key != "node_memory_total_bytes")
                .collect(),
            jobs: BTreeMap::new(),
            configuration: None,
            configuration_version: None,
            boot_id: Some("boot".into()),
            report_seq: 4,
        })
        .await
        .unwrap();
        assert_eq!(streak().await, 0, "gauge-less report is not pressuring");
    }

    #[tokio::test]
    async fn opt_in_pressure_rebalance_relocates_with_fencing() {
        let hub = Hub::new(config());
        let session_a = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: vec![],
                boot_id: Some("boot".into()),
            })
            .await
            .unwrap();
        hub.register(RegisterRequest {
            data_address: None,
            node_id: "node-b".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot".into()),
        })
        .await
        .unwrap();
        let auth_a = AgentAuth {
            node_id: "node-a".into(),
            session_token: session_a.session_token.clone(),
        };
        // node-a starts healthier and wins the first placement.
        report_resources(&hub, &auth_a, 0.1, 10.0, 1).await;
        hub.upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json: rebalance_job_spec_json("orders"),
            desired_state: "running".into(),
            observed_state: "validated".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: vec![],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        assert_eq!(start_operations(&hub, "orders").await[0].node_id, "node-a");
        complete_start_commands(&hub, "node-a", &session_a.session_token).await;
        assert_eq!(
            start_operations(&hub, "orders")
                .await
                .into_iter()
                .filter(|operation_record| operation_record.state == HubOperationState::Succeeded)
                .count(),
            1
        );
        // Sustained pressure: `pressure_streak` consecutive pressuring
        // reports from node-a.
        report_resources(&hub, &auth_a, 0.99, 5.0, 2).await;
        report_resources(&hub, &auth_a, 0.99, 5.0, 3).await;
        hub.reconcile_jobs().await.unwrap();
        let starts = start_operations(&hub, "orders").await;
        assert!(
            starts
                .iter()
                .any(|operation_record| operation_record.node_id == "node-a"
                    && operation_record.state == HubOperationState::Superseded),
            "the abandoned node's start must be superseded: {starts:?}"
        );
        assert!(
            starts
                .iter()
                .any(|operation_record| operation_record.node_id == "node-b"
                    && matches!(
                        operation_record.state,
                        HubOperationState::Queued
                            | HubOperationState::Dispatched
                            | HubOperationState::Acknowledged
                            | HubOperationState::Running
                            | HubOperationState::Succeeded
                    )),
            "the Job must be re-placed onto the remaining target: {starts:?}"
        );
        let stops = hub
            .operations(None)
            .await
            .into_iter()
            .filter(|operation_record| {
                operation_record.resource_id == "orders"
                    && operation_record.operation == "job_stop"
                    && operation_record.node_id == "node-a"
            })
            .count();
        assert_eq!(stops, 1, "the abandoned node must receive a stop command");
    }

    #[tokio::test]
    async fn default_off_policy_never_disturbs_a_pressured_placement() {
        let hub = Hub::new(config());
        let session_a = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: vec![],
                boot_id: Some("boot".into()),
            })
            .await
            .unwrap();
        let auth_a = AgentAuth {
            node_id: "node-a".into(),
            session_token: session_a.session_token.clone(),
        };
        hub.upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json: job_spec_json("orders"),
            desired_state: "running".into(),
            observed_state: "validated".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: vec![],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        complete_start_commands(&hub, "node-a", &session_a.session_token).await;
        // Sustained pressure without the opt-in: placement stays untouched.
        for seq in 1..=5 {
            report_resources(&hub, &auth_a, 0.99, 5.0, seq).await;
        }
        hub.reconcile_jobs().await.unwrap();
        let starts = start_operations(&hub, "orders").await;
        assert_eq!(starts.len(), 1);
        assert_eq!(starts[0].node_id, "node-a");
        assert_eq!(starts[0].state, HubOperationState::Succeeded);
        assert!(hub
            .operations(None)
            .await
            .iter()
            .all(|operation_record| operation_record.operation != "job_stop"));
    }

    #[tokio::test]
    async fn single_node_fleet_skips_relocation_under_pressure() {
        let hub = Hub::new(config());
        let session_a = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: vec![],
                boot_id: Some("boot".into()),
            })
            .await
            .unwrap();
        let auth_a = AgentAuth {
            node_id: "node-a".into(),
            session_token: session_a.session_token.clone(),
        };
        hub.upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json: rebalance_job_spec_json("orders"),
            desired_state: "running".into(),
            observed_state: "validated".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: vec![],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        complete_start_commands(&hub, "node-a", &session_a.session_token).await;
        for seq in 1..=4 {
            report_resources(&hub, &auth_a, 0.99, 99.0, seq).await;
        }
        hub.reconcile_jobs().await.unwrap();
        let starts = start_operations(&hub, "orders").await;
        assert_eq!(starts.len(), 1);
        assert_eq!(starts[0].node_id, "node-a");
        assert_eq!(starts[0].state, HubOperationState::Succeeded);
        assert!(hub
            .operations(None)
            .await
            .iter()
            .all(|operation_record| operation_record.operation != "job_stop"));
    }

    /// Multi-component co-location is order sensitive: the component the
    /// ranked order put on the head node must still be there when the
    /// retained placement re-dispatches (e.g. after a version bump), instead
    /// of drifting with BTreeSet iteration order.
    #[tokio::test]
    async fn retained_replacement_reproduces_the_dispatch_order() {
        let hub = Hub::new(config());
        let session_a = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: vec![],
                boot_id: Some("boot".into()),
            })
            .await
            .unwrap();
        let session_b = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "node-b".into(),
                node_token: "node-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: vec![],
                boot_id: Some("boot".into()),
            })
            .await
            .unwrap();
        let auth_a = AgentAuth {
            node_id: "node-a".into(),
            session_token: session_a.session_token.clone(),
        };
        let auth_b = AgentAuth {
            node_id: "node-b".into(),
            session_token: session_b.session_token.clone(),
        };
        // node-b reports more headroom and must win the head of the ranked
        // order despite sorting after node-a by id.
        report_resources(&hub, &auth_a, 0.9, 10.0, 1).await;
        report_resources(&hub, &auth_b, 0.1, 10.0, 1).await;

        let two_component_spec = |version: u64| {
            serde_json::json!({
                "id": "orders",
                "version": version,
                "placement": "colocated",
                "operators": [
                    {"id": "source-a", "kind": "source"},
                    {"id": "sink-a", "kind": "sink"},
                    {"id": "source-b", "kind": "source"},
                    {"id": "sink-b", "kind": "sink"}
                ],
                "edges": [
                    {"id": "e-a", "from": "source-a", "to": "sink-a"},
                    {"id": "e-b", "from": "source-b", "to": "sink-b"}
                ],
                "sources": [
                    {"operator_id": "source-a", "input_type": "memory", "time": {"mode": "processing_time"}},
                    {"operator_id": "source-b", "input_type": "memory", "time": {"mode": "processing_time"}}
                ],
                "sinks": [
                    {"operator_id": "sink-a", "output_type": "drop"},
                    {"operator_id": "sink-b", "output_type": "drop"}
                ]
            })
            .to_string()
        };
        async fn component_tasks(hub: &Hub, auth: &AgentAuth) -> Option<Vec<String>> {
            hub.commands(auth.clone())
                .await
                .ok()?
                .into_iter()
                .find(|command| command.operation == "job_start")?
                .payload
                .map(|payload| {
                    payload["assignments"]
                        .as_array()
                        .map(|assignments| {
                            let mut tasks: Vec<String> = assignments
                                .iter()
                                .filter_map(|assignment| {
                                    assignment["task_id"].as_str().map(String::from)
                                })
                                .collect();
                            tasks.sort();
                            tasks
                        })
                        .unwrap_or_default()
                })
        }

        hub.upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json: two_component_spec(1),
            desired_state: "running".into(),
            observed_state: "validated".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: vec![],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        let first_on_b = component_tasks(&hub, &auth_b).await.expect("node-b start command");
        let first_on_a = component_tasks(&hub, &auth_a).await.expect("node-a start command");
        assert!(!first_on_b.is_empty() && !first_on_a.is_empty());
        assert!(
            first_on_b.iter().all(|task| !first_on_a.contains(task)),
            "components must be split across the two nodes"
        );
        complete_start_commands(&hub, "node-a", &session_a.session_token).await;
        complete_start_commands(&hub, "node-b", &session_b.session_token).await;

        // Version bump: same node set, same healthy state — the retained
        // placement must re-dispatch with the identical component mapping.
        hub.upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 2,
            spec_json: two_component_spec(2),
            desired_state: "running".into(),
            observed_state: "validated".into(),
            convergence: "pending".into(),
            generation: 2,
            node_ids: vec![],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        assert_eq!(
            component_tasks(&hub, &auth_b).await.expect("node-b re-dispatch"),
            first_on_b,
            "the head node must keep its components across re-dispatches"
        );
        assert_eq!(
            component_tasks(&hub, &auth_a).await.expect("node-a re-dispatch"),
            first_on_a
        );
    }

    #[tokio::test]
    async fn pinned_placement_rejects_auto_rebalance() {
        let hub = Hub::new(config());
        hub.register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot".into()),
        })
        .await
        .unwrap();
        let error = hub
            .upsert_job(JobRecord {
                job_id: "orders".into(),
                version: 1,
                spec_json: rebalance_job_spec_json("orders"),
                desired_state: "running".into(),
                observed_state: "validated".into(),
                convergence: "pending".into(),
                generation: 1,
                node_ids: vec!["node-a".into()],
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 0,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, HubError::Invalid(_)));
    }

    #[tokio::test]
    async fn rebalance_cooldown_blocks_a_move_inside_the_window() {
        let hub = Hub::new(config());
        let session_a = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: vec![],
                boot_id: Some("boot".into()),
            })
            .await
            .unwrap();
        let auth_a = AgentAuth {
            node_id: "node-a".into(),
            session_token: session_a.session_token.clone(),
        };
        // Same auto policy but with a cooldown far in the future relative to
        // the fresh placement.
        let mut spec: serde_json::Value =
            serde_json::from_str(&job_spec_json("orders")).unwrap();
        spec["rebalance"] =
            serde_json::json!({"mode": "auto", "pressure_streak": 1, "cooldown_ms": 3_600_000});
        hub.upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json: spec.to_string(),
            desired_state: "running".into(),
            observed_state: "validated".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: vec![],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        complete_start_commands(&hub, "node-a", &session_a.session_token).await;
        // Pressure trip inside the cooldown window: the placement holds.
        for seq in 1..=3 {
            report_resources(&hub, &auth_a, 0.99, 99.0, seq).await;
        }
        hub.reconcile_jobs().await.unwrap();
        let starts = start_operations(&hub, "orders").await;
        assert_eq!(starts.len(), 1);
        assert_eq!(starts[0].node_id, "node-a");
        assert_eq!(starts[0].state, HubOperationState::Succeeded);
        assert!(hub
            .operations(None)
            .await
            .iter()
            .all(|operation_record| operation_record.operation != "job_stop"));
    }
}
