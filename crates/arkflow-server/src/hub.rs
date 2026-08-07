//! Hub-side node registry and command broker.
//!
//! The Hub owns fleet state; compute nodes own execution. This module contains
//! the transport-neutral state machine used by the HTTP handlers and Agent
//! client protocol.

use crate::agent::{delete_checkpoint_artifact, recovery_record_is_valid};
use crate::api_contract::{OperatorAction, OperatorPrincipal, OperatorRole, ResourceScope};
use crate::storage::{
    AttemptRecord, DesiredMutation, IntentRecord, JobCheckpointRecord, JobRecord, NodeMutation,
    ObservedMutation, PersistedOperation, RolloutRecord, RolloutTargetRecord, RolloutTargetUpdate,
    StorageActor, StorageError,
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
];

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
    rollouts: Arc<RwLock<BTreeMap<String, RolloutRecord>>>,
    events: Arc<RwLock<VecDeque<HubEvent>>>,
    updates: broadcast::Sender<HubEvent>,
    storage: Option<StorageActor>,
    lifecycle: Arc<RwLock<HubLifecycle>>,
    jobs: Arc<RwLock<BTreeMap<String, JobRecord>>>,
    job_checkpoints: Arc<RwLock<BTreeMap<String, Vec<JobCheckpointRecord>>>>,
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
            job_checkpoints: Arc::new(RwLock::new(BTreeMap::new())),
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
        if let Some(storage) = &self.storage {
            job = storage.upsert_job(job).await.map_err(HubError::from)?;
        } else {
            let mut jobs = self.jobs.write().await;
            job.generation = jobs
                .get(&job.job_id)
                .map(|current| current.generation.saturating_add(1))
                .unwrap_or_else(|| job.generation.max(1));
            jobs.insert(job.job_id.clone(), job.clone());
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

    /// Reconcile a bounded set of durable Jobs so Agent failures and Hub
    /// recovery converge without waiting for a new lifecycle request.
    pub async fn reconcile_jobs(&self) -> Result<usize, HubError> {
        let jobs = self.jobs().await?;
        let mut dispatched = 0;
        for job in jobs.into_iter().take(MAX_JOB_RECONCILIATIONS_PER_TICK) {
            dispatched += self.reconcile_job(&job).await?;
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
        let previous_nodes = self
            .operations
            .read()
            .await
            .values()
            .filter(|operation_record| {
                operation_record.resource_id == job.job_id
                    && operation_record.operation == "job_start"
                    && operation_record.generation == job.generation
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
        let targets =
            if operation == "job_start" && job.node_ids.is_empty() && previous_nodes_all_online {
                previous_nodes.iter().cloned().collect::<Vec<_>>()
            } else if operation == "job_stop" {
                let target_ids = if previous_nodes.is_empty() {
                    targets.iter().cloned().collect::<BTreeSet<_>>()
                } else {
                    previous_nodes.clone()
                };
                targets
                    .into_iter()
                    .filter(|node_id| target_ids.contains(node_id))
                    .collect::<Vec<_>>()
            } else {
                targets
            };
        let target_ids = targets.iter().cloned().collect::<BTreeSet<_>>();
        let placement_changed = operation == "job_start" && target_ids != previous_nodes;
        if operation == "job_start" {
            let nodes_to_stop = if placement_changed {
                previous_nodes.iter().collect::<Vec<_>>()
            } else {
                previous_nodes.difference(&target_ids).collect::<Vec<_>>()
            };
            for node_id in nodes_to_stop {
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
        let assignments = plan.assignments_for_nodes(&targets, job.generation);
        let mut recovery_candidates = self
            .job_checkpoints(&job.job_id)
            .await?
            .into_iter()
            .filter(|record| record.status == "completed")
            .filter(|record| recovery_record_is_compatible(&spec, record))
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
        let mut dispatched = 0;
        for node_id in targets {
            if operation == "job_start"
                && !placement_changed
                && self
                    .operations
                    .read()
                    .await
                    .values()
                    .any(|operation_record| {
                        operation_record.node_id == node_id
                            && operation_record.resource_id == job.job_id
                            && operation_record.operation == "job_start"
                            && operation_record.generation == job.generation
                            && operation_record.state == HubOperationState::Succeeded
                    })
            {
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
            let payload = Some(serde_json::json!({
                "job_id": job.job_id,
                "spec": spec,
                "plan": plan,
                "assignments": node_assignments,
                "generation": job.generation,
                "recovery": recovery,
            }));
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
            storage
                .update_job(
                    job_id,
                    None,
                    Some(observed_state.into()),
                    Some(convergence.into()),
                    Some(generation),
                    checkpoint_id.map(str::to_owned),
                    last_error.map(str::to_owned),
                )
                .await
                .map_err(HubError::from)?
        } else {
            let mut jobs = self.jobs.write().await;
            let Some(job) = jobs.get_mut(job_id) else {
                return Ok(None);
            };
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
        ) {
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
        if let Some(storage) = &self.storage {
            storage
                .upsert_job_checkpoint(record.clone())
                .await
                .map_err(HubError::from)?;
            let job = storage
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
                .map_err(HubError::from)?;
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
        job.checkpoint_id = Some(record.checkpoint_id.clone());
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
        let assignments = plan.assignments_for_nodes(&targets, job.generation);
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
            delete_checkpoint_artifact(spec, &artifact).map_err(HubError::Invalid)?;
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
            return Some(OperatorPrincipal::legacy_operator());
        };
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
        if let Some(expected) = self.config.node_token.as_deref() {
            if !bool::from(request.node_token.as_bytes().ct_eq(expected.as_bytes())) {
                return Err(HubError::Unauthorized);
            }
        }
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
        let session_token = format!(
            "node-session-{}",
            SESSION_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
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
                boot_id: old.as_ref().and_then(|record| record.boot_id.clone()),
                report_seq: old.as_ref().map(|record| record.report_seq).unwrap_or(0),
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
                    capabilities_json: serde_json::to_string(&sanitize_capabilities(
                        request.capabilities,
                    ))
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
        node.configuration = report.configuration;
        let persisted_version = node.resource.version.clone();
        let persisted_state = format!("{:?}", node.resource.state).to_lowercase();
        let persisted_capabilities =
            serde_json::to_string(&node.resource.capabilities).unwrap_or_else(|_| "[]".into());
        let persisted_boot_id = node.boot_id.clone();
        let persisted_report_seq = Some(node.report_seq);
        let persisted_lease = node.resource.lease_expires_at_ms;
        drop(nodes);
        let invalidated_job_starts = if boot_changed {
            let mut operations = self.operations.write().await;
            operations
                .values_mut()
                .filter(|operation| {
                    operation.node_id == report.auth.node_id
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
                    operation.state = HubOperationState::NodeUnavailable;
                    operation.finished_at_ms = Some(now);
                    operation.clone()
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
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
        let node = nodes.get_mut(&node_id).ok_or(HubError::NodeUnavailable)?;
        if node.resource.state != NodeConnectionState::Online
            || node.resource.lease_expires_at_ms <= now
            || node.resource.maintenance_state != NodeMaintenanceState::Active
        {
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
            return Err(HubError::Invalid(message));
        }
        let mut operations = self.operations.write().await;
        let requested_checkpoint_id = payload
            .as_ref()
            .and_then(|payload| payload.get("checkpoint_id"))
            .and_then(serde_json::Value::as_str);
        if let Some(operation_id) = operation_id_override.as_deref() {
            if let Some(existing) = operations.get(operation_id) {
                return Ok(existing.clone());
            }
        } else if let Some(existing) = operations.values().find(|item| {
            item.node_id == node_id
                && item.resource_id == resource_id
                && item.operation == operation
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
            rollout_id,
            correlation_id,
            payload,
            required_capabilities,
        };
        node.commands.push_back(command);
        if operations.len() >= MAX_OPERATIONS {
            if let Some(oldest) = operations.keys().next().cloned() {
                operations.remove(&oldest);
            }
        }
        operations.insert(operation_record.id.clone(), operation_record.clone());
        drop(operations);
        drop(nodes);
        if let Some(storage) = self.storage.as_ref() {
            persist_operation(storage, &operation_record)
                .await
                .map_err(HubError::from)?;
        }
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
                let expected_nodes = checkpoint_operations
                    .iter()
                    .map(|operation| operation.node_id.clone())
                    .collect::<BTreeSet<_>>();
                let succeeded_nodes = checkpoint_operations
                    .iter()
                    .filter(|operation| operation.state == HubOperationState::Succeeded)
                    .map(|operation| operation.node_id.clone())
                    .collect::<BTreeSet<_>>();
                let all_nodes_succeeded = if result.state == HubOperationState::Succeeded
                    && checkpoint_id.is_some()
                    && !expected_nodes.is_empty()
                {
                    expected_nodes.is_subset(&succeeded_nodes)
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
            let observed_state = if matches!(
                result.state,
                HubOperationState::Succeeded | HubOperationState::Running
            ) {
                if updated.operation == "job_stop" {
                    "stopped"
                } else {
                    "running"
                }
            } else {
                "failed"
            };
            let _ = self
                .observe_job(
                    &updated.resource_id,
                    updated.generation,
                    observed_state,
                    None,
                    updated.error.as_deref(),
                )
                .await?;
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
        checkpoint_id: None,
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

fn sanitize_metrics(metrics: BTreeMap<String, f64>) -> BTreeMap<String, f64> {
    metrics
        .into_iter()
        .filter(|(key, value)| {
            ALLOWED_NODE_METRICS.contains(&key.as_str()) && value.is_finite() && *value >= 0.0
        })
        .collect()
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
    record.job_version == spec.version.0 && record.format_version == job_state_format_version(spec)
}

fn job_state_format_version(spec: &arkflow_core::job::JobSpec) -> u32 {
    spec.state
        .as_ref()
        .map(|state| state.format_version)
        .unwrap_or(1)
}
static SESSION_SEQUENCE: AtomicU64 = AtomicU64::new(1);
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
        let mut old_version = compatible.clone();
        old_version.job_version = 1;
        assert!(!recovery_record_is_compatible(&spec, &old_version));
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

    fn config() -> HubConfig {
        HubConfig {
            operator_token: Some("operator".into()),
            node_token: Some("node-secret".into()),
            lease_ttl_ms: 1000,
            poll_interval_ms: 10,
        }
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
                node_id: node_id.into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["configuration".into()],
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
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["configuration".into()],
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
                session_token: session.session_token,
            },
            version: "agent-1".into(),
            state: "online".into(),
            capabilities: vec!["configuration".into()],
            streams: vec![],
            operations: vec![],
            events: vec![],
            metrics: BTreeMap::new(),
            configuration: None,
            configuration_version: Some("cfg-health".into()),
            boot_id: Some("boot-health".into()),
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
                node_id: "node-a".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["configuration".into()],
            })
            .await
            .unwrap();
        hub.register(RegisterRequest {
            node_id: "node-b".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["configuration".into()],
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
                session_token: node_a.session_token,
            },
            version: "agent-state".into(),
            state: "online".into(),
            capabilities: vec!["configuration".into()],
            streams: vec![],
            operations: vec![],
            events: vec![],
            metrics: BTreeMap::new(),
            configuration: None,
            configuration_version: Some("cfg-state-a".into()),
            boot_id: Some("boot-state".into()),
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
                    node_id: node_id.into(),
                    node_token: "node-secret".into(),
                    protocol_version: "v1".into(),
                    capabilities: vec!["configuration".into()],
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
                auth,
                version: "agent-e2e".into(),
                state: "online".into(),
                capabilities: vec!["configuration".into()],
                streams: vec![],
                operations: vec![],
                events: vec![],
                metrics: BTreeMap::from([("streams_total".into(), 0.0)]),
                configuration: None,
                configuration_version: Some("cfg-e2e".into()),
                boot_id: Some(format!("boot-{node_id}")),
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
    async fn unsupported_capability_is_rejected_before_dispatch() {
        let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
        let storage = crate::storage::StorageActor::start(store, 8);
        let hub = Hub::with_storage(config(), storage.clone());
        assert!(matches!(
            hub.register(RegisterRequest {
                node_id: "incompatible".into(),
                node_token: "node-secret".into(),
                protocol_version: "v0".into(),
                capabilities: vec![],
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
            node_id: "n1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["configuration".into()],
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
                node_id: "compute-1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["job_runtime".into(), "state_backend".into()],
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
    }

    #[tokio::test]
    async fn periodic_job_reconciliation_retries_a_failed_runtime() {
        let storage =
            StorageActor::start(crate::storage::ControlPlaneStore::in_memory().unwrap(), 8);
        let hub = Hub::with_storage(config(), storage);
        let registration = hub
            .register(RegisterRequest {
                node_id: "compute-1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["job_runtime".into(), "state_backend".into()],
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
            node_id: "compute-1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["job_runtime".into(), "state_backend".into()],
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
                node_id: "compute-1".into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["job_runtime".into(), "state_backend".into()],
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
}
