//! Compute-node Agent client for the Hub pull protocol.

use crate::hub::{
    AgentAuth, AgentCommand, CommandResult, HeartbeatRequest, HubOperationState, NodeReport,
    RegisterRequest, RegisterResponse,
};
use arkflow_core::checkpoint::{
    recovery_manifest_key, CheckpointCoordinator, CheckpointRepository, CheckpointStatus,
    CheckpointStore, RecoveryArtifact, RecoveryArtifactKind, RecoveryPlan, StateSnapshotRef,
    TaskAttemptSnapshot, TaskCheckpointAck,
};
use arkflow_core::configuration::redacted_config;
use arkflow_core::control::OperationState;
use arkflow_core::control_plane::ControlPlane;
use arkflow_core::input::InputConfig;
use arkflow_core::job::{
    JobComponentAdapter, JobPlan, OperatorSpec, SinkSpec, SourceSpec, TaskAttempt,
};
use arkflow_core::output::OutputConfig;
use arkflow_core::processor::ProcessorConfig;
use arkflow_core::state::{RedbStateBackend, StateBackend};
use arkflow_core::temporary::Temporary;
use arkflow_core::Resource;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use reqwest::Client;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use url::Url;

#[derive(Debug, Clone)]
pub struct NodeAgentConfig {
    /// Hub base URL of the active candidate. The failover loop clones the
    /// config with a different `hub_url` per candidate (see `run`), so every
    /// session-scoped call site reads the active address from here.
    pub hub_url: String,
    /// Failover candidates in scan order; `hub_url` is always one of them.
    pub hub_urls: Vec<String>,
    pub api_prefix: String,
    pub node_id: String,
    pub node_token: String,
    pub boot_id: String,
    pub heartbeat_interval: Duration,
    pub report_interval: Duration,
    pub poll_interval: Duration,
    /// Data-plane listen port. `Some` enables the cross-node shuffle data
    /// plane and advertises the `network_shuffle` capability to the Hub;
    /// placement continues to keep every edge co-located until the Hub
    /// learns to split them, so the default (`None`) stays byte-compatible.
    pub data_port: Option<u16>,
    /// Routable host advertised to peers for the data plane. Required for
    /// split placement; without it the node stays colocated-only.
    pub data_host: Option<String>,
}

/// Split-placement context carried by the Hub's job_start payload.
#[derive(Clone, Default)]
struct SplitPlacementPayload {
    /// Full task→node mapping for the Job (remote peers must be nameable).
    task_nodes: Option<BTreeMap<String, String>>,
    /// Peer node → advertised data-plane address.
    node_data_ports: BTreeMap<String, String>,
    /// Hub decision: this start may not initialize an empty durable backend.
    recovery_required: bool,
}

#[derive(Clone, Default)]
struct JobRuntime {
    tasks: Arc<Mutex<BTreeMap<String, JobTask>>>,
    starts: Arc<Mutex<()>>,
    /// Cross-node shuffle data plane. `None` keeps the legacy co-location
    /// contract: every edge is materialized in-process and no data port
    /// listens.
    data_plane: Option<Arc<arkflow_core::executor::remote::NetworkManager>>,
    /// Finished-task observations whose delivery to the Hub failed. They are
    /// retried by the next session; dropping them would leave the Hub
    /// reporting a dead job as running forever (the start operation stays
    /// `Succeeded` and reconcile skips the re-dispatch).
    pending_observations: Arc<Mutex<Vec<FinishedJob>>>,
}

/// One finished kernel task and the outcome its Hub observation carries.
type FinishedJob = (String, u64, Result<(), String>);

struct JobTask {
    generation: u64,
    ephemeral_state: bool,
    recovery_required: bool,
    cancellation: CancellationToken,
    assignments: Vec<TaskAttempt>,
    /// Dedicated bounded runtime for Jobs declaring `resources.cpu_millicores`
    /// (worker threads = ceil(millicores/1000), min 1): one Job cannot occupy
    /// the shared runtime's workers. Shut down (detached, bounded) when the
    /// task is retired. `None` for undeclared Jobs — shared runtime as before.
    dedicated_runtime: Option<Arc<tokio::runtime::Runtime>>,
    watermark_partitions: BTreeMap<String, u32>,
    state: Arc<dyn StateBackend>,
    checkpoint_store_uri: Option<String>,
    /// Unified-kernel handle: command-driven snapshots over the running
    /// graph (the kernel executes the Job's chains).
    kernel: Option<std::sync::Arc<arkflow_core::executor::kernel_handle::KernelJobHandle>>,
    handle: tokio::task::JoinHandle<Result<(), arkflow_core::Error>>,
}

#[derive(Clone)]
struct SharedCheckpointStore {
    client: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
}

impl SharedCheckpointStore {
    pub(crate) fn from_uri(uri: &str) -> Result<Self, String> {
        let url = Url::parse(uri)
            .map_err(|error| format!("invalid checkpoint object_store_uri: {error}"))?;
        let (client, prefix) = object_store::parse_url(&url)
            .map_err(|error| format!("build checkpoint object store: {error}"))?;
        Ok(Self {
            client: Arc::from(client),
            prefix,
        })
    }

    fn path_for(&self, key: &str) -> Result<ObjectPath, arkflow_core::Error> {
        if key.is_empty() || key.contains("..") || key.starts_with('/') {
            return Err(arkflow_core::Error::Config(
                "invalid checkpoint object key".into(),
            ));
        }
        let prefix = self.prefix.to_string();
        Ok(ObjectPath::from(if prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{prefix}/{key}")
        }))
    }

    fn block_on<T, F>(&self, future: F) -> Result<T, arkflow_core::Error>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, object_store::Error>> + Send + 'static,
    {
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| {
                    arkflow_core::Error::Process(format!("build checkpoint runtime: {error}"))
                })?
                .block_on(future)
                .map_err(|error| {
                    arkflow_core::Error::Process(format!("checkpoint object store: {error}"))
                })
        })
        .join()
        .map_err(|_| {
            arkflow_core::Error::Process("checkpoint object store thread panicked".into())
        })?
    }
}

impl CheckpointStore for SharedCheckpointStore {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), arkflow_core::Error> {
        let path = self.path_for(key)?;
        let client = self.client.clone();
        let payload = bytes::Bytes::copy_from_slice(bytes);
        self.block_on(async move { client.put(&path, payload.into()).await.map(|_| ()) })
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, arkflow_core::Error> {
        let path = self.path_for(key)?;
        let client = self.client.clone();
        self.block_on(async move {
            match client.get(&path).await {
                Ok(result) => result.bytes().await.map(|bytes| Some(bytes.to_vec())),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(error) => Err(error),
            }
        })
    }

    fn delete(&self, key: &str) -> Result<(), arkflow_core::Error> {
        let path = self.path_for(key)?;
        let client = self.client.clone();
        self.block_on(async move { client.delete(&path).await })
    }
}

fn checkpoint_repository(
    plan: &JobPlan,
) -> Result<CheckpointRepository<SharedCheckpointStore>, String> {
    let uri = plan
        .spec
        .checkpoint
        .as_ref()
        .ok_or_else(|| "Job has no checkpoint object_store_uri".to_string())?
        .object_store_uri
        .clone();
    Ok(CheckpointRepository::new(SharedCheckpointStore::from_uri(
        &uri,
    )?))
}

pub(crate) fn delete_checkpoint_artifact(
    spec: &arkflow_core::job::JobSpec,
    artifact: &RecoveryArtifact,
) -> Result<(), String> {
    let uri = spec
        .checkpoint
        .as_ref()
        .ok_or_else(|| "Job has no checkpoint object_store_uri".to_string())?
        .object_store_uri
        .clone();
    CheckpointRepository::new(SharedCheckpointStore::from_uri(&uri)?)
        .delete(artifact)
        .map_err(|error| error.to_string())
}

fn recovery_artifact(
    plan: &JobPlan,
    checkpoint_id: &str,
    savepoint: bool,
) -> Result<RecoveryArtifact, String> {
    let kind = if savepoint {
        RecoveryArtifactKind::Savepoint
    } else {
        RecoveryArtifactKind::Checkpoint
    };
    Ok(RecoveryArtifact {
        id: checkpoint_id.to_owned(),
        kind,
        manifest_key: recovery_manifest_key(kind, checkpoint_id),
        job_version: plan.spec.version,
        format_version: plan
            .spec
            .state
            .as_ref()
            .map(|state| state.format_version)
            .unwrap_or(1),
        created_at_ms: 0,
        status: CheckpointStatus::Completed,
    })
}

fn validate_recovery_manifest(
    plan: &JobPlan,
    checkpoint_id: &str,
    state_format_version: u32,
    manifest: &arkflow_core::checkpoint::CheckpointManifest,
    rescale: bool,
) -> Result<(), String> {
    if manifest.checkpoint_id != checkpoint_id {
        return Err(format!(
            "recovery artifact '{checkpoint_id}' does not match the dispatched checkpoint"
        ));
    }
    // The SAME shared evaluation the Hub authorization and the repository
    // sealing apply: an equal state format permits a target-version upgrade,
    // while downgrades, format changes, checksum failures, and manifests
    // without the complete planned task set are incompatible for everyone.
    // A rescale-declared Job waives only the task-set equality half: every
    // entry is redistributed to the new plan's key-group owners at restore
    // instead of restoring per-task snapshots verbatim.
    let planned_tasks = plan
        .tasks
        .iter()
        .map(|task| task.id.clone())
        .collect::<BTreeSet<_>>();
    let compatibility = if rescale {
        let identity = arkflow_core::checkpoint::evaluate_recovery_identity(
            manifest,
            &plan.spec.id,
            plan.spec.version,
            state_format_version,
        );
        if identity.is_compatible()
            && arkflow_core::checkpoint::manifest_has_duplicate_tasks(manifest)
        {
            arkflow_core::checkpoint::RecoveryCompatibility::reject(format!(
                "checkpoint '{}' contains duplicate task entries",
                manifest.checkpoint_id
            ))
        } else {
            identity
        }
    } else {
        arkflow_core::checkpoint::evaluate_recovery_compatibility(
            manifest,
            &plan.spec.id,
            plan.spec.version,
            state_format_version,
            &planned_tasks,
        )
    };
    if !compatibility.is_compatible() {
        return Err(format!(
            "recovery artifact '{checkpoint_id}' is incompatible with Job '{}' version {} state format {}: {}",
            plan.spec.id,
            plan.spec.version.0,
            state_format_version,
            compatibility.reason.unwrap_or_default()
        ));
    }
    Ok(())
}

fn validate_recovery_snapshots<S: CheckpointStore>(
    plan: &JobPlan,
    repository: &CheckpointRepository<S>,
    manifest: &arkflow_core::checkpoint::CheckpointManifest,
    rescale: bool,
) -> Result<(), String> {
    let planned_tasks = plan
        .tasks
        .iter()
        .map(|task| task.id.clone())
        .collect::<BTreeSet<_>>();
    if rescale {
        // Duplicate references still mean a corrupted seal; the task set
        // legitimately differs from the plan when redistributing.
        arkflow_core::checkpoint::validate_state_snapshot_tasks_unique(&manifest.state_snapshots)?;
    } else {
        arkflow_core::checkpoint::validate_state_snapshot_task_set(
            &manifest.state_snapshots,
            &planned_tasks,
        )?;
    }
    let expected_prefix =
        arkflow_core::job::state_namespace_prefix(&plan.spec.id, plan.spec.state.as_ref());
    for snapshot_ref in &manifest.state_snapshots {
        let snapshot = repository
            .read_state_snapshot(snapshot_ref)
            .map_err(|error| error.to_string())?;
        arkflow_core::checkpoint::validate_state_snapshot_namespace(&snapshot, &expected_prefix)?;
    }
    Ok(())
}

pub(crate) fn recovery_record_is_valid(
    spec: &arkflow_core::job::JobSpec,
    record: &crate::storage::JobCheckpointRecord,
) -> bool {
    let Ok(plan) = JobPlan::compile(spec.clone()) else {
        return false;
    };
    let kind = match record.kind.as_str() {
        "checkpoint" => RecoveryArtifactKind::Checkpoint,
        "savepoint" => RecoveryArtifactKind::Savepoint,
        _ => return false,
    };
    let Ok(repository) = checkpoint_repository(&plan) else {
        return false;
    };
    let artifact = RecoveryArtifact {
        id: record.checkpoint_id.clone(),
        kind,
        manifest_key: recovery_manifest_key(kind, &record.checkpoint_id),
        job_version: spec.version,
        format_version: record.format_version,
        created_at_ms: record.created_at_ms,
        status: CheckpointStatus::Completed,
    };
    let Ok(manifest) = repository.read_manifest(&artifact) else {
        return false;
    };
    if validate_recovery_manifest(
        &plan,
        &record.checkpoint_id,
        record.format_version,
        &manifest,
        spec.rescale,
    )
    .is_err()
    {
        return false;
    }
    if validate_recovery_snapshots(&plan, &repository, &manifest, spec.rescale).is_err() {
        return false;
    }
    let task_ids = manifest
        .task_attempts
        .iter()
        .map(|attempt| attempt.task_id.clone())
        .collect::<BTreeSet<_>>();
    if task_ids.is_empty()
        || arkflow_core::checkpoint::validate_state_snapshot_task_set(
            &manifest.state_snapshots,
            &task_ids,
        )
        .is_err()
    {
        return false;
    }
    manifest
        .state_snapshots
        .iter()
        .all(|snapshot| repository.read_state_snapshot(snapshot).is_ok())
}

/// Restore the recovery artifact's keyed state into this node's backend.
///
/// Exact task set: only the snapshots referenced by this node's assignments
/// are read (the pre-existing behavior). Rescale redistribution (`task sets
/// differ && spec.rescale`): EVERY snapshot is read, each entry is rewritten
/// to its new key-group owner's namespace, and only entries owned by this
/// node's assignments are restored — across the fleet each entry lands on
/// exactly one node.
fn restore_recovery_state<S: CheckpointStore>(
    plan: &JobPlan,
    repository: &CheckpointRepository<S>,
    manifest: &arkflow_core::checkpoint::CheckpointManifest,
    assignments: &[arkflow_core::job::TaskAttempt],
    state: &Arc<dyn StateBackend>,
    redistribute: bool,
) -> Result<(), String> {
    let assigned_task_ids = assignments
        .iter()
        .map(|assignment| assignment.task_id.as_str())
        .collect::<BTreeSet<_>>();
    if redistribute {
        let context = arkflow_core::executor::job_runner_adapter::RescaleContext::from_plan(plan)
            .map_err(|error| error.to_string())?;
        let mut entries = Vec::new();
        for snapshot_ref in &manifest.state_snapshots {
            let snapshot = repository
                .read_state_snapshot(snapshot_ref)
                .map_err(|error| error.to_string())?;
            for entry in snapshot.entries {
                let entry = context
                    .redistribute(entry)
                    .map_err(|error| error.to_string())?;
                let owner =
                    arkflow_core::executor::job_runner_adapter::RescaleContext::task_of_namespace(
                        &entry.namespace,
                    )
                    .map_err(|error| error.to_string())?;
                if assigned_task_ids.contains(owner.as_str()) {
                    entries.push(entry);
                }
            }
        }
        let snapshot = arkflow_core::state::StateSnapshot::new(state.format_version(), entries);
        return state.restore(&snapshot).map_err(|error| error.to_string());
    }
    let mut snapshots = manifest
        .state_snapshots
        .iter()
        .filter(|snapshot_ref| assigned_task_ids.contains(snapshot_ref.task_id.as_str()))
        .map(|snapshot_ref| {
            repository
                .read_state_snapshot(snapshot_ref)
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    if snapshots.len() > 1 {
        let entries = snapshots
            .drain(..)
            .flat_map(|snapshot| snapshot.entries)
            .collect();
        let snapshot = arkflow_core::state::StateSnapshot::new(state.format_version(), entries);
        state.restore(&snapshot).map_err(|error| error.to_string())
    } else if let Some(snapshot) = snapshots.pop() {
        state.restore(&snapshot).map_err(|error| error.to_string())
    } else {
        Ok(())
    }
}

fn parse_recovery_payload(
    payload: &serde_json::Value,
) -> Result<(Option<String>, bool, bool), String> {
    let recovery_required = payload
        .get("recovery_required")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let Some(recovery) = payload.get("recovery") else {
        return Ok((None, false, recovery_required));
    };
    if recovery.is_null() {
        return Ok((None, false, recovery_required));
    }
    let checkpoint_id = recovery
        .get("checkpoint_id")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| "recovery payload is missing checkpoint_id".to_string())?;
    let savepoint = recovery
        .get("savepoint")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    Ok((Some(checkpoint_id.to_owned()), savepoint, recovery_required))
}

impl Drop for JobTask {
    /// Last-resort guard: a JobTask dropped without one of the explicit
    /// retirement paths must not let its `Arc<Runtime>` drop inside an
    /// async context (Runtime::drop panics there). The explicit paths
    /// `take()` the runtime first, so this only fires on missed paths. No
    /// runtime context (process teardown) parks the shutdown on a bare
    /// thread instead.
    fn drop(&mut self) {
        if let Some(runtime) = self.dedicated_runtime.take() {
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    handle.spawn_blocking(move || {
                        if let Ok(runtime) = Arc::try_unwrap(runtime) {
                            runtime.shutdown_timeout(Duration::from_secs(10));
                        }
                    });
                }
                Err(_) => {
                    std::thread::spawn(move || {
                        if let Ok(runtime) = Arc::try_unwrap(runtime) {
                            runtime.shutdown_timeout(Duration::from_secs(10));
                        }
                    });
                }
            }
        }
    }
}

/// Bounded, detached teardown of a dedicated runtime. Takes the task's
/// reference (callers `take()` it before awaiting the kernel handle) so the
/// final Arc never drops inside an async context (Runtime::drop panics
/// there); the shutdown itself runs on the blocking pool, outside every
/// runtime's async context. Must run only AFTER the kernel handle resolved:
/// shutting the runtime down first would strand the JoinHandle.
fn shutdown_dedicated_runtime(dedicated: Option<Arc<tokio::runtime::Runtime>>) {
    if let Some(runtime) = dedicated {
        tokio::task::spawn_blocking(move || {
            if let Ok(runtime) = Arc::try_unwrap(runtime) {
                runtime.shutdown_timeout(Duration::from_secs(10));
            }
        });
    }
}

impl JobRuntime {
    async fn generation(&self, job_id: &str) -> Option<u64> {
        self.tasks
            .lock()
            .await
            .get(job_id)
            .map(|task| task.generation)
    }

    /// Per-Job kernel snapshots for the Hub's data-plane metrics export.
    /// Unlike `metrics`, these keep the Job identity so the Hub can label
    /// series per (node, job).
    async fn job_snapshots(
        &self,
    ) -> BTreeMap<String, arkflow_core::executor::metrics::KernelMetricsSnapshot> {
        self.tasks
            .lock()
            .await
            .iter()
            .filter_map(|(job_id, task)| {
                task.kernel
                    .as_ref()
                    .map(|kernel| (job_id.clone(), kernel.metrics().snapshot()))
            })
            .collect()
    }

    /// Task ids each running Job kernel executes on this node, keyed by job
    /// id: the observed runtime state the Hub merges over desired placement.
    async fn job_tasks(&self) -> BTreeMap<String, Vec<String>> {
        self.tasks
            .lock()
            .await
            .iter()
            .filter_map(|(job_id, task)| {
                task.kernel.as_ref().map(|_| {
                    (
                        job_id.clone(),
                        task.assignments
                            .iter()
                            .map(|attempt| attempt.task_id.clone())
                            .collect::<Vec<_>>(),
                    )
                })
            })
            .collect()
    }

    /// Aggregate unified-kernel counters for the Agent report.  A JobTask owns
    /// one kernel handle even when its assigned subgraph has several chains;
    /// summing throughput while taking the maximum latency/lag keeps the
    /// node-level report useful without exposing internal task handles.
    async fn metrics(&self) -> BTreeMap<String, f64> {
        let tasks = self.tasks.lock().await;
        let mut batches_in = 0_u64;
        let mut batches_out = 0_u64;
        let mut rows = 0_u64;
        let mut errors = 0_u64;
        let mut in_flight = 0_u64;
        let mut mean_latency_us = 0_u64;
        let mut checkpoint_duration_ms = 0_u64;
        let mut checkpoint_failures = 0_u64;
        let mut watermark_lag_ms = 0_u64;
        let mut late_events = 0_u64;
        let mut ephemeral_jobs = 0_u64;
        let mut recovery_required_jobs = 0_u64;

        for task in tasks.values() {
            if task.ephemeral_state {
                ephemeral_jobs = ephemeral_jobs.saturating_add(1);
            }
            if task.recovery_required {
                recovery_required_jobs = recovery_required_jobs.saturating_add(1);
            }
            let Some(kernel) = task.kernel.as_ref() else {
                continue;
            };
            let snapshot = kernel.metrics().snapshot();
            for chain in snapshot.chains.values() {
                batches_in = batches_in.saturating_add(chain.batches_in);
                batches_out = batches_out.saturating_add(chain.batches_out);
                rows = rows.saturating_add(chain.rows);
                errors = errors.saturating_add(chain.errors);
                in_flight = in_flight.saturating_add(chain.in_flight);
                mean_latency_us = mean_latency_us.max(chain.mean_latency_us);
            }
            checkpoint_duration_ms = checkpoint_duration_ms.max(snapshot.checkpoint_duration_ms);
            checkpoint_failures = checkpoint_failures.saturating_add(snapshot.checkpoint_failures);
            watermark_lag_ms = watermark_lag_ms.max(snapshot.watermark_lag_ms);
            late_events = late_events.saturating_add(snapshot.late_events);
        }

        BTreeMap::from([
            ("kernel_batches_in".into(), batches_in as f64),
            ("kernel_batches_out".into(), batches_out as f64),
            ("kernel_rows".into(), rows as f64),
            ("kernel_errors".into(), errors as f64),
            ("in_flight".into(), in_flight as f64),
            ("mean_latency_us".into(), mean_latency_us as f64),
            (
                "checkpoint_duration_ms".into(),
                checkpoint_duration_ms as f64,
            ),
            ("checkpoint_failures".into(), checkpoint_failures as f64),
            ("watermark_lag_ms".into(), watermark_lag_ms as f64),
            ("late_events".into(), late_events as f64),
            ("jobs_total".into(), tasks.len() as f64),
            ("jobs_running".into(), tasks.len() as f64),
            ("jobs_ephemeral_state".into(), ephemeral_jobs as f64),
            (
                "jobs_recovery_required".into(),
                recovery_required_jobs as f64,
            ),
        ])
    }

    #[allow(clippy::too_many_arguments)]
    async fn start(
        &self,
        plan: JobPlan,
        assignments: Vec<TaskAttempt>,
        generation: u64,
        recovery_id: Option<String>,
        recovery_savepoint: bool,
        node_id: &str,
        split: &SplitPlacementPayload,
    ) -> Result<(), String> {
        let _start_guard = self.starts.lock().await;
        let job_id = plan.spec.id.to_string();
        if assignments.is_empty() {
            return Err("Job command contains no task assignments".into());
        }
        if split.recovery_required && recovery_id.is_none() {
            return Err(format!(
                "durable Job '{job_id}' requires recovery but no compatible checkpoint was supplied"
            ));
        }
        let ephemeral_state =
            plan.spec.state.as_ref().is_some_and(|state| {
                state.durability == arkflow_core::job::StateDurability::Ephemeral
            });
        let recovery_required = split.recovery_required;
        let local_recovery_marker = durable_recovery_marker(&plan, node_id, generation);
        let (existing, previous_exited_on_its_own) = {
            let tasks = self.tasks.lock().await;
            if tasks
                .get(&job_id)
                .is_some_and(|task| task.generation > generation)
            {
                return Err("job generation is stale".into());
            }
            // A re-delivered start at the generation already running is a
            // no-op success — but only while that kernel is actually alive
            // AND its assignment matches the command: cancelling and
            // restarting a healthy kernel for a command the Hub re-sent
            // after a restart would churn the data plane and (under load)
            // wedge the start path behind a teardown that never finishes. A
            // start whose per-node task set differs from the running
            // kernel's assignment (a drifted mapping) must REPLACE the
            // kernel instead of being swallowed as a healthy no-op. An
            // exited kernel at the same generation (a crash between the
            // poll drain and this reader) must also fall through to the
            // restart path instead of being reported as a healthy no-op.
            let incoming_task_ids = assignments
                .iter()
                .map(|assignment| assignment.task_id.clone())
                .collect::<BTreeSet<_>>();
            if let Some(task) = tasks.get(&job_id) {
                let alive = task.generation == generation && !task.handle.is_finished();
                let same_assignment = task
                    .assignments
                    .iter()
                    .map(|assignment| assignment.task_id.clone())
                    .collect::<BTreeSet<_>>()
                    == incoming_task_ids;
                if alive && same_assignment {
                    return Ok(());
                }
            }
            if local_recovery_marker
                .as_ref()
                .is_some_and(|marker| marker.is_file())
                && recovery_id.is_none()
            {
                return Err(format!(
                    "durable Job '{job_id}' requires recovery because state marker '{}' exists",
                    local_recovery_marker
                        .as_ref()
                        .expect("marker checked above")
                        .display()
                ));
            }
            drop(tasks);
            let mut tasks = self.tasks.lock().await;
            let mut existing = tasks.remove(&job_id);
            if let Some(existing) = existing.as_mut() {
                existing.cancellation.cancel();
            }
            // Observed on the removal lock, right before the cancel: a kernel
            // that exited on its own has a genuine crash outcome worth
            // surfacing below; an exit after the cancel is this start's own
            // teardown and is never reported as a crash.
            let previous_exited_on_its_own = existing
                .as_ref()
                .is_some_and(|task| task.handle.is_finished());
            if let Some(existing) = &existing {
                existing.cancellation.cancel();
            }
            (existing, previous_exited_on_its_own)
        };
        let mut replaced_crash: Option<(u64, String)> = None;
        if let Some(mut existing) = existing {
            let existing_generation = existing.generation;
            let dedicated = existing.dedicated_runtime.take();
            let outcome = await_previous_teardown(
                &job_id,
                &mut existing.handle,
                KERNEL_TEARDOWN_JOIN_TIMEOUT,
            )
            .await;
            let _ = existing.state.close();
            shutdown_dedicated_runtime(dedicated);
            if let Some(manager) = &self.data_plane {
                manager.remove_job_session(&job_id, existing_generation);
            }
            if previous_exited_on_its_own {
                if let Some(Err(error)) = outcome {
                    replaced_crash = Some((existing.generation, error));
                }
            }
        }
        if let Some((crashed_generation, error)) = replaced_crash {
            warn!(
                job_id = %job_id,
                generation = crashed_generation,
                %error,
                "replaced kernel had exited on its own; surfacing its crash as a job observation"
            );
            self.park_observations(vec![(job_id.clone(), crashed_generation, Err(error))])
                .await;
        }
        let task_ids = assignments
            .iter()
            .map(|assignment| assignment.task_id.clone())
            .collect::<Vec<_>>();
        let watermark_partitions = assignments
            .iter()
            .filter_map(|assignment| {
                plan.task(&assignment.task_id).and_then(|task| {
                    task.partitions
                        .first()
                        .map(|partition| (assignment.task_id.clone(), partition.id))
                })
            })
            .collect::<BTreeMap<_, _>>();
        let state_root_base = match plan.spec.state.as_ref() {
            Some(state) if state.durability == arkflow_core::job::StateDurability::Durable => {
                arkflow_core::job::configured_state_root(state)
            }
            Some(_) => std::env::temp_dir()
                .join("arkflow-ephemeral-job-state")
                .join(ephemeral_state_nonce()),
            None => std::env::temp_dir().join("arkflow-stateless-job-state"),
        };
        let state_root = state_root_base
            .join("jobs")
            .join(&job_id)
            .join(format!("node-{}", safe_path_component(node_id)));
        let durable_state =
            plan.spec.state.as_ref().is_some_and(|state| {
                state.durability == arkflow_core::job::StateDurability::Durable
            });
        let state_root = if durable_state {
            state_root
                .join(format!("version-{}", plan.spec.version.0))
                .join(format!("generation-{generation}"))
        } else {
            state_root.join(format!(
                "version-{}-generation-{}",
                plan.spec.version.0, generation
            ))
        };
        let recoverable_state =
            durable_state && plan.spec.requires_state() && plan.spec.checkpoint.is_some();
        let start_marker = recoverable_state.then(|| state_root.join(".arkflow-started"));
        let state_format_version = plan
            .spec
            .state
            .as_ref()
            .map(|state| state.format_version)
            .unwrap_or(1);
        let state_backend = RedbStateBackend::open(state_root, state_format_version)
            .map_err(|error| error.to_string())?;
        let state_backend = match plan.spec.state.as_ref().and_then(|state| state.max_bytes) {
            Some(max_bytes) => state_backend.with_max_bytes(max_bytes),
            None => state_backend,
        };
        let state: Arc<dyn StateBackend> = Arc::new(state_backend);
        let recovery = if let Some(checkpoint_id) = recovery_id {
            // Manifest/snapshot reads are object-store round trips (the
            // CheckpointStore trait is synchronous): run them on the blocking
            // pool so a slow S3 read cannot stall the async runtime's worker
            // threads and delay heartbeats and other commands.
            let plan_for_recovery = plan.clone();
            let assignments_for_recovery = assignments.clone();
            let state_for_restore = state.clone();
            let recovered = tokio::task::spawn_blocking(move || -> Result<RecoveryPlan, String> {
                let repository = checkpoint_repository(&plan_for_recovery)?;
                let artifact =
                    recovery_artifact(&plan_for_recovery, &checkpoint_id, recovery_savepoint)?;
                let manifest = repository
                    .read_manifest(&artifact)
                    .map_err(|error| error.to_string())?;
                let rescale = plan_for_recovery.spec.rescale;
                // Redistribution only runs when the task set actually
                // differs: an identical set restores verbatim exactly as
                // before, rescale declared or not.
                let planned_tasks = plan_for_recovery
                    .tasks
                    .iter()
                    .map(|task| task.id.clone())
                    .collect::<BTreeSet<_>>();
                let manifest_tasks = manifest
                    .task_attempts
                    .iter()
                    .map(|attempt| attempt.task_id.clone())
                    .collect::<BTreeSet<_>>();
                let redistribute = rescale && manifest_tasks != planned_tasks;
                validate_recovery_manifest(
                    &plan_for_recovery,
                    &checkpoint_id,
                    state_for_restore.format_version(),
                    &manifest,
                    redistribute,
                )?;
                validate_recovery_snapshots(
                    &plan_for_recovery,
                    &repository,
                    &manifest,
                    redistribute,
                )?;
                restore_recovery_state(
                    &plan_for_recovery,
                    &repository,
                    &manifest,
                    &assignments_for_recovery,
                    &state_for_restore,
                    redistribute,
                )?;
                RecoveryPlan::from_manifest(&manifest).map_err(|error| error.to_string())
            })
            .await
            .map_err(|error| format!("recovery read task failed: {error}"))??;
            Some(recovered)
        } else {
            None
        };
        if let Some(marker) = start_marker.as_deref() {
            if let Err(error) = persist_start_marker(marker) {
                let _ = state.close();
                return Err(format!(
                    "persist durable Job start marker '{}': {error}",
                    marker.display()
                ));
            }
        }
        let cancellation = CancellationToken::new();
        // Register the Job BEFORE spawning the kernel: an abort of this
        // command task during the spawn window (session teardown, another
        // command's failed result) must not orphan a running kernel that no
        // report, stop, or stop-all can reach. The registered cancellation
        // token lets stop paths cancel the in-flight start, and the
        // placeholder join handle gives stop something to await. The entry is
        // swapped to the real kernel handle once the spawn completes; on spawn
        // failure the entry is removed and the token cancelled.
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        let placeholder_cancellation = cancellation.clone();
        let placeholder_handle = tokio::spawn(async move {
            tokio::select! {
                _ = placeholder_cancellation.cancelled() => {}
                _ = started_rx => {}
            }
            Ok(())
        });
        self.tasks.lock().await.insert(
            job_id.clone(),
            JobTask {
                generation,
                ephemeral_state,
                recovery_required,
                cancellation: cancellation.clone(),
                assignments: assignments.clone(),
                dedicated_runtime: None,
                watermark_partitions: watermark_partitions.clone(),
                state: state.clone(),
                checkpoint_store_uri: plan
                    .spec
                    .checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.object_store_uri.clone()),
                kernel: None,
                handle: placeholder_handle,
            },
        );
        // Spawn the Job on the unified kernel: the same plan, adapter and
        // state backend drive pipelined chain execution, and the handle backs
        // command-driven checkpoints.
        // Remote-edge wiring: only when this node runs a data plane AND the
        // Hub command advertised peers' data ports. Otherwise (the default)
        // the co-location contract holds and the build keeps local edges.
        let remote_context = self.data_plane.as_ref().and_then(|manager| {
            if split.node_data_ports.is_empty() {
                return None;
            }
            let mut node_addrs = BTreeMap::new();
            for (node, addr) in &split.node_data_ports {
                match addr.parse::<std::net::SocketAddr>() {
                    Ok(parsed) => {
                        node_addrs.insert(node.clone(), parsed);
                    }
                    Err(error) => {
                        warn!(%node, %addr, %error, "ignoring malformed peer data-plane address");
                    }
                }
            }
            // The Hub's split dispatch carries the FULL task→node map; the
            // per-node assignment subset alone cannot name remote peers.
            let task_nodes = match split.task_nodes.clone() {
                Some(task_nodes) => task_nodes,
                None => {
                    warn!(
                        node_id = %node_id,
                        "split dispatch without a full task→node map;                          deriving remote peers from the local assignment only"
                    );
                    assignments
                        .iter()
                        .map(|assignment| (assignment.task_id.clone(), assignment.node_id.clone()))
                        .collect()
                }
            };
            Some(std::sync::Arc::new(
                arkflow_core::executor::graph::RemoteEdgeContext {
                    tls: manager.tls_config().cloned(),
                    local_node: node_id.to_string(),
                    task_nodes,
                    node_addrs,
                    manager: manager.clone(),
                    generation,
                },
            ))
        });
        // Declared CPU => dedicated bounded runtime: the kernel (and all its
        // async work) runs on max(1, ceil(millicores/1000)) worker threads
        // owned by this Job instead of the shared runtime's pool. Undeclared
        // Jobs keep the shared runtime, byte-identical to before.
        let mut dedicated_runtime: Option<Arc<tokio::runtime::Runtime>> =
            match plan.spec.resources.cpu_millicores {
                Some(millicores) => {
                    let workers = millicores.div_ceil(1000).max(1) as usize;
                    match tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(workers)
                        .thread_name(format!("arkflow-job-{job_id}"))
                        .enable_all()
                        .build()
                    {
                        Ok(runtime) => Some(Arc::new(runtime)),
                        Err(error) => {
                            return Err(format!(
                                "dedicated runtime for Job '{job_id}' failed to build: {error}"
                            ))
                        }
                    }
                }
                None => None,
            };
        let state_for_spawn = state.clone();
        let owned_plan = plan.clone();
        let owned_task_ids = task_ids.clone();
        let owned_recovery = recovery.clone();
        let owned_remote = remote_context.clone();
        let owned_cancellation = cancellation.clone();
        let spawn_future = async move {
            spawn_kernel_job(
                &owned_plan,
                &owned_task_ids,
                state_for_spawn.clone(),
                owned_recovery.as_ref(),
                owned_cancellation,
                owned_remote.as_deref(),
            )
            .await
        };
        let spawn_result = match &dedicated_runtime {
            Some(runtime) => match runtime.spawn(spawn_future).await {
                Ok(result) => result,
                Err(error) => Err(format!("dedicated kernel task failed: {error}")),
            },
            None => spawn_future.await,
        };
        let kernel = match spawn_result {
            Ok(handle) => {
                // Release the placeholder: the swap below resolves it.
                drop(started_tx);
                Arc::new(handle)
            }
            Err(error) => {
                if let Some(marker) = start_marker.as_deref() {
                    remove_start_marker(marker);
                }
                drop(started_tx);
                let placeholder = self.tasks.lock().await.remove(&job_id);
                if let Some(mut task) = placeholder {
                    let dedicated = task.dedicated_runtime.take();
                    task.cancellation.cancel();
                    let _ = (&mut task.handle).await;
                    shutdown_dedicated_runtime(dedicated);
                }
                if let Some(runtime) = dedicated_runtime.take() {
                    tokio::task::spawn_blocking(move || {
                        if let Ok(runtime) = Arc::try_unwrap(runtime) {
                            runtime.shutdown_timeout(Duration::from_secs(10));
                        }
                    });
                }
                if let Some(manager) = &self.data_plane {
                    manager.remove_job_session(&job_id, generation);
                }
                let _ = state.close();
                return Err(error);
            }
        };
        let handle = kernel.watcher();
        let mut registered = false;
        {
            let mut tasks = self.tasks.lock().await;
            // Swap in the real kernel only if our placeholder still owns the
            // entry: a stop during the spawn removed it (the token is
            // cancelled and the runner winds down on its own), and a stale
            // generation must not overwrite a newer registration.
            let still_ours = tasks
                .get(&job_id)
                .is_some_and(|task| task.generation == generation && task.kernel.is_none());
            if still_ours {
                registered = true;
                tasks.insert(
                    job_id.clone(),
                    JobTask {
                        generation,
                        ephemeral_state,
                        recovery_required,
                        cancellation: cancellation.clone(),
                        assignments,
                        dedicated_runtime: dedicated_runtime.clone(),
                        watermark_partitions,
                        state,
                        checkpoint_store_uri: plan
                            .spec
                            .checkpoint
                            .as_ref()
                            .map(|checkpoint| checkpoint.object_store_uri.clone()),
                        kernel: Some(kernel),
                        handle,
                    },
                );
            }
        }
        if !registered {
            if let Some(manager) = &self.data_plane {
                manager.remove_job_session(&job_id, generation);
            }
            if let Some(runtime) = dedicated_runtime.take() {
                tokio::task::spawn_blocking(move || {
                    if let Ok(runtime) = Arc::try_unwrap(runtime) {
                        runtime.shutdown_timeout(Duration::from_secs(10));
                    }
                });
            }
        }
        Ok(())
    }

    async fn checkpoint(
        &self,
        job_id: &str,
        checkpoint_id: &str,
        generation: u64,
        savepoint: bool,
        node_id: &str,
    ) -> Result<String, String> {
        // Copy what the checkpoint needs and release the tasks lock: the
        // barrier wait and the object-store writes below take seconds on slow
        // storage, and holding the lock across them would block stop/stop-all
        // and new starts for the whole duration.
        let (kernel, assignments, task_watermark_partitions, checkpoint_store_uri) = {
            let tasks = self.tasks.lock().await;
            let task = tasks
                .get(job_id)
                .ok_or_else(|| "Job is not running on this Agent".to_string())?;
            if task.generation != generation {
                return Err("checkpoint generation does not match running Job".into());
            }
            let kernel = task
                .kernel
                .clone()
                .ok_or_else(|| "Job kernel handle is missing".to_string())?;
            (
                kernel,
                task.assignments.clone(),
                task.watermark_partitions.clone(),
                task.checkpoint_store_uri.clone(),
            )
        };
        let (snapshot, source_positions, task_watermarks, watermark_partitions) = kernel
            .checkpoint_barrier_with_details(checkpoint_id, generation)
            .await
            .map_err(|error| error.to_string())?;
        let store_uri = checkpoint_store_uri
            .as_deref()
            .ok_or_else(|| "Job has no checkpoint object_store_uri".to_string())?;
        let store_uri_owned = store_uri.to_owned();
        let checkpoint_id_owned = checkpoint_id.to_owned();
        let snapshot_for_write = snapshot.clone();
        // Object-store round trips are blocking I/O (the CheckpointStore
        // trait is synchronous): run them on the blocking pool so a slow S3
        // write cannot stall the async runtime's worker threads.
        let state_ref = tokio::task::spawn_blocking(move || -> Result<_, String> {
            let repository =
                CheckpointRepository::new(SharedCheckpointStore::from_uri(&store_uri_owned)?);
            repository
                .write_state_snapshot(&checkpoint_id_owned, &snapshot_for_write)
                .map_err(|error| error.to_string())
        })
        .await
        .map_err(|error| format!("checkpoint state write task failed: {error}"))??;
        let state_refs = assignments
            .iter()
            .map(|assignment| StateSnapshotRef {
                task_id: assignment.task_id.clone(),
                node_id: Some(node_id.to_owned()),
                ..state_ref.clone()
            })
            .collect::<Vec<_>>();
        let mut coordinator = CheckpointCoordinator::new(
            assignments[0].job_id.clone(),
            assignments[0].job_version,
            generation,
            snapshot.format_version,
            assignments
                .iter()
                .map(|assignment| assignment.task_id.clone()),
        );
        let barrier = coordinator
            .start(checkpoint_id)
            .map_err(|error| error.to_string())?;
        for (index, assignment) in assignments.iter().enumerate() {
            coordinator
                .acknowledge(TaskCheckpointAck {
                    task_id: assignment.task_id.clone(),
                    attempt_id: assignment.id.clone(),
                    partition: task_watermark_partitions
                        .get(&assignment.task_id)
                        .copied()
                        .unwrap_or_default(),
                    checkpoint_id: barrier.checkpoint_id.clone(),
                    generation: barrier.generation,
                    state: snapshot.clone(),
                    source_positions: if index == 0 {
                        source_positions.clone()
                    } else {
                        Vec::new()
                    },
                    watermark_ms: task_watermarks.get(&assignment.task_id).copied(),
                    watermark_partitions: watermark_partitions
                        .get(&assignment.task_id)
                        .cloned()
                        .unwrap_or_default(),
                })
                .map_err(|error| error.to_string())?;
        }
        let attempts = assignments
            .iter()
            .map(|assignment| TaskAttemptSnapshot {
                task_id: assignment.task_id.clone(),
                attempt_id: assignment.id.clone(),
                node_id: assignment.node_id.clone(),
            })
            .collect();
        let manifest = coordinator
            .complete(attempts, state_refs)
            .map_err(|error| error.to_string())?;
        let kind = if savepoint {
            RecoveryArtifactKind::Savepoint
        } else {
            RecoveryArtifactKind::Checkpoint
        };
        let prefix = if savepoint {
            "savepoints"
        } else {
            "checkpoints"
        };
        let manifest_key = format!(
            "{prefix}/{checkpoint_id}/manifests/{}.json",
            node_id.replace('/', "_")
        );
        let manifest_key_owned = manifest_key.clone();
        let store_uri_for_manifest = store_uri.to_owned();
        let manifest_for_write = manifest.clone();
        let artifact = tokio::task::spawn_blocking(move || -> Result<_, String> {
            let repository = CheckpointRepository::new(SharedCheckpointStore::from_uri(
                &store_uri_for_manifest,
            )?);
            repository
                .write_manifest(&manifest_for_write, kind, manifest_key_owned)
                .map_err(|error| error.to_string())
        })
        .await
        .map_err(|error| format!("checkpoint manifest write task failed: {error}"))??;
        let uri = format!(
            "{}/{}",
            store_uri.trim_end_matches('/'),
            artifact.manifest_key
        );
        Ok(uri)
    }

    async fn aggregate_checkpoint(
        &self,
        job_id: &str,
        checkpoint_id: &str,
        generation: u64,
        savepoint: bool,
        manifest_nodes: &[String],
        planned_task_ids: &[String],
    ) -> Result<String, String> {
        // Copy what the aggregate needs and release the tasks lock: the
        // manifest reads and the object-store write below are slow I/O that
        // must not block stop/stop-all and new starts.
        let (store_uri_owned, job_version, format_version) = {
            let tasks = self.tasks.lock().await;
            let task = tasks
                .get(job_id)
                .ok_or_else(|| "Job is not running on this Agent".to_string())?;
            if task.generation != generation {
                return Err("checkpoint generation does not match running Job".into());
            }
            (
                task.checkpoint_store_uri.clone(),
                task.assignments[0].job_version,
                task.state.format_version(),
            )
        };
        let store_uri = store_uri_owned
            .as_deref()
            .ok_or_else(|| "Job has no checkpoint object_store_uri".to_string())?;
        let kind = if savepoint {
            RecoveryArtifactKind::Savepoint
        } else {
            RecoveryArtifactKind::Checkpoint
        };
        let prefix = if savepoint {
            "savepoints"
        } else {
            "checkpoints"
        };
        let mut aggregate: Option<arkflow_core::checkpoint::CheckpointManifest> = None;
        let mut task_ids = std::collections::BTreeSet::new();
        for node_id in manifest_nodes {
            let key = format!(
                "{prefix}/{checkpoint_id}/manifests/{}.json",
                node_id.replace('/', "_")
            );
            let artifact = RecoveryArtifact {
                id: checkpoint_id.to_owned(),
                kind,
                manifest_key: key,
                job_version,
                format_version,
                created_at_ms: 0,
                status: CheckpointStatus::Completed,
            };
            let store_uri_for_read = store_uri.to_owned();
            let artifact_for_read = artifact.clone();
            // Blocking object-store reads run on the blocking pool.
            let manifest = tokio::task::spawn_blocking(move || -> Result<_, String> {
                let repository = CheckpointRepository::new(SharedCheckpointStore::from_uri(
                    &store_uri_for_read,
                )?);
                repository
                    .read_manifest(&artifact_for_read)
                    .map_err(|error| error.to_string())
            })
            .await
            .map_err(|error| format!("checkpoint manifest read task failed: {error}"))??;
            if let Some(target) = aggregate.as_mut() {
                if target.job_id != manifest.job_id
                    || target.job_version != manifest.job_version
                    || target.generation != manifest.generation
                    || target.format_version != manifest.format_version
                {
                    return Err("checkpoint manifests do not share one job barrier".into());
                }
                for attempt in manifest.task_attempts {
                    if !task_ids.insert(attempt.task_id.clone()) {
                        return Err(format!(
                            "duplicate task '{}' in checkpoint manifests",
                            attempt.task_id
                        ));
                    }
                    target.task_attempts.push(attempt);
                }
                target.source_positions.extend(manifest.source_positions);
                target.watermarks_ms.extend(manifest.watermarks_ms);
                for (task_id, partitions) in manifest.watermark_partitions {
                    target
                        .watermark_partitions
                        .entry(task_id)
                        .or_default()
                        .extend(partitions);
                }
                target.state_snapshots.extend(manifest.state_snapshots);
            } else {
                for attempt in &manifest.task_attempts {
                    task_ids.insert(attempt.task_id.clone());
                }
                aggregate = Some(manifest);
            }
        }
        let mut manifest =
            aggregate.ok_or_else(|| "checkpoint has no agent manifests".to_string())?;
        manifest.checksum = 0;
        manifest.seal();
        let final_key = recovery_manifest_key(kind, checkpoint_id);
        let planned_tasks = planned_task_ids.iter().cloned().collect::<BTreeSet<_>>();
        if planned_tasks.is_empty() {
            return Err("checkpoint has no planned task assignments".into());
        }
        let artifact = {
            let store_uri_for_write = store_uri.to_owned();
            let manifest_for_write = manifest.clone();
            let planned_tasks_for_write = planned_tasks.clone();
            tokio::task::spawn_blocking(move || -> Result<_, String> {
                let repository = CheckpointRepository::new(SharedCheckpointStore::from_uri(
                    &store_uri_for_write,
                )?);
                repository
                    .write_manifest_with_plan(
                        &manifest_for_write,
                        kind,
                        final_key,
                        &planned_tasks_for_write,
                    )
                    .map_err(|error| error.to_string())
            })
            .await
            .map_err(|error| format!("checkpoint aggregate write task failed: {error}"))??
        };
        Ok(format!(
            "{}/{}",
            store_uri.trim_end_matches('/'),
            artifact.manifest_key
        ))
    }

    async fn take_finished(&self) -> Vec<FinishedJob> {
        let mut finished = std::mem::take(&mut *self.pending_observations.lock().await);
        let mut tasks = self.tasks.lock().await;
        let ids = tasks
            .iter()
            .filter(|(_, task)| task.handle.is_finished())
            .map(|(job_id, _)| job_id.clone())
            .collect::<Vec<_>>();
        for job_id in ids {
            if let Some(mut task) = tasks.remove(&job_id) {
                let dedicated = task.dedicated_runtime.take();
                let result = match (&mut task.handle).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => Err(error.to_string()),
                    Err(error) => Err(error.to_string()),
                };
                let _ = task.state.close();
                shutdown_dedicated_runtime(dedicated);
                if let Some(manager) = &self.data_plane {
                    manager.remove_job_session(&job_id, task.generation);
                }
                finished.push((job_id, task.generation, result));
            }
        }
        finished
    }

    /// Park undelivered finished-task observations so the next session
    /// retries them. `take_finished` drains the parked queue first.
    async fn park_observations(&self, observations: Vec<FinishedJob>) {
        if observations.is_empty() {
            return;
        }
        self.pending_observations.lock().await.extend(observations);
    }

    async fn stop_all(&self) {
        let _start_guard = self.starts.lock().await;
        let tasks = {
            let mut tasks = self.tasks.lock().await;
            std::mem::take(&mut *tasks).into_iter().collect::<Vec<_>>()
        };
        for (job_id, mut task) in tasks {
            let dedicated = task.dedicated_runtime.take();
            task.cancellation.cancel();
            let _ = (&mut task.handle).await;
            let _ = task.state.close();
            shutdown_dedicated_runtime(dedicated);
            if let Some(manager) = &self.data_plane {
                manager.remove_job_session(&job_id, task.generation);
            }
        }
    }

    async fn stop(&self, job_id: &str, generation: u64) -> Result<(), String> {
        let _start_guard = self.starts.lock().await;
        let task = {
            let mut tasks = self.tasks.lock().await;
            if let Some(existing) = tasks.get(job_id) {
                if existing.generation > generation {
                    return Err("job generation is stale".into());
                }
            }
            tasks.remove(job_id)
        };
        if let Some(mut task) = task {
            let task_generation = task.generation;
            let dedicated = task.dedicated_runtime.take();
            task.cancellation.cancel();
            let _ = (&mut task.handle).await;
            let _ = task.state.close();
            shutdown_dedicated_runtime(dedicated);
            if let Some(manager) = &self.data_plane {
                manager.remove_job_session(job_id, task_generation);
            }
        }
        Ok(())
    }
}

fn safe_path_component(value: &str) -> String {
    // Keep the mapping injective: node IDs are part of the durable state and
    // marker path. Replacing arbitrary characters with '_' (and truncating)
    // aliases distinct nodes such as `a/b` and `a_b`, allowing them to open
    // the same redb database after a restart.
    let mut component = String::with_capacity(value.len());
    for byte in value.bytes() {
        if matches!(byte, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.') {
            component.push(byte as char);
        } else {
            use std::fmt::Write as _;
            let _ = write!(component, "%{byte:02X}");
        }
    }
    if component.is_empty() {
        "unknown".into()
    } else {
        component
    }
}

fn ephemeral_state_nonce() -> String {
    static ATTEMPT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let attempt = ATTEMPT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{}-{timestamp}-{attempt}", std::process::id())
}

fn durable_recovery_marker(
    plan: &JobPlan,
    node_id: &str,
    generation: u64,
) -> Option<std::path::PathBuf> {
    let state = plan.spec.state.as_ref()?;
    if state.durability != arkflow_core::job::StateDurability::Durable
        || !plan.spec.requires_state()
        || plan.spec.checkpoint.is_none()
    {
        return None;
    }
    Some(
        arkflow_core::job::configured_state_root(state)
            .join("jobs")
            .join(plan.spec.id.as_str())
            .join(format!("node-{}", safe_path_component(node_id)))
            .join(format!("version-{}", plan.spec.version.0))
            .join(format!("generation-{generation}"))
            .join(".arkflow-started"),
    )
}

fn persist_start_marker(marker: &std::path::Path) -> std::io::Result<()> {
    let temporary = marker.with_extension(format!("tmp-{}", std::process::id()));
    if let Err(error) = std::fs::write(&temporary, b"started\n") {
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    if let Err(error) = std::fs::rename(&temporary, marker) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    Ok(())
}

fn remove_start_marker(marker: &std::path::Path) {
    let _ = std::fs::remove_file(marker);
    let temporary = marker.with_extension(format!("tmp-{}", std::process::id()));
    let _ = std::fs::remove_file(temporary);
}

/// Spawn the assigned Job tasks on the unified kernel and return the
/// command-driven snapshot handle. Mirrors the legacy start path's component
/// assembly (same adapter, same state backend) but executes through the
/// kernel's pipelined chains.
async fn spawn_kernel_job(
    plan: &JobPlan,
    task_ids: &[String],
    state: Arc<dyn StateBackend>,
    recovery: Option<&RecoveryPlan>,
    cancellation: CancellationToken,
    remote: Option<&arkflow_core::executor::graph::RemoteEdgeContext>,
) -> Result<arkflow_core::executor::kernel_handle::KernelJobHandle, String> {
    let resource = Resource {
        temporary: HashMap::<String, Arc<dyn Temporary>>::new(),
        input_names: RefCell::new(Vec::new()),
    };
    let mut graph = arkflow_core::executor::graph::ExecutionGraphBuilder::default()
        .with_state(state.clone())
        .build_subgraph(plan, task_ids, &RegistryJobAdapter, &resource, remote)
        .map_err(|error| error.to_string())?;
    // The graph builder constructs temporary resources through `Resource`;
    // transfer those instances into the unified graph so the resource guard
    // connects them before any processor can issue its first lookup.
    graph.temporaries = resource.temporary.values().cloned().collect();
    let inputs = graph
        .chains
        .iter()
        .filter_map(|chain| chain.source.clone())
        .collect::<Vec<_>>();

    // Event-time gates per event-time source chain.  Use the graph's compiled
    // window timing metadata so sliding/session windows keep their exact
    // trigger geometry; the older source-operator scan only knew a list of
    // sizes and could release a row too early.
    let mut watermark_gates = BTreeMap::new();
    let mut shared_trackers =
        BTreeMap::<String, Arc<std::sync::Mutex<arkflow_core::event_time::WatermarkTracker>>>::new(
        );
    // Physical source partition per gated chain: a restored watermark is
    // installed for the task's REAL partition, never a synthesized
    // partition 0.
    let mut gate_partitions: BTreeMap<String, u32> = BTreeMap::new();
    for chain in &graph.chains {
        if let Some(source_time) = chain
            .source_time
            .as_ref()
            .filter(|time| time.mode == arkflow_core::job::TimeMode::EventTime)
        {
            let group = chain
                .watermark_group
                .clone()
                .unwrap_or_else(|| chain.entry_task_id().to_owned());
            let tracker = match shared_trackers.entry(group) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.get().clone(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let tracker =
                        arkflow_core::event_time::WatermarkTracker::from_time_spec(source_time)
                            .map_err(|error| error.to_string())?;
                    let tracker = Arc::new(std::sync::Mutex::new(tracker));
                    entry.insert(tracker.clone());
                    tracker
                }
            };
            let gate =
                arkflow_core::executor::event_time_gate::EventTimeGate::new_with_shared_tracker(
                    source_time,
                    chain.window_timings.clone(),
                    tracker,
                )
                .map_err(|error| error.to_string())?;
            if let Some(partition) = chain.source_partition {
                gate_partitions.insert(chain.entry_task_id().to_owned(), partition);
            }
            watermark_gates.insert(
                chain.entry_task_id().to_owned(),
                Arc::new(tokio::sync::Mutex::new(Some(gate))),
            );
        }
    }

    // Recovery must be applied before the kernel connects and reads any
    // source. The previous order spawned the graph first, allowing a source
    // to consume from its pre-recovery cursor before positions/watermarks
    // were installed.
    if let Some(recovery) = recovery {
        for input in &inputs {
            if let Err(error) = input.connect().await {
                close_inputs(&inputs).await;
                return Err(error.to_string());
            }
        }
        // Seed the complete post-connect assignment before restoring the
        // checkpointed watermark.  An idle physical partition must remain an
        // active MIN frontier until it emits or reaches the configured idle
        // timeout; otherwise the first fast partition can close windows early.
        for chain in &graph.chains {
            let Some(gate) = watermark_gates.get(chain.entry_task_id()) else {
                continue;
            };
            let Some(source) = chain.source.as_ref() else {
                continue;
            };
            let source_id = chain.entry_task_id();
            let partitions = match source.watermark_partitions().await {
                Ok(partitions) => partitions,
                Err(error) => {
                    close_inputs(&inputs).await;
                    return Err(error.to_string());
                }
            };
            let mut partitions = partitions
                .into_iter()
                .map(|partition| partition.with_source_identity(source_id))
                .collect::<Vec<_>>();
            if partitions.is_empty() {
                if let Some(partition) = chain.source_partition {
                    partitions.push(arkflow_core::event_time::EventTimePartition::for_source(
                        source_id, partition,
                    ));
                }
            }
            if !partitions.is_empty() {
                if let Some(gate) = gate.lock().await.as_mut() {
                    gate.seed_partitions(&partitions)
                }
            }
        }
        for input in &inputs {
            if let Err(error) = input.restore_positions(&recovery.source_positions).await {
                close_inputs(&inputs).await;
                return Err(error.to_string());
            }
        }
        for (task_id, partitions) in &recovery.watermark_partitions {
            if let Some(gate) = watermark_gates.get(task_id) {
                let mut gate = gate.lock().await;
                if let Some(gate) = gate.as_mut() {
                    for partition in partitions {
                        gate.restore_partition_key(
                            &arkflow_core::event_time::EventTimePartition::new(
                                partition.topic.clone(),
                                partition.partition,
                            )
                            .with_source_identity(task_id),
                            partition.watermark_ms,
                        );
                    }
                }
            }
        }
        for (task_id, watermark) in &recovery.watermarks_ms {
            if recovery
                .watermark_partitions
                .get(task_id)
                .is_some_and(|partitions| !partitions.is_empty())
            {
                continue;
            }
            if let Some(gate) = watermark_gates.get(task_id) {
                let mut gate_guard = gate.lock().await;
                if let Some(gate) = gate_guard.as_mut() {
                    let known = gate.known_partitions();
                    if known.is_empty() {
                        let partition = gate_partitions.get(task_id).copied().unwrap_or_default();
                        let partition = arkflow_core::event_time::EventTimePartition::for_source(
                            task_id, partition,
                        );
                        gate.restore_partition_key(&partition, *watermark);
                    } else {
                        for partition in known {
                            gate.restore_partition_key(&partition, *watermark);
                        }
                    }
                }
            }
        }
    }

    let mut states = BTreeMap::new();
    for task_id in task_ids {
        let is_stateful = plan
            .task(task_id)
            .and_then(|task| {
                plan.spec
                    .operators
                    .iter()
                    .find(|operator| operator.id == task.operator_id)
            })
            .is_some_and(|operator| {
                operator.stateful || operator.kind == arkflow_core::job::OperatorKind::Window
            });
        if is_stateful {
            states.insert(task_id.clone(), state.clone());
        }
    }
    if states.is_empty() {
        states.insert(task_ids.first().cloned().unwrap_or_default(), state.clone());
    }
    let handle = if recovery.is_some() {
        arkflow_core::executor::kernel_handle::KernelJobRunner::spawn_prepared_with_cancellation_and_state_format(
            graph,
            inputs.clone(),
            states,
            watermark_gates.clone(),
            plan.spec
                .state
                .as_ref()
                .map(|state| state.format_version)
                .unwrap_or(1),
            cancellation,
        )
        .await
    } else {
        arkflow_core::executor::kernel_handle::KernelJobRunner::spawn_with_cancellation_and_state_format(
            graph,
            inputs.clone(),
            states,
            watermark_gates.clone(),
            false,
            plan.spec
                .state
                .as_ref()
                .map(|state| state.format_version)
                .unwrap_or(1),
            cancellation,
        )
        .await
    };
    let handle = match handle {
        Ok(handle) => handle,
        Err(error) => {
            // Prepared inputs are not yet owned by chain tasks when graph
            // startup fails; release them here so a retry can reconnect.
            if recovery.is_some() {
                close_inputs(&inputs).await;
            }
            return Err(error.to_string());
        }
    };
    Ok(handle)
}

async fn close_inputs(inputs: &[Arc<dyn arkflow_core::input::Input>]) {
    for input in inputs.iter().rev() {
        if let Err(error) = input.close().await {
            tracing::warn!(%error, "failed to close input after Job startup failure");
        }
    }
}

struct RegistryJobAdapter;

impl JobComponentAdapter for RegistryJobAdapter {
    fn build_input(
        &self,
        source: &SourceSpec,
        resource: &Resource,
    ) -> Result<Arc<dyn arkflow_core::input::Input>, arkflow_core::Error> {
        InputConfig {
            input_type: source.input_type.clone(),
            name: None,
            codec: None,
            config: Some(source.config.clone()),
        }
        .build(resource)
    }

    fn build_output(
        &self,
        sink: &SinkSpec,
        resource: &Resource,
    ) -> Result<Arc<dyn arkflow_core::output::Output>, arkflow_core::Error> {
        OutputConfig {
            output_type: sink.output_type.clone(),
            name: None,
            codec: None,
            config: Some(sink.config.clone()),
        }
        .build(resource)
    }

    fn build_processor(
        &self,
        operator: &OperatorSpec,
        resource: &Resource,
    ) -> Result<Arc<dyn arkflow_core::processor::Processor>, arkflow_core::Error> {
        let processor_type = operator
            .config
            .get("type")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| {
                arkflow_core::Error::Config(format!(
                    "operator '{}' requires config.type",
                    operator.id
                ))
            })?;
        ProcessorConfig {
            processor_type,
            name: None,
            config: Some(operator.config.clone()),
        }
        .build(resource)
    }
}

/// Trim trailing `/` and drop duplicates, preserving first-seen order.
fn normalize_hub_urls(urls: &[String]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    urls.iter()
        .map(|url| url.trim_end_matches('/'))
        .filter(|url| !url.is_empty())
        .filter(|url| seen.insert((*url).to_owned()))
        .map(str::to_owned)
        .collect()
}

impl NodeAgentConfig {
    pub fn from_engine(config: &arkflow_core::config::EngineConfig) -> Option<Self> {
        let hub_urls = normalize_hub_urls(&config.health_check.hub_urls);
        let hub_url = hub_urls.first()?.clone();
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
        let boot_nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        Some(Self {
            hub_url,
            hub_urls,
            api_prefix: config.health_check.api_prefix.trim_end_matches('/').into(),
            node_id,
            node_token,
            // PID alone can be reused after a real process restart. Include
            // a startup nonce so the Hub invalidates successful starts from
            // the previous local JobRuntime even when the OS reuses the PID.
            boot_id: format!("boot-{}-{boot_nonce}", std::process::id()),
            heartbeat_interval: Duration::from_millis(ttl / 3),
            report_interval: Duration::from_secs(2),
            poll_interval: Duration::from_secs(1),
            data_port: config.health_check.data_port,
            data_host: config.health_check.data_host.clone(),
        })
    }
}

/// Node capabilities advertised at registration and refreshed by heartbeats.
fn agent_capabilities(network_shuffle: bool) -> Vec<String> {
    let mut capabilities = vec![
        "stream_lifecycle".to_string(),
        "configuration".to_string(),
        "metrics".to_string(),
        "job_runtime".to_string(),
        "state_backend".to_string(),
        "checkpoint_recovery".to_string(),
    ];
    if network_shuffle {
        capabilities.push("network_shuffle".to_string());
    }
    capabilities
}

/// A snapshot older than this multiple of the sampling interval is stale and
/// omitted from reports: a dead sampler must not produce a lying dashboard.
const RESOURCE_FRESHNESS_FACTOR: u64 = 2;
/// Lower bound on the derived sampling interval: below this, sysinfo
/// refreshes cost more than fresher gauges are worth.
const MIN_RESOURCE_SAMPLE_INTERVAL: Duration = Duration::from_millis(250);

/// One host resource sample. CPU is only meaningful from the second refresh
/// onward (sysinfo needs a prior window to average over); memory is valid
/// immediately.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ResourceSnapshot {
    pub sampled_at_ms: u64,
    pub cpu_usage_percent: Option<f64>,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub memory_available_bytes: u64,
    /// Logical CPU cores — static capacity for placement feasibility.
    pub cpu_cores: u32,
}

/// Shared latest-snapshot slot between the sampler task and the report path.
#[derive(Clone)]
pub(crate) struct ResourceSampler {
    sample_interval: Duration,
    latest: Arc<std::sync::RwLock<Option<ResourceSnapshot>>>,
}

impl ResourceSampler {
    /// Sample at half the report cadence (bounded below) so every report
    /// reads a snapshot the previous report cannot have seen: a Hub-side
    /// sustained-pressure streak then counts independent observations
    /// instead of one sample echoed across consecutive reports.
    fn new(report_interval: Duration) -> Self {
        Self {
            sample_interval: (report_interval / 2).max(MIN_RESOURCE_SAMPLE_INTERVAL),
            latest: Arc::default(),
        }
    }

    fn publish(&self, snapshot: ResourceSnapshot) {
        *self
            .latest
            .write()
            .expect("resource sampler slot lock poisoned") = Some(snapshot);
    }

    fn fresh_window_ms(&self) -> u64 {
        self.sample_interval.as_millis() as u64 * RESOURCE_FRESHNESS_FACTOR
    }

    /// The latest snapshot when it is still fresh for `now_ms`, else `None`.
    pub(crate) fn fresh(&self, now_ms: u64) -> Option<ResourceSnapshot> {
        let snapshot = *self
            .latest
            .read()
            .expect("resource sampler slot lock poisoned")
            .as_ref()?;
        (now_ms.saturating_sub(snapshot.sampled_at_ms) <= self.fresh_window_ms())
            .then_some(snapshot)
    }
}

/// Merge a fresh snapshot into the report's metrics map under the fixed
/// `node_*` vocabulary; the CPU gauge is skipped until it has a real window.
/// Build the data-plane mTLS material from `ARKFLOW_DATA_PLANE_TLS_CERT`,
/// `_KEY`, and `_CA` (PEM file paths). All three or none: a partial set is
/// an explicit configuration error (a half-loaded TLS config must fail, not
/// silently degrade to plaintext). Files are read once at startup.
fn data_plane_tls_from_env(
) -> Result<Option<arkflow_core::executor::remote::DataPlaneTlsConfig>, String> {
    let cert = std::env::var("ARKFLOW_DATA_PLANE_TLS_CERT").ok();
    let key = std::env::var("ARKFLOW_DATA_PLANE_TLS_KEY").ok();
    let ca = std::env::var("ARKFLOW_DATA_PLANE_TLS_CA").ok();
    let declared = [cert.is_some(), key.is_some(), ca.is_some()];
    if declared == [false, false, false] {
        return Ok(None);
    }
    if declared != [true, true, true] {
        // Fail closed per the authenticated-network-shuffle contract: a
        // half-loaded TLS config must fail startup, never degrade to
        // plaintext.
        return Err(
            "ARKFLOW_DATA_PLANE_TLS_CERT/_KEY/_CA must be set together (partial TLS configuration)"
                .into(),
        );
    }
    let read = |value: Option<String>, name: &str| -> Result<String, String> {
        value
            .map(|path| {
                std::fs::read_to_string(&path).map_err(|error| {
                    format!("data-plane TLS {name} '{path}' could not be read: {error}")
                })
            })
            .transpose()
            .map(|value| value.expect("checked Some above"))
    };
    let cert = read(cert, "certificate")?;
    let key = read(key, "private key")?;
    let ca = read(ca, "fleet CA")?;
    match arkflow_core::executor::remote::DataPlaneTlsConfig::from_pem(&cert, &key, &ca) {
        Ok(tls) => {
            info!("data-plane mTLS enabled (fleet CA anchored)");
            Ok(Some(tls))
        }
        Err(error) => Err(format!("data-plane TLS material rejected: {error}")),
    }
}

fn merge_resource_gauges(metrics: &mut BTreeMap<String, f64>, snapshot: ResourceSnapshot) {
    if let Some(cpu) = snapshot.cpu_usage_percent {
        metrics.insert("node_cpu_usage_percent".into(), cpu);
    }
    metrics.insert(
        "node_memory_used_bytes".into(),
        snapshot.memory_used_bytes as f64,
    );
    metrics.insert(
        "node_memory_total_bytes".into(),
        snapshot.memory_total_bytes as f64,
    );
    metrics.insert(
        "node_memory_available_bytes".into(),
        snapshot.memory_available_bytes as f64,
    );
    if snapshot.cpu_cores > 0 {
        metrics.insert("node_cpu_cores".into(), f64::from(snapshot.cpu_cores));
    }
}

/// Spawn the host resource sampler: a fixed-interval task publishing into the
/// shared slot. Best-effort by construction — every failure mode (unsupported
/// platform, poisoned state, task death) leaves reports running without
/// resource gauges and never touches the session loop.
pub(crate) fn spawn_resource_sampler(
    report_interval: Duration,
    cancellation: CancellationToken,
) -> ResourceSampler {
    let sampler = ResourceSampler::new(report_interval);
    let sample_interval = sampler.sample_interval;
    let task_sampler = sampler.clone();
    tokio::spawn(async move {
        let mut system = sysinfo::System::new();
        system.refresh_memory();
        // Baseline CPU refresh: publishes below only start once a refresh has
        // a prior window to average over, so no bogus 0% is ever reported.
        system.refresh_cpu_usage();
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return,
                _ = tokio::time::sleep(sample_interval) => {}
            }
            system.refresh_memory();
            system.refresh_cpu_usage();
            task_sampler.publish(ResourceSnapshot {
                sampled_at_ms: now_ms(),
                cpu_usage_percent: Some(f64::from(system.global_cpu_usage())),
                memory_used_bytes: system.used_memory(),
                memory_total_bytes: system.total_memory(),
                memory_available_bytes: system.available_memory(),
                cpu_cores: system.cpus().len() as u32,
            });
        }
    });
    sampler
}

pub async fn run(
    cp: ControlPlane,
    config: NodeAgentConfig,
    cancellation: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut backoff = Duration::from_millis(250);
    // Consecutive registration failures since the last success; a full cycle
    // across every candidate is what escalates to the exponential backoff.
    let mut failed_attempts: usize = 0;
    let mut completed_commands = CompletedCommandCache::new(1024);
    let mut job_runtime = JobRuntime::default();
    // Host resource gauges: sampled on an interval derived from the report
    // cadence for the whole process lifetime, so re-registration churn
    // never resets the view.
    let resource_sampler = spawn_resource_sampler(config.report_interval, cancellation.clone());
    // Cross-node shuffle data plane: one listener per Agent process. A bind
    // failure degrades to the co-location contract (warn, no listener) rather
    // than blocking node startup — observability and placement still work.
    let mut data_address: Option<String> = None;
    if let Some(port) = config.data_port {
        let data_secret = std::env::var("ARKFLOW_DATA_PLANE_SECRET")
            .ok()
            .filter(|secret| !secret.is_empty())
            .or_else(|| (!config.node_token.is_empty()).then(|| config.node_token.clone()));
        let data_plane_tls = data_plane_tls_from_env()?;
        let manager = data_secret
            .and_then(|data_secret| {
                match arkflow_core::executor::remote::DataPlaneCredentials::new(
                    config.node_id.clone(),
                    data_secret,
                ) {
                    Ok(credentials) => Some(credentials),
                    Err(error) => {
                        warn!(node_id = %config.node_id, %error, "invalid data-plane credentials; running colocated-only");
                        None
                    }
                }
            })
            .and_then(|credentials| {
                let manager_config = arkflow_core::executor::remote::NetworkManagerConfig {
                    credentials: Some(credentials),
                    channel_capacity: 1024,
                    tls: data_plane_tls.clone(),
                    .. arkflow_core::executor::remote::NetworkManagerConfig::default()
                };
                match arkflow_core::executor::remote::NetworkManager::with_config(
                    manager_config,
                ) {
                    Ok(manager) => Some(manager),
                    Err(error) => {
                        warn!(node_id = %config.node_id, %error, "invalid data-plane resource configuration; running colocated-only");
                        None
                    }
                }
            });
        if let Some(manager) = manager {
            manager.spawn();
            match config
                .data_host
                .as_deref()
                .unwrap_or("127.0.0.1")
                .parse::<std::net::IpAddr>()
            {
                Ok(bind_host) => match manager
                    .bind_tcp(std::net::SocketAddr::from((bind_host, port)))
                    .await
                {
                    Ok(bound) => {
                        data_address = config
                            .data_host
                            .as_ref()
                            .map(|host| format!("{host}:{bound}"));
                        match &data_address {
                            Some(address) => info!(
                                node_id = %config.node_id,
                                %address,
                                "Network shuffle data plane listening"
                            ),
                            None => warn!(
                                node_id = %config.node_id,
                                port = bound,
                                "data plane bound without data_host; the node stays colocated-only"
                            ),
                        }
                        job_runtime.data_plane = Some(manager);
                    }
                    Err(error) => {
                        manager.shutdown();
                        warn!(node_id = %config.node_id, %error, "data plane bind failed; running without network shuffle");
                    }
                },
                Err(error) => {
                    manager.shutdown();
                    warn!(node_id = %config.node_id, %error, "data_host must be a bindable IP address; running colocated-only");
                }
            }
        }
    }
    let network_shuffle = job_runtime.data_plane.is_some();
    // hub-ha stage 3 failover: the queue's front is the next candidate. A
    // successful registration pins its address at the front; a standby 503
    // rotates immediately; a standby's `leader_url` hint jumps the queue.
    let mut candidates: std::collections::VecDeque<String> = if config.hub_urls.is_empty() {
        std::iter::once(config.hub_url.clone()).collect()
    } else {
        config.hub_urls.iter().cloned().collect()
    };
    let failover_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut active_config = config.clone();
    let mut client = build_agent_client(&active_config.hub_url)?;
    // Reason token for the next switch's audit log, carried over from the
    // failure that triggered the rotation.
    let mut pending_reason: &'static str = "transport_error";
    loop {
        if cancellation.is_cancelled() {
            job_runtime.stop_all().await;
            if let Some(manager) = &job_runtime.data_plane {
                manager.shutdown();
            }
            return Ok(());
        }
        let Some(next) = candidates.front().cloned() else {
            return Err("no hub candidate addresses configured".into());
        };
        if next != active_config.hub_url {
            info!(
                node_id = %config.node_id,
                from = %active_config.hub_url,
                to = %next,
                reason = pending_reason,
                "Switching control-plane Hub candidate"
            );
            client = build_agent_client(&next)?;
            active_config.hub_url = next;
            failover_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        match register(&client, &active_config, data_address.clone()).await {
            Ok(session) => {
                info!(node_id = %config.node_id, hub = %active_config.hub_url, "Compute node registered with control-plane Hub");
                backoff = Duration::from_millis(250);
                failed_attempts = 0;
                if let Err(error) = run_session(
                    &client,
                    &cp,
                    &active_config,
                    session,
                    cancellation.clone(),
                    &mut completed_commands,
                    job_runtime.clone(),
                    network_shuffle,
                    &resource_sampler,
                    &failover_counter,
                )
                .await
                {
                    warn!(node_id = %config.node_id, error = %error, "Hub Agent session ended; reconnecting");
                }
                // The winner stays at the front: reconnects prefer the Hub
                // that last accepted a registration. Demotion or death of
                // that Hub surfaces as a Standby/Transport failure below and
                // rotates from there.
            }
            Err(RegisterFailure::Standby { leader_url }) => {
                warn!(
                    node_id = %config.node_id,
                    hub = %active_config.hub_url,
                    "Hub is a standby; advancing to the next candidate"
                );
                match leader_url {
                    // A trusted standby hint points straight at the elected
                    // leader — one probe instead of a full scan.
                    Some(leader) => {
                        pending_reason = "leader_hint";
                        jump_to_candidate(&mut candidates, leader);
                    }
                    None => {
                        pending_reason = "standby_advance";
                        rotate_candidates(&mut candidates);
                    }
                }
                failed_attempts += 1;
            }
            Err(RegisterFailure::Transport(message)) => {
                warn!(
                    node_id = %config.node_id,
                    hub = %active_config.hub_url,
                    error = %message,
                    "Hub Agent registration failed"
                );
                pending_reason = "transport_error";
                rotate_candidates(&mut candidates);
                failed_attempts += 1;
            }
        }
        // A full failed cycle across every candidate triggers the jittered
        // exponential backoff; within a cycle candidates rotate after only a
        // short fixed pause (a standby 503 must not burn the backoff).
        let cycle_len = candidates.len().max(1);
        let sleep_duration = if failed_attempts > 0 && failed_attempts.is_multiple_of(cycle_len) {
            let current = backoff;
            backoff = (backoff * 2).min(Duration::from_secs(10));
            jittered_backoff(current)
        } else {
            Duration::from_millis(200)
        };
        tokio::select! {
            _ = cancellation.cancelled() => {
                job_runtime.stop_all().await;
                if let Some(manager) = &job_runtime.data_plane {
                    manager.shutdown();
                }
                return Ok(())
            },
            _ = tokio::time::sleep(sleep_duration) => {}
        }
    }
}

/// Rotate the failover queue: the failed front candidate moves to the back.
fn rotate_candidates(candidates: &mut std::collections::VecDeque<String>) {
    if let Some(front) = candidates.pop_front() {
        candidates.push_back(front);
    }
}

/// Move `target` to the front of the failover queue (deduplicated), used for
/// standby `leader_url` hints.
fn jump_to_candidate(candidates: &mut std::collections::VecDeque<String>, target: String) {
    candidates.retain(|candidate| candidate != &target);
    candidates.push_front(target);
}

/// Whether an HTTP host is this machine: loopback addresses (the whole
/// 127/8, `::1`, bracketed IPv6 forms) plus `0.0.0.0`/`::` (unspecified
/// addresses that connect to the local host) and the `localhost` literal.
fn is_loopback_host(host: &str) -> bool {
    if host == "localhost" {
        return true;
    }
    let candidate = host.trim_start_matches('[').trim_end_matches(']');
    candidate
        .parse::<std::net::IpAddr>()
        .map(|ip| ip.is_loopback() || ip.is_unspecified())
        .unwrap_or(false)
}

/// Build the Agent's HTTP client. Loopback hubs are never proxied: system
/// proxy settings (macOS/Windows proxy configuration or stray env vars) that
/// intercept 127.0.0.1 traffic silently break registration and command
/// polling, and proxying a same-host control connection is always a
/// misconfiguration. Every request carries a hard timeout: a Hub that dies
/// mid-request (or a connection accepted into a dead listener's backlog that
/// never responds) must fail the session after a bound so the reconnect loop
/// — not a hung socket — owns recovery.
fn build_agent_client(hub_url: &str) -> Result<Client, reqwest::Error> {
    let builder = Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10));
    let loopback = url::Url::parse(hub_url)
        .ok()
        .and_then(|url| url.host_str().map(is_loopback_host))
        .unwrap_or(false);
    if loopback {
        return builder.no_proxy().build();
    }
    builder.build()
}

/// How long a redundant start waits for the previous kernel's WAL-safe
/// teardown before giving up on joining it. Well beyond any healthy teardown
/// (millisecond-scale in practice), well short of "forever".
const KERNEL_TEARDOWN_JOIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Await a superseded kernel's teardown, bounded: a wedged teardown must not
/// hold the start path (and its mutex) forever — after the bound the old task
/// finishes detached and the new start proceeds (its own state-dir open may
/// then fail terminally while the old kernel still holds the lock, which the
/// Hub's retry machinery absorbs as a visible, bounded failure loop).
/// Returns the joined outcome in the `FinishedJob` payload shape, or `None`
/// when the bound expired with the task detached (no outcome exists).
async fn await_previous_teardown(
    job_id: &str,
    handle: &mut tokio::task::JoinHandle<Result<(), arkflow_core::Error>>,
    bound: Duration,
) -> Option<Result<(), String>> {
    match tokio::time::timeout(bound, &mut *handle).await {
        Err(_) => {
            warn!(
                job_id = %job_id,
                bound_ms = bound.as_millis() as u64,
                "previous kernel teardown did not finish within the bounded wait; continuing with the new start"
            );
            None
        }
        Ok(Ok(Ok(()))) => Some(Ok(())),
        Ok(Ok(Err(error))) => Some(Err(error.to_string())),
        Ok(Err(error)) => Some(Err(error.to_string())),
    }
}

/// Classified registration failure (hub-ha stage 3): the failover loop
/// rotates candidates immediately on `Standby` but only escalates to the
/// exponential backoff after a full failed cycle.
enum RegisterFailure {
    /// 503 `hub_standby`: the Hub is reachable but not the leader. Carries
    /// the standby's `leader_url` hint when the shared lease row advertises
    /// the elected leader's address.
    Standby { leader_url: Option<String> },
    /// Connection-level failure or a non-standby HTTP error.
    Transport(String),
}

impl std::fmt::Display for RegisterFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Standby { leader_url } => match leader_url {
                Some(leader) => write!(formatter, "hub is a standby (leader hint: {leader})"),
                None => write!(formatter, "hub is a standby"),
            },
            Self::Transport(message) => write!(formatter, "{message}"),
        }
    }
}

async fn register(
    client: &Client,
    config: &NodeAgentConfig,
    data_address: Option<String>,
) -> Result<RegisterResponse, RegisterFailure> {
    let response = client
        .post(format!(
            "{}{}{}",
            config.hub_url, config.api_prefix, "/agent/register"
        ))
        .json(&RegisterRequest {
            node_id: config.node_id.clone(),
            node_token: config.node_token.clone(),
            protocol_version: "v1".into(),
            // data_address is Some only when the data plane actually bound —
            // a node whose port was taken must not advertise shuffle.
            capabilities: agent_capabilities(data_address.is_some()),
            boot_id: Some(config.boot_id.clone()),
            data_address,
        })
        .send()
        .await
        .map_err(|error| RegisterFailure::Transport(error.to_string()))?;
    let status = response.status();
    if !status.is_success() {
        if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            let problem: serde_json::Value = response
                .json()
                .await
                .map_err(|error| RegisterFailure::Transport(error.to_string()))?;
            if problem["code"] == "hub_standby" {
                let leader_url = problem["details"]["leader_url"]
                    .as_str()
                    .map(str::to_owned);
                return Err(RegisterFailure::Standby { leader_url });
            }
        }
        return Err(RegisterFailure::Transport(format!(
            "HTTP {status}: registration rejected"
        )));
    }
    response
        .json()
        .await
        .map_err(|error| RegisterFailure::Transport(error.to_string()))
}

#[allow(clippy::too_many_arguments)]
async fn run_session(
    client: &Client,
    cp: &ControlPlane,
    config: &NodeAgentConfig,
    session: RegisterResponse,
    cancellation: CancellationToken,
    completed_commands: &mut CompletedCommandCache,
    job_runtime: JobRuntime,
    network_shuffle: bool,
    resource_sampler: &ResourceSampler,
    failover_counter: &std::sync::atomic::AtomicU64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let auth = AgentAuth {
        node_id: config.node_id.clone(),
        session_token: session.session_token,
    };
    let mut heartbeat = tokio::time::interval(config.heartbeat_interval);
    let mut report_tick = tokio::time::interval(config.report_interval);
    let mut poll = tokio::time::interval(config.poll_interval);
    let mut command_tasks = JoinSet::new();
    let mut in_flight_commands = HashSet::new();
    let mut report_seq = 0_u64;
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                command_tasks.abort_all();
                while command_tasks.join_next().await.is_some() {}
                job_runtime.stop_all().await;
                let _ = post_json(client, format!("{}{}{}", config.hub_url, config.api_prefix, "/agent/heartbeat"), &HeartbeatRequest { auth: auth.clone(), state: "draining".into(), protocol_version: Some("v1".into()), software_version: Some(env!("CARGO_PKG_VERSION").into()), capabilities: agent_capabilities(network_shuffle), rollout_id: None }).await;
                return Ok(())
            },
            joined = command_tasks.join_next(), if !command_tasks.is_empty() => {
                match joined {
                    Some(Ok((command_id, Ok(result)))) => {
                        in_flight_commands.remove(&command_id);
                        remember_completed_command(completed_commands, command_id, result);
                    }
                    Some(Ok((command_id, Err(error)))) => {
                        in_flight_commands.remove(&command_id);
                        command_tasks.abort_all();
                        while command_tasks.join_next().await.is_some() {}
                        return Err(error);
                    }
                    Some(Err(error)) => {
                        command_tasks.abort_all();
                        while command_tasks.join_next().await.is_some() {}
                        return Err(error.into());
                    }
                    None => {}
                }
            },
            _ = heartbeat.tick() => { post_json(client, format!("{}{}{}", config.hub_url, config.api_prefix, "/agent/heartbeat"), &HeartbeatRequest { auth: auth.clone(), state: if cp.health().is_running() { "online".into() } else { "starting".into() }, protocol_version: Some("v1".into()), software_version: Some(env!("CARGO_PKG_VERSION").into()), capabilities: agent_capabilities(network_shuffle), rollout_id: None }).await?; }
            _ = report_tick.tick() => { report_seq = report_seq.saturating_add(1); post_json(client, format!("{}{}{}", config.hub_url, config.api_prefix, "/agent/report"), &report(cp, &auth, &config.boot_id, report_seq, &job_runtime, network_shuffle, resource_sampler, &config.hub_url, failover_counter.load(std::sync::atomic::Ordering::Relaxed)).await).await?; }
            _ = poll.tick() => {
                let finished = job_runtime.take_finished().await;
                for (index, (job_id, generation, outcome)) in finished.iter().enumerate() {
                    let (state, error) = match outcome {
                        Ok(()) => ("stopped".into(), None),
                        Err(error) => ("failed".into(), Some(error.clone())),
                    };
                    if let Err(delivery) = post_json(
                        client,
                        format!("{}{}{}", config.hub_url, config.api_prefix, "/agent/job-observations"),
                        &crate::hub::JobObservationRequest {
                            auth: auth.clone(),
                            job_id: job_id.clone(),
                            generation: *generation,
                            state,
                            error,
                        },
                    ).await {
                        // Park this observation and everything behind it: the
                        // task is already removed from the runtime map, so
                        // dropping the observation would leave the Hub
                        // reporting the job as running forever.
                        let mut undelivered = finished[index..].to_vec();
                        undelivered[0] = (job_id.clone(), *generation, outcome.clone());
                        job_runtime.park_observations(undelivered).await;
                        return Err(delivery);
                    }
                }
                let query = agent_auth_query(&auth.node_id);
                let commands: Vec<AgentCommand> = bearer_auth(client.get(format!("{}{}{}?{}", config.hub_url, config.api_prefix, "/agent/commands", query)), &auth.session_token).send().await?.error_for_status()?.json().await?;
                for command in commands {
                    if let Some(result) = replay_cached_command(completed_commands, &command.id) {
                        send_result(client, config, &auth, result).await?;
                        continue;
                    }
                    if !in_flight_commands.insert(command.id.clone()) {
                        continue;
                    }
                    let command_id = command.id.clone();
                    let command_client = client.clone();
                    let command_cp = cp.clone();
                    let command_config = config.clone();
                    let command_auth = auth.clone();
                    let command_runtime = job_runtime.clone();
                    command_tasks.spawn(async move {
                        let result = execute_command(
                            &command_client,
                            &command_cp,
                            &command_config,
                            &command_auth,
                            &command,
                            &command_runtime,
                        )
                        .await;
                        (command_id, result)
                    });
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn report(
    cp: &ControlPlane,
    auth: &AgentAuth,
    // The report boot identity belongs to the Agent process, not the
    // per-registration session credential. The Hub uses the session token to
    // fence delayed transport messages and this stable identity to decide
    // whether a new local JobRuntime must be reconstructed.
    boot_id: &str,
    report_seq: u64,
    job_runtime: &JobRuntime,
    network_shuffle: bool,
    resource_sampler: &ResourceSampler,
    connected_hub: &str,
    hub_failovers: u64,
) -> NodeReport {
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
    metrics.extend(job_runtime.metrics().await);
    // hub-ha stage 3 observability: which Hub this report targets and how
    // many candidate switches the process has performed.
    metrics.insert("hub_failovers".into(), hub_failovers as f64);
    // Host resource gauges ride the same map; a missing or stale sample is
    // simply omitted (observability must never block reporting).
    if let Some(snapshot) = resource_sampler.fresh(now_ms()) {
        merge_resource_gauges(&mut metrics, snapshot);
    }
    NodeReport {
        auth: auth.clone(),
        version: env!("CARGO_PKG_VERSION").into(),
        state: if cp.health().is_running() {
            "online".into()
        } else {
            "starting".into()
        },
        capabilities: agent_capabilities(network_shuffle),
        streams,
        operations: cp.operations().await,
        events: cp.events().await,
        metrics,
        jobs: job_runtime.job_snapshots().await,
        job_tasks: job_runtime.job_tasks().await,
        configuration: redacted_config(&cp.configuration().await).ok(),
        configuration_version,
        // The report rides every poll tick; truncate client-side to the
        // Hub's bound so a long-lived version store cannot bloat reports.
        config_versions: cp
            .versions()
            .unwrap_or_default()
            .into_iter()
            .take(128)
            .collect(),
        boot_id: Some(boot_id.into()),
        report_seq,
        connected_hub: Some(connected_hub.into()),
    }
}

async fn execute_command(
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
async fn execute_job_operation(
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

/// Bounded, insertion-ordered cache of completed command results. When the
/// bound is reached the OLDEST entry is evicted one at a time: clearing the
/// cache wholesale made the Hub's redeliveries of still-active lifecycle
/// commands re-execute (a redelivered job_start would cancel and restart a
/// running Job), breaking the at-most-once lifecycle guarantee.
#[derive(Default)]
struct CompletedCommandCache {
    entries: HashMap<String, CommandResult>,
    order: std::collections::VecDeque<String>,
    capacity: usize,
}

impl CompletedCommandCache {
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: std::collections::VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    fn replay(&self, command_id: &str) -> Option<CommandResult> {
        self.entries.get(command_id).cloned()
    }

    fn remember(&mut self, command_id: String, result: CommandResult) {
        if !self.entries.contains_key(&command_id) {
            while self.entries.len() >= self.capacity {
                if let Some(oldest) = self.order.pop_front() {
                    self.entries.remove(&oldest);
                }
            }
            self.order.push_back(command_id.clone());
        }
        self.entries.insert(command_id, result);
    }
}

fn replay_cached_command(cache: &CompletedCommandCache, command_id: &str) -> Option<CommandResult> {
    cache.replay(command_id)
}

fn remember_completed_command(
    cache: &mut CompletedCommandCache,
    command_id: String,
    result: CommandResult,
) {
    cache.remember(command_id, result);
}

async fn send_result(
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
/// Build the agent command query string.
///
/// The session credential travels only in the `Authorization: Bearer` header:
/// a query string leaks into reverse-proxy and access logs, and an Agent from
/// this release therefore requires a Hub from the same release or later.
fn agent_auth_query(node_id: &str) -> String {
    url::form_urlencoded::Serializer::new(String::new())
        .append_pair("node_id", node_id)
        .finish()
}

/// Attach the session credential as a Bearer header: tokens in URL query
/// strings leak into reverse-proxy and access logs.
fn bearer_auth(builder: reqwest::RequestBuilder, session_token: &str) -> reqwest::RequestBuilder {
    builder.header(
        reqwest::header::AUTHORIZATION,
        format!("Bearer {session_token}"),
    )
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

/// Equal-jitter backoff: sleep uniformly in `[backoff/2, backoff]`.
///
/// Many Agents losing their session at the same moment (a Hub restart, a fleet
/// of expired credentials) would otherwise retry in lockstep — the exponential
/// growth is identical for every node. Randomizing within the window keeps the
/// exponential bound while desynchronizing the re-registration burst.
fn jittered_backoff(backoff: Duration) -> Duration {
    use rand::Rng;
    if backoff.is_zero() {
        return backoff;
    }
    let low = (backoff / 2).as_millis() as u64;
    let high = backoff.as_millis() as u64;
    Duration::from_millis(rand::rng().random_range(low..=high))
}

fn command_is_stale(command_generation: u64, latest_generation: Option<u64>) -> bool {
    latest_generation.is_some_and(|latest| command_generation < latest)
}

/// The jitter must stay inside the equal-jitter window `[backoff/2, backoff]`
/// so the exponential bound survives while simultaneous retries desynchronize.
#[test]
fn jittered_backoff_stays_within_the_equal_jitter_window() {
    for backoff in [
        Duration::from_millis(250),
        Duration::from_secs(1),
        Duration::from_secs(10),
    ] {
        for _ in 0..200 {
            let sleep = jittered_backoff(backoff);
            assert!(
                sleep >= backoff / 2 && sleep <= backoff,
                "sleep {sleep:?} outside [{:?}, {backoff:?}]",
                backoff / 2
            );
        }
    }
    assert_eq!(jittered_backoff(Duration::ZERO), Duration::ZERO);
}

/// A wedged previous kernel must not hold the start path forever: the join
/// wait is bounded and then proceeds (the Hub retry machinery absorbs any
/// bounded state-lock failures that follow). A detached teardown has no
/// outcome — `None` — so the start path knows there is nothing to report.
#[tokio::test]
async fn wedged_previous_teardown_does_not_block_beyond_the_bound() {
    let mut wedged = tokio::spawn(std::future::pending::<Result<(), arkflow_core::Error>>());
    let started = std::time::Instant::now();
    let outcome =
        await_previous_teardown("job-wedge", &mut wedged, Duration::from_millis(100)).await;
    assert!(
        outcome.is_none(),
        "a detached teardown must report no outcome: {outcome:?}"
    );
    assert!(started.elapsed() >= Duration::from_millis(100));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the bounded wait must not turn into an unbounded hang: {:?}",
        started.elapsed()
    );
}

/// A previous kernel that exited on its own with an error must surface that
/// outcome through the join result, so the start path can park a crash
/// observation instead of silently discarding the crash.
#[tokio::test]
async fn crashed_previous_teardown_surfaces_the_join_error() {
    let mut crashed = tokio::spawn(async {
        Result::<(), arkflow_core::Error>::Err(arkflow_core::Error::Process(
            "kernel exploded".into(),
        ))
    });
    let outcome = await_previous_teardown("job-crash", &mut crashed, Duration::from_secs(5)).await;
    assert!(
        matches!(&outcome, Some(Err(message)) if message.contains("kernel exploded")),
        "the crash outcome must pass through: {outcome:?}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{hub_router, ServerConfig};
    use arkflow_core::config::{EngineConfig, HealthCheckConfig, LoggingConfig};
    use arkflow_core::control_plane::ControlPlane;
    use arkflow_core::runtime::RuntimeManager;

    /// Env-mutating TLS tests and any `run()` invocation with a data port
    /// read the same `ARKFLOW_DATA_PLANE_TLS_*` variables: serialize them so
    /// a concurrent data-plane session never observes a half-set environment.
    static ENV_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
        std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

    /// Kernel-starting tests serialize like the two-node smoke suite: in
    /// parallel they saturate a shared runner and the resulting redb lock
    /// contention turns healthy starts into flaky failures.
    static ONE_KERNEL_AT_A_TIME: std::sync::LazyLock<tokio::sync::Mutex<()>> =
        std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

    /// Regression: the session credential used to travel in the URL query
    /// string for the transition window. It now rides only in the
    /// `Authorization: Bearer` header — query strings leak into reverse-proxy
    /// and access logs. An Agent from this release therefore requires a Hub
    /// from the same release or later; the Hub keeps accepting query-only
    /// credentials from Agents that predate this change.
    #[test]
    fn agent_commands_carry_the_credential_only_in_the_header() {
        let auth = AgentAuth {
            node_id: "node-a".into(),
            session_token: "secret-token".into(),
        };
        let query = agent_auth_query(&auth.node_id);
        assert!(query.contains("node_id=node-a"), "{query}");
        assert!(
            !query.contains("session_token"),
            "the query string must not carry the credential anymore: {query}"
        );

        // The header is the only transport for the session credential.
        let request = bearer_auth(
            reqwest::Client::new().get("http://example.invalid"),
            &auth.session_token,
        );
        let request = request.build().unwrap();
        assert_eq!(
            request
                .headers()
                .get(reqwest::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer secret-token")
        );
    }

    /// A finished-task observation whose delivery failed must survive the
    /// session rebuild: the task is already gone from the runtime map, so
    /// dropping the observation would leave the Hub reporting the job as
    /// running forever.
    /// An aborted `job_start` (session teardown aborts command tasks) must
    /// never orphan a RUNNING kernel: the job must be registered in the task
    /// map before the kernel spawn begins, so a later stop can always reach
    /// and cancel it.
    #[tokio::test]
    async fn aborted_start_leaves_no_unregistered_running_kernel() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let runtime = std::sync::Arc::new(JobRuntime::default());
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(serde_json::json!({
            "id": "orders",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{
                "operator_id": "source",
                "input_type": "generate",
                "config": {"context": "node-a", "interval": "10ms", "batch_size": 1},
                "time": {"mode": "processing_time"}
            }],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        }))
        .unwrap();
        let plan = arkflow_core::job::JobPlan::compile(spec).unwrap();
        let assignments = plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .expect("colocated placement succeeds");
        assert!(!assignments.is_empty());
        let spawn_runtime = runtime.clone();
        let start_task = tokio::spawn(async move {
            spawn_runtime
                .start(
                    plan,
                    assignments,
                    1,
                    None,
                    false,
                    "node-a",
                    &SplitPlacementPayload::default(),
                )
                .await
        });
        // Registration-first: observe the entry as early as possible.
        let mut registered = false;
        for _ in 0..2000 {
            if runtime.tasks.lock().await.contains_key("orders") {
                registered = true;
                break;
            }
            if start_task.is_finished() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        if registered {
            // Abort while the start may still be mid-spawn: this mirrors
            // `command_tasks.abort_all()` during session teardown.
            start_task.abort();
        }
        let _ = start_task.await;
        // Whatever stage the start reached, stop must reach the Job and the
        // runtime must end up with no surviving kernel entry.
        runtime.stop("orders", 1).await.unwrap();
        let _ = runtime.take_finished().await;
        assert!(
            runtime.tasks.lock().await.is_empty(),
            "no kernel may survive an aborted start without a registered, cancellable entry"
        );
        // A follow-up start for the same Job must not be wedged by the
        // aborted one (the placeholder or kernel must not hold resources).
        let spawn_runtime = runtime.clone();
        let restart = tokio::spawn(async move {
            let spec: arkflow_core::job::JobSpec = serde_json::from_value(serde_json::json!({
                "id": "orders",
                "version": 1,
                "operators": [
                    {"id": "source", "kind": "source"},
                    {"id": "sink", "kind": "sink"}
                ],
                "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
                "sources": [{
                    "operator_id": "source",
                    "input_type": "generate",
                    "config": {"context": "node-a", "interval": "10ms", "batch_size": 1},
                    "time": {"mode": "processing_time"}
                }],
                "sinks": [{"operator_id": "sink", "output_type": "drop"}]
            }))
            .unwrap();
            let plan = arkflow_core::job::JobPlan::compile(spec).unwrap();
            let assignments = plan
                .assignments_for_nodes(&["node-a".to_string()], 1)
                .expect("colocated placement succeeds");
            spawn_runtime
                .start(
                    plan,
                    assignments,
                    2,
                    None,
                    false,
                    "node-a",
                    &SplitPlacementPayload::default(),
                )
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), restart)
            .await
            .expect("a start after an aborted start must not be wedged")
            .unwrap()
            .unwrap();
        runtime.stop("orders", 2).await.unwrap();
        let _ = runtime.take_finished().await;
        assert!(runtime.tasks.lock().await.is_empty());
    }

    #[tokio::test]
    async fn parked_job_observations_are_redelivered_by_the_next_session() {
        let runtime = JobRuntime::default();
        runtime
            .park_observations(vec![
                ("orders".into(), 3, Err("kernel failed".into())),
                ("billing".into(), 1, Ok(())),
            ])
            .await;

        let finished = runtime.take_finished().await;
        assert_eq!(finished.len(), 2, "parked observations are retried");
        assert_eq!(finished[0].0, "orders");
        assert_eq!(finished[0].1, 3);
        assert!(finished[0].2.is_err());
        assert_eq!(finished[1].0, "billing");

        // A later session with no new finishes must not re-deliver them.
        assert!(runtime.take_finished().await.is_empty());
    }

    /// Compile the generate→drop smoke spec for the replacement-path tests,
    /// mirroring `aborted_start_leaves_no_unregistered_running_kernel`.
    async fn replacement_test_plan(job_id: &str) -> (JobPlan, Vec<TaskAttempt>) {
        let _ = arkflow_plugin::initialize();
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(serde_json::json!({
            "id": job_id,
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{
                "operator_id": "source",
                "input_type": "generate",
                "config": {"context": "node-a", "interval": "10ms", "batch_size": 1},
                "time": {"mode": "processing_time"}
            }],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        }))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let assignments = plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .expect("colocated placement succeeds");
        assert!(!assignments.is_empty());
        (plan, assignments)
    }

    /// Register a synthetic task whose kernel has ALREADY exited with the
    /// given outcome — the "crashed between the poll drain and this reader"
    /// state, made deterministic by yielding until the handle is finished
    /// before the entry becomes visible to `start`.
    async fn insert_exited_task(
        runtime: &JobRuntime,
        job_id: &str,
        generation: u64,
        outcome: Result<(), arkflow_core::Error>,
    ) {
        // redb holds an exclusive file lock per path, and every test in this
        // binary shares one process: key the synthetic state dir uniquely.
        static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let state_root = std::env::temp_dir().join(format!(
            "arkflow-agent-replacement-{job_id}-{generation}-{}-{sequence}",
            std::process::id()
        ));
        let state: Arc<dyn StateBackend> =
            Arc::new(RedbStateBackend::open(state_root, 1).expect("test state backend opens"));
        let handle = tokio::spawn(async move { outcome });
        while !handle.is_finished() {
            tokio::task::yield_now().await;
        }
        runtime.tasks.lock().await.insert(
            job_id.to_string(),
            JobTask {
                generation,
                ephemeral_state: false,
                recovery_required: false,
                cancellation: CancellationToken::new(),
                assignments: Vec::new(),
                dedicated_runtime: None,
                watermark_partitions: BTreeMap::new(),
                state,
                checkpoint_store_uri: None,
                kernel: None,
                handle,
            },
        );
    }

    /// A start re-delivered at the generation whose kernel has already
    /// crashed must NOT be an idempotent no-op success: the crash is surfaced
    /// through the job-observation channel and a fresh kernel takes over.
    #[tokio::test]
    async fn same_generation_start_over_a_crashed_kernel_surfaces_the_crash() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let runtime = Arc::new(JobRuntime::default());
        insert_exited_task(
            &runtime,
            "orders-crash",
            1,
            Err(arkflow_core::Error::Process("kernel crashed".into())),
        )
        .await;
        let (plan, assignments) = replacement_test_plan("orders-crash").await;

        let result = tokio::time::timeout(
            Duration::from_secs(30),
            runtime.start(
                plan,
                assignments,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            ),
        )
        .await
        .expect("the replacement start must not hang");
        assert!(result.is_ok(), "the fresh kernel must start: {result:?}");

        let finished = runtime.take_finished().await;
        assert!(
            finished.iter().any(|(job_id, generation, outcome)| {
                job_id == "orders-crash" && *generation == 1 && outcome.is_err()
            }),
            "the crashed kernel's exit must reach the observation channel: {finished:?}"
        );

        runtime.stop("orders-crash", 1).await.unwrap();
        let _ = runtime.take_finished().await;
        assert!(runtime.tasks.lock().await.is_empty());
    }

    /// A higher-generation start replacing an already-crashed kernel must not
    /// swallow the crash: the observation rides the superseded generation.
    #[tokio::test]
    async fn generation_bump_over_a_crashed_kernel_reports_the_superseded_crash() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let runtime = Arc::new(JobRuntime::default());
        insert_exited_task(
            &runtime,
            "orders-bump",
            1,
            Err(arkflow_core::Error::Process("kernel crashed".into())),
        )
        .await;
        let (plan, assignments) = replacement_test_plan("orders-bump").await;

        let result = tokio::time::timeout(
            Duration::from_secs(30),
            runtime.start(
                plan,
                assignments,
                2,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            ),
        )
        .await
        .expect("the replacing start must not hang");
        assert!(result.is_ok(), "the new generation must start: {result:?}");

        let finished = runtime.take_finished().await;
        assert!(
            finished.iter().any(|(job_id, generation, outcome)| {
                job_id == "orders-bump" && *generation == 1 && outcome.is_err()
            }),
            "the superseded kernel's crash must not be silently discarded: {finished:?}"
        );

        runtime.stop("orders-bump", 2).await.unwrap();
        let _ = runtime.take_finished().await;
        assert!(runtime.tasks.lock().await.is_empty());
    }

    /// A superseded kernel that exited cleanly (Ok) produces no crash
    /// observation — only genuine crashes are parked.
    #[tokio::test]
    async fn clean_replacement_produces_no_crash_observation() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let runtime = Arc::new(JobRuntime::default());
        insert_exited_task(&runtime, "orders-clean", 1, Ok(())).await;
        let (plan, assignments) = replacement_test_plan("orders-clean").await;

        let result = tokio::time::timeout(
            Duration::from_secs(30),
            runtime.start(
                plan,
                assignments,
                2,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            ),
        )
        .await
        .expect("the replacing start must not hang");
        assert!(result.is_ok(), "the new generation must start: {result:?}");

        let finished = runtime.take_finished().await;
        assert!(
            !finished
                .iter()
                .any(|(job_id, generation, _)| job_id == "orders-clean" && *generation == 1),
            "a clean exit must not be reported as a crash: {finished:?}"
        );

        runtime.stop("orders-clean", 2).await.unwrap();
        let _ = runtime.take_finished().await;
        assert!(runtime.tasks.lock().await.is_empty());
    }

    /// Regression guard for the idempotency narrowing: a LIVE kernel at the
    /// same generation still short-circuits the start as a no-op success,
    /// with no observation parked and no restart churn.
    #[tokio::test]
    async fn healthy_same_generation_start_stays_an_idempotent_no_op() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let runtime = Arc::new(JobRuntime::default());
        let (plan, assignments) = replacement_test_plan("orders-idem").await;
        runtime
            .start(
                plan,
                assignments,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .expect("the initial start succeeds");
        let started_at = std::time::Instant::now();

        let (plan, assignments) = replacement_test_plan("orders-idem").await;
        runtime
            .start(
                plan,
                assignments,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .expect("the re-delivered same-generation start is a no-op success");

        assert!(
            started_at.elapsed() < Duration::from_secs(5),
            "the no-op must not wait behind a teardown"
        );
        let tasks = runtime.tasks.lock().await;
        let task = tasks
            .get("orders-idem")
            .expect("the kernel stays registered");
        assert_eq!(task.generation, 1);
        assert!(!task.handle.is_finished(), "the kernel was not restarted");
        assert!(runtime.pending_observations.lock().await.is_empty());
        drop(tasks);

        runtime.stop("orders-idem", 1).await.unwrap();
        let _ = runtime.take_finished().await;
        assert!(runtime.tasks.lock().await.is_empty());
    }

    #[test]
    fn agent_mode_requires_hub_and_stable_identity() {
        let health = HealthCheckConfig {
            hub_urls: vec!["http://hub".into()],
            node_id: Some("node-a".into()),
            ..Default::default()
        };
        let config = EngineConfig {
            streams: vec![],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            health_check: health,
        };
        let agent = NodeAgentConfig::from_engine(&config).unwrap();
        assert_eq!(agent.node_id, "node-a");
        assert_eq!(agent.api_prefix, "/api/v1");
    }

    /// The data-plane port rides the health-check config into the agent and
    /// flips the `network_shuffle` capability; absent (the default) keeps the
    /// co-location contract and never advertises shuffle.
    #[test]
    fn data_plane_port_flows_from_config_into_capabilities() {
        let base = agent_capabilities(false);
        let shuffle = agent_capabilities(true);
        assert!(!base.contains(&"network_shuffle".to_string()));
        assert_eq!(shuffle.len(), base.len() + 1);
        assert!(shuffle.contains(&"network_shuffle".to_string()));

        let mut health = HealthCheckConfig {
            hub_urls: vec!["http://hub".into()],
            node_id: Some("node-a".into()),
            ..Default::default()
        };
        let config = EngineConfig {
            streams: vec![],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            health_check: health.clone(),
        };
        let agent = NodeAgentConfig::from_engine(&config).unwrap();
        assert_eq!(agent.data_port, None);

        health.data_port = Some(9501);
        let config = EngineConfig {
            streams: vec![],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            health_check: health,
        };
        let agent = NodeAgentConfig::from_engine(&config).unwrap();
        assert_eq!(agent.data_port, Some(9501));
    }

    #[test]
    fn expired_commands_are_rejected_by_time_boundary() {
        assert!(command_expired(10, 10));
        assert!(!command_expired(11, 10));
    }

    /// Loopback detection for the no-proxy decision must cover the whole
    /// 127/8 range and both IPv6 spellings — a system interception proxy
    /// breaking `127.0.0.2` would wedge the agent just like `127.0.0.1`.
    /// Both client flavors build: loopback hubs are pinned to no-proxy,
    /// everything else keeps the system proxy configuration.
    #[test]
    fn agent_client_builds_for_loopback_and_remote_hubs() {
        build_agent_client("http://127.0.0.1:8080").expect("loopback client builds");
        build_agent_client("http://hub.example.invalid:8080").expect("remote client builds");
        build_agent_client("not a url").expect("an unparseable hub still yields a client");
    }

    #[test]
    fn loopback_detection_covers_the_whole_loopback_range() {
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("127.255.0.4"));
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("::1"));
        assert!(is_loopback_host("[::1]"));
        assert!(is_loopback_host("0.0.0.0"));
        assert!(!is_loopback_host("10.1.2.3"));
        assert!(!is_loopback_host("::ffff:10.0.0.1"));
        assert!(!is_loopback_host("hub.example.com"));
        assert!(!is_loopback_host(""));
    }

    #[test]
    fn older_command_generation_is_stale() {
        assert!(command_is_stale(41, Some(42)));
        assert!(!command_is_stale(42, Some(42)));
        assert!(!command_is_stale(42, None));
    }

    #[test]
    fn duplicate_command_replays_the_terminal_result() {
        let mut cache = CompletedCommandCache::new(1024);
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
            observed_checkpoint_id: None,
            checkpoint_manifest_uri: None,
            result: None,
        };
        assert!(replay_cached_command(&cache, "cmd-1").is_none());
        remember_completed_command(&mut cache, "cmd-1".into(), result.clone());
        let replay = replay_cached_command(&cache, "cmd-1").unwrap();
        assert_eq!(replay.command_id, result.command_id);
        assert_eq!(replay.state, result.state);
        assert_eq!(replay.action_id, result.action_id);
    }

    #[test]
    fn shared_checkpoint_store_uses_configured_uri() {
        let directory = tempfile::tempdir().unwrap();
        let uri = Url::from_directory_path(directory.path()).unwrap();
        let store = SharedCheckpointStore::from_uri(uri.as_str()).unwrap();
        store
            .put("checkpoints/cp-1/manifest.json", b"manifest")
            .unwrap();
        assert_eq!(
            store
                .get("checkpoints/cp-1/manifest.json")
                .unwrap()
                .as_deref(),
            Some(b"manifest".as_slice())
        );
    }

    #[test]
    fn recovery_payload_requires_a_checkpoint_id() {
        assert_eq!(
            parse_recovery_payload(&serde_json::json!({})).unwrap(),
            (None, false, false)
        );
        assert_eq!(
            parse_recovery_payload(&serde_json::json!({
                "recovery_required": true,
                "recovery": {
                    "checkpoint_id": "cp-1",
                    "savepoint": true
                }
            }))
            .unwrap(),
            (Some("cp-1".into()), true, true)
        );
        assert!(parse_recovery_payload(&serde_json::json!({
            "recovery": {}
        }))
        .is_err());
    }

    #[test]
    fn durable_agent_state_isolated_by_generation() {
        let root = tempfile::tempdir().unwrap();
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(serde_json::json!({
            "id": "orders",
            "version": 4,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "aggregate", "kind": "aggregate", "stateful": true, "key_field": "key"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [
                {"id": "source-aggregate", "from": "source", "to": "aggregate"},
                {"id": "aggregate-sink", "from": "aggregate", "to": "sink"}
            ],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}],
            "state": {
                "backend": "embedded_kv",
                "durability": "durable",
                "root": root.path().display().to_string(),
                "format_version": 1
            },
            "checkpoint": {
                "interval_ms": 1000,
                "retention": 2,
                "object_store_uri": "file:///tmp/arkflow-agent-test-checkpoints"
            }
        }))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let generation_one = durable_recovery_marker(&plan, "node-a", 1).unwrap();
        let generation_two = durable_recovery_marker(&plan, "node-a", 2).unwrap();
        assert_ne!(generation_one, generation_two);
        assert!(generation_one.ends_with("generation-1/.arkflow-started"));
        assert!(generation_two.ends_with("generation-2/.arkflow-started"));
    }

    #[test]
    fn node_state_path_encoding_does_not_alias_distinct_ids() {
        assert_ne!(safe_path_component("node/a"), safe_path_component("node_a"));
        assert_eq!(safe_path_component("node/a"), "node%2Fa");
    }

    #[tokio::test]
    async fn checkpoint_execution_failure_is_ready_for_terminal_result() {
        let command = AgentCommand {
            id: "cmd-checkpoint".into(),
            operation_id: "op-checkpoint".into(),
            node_id: "node-a".into(),
            operation: "job_checkpoint".into(),
            resource_id: "job-a".into(),
            expires_at_ms: now_ms().saturating_add(60_000),
            generation: 3,
            action_id: Some("checkpoint-action".into()),
            config_version_id: None,
            attempt_id: None,
            rollout_id: Some("rollout-1".into()),
            correlation_id: Some("corr-1".into()),
            payload: None,
            required_capabilities: vec!["checkpoint_recovery".into()],
        };
        let config = NodeAgentConfig {
            hub_url: "http://hub".into(),
            hub_urls: vec!["http://hub".into()],
            api_prefix: "/api/v1".into(),
            node_id: "node-a".into(),
            data_host: None,
            node_token: "token".into(),
            boot_id: "boot-1".into(),
            heartbeat_interval: Duration::from_secs(5),
            data_port: None,
            report_interval: Duration::from_secs(5),
            poll_interval: Duration::from_secs(5),
        };
        let error = execute_job_operation(&command, &config, &JobRuntime::default())
            .await
            .unwrap_err();
        assert_eq!(error, "missing checkpoint payload");
    }

    fn snapshot(sampled_at_ms: u64) -> ResourceSnapshot {
        ResourceSnapshot {
            sampled_at_ms,
            cpu_usage_percent: Some(37.5),
            memory_used_bytes: 4_000,
            memory_total_bytes: 8_000,
            memory_available_bytes: 4_000,
            cpu_cores: 2,
        }
    }

    #[test]
    fn resource_gauges_merge_under_the_fixed_vocabulary() {
        let mut metrics = BTreeMap::new();
        metrics.insert("input_messages".into(), 9.0);
        merge_resource_gauges(&mut metrics, snapshot(1));
        let mut keys: Vec<_> = metrics.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "input_messages",
                "node_cpu_cores",
                "node_cpu_usage_percent",
                "node_memory_available_bytes",
                "node_memory_total_bytes",
                "node_memory_used_bytes",
            ]
        );
        assert_eq!(metrics["node_cpu_usage_percent"], 37.5);
        assert_eq!(metrics["node_cpu_cores"], 2.0);
        assert_eq!(metrics["node_memory_total_bytes"], 8_000.0);
    }

    #[test]
    fn cpu_warmup_publishes_memory_gauges_only() {
        let mut cold = snapshot(1);
        cold.cpu_usage_percent = None;
        let mut metrics = BTreeMap::new();
        merge_resource_gauges(&mut metrics, cold);
        assert!(!metrics.contains_key("node_cpu_usage_percent"));
        // Memory gauges plus the static CPU core count.
        assert_eq!(metrics.len(), 4);
    }

    #[test]
    fn stale_snapshots_are_omitted_from_reports() {
        let sampler = ResourceSampler::new(Duration::from_secs(2));
        // The sampling cadence derives from the report interval: half of it,
        // bounded below, so consecutive reports see independent samples.
        assert_eq!(sampler.sample_interval, Duration::from_secs(1));
        assert!(sampler.fresh(1_000).is_none(), "nothing published yet");
        sampler.publish(snapshot(1_000));
        let window_ms = sampler.fresh_window_ms();
        assert!(sampler.fresh(1_000 + window_ms).is_some());
        assert!(sampler.fresh(1_000 + window_ms + 1).is_none());
        // A fresher publish replaces the slot entirely.
        sampler.publish(snapshot(2_000));
        assert!(sampler.fresh(1_000 + window_ms + 1).is_some());
    }

    #[test]
    fn sample_interval_tracks_the_report_interval_with_a_floor() {
        assert_eq!(
            ResourceSampler::new(Duration::from_secs(10)).sample_interval,
            Duration::from_secs(5)
        );
        // absurdly fast reporting must not spin the sampler into the ground
        assert_eq!(
            ResourceSampler::new(Duration::from_millis(100)).sample_interval,
            MIN_RESOURCE_SAMPLE_INTERVAL
        );
    }

    fn rescale_spec_value(parallelism: u32, rescale: bool) -> serde_json::Value {
        serde_json::json!({
            "id": "agent-rescale-job",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "agg", "kind": "aggregate", "stateful": true, "key_field": "key"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [
                {"id": "e1", "from": "source", "to": "agg", "partitioned": true},
                {"id": "e2", "from": "agg", "to": "sink"}
            ],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}],
            "state": {"backend": "embedded_kv", "durability": "durable", "format_version": 1},
            "checkpoint": {"object_store_uri": "memory://rescale-test", "interval_ms": 30000, "retention": 3},
            "parallelism": parallelism,
            "max_parallelism": 16,
            "rescale": rescale
        })
    }

    fn attempt_for(plan: &JobPlan, task_id: &str, node_id: &str) -> arkflow_core::job::TaskAttempt {
        arkflow_core::job::TaskAttempt {
            id: format!("{task_id}:{node_id}:1"),
            job_id: plan.spec.id.clone(),
            job_version: plan.spec.version,
            task_id: task_id.to_owned(),
            generation: 1,
            node_id: node_id.to_owned(),
            state: arkflow_core::job::TaskAttemptState::Queued,
        }
    }

    /// Distributed rescale: a parallelism-1 artifact restored under a
    /// parallelism-2 plan with `rescale: true`. Each node keeps exactly the
    /// entries whose redistributed namespace belongs to one of its assigned
    /// tasks; the two nodes' sets are disjoint and cover every entry.
    #[test]
    fn agent_rescale_restore_partitions_entries_exactly_by_node() {
        let old_spec: arkflow_core::job::JobSpec =
            serde_json::from_value(rescale_spec_value(1, true)).unwrap();
        let old_plan = JobPlan::compile(old_spec).unwrap();
        let new_spec: arkflow_core::job::JobSpec =
            serde_json::from_value(rescale_spec_value(2, true)).unwrap();
        let new_plan = JobPlan::compile(new_spec).unwrap();

        let old_task = old_plan
            .tasks
            .iter()
            .find(|task| task.operator_id == "agg")
            .unwrap();
        let old_namespace = arkflow_core::job::effective_state_namespace(
            &old_plan.spec.id,
            old_plan.spec.state.as_ref(),
            "agg",
            &old_task.id,
        );
        let keys = [
            "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
        ];
        let entries = keys
            .iter()
            .map(|key| arkflow_core::state::StateEntry {
                namespace: old_namespace.clone(),
                key: format!("utf8:{key}").into_bytes(),
                value: format!("v-{key}").into_bytes(),
                expires_at_ms: None,
            })
            .collect();
        let snapshot = arkflow_core::state::StateSnapshot::new(1, entries);

        let root = std::env::temp_dir().join(format!(
            "arkflow-agent-rescale-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let repository = CheckpointRepository::new(
            arkflow_core::checkpoint::FileCheckpointStore::new(&root).unwrap(),
        );
        let mut snapshot_ref = repository
            .write_state_snapshot("c-rescale", &snapshot)
            .unwrap();
        snapshot_ref.task_id = old_task.id.clone();
        let mut manifest = arkflow_core::checkpoint::CheckpointManifest {
            checkpoint_id: "c-rescale".into(),
            job_id: old_plan.spec.id.clone(),
            job_version: old_plan.spec.version,
            generation: 1,
            task_attempts: vec![arkflow_core::checkpoint::TaskAttemptSnapshot {
                task_id: old_task.id.clone(),
                attempt_id: format!("{}:n1:0", old_task.id),
                node_id: "n1".into(),
            }],
            source_positions: Vec::new(),
            watermarks_ms: Default::default(),
            watermark_partitions: Default::default(),
            in_flight_barrier: arkflow_core::checkpoint::CheckpointBarrier {
                checkpoint_id: "c-rescale".into(),
                generation: 1,
                trace_context: None,
            },
            state_snapshots: vec![snapshot_ref],
            format_version: 1,
            checksum: 0,
        };
        manifest.seal();
        repository
            .write_manifest(
                &manifest,
                arkflow_core::checkpoint::RecoveryArtifactKind::Checkpoint,
                arkflow_core::checkpoint::recovery_manifest_key(
                    arkflow_core::checkpoint::RecoveryArtifactKind::Checkpoint,
                    "c-rescale",
                ),
            )
            .unwrap();

        // Split the new plan's aggregate tasks across two nodes by hand.
        let agg_tasks = new_plan
            .tasks
            .iter()
            .filter(|task| task.operator_id == "agg")
            .map(|task| task.id.clone())
            .collect::<Vec<_>>();
        assert_eq!(agg_tasks.len(), 2, "parallelism 2 must plan two agg tasks");
        let node_a = vec![
            attempt_for(&new_plan, &agg_tasks[0], "node-a"),
            attempt_for(&new_plan, "source-0", "node-a"),
        ];
        let node_b = vec![
            attempt_for(&new_plan, &agg_tasks[1], "node-b"),
            attempt_for(&new_plan, "sink-0", "node-b"),
        ];

        let mut restored_total = 0usize;
        for (node, assignments) in [("node-a", &node_a), ("node-b", &node_b)] {
            let state_root = root.join(format!("state-{node}"));
            let backend = RedbStateBackend::open(&state_root, 1).unwrap();
            let state: Arc<dyn StateBackend> = Arc::new(backend);
            restore_recovery_state(&new_plan, &repository, &manifest, assignments, &state, true)
                .unwrap();
            let assigned: BTreeSet<&str> = assignments
                .iter()
                .map(|assignment| assignment.task_id.as_str())
                .collect();
            let mut restored_here = 0usize;
            for key in keys {
                let group = arkflow_core::job::key_group_for_key(
                    key.as_bytes(),
                    new_plan.spec.max_parallelism,
                )
                .unwrap();
                let owner = new_plan
                    .tasks
                    .iter()
                    .find(|task| {
                        task.operator_id == "agg"
                            && task
                                .partitions
                                .iter()
                                .any(|partition| partition.key_group.contains(group))
                    })
                    .unwrap();
                let namespace = arkflow_core::job::effective_state_namespace(
                    &new_plan.spec.id,
                    new_plan.spec.state.as_ref(),
                    "agg",
                    &owner.id,
                );
                let stored = state
                    .get(&namespace, format!("utf8:{key}").as_bytes())
                    .unwrap();
                if assigned.contains(owner.id.as_str()) {
                    assert_eq!(
                        stored.unwrap(),
                        format!("v-{key}").into_bytes(),
                        "{node} owns key {key}"
                    );
                    restored_here += 1;
                } else {
                    assert!(
                        stored.is_none(),
                        "{node} must not restore key {key} owned by {}",
                        owner.id
                    );
                }
            }
            assert!(restored_here > 0, "{node} should own at least one key");
            restored_total += restored_here;
        }
        assert_eq!(
            restored_total,
            keys.len(),
            "entries must partition exactly across nodes"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Without the rescale declaration the distributed recovery keeps the
    /// fail-closed guard with the actionable error.
    #[test]
    fn agent_recovery_without_rescale_fails_closed_on_task_set_change() {
        let old_spec: arkflow_core::job::JobSpec =
            serde_json::from_value(rescale_spec_value(1, false)).unwrap();
        let old_plan = JobPlan::compile(old_spec).unwrap();
        let new_spec: arkflow_core::job::JobSpec =
            serde_json::from_value(rescale_spec_value(2, false)).unwrap();
        let new_plan = JobPlan::compile(new_spec).unwrap();

        let old_task = old_plan
            .tasks
            .iter()
            .find(|task| task.operator_id == "agg")
            .unwrap();
        let mut manifest = arkflow_core::checkpoint::CheckpointManifest {
            checkpoint_id: "c-guard".into(),
            job_id: old_plan.spec.id.clone(),
            job_version: old_plan.spec.version,
            generation: 1,
            task_attempts: vec![arkflow_core::checkpoint::TaskAttemptSnapshot {
                task_id: old_task.id.clone(),
                attempt_id: format!("{}:n1:0", old_task.id),
                node_id: "n1".into(),
            }],
            source_positions: Vec::new(),
            watermarks_ms: Default::default(),
            watermark_partitions: Default::default(),
            in_flight_barrier: arkflow_core::checkpoint::CheckpointBarrier {
                checkpoint_id: "c-guard".into(),
                generation: 1,
                trace_context: None,
            },
            state_snapshots: Vec::new(),
            format_version: 1,
            checksum: 0,
        };
        manifest.seal();
        let error =
            validate_recovery_manifest(&new_plan, "c-guard", 1, &manifest, false).unwrap_err();
        assert!(
            error.contains("task set does not match the planned assignment"),
            "{error}"
        );
        // The rescale flag waives exactly that check for the same artifact.
        assert!(validate_recovery_manifest(&new_plan, "c-guard", 1, &manifest, true).is_ok());
    }

    /// A same-generation start whose per-node task set DIFFERS from the live
    /// kernel's assignment must replace the kernel (the drifted mapping has
    /// to take effect), not report a healthy no-op.
    #[tokio::test]
    async fn drifting_same_generation_start_replaces_the_kernel() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let runtime = Arc::new(JobRuntime::default());
        let (plan, assignments) = replacement_test_plan("orders-drift").await;
        let initial_task_count = assignments.len();
        runtime
            .start(
                plan,
                assignments,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .expect("the initial start succeeds");

        // Same generation, a different (still complete and valid) task set:
        // a two-source plan replaces the one-source plan. The kernel must be
        // replaced with the new assignment instead of no-opping.
        let drift_spec: arkflow_core::job::JobSpec = serde_json::from_value(serde_json::json!({
            "id": "orders-drift",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "extra", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [
                {"id": "e1", "from": "source", "to": "sink"},
                {"id": "e2", "from": "extra", "to": "sink"}
            ],
            "sources": [
                {
                    "operator_id": "source",
                    "input_type": "generate",
                    "config": {"context": "node-a", "interval": "10ms", "batch_size": 1},
                    "time": {"mode": "processing_time"}
                },
                {
                    "operator_id": "extra",
                    "input_type": "generate",
                    "config": {"context": "node-b", "interval": "10ms", "batch_size": 1},
                    "time": {"mode": "processing_time"}
                }
            ],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        }))
        .unwrap();
        let drift_plan = JobPlan::compile(drift_spec).unwrap();
        let drift_assignments = drift_plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .expect("colocated placement succeeds");
        assert!(
            drift_assignments.len() > initial_task_count,
            "the two-source plan must assign more tasks"
        );
        runtime
            .start(
                drift_plan,
                drift_assignments.clone(),
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .expect("the drifted start replaces the kernel");

        {
            let tasks = runtime.tasks.lock().await;
            let task = tasks
                .get("orders-drift")
                .expect("the kernel stays registered");
            assert_eq!(task.generation, 1, "the generation is unchanged");
            let live: std::collections::BTreeSet<String> = task
                .assignments
                .iter()
                .map(|assignment| assignment.task_id.clone())
                .collect();
            let expected: std::collections::BTreeSet<String> = drift_assignments
                .iter()
                .map(|assignment| assignment.task_id.clone())
                .collect();
            assert_eq!(live, expected, "the kernel now runs the drifted assignment");
            assert!(live.contains("extra-0"));
            assert!(!task.handle.is_finished());
        }

        runtime.stop("orders-drift", 1).await.unwrap();
        let _ = runtime.take_finished().await;
    }

    /// A Job declaring cpu_millicores runs on a dedicated runtime with
    /// ceil(millicores/1000) workers (min 1); an undeclared Job keeps the
    /// shared runtime (None).
    #[tokio::test]
    async fn declared_cpu_runs_on_a_dedicated_bounded_runtime() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        // The component catalogue is process-global: resolve it explicitly
        // instead of depending on a sibling test having initialized it (a
        // process-per-test runner like nextest runs this test alone).
        let _ = arkflow_plugin::initialize();
        let runtime = Arc::new(JobRuntime::default());
        let mut spec_value = serde_json::json!({
            "id": "orders-cpu",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{
                "operator_id": "source",
                "input_type": "generate",
                "config": {"context": "node-a", "interval": "10ms", "batch_size": 1},
                "time": {"mode": "processing_time"}
            }],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        });
        spec_value["resources"] = serde_json::json!({"cpu_millicores": 2500});
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(spec_value).unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let assignments = plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .unwrap();
        runtime
            .start(
                plan,
                assignments,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .expect("declared start succeeds");
        {
            let tasks = runtime.tasks.lock().await;
            let task = tasks.get("orders-cpu").expect("registered");
            let dedicated = task
                .dedicated_runtime
                .as_ref()
                .expect("declared cpu jobs own a dedicated runtime");
            assert_eq!(
                dedicated.metrics().num_workers(),
                3,
                "2500 millicores => ceil(2.5) = 3 workers"
            );
        }
        runtime.stop("orders-cpu", 1).await.unwrap();
        let _ = runtime.take_finished().await;
    }

    #[tokio::test]
    async fn undeclared_jobs_keep_the_shared_runtime() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let runtime = Arc::new(JobRuntime::default());
        let (plan, assignments) = replacement_test_plan("orders-shared").await;
        runtime
            .start(
                plan,
                assignments,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .expect("undeclared start succeeds");
        {
            let tasks = runtime.tasks.lock().await;
            let task = tasks.get("orders-shared").expect("registered");
            assert!(
                task.dedicated_runtime.is_none(),
                "undeclared jobs run on the shared runtime"
            );
        }
        runtime.stop("orders-shared", 1).await.unwrap();
        let _ = runtime.take_finished().await;
    }

    /// Partial data-plane TLS configuration fails closed (startup error),
    /// never a silent plaintext fallback.
    #[tokio::test]
    async fn partial_data_plane_tls_configuration_fails_closed() {
        let _guard = ENV_LOCK.lock().await;
        unsafe { std::env::set_var("ARKFLOW_DATA_PLANE_TLS_CERT", "/nonexistent") };
        unsafe { std::env::remove_var("ARKFLOW_DATA_PLANE_TLS_KEY") };
        unsafe { std::env::remove_var("ARKFLOW_DATA_PLANE_TLS_CA") };
        let error = match data_plane_tls_from_env() {
            Err(error) => error,
            Ok(_) => panic!("partial TLS configuration must fail closed"),
        };
        assert!(error.contains("must be set together"), "{error}");
        unsafe { std::env::remove_var("ARKFLOW_DATA_PLANE_TLS_CERT") };
        // Fully absent stays optional (plaintext default).
        assert!(data_plane_tls_from_env().unwrap().is_none());
    }

    /// Complete-but-unreadable TLS material and a valid self-signed set:
    /// the read errors surface verbatim, and real PEM material loads into a
    /// usable mTLS configuration.
    #[tokio::test]
    async fn complete_tls_material_reads_files_and_loads_pem() {
        let _guard = ENV_LOCK.lock().await;
        let missing = format!("/nonexistent-tls-{}", std::process::id());
        unsafe {
            std::env::set_var("ARKFLOW_DATA_PLANE_TLS_CERT", &missing);
            std::env::set_var("ARKFLOW_DATA_PLANE_TLS_KEY", &missing);
            std::env::set_var("ARKFLOW_DATA_PLANE_TLS_CA", &missing);
        }
        let error = match data_plane_tls_from_env() {
            Err(error) => error,
            Ok(_) => panic!("unreadable TLS material must fail closed"),
        };
        assert!(
            error.contains("could not be read"),
            "unreadable material must fail with the file error: {error}"
        );

        let certificate = rcgen::generate_simple_self_signed(vec!["node-a".to_string()]).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let cert_path = directory.path().join("cert.pem");
        let key_path = directory.path().join("key.pem");
        let ca_path = directory.path().join("ca.pem");
        std::fs::write(&cert_path, certificate.cert.pem()).unwrap();
        std::fs::write(&key_path, certificate.key_pair.serialize_pem()).unwrap();
        std::fs::write(&ca_path, certificate.cert.pem()).unwrap();
        unsafe {
            std::env::set_var("ARKFLOW_DATA_PLANE_TLS_CERT", &cert_path);
            std::env::set_var("ARKFLOW_DATA_PLANE_TLS_KEY", &key_path);
            std::env::set_var("ARKFLOW_DATA_PLANE_TLS_CA", &ca_path);
        }
        let tls = data_plane_tls_from_env().expect("complete self-signed material must load");
        assert!(tls.is_some(), "a complete set must produce a TLS config");

        // Readable but non-PEM material is rejected by the parser.
        std::fs::write(&cert_path, "this is not a certificate").unwrap();
        let error = match data_plane_tls_from_env() {
            Err(error) => error,
            Ok(_) => panic!("garbage PEM material must be rejected"),
        };
        assert!(
            error.contains("data-plane TLS") && error.contains(":"),
            "the rejection names the offending material: {error}"
        );
        unsafe {
            std::env::remove_var("ARKFLOW_DATA_PLANE_TLS_CERT");
            std::env::remove_var("ARKFLOW_DATA_PLANE_TLS_KEY");
            std::env::remove_var("ARKFLOW_DATA_PLANE_TLS_CA");
        }
    }

    /// Regression for the Drop guard: a JobTask carrying a dedicated
    /// runtime can be dropped WITHOUT the explicit retirement path — the
    /// guard must park the shutdown on the blocking pool instead of
    /// dropping the Arc<Runtime> inside this async context (which panics).
    #[tokio::test]
    async fn dropping_a_task_with_a_dedicated_runtime_never_panics() {
        let dedicated = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("arkflow-job-dropguard-test")
            .enable_all()
            .build()
            .unwrap();
        let runtime = Arc::new(JobRuntime::default());
        runtime.tasks.lock().await.insert(
            "orders-dropguard".to_string(),
            JobTask {
                generation: 1,
                ephemeral_state: false,
                recovery_required: false,
                cancellation: CancellationToken::new(),
                assignments: Vec::new(),
                dedicated_runtime: Some(Arc::new(dedicated)),
                watermark_partitions: BTreeMap::new(),
                state: Arc::new(
                    arkflow_core::state::InMemoryStateBackend::new(1)
                        .expect("in-memory test backend"),
                ),
                checkpoint_store_uri: None,
                kernel: None,
                handle: tokio::spawn(async { Ok(()) }),
            },
        );
        // Drop WITHOUT calling any stop path — must not panic.
        let dropped = runtime.tasks.lock().await.remove("orders-dropguard");
        drop(dropped);
        // Give the parked shutdown a moment, then prove the runtime still
        // serves other work.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(runtime.tasks.lock().await.is_empty());
    }

    // =====================================================================
    // Coverage additions: object-store guards, recovery validation, runtime
    // preconditions, command settlement, and full Hub/Agent sessions.
    // =====================================================================

    /// Event-driven bounded wait (no fixed sleeps on the hot path).
    async fn wait_for<F, Fut>(budget: Duration, mut condition: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        tokio::time::timeout(budget, async {
            loop {
                if condition().await {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("condition timed out")
    }

    /// A unique scratch directory for redb state backends: the backend holds
    /// an exclusive file lock per path and every test shares one process.
    fn unique_state_dir(tag: &str) -> std::path::PathBuf {
        static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "arkflow-agent-test-{tag}-{}-{sequence}",
            std::process::id()
        ))
    }

    /// A stateless source→sink Job spec value with an optional checkpoint
    /// object-store URI (processing time, `generate` input).
    fn source_sink_spec_value(
        job_id: &str,
        input_type: &str,
        input_config: serde_json::Value,
        time: serde_json::Value,
        checkpoint_uri: Option<String>,
    ) -> serde_json::Value {
        let mut value = serde_json::json!({
            "id": job_id,
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{
                "operator_id": "source",
                "input_type": input_type,
                "config": input_config,
                "time": time
            }],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        });
        if let Some(uri) = checkpoint_uri {
            value["checkpoint"] =
                serde_json::json!({"object_store_uri": uri, "interval_ms": 60000, "retention": 3});
            // Job validation requires a state specification alongside a
            // checkpoint policy; a durable rooted one keeps this spec legal
            // without making the source→sink pair stateful.
            value["state"] = serde_json::json!({
                "backend": "embedded_kv",
                "durability": "durable",
                "root": unique_state_dir(job_id).display().to_string(),
                "format_version": 1
            });
        }
        value
    }

    fn processing_time() -> serde_json::Value {
        serde_json::json!({"mode": "processing_time"})
    }

    /// A sealed manifest whose task attempts cover exactly the plan's tasks.
    fn plan_manifest(
        plan: &JobPlan,
        checkpoint_id: &str,
        state_snapshots: Vec<StateSnapshotRef>,
    ) -> arkflow_core::checkpoint::CheckpointManifest {
        let mut manifest = arkflow_core::checkpoint::CheckpointManifest {
            checkpoint_id: checkpoint_id.to_owned(),
            job_id: plan.spec.id.clone(),
            job_version: plan.spec.version,
            generation: 1,
            task_attempts: plan
                .tasks
                .iter()
                .map(|task| arkflow_core::checkpoint::TaskAttemptSnapshot {
                    task_id: task.id.clone(),
                    attempt_id: format!("{}:node-a:0", task.id),
                    node_id: "node-a".into(),
                })
                .collect(),
            source_positions: Vec::new(),
            watermarks_ms: Default::default(),
            watermark_partitions: Default::default(),
            in_flight_barrier: arkflow_core::checkpoint::CheckpointBarrier {
                checkpoint_id: checkpoint_id.to_owned(),
                generation: 1,
                trace_context: None,
            },
            state_snapshots,
            format_version: plan
                .spec
                .state
                .as_ref()
                .map(|state| state.format_version)
                .unwrap_or(1),
            checksum: 0,
        };
        manifest.seal();
        manifest
    }

    /// Write one state snapshot per planned task (entries supplied by
    /// `entries_for`) plus the sealed manifest at the canonical key.
    fn write_full_artifact(
        plan: &JobPlan,
        store_uri: &str,
        checkpoint_id: &str,
        entries_for: impl Fn(&str) -> Vec<arkflow_core::state::StateEntry>,
    ) -> CheckpointRepository<SharedCheckpointStore> {
        let repository =
            CheckpointRepository::new(SharedCheckpointStore::from_uri(store_uri).unwrap());
        let format_version = plan
            .spec
            .state
            .as_ref()
            .map(|state| state.format_version)
            .unwrap_or(1);
        let mut snapshots = Vec::new();
        for task in &plan.tasks {
            let snapshot =
                arkflow_core::state::StateSnapshot::new(format_version, entries_for(&task.id));
            let mut reference = repository
                .write_state_snapshot(checkpoint_id, &snapshot)
                .unwrap();
            reference.task_id = task.id.clone();
            snapshots.push(reference);
        }
        let manifest = plan_manifest(plan, checkpoint_id, snapshots);
        repository
            .write_manifest(
                &manifest,
                RecoveryArtifactKind::Checkpoint,
                arkflow_core::checkpoint::recovery_manifest_key(
                    RecoveryArtifactKind::Checkpoint,
                    checkpoint_id,
                ),
            )
            .unwrap();
        repository
    }

    /// Write a sealed manifest straight through the store, bypassing the
    /// repository's completeness checks — exactly what a foreign or older
    /// writer could have left behind.
    fn put_manifest_direct(
        store_uri: &str,
        key: &str,
        manifest: &arkflow_core::checkpoint::CheckpointManifest,
    ) {
        SharedCheckpointStore::from_uri(store_uri)
            .unwrap()
            .put(key, &serde_json::to_vec(manifest).unwrap())
            .unwrap();
    }

    #[test]
    fn checkpoint_store_rejects_invalid_keys_and_reads_missing_as_absent() {
        let directory = tempfile::tempdir().unwrap();
        let uri = Url::from_directory_path(directory.path()).unwrap();
        let store = SharedCheckpointStore::from_uri(uri.as_str()).unwrap();
        assert!(store.put("", b"x").is_err(), "empty key");
        assert!(store.put("/absolute", b"x").is_err(), "leading slash");
        assert!(store.put("a/../b", b"x").is_err(), "parent traversal");
        assert_eq!(store.get("missing/key").unwrap(), None);
    }

    #[test]
    fn checkpoint_store_keys_without_a_prefix_stay_rooted() {
        let store = SharedCheckpointStore::from_uri("memory://").unwrap();
        store.put("cp/root-key", b"payload").unwrap();
        assert_eq!(
            store.get("cp/root-key").unwrap().as_deref(),
            Some(b"payload".as_slice())
        );
    }

    #[test]
    fn checkpoint_store_surfaces_backend_errors() {
        // A file:// store rooted at an existing FILE cannot create objects.
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("not-a-directory");
        std::fs::write(&file, b"x").unwrap();
        let uri = Url::from_file_path(&file).unwrap();
        let store = SharedCheckpointStore::from_uri(uri.as_str()).unwrap();
        assert!(store.put("key", b"value").is_err());
    }

    #[test]
    fn recovery_artifact_distinguishes_savepoints() {
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(source_sink_spec_value(
            "artifact-kinds",
            "generate",
            serde_json::json!({"context": "x", "interval": "10ms"}),
            processing_time(),
            Some("memory://artifact-kinds".into()),
        ))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let checkpoint = recovery_artifact(&plan, "cp-1", false).unwrap();
        let savepoint = recovery_artifact(&plan, "sp-1", true).unwrap();
        assert!(matches!(checkpoint.kind, RecoveryArtifactKind::Checkpoint));
        assert!(matches!(savepoint.kind, RecoveryArtifactKind::Savepoint));
        assert!(checkpoint.manifest_key.starts_with("checkpoints/"));
        assert!(savepoint.manifest_key.starts_with("savepoints/"));
    }

    #[test]
    fn recovery_manifest_rejects_checkpoint_id_mismatch() {
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(source_sink_spec_value(
            "manifest-mismatch",
            "generate",
            serde_json::json!({"context": "x", "interval": "10ms"}),
            processing_time(),
            Some("memory://manifest-mismatch".into()),
        ))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let manifest = plan_manifest(&plan, "cp-real", Vec::new());
        let error = validate_recovery_manifest(&plan, "cp-fake", 1, &manifest, false).unwrap_err();
        assert!(
            error.contains("does not match the dispatched checkpoint"),
            "{error}"
        );
    }

    #[test]
    fn rescale_manifests_with_duplicate_task_entries_fail_closed() {
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(source_sink_spec_value(
            "manifest-duplicates",
            "generate",
            serde_json::json!({"context": "x", "interval": "10ms"}),
            processing_time(),
            Some("memory://manifest-duplicates".into()),
        ))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let mut manifest = plan_manifest(&plan, "cp-dup", Vec::new());
        let first = manifest.task_attempts[0].clone();
        manifest.task_attempts.push(first);
        manifest.seal();
        let error = validate_recovery_manifest(&plan, "cp-dup", 1, &manifest, true).unwrap_err();
        assert!(
            error.contains("duplicate task entries"),
            "a corrupted rescale seal must be rejected: {error}"
        );
    }

    #[test]
    fn recovery_snapshot_validation_enforces_task_sets_and_namespaces() {
        let store = tempfile::tempdir().unwrap();
        let uri = format!("file://{}", store.path().display());
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(source_sink_spec_value(
            "snapshot-validation",
            "generate",
            serde_json::json!({"context": "x", "interval": "10ms"}),
            processing_time(),
            Some(uri.clone()),
        ))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let namespace = arkflow_core::job::effective_state_namespace(
            &plan.spec.id,
            plan.spec.state.as_ref(),
            "source",
            "source-0",
        );
        let foreign = arkflow_core::job::effective_state_namespace(
            &arkflow_core::job::JobId::new("another-job").unwrap(),
            plan.spec.state.as_ref(),
            "source",
            "source-0",
        );
        let repository = write_full_artifact(&plan, &uri, "cp-snap", |task_id| {
            if task_id == "source-0" {
                vec![arkflow_core::state::StateEntry {
                    namespace: namespace.clone(),
                    key: b"utf8:k".to_vec(),
                    value: b"v".to_vec(),
                    expires_at_ms: None,
                }]
            } else {
                Vec::new()
            }
        });
        let artifact = recovery_artifact(&plan, "cp-snap", false).unwrap();
        let manifest = repository.read_manifest(&artifact).unwrap();
        assert!(
            validate_recovery_snapshots(&plan, &repository, &manifest, false).is_ok(),
            "a complete, namespace-valid snapshot set restores"
        );

        // Duplicated snapshot references are a corrupted seal even under
        // rescale (the task set legitimately differs there).
        let mut duplicated = manifest.clone();
        let first = duplicated.state_snapshots[0].clone();
        duplicated.state_snapshots.push(first);
        assert!(validate_recovery_snapshots(&plan, &repository, &duplicated, true).is_err());

        // Without rescale, an incomplete snapshot task set is incompatible.
        let mut partial = manifest.clone();
        partial.state_snapshots.pop();
        assert!(validate_recovery_snapshots(&plan, &repository, &partial, false).is_err());

        // An entry outside the Job's state namespace prefix is rejected.
        let rogue = write_full_artifact(&plan, &uri, "cp-rogue", |task_id| {
            if task_id == "source-0" {
                vec![arkflow_core::state::StateEntry {
                    namespace: foreign.clone(),
                    key: b"utf8:k".to_vec(),
                    value: b"v".to_vec(),
                    expires_at_ms: None,
                }]
            } else {
                Vec::new()
            }
        });
        let rogue_read = rogue
            .read_manifest(&recovery_artifact(&plan, "cp-rogue", false).unwrap())
            .unwrap();
        assert!(
            validate_recovery_snapshots(&plan, &rogue, &rogue_read, false).is_err(),
            "foreign namespaces must not restore into this Job"
        );
    }

    #[test]
    fn restore_recovery_state_scopes_snapshots_to_assignments() {
        let store = tempfile::tempdir().unwrap();
        let uri = format!("file://{}", store.path().display());
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(source_sink_spec_value(
            "restore-scope",
            "generate",
            serde_json::json!({"context": "x", "interval": "10ms"}),
            processing_time(),
            Some(uri.clone()),
        ))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let namespaces: BTreeMap<String, String> = plan
            .tasks
            .iter()
            .map(|task| {
                (
                    task.id.clone(),
                    arkflow_core::job::effective_state_namespace(
                        &plan.spec.id,
                        plan.spec.state.as_ref(),
                        &task.operator_id,
                        &task.id,
                    ),
                )
            })
            .collect();
        let repository = write_full_artifact(&plan, &uri, "cp-restore", |task_id| {
            namespaces.get(task_id).map(|namespace| {
                vec![arkflow_core::state::StateEntry {
                    namespace: namespace.clone(),
                    key: format!("utf8:{task_id}").into_bytes(),
                    value: b"owned".to_vec(),
                    expires_at_ms: None,
                }]
            }).unwrap_or_default()
        });
        let manifest = repository
            .read_manifest(&recovery_artifact(&plan, "cp-restore", false).unwrap())
            .unwrap();
        let all_assignments: Vec<TaskAttempt> = plan
            .tasks
            .iter()
            .map(|task| attempt_for(&plan, &task.id, "node-a"))
            .collect();
        let source_only: Vec<TaskAttempt> = vec![attempt_for(&plan, "source-0", "node-a")];

        // Every assigned snapshot's entries land in the backend.
        let backend = RedbStateBackend::open(unique_state_dir("restore-all"), 1).unwrap();
        let state: Arc<dyn StateBackend> = Arc::new(backend);
        restore_recovery_state(&plan, &repository, &manifest, &all_assignments, &state, false)
            .unwrap();
        for (task_id, namespace) in &namespaces {
            assert_eq!(
                state.get(namespace, format!("utf8:{task_id}").as_bytes()).unwrap(),
                Some(b"owned".to_vec()),
                "{task_id} entries must restore"
            );
        }

        // A single assigned snapshot restores without merging.
        let backend = RedbStateBackend::open(unique_state_dir("restore-one"), 1).unwrap();
        let state: Arc<dyn StateBackend> = Arc::new(backend);
        restore_recovery_state(&plan, &repository, &manifest, &source_only, &state, false)
            .unwrap();
        assert_eq!(
            state
                .get(&namespaces["source-0"], b"utf8:source-0")
                .unwrap(),
            Some(b"owned".to_vec())
        );
        assert_eq!(state.get(&namespaces["sink-0"], b"utf8:sink-0").unwrap(), None);

        // No assigned snapshots: a successful no-op.
        let backend = RedbStateBackend::open(unique_state_dir("restore-none"), 1).unwrap();
        let state: Arc<dyn StateBackend> = Arc::new(backend);
        restore_recovery_state(&plan, &repository, &manifest, &[], &state, false).unwrap();
        assert_eq!(state.get(&namespaces["source-0"], b"utf8:source-0").unwrap(), None);
    }

    #[test]
    fn recovery_record_validity_guards_every_rejection() {
        let store = tempfile::tempdir().unwrap();
        let uri = format!("file://{}", store.path().display());
        let spec_value =
            source_sink_spec_value(
                "record-valid",
                "generate",
                serde_json::json!({"context": "x", "interval": "10ms"}),
                processing_time(),
                Some(uri.clone()),
            );
        let plan = JobPlan::compile(serde_json::from_value(spec_value.clone()).unwrap()).unwrap();
        let namespace = arkflow_core::job::effective_state_namespace(
            &plan.spec.id,
            plan.spec.state.as_ref(),
            "source",
            "source-0",
        );
        write_full_artifact(&plan, &uri, "cp-rec", |task_id| {
            if task_id == "source-0" {
                vec![arkflow_core::state::StateEntry {
                    namespace: namespace.clone(),
                    key: b"utf8:k".to_vec(),
                    value: b"v".to_vec(),
                    expires_at_ms: None,
                }]
            } else {
                Vec::new()
            }
        });
        let record = |checkpoint_id: &str, kind: &str| crate::storage::JobCheckpointRecord {
            job_id: "record-valid".into(),
            job_version: 1,
            checkpoint_id: checkpoint_id.into(),
            kind: kind.into(),
            status: "completed".into(),
            manifest_uri: None,
            format_version: 1,
            created_at_ms: 0,
            updated_at_ms: 0,
        };
        let valid: arkflow_core::job::JobSpec = serde_json::from_value(spec_value.clone()).unwrap();
        assert!(
            recovery_record_is_valid(&valid, &record("cp-rec", "checkpoint")),
            "a complete artifact validates"
        );
        assert!(
            !recovery_record_is_valid(&valid, &record("cp-rec", "snapshot")),
            "unknown kinds are rejected"
        );
        assert!(
            !recovery_record_is_valid(&valid, &record("cp-missing", "checkpoint")),
            "a missing manifest is rejected"
        );

        // A spec that cannot compile has no recoverable record.
        let mut broken = spec_value.clone();
        broken["resources"] = serde_json::json!({"cpu_millicores": 0});
        let broken: arkflow_core::job::JobSpec = serde_json::from_value(broken).unwrap();
        assert!(!recovery_record_is_valid(&broken, &record("cp-rec", "checkpoint")));

        // A spec without a checkpoint store cannot be validated either.
        let stateless: arkflow_core::job::JobSpec = serde_json::from_value(source_sink_spec_value(
            "record-valid",
            "generate",
            serde_json::json!({"context": "x", "interval": "10ms"}),
            processing_time(),
            None,
        ))
        .unwrap();
        assert!(!recovery_record_is_valid(&stateless, &record("cp-rec", "checkpoint")));

        // A manifest stored under one checkpoint id but describing another
        // fails the identity check.
        let inner = plan_manifest(&plan, "cp-inner", Vec::new());
        put_manifest_direct(
            &uri,
            &arkflow_core::checkpoint::recovery_manifest_key(
                RecoveryArtifactKind::Checkpoint,
                "cp-outer",
            ),
            &inner,
        );
        assert!(
            !recovery_record_is_valid(&valid, &record("cp-outer", "checkpoint")),
            "an id-mismatched manifest is rejected"
        );

        // Namespace-valid snapshots are re-read as part of validity.
        let foreign_ns = arkflow_core::job::effective_state_namespace(
            &arkflow_core::job::JobId::new("another-job").unwrap(),
            plan.spec.state.as_ref(),
            "source",
            "source-0",
        );
        let rogue_snapshot = arkflow_core::state::StateSnapshot::new(
            1,
            vec![arkflow_core::state::StateEntry {
                namespace: foreign_ns,
                key: b"utf8:k".to_vec(),
                value: b"v".to_vec(),
                expires_at_ms: None,
            }],
        );
        let rogue_repository =
            CheckpointRepository::new(SharedCheckpointStore::from_uri(&uri).unwrap());
        let mut rogue_reference = rogue_repository
            .write_state_snapshot("cp-rogue", &rogue_snapshot)
            .unwrap();
        rogue_reference.task_id = "source-0".into();
        let mut rogue = plan_manifest(&plan, "cp-rogue", vec![rogue_reference]);
        rogue.seal();
        put_manifest_direct(
            &uri,
            &arkflow_core::checkpoint::recovery_manifest_key(
                RecoveryArtifactKind::Checkpoint,
                "cp-rogue",
            ),
            &rogue,
        );
        assert!(
            !recovery_record_is_valid(&valid, &record("cp-rogue", "checkpoint")),
            "foreign-namespace snapshots are rejected"
        );

        // Rescale with an empty task-attempt set passes identity but has no
        // observable task coverage.
        let mut rescale = spec_value.clone();
        rescale["rescale"] = serde_json::json!(true);
        let rescale: arkflow_core::job::JobSpec = serde_json::from_value(rescale).unwrap();
        let repository =
            CheckpointRepository::new(SharedCheckpointStore::from_uri(&uri).unwrap());
        let empty_snapshot = arkflow_core::state::StateSnapshot::new(1, Vec::new());
        let mut empty_reference = repository
            .write_state_snapshot("cp-empty", &empty_snapshot)
            .unwrap();
        empty_reference.task_id = "source-0".into();
        let mut empty_attempts =
            plan_manifest(&plan, "cp-empty", vec![empty_reference]);
        empty_attempts.task_attempts.clear();
        empty_attempts.seal();
        put_manifest_direct(
            &uri,
            &arkflow_core::checkpoint::recovery_manifest_key(
                RecoveryArtifactKind::Checkpoint,
                "cp-empty",
            ),
            &empty_attempts,
        );
        assert!(
            !recovery_record_is_valid(&rescale, &record("cp-empty", "checkpoint")),
            "an artifact without task attempts has no coverage"
        );
    }

    #[test]
    fn node_path_encoding_and_ephemeral_nonces_stay_injective() {
        assert_eq!(safe_path_component(""), "unknown");
        assert_ne!(ephemeral_state_nonce(), ephemeral_state_nonce());
    }

    #[test]
    fn start_marker_persists_atomically_and_removes_cleanly() {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join(".arkflow-started");
        persist_start_marker(&marker).unwrap();
        assert!(marker.is_file());
        assert_eq!(std::fs::read_to_string(&marker).unwrap(), "started\n");
        remove_start_marker(&marker);
        assert!(!marker.exists());
    }

    #[test]
    fn completed_command_cache_evicts_only_the_oldest_entry() {
        let result_for = |command_id: &str| CommandResult {
            command_id: command_id.into(),
            operation_id: "op".into(),
            state: HubOperationState::Succeeded,
            progress: 100,
            error: None,
            correlation_id: None,
            generation: 1,
            observed_generation: None,
            action_id: None,
            failure_class: None,
            config_version_id: None,
            rollout_id: None,
            observed_checkpoint_id: None,
            checkpoint_manifest_uri: None,
            result: None,
        };
        let mut cache = CompletedCommandCache::new(2);
        for command_id in ["cmd-1", "cmd-2", "cmd-3"] {
            remember_completed_command(&mut cache, command_id.into(), result_for(command_id));
        }
        assert!(
            replay_cached_command(&cache, "cmd-1").is_none(),
            "the oldest entry is evicted one at a time"
        );
        assert!(replay_cached_command(&cache, "cmd-2").is_some());
        assert!(replay_cached_command(&cache, "cmd-3").is_some());
        // Re-membering an existing id never evicts a newer one.
        remember_completed_command(&mut cache, "cmd-2".into(), result_for("cmd-2"));
        assert!(replay_cached_command(&cache, "cmd-3").is_some());
    }

    /// The Drop guard's no-runtime fallback: dropped outside every tokio
    /// runtime, the dedicated-runtime shutdown parks on a bare thread.
    #[test]
    fn dropping_a_task_outside_any_runtime_takes_the_bare_thread_path() {
        let task = {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            runtime.block_on(async {
                let dedicated = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .thread_name("arkflow-job-dropguard-sync")
                    .enable_all()
                    .build()
                    .unwrap();
                JobTask {
                    generation: 1,
                    ephemeral_state: false,
                    recovery_required: false,
                    cancellation: CancellationToken::new(),
                    assignments: Vec::new(),
                    dedicated_runtime: Some(Arc::new(dedicated)),
                    watermark_partitions: BTreeMap::new(),
                    state: Arc::new(
                        arkflow_core::state::InMemoryStateBackend::new(1)
                            .expect("in-memory test backend"),
                    ),
                    checkpoint_store_uri: None,
                    kernel: None,
                    handle: tokio::spawn(async { Ok(()) }),
                }
            })
        };
        // Outside every runtime context: must not panic.
        drop(task);
    }

    #[tokio::test]
    async fn metrics_count_ephemeral_and_recovery_required_jobs() {
        let runtime = JobRuntime::default();
        for (job_id, ephemeral, recovery) in
            [("job-eph", true, false), ("job-rec", false, true), ("job-plain", false, false)]
        {
            let state: Arc<dyn StateBackend> = Arc::new(
                arkflow_core::state::InMemoryStateBackend::new(1)
                    .expect("in-memory test backend"),
            );
            runtime.tasks.lock().await.insert(
                job_id.into(),
                JobTask {
                    generation: 1,
                    ephemeral_state: ephemeral,
                    recovery_required: recovery,
                    cancellation: CancellationToken::new(),
                    assignments: Vec::new(),
                    dedicated_runtime: None,
                    watermark_partitions: BTreeMap::new(),
                    state,
                    checkpoint_store_uri: None,
                    kernel: None,
                    handle: tokio::spawn(std::future::pending::<
                        Result<(), arkflow_core::Error>,
                    >()),
                },
            );
        }
        let metrics = runtime.metrics().await;
        assert_eq!(metrics["jobs_total"], 3.0);
        assert_eq!(metrics["jobs_ephemeral_state"], 1.0);
        assert_eq!(metrics["jobs_recovery_required"], 1.0);
        let mut tasks = runtime.tasks.lock().await;
        for task in tasks.values_mut() {
            task.handle.abort();
        }
        tasks.clear();
    }

    #[tokio::test]
    async fn parking_no_observations_is_a_no_op() {
        JobRuntime::default().park_observations(Vec::new()).await;
    }

    // ------------------ JobRuntime preconditions ------------------

    #[tokio::test]
    async fn start_and_stop_reject_command_level_preconditions() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let runtime = Arc::new(JobRuntime::default());
        let (plan, assignments) = replacement_test_plan("orders-guards").await;

        // No assignments at all.
        let error = runtime
            .start(
                plan.clone(),
                Vec::new(),
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(error, "Job command contains no task assignments");

        // The Hub declared recovery required but supplied no checkpoint.
        let error = runtime
            .start(
                plan.clone(),
                assignments.clone(),
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload {
                    recovery_required: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(
            error.contains("requires recovery but no compatible checkpoint"),
            "{error}"
        );

        // A start behind an already-registered newer generation is stale.
        insert_exited_task(&runtime, "orders-guards", 5, Ok(())).await;
        let error = runtime
            .start(
                plan.clone(),
                assignments.clone(),
                3,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(error, "job generation is stale");

        // So is a stop behind a newer generation.
        let error = runtime.stop("orders-guards", 3).await.unwrap_err();
        assert_eq!(error, "job generation is stale");
        let _ = runtime.take_finished().await;
    }

    /// A durable, stateful, checkpointed Job persists its start marker; a
    /// restart at the same generation without a recovery payload must fail
    /// closed on that marker (the state on disk may be mid-mutation).
    #[tokio::test(flavor = "multi_thread")]
    async fn durable_restart_without_recovery_fails_closed_on_the_start_marker() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let state_root = tempfile::tempdir().unwrap();
        let checkpoint_root = tempfile::tempdir().unwrap();
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(serde_json::json!({
            "id": "orders-marker",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "agg", "kind": "map", "stateful": true, "key_field": "key", "config": {"type": "batch", "count": 1, "timeout_ms": 10}},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [
                {"id": "e1", "from": "source", "to": "agg", "partitioned": true},
                {"id": "e2", "from": "agg", "to": "sink"}
            ],
            "sources": [{
                "operator_id": "source",
                "input_type": "generate",
                "config": {"context": "x", "interval": "10ms", "batch_size": 1},
                "time": {"mode": "processing_time"}
            }],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}],
            "state": {
                "backend": "embedded_kv",
                "durability": "durable",
                "root": state_root.path().display().to_string(),
                "format_version": 1
            },
            "checkpoint": {
                "object_store_uri": format!("file://{}", checkpoint_root.path().display()),
                "interval_ms": 60000,
                "retention": 3
            }
        }))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let assignments = plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .unwrap();
        let runtime = Arc::new(JobRuntime::default());
        runtime
            .start(
                plan.clone(),
                assignments.clone(),
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .expect("the durable start persists its marker");
        let marker = durable_recovery_marker(&plan, "node-a", 1).unwrap();
        assert!(marker.is_file(), "the start marker must exist on disk");

        runtime.stop("orders-marker", 1).await.unwrap();
        let _ = runtime.take_finished().await;

        // Stopping does not clear the marker: only a recovery start may run.
        assert!(marker.is_file(), "stopping must not clear the marker");
        let error = runtime
            .start(
                plan,
                assignments,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .unwrap_err();
        assert!(
            error.contains("requires recovery because state marker"),
            "{error}"
        );
        assert!(runtime.tasks.lock().await.is_empty());
    }

    /// A spawn failure must unwind everything the start path registered: the
    /// placeholder task-map entry, the cancellation token, and the marker.
    #[tokio::test(flavor = "multi_thread")]
    async fn spawn_failure_releases_the_placeholder_and_the_start_marker() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let state_root = tempfile::tempdir().unwrap();
        let checkpoint_root = tempfile::tempdir().unwrap();
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(serde_json::json!({
            "id": "orders-spawnfail",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "agg", "kind": "map", "stateful": true, "key_field": "key", "config": {"type": "batch", "count": 1, "timeout_ms": 10}},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [
                {"id": "e1", "from": "source", "to": "agg", "partitioned": true},
                {"id": "e2", "from": "agg", "to": "sink"}
            ],
            "sources": [{
                "operator_id": "source",
                "input_type": "no-such-input",
                "config": {},
                "time": {"mode": "processing_time"}
            }],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}],
            "state": {
                "backend": "embedded_kv",
                "durability": "durable",
                "root": state_root.path().display().to_string(),
                "format_version": 1
            },
            "checkpoint": {
                "object_store_uri": format!("file://{}", checkpoint_root.path().display()),
                "interval_ms": 60000,
                "retention": 3
            }
        }))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let assignments = plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .unwrap();
        let runtime = Arc::new(JobRuntime::default());
        let error = runtime
            .start(
                plan.clone(),
                assignments,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .unwrap_err();
        assert!(
            error.contains("no-such-input") || error.contains("input"),
            "the missing input must surface: {error}"
        );
        assert!(
            runtime.tasks.lock().await.is_empty(),
            "the placeholder entry must be removed"
        );
        let marker = durable_recovery_marker(&plan, "node-a", 1).unwrap();
        assert!(!marker.is_file(), "the start marker must be rolled back");
    }

    /// Ephemeral-state Jobs run against a unique process-temp root and honor
    /// the declared `state.max_bytes` bound.
    #[tokio::test(flavor = "multi_thread")]
    async fn ephemeral_state_jobs_run_against_a_bounded_temp_root() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(serde_json::json!({
            "id": "orders-ephemeral",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "agg", "kind": "map", "stateful": true, "key_field": "key", "config": {"type": "batch", "count": 1, "timeout_ms": 10}},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [
                {"id": "e1", "from": "source", "to": "agg", "partitioned": true},
                {"id": "e2", "from": "agg", "to": "sink"}
            ],
            "sources": [{
                "operator_id": "source",
                "input_type": "generate",
                "config": {"context": "x", "interval": "10ms", "batch_size": 1},
                "time": {"mode": "processing_time"}
            }],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}],
            "state": {
                "backend": "embedded_kv",
                "durability": "ephemeral",
                "format_version": 1,
                "max_bytes": 4096
            }
        }))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let assignments = plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .unwrap();
        let runtime = Arc::new(JobRuntime::default());
        runtime
            .start(
                plan,
                assignments,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .expect("the ephemeral start succeeds");
        let tasks = runtime.tasks.lock().await;
        let task = tasks.get("orders-ephemeral").expect("registered");
        assert!(task.ephemeral_state, "the task carries the ephemeral flag");
        drop(tasks);
        let metrics = runtime.metrics().await;
        assert_eq!(metrics["jobs_ephemeral_state"], 1.0);
        runtime.stop("orders-ephemeral", 1).await.unwrap();
        let _ = runtime.take_finished().await;
    }

    /// A bounded source (generate with `count`) ends its chain normally: the
    /// kernel exits on its own and `take_finished` collects the outcome so
    /// the Hub learns the Job stopped.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_job_that_ends_on_its_own_is_collected_by_take_finished() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let runtime = Arc::new(JobRuntime::default());
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(source_sink_spec_value(
            "orders-bounded",
            "generate",
            serde_json::json!({"context": "x", "interval": "5ms", "count": 2, "batch_size": 1}),
            processing_time(),
            None,
        ))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let assignments = plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .unwrap();
        runtime
            .start(
                plan,
                assignments,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .expect("the bounded start succeeds");
        let finished = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let finished = runtime.take_finished().await;
                if finished.is_empty() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                break finished;
            }
        })
        .await
        .expect("the bounded kernel must exit on its own");
        assert!(
            finished
                .iter()
                .any(|(job_id, generation, _)| job_id == "orders-bounded" && *generation == 1),
            "the finished kernel must be reported: {finished:?}"
        );
        assert!(runtime.tasks.lock().await.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stop_all_cancels_every_registered_task() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let runtime = Arc::new(JobRuntime::default());
        let (plan_a, assignments_a) = replacement_test_plan("orders-stopall-a").await;
        let (plan_b, assignments_b) = replacement_test_plan("orders-stopall-b").await;
        runtime
            .start(
                plan_a,
                assignments_a,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .unwrap();
        runtime
            .start(
                plan_b,
                assignments_b,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .unwrap();
        assert_eq!(runtime.tasks.lock().await.len(), 2);
        runtime.stop_all().await;
        assert!(runtime.tasks.lock().await.is_empty());
        let _ = runtime.take_finished().await;
    }

    /// Checkpoint + aggregate round trip through a real object store,
    /// including the savepoint artifact kind and every aggregation guard.
    #[tokio::test(flavor = "multi_thread")]
    async fn checkpoint_and_aggregate_round_trip_through_the_object_store() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let checkpoint_root = tempfile::tempdir().unwrap();
        let store_uri = format!("file://{}", checkpoint_root.path().display());
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(source_sink_spec_value(
            "orders-checkpoint",
            "generate",
            serde_json::json!({"context": "x", "interval": "10ms", "batch_size": 1}),
            processing_time(),
            Some(store_uri.clone()),
        ))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let planned_task_ids = plan
            .tasks
            .iter()
            .map(|task| task.id.clone())
            .collect::<Vec<_>>();
        let assignments = plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .unwrap();
        let runtime = Arc::new(JobRuntime::default());
        runtime
            .start(
                plan.clone(),
                assignments,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .expect("the checkpointed job starts");

        // Generation mismatches are rejected before any barrier work.
        let node_a = ["node-a".to_string()];
        let error = runtime
            .checkpoint("orders-checkpoint", "cp-1", 9, false, "node-a")
            .await
            .unwrap_err();
        assert_eq!(error, "checkpoint generation does not match running Job");
        let error = runtime
            .aggregate_checkpoint("orders-checkpoint", "cp-1", 9, false, &node_a, &planned_task_ids)
            .await
            .unwrap_err();
        assert_eq!(error, "checkpoint generation does not match running Job");

        // A savepoint writes under the savepoints/ prefix.
        let uri = runtime
            .checkpoint("orders-checkpoint", "sp-1", 1, true, "node-a")
            .await
            .expect("the savepoint barrier completes");
        assert!(
            uri.contains("savepoints/sp-1/"),
            "the savepoint manifest lands under savepoints/: {uri}"
        );

        let planned = planned_task_ids.clone();
        let aggregated = runtime
            .aggregate_checkpoint(
                "orders-checkpoint",
                "sp-1",
                1,
                true,
                &["node-a".to_string()],
                &planned,
            )
            .await
            .expect("the single-node aggregate seals the final manifest");
        assert!(
            aggregated.contains("sp-1"),
            "the aggregate points at the sealed manifest: {aggregated}"
        );

        // Guardrails: no agent manifests, no planned tasks.
        let error = runtime
            .aggregate_checkpoint("orders-checkpoint", "sp-1", 1, true, &[], &planned_task_ids)
            .await
            .unwrap_err();
        assert_eq!(error, "checkpoint has no agent manifests");
        let error = runtime
            .aggregate_checkpoint("orders-checkpoint", "sp-1", 1, true, &node_a, &[])
            .await
            .unwrap_err();
        assert_eq!(error, "checkpoint has no planned task assignments");

        // A second node's manifest that belongs to another job breaks the
        // shared-barrier requirement...
        let repository =
            CheckpointRepository::new(SharedCheckpointStore::from_uri(&store_uri).unwrap());
        let node_a_key = "savepoints/sp-1/manifests/node-a.json".to_string();
        let node_a_artifact = RecoveryArtifact {
            id: "sp-1".into(),
            kind: RecoveryArtifactKind::Savepoint,
            manifest_key: node_a_key.clone(),
            job_version: plan.spec.version,
            format_version: 1,
            created_at_ms: 0,
            status: CheckpointStatus::Completed,
        };
        let node_manifest = repository.read_manifest(&node_a_artifact).unwrap();
        let mut foreign_manifest = node_manifest.clone();
        foreign_manifest.job_id = arkflow_core::job::JobId::new("another-job").unwrap();
        foreign_manifest.seal();
        repository
            .write_manifest(
                &foreign_manifest,
                RecoveryArtifactKind::Savepoint,
                "savepoints/sp-1/manifests/node-b.json".to_string(),
            )
            .unwrap();
        let error = runtime
            .aggregate_checkpoint(
                "orders-checkpoint",
                "sp-1",
                1,
                true,
                &["node-a".to_string(), "node-b".to_string()],
                &planned_task_ids,
            )
            .await
            .unwrap_err();
        assert_eq!(
            error, "checkpoint manifests do not share one job barrier",
            "{error}"
        );

        // ...and a verbatim duplicate breaks task uniqueness.
        let mut same_manifest = node_manifest;
        same_manifest.seal();
        repository
            .write_manifest(
                &same_manifest,
                RecoveryArtifactKind::Savepoint,
                "savepoints/sp-1/manifests/node-b.json".to_string(),
            )
            .unwrap();
        let error = runtime
            .aggregate_checkpoint(
                "orders-checkpoint",
                "sp-1",
                1,
                true,
                &["node-a".to_string(), "node-b".to_string()],
                &planned_task_ids,
            )
            .await
            .unwrap_err();
        assert!(
            error.contains("duplicate task"),
            "duplicate tasks across manifests must fail: {error}"
        );

        runtime.stop("orders-checkpoint", 1).await.unwrap();
        let _ = runtime.take_finished().await;
    }

    /// A checkpoint on a Job without a checkpoint store fails with the
    /// actionable error instead of panicking.
    #[tokio::test(flavor = "multi_thread")]
    async fn checkpoint_without_a_store_uri_fails_with_an_actionable_error() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let runtime = Arc::new(JobRuntime::default());
        let (plan, assignments) = replacement_test_plan("orders-nostore").await;
        runtime
            .start(
                plan,
                assignments,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .unwrap();
        let error = runtime
            .checkpoint("orders-nostore", "cp-1", 1, false, "node-a")
            .await
            .unwrap_err();
        assert_eq!(error, "Job has no checkpoint object_store_uri");
        runtime.stop("orders-nostore", 1).await.unwrap();
        let _ = runtime.take_finished().await;
    }

    /// Recovery start: a complete artifact restores keyed state before the
    /// kernel connects its sources.
    #[tokio::test(flavor = "multi_thread")]
    async fn recovery_start_restores_keyed_state_before_running() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let checkpoint_root = tempfile::tempdir().unwrap();
        let store_uri = format!("file://{}", checkpoint_root.path().display());
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(source_sink_spec_value(
            "orders-recover",
            "generate",
            serde_json::json!({"context": "x", "interval": "10ms", "batch_size": 1}),
            processing_time(),
            Some(store_uri.clone()),
        ))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let namespace = arkflow_core::job::effective_state_namespace(
            &plan.spec.id,
            plan.spec.state.as_ref(),
            "source",
            "source-0",
        );
        write_full_artifact(&plan, &store_uri, "cp-recover", |task_id| {
            if task_id == "source-0" {
                vec![arkflow_core::state::StateEntry {
                    namespace: namespace.clone(),
                    key: b"utf8:counter".to_vec(),
                    value: b"42".to_vec(),
                    expires_at_ms: None,
                }]
            } else {
                Vec::new()
            }
        });
        let assignments = plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .unwrap();
        let runtime = Arc::new(JobRuntime::default());
        runtime
            .start(
                plan.clone(),
                assignments,
                1,
                Some("cp-recover".into()),
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .expect("the recovery start succeeds");
        {
            let tasks = runtime.tasks.lock().await;
            let task = tasks.get("orders-recover").expect("registered");
            assert_eq!(
                task.state.get(&namespace, b"utf8:counter").unwrap(),
                Some(b"42".to_vec()),
                "the checkpointed entry must be restored into the node backend"
            );
        }
        runtime.stop("orders-recover", 1).await.unwrap();
        let _ = runtime.take_finished().await;
    }

    /// Broken artifacts fail closed with distinct, actionable errors and
    /// leave nothing registered.
    #[tokio::test(flavor = "multi_thread")]
    async fn recovery_start_fails_closed_on_broken_artifacts() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let checkpoint_root = tempfile::tempdir().unwrap();
        let store_uri = format!("file://{}", checkpoint_root.path().display());
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(source_sink_spec_value(
            "orders-broken-recovery",
            "generate",
            serde_json::json!({"context": "x", "interval": "10ms", "batch_size": 1}),
            processing_time(),
            Some(store_uri.clone()),
        ))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let assignments = plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .unwrap();
        let runtime = Arc::new(JobRuntime::default());

        // (a) the artifact does not exist at all
        let error = runtime
            .start(
                plan.clone(),
                assignments.clone(),
                1,
                Some("no-such-checkpoint".into()),
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .unwrap_err();
        assert!(!error.is_empty(), "{error}");
        assert!(runtime.tasks.lock().await.is_empty());

        // (b) the manifest exists under the requested key but belongs to a
        // different checkpoint id
        let repository =
            CheckpointRepository::new(SharedCheckpointStore::from_uri(&store_uri).unwrap());
        let mismatch_snapshot = arkflow_core::state::StateSnapshot::new(1, Vec::new());
        let mut mismatch_reference = repository
            .write_state_snapshot("cp-fake", &mismatch_snapshot)
            .unwrap();
        mismatch_reference.task_id = "source-0".into();
        let mismatched = plan_manifest(&plan, "cp-real", vec![mismatch_reference]);
        put_manifest_direct(
            &store_uri,
            &arkflow_core::checkpoint::recovery_manifest_key(
                RecoveryArtifactKind::Checkpoint,
                "cp-fake",
            ),
            &mismatched,
        );
        let error = runtime
            .start(
                plan.clone(),
                assignments.clone(),
                1,
                Some("cp-fake".into()),
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .unwrap_err();
        assert!(
            error.contains("does not match the dispatched checkpoint"),
            "{error}"
        );
        assert!(runtime.tasks.lock().await.is_empty());

        // (c) the attempts cover the plan but the snapshot task set does not
        let partial_snapshots = {
            let snapshot = arkflow_core::state::StateSnapshot::new(1, Vec::new());
            let mut reference = repository
                .write_state_snapshot("cp-partial", &snapshot)
                .unwrap();
            reference.task_id = "source-0".into();
            vec![reference]
        };
        let partial = plan_manifest(&plan, "cp-partial", partial_snapshots);
        put_manifest_direct(
            &store_uri,
            &arkflow_core::checkpoint::recovery_manifest_key(
                RecoveryArtifactKind::Checkpoint,
                "cp-partial",
            ),
            &partial,
        );
        let error = runtime
            .start(
                plan.clone(),
                assignments,
                1,
                Some("cp-partial".into()),
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .unwrap_err();
        assert!(
            error.contains("incompatible") || error.contains("does not match"),
            "an incomplete snapshot set must fail closed: {error}"
        );
        assert!(runtime.tasks.lock().await.is_empty());
    }

    /// A recovery start whose reconnection fails must release the prepared
    /// inputs so a retry can reconnect.
    #[tokio::test(flavor = "multi_thread")]
    async fn recovery_start_releases_inputs_when_reconnect_fails() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let checkpoint_root = tempfile::tempdir().unwrap();
        let store_uri = format!("file://{}", checkpoint_root.path().display());
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(source_sink_spec_value(
            "orders-ws-recovery",
            "websocket",
            serde_json::json!({"url": "ws://127.0.0.1:1/"}),
            processing_time(),
            Some(store_uri.clone()),
        ))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        write_full_artifact(&plan, &store_uri, "cp-ws", |_| Vec::new());
        let assignments = plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .unwrap();
        let runtime = Arc::new(JobRuntime::default());
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            runtime.start(
                plan,
                assignments,
                1,
                Some("cp-ws".into()),
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            ),
        )
        .await
        .expect("the failing reconnect must not hang");
        assert!(result.is_err(), "a dead websocket endpoint must fail recovery");
        assert!(runtime.tasks.lock().await.is_empty());
    }

    /// Event-time sources install watermark gates (a shared tracker per
    /// watermark group) before the kernel consumes anything, and a recovery
    /// start re-installs the checkpointed per-partition and per-task
    /// watermarks into those gates.
    #[tokio::test(flavor = "multi_thread")]
    async fn event_time_recovery_reinstalls_watermark_gates() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let checkpoint_root = tempfile::tempdir().unwrap();
        let store_uri = format!("file://{}", checkpoint_root.path().display());
        let event_time = || {
            serde_json::json!({
                "mode": "event_time",
                "timestamp_field": "value",
                "watermark": {"strategy": "bounded_out_of_orderness", "out_of_orderness_ms": 50}
            })
        };
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(serde_json::json!({
            "id": "orders-eventtime",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "extra", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [
                {"id": "e1", "from": "source", "to": "sink"},
                {"id": "e2", "from": "extra", "to": "sink"}
            ],
            "sources": [
                {
                    "operator_id": "source",
                    "input_type": "generate",
                    "config": {"context": "x", "interval": "10ms", "count": 2, "batch_size": 1},
                    "time": event_time()
                },
                {
                    "operator_id": "extra",
                    "input_type": "generate",
                    "config": {"context": "y", "interval": "10ms", "count": 2, "batch_size": 1},
                    "time": event_time()
                }
            ],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}],
            "state": {
                "backend": "embedded_kv",
                "durability": "durable",
                "root": unique_state_dir("orders-eventtime").display().to_string(),
                "format_version": 1
            },
            "checkpoint": {"object_store_uri": store_uri, "interval_ms": 60000, "retention": 3}
        }))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let repository =
            CheckpointRepository::new(SharedCheckpointStore::from_uri(&store_uri).unwrap());
        let mut snapshots = Vec::new();
        for task in &plan.tasks {
            let snapshot = arkflow_core::state::StateSnapshot::new(1, Vec::new());
            let mut reference = repository
                .write_state_snapshot("cp-eventtime", &snapshot)
                .unwrap();
            reference.task_id = task.id.clone();
            snapshots.push(reference);
        }
        let mut manifest = plan_manifest(&plan, "cp-eventtime", snapshots);
        // One gated source restores its physical partition watermark, the
        // other its task-level watermark (no partition progress recorded).
        manifest.watermark_partitions.insert(
            "source-0".into(),
            vec![arkflow_core::checkpoint::WatermarkPosition::new(
                Some("orders".into()),
                0,
                9_000,
            )],
        );
        manifest.watermarks_ms.insert("extra-0".into(), 4_500);
        manifest.seal();
        repository
            .write_manifest(
                &manifest,
                RecoveryArtifactKind::Checkpoint,
                arkflow_core::checkpoint::recovery_manifest_key(
                    RecoveryArtifactKind::Checkpoint,
                    "cp-eventtime",
                ),
            )
            .unwrap();
        let assignments = plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .unwrap();
        let runtime = Arc::new(JobRuntime::default());
        runtime
            .start(
                plan,
                assignments,
                1,
                Some("cp-eventtime".into()),
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .expect("the event-time recovery start succeeds");
        assert!(runtime.tasks.lock().await.contains_key("orders-eventtime"));
        runtime.stop("orders-eventtime", 1).await.unwrap();
        let _ = runtime.take_finished().await;
    }

    /// Generic operators (map/filter/udf) build through the registry
    /// adapter; a config without `type` fails with the operator's identity.
    #[tokio::test(flavor = "multi_thread")]
    async fn processor_operators_build_through_the_registry_adapter() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let runtime = Arc::new(JobRuntime::default());
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(serde_json::json!({
            "id": "orders-processor",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "shaper", "kind": "map", "config": {"type": "batch", "count": 1, "timeout_ms": 10}},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [
                {"id": "e1", "from": "source", "to": "shaper"},
                {"id": "e2", "from": "shaper", "to": "sink"}
            ],
            "sources": [{
                "operator_id": "source",
                "input_type": "generate",
                "config": {"context": "x", "interval": "10ms", "count": 3, "batch_size": 1},
                "time": {"mode": "processing_time"}
            }],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        }))
        .unwrap();
        let mut plan = JobPlan::compile(spec).unwrap();
        let assignments = plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .unwrap();
        runtime
            .start(
                plan.clone(),
                assignments.clone(),
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .expect("the processor job starts");
        assert!(runtime.tasks.lock().await.contains_key("orders-processor"));
        runtime.stop("orders-processor", 1).await.unwrap();
        let _ = runtime.take_finished().await;

        // A processor without config.type cannot build.
        let mut broken = serde_json::to_value(&plan.spec).unwrap();
        broken["operators"][1]["config"] = serde_json::json!({});
        let broken: arkflow_core::job::JobSpec = serde_json::from_value(broken).unwrap();
        plan = JobPlan::compile(broken).unwrap();
        let error = runtime
            .start(
                plan,
                assignments,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .unwrap_err();
        assert!(
            error.contains("requires config.type"),
            "the operator identity must surface: {error}"
        );
    }

    /// A split payload with peer data-plane addresses builds the remote-edge
    /// context: malformed addresses are skipped and a missing full task map
    /// degrades to the local assignment map.
    #[tokio::test(flavor = "multi_thread")]
    async fn split_payloads_with_peer_addresses_wire_remote_context() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let mut runtime = JobRuntime::default();
        let credentials =
            arkflow_core::executor::remote::DataPlaneCredentials::new("node-a", "secret")
                .expect("test credentials");
        let manager = arkflow_core::executor::remote::NetworkManager::with_config(
            arkflow_core::executor::remote::NetworkManagerConfig {
                credentials: Some(credentials),
                channel_capacity: 16,
                ..Default::default()
            },
        )
        .expect("data plane manager builds");
        manager.spawn();
        runtime.data_plane = Some(manager.clone());
        let (plan, assignments) = replacement_test_plan("orders-remotectx").await;
        let split = SplitPlacementPayload {
            task_nodes: None,
            node_data_ports: BTreeMap::from([
                ("node-a".into(), "127.0.0.1:39601".into()),
                ("bad-node".into(), "not-a-socket-address".into()),
            ]),
            recovery_required: false,
        };
        runtime
            .start(
                plan,
                assignments,
                1,
                None,
                false,
                "node-a",
                &split,
            )
            .await
            .expect("the colocated split start succeeds");
        assert!(runtime.tasks.lock().await.contains_key("orders-remotectx"));
        runtime.stop("orders-remotectx", 1).await.unwrap();
        let _ = runtime.take_finished().await;
        manager.shutdown();
    }

    // ------------------ command settlement ------------------

    /// A minimal always-200 Hub so `execute_command` can deliver results.
    async fn stub_hub_server() -> (String, CancellationToken, tokio::task::JoinHandle<()>) {
        async fn always_ok() -> axum::http::StatusCode {
            axum::http::StatusCode::OK
        }
        let app = axum::Router::new().fallback(always_ok);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let shutdown = cancellation.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .unwrap();
        });
        (format!("http://{address}"), cancellation, task)
    }

    fn test_command(
        operation: &str,
        resource_id: &str,
        generation: u64,
        payload: Option<serde_json::Value>,
        expires_at_ms: u64,
    ) -> AgentCommand {
        AgentCommand {
            id: format!("cmd-{operation}-{resource_id}"),
            operation_id: format!("op-{operation}-{resource_id}"),
            node_id: "node-a".into(),
            operation: operation.into(),
            resource_id: resource_id.into(),
            expires_at_ms,
            generation,
            action_id: Some(format!("action-{operation}")),
            config_version_id: None,
            attempt_id: None,
            rollout_id: None,
            correlation_id: Some("corr-1".into()),
            payload,
            required_capabilities: Vec::new(),
        }
    }

    fn test_node_config(hub_url: &str) -> NodeAgentConfig {
        NodeAgentConfig {
            hub_url: hub_url.into(),
            hub_urls: vec![hub_url.into()],
            api_prefix: "/api/v1".into(),
            node_id: "node-a".into(),
            node_token: "token".into(),
            boot_id: "boot-test".into(),
            heartbeat_interval: Duration::from_secs(5),
            report_interval: Duration::from_secs(5),
            poll_interval: Duration::from_secs(5),
            data_port: None,
            data_host: None,
        }
    }

    fn test_auth() -> AgentAuth {
        AgentAuth {
            node_id: "node-a".into(),
            session_token: "stub-session".into(),
        }
    }

    fn generate_drop_stream(id: &str) -> arkflow_core::stream::StreamConfig {
        arkflow_core::stream::StreamConfig {
            id: Some(id.to_string()),
            input: InputConfig {
                input_type: "generate".into(),
                name: None,
                codec: None,
                config: Some(serde_json::json!({
                    "context": "agent-test",
                    "interval": "50ms",
                    "batch_size": 1
                })),
            },
            pipeline: arkflow_core::pipeline::PipelineConfig {
                thread_num: 1,
                processors: Vec::new(),
            },
            output: OutputConfig {
                output_type: "drop".into(),
                name: None,
                codec: None,
                config: None,
            },
            error_output: None,
            buffer: None,
            durability: None,
            state: None,
            temporary: None,
        }
    }

    fn engine_config_with_stream(stream_id: &str) -> EngineConfig {
        EngineConfig {
            streams: vec![generate_drop_stream(stream_id)],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig::default(),
        }
    }

    /// Every non-kernel command shape settles through the stub Hub with the
    /// state, error and failure class the Hub's retry machinery expects.
    #[tokio::test(flavor = "multi_thread")]
    async fn execute_command_settles_every_command_shape_against_a_stub_hub() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let (hub_url, hub_cancel, hub_task) = stub_hub_server().await;
        let client = build_agent_client(&hub_url).unwrap();
        let config = test_node_config(&hub_url);
        let auth = test_auth();
        let engine_config = engine_config_with_stream("orders-stream");
        let cp = ControlPlane::new(engine_config.clone(), RuntimeManager::new());
        cp.runtime_manager()
            .replace_config(&engine_config)
            .await
            .expect("the stream registers");
        let runtime = Arc::new(JobRuntime::default());
        let live_deadline = now_ms().saturating_add(60_000);

        // Expired before execution.
        let command = test_command("restart", "orders-stream", 1, None, now_ms() - 1_000);
        let result = execute_command(&client, &cp, &config, &auth, &command, &runtime)
            .await
            .unwrap();
        assert_eq!(result.state, HubOperationState::TimedOut);
        assert_eq!(result.failure_class.as_deref(), Some("temporary_execution"));
        assert!(result.error.is_some());

        // A Job command behind the running generation is superseded.
        insert_exited_task(&runtime, "job-stale", 5, Ok(())).await;
        let command = test_command("job_stop", "job-stale", 3, None, live_deadline);
        let result = execute_command(&client, &cp, &config, &auth, &command, &runtime)
            .await
            .unwrap();
        assert_eq!(result.state, HubOperationState::Superseded);
        assert_eq!(result.observed_generation, Some(5));
        assert_eq!(result.failure_class.as_deref(), Some("stale_generation"));

        // A failing Job command carries the observed checkpoint id.
        let command = test_command(
            "job_checkpoint",
            "ghost-job",
            1,
            Some(serde_json::json!({"checkpoint_id": "cp-9"})),
            live_deadline,
        );
        let result = execute_command(&client, &cp, &config, &auth, &command, &runtime)
            .await
            .unwrap();
        assert_eq!(result.state, HubOperationState::Failed);
        assert_eq!(result.observed_checkpoint_id.as_deref(), Some("cp-9"));
        assert_eq!(result.failure_class.as_deref(), Some("permanent_execution"));
        assert!(result.checkpoint_manifest_uri.is_none());

        // A Job restart through the command path stops and starts the kernel.
        let (plan, assignments) = replacement_test_plan("orders-cmd").await;
        let payload = serde_json::json!({
            "plan": serde_json::to_value(&plan).unwrap(),
            "assignments": serde_json::to_value(&assignments).unwrap(),
        });
        let command = test_command("job_restart", "orders-cmd", 1, Some(payload), live_deadline);
        let result = execute_command(&client, &cp, &config, &auth, &command, &runtime)
            .await
            .unwrap();
        assert_eq!(result.state, HubOperationState::Succeeded);
        assert_eq!(result.observed_generation, Some(1));
        assert!(result.error.is_none());
        assert!(runtime.tasks.lock().await.contains_key("orders-cmd"));

        // Validate/diff configuration reports.
        let candidate = arkflow_core::configuration::ConfigCandidate {
            format: arkflow_core::configuration::ConfigFormat::Yaml,
            content: "streams: []\n".into(),
            content_verbatim: None,
        };
        let command = test_command(
            "validate_configuration",
            "config",
            1,
            Some(serde_json::to_value(&candidate).unwrap()),
            live_deadline,
        );
        let result = execute_command(&client, &cp, &config, &auth, &command, &runtime)
            .await
            .unwrap();
        assert_eq!(result.state, HubOperationState::Succeeded);
        let report = result.result.expect("the validation report rides the payload");
        assert_eq!(report["valid"], serde_json::json!(true));

        let invalid = arkflow_core::configuration::ConfigCandidate {
            format: arkflow_core::configuration::ConfigFormat::Yaml,
            content: "streams: [ { broken\n".into(),
            content_verbatim: None,
        };
        let command = test_command(
            "validate_configuration",
            "config",
            1,
            Some(serde_json::to_value(&invalid).unwrap()),
            live_deadline,
        );
        let result = execute_command(&client, &cp, &config, &auth, &command, &runtime)
            .await
            .unwrap();
        assert_eq!(result.state, HubOperationState::Succeeded);
        assert_eq!(
            result.result.as_ref().unwrap()["valid"],
            serde_json::json!(false),
            "an invalid candidate still validates successfully"
        );

        let command = test_command("validate_configuration", "config", 1, None, live_deadline);
        let result = execute_command(&client, &cp, &config, &auth, &command, &runtime)
            .await
            .unwrap();
        assert_eq!(result.state, HubOperationState::Failed);
        assert_eq!(result.error.as_deref(), Some("missing configuration payload"));

        // Diff configuration between two stored versions.
        let changed = arkflow_core::configuration::ConfigCandidate {
            format: arkflow_core::configuration::ConfigFormat::Yaml,
            content: format!(
                "streams: []\n# revision {}\n",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ),
            content_verbatim: None,
        };
        let first = cp.version_store().save_with_parent(&candidate, None).unwrap();
        let second = cp
            .version_store()
            .save_with_parent(&changed, Some(first.id.clone()))
            .unwrap();
        let command = test_command(
            "diff_configuration",
            "config",
            1,
            Some(serde_json::json!({"from": first.id, "to": second.id})),
            live_deadline,
        );
        let result = execute_command(&client, &cp, &config, &auth, &command, &runtime)
            .await
            .unwrap();
        assert_eq!(result.state, HubOperationState::Succeeded);
        assert_eq!(
            result.result.as_ref().unwrap()["changed"],
            serde_json::json!(true)
        );

        // NOTE: unlike every other command shape, a malformed
        // diff_configuration payload escapes `execute_command` as a raw Err
        // instead of settling as a Failed CommandResult (suspected product
        // inconsistency — see the report). The test pins the current
        // behavior so a future fix updates it deliberately.
        let command = test_command(
            "diff_configuration",
            "config",
            1,
            Some(serde_json::json!({"to": second.id})),
            live_deadline,
        );
        let error = execute_command(&client, &cp, &config, &auth, &command, &runtime)
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "missing configuration version",
            "diff payload errors currently escape the command path"
        );

        let command = test_command(
            "diff_configuration",
            "config",
            1,
            Some(serde_json::json!({"from": "no-such-version", "to": second.id})),
            live_deadline,
        );
        let error = execute_command(&client, &cp, &config, &auth, &command, &runtime)
            .await
            .unwrap_err();
        assert!(!error.to_string().is_empty());

        // Stream lifecycle: an unknown stream fails, a known one settles.
        let command = test_command("restart", "ghost-stream", 1, None, live_deadline);
        let result = execute_command(&client, &cp, &config, &auth, &command, &runtime)
            .await
            .unwrap();
        assert_eq!(result.state, HubOperationState::Failed);
        assert!(
            result.error.as_deref().unwrap().contains("Unknown stream runtime"),
            "{:?}",
            result.error
        );

        let command = test_command("restart", "orders-stream", 1, None, live_deadline);
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            execute_command(&client, &cp, &config, &auth, &command, &runtime),
        )
        .await
        .expect("the lifecycle watcher settles")
        .unwrap();
        assert_eq!(
            result.state,
            HubOperationState::Succeeded,
            "a restart of a healthy stream settles: {:?}",
            result.error
        );
        assert_eq!(result.progress, 100);

        // Apply/rollback configuration drive the runtime manager.
        let applied = arkflow_core::configuration::ConfigCandidate {
            format: arkflow_core::configuration::ConfigFormat::Yaml,
            content: serde_json::to_string(&serde_json::json!({
                "streams": [{
                    "id": "orders-stream",
                    "input": {
                        "type": "generate",
                        "context": "applied",
                        "interval": "50ms",
                        "batch_size": 1
                    },
                    "pipeline": {"thread_num": 1, "processors": []},
                    "output": {"type": "drop"}
                }]
            }))
            .unwrap(),
            content_verbatim: None,
        };
        let mut apply_command = test_command(
            "apply_configuration",
            "config",
            1,
            Some(serde_json::to_value(&applied).unwrap()),
            live_deadline,
        );
        apply_command.config_version_id = Some("version-42".into());
        let result = execute_command(&client, &cp, &config, &auth, &apply_command, &runtime)
            .await
            .unwrap();
        assert_eq!(
            result.state,
            HubOperationState::Succeeded,
            "{:?}",
            result.error
        );
        assert_eq!(
            cp.runtime_manager().observed_config_version().await,
            Some("version-42".into())
        );

        let command = test_command("apply_configuration", "config", 1, None, live_deadline);
        let result = execute_command(&client, &cp, &config, &auth, &command, &runtime)
            .await
            .unwrap();
        assert_eq!(result.state, HubOperationState::Failed);
        assert_eq!(result.error.as_deref(), Some("missing configuration payload"));

        let command = test_command(
            "rollback_configuration",
            "config",
            1,
            Some(serde_json::json!({"id": first.id})),
            live_deadline,
        );
        let result = execute_command(&client, &cp, &config, &auth, &command, &runtime)
            .await
            .unwrap();
        assert_eq!(result.state, HubOperationState::Succeeded);

        let command = test_command("rollback_configuration", "config", 1, None, live_deadline);
        let result = execute_command(&client, &cp, &config, &auth, &command, &runtime)
            .await
            .unwrap();
        assert_eq!(result.state, HubOperationState::Failed);
        assert_eq!(result.error.as_deref(), Some("missing configuration version"));

        // Malformed Job payloads surface their own actionable errors.
        let cases = [
            ("job_start", None::<serde_json::Value>, "missing Job plan payload"),
            (
                "job_start",
                Some(serde_json::json!({})),
                "missing Job plan payload",
            ),
            (
                "job_start",
                Some(serde_json::json!({"plan": serde_json::to_value(&plan).unwrap()})),
                "missing Job task assignments",
            ),
            ("job_checkpoint", None, "missing checkpoint payload"),
            (
                "job_checkpoint",
                Some(serde_json::json!({})),
                "missing checkpoint_id",
            ),
            ("job_checkpoint_commit", None, "missing checkpoint aggregation payload"),
        ];
        for (operation, payload, expected) in cases {
            let command = test_command(operation, "any-job", 1, payload, live_deadline);
            let error = execute_job_operation(&command, &config, &runtime)
                .await
                .unwrap_err();
            assert_eq!(error, expected, "{operation}");
        }
        let command = test_command(
            "job_checkpoint_commit",
            "any-job",
            1,
            Some(serde_json::json!({"checkpoint_id": "cp-1"})),
            live_deadline,
        );
        let error = execute_job_operation(&command, &config, &runtime)
            .await
            .unwrap_err();
        assert_eq!(error, "missing checkpoint manifest nodes");
        let command = test_command("job_teleport", "any-job", 1, None, live_deadline);
        let error = execute_job_operation(&command, &config, &runtime)
            .await
            .unwrap_err();
        assert_eq!(error, "unknown Job operation job_teleport");

        // Teardown.
        runtime.stop("orders-cmd", 1).await.unwrap();
        let _ = runtime.take_finished().await;
        let _ = cp.runtime_manager().stop_all().await;
        hub_cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), hub_task).await;
    }

    /// A terminal result must survive a delivery failure: the call still
    /// returns Ok so the completed-command cache can replay it later.
    #[tokio::test]
    async fn terminal_results_survive_delivery_failures() {
        let _ = arkflow_plugin::initialize();
        let config = test_node_config("http://127.0.0.1:1");
        let client = build_agent_client(&config.hub_url).unwrap();
        let auth = test_auth();
        let cp = ControlPlane::new(
            EngineConfig {
                streams: Vec::new(),
                jobs: Vec::new(),
                logging: LoggingConfig::default(),
                health_check: HealthCheckConfig::default(),
            },
            RuntimeManager::new(),
        );
        let command = test_command("restart", "orders-stream", 1, None, now_ms() - 1_000);
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            execute_command(&client, &cp, &config, &auth, &command, &JobRuntime::default()),
        )
        .await
        .expect("delivery failure must not hang")
        .expect("the terminal result is still returned");
        assert_eq!(result.state, HubOperationState::TimedOut);
    }

    // ------------------ full Hub/Agent sessions ------------------

    /// A real Hub served over loopback HTTP, mirroring the two-node smoke
    /// harness (reconcile driven by the caller where needed).
    async fn spawned_hub() -> (
        crate::hub::Hub,
        String,
        CancellationToken,
        tokio::task::JoinHandle<()>,
    ) {
        let hub = crate::hub::Hub::new(crate::hub::HubConfig {
            operator_token: None,
            node_token: None,
            insecure_local: true,
            lease_ttl_ms: 2_000,
            poll_interval_ms: 20,
            session_ttl_ms: 2_000,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let server_hub = hub.clone();
        let server_cancel = cancellation.clone();
        let server = tokio::spawn(async move {
            let _ = axum::serve(
                listener,
                hub_router(server_hub, &ServerConfig::default()).into_make_service(),
            )
            .with_graceful_shutdown(server_cancel.cancelled_owned())
            .await;
        });
        (hub, format!("http://{address}"), cancellation, server)
    }

    fn empty_control_plane() -> ControlPlane {
        ControlPlane::new(
            EngineConfig {
                streams: Vec::new(),
                jobs: Vec::new(),
                logging: LoggingConfig::default(),
                health_check: HealthCheckConfig::default(),
            },
            RuntimeManager::new(),
        )
    }

    /// One Agent session end to end: registration, heartbeats, reports with
    /// stream gauges, Job dispatch, a self-ending kernel whose observation
    /// reaches the Hub, and a clean draining shutdown.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn agent_session_reports_and_delivers_job_observations() {
        let _ = arkflow_plugin::initialize();
        let (hub, hub_url, hub_cancel, hub_server) = spawned_hub().await;
        let reconcile_cancel = CancellationToken::new();
        let reconcile_hub = hub.clone();
        let reconcile_stop = reconcile_cancel.clone();
        let reconcile = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(20));
            loop {
                tokio::select! {
                    _ = reconcile_stop.cancelled() => return,
                    _ = tick.tick() => {
                        let _ = reconcile_hub.reconcile_jobs().await;
                    }
                }
            }
        });

        // The Agent's control plane carries one running stream so reports
        // aggregate stream gauges alongside the kernel counters.
        let engine_config = engine_config_with_stream("orders-stream");
        let cp = ControlPlane::new(engine_config.clone(), RuntimeManager::new());
        cp.runtime_manager()
            .replace_config(&engine_config)
            .await
            .expect("the stream registers");

        let agent_cancel = CancellationToken::new();
        let mut agent = tokio::spawn(run(
            cp.clone(),
            NodeAgentConfig {
                hub_url: hub_url.clone(),
                hub_urls: vec![hub_url.clone()],
                api_prefix: "/api/v1".into(),
                node_id: "node-a".into(),
                node_token: String::new(),
                boot_id: "boot-observation".into(),
                heartbeat_interval: Duration::from_millis(50),
                report_interval: Duration::from_millis(50),
                poll_interval: Duration::from_millis(20),
                data_port: None,
                data_host: None,
            },
            agent_cancel.clone(),
        ));
        wait_for(Duration::from_secs(15), || {
            let hub = hub.clone();
            async move { !hub.nodes().await.is_empty() }
        })
        .await;

        // A bounded Job: the kernel consumes its two messages and exits, and
        // the Agent must deliver that observation to the Hub.
        let job_id = format!("observation-job-{}", std::process::id());
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(source_sink_spec_value(
            &job_id,
            "generate",
            serde_json::json!({"context": "x", "interval": "5ms", "count": 2, "batch_size": 1}),
            processing_time(),
            None,
        ))
        .unwrap();
        hub.upsert_job(crate::storage::JobRecord {
            job_id: job_id.clone(),
            version: 1,
            spec_json: serde_json::to_string(&spec).unwrap(),
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
        wait_for(Duration::from_secs(15), || {
            let hub = hub.clone();
            let job_id = job_id.clone();
            async move {
                hub.job(&job_id)
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|record| record.observed_state == "stopped")
            }
        })
        .await;

        // Draining shutdown: the agent returns Ok after cancelling.
        agent_cancel.cancel();
        tokio::time::timeout(Duration::from_secs(10), &mut agent)
            .await
            .expect("the agent exits on cancellation")
            .unwrap()
            .expect("a cancelled agent exits cleanly");
        agent.abort();
        let _ = cp.runtime_manager().stop_all().await;
        reconcile_cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), reconcile).await;
        hub_cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), hub_server).await;
    }

    /// The data-plane listener: a routable host advertises a data address,
    /// and every unbindable or unadvertisable setup still registers the
    /// node (colocated-only) instead of failing startup. Each stage waits
    /// for ITS node id so earlier stages' draining entries cannot satisfy
    /// the condition.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn data_plane_setup_advertises_or_degrades_gracefully() {
        let _guard = ENV_LOCK.lock().await;
        let _ = arkflow_plugin::initialize();
        let (hub, hub_url, hub_cancel, hub_server) = spawned_hub().await;

        let run_agent = |hub_url: String,
                         node_id: &'static str,
                         data_port: Option<u16>,
                         data_host: Option<String>| {
            let cancel = CancellationToken::new();
            let task = tokio::spawn(run(
                empty_control_plane(),
                NodeAgentConfig {
                    hub_url: hub_url.clone(),
                    hub_urls: vec![hub_url],
                    api_prefix: "/api/v1".into(),
                    node_id: node_id.into(),
                    node_token: "data-plane-secret".into(),
                    boot_id: format!("boot-{node_id}"),
                    heartbeat_interval: Duration::from_millis(50),
                    report_interval: Duration::from_millis(50),
                    poll_interval: Duration::from_millis(50),
                    data_port,
                    data_host,
                },
                cancel.clone(),
            ));
            (task, cancel)
        };
        let stop_agent = |mut agent: tokio::task::JoinHandle<
            Result<(), Box<dyn std::error::Error + Send + Sync>>,
        >,
                          cancel: CancellationToken| async move {
            cancel.cancel();
            let _ = tokio::time::timeout(Duration::from_secs(10), &mut agent).await;
            agent.abort();
        };

        // (1) port 0 + routable host: the node advertises a data address and
        // the network_shuffle capability at registration.
        let (agent, cancel) =
            run_agent(hub_url.clone(), "node-dp-a", Some(0), Some("127.0.0.1".into()));
        wait_for(Duration::from_secs(15), || {
            let hub = hub.clone();
            async move {
                hub.nodes().await.iter().any(|node| {
                    node.id == "node-dp-a"
                        && node.data_address.is_some()
                        && node.capabilities.iter().any(|c| c == "network_shuffle")
                })
            }
        })
        .await;
        stop_agent(agent, cancel).await;

        // (2) a bound port without data_host: no address is advertised (the
        // node stays colocated-only from the Hub's placement perspective).
        let (agent, cancel) = run_agent(hub_url.clone(), "node-dp-b", Some(0), None);
        wait_for(Duration::from_secs(15), || {
            let hub = hub.clone();
            async move {
                hub.nodes()
                    .await
                    .iter()
                    .any(|node| node.id == "node-dp-b" && node.data_address.is_none())
            }
        })
        .await;
        stop_agent(agent, cancel).await;

        // (3) an unparseable data_host never reaches the bind.
        let (agent, cancel) = run_agent(
            hub_url.clone(),
            "node-dp-c",
            Some(0),
            Some("not-an-ip-address".into()),
        );
        wait_for(Duration::from_secs(15), || {
            let hub = hub.clone();
            async move {
                hub.nodes()
                    .await
                    .iter()
                    .any(|node| node.id == "node-dp-c" && node.data_address.is_none())
            }
        })
        .await;
        stop_agent(agent, cancel).await;

        // (4) an occupied port fails the bind and stays colocated.
        let held = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let occupied = held.local_addr().unwrap().port();
        let (agent, cancel) = run_agent(
            hub_url.clone(),
            "node-dp-d",
            Some(occupied),
            Some("127.0.0.1".into()),
        );
        wait_for(Duration::from_secs(15), || {
            let hub = hub.clone();
            async move {
                hub.nodes()
                    .await
                    .iter()
                    .any(|node| node.id == "node-dp-d" && node.data_address.is_none())
            }
        })
        .await;
        stop_agent(agent, cancel).await;
        drop(held);

        hub_cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), hub_server).await;
    }

    /// An unreachable Hub fails registration, backs off with jitter, and a
    /// cancellation during the backoff window exits the loop cleanly.
    #[tokio::test(flavor = "multi_thread")]
    async fn failed_registration_backs_off_and_exits_cleanly() {
        let cancel = CancellationToken::new();
        let mut agent = tokio::spawn(run(
            empty_control_plane(),
            NodeAgentConfig {
                hub_url: "http://127.0.0.1:1".into(),
                hub_urls: vec!["http://127.0.0.1:1".into()],
                api_prefix: "/api/v1".into(),
                node_id: "node-offline".into(),
                node_token: String::new(),
                boot_id: "boot-offline".into(),
                heartbeat_interval: Duration::from_millis(50),
                report_interval: Duration::from_millis(50),
                poll_interval: Duration::from_millis(50),
                data_port: None,
                data_host: None,
            },
            cancel.clone(),
        ));
        // One failed registration round trip plus its backoff window.
        tokio::time::sleep(Duration::from_millis(500)).await;
        cancel.cancel();
        let outcome = tokio::time::timeout(Duration::from_secs(5), &mut agent)
            .await
            .expect("the agent exits after cancellation")
            .unwrap();
        assert!(outcome.is_ok(), "cancellation during backoff exits Ok");
        agent.abort();

        // An already-cancelled agent exits before any registration attempt.
        let pre_cancelled = CancellationToken::new();
        pre_cancelled.cancel();
        let outcome = run(empty_control_plane(), test_node_config("http://127.0.0.1:1"), pre_cancelled).await;
        assert!(outcome.is_ok(), "a pre-cancelled agent exits Ok immediately");
    }

    // ------------------------------------------------------------------
    // Coverage additions: checkpoint kinds/aggregation, marker failures,
    // command settlement, data-plane session bookkeeping, session loop.
    // ------------------------------------------------------------------

    /// A plain checkpoint (not a savepoint) writes under `checkpoints/`, and
    /// the aggregate path merges manifests from several nodes into one
    /// sealed barrier: attempts, positions, watermarks and snapshots all
    /// carry over, with distinct task sets merging successfully.
    #[tokio::test(flavor = "multi_thread")]
    async fn plain_checkpoints_round_trip_and_multi_node_manifests_merge() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let checkpoint_root = tempfile::tempdir().unwrap();
        let store_uri = format!("file://{}", checkpoint_root.path().display());
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(source_sink_spec_value(
            "orders-plain",
            "generate",
            serde_json::json!({"context": "x", "interval": "10ms", "batch_size": 1}),
            processing_time(),
            Some(store_uri.clone()),
        ))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let assignments = plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .unwrap();
        let runtime = Arc::new(JobRuntime::default());
        runtime
            .start(
                plan.clone(),
                assignments,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .expect("the job starts");

        // The plain checkpoint kind lands under checkpoints/.
        let uri = runtime
            .checkpoint("orders-plain", "cp-plain", 1, false, "node-a")
            .await
            .expect("the plain checkpoint barrier completes");
        assert!(
            uri.contains("checkpoints/cp-plain/"),
            "plain checkpoints use the checkpoints/ prefix: {uri}"
        );

        // A second node's manifest with a DISJOINT task set merges cleanly.
        let repository =
            CheckpointRepository::new(SharedCheckpointStore::from_uri(&store_uri).unwrap());
        let node_a_artifact = RecoveryArtifact {
            id: "cp-plain".into(),
            kind: RecoveryArtifactKind::Checkpoint,
            manifest_key: "checkpoints/cp-plain/manifests/node-a.json".into(),
            job_version: plan.spec.version,
            format_version: 1,
            created_at_ms: 0,
            status: CheckpointStatus::Completed,
        };
        let node_manifest = repository.read_manifest(&node_a_artifact).unwrap();
        let mut extra = node_manifest.clone();
        extra.task_attempts = vec![arkflow_core::checkpoint::TaskAttemptSnapshot {
            task_id: "extra-0".into(),
            attempt_id: "extra-0:node-b:0".into(),
            node_id: "node-b".into(),
        }];
        // The aggregated snapshot set must cover the planned tasks, so the
        // extra node's manifest references its own state snapshot.
        let extra_snapshot = arkflow_core::state::StateSnapshot::new(1, Vec::new());
        let mut extra_reference = repository
            .write_state_snapshot("cp-plain", &extra_snapshot)
            .unwrap();
        extra_reference.task_id = "extra-0".into();
        extra.state_snapshots = vec![extra_reference];
        extra.watermarks_ms.insert("extra-0".into(), 1_000);
        extra.watermark_partitions.insert(
            "extra-0".into(),
            vec![arkflow_core::checkpoint::WatermarkPosition::new(
                Some("extra".into()),
                0,
                500,
            )],
        );
        extra.seal();
        repository
            .write_manifest(
                &extra,
                RecoveryArtifactKind::Checkpoint,
                "checkpoints/cp-plain/manifests/node-b.json".to_string(),
            )
            .unwrap();

        let planned = ["source-0".to_string(), "sink-0".to_string(), "extra-0".to_string()];
        let aggregated = runtime
            .aggregate_checkpoint(
                "orders-plain",
                "cp-plain",
                1,
                false,
                &["node-a".to_string(), "node-b".to_string()],
                &planned,
            )
            .await
            .expect("the multi-node aggregate seals one manifest");
        assert!(
            aggregated.contains("checkpoints/cp-plain/"),
            "the aggregate keeps the checkpoint identity: {aggregated}"
        );

        runtime.stop("orders-plain", 1).await.unwrap();
        let _ = runtime.take_finished().await;
    }

    /// The atomic start-marker write surfaces its failure paths: an
    /// unwritable parent fails the temporary write, an existing directory at
    /// the marker path fails the rename, and `remove_start_marker` stays
    /// best-effort in both cases.
    #[test]
    fn start_marker_write_failures_are_surfaced_and_best_effort() {
        let root = tempfile::tempdir().unwrap();
        // (a) the parent directory does not exist: the temporary write fails.
        let missing_parent = root.path().join("no/such/dir/.arkflow-started");
        assert!(persist_start_marker(&missing_parent).is_err());
        // (b) a directory occupies the marker path: the rename fails.
        let occupied = root.path().join(".arkflow-started");
        std::fs::create_dir_all(&occupied).unwrap();
        assert!(
            persist_start_marker(&occupied).is_err(),
            "renaming onto a directory must fail"
        );
        // Removal never panics regardless of the path's shape.
        remove_start_marker(&occupied);
        remove_start_marker(&missing_parent);
    }

    /// A durable, recoverable Job whose start marker cannot be persisted
    /// fails closed: the state backend is closed and nothing is registered.
    #[tokio::test(flavor = "multi_thread")]
    async fn durable_start_fails_closed_when_the_marker_cannot_persist() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let checkpoint_root = tempfile::tempdir().unwrap();
        let store_uri = format!("file://{}", checkpoint_root.path().display());
        // A stateful operator makes the durable state recoverable, so the
        // start path computes and persists the start marker.
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(serde_json::json!({
            "id": "orders-marker",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "agg", "kind": "map", "stateful": true, "key_field": "key", "config": {"type": "batch", "count": 1, "timeout_ms": 10}},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [
                {"id": "e1", "from": "source", "to": "agg", "partitioned": true},
                {"id": "e2", "from": "agg", "to": "sink"}
            ],
            "sources": [{
                "operator_id": "source",
                "input_type": "generate",
                "config": {"context": "x", "interval": "10ms", "batch_size": 1},
                "time": {"mode": "processing_time"}
            }],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}],
            "state": {
                "backend": "embedded_kv",
                "durability": "durable",
                "root": unique_state_dir("orders-marker").display().to_string(),
                "format_version": 1
            },
            "checkpoint": {"object_store_uri": store_uri, "interval_ms": 60000, "retention": 3}
        }))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let assignments = plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .unwrap();
        // Pre-create the marker path as a directory so the atomic rename
        // inside the start path fails.
        let marker = durable_recovery_marker(&plan, "node-a", 1).unwrap();
        std::fs::create_dir_all(&marker).unwrap();
        let runtime = Arc::new(JobRuntime::default());
        let error = runtime
            .start(
                plan,
                assignments,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .unwrap_err();
        assert!(
            error.contains("start marker"),
            "the marker failure must surface: {error}"
        );
        assert!(
            runtime.tasks.lock().await.is_empty(),
            "a failed marker persist must not register the Job"
        );
    }

    /// A previous kernel that PANICKED resolves the join with an error, so
    /// the start path still surfaces an outcome instead of hanging on the
    /// join handle.
    #[tokio::test]
    async fn await_previous_teardown_reports_a_panicked_kernel_join() {
        let mut panicked = tokio::spawn(async {
            panic!("kernel exploded");
            #[allow(unreachable_code)]
            Ok::<(), arkflow_core::Error>(())
        });
        let outcome =
            await_previous_teardown("job-join-panic", &mut panicked, Duration::from_secs(5)).await;
        assert!(
            matches!(&outcome, Some(Err(message)) if message.contains("panic")),
            "a panicked join must surface an error outcome: {outcome:?}"
        );
    }

    /// Backend errors surface through every store verb: a read against a
    /// file-rooted store is an error (not a NotFound), and an unsupported
    /// object-store scheme fails checkpoint repository construction.
    #[test]
    fn checkpoint_store_reads_and_repository_construction_surface_errors() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("not-a-directory");
        std::fs::write(&file, b"x").unwrap();
        let uri = Url::from_file_path(&file).unwrap();
        let store = SharedCheckpointStore::from_uri(uri.as_str()).unwrap();
        assert!(
            store.get("some/key").is_err(),
            "a read against a broken root must error, not read as absent"
        );
        assert!(store.delete("some/key").is_err());

        let spec: arkflow_core::job::JobSpec = serde_json::from_value(source_sink_spec_value(
            "orders-baduri",
            "generate",
            serde_json::json!({"context": "x", "interval": "10ms"}),
            processing_time(),
            Some("unsupported-scheme://nowhere".into()),
        ))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let error = match checkpoint_repository(&plan) {
            Err(error) => error,
            Ok(_) => panic!("an unsupported object-store scheme must fail construction"),
        };
        assert!(
            error.to_lowercase().contains("scheme") || !error.is_empty(),
            "the unsupported scheme must surface: {error}"
        );
    }

    /// Job checkpoint commands settle end to end against a stub Hub: a
    /// checkpoint returns its manifest URI, the aggregation commit merges the
    /// agent manifests, malformed commit payloads fail with actionable
    /// errors, and a job_stop on the running generation succeeds.
    #[tokio::test(flavor = "multi_thread")]
    async fn job_checkpoint_commit_and_stop_commands_settle() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let (hub_url, hub_cancel, hub_task) = stub_hub_server().await;
        let client = build_agent_client(&hub_url).unwrap();
        let config = test_node_config(&hub_url);
        let auth = test_auth();
        let cp = empty_control_plane();
        let runtime = Arc::new(JobRuntime::default());
        let live_deadline = now_ms().saturating_add(120_000);

        let checkpoint_root = tempfile::tempdir().unwrap();
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(source_sink_spec_value(
            "orders-cmds",
            "generate",
            serde_json::json!({"context": "x", "interval": "10ms", "batch_size": 1}),
            processing_time(),
            Some(format!("file://{}", checkpoint_root.path().display())),
        ))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let assignments = plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .unwrap();
        let planned_task_ids = plan
            .tasks
            .iter()
            .map(|task| task.id.clone())
            .collect::<Vec<_>>();

        // Start with a full task map and empty data ports in the payload.
        let payload = serde_json::json!({
            "plan": serde_json::to_value(&plan).unwrap(),
            "assignments": serde_json::to_value(&assignments).unwrap(),
            "task_nodes": {"source-0": "node-a", "sink-0": "node-a"},
            "node_data_ports": {}
        });
        let command = test_command("job_start", "orders-cmds", 1, Some(payload), live_deadline);
        let result = execute_command(&client, &cp, &config, &auth, &command, &runtime)
            .await
            .unwrap();
        assert_eq!(result.state, HubOperationState::Succeeded, "{:?}", result.error);

        // A checkpoint succeeds and reports its manifest URI.
        let command = test_command(
            "job_checkpoint",
            "orders-cmds",
            1,
            Some(serde_json::json!({"checkpoint_id": "cp-cmds"})),
            live_deadline,
        );
        let result = execute_command(&client, &cp, &config, &auth, &command, &runtime)
            .await
            .unwrap();
        assert_eq!(result.state, HubOperationState::Succeeded, "{:?}", result.error);
        let manifest_uri = result
            .checkpoint_manifest_uri
            .expect("a successful checkpoint reports its manifest");
        assert!(manifest_uri.contains("checkpoints/cp-cmds/"), "{manifest_uri}");

        // The aggregation commit merges the agent manifest.
        let command = test_command(
            "job_checkpoint_commit",
            "orders-cmds",
            1,
            Some(serde_json::json!({
                "checkpoint_id": "cp-cmds",
                "manifest_nodes": ["node-a"],
                "planned_task_ids": planned_task_ids,
            })),
            live_deadline,
        );
        let result = execute_command(&client, &cp, &config, &auth, &command, &runtime)
            .await
            .unwrap();
        assert_eq!(result.state, HubOperationState::Succeeded, "{:?}", result.error);
        assert!(result.checkpoint_manifest_uri.is_some());

        // Malformed aggregation payloads settle as terminal failures.
        for payload in [
            serde_json::json!({"checkpoint_id": "cp-cmds", "manifest_nodes": "not-an-array"}),
            serde_json::json!({
                "checkpoint_id": "cp-cmds",
                "manifest_nodes": ["node-a"],
                "planned_task_ids": 7
            }),
        ] {
            let command = test_command("job_checkpoint_commit", "orders-cmds", 1, Some(payload), live_deadline);
            let result = execute_command(&client, &cp, &config, &auth, &command, &runtime)
                .await
                .unwrap();
            assert_eq!(
                result.state,
                HubOperationState::Failed,
                "malformed payload must fail: {:?}",
                result.error
            );
            assert!(result.error.is_some());
        }

        // A stop on the running generation succeeds through the command path.
        let command = test_command("job_stop", "orders-cmds", 1, None, live_deadline);
        let result = execute_command(&client, &cp, &config, &auth, &command, &runtime)
            .await
            .unwrap();
        assert_eq!(result.state, HubOperationState::Succeeded, "{:?}", result.error);
        let _ = runtime.take_finished().await;
        hub_cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), hub_task).await;
    }

    /// Stream command settlement: a generation behind the local stream is
    /// superseded, and a start against an already-running stream settles as
    /// a Failed operation (the runtime manager rejects the transition).
    #[tokio::test(flavor = "multi_thread")]
    async fn stream_commands_settle_stale_and_failed_lifecycle_outcomes() {
        let _ = arkflow_plugin::initialize();
        let (hub_url, hub_cancel, hub_task) = stub_hub_server().await;
        let client = build_agent_client(&hub_url).unwrap();
        let config = test_node_config(&hub_url);
        let auth = test_auth();
        let engine_config = engine_config_with_stream("orders-stream");
        let cp = ControlPlane::new(engine_config.clone(), RuntimeManager::new());
        cp.runtime_manager()
            .replace_config(&engine_config)
            .await
            .expect("the stream registers");
        let runtime = Arc::new(JobRuntime::default());
        let live_deadline = now_ms().saturating_add(120_000);

        // NOTE: a stream command behind the runtime's desired generation is
        // superseded (2594-2599), but arkflow-core's runtime manager never
        // moves `desired_generation` off 0, so that arm is not reachable
        // through a real ControlPlane today and is not exercised here.

        // Starting the already-running stream fails the lifecycle operation
        // and settles as a terminal Failed result.
        let command = test_command("start", "orders-stream", 1, None, live_deadline);
        let result = tokio::time::timeout(
            Duration::from_secs(15),
            execute_command(&client, &cp, &config, &auth, &command, &runtime),
        )
        .await
        .expect("the lifecycle watcher settles")
        .unwrap();
        assert_eq!(
            result.state,
            HubOperationState::Failed,
            "start on a running stream must fail: {:?}",
            result.error
        );
        assert_eq!(result.failure_class.as_deref(), Some("permanent_execution"));

        let _ = cp.runtime_manager().stop_all().await;
        hub_cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), hub_task).await;
    }

    /// A JobRuntime with a data-plane manager releases the per-Job session
    /// on every retirement path: replacement, stop, self-completion (with
    /// the crash outcome surfaced) and stop_all.
    #[tokio::test(flavor = "multi_thread")]
    async fn data_plane_sessions_track_replacement_stop_and_completion() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let credentials =
            arkflow_core::executor::remote::DataPlaneCredentials::new("node-a", "secret")
                .expect("test credentials");
        let manager = arkflow_core::executor::remote::NetworkManager::with_config(
            arkflow_core::executor::remote::NetworkManagerConfig {
                credentials: Some(credentials),
                channel_capacity: 16,
                ..Default::default()
            },
        )
        .expect("data plane manager builds");
        manager.spawn();
        let runtime = JobRuntime {
            data_plane: Some(manager.clone()),
            ..JobRuntime::default()
        };

        // A same-Job replacement removes the previous generation's session.
        let (plan, assignments) = replacement_test_plan("orders-dp").await;
        runtime
            .start(
                plan.clone(),
                assignments.clone(),
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .unwrap();
        runtime
            .start(
                plan,
                assignments,
                2,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .expect("the replacement start succeeds");
        runtime.stop("orders-dp", 2).await.unwrap();

        // A crashed kernel surfaces its error outcome through take_finished.
        insert_exited_task(
            &runtime,
            "crash-dp",
            1,
            Err(arkflow_core::Error::Process("kernel died".into())),
        )
        .await;
        // A bounded job ends on its own and is collected the same way.
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(source_sink_spec_value(
            "bounded-dp",
            "generate",
            serde_json::json!({"context": "x", "interval": "5ms", "count": 2, "batch_size": 1}),
            processing_time(),
            None,
        ))
        .unwrap();
        let bounded = JobPlan::compile(spec).unwrap();
        let bounded_assignments = bounded
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .unwrap();
        runtime
            .start(
                bounded,
                bounded_assignments,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .unwrap();
        let finished = tokio::time::timeout(Duration::from_secs(15), async {
            let mut collected = Vec::new();
            loop {
                let finished = runtime.take_finished().await;
                collected.extend(finished);
                let saw_crash = collected
                    .iter()
                    .any(|(job_id, _, _)| job_id == "crash-dp");
                let saw_bounded = collected
                    .iter()
                    .any(|(job_id, _, _)| job_id == "bounded-dp");
                if saw_crash && saw_bounded {
                    break collected;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the exited kernels are collected");
        assert!(
            finished
                .iter()
                .any(|(job_id, generation, outcome)| job_id == "crash-dp"
                    && *generation == 1
                    && outcome.is_err()),
            "the crash outcome must be reported: {finished:?}"
        );
        assert!(
            finished
                .iter()
                .any(|(job_id, _, outcome)| job_id == "bounded-dp" && outcome.is_ok()),
            "the bounded kernel must be reported: {finished:?}"
        );

        // stop_all cancels whatever is left.
        let (plan, assignments) = replacement_test_plan("orders-dp2").await;
        runtime
            .start(
                plan,
                assignments,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .unwrap();
        runtime.stop_all().await;
        assert!(runtime.tasks.lock().await.is_empty());
        manager.shutdown();
    }

    /// A spawn failure on a Job with a DECLARED CPU (dedicated runtime) and
    /// a data-plane manager releases both: the runtime shuts down off the
    /// async path and the job session is removed.
    #[tokio::test(flavor = "multi_thread")]
    async fn spawn_failure_releases_dedicated_runtime_and_data_plane_session() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let state_root = tempfile::tempdir().unwrap();
        let checkpoint_root = tempfile::tempdir().unwrap();
        let credentials =
            arkflow_core::executor::remote::DataPlaneCredentials::new("node-a", "secret")
                .expect("test credentials");
        let manager = arkflow_core::executor::remote::NetworkManager::with_config(
            arkflow_core::executor::remote::NetworkManagerConfig {
                credentials: Some(credentials),
                channel_capacity: 16,
                ..Default::default()
            },
        )
        .expect("data plane manager builds");
        manager.spawn();
        let runtime = JobRuntime {
            data_plane: Some(manager.clone()),
            ..JobRuntime::default()
        };
        let mut spec = serde_json::json!({
            "id": "orders-spawnfail-dp",
            "version": 1,
            "resources": {"cpu_millicores": 100},
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "agg", "kind": "map", "stateful": true, "key_field": "key", "config": {"type": "batch", "count": 1, "timeout_ms": 10}},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [
                {"id": "e1", "from": "source", "to": "agg", "partitioned": true},
                {"id": "e2", "from": "agg", "to": "sink"}
            ],
            "sources": [{
                "operator_id": "source",
                "input_type": "no-such-input",
                "config": {},
                "time": {"mode": "processing_time"}
            }],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}],
            "state": {
                "backend": "embedded_kv",
                "durability": "durable",
                "root": state_root.path().display().to_string(),
                "format_version": 1
            },
            "checkpoint": {
                "object_store_uri": format!("file://{}", checkpoint_root.path().display()),
                "interval_ms": 60000,
                "retention": 3
            }
        });
        spec["sources"][0]["config"] = serde_json::json!({});
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(spec).unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let assignments = plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .unwrap();
        let error = runtime
            .start(
                plan.clone(),
                assignments,
                1,
                None,
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .unwrap_err();
        assert!(
            error.contains("no-such-input") || error.contains("input"),
            "the missing input must surface: {error}"
        );
        assert!(
            runtime.tasks.lock().await.is_empty(),
            "the placeholder entry must be removed"
        );
        let marker = durable_recovery_marker(&plan, "node-a", 1).unwrap();
        assert!(!marker.is_file(), "the start marker must be rolled back");
        manager.shutdown();
    }

    /// The remote-edge context honours the full task map when the split
    /// dispatch carries peer data ports, and degrades to colocated edges
    /// when the dispatch carries no ports at all.
    #[tokio::test(flavor = "multi_thread")]
    async fn remote_context_uses_the_full_task_map_and_degrades_without_ports() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let credentials =
            arkflow_core::executor::remote::DataPlaneCredentials::new("node-a", "secret")
                .expect("test credentials");
        let manager = arkflow_core::executor::remote::NetworkManager::with_config(
            arkflow_core::executor::remote::NetworkManagerConfig {
                credentials: Some(credentials),
                channel_capacity: 16,
                ..Default::default()
            },
        )
        .expect("data plane manager builds");
        manager.spawn();
        let runtime = JobRuntime {
            data_plane: Some(manager.clone()),
            ..JobRuntime::default()
        };

        // Full task map with peer ports: the remote context is built.
        let (plan, assignments) = replacement_test_plan("orders-ports").await;
        let split = SplitPlacementPayload {
            task_nodes: Some(BTreeMap::from([
                ("source-0".into(), "node-a".into()),
                ("sink-0".into(), "node-a".into()),
            ])),
            node_data_ports: BTreeMap::from([("node-a".into(), "127.0.0.1:39601".into())]),
            recovery_required: false,
        };
        runtime
            .start(
                plan,
                assignments,
                1,
                None,
                false,
                "node-a",
                &split,
            )
            .await
            .expect("the split start with a full map succeeds");
        assert!(runtime.tasks.lock().await.contains_key("orders-ports"));
        runtime.stop("orders-ports", 1).await.unwrap();
        let _ = runtime.take_finished().await;

        // No peer ports: the job stays colocated.
        let (plan, assignments) = replacement_test_plan("orders-noports").await;
        let split = SplitPlacementPayload {
            task_nodes: Some(BTreeMap::from([
                ("source-0".into(), "node-a".into()),
                ("sink-0".into(), "node-a".into()),
            ])),
            node_data_ports: BTreeMap::new(),
            recovery_required: false,
        };
        runtime
            .start(
                plan,
                assignments,
                1,
                None,
                false,
                "node-a",
                &split,
            )
            .await
            .expect("the colocated start succeeds");
        assert!(runtime.tasks.lock().await.contains_key("orders-noports"));
        runtime.stop("orders-noports", 1).await.unwrap();
        let _ = runtime.take_finished().await;
        manager.shutdown();
    }

    /// An agent cancelled before its first loop iteration still tears down
    /// the data plane it bound during startup.
    #[tokio::test(flavor = "multi_thread")]
    async fn pre_cancelled_agent_shuts_its_data_plane_down() {
        let _guard = ENV_LOCK.lock().await;
        let cancel = CancellationToken::new();
        cancel.cancel();
        let outcome = run(
            empty_control_plane(),
            NodeAgentConfig {
                hub_url: "http://127.0.0.1:1".into(),
                hub_urls: vec!["http://127.0.0.1:1".into()],
                api_prefix: "/api/v1".into(),
                node_id: "node-dp-early".into(),
                node_token: "data-plane-secret".into(),
                boot_id: "boot-early".into(),
                heartbeat_interval: Duration::from_millis(50),
                report_interval: Duration::from_millis(50),
                poll_interval: Duration::from_millis(50),
                data_port: Some(0),
                data_host: Some("127.0.0.1".into()),
            },
            cancel,
        )
        .await;
        assert!(outcome.is_ok(), "a pre-cancelled agent exits Ok: {outcome:?}");
    }

    /// A session that loses its Hub mid-flight logs the reconnect and exits
    /// cleanly once cancelled.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn agent_session_reconnects_after_the_hub_goes_away() {
        let _ = arkflow_plugin::initialize();
        let (hub, hub_url, hub_cancel, hub_server) = spawned_hub().await;
        let cancel = CancellationToken::new();
        let mut agent = tokio::spawn(run(
            empty_control_plane(),
            NodeAgentConfig {
                hub_url: hub_url.clone(),
                hub_urls: vec![hub_url.clone()],
                api_prefix: "/api/v1".into(),
                node_id: "node-a".into(),
                node_token: String::new(),
                boot_id: "boot-reconnect".into(),
                heartbeat_interval: Duration::from_millis(50),
                report_interval: Duration::from_millis(50),
                poll_interval: Duration::from_millis(50),
                data_port: None,
                data_host: None,
            },
            cancel.clone(),
        ));
        wait_for(Duration::from_secs(15), || {
            let hub = hub.clone();
            async move { !hub.nodes().await.is_empty() }
        })
        .await;
        // Kill the Hub so the next heartbeat fails and the session unwinds.
        hub_cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), hub_server).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        cancel.cancel();
        let outcome = tokio::time::timeout(Duration::from_secs(10), &mut agent)
            .await
            .expect("the agent exits after cancellation")
            .unwrap();
        assert!(outcome.is_ok(), "the agent exits cleanly: {outcome:?}");
        agent.abort();
    }

    /// A scriptable stub Hub so the session loop itself can be driven.
    struct ScriptedHub {
        batches: std::sync::Mutex<std::collections::VecDeque<Vec<AgentCommand>>>,
        results: std::sync::Mutex<Vec<serde_json::Value>>,
        observations: std::sync::Mutex<Vec<serde_json::Value>>,
        observation_status: u16,
    }

    async fn scripted_hub_server(
        observation_status: u16,
        batches: Vec<Vec<AgentCommand>>,
    ) -> (
        String,
        std::sync::Arc<ScriptedHub>,
        CancellationToken,
        tokio::task::JoinHandle<()>,
    ) {
        use axum::extract::State;
        let hub = std::sync::Arc::new(ScriptedHub {
            batches: std::sync::Mutex::new(batches.into_iter().collect()),
            results: std::sync::Mutex::new(Vec::new()),
            observations: std::sync::Mutex::new(Vec::new()),
            observation_status,
        });
        let app = axum::Router::new()
            .route(
                "/api/v1/agent/commands",
                axum::routing::get(|State(hub): State<std::sync::Arc<ScriptedHub>>| async move {
                    let batch = hub
                        .batches
                        .lock()
                        .unwrap()
                        .pop_front()
                        .unwrap_or_default();
                    axum::Json(batch)
                }),
            )
            .route(
                "/api/v1/agent/commands/{id}/result",
                axum::routing::post(
                    |State(hub): State<std::sync::Arc<ScriptedHub>>,
                     axum::extract::Path(_id): axum::extract::Path<String>,
                     axum::Json(body): axum::Json<serde_json::Value>| async move {
                        hub.results.lock().unwrap().push(body);
                        axum::http::StatusCode::OK
                    },
                ),
            )
            .route(
                "/api/v1/agent/job-observations",
                axum::routing::post(
                    |State(hub): State<std::sync::Arc<ScriptedHub>>,
                     axum::Json(body): axum::Json<serde_json::Value>| async move {
                        hub.observations.lock().unwrap().push(body);
                        axum::http::StatusCode::from_u16(hub.observation_status).unwrap()
                    },
                ),
            )
            .fallback(|| async { axum::http::StatusCode::OK })
            .with_state(hub.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let shutdown = cancellation.clone();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await;
        });
        (format!("http://{address}"), hub, cancellation, task)
    }

    fn fast_session_config(hub_url: &str) -> NodeAgentConfig {
        NodeAgentConfig {
            hub_url: hub_url.into(),
            hub_urls: vec![hub_url.into()],
            api_prefix: "/api/v1".into(),
            node_id: "node-a".into(),
            node_token: "token".into(),
            boot_id: "boot-session".into(),
            heartbeat_interval: Duration::from_millis(50),
            report_interval: Duration::from_millis(50),
            poll_interval: Duration::from_millis(20),
            data_port: None,
            data_host: None,
        }
    }

    /// A leader stub for the hub-ha failover tests: completes registration,
    /// accepts reports (recorded for assertions), and returns empty command
    /// batches so the session stays alive.
    struct FailoverLeader {
        registrations: std::sync::Mutex<Vec<serde_json::Value>>,
        reports: std::sync::Mutex<Vec<serde_json::Value>>,
        /// When set, heartbeats answer 500 so the session ends and the
        /// failover loop's reconnect preference becomes observable.
        fail_heartbeats: std::sync::atomic::AtomicBool,
    }

    async fn failover_leader_server()
    -> (
        String,
        std::sync::Arc<FailoverLeader>,
        CancellationToken,
        tokio::task::JoinHandle<()>,
    ) {
        use axum::extract::State;
        let leader = std::sync::Arc::new(FailoverLeader {
            registrations: std::sync::Mutex::new(Vec::new()),
            reports: std::sync::Mutex::new(Vec::new()),
            fail_heartbeats: std::sync::atomic::AtomicBool::new(false),
        });
        let app = axum::Router::new()
            .route(
                "/api/v1/agent/register",
                axum::routing::post(
                    |State(leader): State<std::sync::Arc<FailoverLeader>>,
                     axum::Json(body): axum::Json<serde_json::Value>| async move {
                        leader.registrations.lock().unwrap().push(body);
                        axum::Json(serde_json::json!({
                            "node_id": "node-a",
                            "session_token": "failover-session",
                            "session_ttl_ms": 3_600_000,
                            "lease_ttl_ms": 15_000,
                            "poll_interval_ms": 50,
                            "protocol_version": "v1",
                        }))
                    },
                ),
            )
            .route(
                "/api/v1/agent/report",
                axum::routing::post(
                    |State(leader): State<std::sync::Arc<FailoverLeader>>,
                     axum::Json(body): axum::Json<serde_json::Value>| async move {
                        leader.reports.lock().unwrap().push(body);
                        axum::http::StatusCode::OK
                    },
                ),
            )
            .route(
                "/api/v1/agent/heartbeat",
                axum::routing::post(
                    |State(leader): State<std::sync::Arc<FailoverLeader>>| async move {
                        if leader
                            .fail_heartbeats
                            .load(std::sync::atomic::Ordering::Relaxed)
                        {
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR
                        } else {
                            axum::http::StatusCode::OK
                        }
                    },
                ),
            )
            .route(
                "/api/v1/agent/commands",
                axum::routing::get(|| async { axum::Json(Vec::<AgentCommand>::new()) }),
            )
            .fallback(|| async { axum::http::StatusCode::OK })
            .with_state(leader.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let shutdown = cancellation.clone();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await;
        });
        (format!("http://{address}"), leader, cancellation, task)
    }

    /// A standby stub: every request gets the 503 `hub_standby` problem,
    /// optionally carrying the `leader_url` hint, and a hit counter for
    /// asserting the candidate order.
    async fn standby_server(
        leader_hint: Option<String>,
    ) -> (
        String,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        CancellationToken,
        tokio::task::JoinHandle<()>,
    ) {
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hits_in_route = hits.clone();
        let app = axum::Router::new().fallback(move || {
            let hint = leader_hint.clone();
            let hits = hits_in_route.clone();
            async move {
                hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let details = hint
                    .as_deref()
                    .map(|leader| serde_json::json!({ "leader_url": leader }));
                axum::response::IntoResponse::into_response((
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    axum::Json(serde_json::json!({
                        "code": "hub_standby",
                        "message": "This Hub instance is a standby",
                        "details": details,
                    })),
                ))
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let shutdown = cancellation.clone();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await;
        });
        (format!("http://{address}"), hits, cancellation, task)
    }

    /// hub-ha stage 3: a standby 503 rotates to the next configured
    /// candidate immediately, and the winner's reports name the connected
    /// Hub and the failover count.
    #[tokio::test(flavor = "multi_thread")]
    async fn standby_503_fails_over_to_next_candidate() {
        let (leader_url, leader, leader_cancel, leader_task) = failover_leader_server().await;
        let (standby_url, standby_hits, standby_cancel, standby_task) =
            standby_server(None).await;

        let cancel = CancellationToken::new();
        let mut config = test_node_config(&standby_url);
        config.hub_urls = vec![standby_url.clone(), leader_url.clone()];
        config.heartbeat_interval = Duration::from_millis(50);
        config.report_interval = Duration::from_millis(50);
        config.poll_interval = Duration::from_millis(50);
        let agent = tokio::spawn(run(empty_control_plane(), config, cancel.clone()));

        wait_for(Duration::from_secs(10), || {
            let leader = leader.clone();
            async move { !leader.registrations.lock().unwrap().is_empty() }
        })
        .await;
        assert!(
            standby_hits.load(std::sync::atomic::Ordering::Relaxed) >= 1,
            "the standby must have been tried first"
        );
        wait_for(Duration::from_secs(10), || {
            let leader = leader.clone();
            let leader_url = leader_url.clone();
            async move {
                leader.reports.lock().unwrap().iter().any(|report| {
                    report["connected_hub"].as_str() == Some(leader_url.as_str())
                        && report["metrics"]["hub_failovers"].as_f64().is_some_and(|count| count >= 1.0)
                })
            }
        })
        .await;

        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), agent).await;
        standby_cancel.cancel();
        leader_cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), standby_task).await;
        let _ = tokio::time::timeout(Duration::from_secs(5), leader_task).await;
    }

    /// Pure queue semantics: rotation moves the failed front to the back; a
    /// hint target jumps to the front, deduplicated.
    #[test]
    fn candidate_queue_rotation_preserves_order_and_hint_jumps() {
        let mut candidates: std::collections::VecDeque<String> = ["a", "b", "c"]
            .iter()
            .map(|value| (*value).to_string())
            .collect();

        rotate_candidates(&mut candidates);
        assert_eq!(candidates.front().unwrap(), "b");
        rotate_candidates(&mut candidates);
        assert_eq!(candidates.front().unwrap(), "c");

        // A known target deduplicates instead of growing the queue.
        jump_to_candidate(&mut candidates, "a".into());
        assert_eq!(candidates.len(), 3);
        assert_eq!(candidates.front().unwrap(), "a");

        // An out-of-list target is inserted at the front.
        jump_to_candidate(&mut candidates, "leader-x".into());
        assert_eq!(candidates.len(), 4);
        assert_eq!(candidates.front().unwrap(), "leader-x");
    }

    /// Reconnects prefer the candidate that last accepted a registration:
    /// after a session ends (heartbeat 500), the Agent re-registers against
    /// the same leader without touching the standby again, and the boot
    /// identity stays stable across the switch.
    #[tokio::test(flavor = "multi_thread")]
    async fn reconnect_prefers_the_candidate_that_last_accepted_registration() {
        let (leader_url, leader, leader_cancel, leader_task) = failover_leader_server().await;
        let (standby_url, standby_hits, standby_cancel, standby_task) =
            standby_server(None).await;

        let cancel = CancellationToken::new();
        let mut config = test_node_config(&standby_url);
        config.hub_urls = vec![standby_url.clone(), leader_url.clone()];
        config.boot_id = "boot-pinning".into();
        config.heartbeat_interval = Duration::from_millis(50);
        config.report_interval = Duration::from_millis(50);
        config.poll_interval = Duration::from_millis(50);
        let agent = tokio::spawn(run(empty_control_plane(), config, cancel.clone()));

        // Phase 1: the standby is tried first, then the leader registers.
        wait_for(Duration::from_secs(10), || {
            let leader = leader.clone();
            async move { !leader.registrations.lock().unwrap().is_empty() }
        })
        .await;
        assert_eq!(
            standby_hits.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "exactly one standby hit before the leader registered"
        );

        // Phase 2: kill the session via heartbeats; the reconnect must go
        // straight back to the pinned leader.
        leader
            .fail_heartbeats
            .store(true, std::sync::atomic::Ordering::Relaxed);
        wait_for(Duration::from_secs(10), || {
            let leader = leader.clone();
            async move { leader.registrations.lock().unwrap().len() >= 2 }
        })
        .await;
        leader
            .fail_heartbeats
            .store(false, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            standby_hits.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the reconnect hit the pinned leader, not the standby"
        );

        // Session continuity across the switch: same boot identity and node.
        let registrations = leader.registrations.lock().unwrap().clone();
        for registration in &registrations {
            assert_eq!(registration["boot_id"].as_str(), Some("boot-pinning"));
            assert_eq!(registration["node_id"].as_str(), Some("node-a"));
        }

        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), agent).await;
        standby_cancel.cancel();
        leader_cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), standby_task).await;
        let _ = tokio::time::timeout(Duration::from_secs(5), leader_task).await;
    }

    /// A standby's `leader_url` hint jumps the queue even when the leader is
    /// not part of the configured candidate list.
    #[tokio::test(flavor = "multi_thread")]
    async fn standby_leader_hint_jumps_to_the_advertised_leader() {
        let (leader_url, leader, leader_cancel, leader_task) = failover_leader_server().await;
        let (standby_url, standby_hits, standby_cancel, standby_task) =
            standby_server(Some(leader_url.clone())).await;

        let cancel = CancellationToken::new();
        let mut config = test_node_config(&standby_url);
        config.hub_urls = vec![standby_url.clone()];
        config.heartbeat_interval = Duration::from_millis(50);
        config.report_interval = Duration::from_millis(50);
        config.poll_interval = Duration::from_millis(50);
        let agent = tokio::spawn(run(empty_control_plane(), config, cancel.clone()));

        wait_for(Duration::from_secs(10), || {
            let leader = leader.clone();
            async move { !leader.registrations.lock().unwrap().is_empty() }
        })
        .await;
        assert!(
            standby_hits.load(std::sync::atomic::Ordering::Relaxed) >= 1,
            "the standby must have been tried first"
        );

        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), agent).await;
        standby_cancel.cancel();
        leader_cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), standby_task).await;
        let _ = tokio::time::timeout(Duration::from_secs(5), leader_task).await;
    }

    fn stub_session() -> crate::hub::RegisterResponse {
        crate::hub::RegisterResponse {
            node_id: "node-a".into(),
            session_token: "stub-session".into(),
            session_ttl_ms: 0,
            lease_ttl_ms: 0,
            poll_interval_ms: 0,
            protocol_version: "v1".into(),
        }
    }

    /// A command whose execution escapes as a raw error ends the session so
    /// the reconnect loop owns recovery.
    #[tokio::test(flavor = "multi_thread")]
    async fn run_session_failing_command_ends_the_session() {
        let _ = arkflow_plugin::initialize();
        let live_deadline = now_ms().saturating_add(120_000);
        let broken = test_command(
            "diff_configuration",
            "config",
            1,
            Some(serde_json::json!({"to": "some-version"})),
            live_deadline,
        );
        let (hub_url, _hub, hub_cancel, hub_task) =
            scripted_hub_server(200, vec![vec![broken]]).await;
        let client = build_agent_client(&hub_url).unwrap();
        let config = fast_session_config(&hub_url);
        let cancel = CancellationToken::new();
        let sampler_cancel = CancellationToken::new();
        let sampler = spawn_resource_sampler(Duration::from_secs(3_600), sampler_cancel.clone());
        let mut cache = CompletedCommandCache::new(16);
        let runtime = JobRuntime::default();
        let error = tokio::time::timeout(
            Duration::from_secs(15),
            run_session(
                &client,
                &empty_control_plane(),
                &config,
                stub_session(),
                cancel,
                &mut cache,
                runtime,
                false,
                &sampler,
                &std::sync::atomic::AtomicU64::new(0),
            ),
        )
        .await
        .expect("the session ends")
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "missing configuration version",
            "{error}"
        );
        sampler_cancel.cancel();
        hub_cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), hub_task).await;
    }

    /// A crashed kernel's observation rides the session as `failed`, and a
    /// delivery failure parks the observation for the next session.
    #[tokio::test(flavor = "multi_thread")]
    async fn run_session_delivers_failed_observations_and_parks_on_failure() {
        let _ = arkflow_plugin::initialize();

        // (a) delivery succeeds: the observation reports the failure state.
        let (hub_url, hub, hub_cancel, hub_task) = scripted_hub_server(200, vec![]).await;
        let client = build_agent_client(&hub_url).unwrap();
        let config = fast_session_config(&hub_url);
        let cancel = CancellationToken::new();
        let sampler_cancel = CancellationToken::new();
        let sampler = spawn_resource_sampler(Duration::from_secs(3_600), sampler_cancel.clone());
        let mut cache = CompletedCommandCache::new(16);
        let cp = empty_control_plane();
        let runtime = JobRuntime::default();
        insert_exited_task(
            &runtime,
            "crash-obs",
            1,
            Err(arkflow_core::Error::Process("kernel died".into())),
        )
        .await;
        let session_cancel = cancel.clone();
        let session = tokio::spawn(async move {
            run_session(
                &client,
                &cp,
                &config,
                stub_session(),
                session_cancel,
                &mut cache,
                runtime,
                false,
                &sampler,
                &std::sync::atomic::AtomicU64::new(0),
            )
            .await
        });
        wait_for(Duration::from_secs(15), || {
            let hub = hub.clone();
            async move { !hub.observations.lock().unwrap().is_empty() }
        })
        .await;
        let observed = hub.observations.lock().unwrap()[0].clone();
        assert_eq!(observed["state"], "failed", "{observed}");
        assert!(
            observed["error"].as_str().unwrap().contains("kernel died"),
            "{observed}"
        );
        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(10), session).await;
        sampler_cancel.cancel();
        hub_cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), hub_task).await;

        // (b) delivery fails: the observation is parked and the session ends.
        let (hub_url, _hub, hub_cancel, hub_task) = scripted_hub_server(500, vec![]).await;
        let client = build_agent_client(&hub_url).unwrap();
        let config = fast_session_config(&hub_url);
        let cancel = CancellationToken::new();
        let sampler_cancel = CancellationToken::new();
        let sampler = spawn_resource_sampler(Duration::from_secs(3_600), sampler_cancel.clone());
        let mut cache = CompletedCommandCache::new(16);
        let runtime = JobRuntime::default();
        insert_exited_task(
            &runtime,
            "crash-park",
            2,
            Err(arkflow_core::Error::Process("kernel died again".into())),
        )
        .await;
        let error = tokio::time::timeout(
            Duration::from_secs(15),
            run_session(
                &client,
                &empty_control_plane(),
                &config,
                stub_session(),
                cancel,
                &mut cache,
                runtime.clone(),
                false,
                &sampler,
                &std::sync::atomic::AtomicU64::new(0),
            ),
        )
        .await
        .expect("the session ends on the delivery failure")
        .unwrap_err();
        assert!(!error.to_string().is_empty(), "{error}");
        let parked = runtime.take_finished().await;
        assert!(
            parked
                .iter()
                .any(|(job_id, generation, outcome)| job_id == "crash-park"
                    && *generation == 2
                    && outcome.is_err()),
            "the undelivered observation must be parked: {parked:?}"
        );
        sampler_cancel.cancel();
        hub_cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), hub_task).await;
    }

    /// The session replays cached terminal results without re-executing and
    /// ignores a duplicate command id inside one poll batch.
    #[tokio::test(flavor = "multi_thread")]
    async fn run_session_replays_cached_commands_and_deduplicates() {
        let _ = arkflow_plugin::initialize();
        let live_deadline = now_ms().saturating_add(120_000);
        let replay = test_command("job_stop", "ghost-replay", 1, None, live_deadline);
        let duplicate = test_command("job_stop", "ghost-dupe", 1, None, live_deadline);
        let replay_id = replay.id.clone();
        let duplicate_id = duplicate.id.clone();
        let (hub_url, hub, hub_cancel, hub_task) =
            scripted_hub_server(200, vec![vec![replay, duplicate.clone(), duplicate]]).await;
        let client = build_agent_client(&hub_url).unwrap();
        let config = fast_session_config(&hub_url);
        let cancel = CancellationToken::new();
        let sampler_cancel = CancellationToken::new();
        let sampler = spawn_resource_sampler(Duration::from_secs(3_600), sampler_cancel.clone());
        let mut cache = CompletedCommandCache::new(16);
        let cached = CommandResult {
            command_id: replay_id.clone(),
            operation_id: "op-cached".into(),
            state: HubOperationState::Succeeded,
            progress: 100,
            error: None,
            correlation_id: None,
            generation: 1,
            observed_generation: None,
            action_id: None,
            failure_class: None,
            config_version_id: None,
            rollout_id: None,
            observed_checkpoint_id: None,
            checkpoint_manifest_uri: None,
            result: None,
        };
        remember_completed_command(&mut cache, replay_id.clone(), cached);
        let runtime = JobRuntime::default();
        let cp = empty_control_plane();
        let session_cancel = cancel.clone();
        let session = tokio::spawn(async move {
            run_session(
                &client,
                &cp,
                &config,
                stub_session(),
                session_cancel,
                &mut cache,
                runtime,
                false,
                &sampler,
                &std::sync::atomic::AtomicU64::new(0),
            )
            .await
        });
        wait_for(Duration::from_secs(15), || {
            let hub = hub.clone();
            let replay_id = replay_id.clone();
            let duplicate_id = duplicate_id.clone();
            async move {
                let results = hub.results.lock().unwrap();
                results.iter().any(|result| {
                    result["command_id"] == replay_id.as_str()
                        && result["operation_id"] == "op-cached"
                }) && results
                    .iter()
                    .any(|result| result["command_id"] == duplicate_id.as_str())
            }
        })
        .await;
        // The cached replay must not re-execute: no runtime entry appears.
        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(10), session).await;
        sampler_cancel.cancel();
        hub_cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), hub_task).await;
    }

    /// Two event-time sources feeding one window share a single watermark
    /// tracker, and recovery reinstalls both the partition-scoped watermark
    /// and skips the task-level one when partition progress was recorded.
    #[tokio::test(flavor = "multi_thread")]
    async fn event_time_sources_sharing_a_window_share_one_tracker() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let _ = arkflow_plugin::initialize();
        let checkpoint_root = tempfile::tempdir().unwrap();
        let store_uri = format!("file://{}", checkpoint_root.path().display());
        let event_time = || {
            serde_json::json!({
                "mode": "event_time",
                "timestamp_field": "value",
                "watermark": {"strategy": "bounded_out_of_orderness", "out_of_orderness_ms": 50}
            })
        };
        let spec: arkflow_core::job::JobSpec = serde_json::from_value(serde_json::json!({
            "id": "orders-shared-evt",
            "version": 1,
            "operators": [
                {"id": "left", "kind": "source"},
                {"id": "right", "kind": "source"},
                {"id": "win", "kind": "window", "stateful": true, "key_field": "key", "config": {
                    "type": "window", "kind": "tumbling", "size_ms": 10000,
                    "timestamp_field": "value", "key_field": "key",
                    "value_fields": ["value"], "trigger": "watermark",
                    "watermark_field": "__watermark_ms"
                }},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [
                {"id": "e1", "from": "left", "to": "win"},
                {"id": "e2", "from": "right", "to": "win"},
                {"id": "e3", "from": "win", "to": "sink"}
            ],
            "sources": [
                {
                    "operator_id": "left",
                    "input_type": "generate",
                    "config": {"context": "x", "interval": "10ms", "count": 4, "batch_size": 1},
                    "time": event_time()
                },
                {
                    "operator_id": "right",
                    "input_type": "generate",
                    "config": {"context": "y", "interval": "10ms", "count": 4, "batch_size": 1},
                    "time": event_time()
                }
            ],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}],
            "state": {
                "backend": "embedded_kv",
                "durability": "durable",
                "root": unique_state_dir("orders-shared-evt").display().to_string(),
                "format_version": 1
            },
            "checkpoint": {"object_store_uri": store_uri, "interval_ms": 60000, "retention": 3}
        }))
        .unwrap();
        let plan = JobPlan::compile(spec).unwrap();
        let repository =
            CheckpointRepository::new(SharedCheckpointStore::from_uri(&store_uri).unwrap());
        let mut snapshots = Vec::new();
        for task in &plan.tasks {
            let snapshot = arkflow_core::state::StateSnapshot::new(1, Vec::new());
            let mut reference = repository
                .write_state_snapshot("cp-shared", &snapshot)
                .unwrap();
            reference.task_id = task.id.clone();
            snapshots.push(reference);
        }
        let mut manifest = plan_manifest(&plan, "cp-shared", snapshots);
        manifest.watermark_partitions.insert(
            "left-0".into(),
            vec![arkflow_core::checkpoint::WatermarkPosition::new(
                Some("orders".into()),
                0,
                9_000,
            )],
        );
        // A task-level watermark whose partition progress was also recorded
        // must not overwrite the restored partitions.
        manifest.watermarks_ms.insert("left-0".into(), 4_500);
        manifest.seal();
        repository
            .write_manifest(
                &manifest,
                RecoveryArtifactKind::Checkpoint,
                arkflow_core::checkpoint::recovery_manifest_key(
                    RecoveryArtifactKind::Checkpoint,
                    "cp-shared",
                ),
            )
            .unwrap();
        let assignments = plan
            .assignments_for_nodes(&["node-a".to_string()], 1)
            .unwrap();
        let runtime = Arc::new(JobRuntime::default());
        runtime
            .start(
                plan,
                assignments,
                1,
                Some("cp-shared".into()),
                false,
                "node-a",
                &SplitPlacementPayload::default(),
            )
            .await
            .expect("the shared-tracker event-time recovery start succeeds");
        assert!(
            runtime
                .tasks
                .lock()
                .await
                .contains_key("orders-shared-evt")
        );
        runtime.stop("orders-shared-evt", 1).await.unwrap();
        let _ = runtime.take_finished().await;
    }

    /// A kernel task that PANICKED resolves the join with an error in
    /// `take_finished`, so the observation reports the failure instead of
    /// assuming a clean exit.
    #[tokio::test(flavor = "multi_thread")]
    async fn take_finished_reports_a_panicked_kernel_join() {
        let _serial = ONE_KERNEL_AT_A_TIME.lock().await;
        let runtime = JobRuntime::default();
        let state: Arc<dyn StateBackend> = Arc::new(
            RedbStateBackend::open(unique_state_dir("join-panic"), 1)
                .expect("test state backend opens"),
        );
        let handle = tokio::spawn(async {
            panic!("kernel join panic");
            #[allow(unreachable_code)]
            Ok::<(), arkflow_core::Error>(())
        });
        // Let the panic resolve before collection so the join is terminal.
        while !handle.is_finished() {
            tokio::task::yield_now().await;
        }
        runtime.tasks.lock().await.insert(
            "join-panic-job".to_string(),
            JobTask {
                generation: 1,
                ephemeral_state: false,
                recovery_required: false,
                cancellation: CancellationToken::new(),
                assignments: Vec::new(),
                dedicated_runtime: None,
                watermark_partitions: BTreeMap::new(),
                state,
                checkpoint_store_uri: None,
                kernel: None,
                handle,
            },
        );
        let finished = runtime.take_finished().await;
        assert_eq!(finished.len(), 1, "{finished:?}");
        let (job_id, generation, outcome) = &finished[0];
        assert_eq!(job_id, "join-panic-job");
        assert_eq!(*generation, 1);
        assert!(
            matches!(outcome, Err(message) if message.contains("panic")),
            "a panicked join must surface: {outcome:?}"
        );
        assert!(runtime.tasks.lock().await.is_empty());
    }

    /// A stream command whose deadline expires while its lifecycle operation
    /// is still mid-flight settles as TimedOut through the watcher's expiry
    /// branch (rather than the pre-execution expiry).
    #[tokio::test(flavor = "multi_thread")]
    async fn stream_command_deadline_expires_mid_lifecycle() {
        let _ = arkflow_plugin::initialize();
        let (hub_url, hub_cancel, hub_task) = stub_hub_server().await;
        let client = build_agent_client(&hub_url).unwrap();
        let config = test_node_config(&hub_url);
        let auth = test_auth();
        let engine_config = engine_config_with_stream("orders-deadline");
        let cp = ControlPlane::new(engine_config.clone(), RuntimeManager::new());
        cp.runtime_manager()
            .replace_config(&engine_config)
            .await
            .expect("the stream registers");
        let runtime = Arc::new(JobRuntime::default());

        // Each attempt uses a fresh command id and a deadline only a
        // millisecond out: the precheck passes, and the first watcher poll
        // after the result round trip lands past the deadline while the
        // lifecycle operation is still mid-flight.
        let mut saw_deadline = false;
        for attempt in 0..40 {
            let mut command = test_command(
                "restart",
                "orders-deadline",
                1,
                None,
                now_ms().saturating_add(1),
            );
            command.id = format!("cmd-deadline-{attempt}");
            command.operation_id = format!("op-deadline-{attempt}");
            let outcome = tokio::time::timeout(
                Duration::from_secs(20),
                execute_command(&client, &cp, &config, &auth, &command, &runtime),
            )
            .await
            .expect("each attempt settles")
            .unwrap();
            match outcome.state {
                HubOperationState::TimedOut
                    if outcome.error.as_deref().is_some_and(|e| e.contains("deadline")) =>
                {
                    saw_deadline = true;
                    break;
                }
                // On a loaded runner the 1ms deadline can lapse before the
                // precheck (`command_expired` guard) settles the attempt
                // through the pre-execution expiry branch; that attempt is a
                // retryable miss, not the branch under test.
                HubOperationState::TimedOut
                    if outcome.error.as_deref() == Some("Command expired before execution") =>
                {
                    continue;
                }
                HubOperationState::Succeeded => continue,
                other => panic!("unexpected intermediate state {other:?}"),
            }
        }
        assert!(
            saw_deadline,
            "at least one attempt must settle through the deadline branch"
        );
        let _ = cp.runtime_manager().stop_all().await;
        hub_cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), hub_task).await;
    }

    /// An agent cancelled before its first loop iteration with no data-plane
    /// secret never builds the manager and still exits cleanly.
    #[tokio::test(flavor = "multi_thread")]
    async fn pre_cancelled_agent_without_a_secret_skips_the_data_plane() {
        let _guard = ENV_LOCK.lock().await;
        let cancel = CancellationToken::new();
        cancel.cancel();
        let outcome = run(
            empty_control_plane(),
            NodeAgentConfig {
                hub_url: "http://127.0.0.1:1".into(),
                hub_urls: vec!["http://127.0.0.1:1".into()],
                api_prefix: "/api/v1".into(),
                node_id: "node-dp-nosecret".into(),
                node_token: String::new(),
                boot_id: "boot-nosecret".into(),
                heartbeat_interval: Duration::from_millis(50),
                report_interval: Duration::from_millis(50),
                poll_interval: Duration::from_millis(50),
                data_port: Some(0),
                data_host: Some("127.0.0.1".into()),
            },
            cancel,
        )
        .await;
        assert!(outcome.is_ok(), "{outcome:?}");
    }
}
