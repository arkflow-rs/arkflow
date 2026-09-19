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
    pub hub_url: String,
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
    let planned_tasks = plan
        .tasks
        .iter()
        .map(|task| task.id.clone())
        .collect::<BTreeSet<_>>();
    let compatibility = arkflow_core::checkpoint::evaluate_recovery_compatibility(
        manifest,
        &plan.spec.id,
        plan.spec.version,
        state_format_version,
        &planned_tasks,
    );
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
) -> Result<(), String> {
    let planned_tasks = plan
        .tasks
        .iter()
        .map(|task| task.id.clone())
        .collect::<BTreeSet<_>>();
    arkflow_core::checkpoint::validate_state_snapshot_task_set(
        &manifest.state_snapshots,
        &planned_tasks,
    )?;
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
    )
    .is_err()
    {
        return false;
    }
    if validate_recovery_snapshots(&plan, &repository, &manifest).is_err() {
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
            // no-op success — but only while that kernel is actually alive:
            // cancelling and restarting a healthy kernel for a command the
            // Hub re-sent after a restart would churn the data plane and
            // (under load) wedge the start path behind a teardown that never
            // finishes. An exited kernel at the same generation (a crash
            // between the poll drain and this reader) must fall through to
            // the restart path instead of being reported as a healthy no-op.
            if tasks
                .get(&job_id)
                .is_some_and(|task| task.generation == generation && !task.handle.is_finished())
            {
                return Ok(());
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
            let existing = tasks.remove(&job_id);
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
        if let Some(existing) = existing {
            let existing_generation = existing.generation;
            let outcome =
                await_previous_teardown(&job_id, existing.handle, KERNEL_TEARDOWN_JOIN_TIMEOUT)
                    .await;
            let _ = existing.state.close();
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
                validate_recovery_manifest(
                    &plan_for_recovery,
                    &checkpoint_id,
                    state_for_restore.format_version(),
                    &manifest,
                )?;
                validate_recovery_snapshots(&plan_for_recovery, &repository, &manifest)?;
                let assigned_task_ids = assignments_for_recovery
                    .iter()
                    .map(|assignment| assignment.task_id.as_str())
                    .collect::<BTreeSet<_>>();
                let mut snapshots = manifest
                    .state_snapshots
                    .iter()
                    .filter(|snapshot_ref| {
                        assigned_task_ids.contains(snapshot_ref.task_id.as_str())
                    })
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
                    let snapshot = arkflow_core::state::StateSnapshot::new(
                        state_for_restore.format_version(),
                        entries,
                    );
                    state_for_restore
                        .restore(&snapshot)
                        .map_err(|error| error.to_string())?;
                } else if let Some(snapshot) = snapshots.pop() {
                    state_for_restore
                        .restore(&snapshot)
                        .map_err(|error| error.to_string())?;
                }
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
            Some(arkflow_core::executor::graph::RemoteEdgeContext {
                local_node: node_id.to_string(),
                task_nodes,
                node_addrs,
                manager: manager.clone(),
                generation,
            })
        });
        let spawn_result = spawn_kernel_job(
            &plan,
            &task_ids,
            state.clone(),
            recovery.as_ref(),
            cancellation.clone(),
            remote_context.as_ref(),
        )
        .await;
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
                if let Some(task) = placeholder {
                    task.cancellation.cancel();
                    let _ = task.handle.await;
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
            if let Some(task) = tasks.remove(&job_id) {
                let result = match task.handle.await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => Err(error.to_string()),
                    Err(error) => Err(error.to_string()),
                };
                let _ = task.state.close();
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
        for (job_id, task) in tasks {
            task.cancellation.cancel();
            let _ = task.handle.await;
            let _ = task.state.close();
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
        if let Some(task) = task {
            let task_generation = task.generation;
            task.cancellation.cancel();
            let _ = task.handle.await;
            let _ = task.state.close();
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
    let mut resource = Resource {
        temporary: HashMap::<String, Arc<dyn Temporary>>::new(),
        input_names: RefCell::new(Vec::new()),
    };
    let mut graph = arkflow_core::executor::graph::ExecutionGraphBuilder::default()
        .with_state(state.clone())
        .build_subgraph(plan, task_ids, &RegistryJobAdapter, &mut resource, remote)
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
                gate.lock()
                    .await
                    .as_mut()
                    .map(|gate| gate.seed_partitions(&partitions));
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
        let boot_nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        Some(Self {
            hub_url: hub_url.trim_end_matches('/').into(),
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

pub async fn run(
    cp: ControlPlane,
    config: NodeAgentConfig,
    cancellation: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = build_agent_client(&config.hub_url)?;
    let mut backoff = Duration::from_millis(250);
    let mut completed_commands = CompletedCommandCache::new(1024);
    let mut job_runtime = JobRuntime::default();
    // Cross-node shuffle data plane: one listener per Agent process. A bind
    // failure degrades to the co-location contract (warn, no listener) rather
    // than blocking node startup — observability and placement still work.
    let mut data_address: Option<String> = None;
    if let Some(port) = config.data_port {
        let data_secret = std::env::var("ARKFLOW_DATA_PLANE_SECRET")
            .ok()
            .filter(|secret| !secret.is_empty())
            .or_else(|| (!config.node_token.is_empty()).then(|| config.node_token.clone()));
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
                let mut manager_config =
                    arkflow_core::executor::remote::NetworkManagerConfig::default();
                manager_config.credentials = Some(credentials);
                manager_config.channel_capacity = 1024;
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
    loop {
        if cancellation.is_cancelled() {
            job_runtime.stop_all().await;
            if let Some(manager) = &job_runtime.data_plane {
                manager.shutdown();
            }
            return Ok(());
        }
        match register(&client, &config, data_address.clone()).await {
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
                    job_runtime.clone(),
                    network_shuffle,
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
        tokio::select! {
            _ = cancellation.cancelled() => {
                job_runtime.stop_all().await;
                if let Some(manager) = &job_runtime.data_plane {
                    manager.shutdown();
                }
                return Ok(())
            },
            _ = tokio::time::sleep(jittered_backoff(backoff)) => {}
        }
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
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
    handle: tokio::task::JoinHandle<Result<(), arkflow_core::Error>>,
    bound: Duration,
) -> Option<Result<(), String>> {
    match tokio::time::timeout(bound, handle).await {
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

async fn register(
    client: &Client,
    config: &NodeAgentConfig,
    data_address: Option<String>,
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
            // data_address is Some only when the data plane actually bound —
            // a node whose port was taken must not advertise shuffle.
            capabilities: agent_capabilities(data_address.is_some()),
            boot_id: Some(config.boot_id.clone()),
            data_address,
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
    completed_commands: &mut CompletedCommandCache,
    job_runtime: JobRuntime,
    network_shuffle: bool,
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
            _ = report_tick.tick() => { report_seq = report_seq.saturating_add(1); post_json(client, format!("{}{}{}", config.hub_url, config.api_prefix, "/agent/report"), &report(cp, &auth, &config.boot_id, report_seq, &job_runtime, network_shuffle).await).await?; }
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
    let wedged = tokio::spawn(std::future::pending::<Result<(), arkflow_core::Error>>());
    let started = std::time::Instant::now();
    let outcome = await_previous_teardown("job-wedge", wedged, Duration::from_millis(100)).await;
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
    let crashed = tokio::spawn(async {
        Result::<(), arkflow_core::Error>::Err(arkflow_core::Error::Process(
            "kernel exploded".into(),
        ))
    });
    let outcome = await_previous_teardown("job-crash", crashed, Duration::from_secs(5)).await;
    assert!(
        matches!(&outcome, Some(Err(message)) if message.contains("kernel exploded")),
        "the crash outcome must pass through: {outcome:?}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::config::{EngineConfig, HealthCheckConfig, LoggingConfig};

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
            hub_url: Some("http://hub".into()),
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
            hub_url: Some("http://hub".into()),
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
}
