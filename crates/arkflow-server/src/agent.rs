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
}

#[derive(Clone, Default)]
struct JobRuntime {
    tasks: Arc<Mutex<BTreeMap<String, JobTask>>>,
    starts: Arc<Mutex<()>>,
}

struct JobTask {
    generation: u64,
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
    let task_ids = manifest
        .task_attempts
        .iter()
        .map(|attempt| attempt.task_id.as_str())
        .collect::<BTreeSet<_>>();
    let snapshot_tasks = manifest
        .state_snapshots
        .iter()
        .map(|snapshot| snapshot.task_id.as_str())
        .collect::<BTreeSet<_>>();
    if task_ids.is_empty() || !task_ids.is_subset(&snapshot_tasks) {
        return false;
    }
    manifest
        .state_snapshots
        .iter()
        .all(|snapshot| repository.read_state_snapshot(snapshot).is_ok())
}

fn parse_recovery_payload(payload: &serde_json::Value) -> Result<(Option<String>, bool), String> {
    let Some(recovery) = payload.get("recovery") else {
        return Ok((None, false));
    };
    if recovery.is_null() {
        return Ok((None, false));
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
    Ok((Some(checkpoint_id.to_owned()), savepoint))
}

impl JobRuntime {
    async fn generation(&self, job_id: &str) -> Option<u64> {
        self.tasks
            .lock()
            .await
            .get(job_id)
            .map(|task| task.generation)
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

        for task in tasks.values() {
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
    ) -> Result<(), String> {
        let _start_guard = self.starts.lock().await;
        let job_id = plan.spec.id.to_string();
        if assignments.is_empty() {
            return Err("Job command contains no task assignments".into());
        }
        let existing = {
            let tasks = self.tasks.lock().await;
            if tasks
                .get(&job_id)
                .is_some_and(|task| task.generation > generation)
            {
                return Err("job generation is stale".into());
            }
            drop(tasks);
            let mut tasks = self.tasks.lock().await;
            let existing = tasks.remove(&job_id);
            if let Some(existing) = &existing {
                existing.cancellation.cancel();
            }
            existing
        };
        if let Some(existing) = existing {
            let _ = existing.handle.await;
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
        let state_root = std::env::temp_dir()
            .join("arkflow-job-state")
            .join(&job_id)
            .join(format!("node-{}", safe_path_component(node_id)))
            .join(format!(
                "version-{}-generation-{}",
                plan.spec.version.0, generation
            ));
        let state_format_version = plan
            .spec
            .state
            .as_ref()
            .map(|state| state.format_version)
            .unwrap_or(1);
        let state: Arc<dyn StateBackend> = Arc::new(
            RedbStateBackend::open(state_root, state_format_version)
                .map_err(|error| error.to_string())?,
        );
        let recovery = if let Some(checkpoint_id) = recovery_id {
            let repository = checkpoint_repository(&plan)?;
            let artifact = recovery_artifact(&plan, &checkpoint_id, recovery_savepoint)?;
            let manifest = repository
                .read_manifest(&artifact)
                .map_err(|error| error.to_string())?;
            validate_recovery_manifest(&plan, &checkpoint_id, state_format_version, &manifest)?;
            let assigned_task_ids = assignments
                .iter()
                .map(|assignment| assignment.task_id.as_str())
                .collect::<BTreeSet<_>>();
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
                let snapshot =
                    arkflow_core::state::StateSnapshot::new(state.format_version(), entries);
                state
                    .restore(&snapshot)
                    .map_err(|error| error.to_string())?;
            } else if let Some(snapshot) = snapshots.pop() {
                state
                    .restore(&snapshot)
                    .map_err(|error| error.to_string())?;
            }
            let recovery =
                RecoveryPlan::from_manifest(&manifest).map_err(|error| error.to_string())?;
            Some(recovery)
        } else {
            None
        };
        let cancellation = CancellationToken::new();
        // Spawn the Job on the unified kernel: the same plan, adapter and
        // state backend drive pipelined chain execution, and the handle backs
        // command-driven checkpoints. The legacy runner stays attached only
        // until its review-fix tests migrate (kernel snapshots take priority).
        let kernel = Arc::new(
            spawn_kernel_job(
                &plan,
                &task_ids,
                state.clone(),
                recovery.as_ref(),
                cancellation.clone(),
            )
            .await?,
        );
        let handle = kernel.watcher();
        self.tasks.lock().await.insert(
            job_id,
            JobTask {
                generation,
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
        let tasks = self.tasks.lock().await;
        let task = tasks
            .get(job_id)
            .ok_or_else(|| "Job is not running on this Agent".to_string())?;
        if task.generation != generation {
            return Err("checkpoint generation does not match running Job".into());
        }
        let (snapshot, source_positions, task_watermarks, watermark_partitions) = task
            .kernel
            .as_ref()
            .ok_or_else(|| "Job kernel handle is missing".to_string())?
            .checkpoint_barrier_with_details(checkpoint_id, generation)
            .await
            .map_err(|error| error.to_string())?;
        let store_uri = task
            .checkpoint_store_uri
            .as_deref()
            .ok_or_else(|| "Job has no checkpoint object_store_uri".to_string())?;
        let repository = CheckpointRepository::new(SharedCheckpointStore::from_uri(store_uri)?);
        let state_ref = repository
            .write_state_snapshot(checkpoint_id, &snapshot)
            .map_err(|error| error.to_string())?;
        let state_refs = task
            .assignments
            .iter()
            .map(|assignment| StateSnapshotRef {
                task_id: assignment.task_id.clone(),
                node_id: Some(node_id.to_owned()),
                ..state_ref.clone()
            })
            .collect::<Vec<_>>();
        let mut coordinator = CheckpointCoordinator::new(
            task.assignments[0].job_id.clone(),
            task.assignments[0].job_version,
            generation,
            snapshot.format_version,
            task.assignments
                .iter()
                .map(|assignment| assignment.task_id.clone()),
        );
        let barrier = coordinator
            .start(checkpoint_id)
            .map_err(|error| error.to_string())?;
        for (index, assignment) in task.assignments.iter().enumerate() {
            coordinator
                .acknowledge(TaskCheckpointAck {
                    task_id: assignment.task_id.clone(),
                    attempt_id: assignment.id.clone(),
                    partition: task
                        .watermark_partitions
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
        let attempts = task
            .assignments
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
        let artifact = repository
            .write_manifest(&manifest, kind, manifest_key)
            .map_err(|error| error.to_string())?;
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
        let tasks = self.tasks.lock().await;
        let task = tasks
            .get(job_id)
            .ok_or_else(|| "Job is not running on this Agent".to_string())?;
        if task.generation != generation {
            return Err("checkpoint generation does not match running Job".into());
        }
        let store_uri = task
            .checkpoint_store_uri
            .as_deref()
            .ok_or_else(|| "Job has no checkpoint object_store_uri".to_string())?;
        let repository = CheckpointRepository::new(SharedCheckpointStore::from_uri(store_uri)?);
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
                job_version: task.assignments[0].job_version,
                format_version: task.state.format_version(),
                created_at_ms: 0,
                status: CheckpointStatus::Completed,
            };
            let manifest = repository
                .read_manifest(&artifact)
                .map_err(|error| error.to_string())?;
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
        let artifact = repository
            .write_manifest_with_plan(&manifest, kind, final_key, &planned_tasks)
            .map_err(|error| error.to_string())?;
        Ok(format!(
            "{}/{}",
            store_uri.trim_end_matches('/'),
            artifact.manifest_key
        ))
    }

    async fn take_finished(&self) -> Vec<(String, u64, Result<(), String>)> {
        let mut finished = Vec::new();
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
                finished.push((job_id, task.generation, result));
            }
        }
        finished
    }

    async fn stop_all(&self) {
        let _start_guard = self.starts.lock().await;
        let tasks = {
            let mut tasks = self.tasks.lock().await;
            std::mem::take(&mut *tasks)
                .into_values()
                .collect::<Vec<_>>()
        };
        for task in tasks {
            task.cancellation.cancel();
            let _ = task.handle.await;
            let _ = task.state.close();
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
            task.cancellation.cancel();
            let _ = task.handle.await;
            let _ = task.state.close();
        }
        Ok(())
    }
}

fn safe_path_component(value: &str) -> String {
    let component: String = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .take(128)
        .collect();
    if component.is_empty() {
        "unknown".into()
    } else {
        component
    }
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
) -> Result<arkflow_core::executor::kernel_handle::KernelJobHandle, String> {
    let mut resource = Resource {
        temporary: HashMap::<String, Arc<dyn Temporary>>::new(),
        input_names: RefCell::new(Vec::new()),
    };
    let mut graph = arkflow_core::executor::graph::ExecutionGraphBuilder::default()
        .with_state(state.clone())
        .build_subgraph(plan, task_ids, &RegistryJobAdapter, &mut resource)
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
    let job_runtime = JobRuntime::default();
    loop {
        if cancellation.is_cancelled() {
            job_runtime.stop_all().await;
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
                    job_runtime.clone(),
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
                return Ok(())
            },
            _ = tokio::time::sleep(backoff) => {}
        }
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
                "job_runtime".into(),
                "state_backend".into(),
                "checkpoint_recovery".into(),
            ],
            boot_id: Some(config.boot_id.clone()),
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
    job_runtime: JobRuntime,
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
                let _ = post_json(client, format!("{}{}{}", config.hub_url, config.api_prefix, "/agent/heartbeat"), &HeartbeatRequest { auth: auth.clone(), state: "draining".into(), protocol_version: Some("v1".into()), software_version: Some(env!("CARGO_PKG_VERSION").into()), capabilities: vec!["stream_lifecycle".into(), "configuration".into(), "metrics".into(), "job_runtime".into(), "state_backend".into(), "checkpoint_recovery".into()], rollout_id: None }).await;
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
            _ = heartbeat.tick() => { post_json(client, format!("{}{}{}", config.hub_url, config.api_prefix, "/agent/heartbeat"), &HeartbeatRequest { auth: auth.clone(), state: if cp.health().is_running() { "online".into() } else { "starting".into() }, protocol_version: Some("v1".into()), software_version: Some(env!("CARGO_PKG_VERSION").into()), capabilities: vec!["stream_lifecycle".into(), "configuration".into(), "metrics".into(), "job_runtime".into(), "state_backend".into(), "checkpoint_recovery".into()], rollout_id: None }).await?; }
            _ = report_tick.tick() => { report_seq = report_seq.saturating_add(1); post_json(client, format!("{}{}{}", config.hub_url, config.api_prefix, "/agent/report"), &report(cp, &auth, &config.boot_id, report_seq, &job_runtime).await).await?; }
            _ = poll.tick() => {
                for (job_id, generation, outcome) in job_runtime.take_finished().await {
                    let (state, error) = match outcome {
                        Ok(()) => ("stopped".into(), None),
                        Err(error) => ("failed".into(), Some(error)),
                    };
                    post_json(
                        client,
                        format!("{}{}{}", config.hub_url, config.api_prefix, "/agent/job-observations"),
                        &crate::hub::JobObservationRequest {
                            auth: auth.clone(),
                            job_id,
                            generation,
                            state,
                            error,
                        },
                    ).await?;
                }
                let query = url::form_urlencoded::Serializer::new(String::new()).append_pair("node_id", &auth.node_id).append_pair("session_token", &auth.session_token).finish();
                let commands: Vec<AgentCommand> = client.get(format!("{}{}{}?{}", config.hub_url, config.api_prefix, "/agent/commands", query)).send().await?.error_for_status()?.json().await?;
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
        capabilities: vec![
            "stream_lifecycle".into(),
            "configuration".into(),
            "metrics".into(),
            "job_runtime".into(),
            "state_backend".into(),
            "checkpoint_recovery".into(),
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
            let (recovery_id, recovery_savepoint) = parse_recovery_payload(payload)?;
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
            jobs: Vec::new(),
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
            (None, false)
        );
        assert_eq!(
            parse_recovery_payload(&serde_json::json!({
                "recovery": {
                    "checkpoint_id": "cp-1",
                    "savepoint": true
                }
            }))
            .unwrap(),
            (Some("cp-1".into()), true)
        );
        assert!(parse_recovery_payload(&serde_json::json!({
            "recovery": {}
        }))
        .is_err());
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
            node_token: "token".into(),
            boot_id: "boot-1".into(),
            heartbeat_interval: Duration::from_secs(5),
            report_interval: Duration::from_secs(5),
            poll_interval: Duration::from_secs(5),
        };
        let error = execute_job_operation(&command, &config, &JobRuntime::default())
            .await
            .unwrap_err();
        assert_eq!(error, "missing checkpoint payload");
    }
}
