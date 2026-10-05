//! Shared checkpoint store and repository: object-store-backed
//! CheckpointStore implementation, the process-wide checkpoint worker,
//! repository construction, and recovery artifact validation/restore.
use arkflow_core::checkpoint::{
    recovery_manifest_key, CheckpointRepository, CheckpointStatus, CheckpointStore,
    RecoveryArtifact, RecoveryArtifactKind,
};
use arkflow_core::job::JobPlan;
use arkflow_core::state::StateBackend;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use url::Url;

#[derive(Clone)]
pub(super) struct SharedCheckpointStore {
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
        // Reply via a std channel: the caller may sit on a runtime worker
        // thread, so the wait must be a plain park (never an async-only
        // primitive). Semantics match the previous per-op thread join.
        let (reply_tx, reply_rx) = std::sync::mpsc::channel::<Result<T, object_store::Error>>();
        let wrapped = async move {
            let outcome = future.await;
            let _ = reply_tx.send(outcome);
        };
        checkpoint_worker()
            .send(CheckpointJob {
                future: Box::pin(wrapped),
            })
            .map_err(|_| arkflow_core::Error::Process("checkpoint worker unavailable".into()))?;
        reply_rx
            .recv()
            .map_err(|_| {
                arkflow_core::Error::Process("checkpoint object store thread panicked".into())
            })?
            .map_err(|error| {
                arkflow_core::Error::Process(format!("checkpoint object store: {error}"))
            })
    }
}

/// One type-erased checkpoint operation for the shared worker thread.
pub(super) struct CheckpointJob {
    pub(super) future: Pin<Box<dyn Future<Output = ()> + Send>>,
}

/// Process-wide checkpoint worker: a single OS thread with its own
/// current-thread runtime drains a bounded command channel, so checkpoint
/// object-store I/O costs neither a thread nor a runtime per operation.
/// Each job is spawned onto that runtime, so operations run concurrently
/// (one Job's slow S3 write cannot delay another Job's recovery read) and
/// a panicking job is isolated inside its own task. Commands are
/// panic-isolated individually, mirroring the storage actor.
pub(super) fn checkpoint_worker() -> &'static flume::Sender<CheckpointJob> {
    static WORKER: OnceLock<flume::Sender<CheckpointJob>> = OnceLock::new();
    WORKER.get_or_init(|| {
        let (sender, receiver) = flume::bounded::<CheckpointJob>(64);
        let spawned = std::thread::Builder::new()
            .name("arkflow-checkpoint-store".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        tracing::error!(%error, "checkpoint worker runtime build failed");
                        return;
                    }
                };
                runtime.block_on(async move {
                    while let Ok(job) = receiver.recv_async().await {
                        let handle = tokio::spawn(job.future);
                        tokio::spawn(async move {
                            if handle.await.is_err() {
                                tracing::error!("checkpoint object store command panicked");
                            }
                        });
                    }
                });
            });
        if let Err(error) = spawned {
            // A spawn failure must not panic inside the OnceLock initializer
            // (that would poison it and turn every later call into a panic).
            // The failed spawn drops the closure and with it the receiver,
            // so every later send fails immediately — `block_on` already
            // maps that to a graceful "worker unavailable" error.
            tracing::error!(
                %error,
                "checkpoint worker thread spawn failed; checkpoint storage unavailable"
            );
        }
        sender
    })
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

pub(super) fn checkpoint_repository(
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

pub(super) fn recovery_artifact(
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

pub(super) fn validate_recovery_manifest(
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

pub(super) fn validate_recovery_snapshots<S: CheckpointStore>(
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
pub(super) fn restore_recovery_state<S: CheckpointStore>(
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

pub(super) fn parse_recovery_payload(
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
