//! High-level entry: run a JobSpec on the unified kernel.
//!
//! One code path for local mode (engine streams compiled to JobSpecs plus
//! YAML-declared jobs) and Agent mode (a subgraph of assigned tasks): compile
//! the plan, build the execution graph through a component adapter, and drive
//! it with `run_graph`.

use crate::executor::graph::ExecutionGraphBuilder;
use crate::executor::kernel_handle::KernelJobRunner;
use crate::executor::task::{
    run_graph, run_graph_with_hooks, run_graph_with_metrics_startup, CheckpointHook,
};
use crate::job::{JobComponentAdapter, JobPlan, JobSpec};
use crate::Error;
use crate::Resource;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Deep-build a local Job without starting it: compile the plan, open the
/// local state backend, and construct the execution graph through the same
/// component path the real startup uses. Every constructed resource is
/// dropped before returning (redb handles release their locks on drop), so
/// the real startup can reopen them. Called before readiness so an invalid
/// component, unsupported backend, or broken graph fails startup visibly.
pub fn validate_local_job(spec: &JobSpec) -> Result<(), Error> {
    let plan = JobPlan::compile(spec.clone())?;
    let state = local_state_backend(&plan)?;
    let builder = match state {
        Some(state) => ExecutionGraphBuilder::default().with_state(state),
        None => ExecutionGraphBuilder::default(),
    };
    let adapter = crate::executor::stream_adapter::StreamJobAdapter::new(None)?;
    let mut resource = crate::Resource {
        temporary: std::collections::HashMap::new(),
        input_names: std::cell::RefCell::new(Vec::new()),
    };
    builder.build(&plan, &adapter, &mut resource)?;
    Ok(())
}

/// Run a whole JobSpec locally (all tasks, single process).
///
/// The graph build consumes `resource` synchronously (component builders use
/// it during construction only) and releases it before the async run, so the
/// returned future is `Send` even though `Resource` itself is not.
pub async fn run_job<A: JobComponentAdapter>(
    spec: &JobSpec,
    adapter: &A,
    resource: &mut Resource,
    cancellation: CancellationToken,
) -> Result<(), Error> {
    run_job_with_metrics(spec, adapter, resource, cancellation, None).await
}

/// Run a local Job through the same command-driven kernel runner used by an
/// Agent.  Jobs with a checkpoint section get an embedded interval driver;
/// jobs without one still use the same graph, state, and event-time setup but
/// do not create a checkpoint task.
pub async fn run_job_with_checkpoints<A: JobComponentAdapter>(
    spec: &JobSpec,
    adapter: &A,
    resource: &mut Resource,
    cancellation: CancellationToken,
) -> Result<(), Error> {
    run_job_with_checkpoints_started(spec, adapter, resource, cancellation, None, None).await
}

/// Run a local Job and notify the caller once the graph's resource startup
/// has completed. The notification is used by the Engine to delay readiness
/// until component construction and resource connection have actually
/// succeeded; the ordinary API keeps the notification optional for existing
/// callers. When `metrics_registry` is `Some`, the spawned Job's
/// `KernelMetrics` are registered under the Job id for observability export
/// and unregistered once the run finishes.
pub async fn run_job_with_checkpoints_started<A: JobComponentAdapter>(
    spec: &JobSpec,
    adapter: &A,
    resource: &mut Resource,
    cancellation: CancellationToken,
    mut startup: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
    metrics_registry: Option<crate::runtime::JobMetricsRegistry>,
) -> Result<(), Error> {
    let plan = JobPlan::compile(spec.clone())?;
    let checkpoint_root = spec
        .checkpoint
        .as_ref()
        .map(|checkpoint| local_checkpoint_root(&checkpoint.object_store_uri))
        .transpose()?;
    let recovery_required = durable_local_recovery_required(&plan);
    let recovery_manifest = if recovery_required {
        let Some(root) = checkpoint_root.as_deref() else {
            return Err(Error::Config(
                "durable local Job state requires a checkpoint artifact before restart".into(),
            ));
        };
        Some(latest_local_checkpoint(root, &plan)?.ok_or_else(|| {
            Error::Config(format!(
                "durable local Job '{}' requires recovery but no compatible checkpoint was found",
                plan.spec.id
            ))
        })?)
    } else {
        None
    };
    let mut rescale_context = None;
    if let Some(manifest) = recovery_manifest.as_ref() {
        match validate_manifest_task_compatibility(manifest, &plan) {
            Ok(()) => {}
            Err(error) if plan.spec.rescale => {
                rescale_context = Some(RescaleContext::from_plan(&plan)?);
                tracing::info!(
                    job = %plan.spec.id,
                    "rescale recovery: redistributing keyed state across the new task set ({error})"
                );
            }
            Err(error) => return Err(error),
        }
    }
    let state = local_state_backend(&plan)?;
    let builder = match state.clone() {
        Some(state) => ExecutionGraphBuilder::default().with_state(state),
        None => ExecutionGraphBuilder::default(),
    };
    let mut graph = builder.build(&plan, adapter, resource)?;
    graph.temporaries = resource.temporary.values().cloned().collect();
    let inputs = graph
        .chains
        .iter()
        .filter_map(|chain| chain.source.clone())
        .collect::<Vec<_>>();
    // Local checkpoint manifests are validated against the logical Job plan,
    // while adjacent stateless processors may be fused into one runtime
    // chain. Record every logical task so a fused graph still produces the
    // exact task set required for recovery.
    let participants = plan
        .tasks
        .iter()
        .map(|task| task.id.clone())
        .collect::<Vec<_>>();
    let states = state_map(&plan, state.clone());
    let mut watermark_gates = event_time_gates(&graph)?;
    let mut prepared_inputs = false;
    if let Some(manifest) = recovery_manifest.as_ref() {
        let Some(root) = checkpoint_root.as_deref() else {
            return Err(Error::Config(
                "local checkpoint recovery requires a checkpoint root".into(),
            ));
        };
        let state = state.as_ref().ok_or_else(|| {
            Error::Config("local checkpoint recovery requires a state backend".into())
        })?;
        let repository = crate::checkpoint::CheckpointRepository::new(
            crate::checkpoint::FileCheckpointStore::new(root)?,
        );
        let namespace_prefix =
            crate::job::state_namespace_prefix(&plan.spec.id, plan.spec.state.as_ref());
        restore_local_snapshot(&repository, manifest, state, &namespace_prefix, rescale_context.as_ref())?;
        for input in &inputs {
            if let Err(error) = input.connect().await {
                close_inputs(&inputs).await;
                let _ = startup
                    .take()
                    .map(|sender| sender.send(Err(error.to_string())));
                return Err(error);
            }
        }
        if let Err(error) = seed_event_time_partitions(&graph, &watermark_gates).await {
            close_inputs(&inputs).await;
            let _ = state.close();
            let _ = startup
                .take()
                .map(|sender| sender.send(Err(error.to_string())));
            return Err(error);
        }
        for input in &inputs {
            if let Err(error) = input.restore_positions(&manifest.source_positions).await {
                close_inputs(&inputs).await;
                let _ = startup
                    .take()
                    .map(|sender| sender.send(Err(error.to_string())));
                return Err(error);
            }
        }
        restore_event_time_watermarks(
            &graph,
            &watermark_gates,
            &manifest.watermarks_ms,
            &manifest.watermark_partitions,
        )
        .await;
        prepared_inputs = true;
    }
    let start_marker = if durable_local_state(&plan) {
        local_state_start_marker(&plan)
    } else {
        None
    };
    if let Some(marker) = start_marker.as_deref() {
        if let Err(error) = persist_start_marker(marker) {
            if prepared_inputs {
                close_inputs(&inputs).await;
            }
            if let Some(state) = state.as_ref() {
                let _ = state.close();
            }
            let error = Error::Process(format!(
                "durable local Job could not persist its start marker: {error}"
            ));
            let _ = startup
                .take()
                .map(|sender| sender.send(Err(error.to_string())));
            return Err(error);
        }
    }
    let spawned_result = if prepared_inputs {
        KernelJobRunner::spawn_prepared_with_cancellation_and_state_format(
            graph,
            inputs.clone(),
            states,
            watermark_gates,
            plan.spec
                .state
                .as_ref()
                .map(|state| state.format_version)
                .unwrap_or(1),
            cancellation.clone(),
        )
        .await
    } else {
        KernelJobRunner::spawn_with_cancellation_and_state_format(
            graph,
            inputs.clone(),
            states,
            std::mem::take(&mut watermark_gates),
            false,
            plan.spec
                .state
                .as_ref()
                .map(|state| state.format_version)
                .unwrap_or(1),
            cancellation.clone(),
        )
        .await
    };
    let spawned = match spawned_result {
        Ok(handle) => handle,
        Err(error) => {
            if let Some(marker) = start_marker.as_deref() {
                remove_start_marker(marker);
            }
            if prepared_inputs {
                close_inputs(&inputs).await;
            }
            if let Some(state) = state.as_ref() {
                let _ = state.close();
            }
            let _ = startup
                .take()
                .map(|sender| sender.send(Err(error.to_string())));
            return Err(error);
        }
    };
    let handle = Arc::new(spawned);
    if let Some(registry) = metrics_registry.as_ref() {
        registry.register(spec.id.as_str(), handle.metrics());
    }
    if let Some(startup) = startup.take() {
        let _ = startup.send(Ok(()));
    }

    let checkpoint_stop = CancellationToken::new();
    let checkpoint_task = spec.checkpoint.as_ref().map(|checkpoint| {
        let handle = handle.clone();
        let plan = plan.clone();
        let participants = participants.clone();
        let stop = checkpoint_stop.clone();
        let checkpoint = checkpoint.clone();
        tokio::spawn(async move {
            run_local_checkpoint_loop(handle, plan, participants, checkpoint, stop).await;
        })
    });

    let result = handle
        .watcher()
        .await
        .map_err(|error| Error::Process(format!("local Job runner task failed: {error}")))?;
    if let Some(registry) = metrics_registry.as_ref() {
        registry.unregister(spec.id.as_str());
    }
    checkpoint_stop.cancel();
    if let Some(task) = checkpoint_task {
        let _ = task.await;
    }
    if let Some(state) = state {
        state.close()?;
    }
    result
}

/// Run a JobSpec with runtime metrics: per-batch counters update the shared
/// `RuntimeMetrics` (input/processing/output/errors) so control-plane
/// snapshots observe kernel activity.
pub async fn run_job_with_metrics<A: JobComponentAdapter>(
    spec: &JobSpec,
    adapter: &A,
    resource: &mut Resource,
    cancellation: CancellationToken,
    metrics: Option<std::sync::Arc<crate::runtime::RuntimeMetrics>>,
) -> Result<(), Error> {
    run_job_with_metrics_started(spec, adapter, resource, cancellation, metrics, None).await
}

/// Metrics-enabled local Job runner with an optional startup handshake. The
/// sender is completed by the graph runner only after temporary stores,
/// sources, sinks, and state backends have connected successfully.
pub async fn run_job_with_metrics_started<A: JobComponentAdapter>(
    spec: &JobSpec,
    adapter: &A,
    resource: &mut Resource,
    cancellation: CancellationToken,
    metrics: Option<std::sync::Arc<crate::runtime::RuntimeMetrics>>,
    startup: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
) -> Result<(), Error> {
    let plan = JobPlan::compile(spec.clone())?;
    let state = local_state_backend(&plan)?;
    let builder = match state.clone() {
        Some(state) => ExecutionGraphBuilder::default().with_state(state),
        None => ExecutionGraphBuilder::default(),
    };
    let mut graph = builder.build(&plan, adapter, resource)?;
    graph.temporaries = resource.temporary.values().cloned().collect();
    let result = run_graph_with_metrics_startup(graph, cancellation, metrics, startup).await;
    if let Some(state) = state {
        state.close()?;
    }
    result
}

/// Run a JobPlan's assigned task subset (Agent mode). The assignment must not
/// split an edge across the co-location boundary.
pub async fn run_job_tasks<A: JobComponentAdapter>(
    plan: &JobPlan,
    task_ids: &[String],
    adapter: &A,
    resource: &mut Resource,
    cancellation: CancellationToken,
) -> Result<(), Error> {
    let mut graph =
        ExecutionGraphBuilder::default().build_subgraph(plan, task_ids, adapter, resource, None)?;
    graph.temporaries = resource.temporary.values().cloned().collect();
    run_graph(graph, cancellation).await
}

/// Run a whole JobSpec with per-chain checkpoint hooks (entry task id →
/// hook). Used by callers that drive a `BarrierCoordinator`.
pub async fn run_job_with_hooks<A: JobComponentAdapter>(
    spec: &JobSpec,
    adapter: &A,
    resource: &mut Resource,
    cancellation: CancellationToken,
    hooks: BTreeMap<String, CheckpointHook>,
) -> Result<(), Error> {
    let plan = JobPlan::compile(spec.clone())?;
    let state = local_state_backend(&plan)?;
    let builder = match state {
        Some(state) => ExecutionGraphBuilder::default().with_state(state),
        None => ExecutionGraphBuilder::default(),
    };
    let mut graph = builder.build(&plan, adapter, resource)?;
    graph.temporaries = resource.temporary.values().cloned().collect();
    run_graph_with_hooks(graph, cancellation, hooks).await
}

/// Build the durable local state backend described by a Job.  Streams that
/// only use the compatibility window operator are allowed to omit a Job state
/// section; the graph builder supplies an in-memory backend for that case.
fn local_state_backend(
    plan: &JobPlan,
) -> Result<Option<std::sync::Arc<dyn crate::state::StateBackend>>, Error> {
    let Some(state) = &plan.spec.state else {
        return Ok(None);
    };
    if state.backend != "embedded_kv" && state.backend != "redb" {
        return Err(Error::Config(format!(
            "local Job state backend '{}' is not supported; use embedded_kv",
            state.backend
        )));
    }
    let root = local_state_root(plan).expect("state section checked above");
    let backend = crate::state::RedbStateBackend::open(root, state.format_version)?;
    let backend = match state.max_bytes {
        Some(max_bytes) => backend.with_max_bytes(max_bytes),
        None => backend,
    };
    Ok(Some(std::sync::Arc::new(backend)))
}

fn local_state_root(plan: &JobPlan) -> Option<PathBuf> {
    let state = plan.spec.state.as_ref()?;
    let base = match state.durability {
        crate::job::StateDurability::Durable => crate::job::configured_state_root(state),
        crate::job::StateDurability::Ephemeral => std::env::temp_dir()
            .join("arkflow-ephemeral-job-state")
            .join(ephemeral_state_nonce()),
    };
    Some(
        base.join("jobs")
            .join(plan.spec.id.as_str())
            .join(format!("version-{}", plan.spec.version.0)),
    )
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

fn local_state_start_marker(plan: &JobPlan) -> Option<PathBuf> {
    Some(local_state_root(plan)?.join(".arkflow-started"))
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

fn durable_local_recovery_required(plan: &JobPlan) -> bool {
    durable_local_state(plan)
        && local_state_start_marker(plan).is_some_and(|marker| marker.is_file())
}

fn durable_local_state(plan: &JobPlan) -> bool {
    plan.spec
        .state
        .as_ref()
        .is_some_and(|state| state.durability == crate::job::StateDurability::Durable)
        && plan.spec.requires_state()
        && plan.spec.checkpoint.is_some()
}

fn state_map(
    plan: &JobPlan,
    state: Option<Arc<dyn crate::state::StateBackend>>,
) -> BTreeMap<String, Arc<dyn crate::state::StateBackend>> {
    let Some(state) = state else {
        return BTreeMap::new();
    };
    plan.tasks
        .iter()
        .filter(|task| {
            plan.spec
                .operators
                .iter()
                .find(|operator| operator.id == task.operator_id)
                .is_some_and(|operator| {
                    operator.stateful || operator.kind == crate::job::OperatorKind::Window
                })
        })
        .map(|task| (task.id.clone(), state.clone()))
        .collect()
}

fn event_time_gates(
    graph: &crate::executor::graph::ExecutionGraph,
) -> Result<
    BTreeMap<
        String,
        Arc<tokio::sync::Mutex<Option<crate::executor::event_time_gate::EventTimeGate>>>,
    >,
    Error,
> {
    let mut gates = BTreeMap::new();
    let mut shared_trackers =
        BTreeMap::<String, Arc<std::sync::Mutex<crate::event_time::WatermarkTracker>>>::new();
    for chain in &graph.chains {
        let Some(source_time) = chain
            .source_time
            .as_ref()
            .filter(|time| time.mode == crate::job::TimeMode::EventTime)
        else {
            continue;
        };
        let group = chain
            .watermark_group
            .clone()
            .unwrap_or_else(|| chain.entry_task_id().to_owned());
        // The group is the identity of the downstream watermark component.
        // Source edges feeding the same Window must classify lateness against
        // one shared minimum, even when their local source contracts or
        // timing vectors were constructed independently.
        let tracker_key = group;
        let tracker = match shared_trackers.entry(tracker_key) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.get().clone(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                let tracker = crate::event_time::WatermarkTracker::from_time_spec(source_time)?;
                let tracker = Arc::new(std::sync::Mutex::new(tracker));
                entry.insert(tracker.clone());
                tracker
            }
        };
        let gate = crate::executor::event_time_gate::EventTimeGate::new_with_shared_tracker(
            source_time,
            chain.window_timings.clone(),
            tracker,
        )?;
        gates.insert(
            chain.entry_task_id().to_owned(),
            Arc::new(tokio::sync::Mutex::new(Some(gate))),
        );
    }
    Ok(gates)
}

/// Seed every event-time gate with the connector's complete assignment before
/// restoring checkpointed progress.  A Kafka reader may have several idle
/// physical partitions; leaving those partitions out would let the first fast
/// partition advance the shared minimum before the idle partition's first
/// record arrives.
async fn seed_event_time_partitions(
    graph: &crate::executor::graph::ExecutionGraph,
    gates: &BTreeMap<
        String,
        Arc<tokio::sync::Mutex<Option<crate::executor::event_time_gate::EventTimeGate>>>,
    >,
) -> Result<(), Error> {
    for chain in &graph.chains {
        let Some(gate) = gates.get(chain.entry_task_id()) else {
            continue;
        };
        let Some(source) = chain.source.as_ref() else {
            continue;
        };
        let source_id = chain.entry_task_id();
        let mut partitions = source
            .watermark_partitions()
            .await?
            .into_iter()
            .map(|partition| partition.with_source_identity(source_id))
            .collect::<Vec<_>>();
        if partitions.is_empty() {
            if let Some(partition) = chain.source_partition {
                partitions.push(crate::event_time::EventTimePartition::for_source(
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
    Ok(())
}

async fn restore_event_time_watermarks(
    graph: &crate::executor::graph::ExecutionGraph,
    gates: &BTreeMap<
        String,
        Arc<tokio::sync::Mutex<Option<crate::executor::event_time_gate::EventTimeGate>>>,
    >,
    watermarks_ms: &BTreeMap<String, i64>,
    watermark_partitions: &BTreeMap<String, Vec<crate::checkpoint::WatermarkPosition>>,
) {
    for (task_id, partitions) in watermark_partitions {
        if let Some(gate) = gates.get(task_id) {
            let mut gate = gate.lock().await;
            if let Some(gate) = gate.as_mut() {
                for partition in partitions {
                    gate.restore_partition_key(
                        &crate::event_time::EventTimePartition::new(
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
    for chain in &graph.chains {
        if watermark_partitions
            .get(chain.entry_task_id())
            .is_some_and(|partitions| !partitions.is_empty())
        {
            continue;
        }
        let Some(watermark) = watermarks_ms.get(chain.entry_task_id()) else {
            continue;
        };
        if let Some(gate) = gates.get(chain.entry_task_id()) {
            let mut gate_guard = gate.lock().await;
            if let Some(gate) = gate_guard.as_mut() {
                let known = gate.known_partitions();
                if known.is_empty() {
                    let partition = crate::event_time::EventTimePartition::for_source(
                        chain.entry_task_id(),
                        chain.source_partition.unwrap_or(0),
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

async fn close_inputs(inputs: &[Arc<dyn crate::input::Input>]) {
    for input in inputs.iter().rev() {
        if let Err(error) = input.close().await {
            tracing::warn!(%error, "failed to close input after local Job startup failure");
        }
    }
}

/// Fail closed when a recovery artifact was written under a different task
/// set than the current plan: keyed state namespaces embed task ids, so a
/// parallelism (or operator-topology) change would silently strand the old
/// state behind renamed namespaces while source positions still restore.
/// Until key redistribution lands, the operator must keep the parallelism
/// fixed or reset state with a fresh checkpoint/savepoint.
fn validate_manifest_task_compatibility(
    manifest: &crate::checkpoint::CheckpointManifest,
    plan: &JobPlan,
) -> Result<(), Error> {
    let plan_tasks: BTreeSet<&str> = plan.tasks.iter().map(|task| task.id.as_str()).collect();
    let manifest_tasks: BTreeSet<&str> = manifest
        .task_attempts
        .iter()
        .map(|attempt| attempt.task_id.as_str())
        .collect();
    if plan_tasks != manifest_tasks {
        let removed: Vec<&str> = manifest_tasks.difference(&plan_tasks).copied().collect();
        let added: Vec<&str> = plan_tasks.difference(&manifest_tasks).copied().collect();
        return Err(Error::Config(format!(
            "recovery artifact '{}' was written under a different task set than the current plan \
             (parallelism or operator topology changed; removed tasks {:?}, added tasks {:?}); \
             keyed state cannot yet redistribute across a parallelism change — restore the original \
             parallelism or reset state with a fresh checkpoint/savepoint",
            manifest.checkpoint_id, removed, added
        )));
    }
    Ok(())
}

fn latest_local_checkpoint(
    root: &std::path::Path,
    plan: &JobPlan,
) -> Result<Option<crate::checkpoint::CheckpointManifest>, Error> {
    let (directory, prefix, kind) = match plan.spec.recovery {
        crate::job::RecoveryPolicy::LatestSavepoint => (
            root.join("savepoints"),
            "savepoints",
            crate::checkpoint::RecoveryArtifactKind::Savepoint,
        ),
        _ => (
            root.join("checkpoints"),
            "checkpoints",
            crate::checkpoint::RecoveryArtifactKind::Checkpoint,
        ),
    };
    let entries = match std::fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(Error::Process(format!(
                "scan local recovery directory '{}': {error}",
                directory.display()
            )))
        }
    };
    let mut candidates = Vec::new();
    for entry in entries {
        let entry = entry
            .map_err(|error| Error::Process(format!("scan local recovery directory: {error}")))?;
        let path = entry.path().join("manifest.json");
        if !path.is_file() {
            continue;
        }
        let modified = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        candidates.push((modified, path));
    }
    candidates.sort_by_key(|(modified, _)| *modified);
    let repository = crate::checkpoint::CheckpointRepository::new(
        crate::checkpoint::FileCheckpointStore::new(root)?,
    );
    let state_format = plan
        .spec
        .state
        .as_ref()
        .map(|state| state.format_version)
        .unwrap_or(1);
    for (_, path) in candidates.into_iter().rev() {
        let Some(id) = path
            .parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str())
        else {
            continue;
        };
        let artifact = crate::checkpoint::RecoveryArtifact {
            id: id.to_owned(),
            kind,
            manifest_key: format!("{prefix}/{id}/manifest.json"),
            job_version: plan.spec.version,
            format_version: state_format,
            created_at_ms: 0,
            status: crate::checkpoint::CheckpointStatus::Completed,
        };
        let Ok(manifest) = repository.read_manifest(&artifact) else {
            continue;
        };
        if manifest.state_snapshots.is_empty() {
            continue;
        }
        // One shared compatibility evaluation (task membership, formats,
        // version direction, checksum) — the same verdict the Hub and Agent
        // would reach for this artifact.
        let planned_tasks = plan
            .tasks
            .iter()
            .map(|task| task.id.clone())
            .collect::<BTreeSet<_>>();
        let compatibility = crate::checkpoint::evaluate_recovery_compatibility(
            &manifest,
            &plan.spec.id,
            plan.spec.version,
            state_format,
            &planned_tasks,
        );
        if !compatibility.is_compatible() {
            tracing::warn!(
                job_id = %plan.spec.id,
                checkpoint = %manifest.checkpoint_id,
                reason = compatibility.reason.as_deref().unwrap_or("incompatible"),
                "skipping incompatible local recovery artifact"
            );
            continue;
        }
        if let Err(reason) = crate::checkpoint::validate_state_snapshot_task_set(
            &manifest.state_snapshots,
            &planned_tasks,
        ) {
            tracing::warn!(
                job_id = %plan.spec.id,
                checkpoint = %manifest.checkpoint_id,
                expected = ?planned_tasks,
                reason = %reason,
                "skipping local recovery artifact with invalid task snapshots"
            );
            continue;
        }
        let namespace_prefix =
            crate::job::state_namespace_prefix(&plan.spec.id, plan.spec.state.as_ref());
        let snapshots_are_compatible = manifest.state_snapshots.iter().all(|snapshot_ref| {
            repository
                .read_state_snapshot(snapshot_ref)
                .ok()
                .is_some_and(|snapshot| {
                    crate::checkpoint::validate_state_snapshot_namespace(
                        &snapshot,
                        &namespace_prefix,
                    )
                    .is_ok()
                })
        });
        if snapshots_are_compatible {
            return Ok(Some(manifest));
        }
    }
    Ok(None)
}

/// Redistributes keyed state entries across a rescaled task set: each
/// entry's user key is recovered from its operator-specific state-key
/// encoding, hashed with the same normalization the routing path uses, and
/// the entry is rewritten under the new owning task's namespace.
pub(crate) struct RescaleContext {
    plan: JobPlan,
}

impl RescaleContext {
    fn from_plan(plan: &JobPlan) -> Result<Self, Error> {
        Ok(Self { plan: plan.clone() })
    }

    /// The routing-hash input for one state entry: the user key in the exact
    /// byte form `hash_column`/`task_for_key` hash.
    fn routing_key_bytes(
        &self,
        namespace: &str,
        key: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let operator_id = namespace_operator(namespace)?;
        let is_window = self
            .plan
            .spec
            .operators
            .iter()
            .any(|operator| operator.id == operator_id && operator.kind == crate::job::OperatorKind::Window);
        if is_window {
            // Window state key = window_start (8-byte BE) + utf8 user key.
            let user_key = key
                .get(8..)
                .ok_or_else(|| Error::Process("corrupt window state key during rescale".into()))?;
            return Ok(user_key.to_vec());
        }
        // StatefulOperator state key = "<tag>:" + value encoding, where the
        // post-tag bytes are exactly the routing hash input; the null
        // sentinels ("null:<tag>") hash as-is.
        if let Some(rest) = key.strip_prefix(b"utf8:") {
            return Ok(rest.to_vec());
        }
        for tag in [
            &b"binary:"[..],
            b"i8:",
            b"i16:",
            b"i32:",
            b"i64:",
            b"u8:",
            b"u16:",
            b"u32:",
            b"u64:",
        ] {
            if let Some(rest) = key.strip_prefix(tag) {
                return Ok(rest.to_vec());
            }
        }
        if key.starts_with(b"null:") {
            return Ok(key.to_vec());
        }
        Err(Error::Process(format!(
            "state entry for operator '{operator_id}' uses an unrecognized key encoding;              rescale redistribution cannot derive its routing key"
        )))
    }

    fn redistribute(
        &self,
        entry: crate::state::StateEntry,
    ) -> Result<crate::state::StateEntry, Error> {
        let operator_id = namespace_operator(&entry.namespace)?;
        let routing_key = self.routing_key_bytes(&entry.namespace, &entry.key)?;
        let group = crate::job::key_group_for_key(&routing_key, self.plan.spec.max_parallelism)?;
        let owner = self
            .plan
            .tasks
            .iter()
            .find(|task| {
                task.operator_id == operator_id
                    && task
                        .partitions
                        .iter()
                        .any(|partition| partition.key_group.contains(group))
            })
            .ok_or_else(|| {
                Error::Process(format!(
                    "rescale found no owner task for key group {group} of operator '{operator_id}'"
                ))
            })?;
        let new_namespace = crate::job::effective_state_namespace(
            &self.plan.spec.id,
            self.plan.spec.state.as_ref(),
            &operator_id,
            &owner.id,
        );
        Ok(crate::state::StateEntry {
            namespace: new_namespace,
            ..entry
        })
    }
}

/// Extract the operator-id segment from a state namespace built by
/// `effective_state_namespace` (`...:operator:<id>:task:<id>` with
/// percent-encoded components).
fn namespace_operator(namespace: &str) -> Result<String, Error> {
    let marker = ":operator:";
    let start = namespace
        .find(marker)
        .ok_or_else(|| Error::Process("state namespace lacks an operator segment".into()))?
        + marker.len();
    let rest = &namespace[start..];
    let end = rest.find(":task:").unwrap_or(rest.len());
    Ok(rest[..end].replace("%3A", ":").replace("%25", "%"))
}

fn restore_local_snapshot<S: crate::checkpoint::CheckpointStore>(
    repository: &crate::checkpoint::CheckpointRepository<S>,
    manifest: &crate::checkpoint::CheckpointManifest,
    state: &Arc<dyn crate::state::StateBackend>,
    namespace_prefix: &str,
    rescale: Option<&RescaleContext>,
) -> Result<(), Error> {
    let mut entries = BTreeMap::<(String, Vec<u8>), crate::state::StateEntry>::new();
    for snapshot_ref in &manifest.state_snapshots {
        let snapshot = repository.read_state_snapshot(snapshot_ref)?;
        crate::checkpoint::validate_state_snapshot_namespace(&snapshot, namespace_prefix)
            .map_err(Error::Config)?;
        if snapshot.format_version != state.format_version() {
            return Err(Error::Config(format!(
                "checkpoint state format {} is incompatible with local backend format {}",
                snapshot.format_version,
                state.format_version()
            )));
        }
        for entry in snapshot.entries {
            let entry = match rescale {
                Some(context) => context.redistribute(entry)?,
                None => entry,
            };
            entries.insert((entry.namespace.clone(), entry.key.clone()), entry);
        }
    }
    let snapshot =
        crate::state::StateSnapshot::new(state.format_version(), entries.into_values().collect());
    state.restore(&snapshot)
}

fn local_checkpoint_catalog(
    root: &std::path::Path,
    store: &crate::checkpoint::FileCheckpointStore,
    plan: &JobPlan,
) -> crate::checkpoint::CheckpointCatalog {
    let mut catalog = crate::checkpoint::CheckpointCatalog::default();
    let directory = root.join("checkpoints");
    let Ok(entries) = std::fs::read_dir(directory) else {
        return catalog;
    };
    let repository = crate::checkpoint::CheckpointRepository::new(store.clone());
    let format_version = plan
        .spec
        .state
        .as_ref()
        .map(|state| state.format_version)
        .unwrap_or(1);
    for entry in entries.flatten() {
        let Some(id) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let path = entry.path().join("manifest.json");
        if !path.is_file() {
            continue;
        }
        let artifact = crate::checkpoint::RecoveryArtifact {
            id: id.clone(),
            kind: crate::checkpoint::RecoveryArtifactKind::Checkpoint,
            manifest_key: format!("checkpoints/{id}/manifest.json"),
            job_version: plan.spec.version,
            format_version,
            created_at_ms: entry
                .metadata()
                .and_then(|metadata| metadata.modified())
                .ok()
                .and_then(|modified| {
                    modified
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .ok()
                })
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or_default(),
            status: crate::checkpoint::CheckpointStatus::Completed,
        };
        if let Ok(manifest) = repository.read_manifest(&artifact) {
            if manifest.job_id == plan.spec.id
                && manifest.job_version <= plan.spec.version
                && manifest.format_version == format_version
            {
                catalog.record(artifact);
            }
        }
    }
    catalog
}

async fn run_local_checkpoint_loop(
    handle: Arc<crate::executor::kernel_handle::KernelJobHandle>,
    plan: JobPlan,
    participants: Vec<String>,
    checkpoint: crate::job::CheckpointSpec,
    stop: CancellationToken,
) {
    let root = match local_checkpoint_root(&checkpoint.object_store_uri) {
        Ok(root) => root,
        Err(error) => {
            tracing::error!(job_id = %plan.spec.id, %error, "local Job checkpointing disabled");
            return;
        }
    };
    let store = match crate::checkpoint::FileCheckpointStore::new(&root) {
        Ok(store) => store,
        Err(error) => {
            tracing::error!(job_id = %plan.spec.id, %error, "local Job checkpoint store could not be opened");
            return;
        }
    };
    let mut catalog = local_checkpoint_catalog(&root, &store, &plan);
    let mut ticker =
        tokio::time::interval(std::time::Duration::from_millis(checkpoint.interval_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sequence = 0u64;
    loop {
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = ticker.tick() => {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|duration| duration.as_millis())
                    .unwrap_or(sequence as u128);
                let checkpoint_id = format!("local-{}-{timestamp}-{sequence}", plan.spec.id);
                sequence = sequence.saturating_add(1);
                match handle.checkpoint_barrier_with_details(checkpoint_id.clone(), 0).await {
                    Ok((snapshot, source_positions, watermarks_ms, watermark_partitions)) => {
                        if let Err(error) = persist_local_checkpoint(
                            &store,
                            &mut catalog,
                            &plan,
                            &participants,
                            &checkpoint,
                            snapshot,
                            source_positions,
                            watermarks_ms,
                            watermark_partitions,
                            &checkpoint_id,
                        ) {
                            tracing::warn!(job_id = %plan.spec.id, %error, "local Job checkpoint failed; data processing continues");
                        }
                    }
                    Err(error) => {
                        tracing::warn!(job_id = %plan.spec.id, %error, "local Job barrier failed; data processing continues");
                    }
                }
            }
        }
    }
}

fn persist_local_checkpoint(
    store: &crate::checkpoint::FileCheckpointStore,
    catalog: &mut crate::checkpoint::CheckpointCatalog,
    plan: &JobPlan,
    participants: &[String],
    checkpoint: &crate::job::CheckpointSpec,
    snapshot: crate::state::StateSnapshot,
    source_positions: Vec<crate::checkpoint::SourcePosition>,
    watermarks_ms: BTreeMap<String, i64>,
    watermark_partitions: BTreeMap<String, Vec<crate::checkpoint::WatermarkPosition>>,
    checkpoint_id: &str,
) -> Result<(), Error> {
    let repository = crate::checkpoint::CheckpointRepository::new(store.clone());
    let state_ref = repository.write_state_snapshot(checkpoint_id, &snapshot)?;
    let state_refs = participants
        .iter()
        .map(|task_id| crate::checkpoint::StateSnapshotRef {
            task_id: task_id.clone(),
            ..state_ref.clone()
        })
        .collect::<Vec<_>>();
    let mut coordinator = crate::checkpoint::CheckpointCoordinator::new(
        plan.spec.id.clone(),
        plan.spec.version,
        0,
        snapshot.format_version,
        participants.iter().cloned(),
    );
    let barrier = coordinator.start(checkpoint_id.to_owned())?;
    let mut positions_pending = Some(source_positions);
    for task_id in participants {
        let is_source = plan
            .task(task_id)
            .and_then(|task| {
                plan.spec
                    .operators
                    .iter()
                    .find(|operator| operator.id == task.operator_id)
            })
            .is_some_and(|operator| operator.kind == crate::job::OperatorKind::Source);
        coordinator.acknowledge(crate::checkpoint::TaskCheckpointAck {
            task_id: task_id.clone(),
            attempt_id: format!("{task_id}:local:0"),
            partition: plan
                .task(task_id)
                .and_then(|task| task.partitions.first())
                .map(|partition| partition.id)
                .unwrap_or_default(),
            checkpoint_id: barrier.checkpoint_id.clone(),
            generation: barrier.generation,
            state: snapshot.clone(),
            source_positions: if is_source {
                positions_pending.take().unwrap_or_default()
            } else {
                Vec::new()
            },
            watermark_ms: watermarks_ms.get(task_id).copied(),
            watermark_partitions: watermark_partitions
                .get(task_id)
                .cloned()
                .unwrap_or_default(),
        })?;
    }
    let attempts = participants
        .iter()
        .map(|task_id| crate::checkpoint::TaskAttemptSnapshot {
            task_id: task_id.clone(),
            attempt_id: format!("{task_id}:local:0"),
            node_id: "local".into(),
        })
        .collect::<Vec<_>>();
    let manifest = coordinator.complete(attempts, state_refs)?;
    let planned_tasks = participants.iter().cloned().collect::<BTreeSet<_>>();
    let artifact = repository.write_checkpoint_with_plan(&manifest, &planned_tasks)?;
    catalog.record(artifact);
    for removed in catalog.retain_checkpoints(checkpoint.retention as usize) {
        repository.delete(&removed)?;
    }
    Ok(())
}

fn local_checkpoint_root(uri: &str) -> Result<PathBuf, Error> {
    if let Some(path) = uri.strip_prefix("file://") {
        let path = PathBuf::from(path);
        if path.as_os_str().is_empty() {
            return Err(Error::Config(
                "file checkpoint URI has an empty path".into(),
            ));
        }
        return Ok(path);
    }
    if uri.contains("://") {
        return Err(Error::Config(format!(
            "local Job checkpoint store '{}' is not local; use file:///path",
            uri
        )));
    }
    Ok(PathBuf::from(uri))
}

/// Convenience for building the shared `Resource` outside the executor.
pub fn shared_resource(resource: Resource) -> std::sync::Arc<Resource> {
    std::sync::Arc::new(resource)
}

#[cfg(test)]
mod validation_tests {
    use super::*;

    #[test]
    fn rescale_across_parallelism_fails_closed_on_recovery() {
        let mut spec = crate::job::JobSpec {
            rescale: false,
            rebalance: None,
            placement: crate::job::PlacementStrategy::Colocated,
            id: JobId::new("rescale-guard").unwrap(),
            version: JobVersion(1),
            max_parallelism: 8,
            parallelism: 1,
            operators: vec![
                OperatorSpec {
                    id: "source".into(),
                    kind: OperatorKind::Source,
                    stateful: false,
                    key_field: None,
                    config: serde_json::json!({}),
                },
                OperatorSpec {
                    id: "sink".into(),
                    kind: OperatorKind::Sink,
                    stateful: false,
                    key_field: None,
                    config: serde_json::json!({}),
                },
            ],
            edges: vec![crate::job::EdgeSpec {
                id: "edge".into(),
                from: "source".into(),
                to: "sink".into(),
                partitioned: false,
            }],
            sources: vec![SourceSpec {
                operator_id: "source".into(),
                input_type: "vec".into(),
                codec: None,
                config: serde_json::json!({}),
                time: crate::job::TimeSpec {
                    mode: crate::job::TimeMode::ProcessingTime,
                    timestamp_field: None,
                    watermark: None,
                    allowed_lateness_ms: 0,
                    late_event_policy: Default::default(),
                    late_event_route: None,
                },
            }],
            sinks: vec![SinkSpec {
                operator_id: "sink".into(),
                output_type: "collect".into(),
                codec: None,
                config: serde_json::json!({}),
            }],
            state: None,
            checkpoint: None,
            recovery: Default::default(),
        };
        let manifest = crate::checkpoint::CheckpointManifest {
            checkpoint_id: "c-1".into(),
            job_id: spec.id.clone(),
            job_version: spec.version,
            generation: 1,
            task_attempts: vec![
                crate::checkpoint::TaskAttemptSnapshot {
                    task_id: "source-0".into(),
                    attempt_id: "source-0:n1:0".into(),
                    node_id: "n1".into(),
                },
                crate::checkpoint::TaskAttemptSnapshot {
                    task_id: "sink-0".into(),
                    attempt_id: "sink-0:n1:0".into(),
                    node_id: "n1".into(),
                },
            ],
            source_positions: Vec::new(),
            watermarks_ms: Default::default(),
            watermark_partitions: Default::default(),
            in_flight_barrier: crate::checkpoint::CheckpointBarrier {
                checkpoint_id: "c-1".into(),
                generation: 1,
                trace_context: None,
            },
            state_snapshots: Vec::new(),
            format_version: 1,
            checksum: 0,
        };
        // Same parallelism: compatible.
        let same = JobPlan::compile(spec.clone()).unwrap();
        assert!(validate_manifest_task_compatibility(&manifest, &same).is_ok());
        // Parallelism change: fail closed with an actionable error.
        spec.parallelism = 2;
        let rescaled = JobPlan::compile(spec).unwrap();
        let error = validate_manifest_task_compatibility(&manifest, &rescaled)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("parallelism or operator topology changed"),
            "{error}"
        );
        assert!(error.contains("fresh checkpoint"), "{error}");
    }

    fn rescale_job_spec(parallelism: u32) -> JobSpec {
        let mut spec = local_spec("vec", "collect");
        spec.id = JobId::new("rescale-job").unwrap();
        spec.max_parallelism = 16;
        spec.parallelism = parallelism;
        spec.state = Some(StateSpec {
            backend: "embedded_kv".into(),
            durability: StateDurability::Ephemeral,
            root: None,
            namespace: None,
            ttl_ms: None,
            format_version: 1,
            max_pending_transactions: None,
            max_bytes: None,
        });
        spec.operators = vec![
            OperatorSpec {
                id: "source".into(),
                kind: OperatorKind::Source,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
            OperatorSpec {
                id: "agg".into(),
                kind: OperatorKind::Aggregate,
                stateful: true,
                key_field: Some("key".into()),
                config: serde_json::json!({}),
            },
            OperatorSpec {
                id: "sink".into(),
                kind: OperatorKind::Sink,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
        ];
        spec.edges = vec![
            crate::job::EdgeSpec {
                id: "e1".into(),
                from: "source".into(),
                to: "agg".into(),
                partitioned: true,
            },
            crate::job::EdgeSpec {
                id: "e2".into(),
                from: "agg".into(),
                to: "sink".into(),
                partitioned: false,
            },
        ];
        spec
    }

    #[test]
    fn rescale_redistributes_stateful_entries_by_key_group() {
        // Snapshot written under parallelism 1; restart under parallelism 4:
        // every entry must land on the task that owns its key group.
        let old_spec = rescale_job_spec(1);
        let old_plan = JobPlan::compile(old_spec).unwrap();
        let new_plan = JobPlan::compile(rescale_job_spec(4)).unwrap();
        let context = RescaleContext::from_plan(&new_plan).unwrap();

        let old_task = old_plan
            .tasks
            .iter()
            .find(|task| task.operator_id == "agg")
            .unwrap();
        let old_namespace = crate::job::effective_state_namespace(
            &old_plan.spec.id,
            None,
            "agg",
            &old_task.id,
        );
        for user_key in ["alpha", "beta", "gamma", "delta"] {
            let state_key = format!("utf8:{user_key}").into_bytes();
            let entry = crate::state::StateEntry {
                namespace: old_namespace.clone(),
                key: state_key.clone(),
                value: b"42".to_vec(),
                expires_at_ms: None,
            };
            let moved = context.redistribute(entry).unwrap();
            // The new namespace must belong to the new owning task.
            let group = crate::job::key_group_for_key(user_key.as_bytes(), 16).unwrap();
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
            let expected = crate::job::effective_state_namespace(
                &new_plan.spec.id,
                None,
                "agg",
                &owner.id,
            );
            assert_eq!(moved.namespace, expected, "key {user_key}");
            assert_eq!(moved.key, state_key);
            assert_eq!(moved.value, b"42".to_vec());
        }
    }

    #[test]
    fn rescale_redistributes_window_entries_stripping_window_start() {
        let old_plan = JobPlan::compile(rescale_job_spec(2)).unwrap();
        let mut window_spec = rescale_job_spec(4);
        // Turn the aggregate into a window operator so the window key layout
        // (window_start + utf8 key) applies.
        window_spec
            .operators
            .retain(|operator| operator.id != "agg");
        window_spec.operators.push(OperatorSpec {
            id: "agg".into(),
            kind: OperatorKind::Window,
            stateful: true,
            key_field: Some("key".into()),
            config: serde_json::json!({
                "trigger": "watermark",
                "kind": "tumbling",
                "size_ms": 1000,
                "key_field": "key",
                "timestamp_field": "ts",
                "value_fields": ["value"]
            }),
        });
        let new_plan = JobPlan::compile(window_spec).unwrap();
        let context = RescaleContext::from_plan(&new_plan).unwrap();

        let old_task = old_plan
            .tasks
            .iter()
            .find(|task| task.operator_id == "agg")
            .unwrap();
        let old_namespace =
            crate::job::effective_state_namespace(&old_plan.spec.id, None, "agg", &old_task.id);
        // Window state key: 8-byte BE window_start + utf8 user key.
        let mut state_key = 5_000i64.to_be_bytes().to_vec();
        state_key.extend_from_slice(b"omega");
        let entry = crate::state::StateEntry {
            namespace: old_namespace,
            key: state_key.clone(),
            value: b"7".to_vec(),
            expires_at_ms: None,
        };
        let moved = context.redistribute(entry).unwrap();
        let group = crate::job::key_group_for_key(b"omega", 16).unwrap();
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
        let expected =
            crate::job::effective_state_namespace(&new_plan.spec.id, None, "agg", &owner.id);
        assert_eq!(moved.namespace, expected);
        assert_eq!(moved.key, state_key);
    }

    #[test]
    fn rescale_rejects_unknown_key_encoding() {
        let new_plan = JobPlan::compile(rescale_job_spec(4)).unwrap();
        let context = RescaleContext::from_plan(&new_plan).unwrap();
        let old_plan = JobPlan::compile(rescale_job_spec(1)).unwrap();
        let old_task = old_plan
            .tasks
            .iter()
            .find(|task| task.operator_id == "agg")
            .unwrap();
        let entry = crate::state::StateEntry {
            namespace: crate::job::effective_state_namespace(
                &old_plan.spec.id,
                None,
                "agg",
                &old_task.id,
            ),
            key: b"no-known-prefix".to_vec(),
            value: Vec::new(),
            expires_at_ms: None,
        };
        assert!(context.redistribute(entry).is_err());
    }

    use crate::job::{
        CheckpointSpec, JobId, JobVersion, OperatorKind, OperatorSpec, SinkSpec, SourceSpec,
        StateDurability, StateSpec, TimeMode, TimeSpec,
    };

    fn local_spec(input_type: &str, output_type: &str) -> JobSpec {
        JobSpec {
            rebalance: None,
            id: JobId::new("validate-job").unwrap(),
            version: JobVersion(1),
            max_parallelism: 1,
            parallelism: 1,
            operators: vec![
                OperatorSpec {
                    id: "source".into(),
                    kind: OperatorKind::Source,
                    stateful: false,
                    key_field: None,
                    config: serde_json::json!({"type": input_type}),
                },
                OperatorSpec {
                    id: "sink".into(),
                    kind: OperatorKind::Sink,
                    stateful: false,
                    key_field: None,
                    config: serde_json::json!({"type": output_type}),
                },
            ],
            edges: vec![crate::job::EdgeSpec {
                id: "source-sink".into(),
                from: "source".into(),
                to: "sink".into(),
                partitioned: false,
            }],
            sources: vec![SourceSpec {
                codec: None,
                operator_id: "source".into(),
                input_type: input_type.into(),
                config: serde_json::json!({}),
                time: TimeSpec {
                    mode: TimeMode::ProcessingTime,
                    timestamp_field: None,
                    watermark: None,
                    allowed_lateness_ms: 0,
                    late_event_policy: Default::default(),
                    late_event_route: None,
                },
            }],
            sinks: vec![SinkSpec {
                codec: None,
                operator_id: "sink".into(),
                output_type: output_type.into(),
                config: serde_json::json!({}),
            }],
            state: None,
            checkpoint: None,
            placement: crate::job::PlacementStrategy::Colocated,
            recovery: Default::default(),
            rescale: false,
        }
    }

    /// Task 2.5 / 6.1: a local Job referencing an unknown component fails
    /// the side-effect-free deep build instead of failing later at runtime.
    #[test]
    fn validate_local_job_rejects_unknown_components() {
        let spec = local_spec("no-such-input", "no-such-output");
        let error =
            validate_local_job(&spec).expect_err("unknown input component must fail validation");
        assert!(error.to_string().contains("Unknown input type"));
    }

    #[test]
    fn durable_local_state_reuses_a_stable_root_and_requires_a_checkpoint_after_start() {
        let temp = tempfile::tempdir().unwrap();
        let mut spec = local_spec("memory", "drop");
        spec.operators.insert(
            1,
            OperatorSpec {
                id: "aggregate".into(),
                kind: OperatorKind::Aggregate,
                stateful: true,
                key_field: Some("key".into()),
                config: serde_json::json!({}),
            },
        );
        spec.edges = vec![
            crate::job::EdgeSpec {
                id: "source-aggregate".into(),
                from: "source".into(),
                to: "aggregate".into(),
                partitioned: false,
            },
            crate::job::EdgeSpec {
                id: "aggregate-sink".into(),
                from: "aggregate".into(),
                to: "sink".into(),
                partitioned: false,
            },
        ];
        spec.state = Some(StateSpec {
            backend: "embedded_kv".into(),
            durability: StateDurability::Durable,
            root: Some(temp.path().display().to_string()),
            namespace: Some("stable".into()),
            ttl_ms: None,
            format_version: 1,
            max_pending_transactions: None,
            max_bytes: None,
        });
        spec.checkpoint = Some(CheckpointSpec {
            interval_ms: 1_000,
            retention: 1,
            object_store_uri: format!("file://{}", temp.path().join("checkpoints").display()),
        });
        let plan = JobPlan::compile(spec).unwrap();
        let first_root = local_state_root(&plan).unwrap();
        let second_root = local_state_root(&plan).unwrap();
        assert_eq!(first_root, second_root);
        assert!(!durable_local_recovery_required(&plan));
        std::fs::create_dir_all(&first_root).unwrap();
        std::fs::write(local_state_start_marker(&plan).unwrap(), b"started\n").unwrap();
        assert!(durable_local_recovery_required(&plan));
    }

    #[test]
    fn ephemeral_state_does_not_turn_an_empty_start_into_recovery() {
        let temp = tempfile::tempdir().unwrap();
        let mut spec = local_spec("memory", "drop");
        spec.state = Some(StateSpec {
            backend: "embedded_kv".into(),
            durability: StateDurability::Ephemeral,
            root: Some(temp.path().display().to_string()),
            namespace: None,
            ttl_ms: None,
            format_version: 1,
            max_pending_transactions: None,
            max_bytes: None,
        });
        spec.checkpoint = None;
        let plan = JobPlan::compile(spec).unwrap();
        let first_root = local_state_root(&plan).unwrap();
        let second_root = local_state_root(&plan).unwrap();
        assert_ne!(
            first_root, second_root,
            "each ephemeral Job attempt must get an isolated state directory"
        );
        assert!(!durable_local_recovery_required(&plan));
    }
}

#[cfg(test)]
mod metrics_registry_tests {
    use super::*;
    use crate::input::{Ack, Input};
    use crate::job::{
        EdgeSpec, JobId, JobVersion, OperatorKind, OperatorSpec, SinkSpec, SourceSpec, TimeMode,
        TimeSpec,
    };
    use crate::output::Output;
    use crate::processor::Processor;
    use crate::{Error, MessageBatch, MessageBatchRef, ProcessResult};
    use async_trait::async_trait;
    use datafusion::arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use std::sync::Mutex;

    struct OneBatchThenEofInput {
        sent: Mutex<bool>,
    }

    #[async_trait]
    impl Input for OneBatchThenEofInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            let mut sent = self.sent.lock().unwrap();
            if *sent {
                return Err(Error::EOF);
            }
            *sent = true;
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(vec![1]))],
            )
            .unwrap();
            Ok((
                Arc::new(MessageBatch::new_arrow(batch)),
                Arc::new(crate::input::NoopAck),
            ))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct DevNullOutput;

    #[async_trait]
    impl Output for DevNullOutput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn write(&self, _msg: MessageBatchRef) -> Result<(), Error> {
            Ok(())
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct PassThrough;

    #[async_trait]
    impl Processor for PassThrough {
        async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
            Ok(ProcessResult::Single(batch))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct MinimalAdapter;

    impl crate::job::JobComponentAdapter for MinimalAdapter {
        fn build_input(
            &self,
            _source: &SourceSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Input>, Error> {
            Ok(Arc::new(OneBatchThenEofInput {
                sent: Mutex::new(false),
            }))
        }
        fn build_output(
            &self,
            _sink: &SinkSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Output>, Error> {
            Ok(Arc::new(DevNullOutput))
        }
        fn build_processor(
            &self,
            _operator: &OperatorSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Processor>, Error> {
            Ok(Arc::new(PassThrough))
        }
    }

    fn registry_spec() -> JobSpec {
        JobSpec {
            rebalance: None,
            id: JobId::new("registry-metrics-job").unwrap(),
            version: JobVersion(1),
            max_parallelism: 1,
            parallelism: 1,
            operators: vec![
                OperatorSpec {
                    id: "source".into(),
                    kind: OperatorKind::Source,
                    stateful: false,
                    key_field: None,
                    config: serde_json::json!({}),
                },
                OperatorSpec {
                    id: "sink".into(),
                    kind: OperatorKind::Sink,
                    stateful: false,
                    key_field: None,
                    config: serde_json::json!({}),
                },
            ],
            edges: vec![EdgeSpec {
                id: "source-sink".into(),
                from: "source".into(),
                to: "sink".into(),
                partitioned: false,
            }],
            sources: vec![SourceSpec {
                codec: None,
                operator_id: "source".into(),
                input_type: "registry-test-input".into(),
                config: serde_json::json!({}),
                time: TimeSpec {
                    mode: TimeMode::ProcessingTime,
                    timestamp_field: None,
                    watermark: None,
                    allowed_lateness_ms: 0,
                    late_event_policy: Default::default(),
                    late_event_route: None,
                },
            }],
            sinks: vec![SinkSpec {
                codec: None,
                operator_id: "sink".into(),
                output_type: "registry-test-output".into(),
                config: serde_json::json!({}),
            }],
            state: None,
            checkpoint: None,
            placement: crate::job::PlacementStrategy::Colocated,
            recovery: Default::default(),
            rescale: false,
        }
    }

    /// The local runner registers the Job's KernelMetrics once the kernel has
    /// spawned (before startup completion is reported) and unregisters them
    /// when the run finishes, so export never observes a finished Job.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registers_on_startup_and_unregisters_on_completion() {
        let registry = crate::runtime::JobMetricsRegistry::default();
        let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
        let spec = registry_spec();
        let task_registry = registry.clone();
        let run = tokio::spawn(async move {
            let adapter = MinimalAdapter;
            let mut resource = Resource {
                temporary: std::collections::HashMap::new(),
                input_names: std::cell::RefCell::new(Vec::new()),
            };
            run_job_with_checkpoints_started(
                &spec,
                &adapter,
                &mut resource,
                CancellationToken::default(),
                Some(startup_tx),
                Some(task_registry),
            )
            .await
        });

        startup_rx.await.unwrap().unwrap();
        let metrics = registry
            .get("registry-metrics-job")
            .expect("Job metrics must be registered once startup completed");
        // The handle is the live kernel registry: chains appear as the graph's
        // event loops spin up.
        let _ = metrics.snapshot();

        run.await.unwrap().unwrap();
        assert!(registry.get("registry-metrics-job").is_none());
        assert!(registry.snapshots().is_empty());
    }

    /// A run without a registry keeps the legacy behavior (no registration).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runs_without_a_registry_are_untracked() {
        let registry = crate::runtime::JobMetricsRegistry::default();
        let spec = registry_spec();
        let adapter = MinimalAdapter;
        let mut resource = Resource {
            temporary: std::collections::HashMap::new(),
            input_names: std::cell::RefCell::new(Vec::new()),
        };
        run_job_with_checkpoints_started(
            &spec,
            &adapter,
            &mut resource,
            CancellationToken::default(),
            None,
            None,
        )
        .await
        .unwrap();
        assert!(registry.snapshots().is_empty());
    }
}
