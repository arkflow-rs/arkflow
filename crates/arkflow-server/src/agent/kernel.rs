//! Kernel job execution: JobTask/JobRuntime bookkeeping, dedicated-runtime
//! teardown, checkpoint/repository adapters for running tasks, kernel spawn,
//! and the registry component adapter.
use super::checkpoint::{
    checkpoint_repository, recovery_artifact, restore_recovery_state, validate_recovery_manifest,
    validate_recovery_snapshots, SharedCheckpointStore,
};
use arkflow_core::checkpoint::{
    recovery_manifest_key, CheckpointCoordinator, CheckpointRepository, CheckpointStatus,
    RecoveryArtifact, RecoveryArtifactKind, RecoveryPlan, StateSnapshotRef, TaskAttemptSnapshot,
    TaskCheckpointAck,
};
use arkflow_core::input::InputConfig;
use arkflow_core::job::{
    JobComponentAdapter, JobPlan, OperatorSpec, SinkSpec, SourceSpec, TaskAttempt,
};
use arkflow_core::output::OutputConfig;
use arkflow_core::processor::ProcessorConfig;
use arkflow_core::state::{RedbStateBackend, StateBackend};
use arkflow_core::temporary::Temporary;
use arkflow_core::Resource;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Split-placement context carried by the Hub's job_start payload.
#[derive(Clone, Default)]
pub(super) struct SplitPlacementPayload {
    /// Full task→node mapping for the Job (remote peers must be nameable).
    pub(super) task_nodes: Option<BTreeMap<String, String>>,
    /// Peer node → advertised data-plane address.
    pub(super) node_data_ports: BTreeMap<String, String>,
    /// Hub decision: this start may not initialize an empty durable backend.
    pub(super) recovery_required: bool,
}

#[derive(Clone, Default)]
pub(super) struct JobRuntime {
    pub(super) tasks: Arc<Mutex<BTreeMap<String, JobTask>>>,
    pub(super) starts: Arc<Mutex<()>>,
    /// Cross-node shuffle data plane. `None` keeps the legacy co-location
    /// contract: every edge is materialized in-process and no data port
    /// listens.
    pub(super) data_plane: Option<Arc<arkflow_core::executor::remote::NetworkManager>>,
    /// Finished-task observations whose delivery to the Hub failed. They are
    /// retried by the next session; dropping them would leave the Hub
    /// reporting a dead job as running forever (the start operation stays
    /// `Succeeded` and reconcile skips the re-dispatch).
    pub(super) pending_observations: Arc<Mutex<Vec<FinishedJob>>>,
}

/// One finished kernel task and the outcome its Hub observation carries.
pub(super) type FinishedJob = (String, u64, Result<(), String>);

pub(super) struct JobTask {
    pub(super) generation: u64,
    pub(super) ephemeral_state: bool,
    pub(super) recovery_required: bool,
    pub(super) cancellation: CancellationToken,
    pub(super) assignments: Vec<TaskAttempt>,
    /// Dedicated bounded runtime for Jobs declaring `resources.cpu_millicores`
    /// (worker threads = ceil(millicores/1000), min 1): one Job cannot occupy
    /// the shared runtime's workers. Shut down (detached, bounded) when the
    /// task is retired. `None` for undeclared Jobs — shared runtime as before.
    pub(super) dedicated_runtime: Option<Arc<tokio::runtime::Runtime>>,
    pub(super) watermark_partitions: BTreeMap<String, u32>,
    pub(super) state: Arc<dyn StateBackend>,
    pub(super) checkpoint_store_uri: Option<String>,
    /// Unified-kernel handle: command-driven snapshots over the running
    /// graph (the kernel executes the Job's chains).
    pub(super) kernel:
        Option<std::sync::Arc<arkflow_core::executor::kernel_handle::KernelJobHandle>>,
    pub(super) handle: tokio::task::JoinHandle<Result<(), arkflow_core::Error>>,
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
    pub(super) async fn generation(&self, job_id: &str) -> Option<u64> {
        self.tasks
            .lock()
            .await
            .get(job_id)
            .map(|task| task.generation)
    }

    /// Per-Job kernel snapshots for the Hub's data-plane metrics export.
    /// Unlike `metrics`, these keep the Job identity so the Hub can label
    /// series per (node, job).
    pub(super) async fn job_snapshots(
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
    pub(super) async fn job_tasks(&self) -> BTreeMap<String, Vec<String>> {
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
    pub(super) async fn metrics(&self) -> BTreeMap<String, f64> {
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
    pub(super) async fn start(
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
                            // Same cleanup as the marker-failure path below:
                            // fail before persisting anything, and close the
                            // opened state so it does not leak past a start
                            // that never reached registration.
                            let _ = state.close();
                            return Err(format!(
                                "dedicated runtime for Job '{job_id}' failed to build: {error}"
                            ));
                        }
                    }
                }
                None => None,
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
                        "split dispatch without a full task→node map; deriving remote peers from the local assignment only"
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

    pub(super) async fn checkpoint(
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

    pub(super) async fn aggregate_checkpoint(
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

    pub(super) async fn take_finished(&self) -> Vec<FinishedJob> {
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
    pub(super) async fn park_observations(&self, observations: Vec<FinishedJob>) {
        if observations.is_empty() {
            return;
        }
        self.pending_observations.lock().await.extend(observations);
    }

    pub(super) async fn stop_all(&self) {
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

    pub(super) async fn stop(&self, job_id: &str, generation: u64) -> Result<(), String> {
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

pub(super) fn safe_path_component(value: &str) -> String {
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

pub(super) fn ephemeral_state_nonce() -> String {
    static ATTEMPT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let attempt = ATTEMPT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{}-{timestamp}-{attempt}", std::process::id())
}

pub(super) fn durable_recovery_marker(
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

pub(super) fn persist_start_marker(marker: &std::path::Path) -> std::io::Result<()> {
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

pub(super) fn remove_start_marker(marker: &std::path::Path) {
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
pub(super) async fn await_previous_teardown(
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
