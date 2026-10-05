//! Process-local supervision primitives for control-plane managed Streams.

use crate::config::EngineConfig;
use crate::control::{
    ControlEvent, ConvergenceState, DesiredState, OperationRecord, OperationState,
    RuntimeErrorEvent, StreamMetricsSnapshot, StreamState, StreamStatus,
};
use crate::stream::StreamConfig;
use crate::Error;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};
use tokio_util::sync::CancellationToken;

const MAX_RECENT_ERRORS: usize = 32;
const MAX_EVENTS: usize = 128;
const MAX_OPERATIONS: usize = 256;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Default)]
pub struct EventStore {
    events: Arc<Mutex<VecDeque<ControlEvent>>>,
}

/// Bounded in-memory administrative operation registry. The domain layer owns
/// lifecycle execution; transport layers only observe these records.
#[derive(Clone, Default)]
pub struct OperationStore {
    operations: Arc<RwLock<BTreeMap<String, OperationRecord>>>,
}

impl OperationStore {
    pub async fn find_or_create(
        &self,
        operation: impl Into<String>,
        resource_type: impl Into<String>,
        resource_id: impl Into<String>,
        correlation_id: Option<String>,
    ) -> OperationRecord {
        let operation = operation.into();
        let resource_type = resource_type.into();
        let resource_id = resource_id.into();
        let mut operations = self.operations.write().await;
        if let Some(active) = operations.values().find(|record| {
            record.operation == operation
                && record.resource_type == resource_type
                && record.resource_id == resource_id
                && matches!(
                    record.state,
                    OperationState::Queued | OperationState::Running
                )
        }) {
            return active.clone();
        }
        let now = now_ms();
        let id = format!("op-{}", OPERATION_SEQUENCE.fetch_add(1, Ordering::Relaxed));
        let record = OperationRecord {
            id: id.clone(),
            operation,
            resource_type,
            resource_id,
            state: OperationState::Queued,
            progress: 0,
            created_at_ms: now,
            started_at_ms: None,
            finished_at_ms: None,
            correlation_id,
            error: None,
            result: None,
        };
        if operations.len() >= MAX_OPERATIONS {
            if let Some(oldest) = operations.keys().next().cloned() {
                operations.remove(&oldest);
            }
        }
        operations.insert(id, record.clone());
        record
    }

    pub async fn create(
        &self,
        operation: impl Into<String>,
        resource_type: impl Into<String>,
        resource_id: impl Into<String>,
        correlation_id: Option<String>,
    ) -> OperationRecord {
        let now = now_ms();
        let id = format!("op-{}", OPERATION_SEQUENCE.fetch_add(1, Ordering::Relaxed));
        let record = OperationRecord {
            id: id.clone(),
            operation: operation.into(),
            resource_type: resource_type.into(),
            resource_id: resource_id.into(),
            state: OperationState::Queued,
            progress: 0,
            created_at_ms: now,
            started_at_ms: None,
            finished_at_ms: None,
            correlation_id,
            error: None,
            result: None,
        };
        let mut operations = self.operations.write().await;
        if operations.len() >= MAX_OPERATIONS {
            if let Some(oldest) = operations.keys().next().cloned() {
                operations.remove(&oldest);
            }
        }
        operations.insert(id, record.clone());
        record
    }

    pub async fn update(
        &self,
        id: &str,
        state: OperationState,
        progress: u8,
        error: Option<String>,
    ) -> Option<OperationRecord> {
        let mut operations = self.operations.write().await;
        let record = operations.get_mut(id)?;
        if matches!(
            record.state,
            OperationState::Succeeded
                | OperationState::Failed
                | OperationState::Cancelled
                | OperationState::TimedOut
        ) {
            return Some(record.clone());
        }
        let now = now_ms();
        record.state = state;
        record.progress = progress;
        if record.started_at_ms.is_none() && state == OperationState::Running {
            record.started_at_ms = Some(now);
        }
        if matches!(
            state,
            OperationState::Succeeded
                | OperationState::Failed
                | OperationState::Cancelled
                | OperationState::TimedOut
        ) {
            record.finished_at_ms = Some(now);
        }
        record.error = error;
        Some(record.clone())
    }

    pub async fn set_result(
        &self,
        id: &str,
        result: std::collections::BTreeMap<String, String>,
    ) -> Option<OperationRecord> {
        let mut operations = self.operations.write().await;
        let record = operations.get_mut(id)?;
        record.result = Some(result);
        Some(record.clone())
    }

    pub async fn get(&self, id: &str) -> Option<OperationRecord> {
        self.operations.read().await.get(id).cloned()
    }

    pub async fn list(&self) -> Vec<OperationRecord> {
        let mut records: Vec<_> = self.operations.read().await.values().cloned().collect();
        records.sort_by_key(|record| std::cmp::Reverse(record.created_at_ms));
        records
    }

    pub async fn cancel(&self, id: &str) -> Option<OperationRecord> {
        self.update(
            id,
            OperationState::Cancelled,
            100,
            Some("Cancelled by operator".into()),
        )
        .await
    }
}

static OPERATION_SEQUENCE: AtomicU64 = AtomicU64::new(1);

impl EventStore {
    pub async fn record(&self, event: ControlEvent) {
        let mut events = self.events.lock().await;
        if events.len() == MAX_EVENTS {
            events.pop_front();
        }
        events.push_back(event);
    }

    pub async fn snapshot(&self) -> Vec<ControlEvent> {
        self.events.lock().await.iter().cloned().collect()
    }
}

/// Runtime counters shared by a Stream task and control-plane snapshots.
pub struct RuntimeMetrics {
    pub input_batches: AtomicU64,
    pub input_messages: AtomicU64,
    pub processing_errors: AtomicU64,
    pub output_batches: AtomicU64,
    pub output_messages: AtomicU64,
    pub input_errors: AtomicU64,
    pub input_reconnects: AtomicU64,
    pub output_errors: AtomicU64,
    pub restarts: AtomicU64,
    pub kernel: Arc<crate::executor::metrics::KernelMetrics>,
}

impl RuntimeMetrics {
    pub fn snapshot(&self) -> StreamMetricsSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        let kernel = self.kernel.snapshot();
        let in_flight = kernel
            .chains
            .values()
            .map(|metrics| metrics.in_flight)
            .sum();
        let mean_latency_us = kernel
            .chains
            .values()
            .map(|metrics| metrics.mean_latency_us)
            .max()
            .unwrap_or_default();
        StreamMetricsSnapshot {
            input_batches: load(&self.input_batches),
            input_messages: load(&self.input_messages),
            processing_errors: load(&self.processing_errors),
            output_batches: load(&self.output_batches),
            output_messages: load(&self.output_messages),
            input_errors: load(&self.input_errors),
            input_reconnects: load(&self.input_reconnects),
            output_errors: load(&self.output_errors),
            restarts: load(&self.restarts),
            kernel_chains: kernel.chains,
            in_flight,
            mean_latency_us,
            checkpoint_duration_ms: kernel.checkpoint_duration_ms,
            checkpoint_failures: kernel.checkpoint_failures,
            watermark_lag_ms: kernel.watermark_lag_ms,
            late_events: kernel.late_events,
        }
    }
}

impl Default for RuntimeMetrics {
    fn default() -> Self {
        Self {
            input_batches: AtomicU64::new(0),
            input_messages: AtomicU64::new(0),
            processing_errors: AtomicU64::new(0),
            output_batches: AtomicU64::new(0),
            output_messages: AtomicU64::new(0),
            input_errors: AtomicU64::new(0),
            input_reconnects: AtomicU64::new(0),
            output_errors: AtomicU64::new(0),
            restarts: AtomicU64::new(0),
            kernel: Arc::new(crate::executor::metrics::KernelMetrics::default()),
        }
    }
}

/// Mutable state associated with one registered Stream.
pub struct RuntimeEntry {
    pub id: String,
    pub config: StreamConfig,
    /// Registration index (for deterministic legacy stream-id derivation).
    pub index: usize,
    pub state: StreamState,
    pub cancellation: CancellationToken,
    pub handle: Option<JoinHandle<Result<(), Error>>>,
    pub metrics: Arc<RuntimeMetrics>,
    pub started_at_ms: Option<u64>,
    pub active_operation_id: Option<String>,
    pub desired_state: DesiredState,
    pub desired_generation: u64,
    pub desired_config_version: Option<String>,
    pub observed_generation: Option<u64>,
    pub observed_config_version: Option<String>,
    pub convergence: ConvergenceState,
    pub intent_id: Option<String>,
    pub attempt_id: Option<String>,
    pub last_completed_action_id: Option<String>,
    pub retry_count: u32,
    pub next_retry_at_ms: Option<u64>,
    pub node_id: String,
    pub recent_errors: VecDeque<RuntimeErrorEvent>,
}

impl RuntimeEntry {
    pub fn new(id: String, config: StreamConfig, index: usize) -> Self {
        Self {
            id,
            config,
            index,
            state: StreamState::Created,
            cancellation: CancellationToken::new(),
            handle: None,
            metrics: Arc::new(RuntimeMetrics::default()),
            started_at_ms: None,
            active_operation_id: None,
            desired_state: DesiredState::Stopped,
            desired_generation: 0,
            desired_config_version: None,
            observed_generation: None,
            observed_config_version: None,
            convergence: ConvergenceState::InSync,
            intent_id: None,
            attempt_id: None,
            last_completed_action_id: None,
            retry_count: 0,
            next_retry_at_ms: None,
            node_id: "local-node".to_string(),
            recent_errors: VecDeque::with_capacity(MAX_RECENT_ERRORS),
        }
    }

    pub fn record_error(&mut self, stage: impl Into<String>, message: impl Into<String>) {
        if self.recent_errors.len() == MAX_RECENT_ERRORS {
            self.recent_errors.pop_front();
        }
        self.recent_errors.push_back(RuntimeErrorEvent {
            occurred_at_ms: now_ms(),
            stage: stage.into(),
            message: message.into(),
        });
    }

    pub fn snapshot(&self) -> StreamStatus {
        StreamStatus {
            id: self.id.clone(),
            state: self.state,
            desired_state: Some(self.desired_state),
            desired_generation: self.desired_generation,
            desired_config_version: self.desired_config_version.clone(),
            observed_generation: self.observed_generation,
            observed_config_version: self.observed_config_version.clone(),
            convergence: self.convergence,
            intent_id: self.intent_id.clone(),
            attempt_id: self.attempt_id.clone(),
            last_completed_action_id: self.last_completed_action_id.clone(),
            retry_count: self.retry_count,
            next_retry_at_ms: self.next_retry_at_ms,
            transition_started_at_ms: self.started_at_ms,
            active_operation_id: self.active_operation_id.clone(),
            node_id: Some(self.node_id.clone()),
            started_at_ms: self.started_at_ms,
            last_error: self.recent_errors.back().cloned(),
            metrics: self.metrics.snapshot(),
        }
    }
}

/// Registry of live kernel Job metrics keyed by Job id. The local Job runner
/// registers each spawned Job's `KernelMetrics` so observability export can
/// snapshot running Jobs without holding their handles. Locks are never held
/// across awaits.
#[derive(Clone, Default)]
pub struct JobMetricsRegistry {
    jobs: Arc<std::sync::Mutex<BTreeMap<String, Arc<crate::executor::metrics::KernelMetrics>>>>,
}

impl JobMetricsRegistry {
    pub fn register(
        &self,
        job_id: impl Into<String>,
        metrics: Arc<crate::executor::metrics::KernelMetrics>,
    ) {
        self.jobs.lock().unwrap().insert(job_id.into(), metrics);
    }

    pub fn unregister(&self, job_id: &str) {
        self.jobs.lock().unwrap().remove(job_id);
    }

    pub fn get(&self, job_id: &str) -> Option<Arc<crate::executor::metrics::KernelMetrics>> {
        self.jobs.lock().unwrap().get(job_id).cloned()
    }

    /// Snapshot every registered Job's kernel metrics.
    pub fn snapshots(&self) -> BTreeMap<String, crate::executor::metrics::KernelMetricsSnapshot> {
        self.jobs
            .lock()
            .unwrap()
            .iter()
            .map(|(job_id, metrics)| (job_id.clone(), metrics.snapshot()))
            .collect()
    }
}

/// Process-local registry of independently managed Stream runtimes.
#[derive(Clone)]
pub struct RuntimeManager {
    entries: Arc<RwLock<BTreeMap<String, Arc<Mutex<RuntimeEntry>>>>>,
    events: EventStore,
    observed_config_version: Arc<RwLock<Option<String>>>,
    job_metrics: JobMetricsRegistry,
    /// Monotonically increasing stream-index counter: never reused after
    /// de-registration, so derived job IDs / state namespaces / checkpoint
    /// paths cannot collide across the registry's lifetime.
    next_index: Arc<AtomicU64>,
}

impl Default for RuntimeManager {
    fn default() -> Self {
        Self {
            entries: Arc::new(RwLock::new(BTreeMap::new())),
            events: EventStore::default(),
            observed_config_version: Arc::new(RwLock::new(None)),
            job_metrics: JobMetricsRegistry::default(),
            next_index: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl RuntimeManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Metrics registry for locally running kernel Jobs (observability).
    pub fn job_metrics(&self) -> JobMetricsRegistry {
        self.job_metrics.clone()
    }

    /// Register a Stream without awaiting while holding the registry lock.
    pub async fn register(&self, id: String, config: StreamConfig) -> Result<(), Error> {
        let mut entries = self.entries.write().await;
        if entries.contains_key(&id) {
            return Err(Error::Config(format!(
                "Stream runtime already registered: {id}"
            )));
        }
        // Global monotonic index: never reused after de-registration, so
        // derived job IDs / state namespaces / checkpoint paths cannot
        // collide across the registry's lifetime (replace_config's
        // stop→remove→register used to shrink len() and reuse indices).
        let index = self.next_index.fetch_add(1, Ordering::Relaxed) as usize;
        entries.insert(
            id.clone(),
            Arc::new(Mutex::new(RuntimeEntry::new(id, config, index))),
        );
        Ok(())
    }

    pub async fn get(&self, id: &str) -> Option<Arc<Mutex<RuntimeEntry>>> {
        self.entries.read().await.get(id).cloned()
    }

    pub async fn set_active_operation(&self, id: &str, operation_id: Option<String>) {
        if let Some(entry) = self.get(id).await {
            entry.lock().await.active_operation_id = operation_id;
        }
    }

    pub async fn set_last_completed_action(&self, id: &str, action_id: String) {
        if let Some(entry) = self.get(id).await {
            entry.lock().await.last_completed_action_id = Some(action_id);
        }
    }

    pub async fn set_observed_config_version(&self, version: String) {
        *self.observed_config_version.write().await = Some(version.clone());
        let entries = self.entries.read().await;
        for entry in entries.values() {
            entry.lock().await.observed_config_version = Some(version.clone());
        }
    }

    pub async fn observed_config_version(&self) -> Option<String> {
        self.observed_config_version.read().await.clone()
    }

    pub async fn remove(&self, id: &str) -> Option<Arc<Mutex<RuntimeEntry>>> {
        self.entries.write().await.remove(id)
    }

    pub async fn ids(&self) -> Vec<String> {
        self.entries.read().await.keys().cloned().collect()
    }

    pub fn event_store(&self) -> EventStore {
        self.events.clone()
    }

    async fn record_event(
        &self,
        event_type: impl Into<String>,
        stream_id: Option<String>,
        outcome: impl Into<String>,
        message: Option<String>,
    ) {
        self.events
            .record(ControlEvent {
                occurred_at_ms: now_ms(),
                event_type: event_type.into(),
                stream_id,
                outcome: outcome.into(),
                message,
                operation_id: None,
                correlation_id: None,
                actor: None,
            })
            .await;
    }

    /// Snapshot entries after releasing the registry read lock, so individual
    /// entry locks are never held while the registry is being accessed.
    pub async fn snapshots(&self) -> Vec<StreamStatus> {
        let entries: Vec<_> = self.entries.read().await.values().cloned().collect();
        let mut snapshots = Vec::with_capacity(entries.len());
        for entry in entries {
            snapshots.push(entry.lock().await.snapshot());
        }
        snapshots.sort_by(|a, b| a.id.cmp(&b.id));
        snapshots
    }

    /// Build and start one registered Stream. The build happens outside the
    /// entry lock so component construction cannot block lifecycle commands.
    pub async fn start(&self, id: &str) -> Result<(), Error> {
        let entry = self
            .get(id)
            .await
            .ok_or_else(|| Error::Config(format!("Unknown stream runtime: {id}")))?;

        let (config, cancellation, stale_handle) = {
            let mut runtime = entry.lock().await;
            match runtime.state {
                StreamState::Created | StreamState::Stopped | StreamState::Failed => {}
                _ => {
                    return Err(Error::Config(format!(
                        "Stream runtime '{}' is already transitioning or running",
                        id
                    )))
                }
            }
            runtime.state = StreamState::Starting;
            runtime.cancellation = CancellationToken::new();
            (
                runtime.config.clone(),
                runtime.cancellation.clone(),
                runtime.handle.take(),
            )
        };

        if let Some(handle) = stale_handle {
            let wait_result = await_task(handle).await;
            settle_detached_task(&entry, &wait_result, "stale-startup-task").await;
            wait_result?;
        }

        let metrics = entry.lock().await.metrics.clone();
        let index = entry.lock().await.index;
        // Unified kernel: the StreamConfig compiles to a JobSpec and runs
        // through the same executor as declared Jobs and Agent subgraphs.
        let spec = match crate::executor::stream_compiler::compile_stream(&config, index) {
            Ok(spec) => spec,
            Err(error) => {
                let mut runtime = entry.lock().await;
                runtime.state = StreamState::Failed;
                runtime.record_error("build", error.to_string());
                drop(runtime);
                self.record_event(
                    "stream_start",
                    Some(id.to_string()),
                    "failed",
                    Some(error.to_string()),
                )
                .await;
                return Err(error);
            }
        };
        let stream_id = id.to_string();

        // Dry-run: compile + component construction + graph build so config
        // errors (unknown inputs/processors, broken graphs) surface in `start`
        // synchronously — reconciliation relies on that. The validation WAL
        // (durability-enabled streams) is closed on every path — including
        // build failures — so the real runtime can reopen the same redb path
        // without an exclusive-lock failure. The discarded graph is rebuilt
        // by the spawned run.
        {
            let adapter = match crate::executor::stream_adapter::StreamJobAdapter::with_temporary(
                config.durability.as_ref(),
                config.temporary.clone(),
            ) {
                Ok(adapter) => adapter,
                Err(error) => {
                    let mut runtime = entry.lock().await;
                    runtime.state = StreamState::Failed;
                    runtime.record_error("build", error.to_string());
                    drop(runtime);
                    self.record_event(
                        "stream_start",
                        Some(id.to_string()),
                        "failed",
                        Some(error.to_string()),
                    )
                    .await;
                    return Err(error);
                }
            };
            let build_result = async {
                let resource = adapter.build_resource()?;
                let plan = crate::job::JobPlan::compile(spec.clone())?;
                crate::executor::graph::ExecutionGraphBuilder::default()
                    .build(&plan, &adapter, &resource)?;
                Ok::<(), Error>(())
            }
            .await;
            let close_result = adapter.close().await;
            match (build_result, close_result) {
                (Ok(()), Ok(())) => {}
                (Err(error), _) => {
                    let mut runtime = entry.lock().await;
                    runtime.state = StreamState::Failed;
                    runtime.record_error("build", error.to_string());
                    drop(runtime);
                    self.record_event(
                        "stream_start",
                        Some(id.to_string()),
                        "failed",
                        Some(error.to_string()),
                    )
                    .await;
                    return Err(error);
                }
                (Ok(()), Err(error)) => {
                    let mut runtime = entry.lock().await;
                    runtime.state = StreamState::Failed;
                    runtime.record_error("close", error.to_string());
                    drop(runtime);
                    self.record_event(
                        "stream_start",
                        Some(id.to_string()),
                        "failed",
                        Some(error.to_string()),
                    )
                    .await;
                    return Err(error);
                }
            }
        }

        let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
        let handle = self.spawn_supervised(entry.clone(), async move {
            // Adapter construction builds the WAL — the object-store
            // backend's recovery GETs every segment — so keep it off the
            // async workers.
            let durability = config.durability.clone();
            let temporary = config.temporary.clone();
            let adapter = tokio::task::spawn_blocking(move || {
                crate::executor::stream_adapter::StreamJobAdapter::with_temporary(
                    durability.as_ref(),
                    temporary,
                )
            })
            .await
            .map_err(|e| {
                Error::Process(format!("stream adapter construction task failed: {e}")
                )
            })??;
            // The adapter owns the WAL flusher. Close it even when resource
            // construction or graph startup fails before the graph takes
            // ownership of the source.
            let run_result = async {
                let mut resource = adapter.build_resource()?;
                crate::executor::run_job_with_metrics_started(
                    &spec,
                    &adapter,
                    &mut resource,
                    cancellation,
                    Some(metrics),
                    Some(startup_tx),
                )
                .await
            }
            .await;
            let close_result = adapter.close().await;
            match (run_result, close_result) {
                (Err(error), _) => {
                    tracing::warn!(stream_id = %stream_id, %error, "kernel stream run failed");
                    Err(error)
                }
                (Ok(()), Err(error)) => {
                    tracing::warn!(stream_id = %stream_id, %error, "kernel stream adapter close failed");
                    Err(error)
                }
                (Ok(()), Ok(())) => Ok(()),
            }
        });

        {
            // Make the task joinable from spawn time, not only after the
            // startup handshake: the spawned adapter already opened the WAL,
            // so a stop() during `Starting` must await THIS task before
            // reporting `Stopped` (an immediate start() would otherwise race
            // the old adapter for the exclusive WAL lock).
            entry.lock().await.handle = Some(handle);
        }

        match startup_rx.await {
            Ok(Ok(())) => {
                let mut runtime = entry.lock().await;
                if runtime.state != StreamState::Starting {
                    let state = runtime.state;
                    let task_handle = runtime.handle.take();
                    drop(runtime);
                    let result = match task_handle {
                        Some(task_handle) => await_task(task_handle).await,
                        // A concurrent stop()/restart() already joined the task.
                        None => Ok(()),
                    };
                    return match result {
                        Ok(()) if matches!(state, StreamState::Stopped) => Ok(()),
                        Ok(()) => Err(Error::Process(format!(
                            "stream '{}' left startup in unexpected state {:?}",
                            id, state
                        ))),
                        Err(error) => Err(error),
                    };
                }
                runtime.state = StreamState::Running;
                runtime.started_at_ms = Some(now_ms());
                drop(runtime);
                self.record_event("stream_start", Some(id.to_string()), "succeeded", None)
                    .await;
                Ok(())
            }
            Ok(Err(message)) => {
                let startup_error = Error::Process(format!(
                    "stream '{}' resource startup failed: {message}",
                    id
                ));
                let task_handle = entry.lock().await.handle.take();
                let wait_result = match task_handle {
                    Some(task_handle) => await_task(task_handle).await,
                    // A concurrent stop() joined the task and settled the state.
                    None => Ok(()),
                };
                settle_detached_task(&entry, &wait_result, "startup").await;
                if wait_result.is_ok() {
                    let mut runtime = entry.lock().await;
                    runtime.state = StreamState::Failed;
                    runtime.record_error("startup", startup_error.to_string());
                    Err(startup_error)
                } else {
                    Err(wait_result.expect_err("startup task result was checked"))
                }
            }
            Err(_) => {
                let startup_error = Error::Process(format!(
                    "stream '{}' stopped before reporting startup readiness",
                    id
                ));
                let task_handle = entry.lock().await.handle.take();
                let wait_result = match task_handle {
                    Some(task_handle) => await_task(task_handle).await,
                    // A concurrent stop() joined the task and settled the state.
                    None => Ok(()),
                };
                settle_detached_task(&entry, &wait_result, "startup").await;
                if wait_result.is_ok() {
                    let mut runtime = entry.lock().await;
                    runtime.state = StreamState::Failed;
                    runtime.record_error("startup", startup_error.to_string());
                    Err(startup_error)
                } else {
                    Err(wait_result.expect_err("startup task result was checked"))
                }
            }
        }
    }

    fn spawn_supervised<F>(
        &self,
        entry: Arc<Mutex<RuntimeEntry>>,
        future: F,
    ) -> JoinHandle<Result<(), Error>>
    where
        F: Future<Output = Result<(), Error>> + Send + 'static,
    {
        let events = self.events.clone();
        tokio::spawn(async move {
            let result = future.await;
            let event = {
                let mut runtime = entry.lock().await;
                let id = runtime.id.clone();
                match &result {
                    Ok(()) => {
                        runtime.state = StreamState::Stopped;
                        ("stream_stop", id, "succeeded", None)
                    }
                    Err(error) => {
                        runtime.state = StreamState::Failed;
                        runtime.record_error("runtime", error.to_string());
                        ("stream_failure", id, "failed", Some(error.to_string()))
                    }
                }
            };
            events
                .record(ControlEvent {
                    occurred_at_ms: now_ms(),
                    event_type: event.0.to_string(),
                    stream_id: Some(event.1),
                    outcome: event.2.to_string(),
                    message: event.3,
                    operation_id: None,
                    correlation_id: None,
                    actor: None,
                })
                .await;
            result
        })
    }

    pub async fn start_all(&self) -> Result<(), Error> {
        for id in self.ids().await {
            self.start(&id).await?;
        }
        Ok(())
    }

    /// Reconcile registered runtimes with a validated candidate configuration.
    /// Unchanged entries remain untouched; changed/new entries are rebuilt.
    /// A failed reconciliation attempts to restore the complete prior snapshot.
    pub async fn replace_config(&self, config: &EngineConfig) -> Result<Vec<String>, Error> {
        let new_ids = config.stream_ids()?;
        let current = self.config_snapshot().await;
        let mut affected = Vec::new();

        let result = async {
            for (id, old_config, old_state) in &current {
                let new_config = new_ids
                    .iter()
                    .position(|new_id| new_id == id)
                    .map(|index| &config.streams[index]);
                let changed = match new_config {
                    Some(new_config) => {
                        serde_json::to_value(old_config)? != serde_json::to_value(new_config)?
                    }
                    None => true,
                };
                if changed {
                    self.stop(id).await?;
                    self.remove(id).await;
                    affected.push(id.clone());
                    if let Some(new_config) = new_config {
                        self.register(id.clone(), new_config.clone()).await?;
                        if is_active(*old_state) {
                            self.start(id).await?;
                        }
                    }
                }
            }

            for (index, id) in new_ids.iter().enumerate() {
                if !current.iter().any(|(current_id, _, _)| current_id == id) {
                    self.register(id.clone(), config.streams[index].clone())
                        .await?;
                    self.start(id).await?;
                    affected.push(id.clone());
                }
            }
            Ok::<(), Error>(())
        }
        .await;

        if let Err(error) = result {
            if let Err(restore_error) = self.restore_snapshot(&current).await {
                return Err(Error::Process(format!(
                    "Configuration apply failed: {error}; restoration also failed: {restore_error}"
                )));
            }
            return Err(error);
        }
        Ok(affected)
    }

    async fn config_snapshot(&self) -> Vec<(String, StreamConfig, StreamState)> {
        let entries: Vec<_> = self.entries.read().await.values().cloned().collect();
        let mut snapshot = Vec::with_capacity(entries.len());
        for entry in entries {
            let runtime = entry.lock().await;
            snapshot.push((runtime.id.clone(), runtime.config.clone(), runtime.state));
        }
        snapshot
    }

    async fn restore_snapshot(
        &self,
        snapshot: &[(String, StreamConfig, StreamState)],
    ) -> Result<(), Error> {
        let _ = self.stop_all().await;
        for id in self.ids().await {
            self.remove(&id).await;
        }
        for (id, config, state) in snapshot {
            self.register(id.clone(), config.clone()).await?;
            if is_active(*state) {
                self.start(id).await?;
            }
        }
        Ok(())
    }

    /// Stop one Stream and await its task after releasing the entry lock.
    pub async fn stop(&self, id: &str) -> Result<(), Error> {
        let entry = self
            .get(id)
            .await
            .ok_or_else(|| Error::Config(format!("Unknown stream runtime: {id}")))?;

        let handle = {
            let mut runtime = entry.lock().await;
            match runtime.state {
                StreamState::Created | StreamState::Stopped => return Ok(()),
                // A restart in progress already cancelled the token and took
                // the handle (atomically, above); its internal stop phase
                // fulfils the stop intent. Early-return: intercepting here
                // would race the restart's start() into a resurrection.
                StreamState::Restarting => return Ok(()),
                StreamState::Stopping => {
                    return Err(Error::Config(format!(
                        "Stream runtime '{}' is already stopping",
                        id
                    )))
                }
                _ => {}
            }
            runtime.state = StreamState::Stopping;
            runtime.cancellation.cancel();
            runtime.handle.take()
        };

        let wait_result = match handle {
            Some(handle) => await_task(handle).await,
            None => Ok(()),
        };
        let mut runtime = entry.lock().await;
        match &wait_result {
            Ok(()) => runtime.state = StreamState::Stopped,
            Err(error) => {
                runtime.state = StreamState::Failed;
                runtime.record_error("shutdown", error.to_string());
            }
        }
        drop(runtime);
        wait_result?;
        self.record_event("stream_stop", Some(id.to_string()), "succeeded", None)
            .await;
        Ok(())
    }

    pub async fn stop_all(&self) -> Result<(), Error> {
        let mut first_error = None;
        for id in self.ids().await {
            if let Err(error) = self.stop(&id).await {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub async fn restart(&self, id: &str) -> Result<(), Error> {
        let entry = self
            .get(id)
            .await
            .ok_or_else(|| Error::Config(format!("Unknown stream runtime: {id}")))?;
        // State transition + cancellation + handle extraction in ONE lock
        // block: a concurrent stop() seeing Restarting must not find the
        // handle still present (the old two-block structure let stop take
        // the handle, join, set Stopped, return Ok — then restart's start()
        // resurrected the stream).
        let handle = {
            let mut runtime = entry.lock().await;
            match runtime.state {
                StreamState::Running | StreamState::Failed | StreamState::Stopped => {
                    runtime.state = StreamState::Restarting;
                    runtime.cancellation.cancel();
                    runtime.handle.take()
                }
                _ => {
                    return Err(Error::Config(format!(
                        "Stream runtime '{}' is already transitioning",
                        id
                    )))
                }
            }
        };
        let wait_result = match handle {
            Some(handle) => await_task(handle).await,
            None => Ok(()),
        };
        {
            let mut runtime = entry.lock().await;
            match &wait_result {
                Ok(()) => runtime.state = StreamState::Stopped,
                Err(error) => {
                    runtime.state = StreamState::Failed;
                    runtime.record_error("restart", error.to_string());
                }
            }
            runtime.metrics.restarts.fetch_add(1, Ordering::Relaxed);
        }
        wait_result?;
        self.record_event("stream_restart", Some(id.to_string()), "requested", None)
            .await;
        self.start(id).await
    }

    /// Await all currently owned tasks. Natural EOF completion and explicit
    /// shutdown both remove the handle from the registry before this method
    /// returns.
    pub async fn wait_all(&self) -> Result<(), Error> {
        let entries: Vec<_> = self.entries.read().await.values().cloned().collect();
        for entry in entries {
            let handle = entry.lock().await.handle.take();
            if let Some(handle) = handle {
                let wait_result = await_task(handle).await;
                settle_detached_task(&entry, &wait_result, "wait-all").await;
                wait_result?;
            }
        }
        Ok(())
    }
}

/// A task handle is removed from its registry before it is awaited.  If the
/// bounded wait expires, the task is aborted and no supervisor continuation is
/// left to move the entry out of `Stopping`, `Restarting`, or `Running`.  Keep
/// the registry truthful for every caller that owns a detached handle,
/// including startup and `wait_all` paths.
async fn settle_detached_task(
    entry: &Arc<Mutex<RuntimeEntry>>,
    result: &Result<(), Error>,
    phase: &str,
) {
    let mut runtime = entry.lock().await;
    match result {
        Ok(()) => {
            if matches!(
                runtime.state,
                StreamState::Starting
                    | StreamState::Running
                    | StreamState::Stopping
                    | StreamState::Restarting
            ) {
                runtime.state = StreamState::Stopped;
            }
        }
        Err(error) => {
            runtime.state = StreamState::Failed;
            runtime.record_error(phase, error.to_string());
        }
    }
}

async fn await_task(mut handle: JoinHandle<Result<(), Error>>) -> Result<(), Error> {
    match timeout(SHUTDOWN_TIMEOUT, &mut handle).await {
        Ok(result) => {
            result.map_err(|error| Error::Process(format!("Stream task join failed: {error}")))?
        }
        Err(_) => {
            handle.abort();
            // Await the cancellation briefly so resource guards (WAL
            // flushers, temporary stores, and source connections) are
            // dropped before the caller is allowed to restart the stream.
            // A task stuck in a non-cooperative blocking call must not make
            // shutdown wait forever, so the original timeout remains the
            // reported lifecycle error.
            let _ = timeout(Duration::from_secs(1), handle).await;
            Err(Error::Timeout)
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn is_active(state: StreamState) -> bool {
    matches!(
        state,
        StreamState::Starting
            | StreamState::Running
            | StreamState::Stopping
            | StreamState::Restarting
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EngineConfig, LoggingConfig, NodeConfig};
    use crate::input::InputConfig;
    use crate::output::OutputConfig;
    use crate::pipeline::PipelineConfig;
    pub(super) fn stream_config() -> StreamConfig {
        StreamConfig {
            id: Some("orders".into()),
            input: InputConfig {
                input_type: "memory".into(),
                name: None,
                codec: None,
                config: None,
            },
            pipeline: PipelineConfig {
                thread_num: 1,
                processors: vec![],
            },
            output: OutputConfig {
                output_type: "stdout".into(),
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

    #[tokio::test]
    async fn operation_store_is_idempotently_terminal() {
        let store = OperationStore::default();
        let record = store
            .create("start", "stream", "orders", Some("corr-1".into()))
            .await;
        assert_eq!(record.state, OperationState::Queued);
        store
            .update(&record.id, OperationState::Running, 10, None)
            .await;
        let cancelled = store.cancel(&record.id).await.unwrap();
        assert_eq!(cancelled.state, OperationState::Cancelled);
        let unchanged = store
            .update(&record.id, OperationState::Succeeded, 100, None)
            .await
            .unwrap();
        assert_eq!(unchanged.state, OperationState::Cancelled);
        assert_eq!(unchanged.correlation_id.as_deref(), Some("corr-1"));
    }

    #[tokio::test]
    async fn operation_store_covers_success_failure_concurrency_timeout_and_isolation() {
        let store = OperationStore::default();
        let orders = store
            .find_or_create("start", "stream", "orders", Some("orders-corr".into()))
            .await;
        store
            .update(&orders.id, OperationState::Running, 10, None)
            .await;
        let succeeded = store
            .update(&orders.id, OperationState::Succeeded, 100, None)
            .await
            .unwrap();
        assert_eq!(succeeded.state, OperationState::Succeeded);
        assert_eq!(succeeded.progress, 100);

        let metrics = store
            .find_or_create("start", "stream", "metrics", Some("metrics-corr".into()))
            .await;
        let (duplicate_left, duplicate_right) = tokio::join!(
            store.find_or_create("stop", "stream", "metrics", None),
            store.find_or_create("stop", "stream", "metrics", None),
        );
        assert_eq!(duplicate_left.id, duplicate_right.id);
        assert_ne!(duplicate_left.id, orders.id);
        assert_eq!(metrics.resource_id, "metrics");

        let failed = store
            .update(
                &duplicate_left.id,
                OperationState::Failed,
                100,
                Some("expected failure".into()),
            )
            .await
            .unwrap();
        assert_eq!(failed.state, OperationState::Failed);
        assert_eq!(failed.error.as_deref(), Some("expected failure"));

        let timed_out = store.create("restart", "stream", "orders", None).await;
        store
            .update(
                &timed_out.id,
                OperationState::TimedOut,
                100,
                Some("operation deadline exceeded".into()),
            )
            .await;
        let stable_timeout = store
            .update(&timed_out.id, OperationState::Succeeded, 100, None)
            .await
            .unwrap();
        assert_eq!(stable_timeout.state, OperationState::TimedOut);
        assert_eq!(
            stable_timeout.error.as_deref(),
            Some("operation deadline exceeded")
        );
        let terminal_cancel = store.cancel(&timed_out.id).await.unwrap();
        assert_eq!(terminal_cancel.state, OperationState::TimedOut);
        let mut observed = std::collections::BTreeMap::new();
        observed.insert("observed_state".into(), "stopped".into());
        let reconciled = store.set_result(&timed_out.id, observed).await.unwrap();
        assert_eq!(
            reconciled
                .result
                .as_ref()
                .and_then(|result| result.get("observed_state"))
                .map(String::as_str),
            Some("stopped")
        );

        let records = store.list().await;
        assert_eq!(
            records
                .iter()
                .filter(|record| record.resource_id == "orders")
                .count(),
            2
        );
        assert_eq!(
            records
                .iter()
                .filter(|record| record.resource_id == "metrics")
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn manager_registers_and_snapshots_entries() {
        let manager = RuntimeManager::new();
        manager
            .register("orders".into(), stream_config())
            .await
            .unwrap();
        let entry = manager.get("orders").await.unwrap();
        entry.lock().await.state = StreamState::Running;

        let snapshots = manager.snapshots().await;
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].id, "orders");
        assert_eq!(snapshots[0].state, StreamState::Running);
        assert!(manager.get("missing").await.is_none());
    }

    #[tokio::test]
    async fn manager_rejects_duplicate_and_can_remove_entries() {
        let manager = RuntimeManager::new();
        manager
            .register("orders".into(), stream_config())
            .await
            .unwrap();
        assert!(manager
            .register("orders".into(), stream_config())
            .await
            .is_err());
        assert!(manager.remove("orders").await.is_some());
        assert!(manager.ids().await.is_empty());
    }

    #[tokio::test]
    async fn manager_waits_for_registered_task_completion() {
        let manager = RuntimeManager::new();
        manager
            .register("orders".into(), stream_config())
            .await
            .unwrap();
        let entry = manager.get("orders").await.unwrap();
        entry.lock().await.handle = Some(tokio::spawn(async { Ok(()) }));

        manager.wait_all().await.unwrap();
        assert!(entry.lock().await.handle.is_none());
    }

    #[tokio::test]
    async fn manager_shutdown_stops_all_registered_tasks() {
        let manager = RuntimeManager::new();
        manager
            .register("orders".into(), stream_config())
            .await
            .unwrap();
        let entry = manager.get("orders").await.unwrap();
        let cancellation = entry.lock().await.cancellation.clone();
        entry.lock().await.state = StreamState::Running;
        entry.lock().await.handle = Some(tokio::spawn(async move {
            cancellation.cancelled().await;
            Ok(())
        }));

        manager.stop_all().await.unwrap();
        assert_eq!(entry.lock().await.state, StreamState::Stopped);
    }

    #[tokio::test]
    async fn one_supervised_failure_does_not_change_another_runtime() {
        let manager = RuntimeManager::new();
        manager
            .register("failed".into(), stream_config())
            .await
            .unwrap();
        let mut healthy_config = stream_config();
        healthy_config.id = Some("healthy".into());
        manager
            .register("healthy".into(), healthy_config)
            .await
            .unwrap();

        let failed = manager.get("failed").await.unwrap();
        let healthy = manager.get("healthy").await.unwrap();
        failed.lock().await.state = StreamState::Running;
        healthy.lock().await.state = StreamState::Running;
        let handle = manager.spawn_supervised(failed.clone(), async {
            Err(Error::Process("expected failure".into()))
        });
        handle.await.unwrap().unwrap_err();

        assert_eq!(failed.lock().await.state, StreamState::Failed);
        assert_eq!(healthy.lock().await.state, StreamState::Running);
    }

    #[tokio::test]
    async fn stopping_one_runtime_leaves_another_running() {
        let manager = RuntimeManager::new();
        manager
            .register("orders".into(), stream_config())
            .await
            .unwrap();
        let mut metrics_config = stream_config();
        metrics_config.id = Some("metrics".into());
        manager
            .register("metrics".into(), metrics_config)
            .await
            .unwrap();

        for id in ["orders", "metrics"] {
            let entry = manager.get(id).await.unwrap();
            let cancellation = entry.lock().await.cancellation.clone();
            entry.lock().await.state = StreamState::Running;
            entry.lock().await.handle = Some(tokio::spawn(async move {
                cancellation.cancelled().await;
                Ok(())
            }));
        }

        manager.stop("orders").await.unwrap();
        assert_eq!(
            manager.get("orders").await.unwrap().lock().await.state,
            StreamState::Stopped
        );
        assert_eq!(
            manager.get("metrics").await.unwrap().lock().await.state,
            StreamState::Running
        );
        manager.stop("metrics").await.unwrap();
    }

    #[tokio::test]
    async fn replacing_empty_configuration_is_a_noop() {
        let manager = RuntimeManager::new();
        let config = EngineConfig {
            streams: vec![],
            jobs: Vec::new(),
            logging: crate::config::LoggingConfig::default(),
            node: crate::config::NodeConfig::default(),
        };
        assert!(manager.replace_config(&config).await.unwrap().is_empty());
        assert!(manager.ids().await.is_empty());
    }

    #[tokio::test]
    async fn replacing_configuration_reconciles_remove_without_starting_new_components() {
        let manager = RuntimeManager::new();
        manager
            .register("orders".into(), stream_config())
            .await
            .unwrap();
        let config = EngineConfig {
            streams: vec![],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            node: NodeConfig::default(),
        };
        let affected = manager.replace_config(&config).await.unwrap();
        assert_eq!(affected, vec!["orders"]);
        assert!(manager.get("orders").await.is_none());
        assert!(manager.ids().await.is_empty());
    }

    #[tokio::test]
    async fn failed_reconciliation_restores_previous_registry_snapshot() {
        let manager = RuntimeManager::new();
        manager
            .register("orders".into(), stream_config())
            .await
            .unwrap();
        let mut invalid = stream_config();
        invalid.id = Some("broken".into());
        invalid.input.input_type = "missing-input".into();
        let config = EngineConfig {
            streams: vec![invalid],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            node: NodeConfig::default(),
        };
        assert!(manager.replace_config(&config).await.is_err());
        assert!(manager.get("broken").await.is_none());
        assert!(manager.get("orders").await.is_some());
    }

    #[test]
    fn runtime_types_are_usable_in_engine_configs() {
        let config = EngineConfig {
            streams: vec![stream_config()],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            node: NodeConfig::default(),
        };
        assert_eq!(config.stream_ids().unwrap(), ["orders"]);
    }

    #[test]
    fn metrics_snapshot_and_recent_errors_are_bounded() {
        let metrics = RuntimeMetrics::default();
        metrics.input_batches.fetch_add(2, Ordering::Relaxed);
        metrics.output_messages.fetch_add(5, Ordering::Relaxed);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.input_batches, 2);
        assert_eq!(snapshot.output_messages, 5);

        let mut entry = RuntimeEntry::new("orders".into(), stream_config(), 0);
        for index in 0..(MAX_RECENT_ERRORS + 1) {
            entry.record_error("test", index.to_string());
        }
        assert_eq!(entry.recent_errors.len(), MAX_RECENT_ERRORS);
        assert_eq!(
            entry.snapshot().last_error.unwrap().message,
            MAX_RECENT_ERRORS.to_string()
        );
    }
}

// ---------- durability / WAL lifecycle tests (task 2.4) ----------

#[cfg(test)]
mod durability_tests {
    use super::*;
    use crate::input::{Input, InputBuilder};
    use crate::output::{Output, OutputBuilder};
    use crate::Error;
    use std::sync::Arc;

    struct EofInput;

    #[async_trait::async_trait]
    impl Input for EofInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(
            &self,
        ) -> Result<(crate::MessageBatchRef, Arc<dyn crate::input::Ack>), Error> {
            Err(Error::EOF)
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct EofInputBuilder;

    impl InputBuilder for EofInputBuilder {
        fn build(
            &self,
            _name: Option<&str>,
            _config: &Option<serde_json::Value>,
            _codec: Option<Arc<dyn crate::codec::Codec>>,
            _resource: &crate::Resource,
        ) -> Result<Arc<dyn Input>, Error> {
            Ok(Arc::new(EofInput))
        }
    }

    struct DevNullOutput;

    #[async_trait::async_trait]
    impl Output for DevNullOutput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn write(&self, _msg: crate::MessageBatchRef) -> Result<(), Error> {
            Ok(())
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct DevNullOutputBuilder;

    impl OutputBuilder for DevNullOutputBuilder {
        fn build(
            &self,
            _name: Option<&str>,
            _config: &Option<serde_json::Value>,
            _codec: Option<Arc<dyn crate::codec::Codec>>,
            _resource: &crate::Resource,
        ) -> Result<Arc<dyn Output>, Error> {
            Ok(Arc::new(DevNullOutput))
        }
    }

    /// Task 2.4: a durability-enabled stream's dry-run validation opens the
    /// WAL; the real runtime then rebuilds the adapter and reopens the same
    /// redb path. Validation must have flushed and closed its WAL — on every
    /// path — or the real start (and every restart) fails with an
    /// exclusive-lock error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durability_enabled_start_reopens_the_same_wal_path() {
        let _ = crate::input::register_input_builder(
            "runtime-test-eof-input",
            Arc::new(EofInputBuilder),
        );
        let _ = crate::output::register_output_builder(
            "runtime-test-devnull-output",
            Arc::new(DevNullOutputBuilder),
        );
        let directory = tempfile::tempdir().unwrap();
        let mut config = super::tests::stream_config();
        config.input.input_type = "runtime-test-eof-input".into();
        config.output.output_type = "runtime-test-devnull-output".into();
        config.durability = Some(crate::wal::WalConfig::local(
            true,
            directory.path().to_string_lossy().to_string(),
            crate::wal::SyncPolicy::GroupCommit,
        ));

        let manager = RuntimeManager::new();
        manager.register("orders".into(), config).await.unwrap();
        manager.start("orders").await.unwrap();
        manager.wait_all().await.unwrap();
        manager.stop("orders").await.unwrap();

        // Restart: the dry-run and the real runtime must both reopen the WAL
        // path the first start already used.
        manager.start("orders").await.unwrap();
        manager.wait_all().await.unwrap();
        manager.stop("orders").await.unwrap();
    }

    fn one_row_batch() -> crate::MessageBatchRef {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        Arc::new(crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from(vec![1]))],
            )
            .unwrap(),
        ))
    }

    /// The dev-null output settles batches and closes directly; the streams
    /// above never produce a row, so the helpers are pinned here.
    #[tokio::test]
    async fn devnull_output_settles_directly() {
        let output = DevNullOutput;
        output.connect().await.unwrap();
        output.write(one_row_batch()).await.unwrap();
        output.close().await.unwrap();
    }
}

#[cfg(test)]
mod startup_failure_tests {
    use super::*;
    use crate::input::{Input, InputBuilder, InputConfig};
    use crate::output::{Output, OutputBuilder, OutputConfig};
    use crate::pipeline::PipelineConfig;
    use std::sync::Arc;

    struct ConnectFailureInput;

    #[async_trait::async_trait]
    impl Input for ConnectFailureInput {
        async fn connect(&self) -> Result<(), Error> {
            Err(Error::Connection(
                "injected asynchronous input failure".into(),
            ))
        }

        async fn read(
            &self,
        ) -> Result<(crate::MessageBatchRef, Arc<dyn crate::input::Ack>), Error> {
            Err(Error::EOF)
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct ConnectFailureInputBuilder;

    impl InputBuilder for ConnectFailureInputBuilder {
        fn build(
            &self,
            _name: Option<&str>,
            _config: &Option<serde_json::Value>,
            _codec: Option<Arc<dyn crate::codec::Codec>>,
            _resource: &crate::Resource,
        ) -> Result<Arc<dyn Input>, Error> {
            Ok(Arc::new(ConnectFailureInput))
        }
    }

    struct ConnectFailureOutput;

    #[async_trait::async_trait]
    impl Output for ConnectFailureOutput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn write(&self, _msg: crate::MessageBatchRef) -> Result<(), Error> {
            Ok(())
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct ConnectFailureOutputBuilder;

    impl OutputBuilder for ConnectFailureOutputBuilder {
        fn build(
            &self,
            _name: Option<&str>,
            _config: &Option<serde_json::Value>,
            _codec: Option<Arc<dyn crate::codec::Codec>>,
            _resource: &crate::Resource,
        ) -> Result<Arc<dyn Output>, Error> {
            Ok(Arc::new(ConnectFailureOutput))
        }
    }

    /// Task 2.5: a dry-run (compile/build) failure during `start` transitions
    /// the runtime from `Starting` to `Failed` before the error is returned,
    /// so a later start is not rejected because the old state lingers.
    #[tokio::test]
    async fn dry_run_failure_marks_runtime_failed() {
        let manager = RuntimeManager::new();
        let mut config = super::tests::stream_config();
        config.input = InputConfig {
            input_type: "definitely-missing-input".into(),
            name: None,
            codec: None,
            config: None,
        };
        config.output = OutputConfig {
            output_type: "definitely-missing-output".into(),
            name: None,
            codec: None,
            config: None,
        };
        config.pipeline = PipelineConfig {
            thread_num: 1,
            processors: vec![],
        };
        manager.register("broken".into(), config).await.unwrap();
        assert!(manager.start("broken").await.is_err());
        let entry = manager.get("broken").await.unwrap();
        let runtime = entry.lock().await;
        assert_eq!(runtime.state, StreamState::Failed);
        assert!(runtime.snapshot().last_error.is_some());
        // A later start attempts a fresh build rather than being rejected as
        // "already transitioning".
        drop(runtime);
        assert!(manager.start("broken").await.is_err());
    }

    /// Resource connection happens after the synchronous dry-run. The startup
    /// handshake must keep the runtime out of `Running` when that asynchronous
    /// phase fails, and a subsequent start must be allowed to retry.
    #[tokio::test]
    async fn async_resource_failure_marks_runtime_failed() {
        let _ = crate::input::register_input_builder(
            "runtime-test-connect-failure-input",
            Arc::new(ConnectFailureInputBuilder),
        );
        let _ = crate::output::register_output_builder(
            "runtime-test-connect-failure-output",
            Arc::new(ConnectFailureOutputBuilder),
        );
        let mut config = super::tests::stream_config();
        config.input = InputConfig {
            input_type: "runtime-test-connect-failure-input".into(),
            name: None,
            codec: None,
            config: None,
        };
        config.output = OutputConfig {
            output_type: "runtime-test-connect-failure-output".into(),
            name: None,
            codec: None,
            config: None,
        };
        config.pipeline = PipelineConfig {
            thread_num: 1,
            processors: vec![],
        };

        let manager = RuntimeManager::new();
        manager
            .register("connect-broken".into(), config)
            .await
            .unwrap();
        let error = manager.start("connect-broken").await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("injected asynchronous input failure"),
            "unexpected startup error: {error}"
        );
        let entry = manager.get("connect-broken").await.unwrap();
        let runtime = entry.lock().await;
        assert_eq!(runtime.state, StreamState::Failed);
        assert!(runtime.handle.is_none());
    }

    fn one_row_batch() -> crate::MessageBatchRef {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        Arc::new(crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from(vec![1]))],
            )
            .unwrap(),
        ))
    }

    /// The injected-failure helpers settle directly: the connect failure
    /// above stops the graph before read/write ever run.
    #[tokio::test]
    async fn connect_failure_helpers_settle_directly() {
        let input = ConnectFailureInput;
        assert!(input.connect().await.is_err());
        assert!(matches!(input.read().await, Err(Error::EOF)));
        input.close().await.unwrap();

        let output = ConnectFailureOutput;
        output.connect().await.unwrap();
        output.write(one_row_batch()).await.unwrap();
        output.close().await.unwrap();
    }
}

#[cfg(test)]
mod validation_lifecycle_tests {
    use super::*;
    use crate::input::{Input, InputBuilder};
    use crate::output::{Output, OutputBuilder};
    use crate::Error;
    use std::sync::Arc;

    struct EofInput2;

    #[async_trait::async_trait]
    impl Input for EofInput2 {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(
            &self,
        ) -> Result<(crate::MessageBatchRef, Arc<dyn crate::input::Ack>), Error> {
            Err(Error::EOF)
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct EofInputBuilder2;

    impl InputBuilder for EofInputBuilder2 {
        fn build(
            &self,
            _name: Option<&str>,
            _config: &Option<serde_json::Value>,
            _codec: Option<Arc<dyn crate::codec::Codec>>,
            _resource: &crate::Resource,
        ) -> Result<Arc<dyn Input>, Error> {
            Ok(Arc::new(EofInput2))
        }
    }

    struct DevNull2;

    #[async_trait::async_trait]
    impl Output for DevNull2 {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn write(&self, _msg: crate::MessageBatchRef) -> Result<(), Error> {
            Ok(())
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct DevNullBuilder2;

    impl OutputBuilder for DevNullBuilder2 {
        fn build(
            &self,
            _name: Option<&str>,
            _config: &Option<serde_json::Value>,
            _codec: Option<Arc<dyn crate::codec::Codec>>,
            _resource: &crate::Resource,
        ) -> Result<Arc<dyn Output>, Error> {
            Ok(Arc::new(DevNull2))
        }
    }

    /// Task 6.4: validating a durability-enabled candidate never opens (or
    /// leaves open) the WAL or a state backend the real startup needs: after
    /// validation, the same stream starts on the same redb path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn validation_releases_resources_before_real_startup() {
        let _ = crate::input::register_input_builder(
            "runtime-test-eof-input",
            Arc::new(EofInputBuilder2),
        );
        let _ = crate::output::register_output_builder(
            "runtime-test-devnull-output",
            Arc::new(DevNullBuilder2),
        );
        let directory = tempfile::tempdir().unwrap();
        let mut config = super::tests::stream_config();
        config.input.input_type = "runtime-test-eof-input".into();
        config.output.output_type = "runtime-test-devnull-output".into();
        config.durability = Some(crate::wal::WalConfig::local(
            true,
            directory.path().to_string_lossy().to_string(),
            crate::wal::SyncPolicy::GroupCommit,
        ));

        let report = crate::configuration::validate_config(&crate::config::EngineConfig {
            streams: vec![config.clone()],
            jobs: Vec::new(),
            logging: crate::config::LoggingConfig::default(),
            node: crate::config::NodeConfig::default(),
        });
        assert!(report.valid, "{report:?}");

        // The real startup reopens the validated WAL path without an
        // exclusive-lock failure caused by the validator.
        let manager = RuntimeManager::new();
        manager.register("orders".into(), config).await.unwrap();
        manager.start("orders").await.unwrap();
        manager.wait_all().await.unwrap();
        manager.stop("orders").await.unwrap();
    }

    fn one_row_batch() -> crate::MessageBatchRef {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        Arc::new(crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from(vec![1]))],
            )
            .unwrap(),
        ))
    }

    /// The eof/devnull helpers and builders settle directly. Both this module
    /// and the durability module register the same builder names, so exactly
    /// one module's builders win the race; the direct calls keep the other
    /// module's helpers exercised regardless of test order.
    #[tokio::test]
    async fn eof_helpers_and_builders_settle_directly() {
        let input = EofInput2;
        input.connect().await.unwrap();
        assert!(matches!(input.read().await, Err(Error::EOF)));
        input.close().await.unwrap();

        let output = DevNull2;
        output.connect().await.unwrap();
        output.write(one_row_batch()).await.unwrap();
        output.close().await.unwrap();

        let resource = crate::Resource {
            temporary: std::collections::HashMap::new(),
            input_names: std::cell::RefCell::new(Vec::new()),
        };
        EofInputBuilder2
            .build(None, &None, None, &resource)
            .unwrap();
        DevNullBuilder2.build(None, &None, None, &resource).unwrap();
    }
}

#[cfg(test)]
mod race_tests {
    use super::*;
    use crate::input::InputConfig;
    use crate::output::OutputConfig;
    use crate::pipeline::PipelineConfig;

    /// Spec "De-registration and re-registration never reuse an index":
    /// replace_config's stop→remove→register used to shrink len() and let
    /// two concurrently active streams share a derived job ID / state
    /// namespace prefix.
    #[tokio::test]
    async fn register_remove_register_never_reuses_an_index() {
        let manager = RuntimeManager::new();
        let config = |id: &str| StreamConfig {
            id: Some(id.to_string()),
            input: InputConfig {
                input_type: "memory".into(),
                name: None,
                codec: None,
                config: None,
            },
            pipeline: PipelineConfig {
                thread_num: 1,
                processors: vec![],
            },
            output: OutputConfig {
                output_type: "stdout".into(),
                name: None,
                codec: None,
                config: None,
            },
            error_output: None,
            buffer: None,
            durability: None,
            state: None,
            temporary: Default::default(),
        };
        manager.register("a".into(), config("a")).await.unwrap();
        manager.register("b".into(), config("b")).await.unwrap();

        // Remove "a" (replace_config does this internally).
        manager.entries.write().await.remove("a");

        // Register a replacement — the old code gave it index 1 (== b's).
        manager.register("c".into(), config("c")).await.unwrap();

        let (index_b, index_c) = {
            let entries = manager.entries.read().await;
            let b = entries.get("b").unwrap().lock().await.index;
            let c = entries.get("c").unwrap().lock().await.index;
            (b, c)
        };
        assert_ne!(
            index_b, index_c,
            "two active streams must never share an index"
        );
        assert!(
            index_c > index_b,
            "re-registration must get a strictly greater index: b={index_b} c={index_c}"
        );
    }

    /// Spec "Concurrent stop during restart does not leave a Running stream":
    /// the old two-lock-block restart let a concurrent stop take the handle,
    /// join, set Stopped, return Ok — then restart's start() resurrected.
    #[tokio::test]
    async fn concurrent_stop_during_restart_does_not_resurrect() {
        let manager = RuntimeManager::new();
        // Register without starting: the race is at the lock/state level,
        // not the running-task level.
        let config_s = StreamConfig {
            id: Some("s".to_string()),
            input: InputConfig {
                input_type: "memory".into(),
                name: None,
                codec: None,
                config: None,
            },
            pipeline: PipelineConfig {
                thread_num: 1,
                processors: vec![],
            },
            output: OutputConfig {
                output_type: "stdout".into(),
                name: None,
                codec: None,
                config: None,
            },
            error_output: None,
            buffer: None,
            durability: None,
            state: None,
            temporary: Default::default(),
        };
        manager.register("s".into(), config_s).await.unwrap();

        // Manually set up the pre-restart state: Running with a handle
        // (represented by a spawned no-op task).
        let entry = manager.entries.read().await.get("s").cloned().unwrap();
        {
            let mut rt = entry.lock().await;
            rt.state = StreamState::Running;
            rt.handle = Some(tokio::spawn(async { Ok(()) }));
        }

        // Simultaneously issue restart and stop.
        let m1 = manager.clone();
        let m2 = manager.clone();
        let (restart_result, stop_result) = tokio::join!(async { m1.restart("s").await }, async {
            m2.stop("s").await
        },);
        // The restart may legitimately fail here (the test process has no
        // plugin registrations, so start() cannot compile the input). What
        // matters is that the STOP returned Ok without intercepting the
        // restart's handle — the old two-block structure let stop take the
        // handle, join, set Stopped, return Ok, and then restart's start()
        // resurrected the stream into Running.
        let _ = restart_result;
        stop_result.unwrap_or_else(|e| panic!("stop failed: {e}"));

        // The stop early-returns on Restarting (the restart's internal stop
        // fulfils its intent). The restart completes its cycle — the old
        // two-block code let stop take the handle, join, set Stopped, return
        // Ok, and then restart's start() resurrected the stream into
        // Running with the stop already reported successful. The fix makes
        // this interleaving impossible: handle extraction is atomic with
        // the Restarting transition.
        let state = entry.lock().await.state;
        // With the fix, stop early-returned on Restarting (never touched the
        // handle); restart completed its own cycle (start fails on the
        // unregistered input type → Failed). The silent-resurrection shape —
        // stop Ok + stream Running from the restart's start — requires stop
        // to have taken the handle between restart's two lock blocks, which
        // the atomic extraction makes impossible.
        assert!(
            !matches!(state, StreamState::Running),
            "restart's start failed (no plugin registrations); the stream must not be Running: {state:?}"
        );
        assert!(
            matches!(state, StreamState::Failed | StreamState::Stopped),
            "coherent terminal state expected, got {state:?}"
        );
    }
}

#[cfg(test)]
mod coverage_tests {
    use super::*;
    use crate::config::{EngineConfig, LoggingConfig, NodeConfig};
    use crate::input::{Input, InputBuilder, InputConfig};
    use crate::output::{Output, OutputBuilder, OutputConfig};
    use crate::pipeline::PipelineConfig;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    fn stream_config() -> StreamConfig {
        StreamConfig {
            id: Some("orders".into()),
            input: InputConfig {
                input_type: "memory".into(),
                name: None,
                codec: None,
                config: None,
            },
            pipeline: PipelineConfig {
                thread_num: 1,
                processors: vec![],
            },
            output: OutputConfig {
                output_type: "stdout".into(),
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

    fn engine_config(streams: Vec<StreamConfig>) -> EngineConfig {
        EngineConfig {
            streams,
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            node: NodeConfig::default(),
        }
    }

    /// An input that connects and immediately reaches end-of-stream.
    struct EofInput;

    #[async_trait::async_trait]
    impl Input for EofInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(
            &self,
        ) -> Result<(crate::MessageBatchRef, Arc<dyn crate::input::Ack>), Error> {
            Err(Error::EOF)
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct EofInputBuilder;

    impl InputBuilder for EofInputBuilder {
        fn build(
            &self,
            _name: Option<&str>,
            _config: &Option<serde_json::Value>,
            _codec: Option<Arc<dyn crate::codec::Codec>>,
            _resource: &crate::Resource,
        ) -> Result<Arc<dyn Input>, Error> {
            Ok(Arc::new(EofInput))
        }
    }

    /// An output that discards every batch and records the settlements.
    struct DevNullOutput {
        wrote: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl Output for DevNullOutput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn write(&self, _msg: crate::MessageBatchRef) -> Result<(), Error> {
            self.wrote.store(true, Ordering::SeqCst);
            Ok(())
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct DevNullOutputBuilder;

    impl OutputBuilder for DevNullOutputBuilder {
        fn build(
            &self,
            _name: Option<&str>,
            _config: &Option<serde_json::Value>,
            _codec: Option<Arc<dyn crate::codec::Codec>>,
            _resource: &crate::Resource,
        ) -> Result<Arc<dyn Output>, Error> {
            Ok(Arc::new(DevNullOutput {
                wrote: Arc::new(AtomicBool::new(false)),
            }))
        }
    }

    fn eof_stream_config(id: &str) -> StreamConfig {
        let mut config = stream_config();
        config.id = Some(id.into());
        config.input.input_type = "runtime-cov-eof-input".into();
        config.output.output_type = "runtime-cov-devnull-output".into();
        config
    }

    async fn force_state(manager: &RuntimeManager, id: &str, state: StreamState) {
        let entry = manager.get(id).await.unwrap();
        entry.lock().await.state = state;
    }

    async fn state_of(manager: &RuntimeManager, id: &str) -> StreamState {
        manager.get(id).await.unwrap().lock().await.state
    }

    #[tokio::test]
    async fn find_or_create_reuses_the_active_operation() {
        let store = OperationStore::default();
        let first = store.create("start", "stream", "orders", None).await;
        store
            .update(&first.id, OperationState::Running, 10, None)
            .await;
        let second = store
            .find_or_create("start", "stream", "orders", None)
            .await;
        assert_eq!(second.id, first.id, "an active operation is reused");
        assert_eq!(second.state, OperationState::Running);
        assert_eq!(store.get(&first.id).await.unwrap().id, first.id);
        assert!(store.get("missing").await.is_none());
    }

    #[tokio::test]
    async fn find_or_create_starts_fresh_after_the_active_operation_finishes() {
        let store = OperationStore::default();
        let first = store.create("start", "stream", "orders", None).await;
        store
            .update(&first.id, OperationState::Succeeded, 100, None)
            .await;
        let second = store
            .find_or_create("start", "stream", "orders", None)
            .await;
        assert_ne!(second.id, first.id, "a terminal operation is never reused");
        assert_eq!(second.state, OperationState::Queued);
    }

    #[tokio::test]
    async fn find_or_create_evicts_beyond_the_registry_bound() {
        let store = OperationStore::default();
        let mut created = Vec::new();
        for index in 0..=(MAX_OPERATIONS + 4) {
            let record = store
                .find_or_create("restart", "stream", format!("stream-{index}"), None)
                .await;
            created.push(record.id);
        }
        let records = store.list().await;
        assert_eq!(records.len(), MAX_OPERATIONS, "the registry is bounded");
        let mut missing = 0;
        for id in &created {
            if store.get(id).await.is_none() {
                missing += 1;
            }
        }
        assert_eq!(missing, 5, "the overflow was evicted");
    }

    #[tokio::test]
    async fn operation_store_bounds_its_registered_records() {
        let store = OperationStore::default();
        let mut created = Vec::new();
        for index in 0..=(MAX_OPERATIONS + 4) {
            let record = store
                .create("restart", "stream", format!("stream-{index}"), None)
                .await;
            created.push(record.id);
        }
        let records = store.list().await;
        assert_eq!(records.len(), MAX_OPERATIONS, "the registry is bounded");
        // The registry evicts by key order; exactly five of the created
        // records were dropped and the rest remain retrievable.
        let mut missing = 0;
        for id in &created {
            if store.get(id).await.is_none() {
                missing += 1;
            }
        }
        assert_eq!(missing, 5, "the overflow was evicted");
    }

    #[tokio::test]
    async fn event_store_bounds_and_snapshots_its_events() {
        let store = EventStore::default();
        for index in 0..=(MAX_EVENTS + 1) {
            store
                .record(ControlEvent {
                    occurred_at_ms: index as u64,
                    event_type: format!("event-{index}"),
                    stream_id: None,
                    outcome: "ok".into(),
                    message: None,
                    operation_id: None,
                    correlation_id: None,
                    actor: None,
                })
                .await;
        }
        let events = store.snapshot().await;
        assert_eq!(events.len(), MAX_EVENTS, "the event log is bounded");
        assert_eq!(
            events[0].event_type, "event-2",
            "the two oldest events were dropped"
        );
    }

    #[tokio::test]
    async fn manager_mutators_set_observed_state_on_entries() {
        let manager = RuntimeManager::new();
        manager
            .register("orders".into(), stream_config())
            .await
            .unwrap();
        // Missing entries are tolerated.
        manager.set_active_operation("ghost", None).await;
        manager
            .set_last_completed_action("ghost", "action".into())
            .await;

        manager
            .set_active_operation("orders", Some("op-9".into()))
            .await;
        manager
            .set_last_completed_action("orders", "action-1".into())
            .await;
        manager.set_observed_config_version("cfg-7".into()).await;
        assert_eq!(
            manager.observed_config_version().await.as_deref(),
            Some("cfg-7")
        );

        let entry = manager.get("orders").await.unwrap();
        let runtime = entry.lock().await;
        assert_eq!(runtime.active_operation_id.as_deref(), Some("op-9"));
        assert_eq!(
            runtime.last_completed_action_id.as_deref(),
            Some("action-1")
        );
        assert_eq!(
            runtime.observed_config_version.as_deref(),
            Some("cfg-7"),
            "the observed version propagates to every entry"
        );
    }

    #[tokio::test]
    async fn start_rejects_a_runtime_that_is_transitioning_or_running() {
        let manager = RuntimeManager::new();
        manager
            .register("orders".into(), stream_config())
            .await
            .unwrap();
        force_state(&manager, "orders", StreamState::Running).await;
        let error = manager.start("orders").await.unwrap_err().to_string();
        assert!(
            error.contains("already transitioning or running"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn start_settles_a_stale_handle_and_reports_compile_failures() {
        let manager = RuntimeManager::new();
        let mut config = stream_config();
        // A stream id longer than the Job id limit fails at compile time,
        // before any component is built.
        config.id = Some("o".repeat(200));
        manager.register("broken".into(), config).await.unwrap();
        {
            let entry = manager.get("broken").await.unwrap();
            let mut runtime = entry.lock().await;
            runtime.state = StreamState::Stopped;
            runtime.handle = Some(tokio::spawn(async { Ok(()) }));
        }
        let error = manager.start("broken").await.unwrap_err().to_string();
        assert!(error.contains("cannot map to a Job id"), "{error}");
        assert_eq!(
            state_of(&manager, "broken").await,
            StreamState::Failed,
            "a compile failure marks the runtime Failed"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn start_fails_when_the_wal_path_cannot_be_opened() {
        let directory = tempfile::tempdir().unwrap();
        let wal_path = directory.path().join("wal");
        std::fs::write(&wal_path, b"definitely-not-a-redb-file").unwrap();
        let mut config = stream_config();
        config.durability = Some(crate::wal::WalConfig::local(
            true,
            wal_path.to_string_lossy().to_string(),
            crate::wal::SyncPolicy::GroupCommit,
        ));
        let manager = RuntimeManager::new();
        manager.register("orders".into(), config).await.unwrap();
        assert!(manager.start("orders").await.is_err());
        assert_eq!(
            state_of(&manager, "orders").await,
            StreamState::Failed,
            "an unopenable WAL marks the runtime Failed"
        );
    }

    #[tokio::test]
    async fn start_all_fails_fast_on_a_broken_stream() {
        let manager = RuntimeManager::new();
        manager
            .register("broken".into(), stream_config())
            .await
            .unwrap();
        assert!(manager.start_all().await.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replace_config_restarts_a_changed_active_stream() {
        let _ = crate::input::register_input_builder(
            "runtime-cov-eof-input",
            Arc::new(EofInputBuilder),
        );
        let _ = crate::output::register_output_builder(
            "runtime-cov-devnull-output",
            Arc::new(DevNullOutputBuilder),
        );
        let manager = RuntimeManager::new();
        manager
            .register("orders".into(), eof_stream_config("orders"))
            .await
            .unwrap();
        force_state(&manager, "orders", StreamState::Running).await;

        let mut changed = eof_stream_config("orders");
        changed.pipeline.thread_num = 2;
        let affected = manager
            .replace_config(&engine_config(vec![changed]))
            .await
            .unwrap();
        assert_eq!(affected, vec!["orders"]);
        assert!(
            manager.get("orders").await.is_some(),
            "the changed stream is re-registered"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replace_config_registers_and_starts_brand_new_streams() {
        let _ = crate::input::register_input_builder(
            "runtime-cov-eof-input",
            Arc::new(EofInputBuilder),
        );
        let _ = crate::output::register_output_builder(
            "runtime-cov-devnull-output",
            Arc::new(DevNullOutputBuilder),
        );
        let manager = RuntimeManager::new();
        let affected = manager
            .replace_config(&engine_config(vec![eof_stream_config("fresh")]))
            .await
            .unwrap();
        assert_eq!(affected, vec!["fresh"]);
        assert!(manager.get("fresh").await.is_some());
        manager.stop_all().await.unwrap();
    }

    #[tokio::test]
    async fn replace_config_reports_a_restoration_failure() {
        let manager = RuntimeManager::new();
        // A previously "running" stream whose old config is itself broken:
        // reconciliation fails, and so does the restoration attempt.
        let mut old_config = stream_config();
        old_config.input.input_type = "missing-input-old".into();
        manager.register("orders".into(), old_config).await.unwrap();
        force_state(&manager, "orders", StreamState::Running).await;

        let mut new_config = stream_config();
        new_config.input.input_type = "missing-input-new".into();
        let error = manager
            .replace_config(&engine_config(vec![new_config]))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("Configuration apply failed")
                && error.contains("restoration also failed"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn stop_rejects_a_runtime_that_is_already_stopping() {
        let manager = RuntimeManager::new();
        manager
            .register("orders".into(), stream_config())
            .await
            .unwrap();
        force_state(&manager, "orders", StreamState::Stopping).await;
        let error = manager.stop("orders").await.unwrap_err().to_string();
        assert!(error.contains("already stopping"), "{error}");
    }

    #[tokio::test]
    async fn stop_marks_a_failed_task_failed() {
        let manager = RuntimeManager::new();
        manager
            .register("orders".into(), stream_config())
            .await
            .unwrap();
        {
            let entry = manager.get("orders").await.unwrap();
            let mut runtime = entry.lock().await;
            runtime.state = StreamState::Running;
            runtime.handle = Some(tokio::spawn(async {
                Err(Error::Process("task failed".into()))
            }));
        }
        let error = manager.stop("orders").await.unwrap_err();
        assert!(error.to_string().contains("task failed"));
        assert_eq!(state_of(&manager, "orders").await, StreamState::Failed);
    }

    #[tokio::test]
    async fn stop_all_surfaces_the_first_failure() {
        let manager = RuntimeManager::new();
        manager
            .register("bad".into(), stream_config())
            .await
            .unwrap();
        manager
            .register("ok".into(), {
                let mut config = stream_config();
                config.id = Some("ok".into());
                config
            })
            .await
            .unwrap();
        {
            let entry = manager.get("bad").await.unwrap();
            let mut runtime = entry.lock().await;
            runtime.state = StreamState::Running;
            runtime.handle = Some(tokio::spawn(async {
                Err(Error::Process("bad task".into()))
            }));
        }
        force_state(&manager, "ok", StreamState::Created).await;
        assert!(manager.stop_all().await.is_err());
    }

    #[tokio::test]
    async fn restart_rejects_a_runtime_that_is_transitioning() {
        let manager = RuntimeManager::new();
        manager
            .register("orders".into(), stream_config())
            .await
            .unwrap();
        force_state(&manager, "orders", StreamState::Starting).await;
        let error = manager.restart("orders").await.unwrap_err().to_string();
        assert!(error.contains("already transitioning"), "{error}");
    }

    #[tokio::test]
    async fn restart_without_a_handle_completes_the_cycle() {
        let manager = RuntimeManager::new();
        manager
            .register("orders".into(), stream_config())
            .await
            .unwrap();
        force_state(&manager, "orders", StreamState::Running).await;
        // No plugin registrations: the restart's internal start fails, but the
        // stop half of the cycle completed (Stopped -> restart counter bumps).
        assert!(manager.restart("orders").await.is_err());
        let entry = manager.get("orders").await.unwrap();
        let runtime = entry.lock().await;
        assert_eq!(
            runtime.metrics.restarts.load(Ordering::Relaxed),
            1,
            "the restart counter advances even when the start half fails"
        );
        assert!(runtime.handle.is_none());
    }

    #[tokio::test]
    async fn restart_marks_a_failed_task_failed() {
        let manager = RuntimeManager::new();
        manager
            .register("orders".into(), stream_config())
            .await
            .unwrap();
        {
            let entry = manager.get("orders").await.unwrap();
            let mut runtime = entry.lock().await;
            runtime.state = StreamState::Running;
            runtime.handle = Some(tokio::spawn(async {
                Err(Error::Process("restart boom".into()))
            }));
        }
        let error = manager.restart("orders").await.unwrap_err();
        assert!(error.to_string().contains("restart boom"));
        assert_eq!(state_of(&manager, "orders").await, StreamState::Failed);
    }

    #[tokio::test]
    async fn wait_all_settles_every_task_and_surfaces_failures() {
        let manager = RuntimeManager::new();
        let config = |id: &str| {
            let mut config = stream_config();
            config.id = Some(id.into());
            config
        };
        // "a-ok" is visited first: a completing task settles the entry to
        // Stopped. "z-bad" then fails and wait_all returns the error.
        manager
            .register("a-ok".into(), config("a-ok"))
            .await
            .unwrap();
        manager
            .register("z-bad".into(), config("z-bad"))
            .await
            .unwrap();
        {
            let entry = manager.get("a-ok").await.unwrap();
            let mut runtime = entry.lock().await;
            runtime.state = StreamState::Running;
            runtime.handle = Some(tokio::spawn(async { Ok(()) }));
        }
        {
            let entry = manager.get("z-bad").await.unwrap();
            let mut runtime = entry.lock().await;
            runtime.state = StreamState::Running;
            runtime.handle = Some(tokio::spawn(async {
                Err(Error::Process("wait boom".into()))
            }));
        }
        let error = manager.wait_all().await.unwrap_err();
        assert!(error.to_string().contains("wait boom"));
        assert_eq!(state_of(&manager, "a-ok").await, StreamState::Stopped);
        assert_eq!(state_of(&manager, "z-bad").await, StreamState::Failed);
    }

    /// A task that ignores its cancellation token is aborted after the
    /// shutdown timeout; the runtime reports the timeout as the lifecycle
    /// error. Paused time makes the 30s timeout fire immediately.
    #[tokio::test(start_paused = true)]
    async fn stop_aborts_an_unresponsive_task_after_the_timeout() {
        let manager = RuntimeManager::new();
        manager
            .register("orders".into(), stream_config())
            .await
            .unwrap();
        {
            let entry = manager.get("orders").await.unwrap();
            let mut runtime = entry.lock().await;
            runtime.state = StreamState::Running;
            runtime.handle = Some(tokio::spawn(async {
                std::future::pending::<()>().await;
                unreachable!("the pending future never resolves");
            }));
        }
        let error = manager.stop("orders").await.unwrap_err();
        assert!(
            matches!(error, Error::Timeout),
            "expected a shutdown timeout, got {error}"
        );
        assert_eq!(state_of(&manager, "orders").await, StreamState::Failed);
    }

    /// The eof/devnull helpers settle directly: the reconciliation streams
    /// above never produce a row, so write/close would otherwise stay dark.
    #[tokio::test]
    async fn eof_helpers_settle_directly() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;

        EofInput.connect().await.unwrap();
        assert!(matches!(EofInput.read().await, Err(Error::EOF)));
        EofInput.close().await.unwrap();

        let batch = Arc::new(crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from(vec![1]))],
            )
            .unwrap(),
        ));
        DevNullOutput {
            wrote: Arc::new(AtomicBool::new(false)),
        }
        .write(batch)
        .await
        .unwrap();
    }
}
