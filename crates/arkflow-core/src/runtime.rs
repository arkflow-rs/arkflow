//! Process-local supervision primitives for control-plane managed Streams.

use crate::config::EngineConfig;
use crate::control::{
    ControlEvent, DesiredState, OperationRecord, OperationState, RuntimeErrorEvent,
    StreamMetricsSnapshot, StreamState, StreamStatus,
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
            OperationState::Cancelled | OperationState::TimedOut
        ) && !matches!(state, OperationState::Cancelled | OperationState::TimedOut)
        {
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
}

impl RuntimeMetrics {
    pub fn snapshot(&self) -> StreamMetricsSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
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
        }
    }
}

/// Mutable state associated with one registered Stream.
pub struct RuntimeEntry {
    pub id: String,
    pub config: StreamConfig,
    pub state: StreamState,
    pub cancellation: CancellationToken,
    pub handle: Option<JoinHandle<Result<(), Error>>>,
    pub metrics: Arc<RuntimeMetrics>,
    pub started_at_ms: Option<u64>,
    pub active_operation_id: Option<String>,
    pub node_id: String,
    pub recent_errors: VecDeque<RuntimeErrorEvent>,
}

impl RuntimeEntry {
    pub fn new(id: String, config: StreamConfig) -> Self {
        Self {
            id,
            config,
            state: StreamState::Created,
            cancellation: CancellationToken::new(),
            handle: None,
            metrics: Arc::new(RuntimeMetrics::default()),
            started_at_ms: None,
            active_operation_id: None,
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
            desired_state: Some(match self.state {
                StreamState::Running | StreamState::Starting | StreamState::Restarting => {
                    DesiredState::Running
                }
                _ => DesiredState::Stopped,
            }),
            transition_started_at_ms: self.started_at_ms,
            active_operation_id: self.active_operation_id.clone(),
            node_id: Some(self.node_id.clone()),
            started_at_ms: self.started_at_ms,
            last_error: self.recent_errors.back().cloned(),
            metrics: self.metrics.snapshot(),
        }
    }
}

/// Process-local registry of independently managed Stream runtimes.
#[derive(Clone, Default)]
pub struct RuntimeManager {
    entries: Arc<RwLock<BTreeMap<String, Arc<Mutex<RuntimeEntry>>>>>,
    events: EventStore,
}

impl RuntimeManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a Stream without awaiting while holding the registry lock.
    pub async fn register(&self, id: String, config: StreamConfig) -> Result<(), Error> {
        let mut entries = self.entries.write().await;
        if entries.contains_key(&id) {
            return Err(Error::Config(format!(
                "Stream runtime already registered: {id}"
            )));
        }
        entries.insert(
            id.clone(),
            Arc::new(Mutex::new(RuntimeEntry::new(id, config))),
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
            await_task(handle).await?;
        }

        let metrics = entry.lock().await.metrics.clone();
        let mut stream = match config.build() {
            Ok(stream) => stream.with_metrics(metrics),
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

        let handle =
            self.spawn_supervised(entry.clone(), async move { stream.run(cancellation).await });

        let mut runtime = entry.lock().await;
        runtime.state = StreamState::Running;
        runtime.started_at_ms = Some(now_ms());
        runtime.handle = Some(handle);
        drop(runtime);
        self.record_event("stream_start", Some(id.to_string()), "succeeded", None)
            .await;
        Ok(())
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

        if let Some(handle) = handle {
            await_task(handle).await?;
        }

        let mut runtime = entry.lock().await;
        runtime.state = StreamState::Stopped;
        drop(runtime);
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
        {
            let mut runtime = entry.lock().await;
            match runtime.state {
                StreamState::Running | StreamState::Failed | StreamState::Stopped => {
                    runtime.state = StreamState::Restarting;
                    runtime.cancellation.cancel();
                }
                _ => {
                    return Err(Error::Config(format!(
                        "Stream runtime '{}' is already transitioning",
                        id
                    )))
                }
            }
        }

        // stop() accepts Restarting as an active state and performs the
        // resource/task join. Starting then creates a fresh cancellation
        // token and component graph.
        let handle = {
            let mut runtime = entry.lock().await;
            runtime.handle.take()
        };
        if let Some(handle) = handle {
            await_task(handle).await?;
        }
        {
            let mut runtime = entry.lock().await;
            runtime.state = StreamState::Stopped;
            runtime.metrics.restarts.fetch_add(1, Ordering::Relaxed);
        }
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
                await_task(handle).await?;
            }
        }
        Ok(())
    }
}

async fn await_task(mut handle: JoinHandle<Result<(), Error>>) -> Result<(), Error> {
    match timeout(SHUTDOWN_TIMEOUT, &mut handle).await {
        Ok(result) => {
            result.map_err(|error| Error::Process(format!("Stream task join failed: {error}")))?
        }
        Err(_) => {
            handle.abort();
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
    use crate::config::{EngineConfig, HealthCheckConfig, LoggingConfig};
    use crate::input::InputConfig;
    use crate::output::OutputConfig;
    use crate::pipeline::PipelineConfig;

    fn stream_config() -> StreamConfig {
        StreamConfig {
            id: Some("orders".into()),
            input: InputConfig {
                input_type: "generate".into(),
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
            logging: crate::config::LoggingConfig::default(),
            health_check: crate::config::HealthCheckConfig::default(),
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
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig::default(),
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
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig::default(),
        };
        assert!(manager.replace_config(&config).await.is_err());
        assert!(manager.get("broken").await.is_none());
        assert!(manager.get("orders").await.is_some());
    }

    #[test]
    fn runtime_types_are_usable_in_engine_configs() {
        let config = EngineConfig {
            streams: vec![stream_config()],
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig::default(),
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

        let mut entry = RuntimeEntry::new("orders".into(), stream_config());
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
