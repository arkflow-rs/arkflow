//! Transport-neutral control-plane domain facade.
//!
//! This module deliberately has no Axum or HTTP types. `arkflow-server` uses
//! it to expose resources and operations while the Engine remains embeddable.

use crate::config::EngineConfig;
use crate::configuration::{
    parse_and_validate, validate_config, ConfigCandidate, ConfigVersion, ConfigVersionStore,
};
use crate::control::{
    ControlEvent, EngineStatus, NodeResource, OperationRecord, OperationState, Page, StreamStatus,
    SystemResource,
};
use crate::runtime::{EventStore, OperationStore, RuntimeManager};
use crate::Error;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;
use tokio::sync::RwLock;
use tokio::time::{timeout, Duration};

const LIFECYCLE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Default)]
pub struct ControlHealth {
    ready: AtomicBool,
    running: AtomicBool,
}

impl ControlHealth {
    pub fn set_ready(&self, value: bool) {
        self.ready.store(value, Ordering::SeqCst);
    }

    pub fn set_running(&self, value: bool) {
        self.running.store(value, Ordering::SeqCst);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

#[derive(Clone)]
pub struct ControlPlane {
    runtime_manager: RuntimeManager,
    operations: OperationStore,
    events: EventStore,
    started_at: Instant,
    health: Arc<ControlHealth>,
    configuration: Arc<RwLock<EngineConfig>>,
    version_store: ConfigVersionStore,
    node_id: Arc<str>,
    api_token: Option<String>,
    draft: Arc<RwLock<Option<ConfigCandidate>>>,
}

impl ControlPlane {
    pub fn new(config: EngineConfig, runtime_manager: RuntimeManager) -> Self {
        let health = Arc::new(ControlHealth::default());
        Self::with_health(config, runtime_manager, health)
    }

    pub fn with_health(
        config: EngineConfig,
        runtime_manager: RuntimeManager,
        health: Arc<ControlHealth>,
    ) -> Self {
        let events = runtime_manager.event_store();
        let api_token = config.health_check.api_token.clone();
        Self {
            runtime_manager,
            operations: OperationStore::default(),
            events,
            started_at: Instant::now(),
            health,
            configuration: Arc::new(RwLock::new(config)),
            // Overridable so deployments can keep version history on a
            // persistent volume; default unchanged.
            version_store: ConfigVersionStore::new(
                std::env::var("ARKFLOW_CONFIG_HISTORY_DIR")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| ".arkflow/config-history".into()),
            ),
            node_id: Arc::from("local-node"),
            api_token,
            draft: Arc::new(RwLock::new(None)),
        }
    }

    pub fn health(&self) -> Arc<ControlHealth> {
        self.health.clone()
    }

    pub fn runtime_manager(&self) -> RuntimeManager {
        self.runtime_manager.clone()
    }

    pub fn operation_store(&self) -> OperationStore {
        self.operations.clone()
    }

    pub fn version_store(&self) -> ConfigVersionStore {
        self.version_store.clone()
    }

    pub fn api_token(&self) -> Option<&str> {
        self.api_token.as_deref()
    }

    pub fn authorized(&self, supplied: Option<&str>) -> bool {
        let Some(expected) = self.api_token() else {
            return true;
        };
        supplied.is_some_and(|value| value.as_bytes().ct_eq(expected.as_bytes()).into())
    }

    pub async fn system(&self) -> SystemResource {
        let streams = self.runtime_manager.snapshots().await;
        let operations = self.operations.list().await;
        SystemResource {
            id: "arkflow-control-plane".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            state: if self.health.is_running() {
                "running"
            } else {
                "stopped"
            }
            .into(),
            node_count: 1,
            stream_count: streams.len(),
            active_operations: operations
                .iter()
                .filter(|op| matches!(op.state, OperationState::Queued | OperationState::Running))
                .count(),
            capabilities: vec![
                "single_node".into(),
                "stream_lifecycle".into(),
                "configuration_versioning".into(),
                "component_catalogue".into(),
            ],
        }
    }

    pub async fn node(&self) -> NodeResource {
        let status = self.status().await;
        NodeResource {
            id: self.node_id.to_string(),
            role: "standalone".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            state: status.state,
            uptime_seconds: status.uptime_seconds,
            capabilities: vec!["stream_runtime".into(), "local_configuration".into()],
            streams_total: status.streams_total,
            streams_running: status.streams_running,
            streams_failed: status.streams_failed,
            maintenance_state: Default::default(),
        }
    }

    pub async fn status(&self) -> EngineStatus {
        let streams = self.runtime_manager.snapshots().await;
        EngineStatus {
            version: env!("CARGO_PKG_VERSION").into(),
            state: if self.health.is_running() {
                "running"
            } else {
                "stopped"
            }
            .into(),
            uptime_seconds: self.started_at.elapsed().as_secs(),
            streams_total: streams.len(),
            streams_running: streams
                .iter()
                .filter(|stream| matches!(stream.state, crate::control::StreamState::Running))
                .count(),
            streams_failed: streams
                .iter()
                .filter(|stream| matches!(stream.state, crate::control::StreamState::Failed))
                .count(),
        }
    }

    pub async fn streams(&self, page: usize, page_size: usize) -> Page<StreamStatus> {
        let all = self.runtime_manager.snapshots().await;
        let page = page.max(1);
        let page_size = page_size.clamp(1, 100);
        let total = all.len();
        let start = (page - 1).saturating_mul(page_size);
        let items = all.into_iter().skip(start).take(page_size).collect();
        Page {
            items,
            page,
            page_size,
            total,
        }
    }

    pub async fn stream(&self, id: &str) -> Option<StreamStatus> {
        let entry = self.runtime_manager.get(id).await?;
        let status = entry.lock().await.snapshot();
        Some(status)
    }

    pub async fn events(&self) -> Vec<ControlEvent> {
        self.events.snapshot().await
    }

    pub async fn operations(&self) -> Vec<OperationRecord> {
        self.operations.list().await
    }

    pub async fn operation(&self, id: &str) -> Option<OperationRecord> {
        self.operations.get(id).await
    }

    pub async fn cancel_operation(&self, id: &str) -> Option<OperationRecord> {
        let record = self.operations.cancel(id).await?;
        self.events
            .record(ControlEvent {
                occurred_at_ms: now_ms(),
                event_type: "operation_cancelled".into(),
                stream_id: Some(record.resource_id.clone()),
                outcome: "cancelled".into(),
                message: record.error.clone(),
                operation_id: Some(record.id.clone()),
                correlation_id: record.correlation_id.clone(),
                actor: None,
            })
            .await;
        Some(record)
    }

    pub async fn lifecycle(
        &self,
        id: &str,
        operation: &str,
        correlation_id: Option<String>,
    ) -> Result<OperationRecord, Error> {
        if self.runtime_manager.get(id).await.is_none() {
            return Err(Error::Config(format!("Unknown stream runtime: {id}")));
        }
        let record = self
            .operations
            .find_or_create(operation, "stream", id, correlation_id)
            .await;
        if !matches!(record.state, OperationState::Queued) {
            return Ok(record);
        }
        let store = self.operations.clone();
        let manager = self.runtime_manager.clone();
        let events = self.events.clone();
        let operation_id = record.id.clone();
        let stream_id = id.to_string();
        let operation_name = operation.to_string();
        let correlation_id = record.correlation_id.clone();
        manager
            .set_active_operation(&stream_id, Some(operation_id.clone()))
            .await;
        tokio::spawn(async move {
            store
                .update(&operation_id, OperationState::Running, 10, None)
                .await;
            let result = timeout(LIFECYCLE_TIMEOUT, async {
                match operation_name.as_str() {
                    "start" => manager.start(&stream_id).await,
                    "stop" => manager.stop(&stream_id).await,
                    "restart" => manager.restart(&stream_id).await,
                    _ => Err(Error::Config(format!(
                        "Unknown lifecycle operation: {operation_name}"
                    ))),
                }
            })
            .await;
            let observed_state = manager
                .snapshots()
                .await
                .into_iter()
                .find(|stream| stream.id == stream_id)
                .map(|stream| format!("{:?}", stream.state).to_lowercase())
                .unwrap_or_else(|| "unknown".into());
            let mut reconciliation = BTreeMap::new();
            reconciliation.insert("observed_state".into(), observed_state.clone());
            let cancelled = store
                .get(&operation_id)
                .await
                .is_some_and(|record| record.state == OperationState::Cancelled);
            store.set_result(&operation_id, reconciliation).await;
            let (_state, outcome, message) = match result {
                Ok(Ok(())) => {
                    store
                        .update(&operation_id, OperationState::Succeeded, 100, None)
                        .await;
                    if cancelled {
                        (
                            OperationState::Cancelled,
                            "cancelled_reconciled",
                            Some(format!(
                                "Cancellation requested; lifecycle completed with observed state {}",
                                observed_state
                            )),
                        )
                    } else {
                        (OperationState::Succeeded, "succeeded", None)
                    }
                }
                Ok(Err(error)) => {
                    store
                        .update(
                            &operation_id,
                            OperationState::Failed,
                            100,
                            Some(error.to_string()),
                        )
                        .await;
                    if cancelled {
                        (
                            OperationState::Cancelled,
                            "cancelled_reconciled",
                            Some(format!(
                                "Cancellation requested; lifecycle ended with error: {}",
                                error
                            )),
                        )
                    } else {
                        (OperationState::Failed, "failed", Some(error.to_string()))
                    }
                }
                Err(_elapsed) => {
                    store
                        .update(
                            &operation_id,
                            OperationState::TimedOut,
                            100,
                            Some("Lifecycle operation timed out".into()),
                        )
                        .await;
                    if cancelled {
                        (
                            OperationState::Cancelled,
                            "cancelled_reconciled",
                            Some("Cancellation requested; lifecycle operation timed out".into()),
                        )
                    } else {
                        (
                            OperationState::TimedOut,
                            "timed_out",
                            Some("Lifecycle operation timed out".into()),
                        )
                    }
                }
            };
            events
                .record(ControlEvent {
                    occurred_at_ms: now_ms(),
                    event_type: "stream_operation".into(),
                    stream_id: Some(stream_id.clone()),
                    outcome: outcome.into(),
                    message,
                    operation_id: Some(operation_id.clone()),
                    correlation_id,
                    actor: None,
                })
                .await;
            manager.set_active_operation(&stream_id, None).await;
        });
        Ok(record)
    }

    pub async fn configuration(&self) -> EngineConfig {
        self.configuration.read().await.clone()
    }

    pub async fn draft(&self) -> Option<ConfigCandidate> {
        self.draft.read().await.clone()
    }

    pub async fn set_draft(&self, candidate: ConfigCandidate) -> ConfigCandidate {
        *self.draft.write().await = Some(candidate.clone());
        candidate
    }

    pub fn versions(&self) -> Result<Vec<ConfigVersion>, Error> {
        self.version_store
            .list()
            .map_err(|error| Error::Config(error.to_string()))
    }

    pub async fn apply_configuration(
        &self,
        candidate: &ConfigCandidate,
    ) -> Result<serde_json::Value, Error> {
        let config = candidate
            .parse()
            .map_err(|issue| Error::Config(issue.message))?;
        let report = validate_config(&config);
        if !report.valid {
            return Err(Error::Config("Configuration validation failed".into()));
        }
        let parent = self
            .version_store
            .list()
            .ok()
            .and_then(|items| items.first().map(|item| item.id.clone()));
        let version = self
            .version_store
            .save_with_parent(candidate, parent)
            .map_err(|error| Error::Config(error.to_string()))?;
        let affected = match self.runtime_manager.replace_config(&config).await {
            Ok(affected) => affected,
            Err(error) => {
                // Retract the never-active version so the history only
                // contains rollback points that really ran. Best-effort: a
                // failed delete leaves a stale record (harmless for
                // diffing) and is logged.
                if let Err(delete_error) = self.version_store.delete_version(&version.id) {
                    tracing::warn!(
                        version = %version.id,
                        %delete_error,
                        "failed to retract the never-applied configuration version"
                    );
                }
                return Err(error);
            }
        };
        *self.configuration.write().await = config;
        Ok(serde_json::json!({"version": version, "affected_streams": affected}))
    }

    pub async fn rollback_configuration(&self, id: &str) -> Result<serde_json::Value, Error> {
        let candidate = self
            .version_store
            .load(id)
            .map_err(|error| Error::Config(error.to_string()))?;
        let config = candidate
            .parse()
            .map_err(|issue| Error::Config(issue.message))?;
        let report = validate_config(&config);
        if !report.valid {
            return Err(Error::Config("Configuration validation failed".into()));
        }
        let version = self
            .version_store
            .save_with_parent(&candidate, Some(id.to_string()))
            .map_err(|error| Error::Config(error.to_string()))?;
        let affected = match self.runtime_manager.replace_config(&config).await {
            Ok(affected) => affected,
            Err(error) => {
                // Same retraction rule as apply: a rollback that failed to
                // become active must not linger as a rollback point.
                if let Err(delete_error) = self.version_store.delete_version(&version.id) {
                    tracing::warn!(
                        version = %version.id,
                        %delete_error,
                        "failed to retract the never-applied rollback version"
                    );
                }
                return Err(error);
            }
        };
        *self.configuration.write().await = config;
        Ok(
            serde_json::json!({"rollback_from": id, "version": version, "affected_streams": affected}),
        )
    }

    pub fn validate_configuration(
        &self,
        candidate: &ConfigCandidate,
    ) -> crate::configuration::ConfigValidationReport {
        match parse_and_validate(candidate) {
            Ok(report) => report,
            Err(issue) => crate::configuration::ConfigValidationReport {
                valid: false,
                errors: vec![issue],
            },
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod retraction_tests {
    use super::*;
    use crate::config::HealthCheckConfig;
    use crate::input::{Input, InputBuilder};
    use crate::output::{Output, OutputBuilder};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Succeeds for the validation dry-build, fails for the real build: a
    /// component error that only surfaces after validation, which is the
    /// retraction path's trigger (validation and replace each build once).
    struct SecondBuildFailsInput;
    static BUILDS: AtomicUsize = AtomicUsize::new(0);

    #[async_trait::async_trait]
    impl Input for SecondBuildFailsInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(crate::MessageBatchRef, Arc<dyn crate::input::Ack>), Error>
        {
            std::future::pending().await
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct SecondBuildFailsInputBuilder;

    impl InputBuilder for SecondBuildFailsInputBuilder {
        fn build(
            &self,
            _name: Option<&String>,
            _config: &Option<serde_json::Value>,
            _codec: Option<Arc<dyn crate::codec::Codec>>,
            _resource: &crate::Resource,
        ) -> Result<Arc<dyn Input>, Error> {
            if BUILDS.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(Arc::new(SecondBuildFailsInput))
            } else {
                Err(Error::Config("second build fails".into()))
            }
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
            _name: Option<&String>,
            _config: &Option<serde_json::Value>,
            _codec: Option<Arc<dyn crate::codec::Codec>>,
            _resource: &crate::Resource,
        ) -> Result<Arc<dyn Output>, Error> {
            Ok(Arc::new(DevNullOutput))
        }
    }

    /// Serializes the two tests that touch the process-global
    /// `ARKFLOW_CONFIG_HISTORY_DIR` (and restores it via RAII): a parallel
    /// sibling constructing a ControlPlane while the override is active
    /// would silently redirect its version history into the temp dir.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct EnvOverride<'a> {
        _guard: std::sync::MutexGuard<'a, ()>,
    }

    impl Drop for EnvOverride<'_> {
        fn drop(&mut self) {
            std::env::remove_var("ARKFLOW_CONFIG_HISTORY_DIR");
        }
    }

    fn override_history_dir(directory: &tempfile::TempDir) -> EnvOverride<'static> {
        // Take the lock BEFORE publishing the variable so a sibling test
        // holding it cannot observe the override mid-flight; Drop clears
        // the variable even on panic.
        let _guard = ENV_LOCK.lock().unwrap_or_else(|poison| poison.into_inner());
        std::env::set_var(
            "ARKFLOW_CONFIG_HISTORY_DIR",
            directory.path().as_os_str(),
        );
        EnvOverride { _guard }
    }

    fn engine() -> crate::engine::Engine {
        crate::engine::Engine::new(EngineConfig {
            streams: vec![],
            jobs: Vec::new(),
            logging: crate::config::LoggingConfig::default(),
            health_check: HealthCheckConfig::default(),
        })
    }

    fn stream_candidate() -> crate::configuration::ConfigCandidate {
        crate::configuration::ConfigCandidate {
            format: crate::configuration::ConfigFormat::Json,
            content: serde_json::json!({
                "streams": [{
                    "id": "orders",
                    "input": {"type": "cp-retraction-input"},
                    "pipeline": {"thread_num": 1, "processors": []},
                    "output": {"type": "cp-retraction-output"}
                }]
            })
            .to_string(),
            content_verbatim: None,
        }
    }

    /// Spec: a version whose runtime application fails is retracted, so the
    /// history only contains versions that were actually active.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)] // the cross-await hold IS the serialization
    async fn failed_apply_retracts_the_never_active_version() {
        let _serialization = ENV_LOCK.lock().unwrap_or_else(|poison| poison.into_inner());
        let _ = crate::input::register_input_builder(
            "cp-retraction-input",
            Arc::new(SecondBuildFailsInputBuilder),
        );
        let _ = crate::output::register_output_builder(
            "cp-retraction-output",
            Arc::new(DevNullOutputBuilder),
        );
        BUILDS.store(0, Ordering::SeqCst);
        let engine = engine();
        let control_plane = engine.control_plane();

        let error = control_plane
            .apply_configuration(&stream_candidate())
            .await
            .expect_err("the real build fails after validation");
        assert!(
            error.to_string().contains("second build fails"),
            "{error}"
        );
        let versions = control_plane.version_store().list().unwrap();
        assert!(
            versions.is_empty(),
            "the never-applied version must be retracted: {versions:?}"
        );
    }

    /// Spec: a successful apply keeps its version (the retraction is not
    /// over-eager), and `ARKFLOW_CONFIG_HISTORY_DIR` redirects the store.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successful_apply_keeps_version_and_env_dir_redirects() {
        // A build that never fails: reuse the same builder fresh counter.
        struct AlwaysOkInput;
        #[async_trait::async_trait]
        impl Input for AlwaysOkInput {
            async fn connect(&self) -> Result<(), Error> {
                Ok(())
            }
            async fn read(
                &self,
            ) -> Result<(crate::MessageBatchRef, Arc<dyn crate::input::Ack>), Error> {
                std::future::pending().await
            }
            async fn close(&self) -> Result<(), Error> {
                Ok(())
            }
        }
        struct AlwaysOkInputBuilder;
        impl InputBuilder for AlwaysOkInputBuilder {
            fn build(
                &self,
                _name: Option<&String>,
                _config: &Option<serde_json::Value>,
                _codec: Option<Arc<dyn crate::codec::Codec>>,
                _resource: &crate::Resource,
            ) -> Result<Arc<dyn Input>, Error> {
                Ok(Arc::new(AlwaysOkInput))
            }
        }
        let _ = crate::input::register_input_builder(
            "cp-retraction-always-ok-input",
            Arc::new(AlwaysOkInputBuilder),
        );
        let _ = crate::output::register_output_builder(
            "cp-retraction-output",
            Arc::new(DevNullOutputBuilder),
        );
        let directory = tempfile::tempdir().unwrap();
        let _env = override_history_dir(&directory);
        let engine = engine();
        let control_plane = engine.control_plane();
        let candidate = crate::configuration::ConfigCandidate {
            format: crate::configuration::ConfigFormat::Json,
            content: serde_json::json!({
                "streams": [{
                    "id": "orders",
                    "input": {"type": "cp-retraction-always-ok-input"},
                    "pipeline": {"thread_num": 1, "processors": []},
                    "output": {"type": "cp-retraction-output"}
                }]
            })
            .to_string(),
            content_verbatim: None,
        };
        control_plane
            .apply_configuration(&candidate)
            .await
            .expect("apply succeeds");
        let versions = control_plane.version_store().list().unwrap();
        assert_eq!(versions.len(), 1, "the applied version is retained");
        let entries = std::fs::read_dir(directory.path()).unwrap().count();
        assert_eq!(entries, 1, "the version lives under the env-provided dir");
    }
}
