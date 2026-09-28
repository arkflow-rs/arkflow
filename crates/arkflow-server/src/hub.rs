//! Hub-side node registry and command broker.
//!
//! The Hub owns fleet state; compute nodes own execution. This module contains
//! the transport-neutral state machine used by the HTTP handlers and Agent
//! client protocol.

use crate::agent::{delete_checkpoint_artifact, recovery_record_is_valid};
use crate::api_contract::{OperatorAction, OperatorPrincipal, OperatorRole, ResourceScope};
use crate::storage::{
    AttemptRecord, DesiredMutation, IntentRecord, JobCheckpointRecord, JobRecord, JobVersionRecord,
    NodeMutation, ObservedMutation, PersistedOperation, RolloutRecord, RolloutTargetRecord,
    RolloutTargetUpdate, StorageActor, StorageError,
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

mod checkpoint;
mod command_metrics;
mod error;
mod jobs;
mod leadership;
mod lifecycle;
mod nodes;
mod observability;
mod operations;
mod operator;
mod placement;
mod rollout;
mod wire;

#[cfg(test)]
mod session_report_tests;
#[cfg(test)]
mod tests;

pub use command_metrics::CommandMetrics;
pub use error::HubError;
pub(crate) use wire::default_protocol_version;
pub use wire::{
    AgentAuth, AgentCommand, CommandResult, HeartbeatRequest, HubEvent, HubNode, HubNodeMetrics,
    HubOperation, HubOperationState, JobObservationRequest, NodeConnectionState, NodeReport,
    RegisterRequest, RegisterResponse,
};
pub use leadership::{HubHaConfig, Leadership};

// Internal helpers referenced across submodules.
pub(crate) use checkpoint::recovery_record_is_compatible;
pub(crate) use nodes::{bounded_text, parse_operator_credential, required_capabilities};
pub(crate) use operations::{is_durable_job_start, MAX_JOB_OPERATION_RETRIES};
pub(crate) use placement::node_under_pressure;
// Private helpers the module tests reach through the root glob.
#[cfg(test)]
pub(crate) use checkpoint::job_state_format_version;
#[cfg(test)]
pub(crate) use nodes::{sanitize_capabilities, sanitize_metrics};
#[cfg(test)]
pub(crate) use placement::{rank_candidates, RESOURCE_GAUGE_FRESH_MS};

const MAX_NODES: usize = 256;
const MAX_COMMANDS_PER_NODE: usize = 128;
const MAX_OPERATIONS: usize = 1024;
const MAX_EVENTS: usize = 2048;
const SUPPORTED_PROTOCOL_VERSION: &str = "v1";

#[derive(Debug, Clone)]
pub struct HubConfig {
    pub operator_token: Option<String>,
    pub node_token: Option<String>,
    /// Explicitly permit the legacy unauthenticated behavior for an
    /// in-process/local-development Hub. Production startup validates this
    /// flag against the loopback bind address before serving.
    pub insecure_local: bool,
    pub lease_ttl_ms: u64,
    pub poll_interval_ms: u64,
    /// Hard lifetime of an issued agent session credential. Credentials are
    /// never renewed: after expiry every agent request is rejected and the
    /// Agent transparently re-registers through its normal reconnect loop.
    pub session_ttl_ms: u64,
}

pub fn default_session_ttl_ms() -> u64 {
    3_600_000
}

#[derive(Debug, Clone)]
pub(crate) struct NodeRecord {
    resource: HubNode,
    session_token: String,
    /// Wall-clock instant after which the session token stops authenticating.
    /// Credentials are never renewed; only re-registration mints a new one.
    session_expires_at_ms: u64,
    boot_id: Option<String>,
    report_seq: u64,
    commands: VecDeque<AgentCommand>,
    /// Commands returned by the poll endpoint remain leased until a terminal
    /// result arrives. Keeping the lease separately from the queue lets the
    /// Hub recover a command whose HTTP response was lost after it was popped
    /// but before the Agent executed it.
    leased_commands: BTreeMap<String, AgentCommand>,
    streams: Vec<StreamStatus>,
    operations: Vec<OperationRecord>,
    events: Vec<ControlEvent>,
    metrics: BTreeMap<String, f64>,
    /// Last report that carried the metrics above (0 = never reported).
    /// Heartbeats refresh `last_seen_at_ms` but not this, so gauge freshness
    /// stays honest for placement ranking and pressure detection.
    last_report_at_ms: u64,
    /// Consecutive reports over the fleet pressure predicate; reset by any
    /// under-threshold (or gauge-less) report. Drives opt-in rebalancing.
    pressure_streak: u32,
    /// Most recent per-Job kernel snapshots reported by the Agent.
    jobs: BTreeMap<String, arkflow_core::executor::metrics::KernelMetricsSnapshot>,
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
    job_versions: Arc<RwLock<BTreeMap<String, Vec<JobVersionRecord>>>>,
    job_checkpoints: Arc<RwLock<BTreeMap<String, Vec<JobCheckpointRecord>>>>,
    /// The node order each Job's placement was actually dispatched in, so a
    /// retained placement re-dispatches with the identical task→node mapping
    /// (split round-robin and multi-component co-location are order
    /// sensitive). In-memory by design: after a Hub restart the order falls
    /// back to node-id order until the next ranked placement, which is a
    /// legal re-placement (state restores per task attempt).
    placement_order: Arc<RwLock<BTreeMap<String, Vec<String>>>>,
    command_metrics: Arc<CommandMetrics>,
    /// Lease-election configuration (hub-ha stage 2). Default is disabled,
    /// which keeps single-instance behavior byte-identical.
    ha: HubHaConfig,
    /// Current leadership view. `Disabled` bypasses every gate.
    leadership: Arc<RwLock<Leadership>>,
    /// Leadership transitions observed by this process (observability).
    leadership_transitions: Arc<AtomicU64>,
    /// Optional OIDC JWT bearer federation (see `crate::oidc`). Static
    /// operator credentials keep priority when both are configured.
    oidc: Option<Arc<crate::oidc::OidcFederation>>,
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

static HUB_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

pub fn now_ms_for_metrics() -> u64 {
    now_ms()
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
            job_versions: Arc::new(RwLock::new(BTreeMap::new())),
            job_checkpoints: Arc::new(RwLock::new(BTreeMap::new())),
            placement_order: Arc::new(RwLock::new(BTreeMap::new())),
            command_metrics: Arc::new(CommandMetrics::default()),
            ha: HubHaConfig::default(),
            leadership: Arc::new(RwLock::new(Leadership::Disabled)),
            leadership_transitions: Arc::new(AtomicU64::new(0)),
            oidc: None,
        }
    }

    pub fn with_storage(config: HubConfig, storage: StorageActor) -> Self {
        let mut hub = Self::new(config);
        hub.storage = Some(storage);
        hub
    }

    /// Enables OIDC JWT bearer principals and (when client credentials are
    /// configured) the browser login flow. See `crate::oidc`.
    pub fn with_oidc(mut self, oidc: Arc<crate::oidc::OidcFederation>) -> Self {
        self.oidc = Some(oidc);
        self
    }

    /// The federation when the browser login flow is enabled.
    pub fn oidc_login(&self) -> Option<Arc<crate::oidc::OidcFederation>> {
        let federation = self.oidc.as_ref()?;
        federation.login_enabled().then(|| federation.clone())
    }

    /// Resolves an `arkflow_session` cookie value to its principal.
    pub fn oidc_session_principal(&self, session_id: &str) -> Option<OperatorPrincipal> {
        let federation = self.oidc.as_ref()?;
        federation.resolve_session(session_id)
    }

    pub fn has_storage(&self) -> bool {
        self.storage.is_some()
    }

    /// Startup diagnostics for the fail-open token defaults: an unset token
    /// means the corresponding route class accepts unauthenticated callers.
    pub fn operator_token_is_set(&self) -> bool {
        self.config
            .operator_token
            .as_deref()
            .is_some_and(|token| !token.trim().is_empty())
    }

    pub fn node_token_is_set(&self) -> bool {
        self.config
            .node_token
            .as_deref()
            .is_some_and(|token| !token.trim().is_empty())
    }
}
