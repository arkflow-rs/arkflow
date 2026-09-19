//! Versioned contracts for the distributed, stateful Job runtime.
//!
//! This module is deliberately independent from the legacy YAML [`Stream`]
//! runtime. A Job is a deployable, partitioned dataflow; a Stream remains the
//! compatibility runtime for existing configurations.

use crate::{input::Input, output::Output, processor::Processor, Error, Resource};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub const DEFAULT_MAX_PARALLELISM: u32 = 128;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(String);

impl JobId {
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(Error::Config(format!(
                "invalid Job id '{value}'; use 1-128 letters, numbers, '-' or '_'"
            )));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct JobVersion(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Draft,
    Validated,
    Starting,
    Running,
    Stopping,
    Stopped,
    Recovering,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobDesiredState {
    #[default]
    Stopped,
    Running,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobConvergenceState {
    #[default]
    Unknown,
    Pending,
    Applying,
    InSync,
    Degraded,
    Blocked,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct JobMetricsSnapshot {
    pub watermark_lag_ms: u64,
    pub state_bytes: u64,
    pub checkpoint_duration_ms: u64,
    pub checkpoint_failures: u64,
    pub recovery_progress: f64,
    pub task_pressure: f64,
    pub partition_health: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorKind {
    Source,
    Map,
    Filter,
    Aggregate,
    Window,
    Join,
    Sink,
    Udf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorSpec {
    pub id: String,
    pub kind: OperatorKind,
    #[serde(default)]
    pub stateful: bool,
    #[serde(default)]
    pub key_field: Option<String>,
    #[serde(default)]
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeSpec {
    pub id: String,
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub partitioned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSpec {
    pub operator_id: String,
    pub input_type: String,
    #[serde(default)]
    pub config: serde_json::Value,
    pub time: TimeSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkSpec {
    pub operator_id: String,
    pub output_type: String,
    #[serde(default)]
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeSpec {
    pub mode: TimeMode,
    #[serde(default)]
    pub timestamp_field: Option<String>,
    #[serde(default)]
    pub watermark: Option<WatermarkSpec>,
    #[serde(default)]
    pub allowed_lateness_ms: u64,
    #[serde(default)]
    pub late_event_policy: LateEventPolicy,
    #[serde(default)]
    pub late_event_route: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeMode {
    ProcessingTime,
    EventTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatermarkSpec {
    pub strategy: WatermarkStrategy,
    #[serde(default)]
    pub out_of_orderness_ms: u64,
    #[serde(default)]
    pub idle_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatermarkStrategy {
    #[default]
    BoundedOutOfOrderness,
    Monotonous,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LateEventPolicy {
    #[default]
    Drop,
    Route,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateSpec {
    pub backend: String,
    /// Whether state may be discarded when the process or node is replaced.
    /// Durable is the safe default for stateful Jobs.
    #[serde(default)]
    pub durability: StateDurability,
    /// Stable working-state root. When absent, the runtime uses
    /// `ARKFLOW_STATE_ROOT` or `data/arkflow-state` for durable state.
    #[serde(default)]
    pub root: Option<String>,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub ttl_ms: Option<u64>,
    #[serde(default = "default_state_format_version")]
    pub format_version: u32,
    /// Maximum simultaneously staged state journal transactions for this Job
    /// (one per open window group or unacknowledged output). Raise it when a
    /// window operator sees very high per-window key cardinality; the
    /// default bounds staged memory, so pair a raise with realistic
    /// capacity planning.
    #[serde(default)]
    pub max_pending_transactions: Option<usize>,
    /// Optional live state-byte budget enforced by disk-backed state.
    #[serde(default)]
    pub max_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateDurability {
    #[default]
    Durable,
    Ephemeral,
}

fn default_state_format_version() -> u32 {
    1
}

/// Encode a namespace component without allowing its separators to become
/// ambiguous with the surrounding state path.  Keep the common identifier
/// characters readable while escaping every other byte, including `%`.
fn encode_state_component(value: &str) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            encoded.push(byte as char);
        } else {
            write!(&mut encoded, "%{byte:02X}").expect("writing to a String cannot fail");
        }
    }
    encoded
}

/// Build the stable namespace used by every keyed/window operator in a Job.
/// The configured namespace is a logical prefix; Job/operator/task identity is
/// always retained so equal user prefixes cannot collide across the plan.
pub fn effective_state_namespace(
    job_id: &JobId,
    state: Option<&StateSpec>,
    operator_id: &str,
    task_id: &str,
) -> String {
    let prefix = state
        .and_then(|state| state.namespace.as_deref())
        .filter(|prefix| !prefix.trim().is_empty())
        .unwrap_or("default");
    format!(
        "job:{}:state:{}:operator:{}:task:{}",
        encode_state_component(job_id.as_str()),
        encode_state_component(prefix),
        encode_state_component(operator_id),
        encode_state_component(task_id),
    )
}

/// The stable logical prefix shared by every operator/task namespace in one
/// Job. Checkpoint compatibility uses this prefix to reject artifacts written
/// for a different user namespace before restoring any bytes.
pub fn state_namespace_prefix(job_id: &JobId, state: Option<&StateSpec>) -> String {
    let prefix = state
        .and_then(|state| state.namespace.as_deref())
        .filter(|prefix| !prefix.trim().is_empty())
        .unwrap_or("default");
    format!(
        "job:{}:state:{}:",
        encode_state_component(job_id.as_str()),
        encode_state_component(prefix),
    )
}

/// Resolve a durable working-state root without falling back to the process
/// temporary directory. The caller appends Job/version/node/generation
/// components appropriate for its execution mode.
pub fn configured_state_root(state: &StateSpec) -> PathBuf {
    state
        .root
        .as_deref()
        .filter(|root| !root.trim().is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("ARKFLOW_STATE_ROOT").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("data/arkflow-state"))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointSpec {
    #[serde(default = "default_checkpoint_interval")]
    pub interval_ms: u64,
    #[serde(default = "default_checkpoint_retention")]
    pub retention: u32,
    pub object_store_uri: String,
}

fn default_checkpoint_interval() -> u64 {
    30_000
}

fn default_checkpoint_retention() -> u32 {
    3
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryPolicy {
    #[default]
    LatestCheckpoint,
    LatestSavepoint,
    Fail,
}

/// Placement strategy for a Job's tasks across compute nodes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementStrategy {
    /// Keep every connected component of the Job graph on one node — the
    /// historical contract. An edge split across nodes fails placement.
    #[default]
    Colocated,
    /// Spread tasks across nodes in physical plan order (round-robin); data
    /// edges cross nodes via the network shuffle data plane. Side edges
    /// (error, late-event route) must stay co-located.
    Split,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSpec {
    pub id: JobId,
    pub version: JobVersion,
    #[serde(default = "default_max_parallelism")]
    pub max_parallelism: u32,
    #[serde(default = "default_parallelism")]
    pub parallelism: u32,
    pub operators: Vec<OperatorSpec>,
    #[serde(default)]
    pub edges: Vec<EdgeSpec>,
    #[serde(default)]
    pub sources: Vec<SourceSpec>,
    #[serde(default)]
    pub sinks: Vec<SinkSpec>,
    #[serde(default)]
    pub state: Option<StateSpec>,
    #[serde(default)]
    pub checkpoint: Option<CheckpointSpec>,
    #[serde(default)]
    pub recovery: RecoveryPolicy,
    /// Task placement across compute nodes. Defaults to the historical
    /// co-location contract.
    #[serde(default)]
    pub placement: PlacementStrategy,
}

fn default_max_parallelism() -> u32 {
    DEFAULT_MAX_PARALLELISM
}

fn default_parallelism() -> u32 {
    1
}

impl JobSpec {
    /// Enumerate all runtime side edges which must remain local to a task
    /// subtask.  These edges are not ordinary dataflow edges: error routes
    /// and late-event routes are created by the executor while it builds a
    /// graph, so placement must account for them before any Agent is
    /// dispatched.
    pub fn side_edges(&self) -> Vec<SideEdgeSpec> {
        let mut side_edges = BTreeSet::new();

        for edge in &self.edges {
            let is_error_sink = self
                .operators
                .iter()
                .find(|operator| operator.id == edge.to)
                .and_then(|operator| operator.config.get("__arkflow_error_sink"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            if is_error_sink {
                side_edges.insert(SideEdgeSpec {
                    from: edge.from.clone(),
                    to: edge.to.clone(),
                    kind: SideEdgeKind::Error,
                });
            }
        }

        for source in &self.sources {
            let Some(route) = source.time.late_event_route.as_ref() else {
                continue;
            };
            side_edges.insert(SideEdgeSpec {
                from: source.operator_id.clone(),
                to: route.clone(),
                kind: SideEdgeKind::LateEvent,
            });

            // Unified event-time Session windows own their dynamic deadline.
            // A late row can therefore be emitted directly by the window
            // task, rather than by the source gate.  Account for that extra
            // runtime edge during placement as well.
            if source.time.mode != TimeMode::EventTime {
                continue;
            }
            for operator in &self.operators {
                if operator.kind != OperatorKind::Window
                    || !self.operator_reachable(&source.operator_id, &operator.id)
                {
                    continue;
                }
                let Ok(config) = serde_json::from_value::<
                    crate::executor::window::WindowOperatorConfig,
                >(operator.config.clone()) else {
                    continue;
                };
                if config.trigger == crate::executor::window::WindowTrigger::Watermark
                    && !config.legacy_payload
                    && matches!(
                        config.kind,
                        crate::executor::window::WindowKind::Session { .. }
                    )
                {
                    side_edges.insert(SideEdgeSpec {
                        from: operator.id.clone(),
                        to: route.clone(),
                        kind: SideEdgeKind::LateEvent,
                    });
                }
            }
        }

        side_edges.into_iter().collect()
    }

    fn operator_reachable(&self, from: &str, target: &str) -> bool {
        let mut queue = vec![from.to_owned()];
        let mut visited = BTreeSet::new();
        while let Some(operator_id) = queue.pop() {
            if !visited.insert(operator_id.clone()) {
                continue;
            }
            if operator_id == target {
                return true;
            }
            queue.extend(
                self.edges
                    .iter()
                    .filter(|edge| edge.from == operator_id)
                    .map(|edge| edge.to.clone()),
            );
        }
        false
    }

    /// Whether the Job contains an operator whose runtime state must survive
    /// a restart. A Job may still declare a backend/checkpoint for metrics or
    /// future use, but stateless source/sink plans have no keyed state that
    /// requires a recovery artifact before they can be started again.
    pub fn requires_state(&self) -> bool {
        self.operators
            .iter()
            .any(|operator| operator.stateful || operator.kind == OperatorKind::Window)
    }

    pub fn validate(&self) -> Result<(), Error> {
        JobId::new(self.id.as_str())?;
        if let Some(state) = self.state.as_ref() {
            if state.max_pending_transactions == Some(0) {
                // A zero bound fails every journal begin at runtime; reject it
                // where the operator can still fix the spec.
                return Err(Error::Config(
                    "state.max_pending_transactions must be positive".into(),
                ));
            }
        }
        if self.sources.is_empty() {
            return Err(Error::Config(
                "Job requires at least one executable source".into(),
            ));
        }
        if self.sinks.is_empty() {
            return Err(Error::Config(
                "Job requires at least one executable sink".into(),
            ));
        }
        if self.parallelism == 0 || self.parallelism > self.max_parallelism {
            return Err(Error::Config(format!(
                "Job '{}' parallelism must be between 1 and max_parallelism",
                self.id
            )));
        }
        if self.max_parallelism == 0 {
            return Err(Error::Config("Job max_parallelism must be positive".into()));
        }

        let requires_state = self.requires_state();
        if self.state.is_none() {
            if requires_state {
                return Err(Error::Config(
                    "stateful or Window Jobs require an explicit state specification; configure state.durability: ephemeral for non-recoverable development runs or provide durable state and checkpoints".into(),
                ));
            }
            if self.checkpoint.is_some() {
                return Err(Error::Config(
                    "checkpointing a Job requires a state specification".into(),
                ));
            }
        }

        let mut operator_ids = BTreeSet::new();
        for operator in &self.operators {
            if operator.id.is_empty() || !operator_ids.insert(operator.id.clone()) {
                return Err(Error::Config(format!(
                    "Job '{}' contains a duplicate or empty operator id",
                    self.id
                )));
            }
            if operator.kind == OperatorKind::Join {
                return Err(Error::Config(format!(
                    "Join operator '{}' is not supported by the distributed runtime; use a supported single-input operator or a dedicated multi-input Join runtime",
                    operator.id
                )));
            }
            if operator.stateful && operator.key_field.is_none() {
                return Err(Error::Config(format!(
                    "stateful operator '{}' requires key_field",
                    operator.id
                )));
            }
            if operator.stateful
                && matches!(operator.kind, OperatorKind::Source | OperatorKind::Sink)
            {
                return Err(Error::Config(format!(
                    "stateful {} operator '{}' is not supported",
                    match operator.kind {
                        OperatorKind::Source => "Source",
                        OperatorKind::Sink => "Sink",
                        _ => unreachable!("matched Source or Sink above"),
                    },
                    operator.id
                )));
            }
            if operator.kind == OperatorKind::Window {
                let window: crate::executor::window::WindowOperatorConfig =
                    serde_json::from_value(operator.config.clone()).map_err(|error| {
                        Error::Config(format!(
                            "window operator '{}' has invalid config: {error}",
                            operator.id
                        ))
                    })?;
                window.validate()?;
            }
        }

        let mut edge_ids = BTreeSet::new();
        let mut edge_pairs = BTreeSet::new();
        for edge in &self.edges {
            if edge.id.is_empty() || !edge_ids.insert(edge.id.clone()) {
                return Err(Error::Config(format!(
                    "Job '{}' contains a duplicate or empty edge id",
                    self.id
                )));
            }
            if edge.from == edge.to {
                return Err(Error::Config(format!(
                    "Job '{}' contains a self-loop on operator '{}'",
                    self.id, edge.from
                )));
            }
            if !operator_ids.contains(&edge.from) || !operator_ids.contains(&edge.to) {
                return Err(Error::Config(format!(
                    "edge '{}' references an unknown operator",
                    edge.id
                )));
            }
            // Sources have no input port and sinks have no output port; a
            // misdirected edge otherwise surfaces only as a runtime failure
            // (or a silently dead channel for sink out-edges).
            let kind_of = |id: &str| {
                self.operators
                    .iter()
                    .find(|operator| operator.id == id)
                    .map(|operator| operator.kind)
            };
            if kind_of(&edge.from) == Some(OperatorKind::Sink) {
                return Err(Error::Config(format!(
                    "edge '{}' takes input from sink operator '{}', which has no output port",
                    edge.id, edge.from
                )));
            }
            if kind_of(&edge.to) == Some(OperatorKind::Source) {
                return Err(Error::Config(format!(
                    "edge '{}' feeds source operator '{}', which has no input port",
                    edge.id, edge.to
                )));
            }
            if !edge_pairs.insert((edge.from.clone(), edge.to.clone())) {
                return Err(Error::Config(format!(
                    "Job '{}' contains duplicate edges from '{}' to '{}'",
                    self.id, edge.from, edge.to
                )));
            }
        }

        // A late-event Route is a runtime side edge. It is intentionally not
        // required to be repeated in `edges`, but it must still be validated
        // as part of the operator DAG so placement and reachability cannot
        // split the route target away from its source.
        let mut late_route_links = BTreeSet::new();
        for source in &self.sources {
            let Some(route_operator) = source.time.late_event_route.as_deref() else {
                continue;
            };
            let Some(route) = self
                .operators
                .iter()
                .find(|operator| operator.id == route_operator)
            else {
                return Err(Error::Config(format!(
                    "late-event route target '{}' for source '{}' is unknown",
                    route_operator, source.operator_id
                )));
            };
            if route.kind == OperatorKind::Source || route.id == source.operator_id {
                return Err(Error::Config(format!(
                    "late-event route target '{}' for source '{}' must not be a Source or itself",
                    route_operator, source.operator_id
                )));
            }
            if edge_pairs.contains(&(source.operator_id.clone(), route_operator.to_owned())) {
                return Err(Error::Config(format!(
                    "late-event route target '{}' for source '{}' must be a side target, not a normal edge",
                    route_operator, source.operator_id
                )));
            }
            late_route_links.insert((source.operator_id.clone(), route_operator.to_owned()));
        }

        let mut outgoing = BTreeMap::<String, Vec<String>>::new();
        let mut indegree = operator_ids
            .iter()
            .map(|operator_id| (operator_id.clone(), 0usize))
            .collect::<BTreeMap<_, _>>();
        for edge in &self.edges {
            outgoing
                .entry(edge.from.clone())
                .or_default()
                .push(edge.to.clone());
            if let Some(degree) = indegree.get_mut(&edge.to) {
                *degree += 1;
            }
        }
        for (from, to) in &late_route_links {
            outgoing.entry(from.clone()).or_default().push(to.clone());
            if let Some(degree) = indegree.get_mut(to) {
                *degree += 1;
            }
        }
        let mut ready = indegree
            .iter()
            .filter_map(|(operator_id, degree)| (*degree == 0).then_some(operator_id.clone()))
            .collect::<std::collections::VecDeque<_>>();
        let mut visited = 0usize;
        while let Some(operator_id) = ready.pop_front() {
            visited += 1;
            for downstream in outgoing.get(&operator_id).into_iter().flatten() {
                let degree = indegree
                    .get_mut(downstream)
                    .expect("edge endpoints were validated above");
                *degree -= 1;
                if *degree == 0 {
                    ready.push_back(downstream.clone());
                }
            }
        }
        if visited != operator_ids.len() {
            return Err(Error::Config(format!(
                "Job '{}' operator graph must be acyclic",
                self.id
            )));
        }

        for source in &self.sources {
            let Some(operator) = self
                .operators
                .iter()
                .find(|operator| operator.id == source.operator_id)
            else {
                return Err(Error::Config(format!(
                    "source references unknown operator '{}'",
                    source.operator_id
                )));
            };
            if operator.kind != OperatorKind::Source {
                return Err(Error::Config(format!(
                    "source '{}' must reference a Source operator",
                    source.operator_id
                )));
            }
            source.time.validate(&source.operator_id)?;
        }

        for sink in &self.sinks {
            let Some(operator) = self
                .operators
                .iter()
                .find(|operator| operator.id == sink.operator_id)
            else {
                return Err(Error::Config(format!(
                    "sink references unknown operator '{}'",
                    sink.operator_id
                )));
            };
            if operator.kind != OperatorKind::Sink {
                return Err(Error::Config(format!(
                    "sink '{}' must reference a Sink operator",
                    sink.operator_id
                )));
            }
        }

        // Every declared source must have a path to a sink. Otherwise the
        // runner can acknowledge input from an executable source without ever
        // dispatching the batch to an output.
        for source in &self.sources {
            let mut reachable = BTreeSet::from([source.operator_id.as_str()]);
            let mut pending = std::collections::VecDeque::from([source.operator_id.as_str()]);
            while let Some(operator_id) = pending.pop_front() {
                for downstream in outgoing.get(operator_id).into_iter().flatten() {
                    if reachable.insert(downstream.as_str()) {
                        pending.push_back(downstream.as_str());
                    }
                }
            }
            if !self
                .sinks
                .iter()
                .any(|sink| reachable.contains(sink.operator_id.as_str()))
            {
                return Err(Error::Config(format!(
                    "source '{}' cannot reach any executable sink",
                    source.operator_id
                )));
            }
        }

        let mut incoming = BTreeMap::<&str, Vec<&str>>::new();
        for edge in &self.edges {
            incoming
                .entry(edge.to.as_str())
                .or_default()
                .push(edge.from.as_str());
        }
        for (from, to) in &late_route_links {
            incoming.entry(to.as_str()).or_default().push(from.as_str());
        }
        for sink in &self.sinks {
            let mut reachable = BTreeSet::from([sink.operator_id.as_str()]);
            let mut pending = std::collections::VecDeque::from([sink.operator_id.as_str()]);
            while let Some(operator_id) = pending.pop_front() {
                for upstream in incoming.get(operator_id).into_iter().flatten() {
                    if reachable.insert(upstream) {
                        pending.push_back(upstream);
                    }
                }
            }
            if !self
                .sources
                .iter()
                .any(|source| reachable.contains(source.operator_id.as_str()))
            {
                return Err(Error::Config(format!(
                    "sink '{}' cannot be reached from any executable source",
                    sink.operator_id
                )));
            }
        }

        if self.checkpoint.is_some() && self.state.is_none() {
            return Err(Error::Config(
                "checkpointing a Job requires a state specification".into(),
            ));
        }
        if let Some(checkpoint) = &self.checkpoint {
            if checkpoint.interval_ms == 0 || checkpoint.retention == 0 {
                return Err(Error::Config(
                    "checkpoint interval and retention must be positive".into(),
                ));
            }
            if checkpoint.object_store_uri.trim().is_empty() {
                return Err(Error::Config(
                    "checkpoint object_store_uri is required".into(),
                ));
            }
        }
        if let Some(state) = &self.state {
            if state
                .root
                .as_deref()
                .is_some_and(|root| root.trim().is_empty())
            {
                return Err(Error::Config(
                    "Job state root must not be empty when configured".into(),
                ));
            }
            if state.format_version == 0 {
                return Err(Error::Config(
                    "Job state format_version must be positive".into(),
                ));
            }
            if state.max_bytes == Some(0) {
                return Err(Error::Config(
                    "Job state max_bytes must be positive".into(),
                ));
            }
            if requires_state
                && state.durability == StateDurability::Durable
                && self.checkpoint.is_none()
            {
                return Err(Error::Config(
                    "durable stateful or Window Jobs require a checkpoint specification".into(),
                ));
            }
        }
        Ok(())
    }
}

impl TimeSpec {
    fn validate(&self, operator_id: &str) -> Result<(), Error> {
        match self.mode {
            TimeMode::ProcessingTime => {
                if self.watermark.is_some() {
                    return Err(Error::Config(format!(
                        "processing-time source '{operator_id}' cannot define a watermark"
                    )));
                }
            }
            TimeMode::EventTime => {
                if self.timestamp_field.as_deref().is_none_or(str::is_empty) {
                    return Err(Error::Config(format!(
                        "event-time source '{operator_id}' requires timestamp_field"
                    )));
                }
                if self.watermark.is_none() {
                    return Err(Error::Config(format!(
                        "event-time source '{operator_id}' requires watermark"
                    )));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyGroupRange {
    pub start: u32,
    pub end: u32,
}

impl KeyGroupRange {
    pub fn contains(&self, key_group: u32) -> bool {
        self.start <= key_group && key_group <= self.end
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionSpec {
    pub id: u32,
    pub key_group: KeyGroupRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSpec {
    pub id: String,
    pub operator_id: String,
    pub subtask: u32,
    pub partitions: Vec<PartitionSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskAttempt {
    pub id: String,
    pub job_id: JobId,
    pub job_version: JobVersion,
    pub task_id: String,
    pub generation: u64,
    pub node_id: String,
    pub state: TaskAttemptState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobPlan {
    pub spec: JobSpec,
    pub tasks: Vec<TaskSpec>,
}

/// Runtime edge which is not represented by a normal dataflow channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SideEdgeKind {
    Error,
    LateEvent,
}

impl SideEdgeKind {
    fn label(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::LateEvent => "late-event",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SideEdgeSpec {
    pub from: String,
    pub to: String,
    pub kind: SideEdgeKind,
}

impl JobPlan {
    pub fn compile(spec: JobSpec) -> Result<Self, Error> {
        spec.validate()?;
        let mut tasks = Vec::new();
        let max_parallelism = spec.max_parallelism;
        for operator in &spec.operators {
            for subtask in 0..spec.parallelism {
                let range = key_group_range(max_parallelism, spec.parallelism, subtask);
                tasks.push(TaskSpec {
                    id: format!("{}-{subtask}", operator.id),
                    operator_id: operator.id.clone(),
                    subtask,
                    partitions: vec![PartitionSpec {
                        id: subtask,
                        key_group: range,
                    }],
                });
            }
        }
        Ok(Self { spec, tasks })
    }

    pub fn task(&self, task_id: &str) -> Option<&TaskSpec> {
        self.tasks.iter().find(|task| task.id == task_id)
    }

    pub fn assignments_for_node(&self, node_id: &str) -> Vec<TaskAttempt> {
        self.tasks
            .iter()
            .map(|task| TaskAttempt {
                id: format!("{}:{node_id}:0", task.id),
                job_id: self.spec.id.clone(),
                job_version: self.spec.version,
                task_id: task.id.clone(),
                generation: 0,
                node_id: node_id.to_owned(),
                state: TaskAttemptState::Queued,
            })
            .collect()
    }

    /// Assign every plan task to a compute node under the Job's placement
    /// strategy. Colocated keeps each connected component on one node; split
    /// round-robins tasks in physical plan order and rejects the assignment
    /// when a side edge (error, late-event route) would cross nodes.
    pub fn assignments_for_nodes(
        &self,
        node_ids: &[String],
        generation: u64,
    ) -> Result<Vec<TaskAttempt>, Error> {
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }
        match self.spec.placement {
            // Split placement REJECTS the whole assignment when a side edge
            // would cross nodes — callers surface the error as a failed
            // placement, never a panic inside placement bookkeeping.
            PlacementStrategy::Split => self.assignments_split(node_ids, generation),
            PlacementStrategy::Colocated => Ok(self.assignments_colocated(node_ids, generation)),
        }
    }

    /// Split placement: physical plan order round-robin across the node set.
    /// Side edges (error, late-event route) must stay co-located — a split
    /// that would separate one fails placement before any dispatch.
    fn assignments_split(
        &self,
        node_ids: &[String],
        generation: u64,
    ) -> Result<Vec<TaskAttempt>, Error> {
        let assignments = self
            .tasks
            .iter()
            .enumerate()
            .map(|(index, task)| {
                let node_id = &node_ids[index % node_ids.len()];
                TaskAttempt {
                    id: format!("{}:{node_id}:{generation}", task.id),
                    job_id: self.spec.id.clone(),
                    job_version: self.spec.version,
                    task_id: task.id.clone(),
                    generation,
                    node_id: node_id.clone(),
                    state: TaskAttemptState::Queued,
                }
            })
            .collect::<Vec<_>>();
        self.validate_side_edge_assignments(&assignments)?;
        Ok(assignments)
    }

    /// Validate every task assignment participating in a runtime side edge.
    /// This is intentionally public so the Hub and Agent can both enforce the
    /// same invariant at their trust boundaries.
    pub fn validate_side_edge_assignments(
        &self,
        assignments: &[TaskAttempt],
    ) -> Result<(), Error> {
        let mut node_by_task = BTreeMap::new();
        for assignment in assignments {
            if node_by_task
                .insert(assignment.task_id.clone(), assignment.node_id.clone())
                .is_some()
            {
                return Err(Error::Config(format!(
                    "duplicate task assignment for '{}'",
                    assignment.task_id
                )));
            }
        }
        self.validate_side_edge_nodes(&node_by_task)
    }

    /// Validate a task→node view before graph construction.  Missing tasks
    /// fail closed instead of allowing a partial assignment to hide a side
    /// route until an event reaches it at runtime.
    pub fn validate_side_edge_nodes(
        &self,
        node_by_task: &BTreeMap<String, String>,
    ) -> Result<(), Error> {
        for side_edge in self.spec.side_edges() {
            for from_subtask in 0..self.spec.parallelism {
                let from_task = format!("{}-{from_subtask}", side_edge.from);
                let from_node = node_by_task.get(&from_task).ok_or_else(|| {
                    Error::Config(format!(
                        "{} side edge '{} -> {}' is missing assignment for task '{}'",
                        side_edge.kind.label(),
                        side_edge.from,
                        side_edge.to,
                        from_task
                    ))
                })?;
                // Runtime late/error routes broadcast from one source task to
                // every target task on the local graph. Matching only equal
                // subtasks would accept a placement where a cross-subtask
                // target is remote and its route is silently omitted.
                for to_subtask in 0..self.spec.parallelism {
                    let to_task = format!("{}-{to_subtask}", side_edge.to);
                    let to_node = node_by_task.get(&to_task).ok_or_else(|| {
                        Error::Config(format!(
                            "{} side edge '{} -> {}' is missing assignment for task '{}'",
                            side_edge.kind.label(),
                            side_edge.from,
                            side_edge.to,
                            to_task
                        ))
                    })?;
                    if from_node != to_node {
                        return Err(Error::Config(format!(
                            "split placement requires co-located side edges: {} edge '{} -> {}' splits across '{}' and '{}' (tasks '{}' and '{}')",
                            side_edge.kind.label(),
                            side_edge.from,
                            side_edge.to,
                            from_node,
                            to_node,
                            from_task,
                            to_task
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    fn assignments_colocated(
        &self,
        node_ids: &[String],
        generation: u64,
    ) -> Vec<TaskAttempt> {
        // The colocated runner has no cross-node data edges. Co-locate
        // every connected operator component so an edge can never disappear
        // merely because its endpoints were assigned to different Agents.
        let mut adjacency = BTreeMap::<String, BTreeSet<String>>::new();
        for operator in &self.spec.operators {
            adjacency.entry(operator.id.clone()).or_default();
        }
        for edge in &self.spec.edges {
            adjacency
                .entry(edge.from.clone())
                .or_default()
                .insert(edge.to.clone());
            adjacency
                .entry(edge.to.clone())
                .or_default()
                .insert(edge.from.clone());
        }
        for side_edge in self.spec.side_edges() {
            adjacency
                .entry(side_edge.from.clone())
                .or_default()
                .insert(side_edge.to.clone());
            adjacency
                .entry(side_edge.to)
                .or_default()
                .insert(side_edge.from);
        }
        let mut component_by_operator = BTreeMap::new();
        let mut visited = BTreeSet::new();
        let mut component_index = 0;
        for operator in &self.spec.operators {
            if !visited.insert(operator.id.clone()) {
                continue;
            }
            let mut pending = vec![operator.id.clone()];
            while let Some(current) = pending.pop() {
                component_by_operator.insert(current.clone(), component_index);
                for neighbor in adjacency.get(&current).into_iter().flatten() {
                    if visited.insert(neighbor.clone()) {
                        pending.push(neighbor.clone());
                    }
                }
            }
            component_index += 1;
        }
        self.tasks
            .iter()
            .map(|task| {
                let component = component_by_operator
                    .get(&task.operator_id)
                    .copied()
                    .unwrap_or_default();
                let node_id = &node_ids[component % node_ids.len()];
                TaskAttempt {
                    id: format!("{}:{node_id}:{generation}", task.id),
                    job_id: self.spec.id.clone(),
                    job_version: self.spec.version,
                    task_id: task.id.clone(),
                    generation,
                    node_id: node_id.clone(),
                    state: TaskAttemptState::Queued,
                }
            })
            .collect()
    }
}

fn key_group_range(max_parallelism: u32, parallelism: u32, subtask: u32) -> KeyGroupRange {
    let start = (u64::from(subtask) * u64::from(max_parallelism) / u64::from(parallelism)) as u32;
    let end = ((u64::from(subtask + 1) * u64::from(max_parallelism) / u64::from(parallelism))
        .saturating_sub(1)) as u32;
    KeyGroupRange { start, end }
}

/// Stable FNV-1a based key-group assignment. It intentionally does not use
/// Rust's randomized `Hash` implementation so ownership is reproducible
/// across process restarts and Compute nodes.
pub fn key_group_for_key(key: &[u8], max_parallelism: u32) -> Result<u32, Error> {
    if max_parallelism == 0 {
        return Err(Error::Config("max_parallelism must be positive".into()));
    }
    let mut hash = 0xcbf29ce484222325u64;
    for byte in key {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    Ok((hash % u64::from(max_parallelism)) as u32)
}

pub fn task_for_key<'a>(
    plan: &'a JobPlan,
    operator_id: &str,
    key: &[u8],
) -> Result<&'a TaskSpec, Error> {
    let group = key_group_for_key(key, plan.spec.max_parallelism)?;
    plan.tasks
        .iter()
        .find(|task| {
            task.operator_id == operator_id
                && task
                    .partitions
                    .iter()
                    .any(|partition| partition.key_group.contains(group))
        })
        .ok_or_else(|| {
            Error::Config(format!(
                "no task owns key group {group} for '{operator_id}'"
            ))
        })
}

#[derive(Debug)]
pub struct TaskAttemptController {
    attempt: TaskAttempt,
    cancellation: CancellationToken,
}

impl TaskAttemptController {
    pub fn new(attempt: TaskAttempt) -> Self {
        Self {
            attempt,
            cancellation: CancellationToken::new(),
        }
    }

    pub fn attempt(&self) -> &TaskAttempt {
        &self.attempt
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn is_stale(&self, generation: u64) -> bool {
        generation < self.attempt.generation || self.attempt.state == TaskAttemptState::Superseded
    }

    pub fn start(&mut self, generation: u64) -> Result<(), Error> {
        if self.is_stale(generation) {
            return Err(Error::Config("stale task attempt generation".into()));
        }
        self.attempt.generation = generation;
        self.attempt.state = TaskAttemptState::Running;
        Ok(())
    }

    pub fn stop(&mut self, generation: u64) -> Result<(), Error> {
        if self.is_stale(generation) {
            return Err(Error::Config("stale task attempt generation".into()));
        }
        self.attempt.state = TaskAttemptState::Stopping;
        self.cancellation.cancel();
        Ok(())
    }

    pub fn supersede(&mut self) {
        self.attempt.state = TaskAttemptState::Superseded;
        self.cancellation.cancel();
    }
}

pub fn bounded_job_channel<T>(
    capacity: usize,
) -> Result<(flume::Sender<T>, flume::Receiver<T>), Error> {
    if capacity == 0 {
        return Err(Error::Config(
            "Job channel capacity must be positive".into(),
        ));
    }
    Ok(flume::bounded(capacity))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskAttemptState {
    Queued,
    Starting,
    Running,
    Stopping,
    Succeeded,
    Failed,
    Cancelled,
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobCommand {
    Start {
        generation: u64,
    },
    Stop {
        generation: u64,
    },
    Restart {
        generation: u64,
        action_id: String,
    },
    Cancel {
        generation: u64,
    },
    Restore {
        generation: u64,
        checkpoint_id: String,
    },
}

impl JobCommand {
    pub fn generation(&self) -> u64 {
        match self {
            Self::Start { generation }
            | Self::Stop { generation }
            | Self::Restart { generation, .. }
            | Self::Cancel { generation }
            | Self::Restore { generation, .. } => *generation,
        }
    }
}

/// Adapter boundary for reusing existing component builders from a Job plan.
/// Implementations may build the same Input/Output/Processor types used by the
/// legacy Stream runtime, but the Job runtime owns task and state lifecycle.
pub trait JobComponentAdapter: Send + Sync {
    fn build_input(
        &self,
        source: &SourceSpec,
        resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error>;

    fn build_output(&self, sink: &SinkSpec, resource: &Resource) -> Result<Arc<dyn Output>, Error>;

    fn build_processor(
        &self,
        operator: &OperatorSpec,
        resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn base_job() -> JobSpec {
        JobSpec {
            placement: PlacementStrategy::Colocated,
            id: JobId::new("orders").unwrap(),
            version: JobVersion(1),
            max_parallelism: 16,
            parallelism: 2,
            operators: vec![
                OperatorSpec {
                    id: "source".into(),
                    kind: OperatorKind::Source,
                    stateful: false,
                    key_field: None,
                    config: serde_json::json!({}),
                },
                OperatorSpec {
                    id: "aggregate".into(),
                    kind: OperatorKind::Aggregate,
                    stateful: true,
                    key_field: Some("customer_id".into()),
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
            edges: vec![
                EdgeSpec {
                    id: "source-aggregate".into(),
                    from: "source".into(),
                    to: "aggregate".into(),
                    partitioned: true,
                },
                EdgeSpec {
                    id: "aggregate-sink".into(),
                    from: "aggregate".into(),
                    to: "sink".into(),
                    partitioned: false,
                },
            ],
            sources: vec![SourceSpec {
                operator_id: "source".into(),
                input_type: "memory".into(),
                config: serde_json::json!({}),
                time: TimeSpec {
                    mode: TimeMode::EventTime,
                    timestamp_field: Some("timestamp".into()),
                    watermark: Some(WatermarkSpec {
                        strategy: WatermarkStrategy::BoundedOutOfOrderness,
                        out_of_orderness_ms: 1_000,
                        idle_timeout_ms: Some(10_000),
                    }),
                    allowed_lateness_ms: 500,
                    late_event_policy: LateEventPolicy::Route,
                    late_event_route: None,
                },
            }],
            sinks: vec![SinkSpec {
                operator_id: "sink".into(),
                output_type: "drop".into(),
                config: serde_json::json!({}),
            }],
            state: Some(StateSpec {
                backend: "embedded_kv".into(),
                durability: StateDurability::Durable,
                root: None,
                namespace: Some("orders".into()),
                ttl_ms: None,
                format_version: 1,
                max_pending_transactions: None,
                max_bytes: None,
            }),
            checkpoint: Some(CheckpointSpec {
                interval_ms: 1_000,
                retention: 2,
                object_store_uri: "s3://arkflow/checkpoints/orders".into(),
            }),
            recovery: RecoveryPolicy::LatestCheckpoint,
        }
    }

    #[test]
    fn rejects_unsupported_join_operator() {
        let mut job = base_job();
        job.operators.push(OperatorSpec {
            id: "join".into(),
            kind: OperatorKind::Join,
            stateful: true,
            key_field: Some("customer_id".into()),
            config: serde_json::json!({}),
        });
        let error = job.validate().unwrap_err().to_string();
        assert!(error.contains("Join operator 'join' is not supported"), "{error}");
    }

    #[test]
    fn validates_a_stateful_event_time_job() {
        assert!(base_job().validate().is_ok());
    }

    #[test]
    fn rejects_stateful_job_without_state_or_checkpoint_contract() {
        let mut missing_state = base_job();
        missing_state.state = None;
        let error = missing_state.validate().unwrap_err().to_string();
        assert!(error.contains("require an explicit state specification"), "{error}");

        let mut missing_checkpoint = base_job();
        missing_checkpoint.checkpoint = None;
        let error = missing_checkpoint.validate().unwrap_err().to_string();
        assert!(
            error.contains("durable stateful or Window Jobs require a checkpoint"),
            "{error}"
        );
    }

    #[test]
    fn ephemeral_stateful_job_may_omit_checkpoint() {
        let mut job = base_job();
        job.checkpoint = None;
        job.state.as_mut().unwrap().durability = StateDurability::Ephemeral;
        assert!(job.validate().is_ok());
    }

    #[test]
    fn rejects_cyclic_operator_graphs() {
        let mut job = base_job();
        // A cycle between two interior operators: edges into/out of sources
        // are rejected by the port-direction check before reachability runs.
        job.operators.push(OperatorSpec {
            id: "enrich".into(),
            kind: OperatorKind::Map,
            stateful: false,
            key_field: None,
            config: serde_json::json!({}),
        });
        job.edges.push(EdgeSpec {
            id: "aggregate-enrich".into(),
            from: "aggregate".into(),
            to: "enrich".into(),
            partitioned: true,
        });
        job.edges.push(EdgeSpec {
            id: "enrich-aggregate".into(),
            from: "enrich".into(),
            to: "aggregate".into(),
            partitioned: true,
        });
        let error = job.validate().unwrap_err().to_string();
        assert!(error.contains("operator graph must be acyclic"));
    }

    /// A zero pending bound validates at config time and then fails every
    /// journal begin at runtime, so the spec is rejected where the operator can
    /// still fix it.
    #[test]
    fn rejects_a_zero_pending_transaction_bound() {
        let mut job = base_job();
        job.state = Some(StateSpec {
            backend: "embedded_kv".into(),
            durability: StateDurability::Durable,
            root: None,
            namespace: Some("orders".into()),
            ttl_ms: None,
            format_version: 1,
            max_pending_transactions: Some(0),
            max_bytes: None,
        });
        let error = job.validate().unwrap_err().to_string();
        assert!(error.contains("max_pending_transactions"), "{error}");

        // A positive bound and the default are accepted.
        if let Some(state) = job.state.as_mut() {
            state.max_pending_transactions = Some(64);
        }
        job.validate().unwrap();
        if let Some(state) = job.state.as_mut() {
            state.max_pending_transactions = None;
        }
        job.validate().unwrap();
    }

    #[test]
    fn rejects_a_zero_state_byte_budget() {
        let mut job = base_job();
        job.state.as_mut().expect("base job state").max_bytes = Some(0);
        let error = job.validate().unwrap_err().to_string();
        assert!(error.contains("max_bytes"), "{error}");

        job.state.as_mut().expect("base job state").max_bytes = Some(1);
        job.validate().unwrap();
    }

    #[test]
    fn effective_state_namespace_preserves_job_operator_and_task_identity() {
        let job = base_job();
        assert_eq!(
            effective_state_namespace(
                &job.id,
                job.state.as_ref(),
                "aggregate",
                "aggregate-1"
            ),
            "job:orders:state:orders:operator:aggregate:task:aggregate-1"
        );
        assert_eq!(
            effective_state_namespace(&job.id, None, "aggregate", "aggregate-1"),
            "job:orders:state:default:operator:aggregate:task:aggregate-1"
        );
    }

    #[test]
    fn effective_state_namespace_escapes_component_separators() {
        let mut job = base_job();
        job.state.as_mut().unwrap().namespace = Some("stable:evil".into());
        let namespace = effective_state_namespace(
            &job.id,
            job.state.as_ref(),
            "aggregate:one",
            "aggregate:one-0",
        );
        assert_eq!(
            namespace,
            "job:orders:state:stable%3Aevil:operator:aggregate%3Aone:task:aggregate%3Aone-0"
        );
        assert_eq!(
            state_namespace_prefix(&job.id, job.state.as_ref()),
            "job:orders:state:stable%3Aevil:"
        );
    }

    #[test]
    fn rejects_edges_into_a_source_or_out_of_a_sink() {
        let mut job = base_job();
        job.edges.push(EdgeSpec {
            id: "sink-aggregate".into(),
            from: "sink".into(),
            to: "aggregate".into(),
            partitioned: false,
        });
        let error = job.validate().unwrap_err().to_string();
        assert!(
            error.contains("no output port"),
            "edges out of a sink are rejected at validate time: {error}"
        );

        let mut job = base_job();
        job.edges.push(EdgeSpec {
            id: "aggregate-source".into(),
            from: "aggregate".into(),
            to: "source".into(),
            partitioned: true,
        });
        let error = job.validate().unwrap_err().to_string();
        assert!(
            error.contains("no input port"),
            "edges into a source are rejected at validate time: {error}"
        );
    }

    #[test]
    fn rejects_invalid_job_id() {
        assert!(JobId::new("bad id").is_err());
        let mut job = base_job();
        job.id = serde_json::from_value(serde_json::json!("../outside")).unwrap();
        assert!(job.validate().is_err());
    }

    #[test]
    fn defaults_state_format_version_to_one() {
        let mut value = serde_json::to_value(base_job()).unwrap();
        value["state"] = serde_json::json!({"backend": "embedded_kv"});
        let job: JobSpec = serde_json::from_value(value).unwrap();
        assert_eq!(job.state.unwrap().format_version, 1);
    }

    #[test]
    fn rejects_zero_state_format_version() {
        let mut job = base_job();
        job.state.as_mut().unwrap().format_version = 0;
        assert!(job.validate().is_err());
    }

    #[test]
    fn rejects_state_without_key() {
        let mut job = base_job();
        job.operators[1].key_field = None;
        assert!(job
            .validate()
            .unwrap_err()
            .to_string()
            .contains("key_field"));
    }

    #[test]
    fn rejects_stateful_sources_and_sinks() {
        let mut source = base_job();
        source.operators[0].stateful = true;
        source.operators[0].key_field = Some("customer_id".into());
        assert!(source
            .validate()
            .unwrap_err()
            .to_string()
            .contains("stateful Source"));

        let mut sink = base_job();
        sink.operators[2].stateful = true;
        sink.operators[2].key_field = Some("customer_id".into());
        assert!(sink
            .validate()
            .unwrap_err()
            .to_string()
            .contains("stateful Sink"));
    }

    #[test]
    fn rejects_sources_that_cannot_reach_a_sink() {
        let mut job = base_job();
        job.operators.push(OperatorSpec {
            id: "orphan-source".into(),
            kind: OperatorKind::Source,
            stateful: false,
            key_field: None,
            config: serde_json::json!({}),
        });
        job.sources.push(SourceSpec {
            operator_id: "orphan-source".into(),
            input_type: "memory".into(),
            config: serde_json::json!({}),
            time: job.sources[0].time.clone(),
        });
        assert!(job
            .validate()
            .unwrap_err()
            .to_string()
            .contains("orphan-source"));
    }

    #[test]
    fn rejects_sinks_that_cannot_be_reached_from_a_source() {
        let mut job = base_job();
        job.operators.push(OperatorSpec {
            id: "orphan-sink".into(),
            kind: OperatorKind::Sink,
            stateful: false,
            key_field: None,
            config: serde_json::json!({}),
        });
        job.sinks.push(SinkSpec {
            operator_id: "orphan-sink".into(),
            output_type: "drop".into(),
            config: serde_json::json!({}),
        });
        assert!(job
            .validate()
            .unwrap_err()
            .to_string()
            .contains("orphan-sink"));
    }

    #[test]
    fn rejects_event_time_without_watermark() {
        let mut job = base_job();
        job.sources[0].time.watermark = None;
        assert!(job
            .validate()
            .unwrap_err()
            .to_string()
            .contains("watermark"));
    }

    #[test]
    fn rejects_duplicate_operator_ids() {
        let mut job = base_job();
        job.operators.push(job.operators[0].clone());
        assert!(job.validate().is_err());
    }

    #[test]
    fn command_exposes_generation_for_fencing() {
        let command = JobCommand::Restore {
            generation: 42,
            checkpoint_id: "cp-1".into(),
        };
        assert_eq!(command.generation(), 42);
    }

    #[test]
    fn compiles_stable_tasks_and_routes_keys() {
        let job = base_job();
        let plan = JobPlan::compile(job).unwrap();
        assert_eq!(plan.tasks.len(), 6);
        let task = task_for_key(&plan, "aggregate", b"customer-1").unwrap();
        assert_eq!(task.operator_id, "aggregate");
    }

    #[test]
    fn assignments_keep_connected_edges_on_one_node() {
        let plan = JobPlan::compile(base_job()).unwrap();
        let nodes = vec!["node-a".into(), "node-b".into()];
        let assignments = plan.assignments_for_nodes(&nodes, 4).unwrap();
        for edge in &plan.spec.edges {
            let from = assignments
                .iter()
                .find(|assignment| plan.task(&assignment.task_id).unwrap().operator_id == edge.from)
                .unwrap();
            let to = assignments
                .iter()
                .find(|assignment| plan.task(&assignment.task_id).unwrap().operator_id == edge.to)
                .unwrap();
            assert_eq!(from.node_id, to.node_id, "edge {} crosses nodes", edge.id);
        }
    }

    #[test]
    fn fences_stale_task_attempts() {
        let attempt = TaskAttempt {
            id: "aggregate-0:node-a:0".into(),
            job_id: JobId::new("orders").unwrap(),
            job_version: JobVersion(1),
            task_id: "aggregate-0".into(),
            generation: 3,
            node_id: "node-a".into(),
            state: TaskAttemptState::Queued,
        };
        let mut controller = TaskAttemptController::new(attempt);
        assert!(controller.start(2).is_err());
        controller.start(3).unwrap();
        controller.supersede();
        assert!(controller.start(4).is_err());
    }

    #[test]
    fn rejects_unbounded_job_channel() {
        assert!(bounded_job_channel::<u8>(0).is_err());
    }
}

#[cfg(test)]
mod placement_tests {
    use super::tests::base_job;
    use super::*;

    fn split_job() -> JobSpec {
        let mut job = base_job();
        job.placement = PlacementStrategy::Split;
        job
    }

    #[test]
    fn split_placement_round_robins_tasks_deterministically() {
        let plan = JobPlan::compile(split_job()).unwrap();
        let nodes = vec!["a".to_string(), "b".to_string()];
        let assignments = plan.assignments_for_nodes(&nodes, 7).unwrap();
        assert_eq!(assignments.len(), 6); // 3 operators × 2 subtasks

        // Physical order alternates across the node set.
        assert_eq!(assignments[0].node_id, "a");
        assert_eq!(assignments[1].node_id, "b");
        assert_eq!(assignments[2].node_id, "a");
        assert_eq!(assignments[3].node_id, "b");

        // Deterministic: the same placement input derives the same mapping.
        assert_eq!(plan.assignments_for_nodes(&nodes, 7).unwrap(), assignments);
        // Generation rides the attempt identity.
        assert!(assignments.iter().all(|attempt| attempt.generation == 7));

        // Same-operator subtasks spread across nodes: the aggregate operator's
        // two subtasks land on different nodes.
        let aggregate_nodes: BTreeSet<String> = assignments
            .iter()
            .filter(|attempt| attempt.task_id.starts_with("aggregate"))
            .map(|attempt| attempt.node_id.clone())
            .collect();
        assert_eq!(aggregate_nodes.len(), 2);
    }

    #[test]
    fn split_placement_rejects_cross_node_late_event_route() {
        let mut job = split_job();
        // parallelism 1: source is task 0 (node a); a dedicated late-event
        // sink operator is task 3 (node b) — the route crosses nodes under
        // any alternating assignment.
        job.parallelism = 1;
        job.operators.push(OperatorSpec {
            id: "late_sink".into(),
            kind: OperatorKind::Sink,
            stateful: false,
            key_field: None,
            config: serde_json::json!({}),
        });
        for source in &mut job.sources {
            source.time.late_event_route = Some("late_sink".into());
        }
        let plan = JobPlan::compile(job).unwrap();
        let nodes = vec!["a".to_string(), "b".to_string()];
        let error = plan
            .assignments_for_nodes(&nodes, 1)
            .expect_err("side-edge split must reject the placement");
        assert!(
            error.to_string().contains("co-located side edges"),
            "expected a side-edge placement rejection, got {error}"
        );
    }

    #[test]
    fn side_edge_validation_checks_all_source_target_subtasks() {
        let mut job = split_job();
        job.parallelism = 2;
        job.operators.push(OperatorSpec {
            id: "late_sink".into(),
            kind: OperatorKind::Sink,
            stateful: false,
            key_field: None,
            config: serde_json::json!({}),
        });
        job.sources[0].time.late_event_route = Some("late_sink".into());
        let plan = JobPlan::compile(job).unwrap();

        // Equal-subtask placement is co-located, but the runtime route also
        // broadcasts source-0 to late_sink-1 (and vice versa).
        let assignments = BTreeMap::from([
            ("source-0".to_string(), "a".to_string()),
            ("source-1".to_string(), "b".to_string()),
            ("late_sink-0".to_string(), "a".to_string()),
            ("late_sink-1".to_string(), "b".to_string()),
        ]);
        let error = plan
            .validate_side_edge_nodes(&assignments)
            .expect_err("cross-subtask side route must be rejected");
        assert!(error.to_string().contains("source-0"));
        assert!(error.to_string().contains("late_sink-1"));
    }

    #[test]
    fn split_placement_rejects_dynamic_session_late_route() {
        let mut job = split_job();
        job.parallelism = 1;
        // Keep the source-side route on node a while the session window lands
        // on node b.  The source route alone would pass the old check; the
        // dynamic window route is the regression this test protects.
        let window = job
            .operators
            .iter_mut()
            .find(|operator| operator.id == "aggregate")
            .expect("base job window placeholder");
        window.kind = OperatorKind::Window;
        window.config = serde_json::json!({
            "kind": "session",
            "gap_ms": 1_000,
            "timestamp_field": "timestamp",
            "key_field": "customer_id",
            "value_fields": ["amount"],
            "trigger": "watermark",
            "watermark_field": "timestamp"
        });
        let late_sink = OperatorSpec {
            id: "late_sink".into(),
            kind: OperatorKind::Sink,
            stateful: false,
            key_field: None,
            config: serde_json::json!({}),
        };
        let sink_index = job
            .operators
            .iter()
            .position(|operator| operator.id == "sink")
            .expect("base sink");
        job.operators.insert(sink_index, late_sink);
        job.sources[0].time.late_event_route = Some("late_sink".into());

        let plan = JobPlan::compile(job).unwrap();
        let error = plan
            .assignments_for_nodes(&["a".into(), "b".into()], 1)
            .expect_err("dynamic session side edge must reject the placement");
        assert!(
            error.to_string().contains("aggregate -> late_sink")
                || error.to_string().contains("window -> late_sink"),
            "expected the dynamic window side edge in the rejection, got {error}"
        );
    }

    #[test]
    fn split_placement_rejects_cross_node_error_route() {
        let mut job = split_job();
        job.parallelism = 1;
        job.operators.push(OperatorSpec {
            id: "error_sink".into(),
            kind: OperatorKind::Sink,
            stateful: false,
            key_field: None,
            config: serde_json::json!({"__arkflow_error_sink": true}),
        });
        job.edges.push(EdgeSpec {
            id: "source-error".into(),
            from: "source".into(),
            to: "error_sink".into(),
            partitioned: false,
        });
        let plan = JobPlan::compile(job).unwrap();
        let error = plan
            .assignments_for_nodes(&["a".into(), "b".into()], 1)
            .expect_err("error side edge must reject the placement");
        assert!(
            error.to_string().contains("error edge 'source -> error_sink'"),
            "expected the error side edge in the rejection, got {error}"
        );
    }

    #[test]
    fn split_placement_accepts_colocated_side_edge_with_split_data_edges() {
        let mut job = split_job();
        job.parallelism = 1;
        let late_sink = OperatorSpec {
            id: "late_sink".into(),
            kind: OperatorKind::Sink,
            stateful: false,
            key_field: None,
            config: serde_json::json!({}),
        };
        let sink_index = job
            .operators
            .iter()
            .position(|operator| operator.id == "sink")
            .expect("base sink");
        job.operators.insert(sink_index, late_sink);
        job.sources[0].time.late_event_route = Some("late_sink".into());

        let plan = JobPlan::compile(job).unwrap();
        let assignments = plan
            .assignments_for_nodes(&["a".into(), "b".into()], 1)
            .expect("co-located side edge should be accepted");
        assert_eq!(
            assignments
                .iter()
                .find(|assignment| assignment.task_id == "source-0")
                .unwrap()
                .node_id,
            "a"
        );
        assert_eq!(
            assignments
                .iter()
                .find(|assignment| assignment.task_id == "late_sink-0")
                .unwrap()
                .node_id,
            "a"
        );
        assert_ne!(
            assignments
                .iter()
                .find(|assignment| assignment.task_id == "aggregate-0")
                .unwrap()
                .node_id,
            assignments
                .iter()
                .find(|assignment| assignment.task_id == "source-0")
                .unwrap()
                .node_id
        );
    }

    #[test]
    fn colocated_placement_ignores_split_rules() {
        let plan = JobPlan::compile(base_job()).unwrap();
        let nodes = vec!["a".to_string(), "b".to_string()];
        // Colocated keeps whole components: every task of one job lands on ONE
        // node (round-robin over components, single component here).
        let assignments = plan.assignments_for_nodes(&nodes, 1).unwrap();
        assert!(assignments.iter().all(|attempt| attempt.node_id == "a"));
    }

}
