//! Versioned contracts for the distributed, stateful Job runtime.
//!
//! This module is deliberately independent from the legacy YAML [`Stream`]
//! runtime. A Job is a deployable, partitioned dataflow; a Stream remains the
//! compatibility runtime for existing configurations.

use crate::{input::Input, output::Output, processor::Processor, Error, Resource};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
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
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub ttl_ms: Option<u64>,
    #[serde(default)]
    pub format_version: u32,
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
}

fn default_max_parallelism() -> u32 {
    DEFAULT_MAX_PARALLELISM
}

fn default_parallelism() -> u32 {
    1
}

impl JobSpec {
    pub fn validate(&self) -> Result<(), Error> {
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

        let mut operator_ids = BTreeSet::new();
        for operator in &self.operators {
            if operator.id.is_empty() || !operator_ids.insert(operator.id.clone()) {
                return Err(Error::Config(format!(
                    "Job '{}' contains a duplicate or empty operator id",
                    self.id
                )));
            }
            if operator.stateful && operator.key_field.is_none() {
                return Err(Error::Config(format!(
                    "stateful operator '{}' requires key_field",
                    operator.id
                )));
            }
        }

        for edge in &self.edges {
            if !operator_ids.contains(&edge.from) || !operator_ids.contains(&edge.to) {
                return Err(Error::Config(format!(
                    "edge '{}' references an unknown operator",
                    edge.id
                )));
            }
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

    pub fn assignments_for_nodes(&self, node_ids: &[String], generation: u64) -> Vec<TaskAttempt> {
        if node_ids.is_empty() {
            return Vec::new();
        }
        self.tasks
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

    fn base_job() -> JobSpec {
        JobSpec {
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
                },
            }],
            sinks: vec![SinkSpec {
                operator_id: "sink".into(),
                output_type: "drop".into(),
                config: serde_json::json!({}),
            }],
            state: Some(StateSpec {
                backend: "embedded_kv".into(),
                namespace: Some("orders".into()),
                ttl_ms: None,
                format_version: 1,
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
    fn validates_a_stateful_event_time_job() {
        assert!(base_job().validate().is_ok());
    }

    #[test]
    fn rejects_invalid_job_id() {
        assert!(JobId::new("bad id").is_err());
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
