//! Execution graph construction: JobPlan tasks to chains of operators joined
//! by bounded in-process channels, with operator-chain fusion.

use crate::input::Input;
use crate::job::{JobComponentAdapter, JobPlan, KeyGroupRange, OperatorKind, TaskSpec};
use crate::output::Output;
use crate::processor::Processor;
use crate::Error;
use crate::Resource;
use flume::{Receiver, Sender};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// An outbound edge endpoint from a task inside a chain.
#[derive(Clone)]
pub enum EdgeTarget {
    /// Forward every envelope to this channel (single downstream subtask).
    Forward(Sender<super::envelope::Envelope>),
    /// Hash the routing key and pick one channel by key-group bucket.
    /// `channels` is indexed by downstream subtask.
    Partitioned {
        channels: Vec<Sender<super::envelope::Envelope>>,
        key_field: String,
        /// Key-group ownership for each channel, in the same order as
        /// `channels`.  Routing must use the JobPlan's max-parallelism
        /// assignment rather than modulo the number of physical tasks.
        key_group_ranges: Vec<KeyGroupRange>,
        max_parallelism: u32,
    },
    /// Send to every downstream subtask channel (non-partitioned fan-out).
    Broadcast(Vec<Sender<super::envelope::Envelope>>),
}

/// A chain: one source task, one sink task, or a fused run of pass-through
/// processor tasks that share a single event loop.
pub struct Chain {
    /// Task ids covered by this chain, in execution order.
    pub task_ids: Vec<String>,
    /// Source input when this chain starts at a source task.
    pub source: Option<Arc<dyn Input>>,
    /// Fused processors for intermediate tasks (aligned with the non-source,
    /// non-sink portion of `task_ids`).
    pub processors: Vec<Arc<dyn Processor>>,
    /// Sink output when this chain ends at a sink task.
    pub sink: Option<Arc<dyn Output>>,
    /// Inbound channels (empty for source chains).
    pub inputs: Vec<Receiver<super::envelope::Envelope>>,
    /// Outbound edges keyed by the upstream task id inside this chain.
    pub outputs: BTreeMap<String, Vec<EdgeTarget>>,
    /// Error-only outbound edges. These are used when a processor fails; they
    /// must not receive successful data like ordinary broadcast edges.
    pub error_outputs: BTreeMap<String, Vec<EdgeTarget>>,
    /// Outbound edges used only for source-side late-event Route actions.
    /// They are separate from normal data edges so a late row cannot be
    /// delivered to the main window and the late sink at the same time.
    pub late_event_outputs: BTreeMap<String, Vec<EdgeTarget>>,
    /// Event-time configuration for a source chain. The kernel uses this to
    /// construct a gate automatically for local and Agent execution alike.
    pub source_time: Option<crate::job::TimeSpec>,
    /// Physical source partition represented by this chain.
    pub source_partition: Option<u32>,
    /// Bounded processor worker concurrency for the source chain (the legacy
    /// `pipeline.thread_num`). 1 = the ordinary single-threaded chain loop;
    /// values above 1 run a bounded, ordered, cancellable worker pool while
    /// the source stays a single task (partition topology unchanged).
    pub processor_parallelism: usize,
    /// Downstream window timing definitions used by the source gate.
    pub window_timings: Vec<super::event_time_gate::WindowTiming>,
    /// Stable group identity for event-time gates that feed the same normal
    /// watermark windows. Sources in one group share only the tracker; their
    /// held deliveries remain on their own gate.
    pub watermark_group: Option<String>,
}

impl Chain {
    pub fn is_source(&self) -> bool {
        self.source.is_some()
    }

    /// A copy sharing the processing topology (processors, sink, outputs)
    /// for bounded worker pools. Source/input ownership stays with the
    /// original chain's event loop.
    pub fn share_for_workers(&self) -> Chain {
        Chain {
            task_ids: self.task_ids.clone(),
            source: None,
            processors: self.processors.clone(),
            sink: self.sink.clone(),
            inputs: Vec::new(),
            outputs: self.outputs.clone(),
            error_outputs: self.error_outputs.clone(),
            late_event_outputs: self.late_event_outputs.clone(),
            source_time: None,
            source_partition: None,
            processor_parallelism: 1,
            window_timings: Vec::new(),
            watermark_group: None,
        }
    }

    pub fn entry_task_id(&self) -> &str {
        self.task_ids.first().map(String::as_str).unwrap_or("")
    }
}

/// A materialized execution graph: chains plus how they connect.
pub struct ExecutionGraph {
    pub chains: Vec<Chain>,
    /// Capacity used for every inter-chain channel.
    pub channel_capacity: usize,
    /// Temporary stores shared by this graph's processors. The resource
    /// guard connects them (dependency order) before any chain spawns and
    /// closes them at shutdown.
    pub temporaries: Vec<Arc<dyn crate::temporary::Temporary>>,
}

/// Fuse a JobPlan's assigned tasks into chains connected by bounded channels.
///
/// Fusion rule (conservative first cut): consecutive processor tasks fuse when
/// both are non-source/non-sink/non-stateful, the upstream operator has exactly
/// this one downstream, the downstream has exactly this one upstream, and the
/// edge routes each subtask to a single downstream subtask (`partitioned`
/// edges forward by subtask, or single-parallelism plans). Source tasks never
/// fuse with processors, sink tasks never fuse with processors, and stateful
/// tasks always break a chain so their state snapshots stay independently
/// checkpointable.
///
/// When `state` is provided, stateful operators are wrapped with
/// [`super::stateful::StatefulOperator`] (keyed counter injection into the
/// task's state namespace) so barrier snapshots capture their state.
pub struct ExecutionGraphBuilder {
    channel_capacity: usize,
    state_backend: Option<Arc<dyn crate::state::StateBackend>>,
}

impl Default for ExecutionGraphBuilder {
    fn default() -> Self {
        Self::new(DEFAULT_CHANNEL_CAPACITY)
    }
}

struct PlanIndex<'a> {
    plan: &'a JobPlan,
    source_operators: BTreeSet<&'a str>,
    sink_operators: BTreeSet<&'a str>,
    /// operator id -> stateful flag
    operator_stateful: BTreeMap<&'a str, bool>,
    /// operator id -> upstream operator ids
    upstream: BTreeMap<&'a str, BTreeSet<&'a str>>,
    /// operator id -> downstream operator ids
    downstream: BTreeMap<&'a str, BTreeSet<&'a str>>,
    /// (from, to) -> partitioned flag
    edge_partitioned: BTreeMap<(&'a str, &'a str), bool>,
}

impl<'a> PlanIndex<'a> {
    fn new(plan: &'a JobPlan) -> Self {
        let source_operators = plan
            .spec
            .sources
            .iter()
            .map(|source| source.operator_id.as_str())
            .collect();
        let sink_operators = plan
            .spec
            .sinks
            .iter()
            .map(|sink| sink.operator_id.as_str())
            .collect();
        let operator_stateful = plan
            .spec
            .operators
            .iter()
            .map(|operator| {
                (
                    operator.id.as_str(),
                    operator.stateful || operator.kind == OperatorKind::Window,
                )
            })
            .collect();
        let mut upstream: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        let mut downstream: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        let mut edge_partitioned = BTreeMap::new();
        for operator in &plan.spec.operators {
            upstream.entry(operator.id.as_str()).or_default();
            downstream.entry(operator.id.as_str()).or_default();
        }
        for edge in &plan.spec.edges {
            upstream
                .entry(edge.to.as_str())
                .or_default()
                .insert(edge.from.as_str());
            downstream
                .entry(edge.from.as_str())
                .or_default()
                .insert(edge.to.as_str());
            edge_partitioned.insert((edge.from.as_str(), edge.to.as_str()), edge.partitioned);
        }
        Self {
            plan,
            source_operators,
            sink_operators,
            operator_stateful,
            upstream,
            downstream,
            edge_partitioned,
        }
    }

    fn is_source(&self, operator_id: &str) -> bool {
        self.source_operators.contains(operator_id)
    }

    fn is_sink(&self, operator_id: &str) -> bool {
        self.sink_operators.contains(operator_id)
    }

    fn is_error_sink(&self, operator_id: &str) -> bool {
        self.plan
            .spec
            .operators
            .iter()
            .find(|operator| operator.id == operator_id)
            .and_then(|operator| operator.config.get("__arkflow_error_sink"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    }

    fn task(&self, task_id: &str) -> Option<&TaskSpec> {
        self.plan.task(task_id)
    }

    /// Whether the edge has exactly one downstream task in this plan. A
    /// partitioned edge still needs every downstream subtask as a hash bucket;
    /// fusing it while several buckets exist would bypass that routing.
    fn edge_routes_single_subtask(&self, from: &str, to: &str) -> bool {
        if !self.edge_partitioned.contains_key(&(from, to)) {
            return false;
        }
        self.plan
            .tasks
            .iter()
            .filter(|task| task.operator_id == to)
            .count()
            <= 1
    }

    /// Whether the operator pair (from, to) may fuse into one chain.
    fn fusable_pair(&self, from: &str, to: &str) -> bool {
        if self.is_source(from) || self.is_sink(from) || self.is_source(to) || self.is_sink(to) {
            return false;
        }
        if self.operator_stateful.get(from).copied().unwrap_or(false)
            || self.operator_stateful.get(to).copied().unwrap_or(false)
        {
            return false;
        }
        // Fusing requires the pair to be each other's sole downstream/upstream,
        // otherwise the fused loop would drop or duplicate edges.
        let single_downstream = self
            .downstream
            .get(from)
            .is_some_and(|set| set.len() == 1 && set.contains(to));
        let single_upstream = self
            .upstream
            .get(to)
            .is_some_and(|set| set.len() == 1 && set.contains(from));
        single_downstream && single_upstream && self.edge_routes_single_subtask(from, to)
    }
}

impl ExecutionGraphBuilder {
    pub fn new(channel_capacity: usize) -> Self {
        Self {
            channel_capacity: channel_capacity.max(1),
            state_backend: None,
        }
    }

    /// Attach the Job's state backend; stateful operators get the
    /// `StatefulOperator` wrapper during graph build.
    pub fn with_state(mut self, state_backend: Arc<dyn crate::state::StateBackend>) -> Self {
        self.state_backend = Some(state_backend);
        self
    }

    pub fn with_capacity(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = channel_capacity.max(1);
        self
    }

    /// Build the full graph for a plan (local mode).
    pub fn build<A: JobComponentAdapter>(
        &self,
        plan: &JobPlan,
        adapter: &A,
        resource: &Resource,
    ) -> Result<ExecutionGraph, Error> {
        let task_ids = plan
            .tasks
            .iter()
            .map(|task| task.id.clone())
            .collect::<Vec<_>>();
        self.build_subgraph(plan, &task_ids, adapter, resource)
    }

    /// Build the subgraph for the assigned task subset (Agent mode). The same
    /// code path as `build`; the assignment must not split an edge between
    /// nodes (the Hub placement already guarantees co-location).
    pub fn build_subgraph<A: JobComponentAdapter>(
        &self,
        plan: &JobPlan,
        task_ids: &[String],
        adapter: &A,
        resource: &Resource,
    ) -> Result<ExecutionGraph, Error> {
        plan.spec.validate()?;
        validate_shared_watermark_specs(plan)?;
        let index = PlanIndex::new(plan);
        let tasks = task_ids
            .iter()
            .map(|task_id| {
                index
                    .task(task_id)
                    .cloned()
                    .ok_or_else(|| Error::Config(format!("unknown Job task '{task_id}'")))
            })
            .collect::<Result<Vec<TaskSpec>, Error>>()?;
        if tasks.is_empty() {
            return Err(Error::Config("Job assignment contains no tasks".into()));
        }
        // Window operators are stateful even when a local StreamConfig has no
        // explicit Job state section. Give that local graph a real backend;
        // explicit Job state backends still take precedence and all other
        // stateful operators retain the strict configuration requirement.
        let state_backend = match self.state_backend.clone() {
            Some(backend) => Some(backend),
            None if tasks.iter().any(|task| {
                plan.spec
                    .operators
                    .iter()
                    .find(|operator| operator.id == task.operator_id)
                    .is_some_and(|operator| operator.kind == crate::job::OperatorKind::Window)
            }) =>
            {
                Some(Arc::new(crate::state::InMemoryStateBackend::new(1)?)
                    as Arc<dyn crate::state::StateBackend>)
            }
            None => None,
        };
        // All stateful operators in one materialized graph share the same
        // journal.  A window and a keyed processor can be in different
        // chains, yet still mutate the same backend during one delivery
        // epoch; separate journals would give them separate finalize locks
        // and version maps, allowing an older rollback to overwrite a later
        // commit.  Namespaces keep their state isolated while the journal
        // serializes the apply/ack/undo boundary across the graph.
        let shared_journal = state_backend
            .clone()
            .map(|backend| Arc::new(super::state_journal::StateJournal::new(backend)));
        let _assigned: BTreeSet<&str> = tasks.iter().map(|task| task.id.as_str()).collect();

        // 1. Group tasks into fusable runs. Tasks are visited in plan order
        // (operator-major, subtask-minor); a run continues while the previous
        // task's operator may fuse with the next task's operator at the same
        // subtask.
        let mut runs: Vec<Vec<TaskSpec>> = Vec::new();
        for task in &tasks {
            let fusable_into_previous =
                runs.last()
                    .and_then(|run| run.last())
                    .is_some_and(|previous| {
                        previous.subtask == task.subtask
                            && index.fusable_pair(&previous.operator_id, &task.operator_id)
                    });
            if fusable_into_previous {
                runs.last_mut().unwrap().push(task.clone());
            } else {
                runs.push(vec![task.clone()]);
            }
        }

        // 2. Allocate one channel per (upstream task, downstream run entry).
        // For each plan edge and each assigned upstream task, resolve its
        // downstream targets inside the assignment and connect the upstream
        // task's run to each target run's entry task.
        let mut run_of_task: BTreeMap<&str, usize> = BTreeMap::new();
        for (run_index, run) in runs.iter().enumerate() {
            for task in run {
                run_of_task.insert(task.id.as_str(), run_index);
            }
        }
        // (upstream task id) -> list of (edge kind, downstream entry task ids)
        let mut outbound: BTreeMap<String, Vec<OutboundEdge>> = BTreeMap::new();
        for task in &tasks {
            let downstream_operators = index
                .downstream
                .get(task.operator_id.as_str())
                .cloned()
                .unwrap_or_default();
            for downstream_operator in downstream_operators {
                let Some(partitioned) = index
                    .edge_partitioned
                    .get(&(task.operator_id.as_str(), downstream_operator))
                    .copied()
                else {
                    continue;
                };
                // Downstream tasks of this operator inside the assignment,
                // with the edge's routing rule (mirrors the legacy runner).
                let target_tasks: Vec<&TaskSpec> = tasks
                    .iter()
                    .filter(|candidate| candidate.operator_id == downstream_operator)
                    .collect();
                if target_tasks.is_empty() {
                    return Err(Error::Config(format!(
                        "Job assignment splits edge '{}->{}'; connected tasks must be co-located",
                        task.operator_id, downstream_operator
                    )));
                }
                // Map target tasks to the runs that will consume them. Targets
                // inside the upstream task's own run are fused internal edges
                // — the chain already carries them with zero channel hops.
                let upstream_run = *run_of_task
                    .get(task.id.as_str())
                    .ok_or_else(|| Error::Config(format!("task '{}' lost its run", task.id)))?;
                let mut target_runs: Vec<usize> = Vec::new();
                for target in &target_tasks {
                    let target_run = *run_of_task.get(target.id.as_str()).ok_or_else(|| {
                        Error::Config(format!("task '{}' lost its run", target.id))
                    })?;
                    if target_run != upstream_run && !target_runs.contains(&target_run) {
                        target_runs.push(target_run);
                    }
                }
                if target_runs.is_empty() {
                    continue;
                }
                let targets = target_runs
                    .iter()
                    .map(|run_index| runs[*run_index][0].id.clone())
                    .collect::<Vec<_>>();
                let key_group_ranges = if partitioned {
                    target_runs
                        .iter()
                        .map(|run_index| {
                            runs[*run_index]
                                .first()
                                .and_then(|task| task.partitions.first())
                                .map(|partition| partition.key_group.clone())
                                .ok_or_else(|| {
                                    Error::Config(format!(
                                        "target run for '{}->{}' has no key-group partition",
                                        task.operator_id, downstream_operator
                                    ))
                                })
                        })
                        .collect::<Result<Vec<_>, Error>>()?
                } else {
                    Vec::new()
                };
                let kind = if partitioned {
                    OutboundKind::Route
                } else if targets.len() == 1 {
                    OutboundKind::Forward
                } else {
                    OutboundKind::Broadcast
                };
                outbound
                    .entry(task.id.clone())
                    .or_default()
                    .push(OutboundEdge {
                        kind,
                        error: index.is_error_sink(&downstream_operator),
                        late_route: false,
                        targets,
                        key_group_ranges,
                        max_parallelism: plan.spec.max_parallelism,
                        key_field: index
                            .plan
                            .spec
                            .operators
                            .iter()
                            .find(|operator| operator.id == downstream_operator)
                            .and_then(|operator| operator.key_field.clone())
                            .unwrap_or_default(),
                    });
            }

            // A late-event route is a logical side edge of the source, not a
            // normal data edge. Materialize it even when the JobSpec does not
            // repeat the route as a regular source->operator edge; the route
            // target still participates in the same local graph and receives
            // barriers/EOS through the control path.
            if index.is_source(&task.operator_id) {
                if let Some(route_operator) = plan
                    .spec
                    .sources
                    .iter()
                    .find(|source| source.operator_id == task.operator_id)
                    .and_then(|source| source.time.late_event_route.as_deref())
                {
                    let target_tasks = tasks
                        .iter()
                        .filter(|candidate| candidate.operator_id == route_operator)
                        .collect::<Vec<_>>();
                    if target_tasks.is_empty() {
                        return Err(Error::Config(format!(
                            "late-event route target '{}' for source '{}' is not in this Job assignment",
                            route_operator, task.operator_id
                        )));
                    }
                    let upstream_run = *run_of_task
                        .get(task.id.as_str())
                        .ok_or_else(|| Error::Config(format!("task '{}' lost its run", task.id)))?;
                    let mut target_runs = Vec::new();
                    for target in target_tasks {
                        let target_run = *run_of_task.get(target.id.as_str()).ok_or_else(|| {
                            Error::Config(format!("task '{}' lost its run", target.id))
                        })?;
                        if target_run != upstream_run && !target_runs.contains(&target_run) {
                            target_runs.push(target_run);
                        }
                    }
                    if !target_runs.is_empty() {
                        let targets = target_runs
                            .iter()
                            .map(|run_index| runs[*run_index][0].id.clone())
                            .collect::<Vec<_>>();
                        outbound
                            .entry(task.id.clone())
                            .or_default()
                            .push(OutboundEdge {
                                kind: if targets.len() == 1 {
                                    OutboundKind::Forward
                                } else {
                                    OutboundKind::Broadcast
                                },
                                error: false,
                                late_route: true,
                                targets,
                                key_group_ranges: Vec::new(),
                                max_parallelism: plan.spec.max_parallelism,
                                key_field: index
                                    .plan
                                    .spec
                                    .operators
                                    .iter()
                                    .find(|operator| operator.id == route_operator)
                                    .and_then(|operator| operator.key_field.clone())
                                    .unwrap_or_default(),
                            });
                    }
                }
            }

            // Event-time Session windows own their dynamic per-key deadline.
            // Their late rows therefore bypass the source gate and need a
            // side edge from the window task itself.  The source-side route
            // edge above remains necessary for tumbling/sliding windows;
            // this additional edge is only materialized for unified,
            // watermark-triggered Session operators.
            let dynamic_session_window = plan
                .spec
                .operators
                .iter()
                .find(|operator| operator.id == task.operator_id)
                .and_then(|operator| {
                    serde_json::from_value::<super::window::WindowOperatorConfig>(
                        operator.config.clone(),
                    )
                    .ok()
                })
                .is_some_and(|config| {
                    config.trigger == super::window::WindowTrigger::Watermark
                        && !config.legacy_payload
                        && matches!(config.kind, super::window::WindowKind::Session { .. })
                });
            if dynamic_session_window {
                let route_operators = plan
                    .spec
                    .sources
                    .iter()
                    .filter(|source| source.time.mode == crate::job::TimeMode::EventTime)
                    .filter(|source| {
                        source.time.late_event_route.is_some()
                            && operator_reachable(plan, &source.operator_id, &task.operator_id)
                    })
                    .filter_map(|source| source.time.late_event_route.clone())
                    .collect::<BTreeSet<_>>();
                for route_operator in route_operators {
                    let target_tasks = tasks
                        .iter()
                        .filter(|candidate| candidate.operator_id == route_operator)
                        .collect::<Vec<_>>();
                    if target_tasks.is_empty() {
                        return Err(Error::Config(format!(
                            "late-event route target '{}' for window '{}' is not in this Job assignment",
                            route_operator, task.operator_id
                        )));
                    }
                    let upstream_run = *run_of_task
                        .get(task.id.as_str())
                        .ok_or_else(|| Error::Config(format!("task '{}' lost its run", task.id)))?;
                    let mut target_runs = Vec::new();
                    for target in target_tasks {
                        let target_run = *run_of_task.get(target.id.as_str()).ok_or_else(|| {
                            Error::Config(format!("task '{}' lost its run", target.id))
                        })?;
                        if target_run != upstream_run && !target_runs.contains(&target_run) {
                            target_runs.push(target_run);
                        }
                    }
                    if !target_runs.is_empty() {
                        let targets = target_runs
                            .iter()
                            .map(|run_index| runs[*run_index][0].id.clone())
                            .collect::<Vec<_>>();
                        outbound
                            .entry(task.id.clone())
                            .or_default()
                            .push(OutboundEdge {
                                kind: if targets.len() == 1 {
                                    OutboundKind::Forward
                                } else {
                                    OutboundKind::Broadcast
                                },
                                error: false,
                                late_route: true,
                                targets,
                                key_group_ranges: Vec::new(),
                                max_parallelism: plan.spec.max_parallelism,
                                key_field: index
                                    .plan
                                    .spec
                                    .operators
                                    .iter()
                                    .find(|operator| operator.id == route_operator)
                                    .and_then(|operator| operator.key_field.clone())
                                    .unwrap_or_default(),
                            });
                    }
                }
            }
        }

        // 3. Materialize channels per (upstream task, target run entry).
        let mut senders: BTreeMap<(String, String), Sender<super::envelope::Envelope>> =
            BTreeMap::new();
        // A run may have several upstream edges. Keep every receiver instead
        // of indexing only by target task id; overwriting here silently turns
        // a multi-input vertex into a single-input vertex.
        let mut receivers: BTreeMap<String, Vec<Receiver<super::envelope::Envelope>>> =
            BTreeMap::new();
        for (upstream_task_id, edges) in &outbound {
            for edge in edges {
                for target in &edge.targets {
                    if !senders.contains_key(&(upstream_task_id.clone(), target.clone())) {
                        let (sender, receiver) = flume::bounded(self.channel_capacity);
                        senders.insert((upstream_task_id.clone(), target.clone()), sender);
                        receivers.entry(target.clone()).or_default().push(receiver);
                    }
                }
            }
        }

        // 4. Assemble chains. The legacy `pipeline.thread_num` rides the
        // source config; it applies to the fused PROCESSOR chains (bounded,
        // ordered worker concurrency) and never changes the source partition
        // topology.
        let processor_parallelism = plan
            .spec
            .sources
            .iter()
            .find_map(|source| {
                source
                    .config
                    .get("__arkflow_processor_parallelism")
                    .and_then(serde_json::Value::as_u64)
            })
            .or_else(|| {
                plan.spec.operators.iter().find_map(|operator| {
                    operator
                        .config
                        .get("__arkflow_processor_parallelism")
                        .and_then(serde_json::Value::as_u64)
                })
            })
            .map(|value| (value as usize).max(1));
        let mut chains = Vec::with_capacity(runs.len());
        for run in &runs {
            let first = run.first().unwrap();
            let last = run.last().unwrap();
            let is_source = index.is_source(&first.operator_id);
            let is_sink = index.is_sink(&last.operator_id);
            let has_stateful_processor = run.iter().any(|task| {
                plan.spec
                    .operators
                    .iter()
                    .find(|operator| operator.id == task.operator_id)
                    .is_some_and(|operator| {
                        operator.stateful || operator.kind == crate::job::OperatorKind::Window
                    })
            });

            let source = if is_source {
                Some(build_source_input(plan, &index, first, adapter, resource)?)
            } else {
                None
            };
            let sink = if is_sink {
                let sink_spec = plan
                    .spec
                    .sinks
                    .iter()
                    .find(|sink| sink.operator_id == last.operator_id)
                    .unwrap();
                Some(adapter.build_output(sink_spec, resource)?)
            } else {
                None
            };

            let mut processors = Vec::with_capacity(run.len());
            for task in run {
                if index.is_source(&task.operator_id) || index.is_sink(&task.operator_id) {
                    continue;
                }
                let operator = plan
                    .spec
                    .operators
                    .iter()
                    .find(|operator| operator.id == task.operator_id)
                    .ok_or_else(|| {
                        Error::Config(format!("task '{}' references unknown operator", task.id))
                    })?;
                let processor: Arc<dyn Processor> =
                    if operator.kind == crate::job::OperatorKind::Window {
                        let backend = state_backend.clone().ok_or_else(|| {
                            Error::Config(format!(
                                "stateful operator '{}' requires a Job state backend",
                                operator.id
                            ))
                        })?;
                        let config: super::window::WindowOperatorConfig =
                            serde_json::from_value(operator.config.clone()).map_err(|error| {
                                Error::Config(format!(
                                    "window operator '{}' has invalid config: {error}",
                                    operator.id
                                ))
                            })?;
                        config.validate()?;
                        let namespace = format!("job:{}:task:{}", plan.spec.id, task.id);
                        let event_time_source = event_time_source_for_operator(plan, &operator.id);
                        let late_event_policy = event_time_source
                            .map(|source| source.time.late_event_policy)
                            .unwrap_or_default();
                        let late_event_route_configured = event_time_source
                            .and_then(|source| source.time.late_event_route.as_ref())
                            .is_some();
                        Arc::new(
                        super::window::ColumnarWindowOperator::with_journal_and_late_event_policy(
                            config,
                            backend,
                            shared_journal
                                .clone()
                                .expect("window backend checked above"),
                            namespace,
                            late_event_policy,
                            late_event_route_configured,
                        ),
                    )
                    } else {
                        let processor = adapter.build_processor(operator, resource)?;
                        if !operator.stateful {
                            processor
                        } else {
                            state_backend.as_ref().ok_or_else(|| {
                                Error::Config(format!(
                                    "stateful operator '{}' requires a Job state backend",
                                    operator.id
                                ))
                            })?;
                            Arc::new(super::stateful::StatefulOperator::with_journal(
                                processor,
                                shared_journal.clone().expect("state backend checked above"),
                                format!("job:{}:task:{}", plan.spec.id, task.id),
                                operator.key_field.clone().ok_or_else(|| {
                                    Error::Config(format!(
                                        "stateful operator '{}' requires key_field",
                                        operator.id
                                    ))
                                })?,
                                plan.spec.state.as_ref().and_then(|state| state.ttl_ms),
                                operator
                                    .config
                                    .get("state_output_field")
                                    .and_then(serde_json::Value::as_str)
                                    .unwrap_or("__arkflow_state_count")
                                    .to_owned(),
                            ))
                        }
                    };
                processors.push(processor);
            }

            let inputs = run
                .first()
                .and_then(|task| receivers.remove(&task.id))
                .unwrap_or_default();

            let mut outputs = BTreeMap::new();
            let mut error_outputs = BTreeMap::new();
            let mut late_event_outputs = BTreeMap::new();
            for task in run {
                if let Some(edges) = outbound.remove(&task.id) {
                    let mut targets = Vec::with_capacity(edges.len());
                    for edge in edges {
                        let channels = edge
                            .targets
                            .iter()
                            .filter_map(|target| {
                                senders.get(&(task.id.clone(), target.clone())).cloned()
                            })
                            .collect::<Vec<_>>();
                        let target = match edge.kind {
                            OutboundKind::Forward => {
                                EdgeTarget::Forward(channels.into_iter().next().ok_or_else(
                                    || Error::Process("forward edge lost its channel".into()),
                                )?)
                            }
                            OutboundKind::Broadcast => EdgeTarget::Broadcast(channels),
                            OutboundKind::Route => EdgeTarget::Partitioned {
                                channels,
                                key_field: edge.key_field,
                                key_group_ranges: edge.key_group_ranges,
                                max_parallelism: edge.max_parallelism,
                            },
                        };
                        if edge.late_route {
                            late_event_outputs
                                .entry(task.id.clone())
                                .or_insert_with(Vec::new)
                                .push(target);
                        } else if edge.error {
                            error_outputs
                                .entry(task.id.clone())
                                .or_insert_with(Vec::new)
                                .push(target);
                        } else {
                            targets.push(target);
                        }
                    }
                    if !targets.is_empty() {
                        outputs.insert(task.id.clone(), targets);
                    }
                }
            }

            chains.push(Chain {
                task_ids: run.iter().map(|task| task.id.clone()).collect(),
                source,
                processors,
                sink,
                inputs,
                outputs,
                error_outputs,
                late_event_outputs,
                source_time: if is_source {
                    plan.spec
                        .sources
                        .iter()
                        .find(|source| source.operator_id == first.operator_id)
                        .map(|source| source.time.clone())
                } else {
                    None
                },
                source_partition: if is_source {
                    // A single source task deliberately keeps the connector's
                    // subscription to all physical partitions. Its plan
                    // partition 0 is a task identity, not a physical Kafka
                    // assignment, so do not use it as a watermark fallback.
                    let source_task_count = plan
                        .tasks
                        .iter()
                        .filter(|candidate| candidate.operator_id == first.operator_id)
                        .count();
                    first
                        .partitions
                        .first()
                        .filter(|partition| source_task_count > 1 || partition.id != 0)
                        .map(|partition| partition.id)
                } else {
                    None
                },
                // Stateful and window operators own a mutable state epoch.
                // Keep their chain single-threaded so a worker pool cannot
                // interleave state updates or cross a checkpoint cut.
                processor_parallelism: if has_stateful_processor {
                    1
                } else {
                    processor_parallelism.unwrap_or(1)
                },
                window_timings: if is_source {
                    window_timings_for_source(plan, &first.operator_id)?
                } else {
                    Vec::new()
                },
                watermark_group: if is_source {
                    Some(watermark_group_for_source(plan, &first.operator_id))
                } else {
                    None
                },
            });
        }

        if !outbound.is_empty() {
            return Err(Error::Process(
                "execution graph left dangling outbound edges".into(),
            ));
        }

        Ok(ExecutionGraph {
            chains,
            channel_capacity: self.channel_capacity,
            temporaries: Vec::new(),
        })
    }
}

enum OutboundKind {
    Forward,
    Broadcast,
    Route,
}

struct OutboundEdge {
    kind: OutboundKind,
    error: bool,
    late_route: bool,
    targets: Vec<String>,
    key_field: String,
    key_group_ranges: Vec<KeyGroupRange>,
    max_parallelism: u32,
}

fn build_source_input<A: JobComponentAdapter>(
    plan: &JobPlan,
    _index: &PlanIndex<'_>,
    task: &TaskSpec,
    adapter: &A,
    resource: &Resource,
) -> Result<Arc<dyn Input>, Error> {
    let source_spec = plan
        .spec
        .sources
        .iter()
        .find(|source| source.operator_id == task.operator_id)
        .unwrap();
    let input = adapter.build_input(source_spec, resource)?;
    let parallel_tasks = plan
        .tasks
        .iter()
        .filter(|candidate| candidate.operator_id == task.operator_id)
        .count();
    // Partition assignment derives from the operator's ACTUAL task count: a
    // single source task keeps the connector's all-partition subscription
    // (pinning it to physical partition 0 would silently drop every other
    // partition); only multiple physical tasks receive explicit, stable
    // partitions.
    if parallel_tasks <= 1 {
        return Ok(input);
    }
    if !input.supports_partitioning() {
        return Err(Error::Config(format!(
            "source '{}' does not support partitioned task execution",
            task.operator_id
        )));
    }
    let partition = task
        .partitions
        .first()
        .map(|partition| partition.id)
        .ok_or_else(|| Error::Config(format!("task '{}' has no source partition", task.id)))?;
    input.assign_partition(partition)?;
    Ok(input)
}

/// Collect the timing shape of every normal-path window reachable from a
/// source. The source gate uses the latest containing-window end, which is
/// essential for sliding windows where one event belongs to several windows.
fn window_timings_for_source(
    plan: &JobPlan,
    source_operator_id: &str,
) -> Result<Vec<super::event_time_gate::WindowTiming>, Error> {
    let mut queue = plan
        .spec
        .edges
        .iter()
        .filter(|edge| edge.from == source_operator_id)
        .map(|edge| edge.to.clone())
        .collect::<Vec<_>>();
    let mut visited = BTreeSet::new();
    let mut timings = Vec::new();
    while let Some(operator_id) = queue.pop() {
        if !visited.insert(operator_id.clone()) {
            continue;
        }
        if let Some(operator) = plan
            .spec
            .operators
            .iter()
            .find(|operator| operator.id == operator_id)
        {
            if operator.kind == crate::job::OperatorKind::Window {
                let config: super::window::WindowOperatorConfig =
                    serde_json::from_value(operator.config.clone()).map_err(|error| {
                        Error::Config(format!(
                            "window operator '{}' has invalid config: {error}",
                            operator.id
                        ))
                    })?;
                config.validate()?;
                // Only watermark-triggered event-time windows belong in the
                // source gate. Processing-time windows must receive future
                // timestamps immediately so their interval timer can fire;
                // session windows have dynamic, per-key boundaries owned by
                // the window operator and cannot use a static event+gap end.
                if config.trigger == super::window::WindowTrigger::Watermark {
                    timings.push(match config.kind {
                        super::window::WindowKind::Tumbling { size_ms } => {
                            super::event_time_gate::WindowTiming::Tumbling { size_ms }
                        }
                        super::window::WindowKind::Sliding { size_ms, slide_ms } => {
                            super::event_time_gate::WindowTiming::Sliding { size_ms, slide_ms }
                        }
                        super::window::WindowKind::Session { gap_ms } => {
                            super::event_time_gate::WindowTiming::Session { gap_ms }
                        }
                    });
                }
            }
            queue.extend(
                plan.spec
                    .edges
                    .iter()
                    .filter(|edge| edge.from == operator_id)
                    .map(|edge| edge.to.clone()),
            );
        }
    }
    Ok(timings)
}

fn operator_reachable(plan: &JobPlan, from: &str, target: &str) -> bool {
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
            plan.spec
                .edges
                .iter()
                .filter(|edge| edge.from == operator_id)
                .map(|edge| edge.to.clone()),
        );
    }
    false
}

/// Find an event-time source whose normal path reaches an operator.  Shared
/// watermark validation guarantees that sources feeding the same window have
/// a compatible time contract; the first source is therefore sufficient for
/// the window's dynamic late-event policy and route configuration.
fn event_time_source_for_operator<'a>(
    plan: &'a JobPlan,
    operator_id: &str,
) -> Option<&'a crate::job::SourceSpec> {
    plan.spec.sources.iter().find(|source| {
        source.time.mode == crate::job::TimeMode::EventTime
            && operator_reachable(plan, &source.operator_id, operator_id)
    })
}

fn reachable_watermark_windows(plan: &JobPlan, source_operator_id: &str) -> BTreeSet<String> {
    let mut queue = plan
        .spec
        .edges
        .iter()
        .filter(|edge| edge.from == source_operator_id)
        .map(|edge| edge.to.clone())
        .collect::<Vec<_>>();
    let mut visited = BTreeSet::new();
    let mut windows = BTreeSet::new();
    while let Some(operator_id) = queue.pop() {
        if !visited.insert(operator_id.clone()) {
            continue;
        }
        if let Some(operator) = plan
            .spec
            .operators
            .iter()
            .find(|operator| operator.id == operator_id)
        {
            if operator.kind == crate::job::OperatorKind::Window {
                let is_watermark_window = serde_json::from_value::<
                    super::window::WindowOperatorConfig,
                >(operator.config.clone())
                .map(|config| config.trigger == super::window::WindowTrigger::Watermark)
                .unwrap_or(false);
                if is_watermark_window {
                    windows.insert(operator.id.clone());
                }
            }
            queue.extend(
                plan.spec
                    .edges
                    .iter()
                    .filter(|edge| edge.from == operator_id)
                    .map(|edge| edge.to.clone()),
            );
        }
    }
    windows
}

/// Identify the connected watermark-window component reachable from a source.
/// Using the complete connected component matters for overlapping paths: if
/// source A reaches windows X and Y while source B reaches only Y, the two
/// sources must share a tracker. A simple `join(X,Y)` versus `join(Y)` key
/// would incorrectly create two independent minima.
fn watermark_group_for_source(plan: &JobPlan, source_operator_id: &str) -> String {
    let source_windows = plan
        .spec
        .sources
        .iter()
        .filter(|source| source.time.mode == crate::job::TimeMode::EventTime)
        .map(|source| {
            (
                source.operator_id.clone(),
                reachable_watermark_windows(plan, &source.operator_id),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let Some(initial_windows) = source_windows.get(source_operator_id) else {
        return format!("source:{source_operator_id}");
    };
    if initial_windows.is_empty() {
        return format!("source:{source_operator_id}");
    }

    let mut component_sources = BTreeSet::from([source_operator_id.to_owned()]);
    let mut component_windows = initial_windows.clone();
    loop {
        let mut changed = false;
        for (source_id, windows) in &source_windows {
            if component_sources.contains(source_id)
                || !windows
                    .iter()
                    .any(|window| component_windows.contains(window))
            {
                continue;
            }
            component_sources.insert(source_id.clone());
            component_windows.extend(windows.iter().cloned());
            changed = true;
        }
        if !changed {
            break;
        }
    }
    format!(
        "watermark:{}",
        component_sources.into_iter().collect::<Vec<_>>().join("|")
    )
}

/// A shared downstream watermark tracker can only combine sources that use
/// the same event-time contract.  The tracker owns the strategy,
/// out-of-orderness and idle policy, while each gate owns timestamp extraction
/// and late-event handling; silently selecting the first source's tracker
/// configuration for an incompatible sibling would make either source's
/// watermark invalid.  Reject that graph at build time instead of letting the
/// two source chains classify rows against different semantics.
fn validate_shared_watermark_specs(plan: &JobPlan) -> Result<(), Error> {
    let mut groups = BTreeMap::<String, Vec<&crate::job::SourceSpec>>::new();
    for source in &plan.spec.sources {
        if source.time.mode == crate::job::TimeMode::EventTime {
            groups
                .entry(watermark_group_for_source(plan, &source.operator_id))
                .or_default()
                .push(source);
        }
    }
    for (group, sources) in groups {
        let Some(reference) = sources.first() else {
            continue;
        };
        if let Some(incompatible) = sources
            .iter()
            .skip(1)
            .find(|source| source.time != reference.time)
        {
            return Err(Error::Config(format!(
                "event-time sources '{}' and '{}' share watermark group '{}' but have incompatible TimeSpec values",
                reference.operator_id, incompatible.operator_id, group
            )));
        }
    }
    Ok(())
}
