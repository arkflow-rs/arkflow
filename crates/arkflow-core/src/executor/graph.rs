//! Execution graph construction: JobPlan tasks to chains of operators joined
//! by bounded in-process channels, with operator-chain fusion.

use crate::Error;
use crate::input::Input;
use crate::job::{JobComponentAdapter, JobPlan, TaskSpec};
use crate::output::Output;
use crate::processor::Processor;
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
}

impl Chain {
    pub fn is_source(&self) -> bool {
        self.source.is_some()
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
            .map(|operator| (operator.id.as_str(), operator.stateful))
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

    fn task(&self, task_id: &str) -> Option<&TaskSpec> {
        self.plan.task(task_id)
    }

    /// Whether `from`'s task with `subtask` routes to exactly the one task
    /// (`to`, `subtask`) over this edge.
    fn edge_routes_single_subtask(&self, from: &str, to: &str) -> bool {
        let Some(partitioned) = self.edge_partitioned.get(&(from, to)).copied() else {
            return false;
        };
        partitioned || self.plan.spec.parallelism == 1
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
    pub fn with_state(
        mut self,
        state_backend: Arc<dyn crate::state::StateBackend>,
    ) -> Self {
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
        let index = PlanIndex::new(plan);
        let tasks = task_ids
            .iter()
            .map(|task_id| {
                index.task(task_id).cloned().ok_or_else(|| {
                    Error::Config(format!("unknown Job task '{task_id}'"))
                })
            })
            .collect::<Result<Vec<TaskSpec>, Error>>()?;
        if tasks.is_empty() {
            return Err(Error::Config("Job assignment contains no tasks".into()));
        }
        let _assigned: BTreeSet<&str> = tasks.iter().map(|task| task.id.as_str()).collect();

        // 1. Group tasks into fusable runs. Tasks are visited in plan order
        // (operator-major, subtask-minor); a run continues while the previous
        // task's operator may fuse with the next task's operator at the same
        // subtask.
        let mut runs: Vec<Vec<TaskSpec>> = Vec::new();
        for task in &tasks {
            let fusable_into_previous = runs
                .last()
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
                let target_tasks: Vec<&TaskSpec> = if partitioned {
                    runs.iter()
                        .flatten()
                        .find(|candidate| {
                            candidate.operator_id == downstream_operator
                                && candidate.subtask == task.subtask
                        })
                        .into_iter()
                        .collect()
                } else {
                    tasks
                        .iter()
                        .filter(|candidate| candidate.operator_id == downstream_operator)
                        .collect()
                };
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
                    let target_run = *run_of_task
                        .get(target.id.as_str())
                        .ok_or_else(|| Error::Config(format!("task '{}' lost its run", target.id)))?;
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
                        targets,
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
        }

        // 3. Materialize channels per (upstream task, target run entry).
        let mut senders: BTreeMap<(String, String), Sender<super::envelope::Envelope>> =
            BTreeMap::new();
        let mut receivers: BTreeMap<String, Receiver<super::envelope::Envelope>> = BTreeMap::new();
        for (upstream_task_id, edges) in &outbound {
            for edge in edges {
                for target in &edge.targets {
                    if !senders.contains_key(&(upstream_task_id.clone(), target.clone())) {
                        let (sender, receiver) = flume::bounded(self.channel_capacity);
                        senders.insert((upstream_task_id.clone(), target.clone()), sender);
                        receivers.insert(target.clone(), receiver);
                    }
                }
            }
        }

        // 4. Assemble chains.
        let mut chains = Vec::with_capacity(runs.len());
        for run in &runs {
            let first = run.first().unwrap();
            let last = run.last().unwrap();
            let is_source = index.is_source(&first.operator_id);
            let is_sink = index.is_sink(&last.operator_id);

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
                        Error::Config(format!(
                            "task '{}' references unknown operator",
                            task.id
                        ))
                    })?;
                let processor = adapter.build_processor(operator, resource)?;
                let processor: Arc<dyn Processor> = if operator.stateful {
                    let backend = self.state_backend.clone().ok_or_else(|| {
                        Error::Config(format!(
                            "stateful operator '{}' requires a Job state backend",
                            operator.id
                        ))
                    })?;
                    Arc::new(super::stateful::StatefulOperator::new(
                        processor,
                        backend,
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
                } else {
                    processor
                };
                processors.push(processor);
            }

            let inputs = run
                .first()
                .map(|task| receivers.remove(&task.id))
                .flatten()
                .into_iter()
                .collect::<Vec<_>>();

            let mut outputs = BTreeMap::new();
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
                            OutboundKind::Forward => EdgeTarget::Forward(
                                channels.into_iter().next().ok_or_else(|| {
                                    Error::Process("forward edge lost its channel".into())
                                })?,
                            ),
                            OutboundKind::Broadcast => EdgeTarget::Broadcast(channels),
                            OutboundKind::Route => EdgeTarget::Partitioned {
                                channels,
                                key_field: edge.key_field,
                            },
                        };
                        targets.push(target);
                    }
                    outputs.insert(task.id.clone(), targets);
                }
            }

            chains.push(Chain {
                task_ids: run.iter().map(|task| task.id.clone()).collect(),
                source,
                processors,
                sink,
                inputs,
                outputs,
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
    targets: Vec<String>,
    key_field: String,
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
    if parallel_tasks > 1 && !input.supports_partitioning() {
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
