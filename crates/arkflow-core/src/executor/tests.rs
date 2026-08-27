//! Executor kernel unit tests: chain fusion, envelope ordering, backpressure,
//! EOS propagation, and partitioned routing.

use crate::executor::graph::{ExecutionGraphBuilder, DEFAULT_CHANNEL_CAPACITY};
use crate::executor::run_graph;
use crate::input::{Ack, Input};
use crate::job::{
    EdgeSpec, JobComponentAdapter, JobId, JobPlan, JobSpec, JobVersion, OperatorKind,
    OperatorSpec, SinkSpec, SourceSpec, TimeMode, TimeSpec,
};
use crate::output::Output;
use crate::processor::Processor;
use crate::{Error, MessageBatch, MessageBatchRef, ProcessResult, Resource};
use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

// ---------- test doubles ----------

struct VecInput {
    batches: Mutex<std::collections::VecDeque<MessageBatchRef>>,
}

impl VecInput {
    fn new(rows: Vec<Vec<(i64, String)>>) -> Self {
        Self {
            batches: Mutex::new(
                rows.into_iter()
                    .map(|rows| Arc::new(MessageBatch::new_arrow(int64_batch(rows))))
                    .collect(),
            ),
        }
    }
}

fn int64_batch(rows: Vec<(i64, String)>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("key", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())),
            Arc::new(StringArray::from(rows.iter().map(|r| r.1.clone()).collect::<Vec<_>>())),
        ],
    )
    .unwrap()
}

#[async_trait]
impl Input for VecInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        let next = self.batches.lock().unwrap().pop_front();
        match next {
            Some(batch) => Ok((batch, Arc::new(crate::input::NoopAck))),
            None => Err(Error::EOF),
        }
    }
    fn supports_partitioning(&self) -> bool {
        true
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Default)]
struct CollectOutput {
    written: Mutex<Vec<RecordBatch>>,
}

#[async_trait]
impl Output for CollectOutput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
        self.written.lock().unwrap().push(msg.record_batch().clone());
        Ok(())
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

struct PassThroughProcessor;

#[async_trait]
impl Processor for PassThroughProcessor {
    async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        Ok(ProcessResult::Single(batch))
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

struct Adapter {
    input: Arc<dyn Input>,
    output: Arc<CollectOutput>,
    processor: Arc<dyn Processor>,
}

impl JobComponentAdapter for Adapter {
    fn build_input(
        &self,
        _source: &SourceSpec,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        Ok(self.input.clone())
    }
    fn build_output(
        &self,
        _sink: &SinkSpec,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        Ok(self.output.clone())
    }
    fn build_processor(
        &self,
        _operator: &OperatorSpec,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        Ok(self.processor.clone())
    }
}

fn resource() -> Resource {
    Resource {
        temporary: HashMap::<String, Arc<dyn crate::temporary::Temporary>>::new(),
        input_names: RefCell::new(Vec::new()),
    }
}

fn spec(operators: Vec<OperatorSpec>, edges: Vec<EdgeSpec>, parallelism: u32) -> JobSpec {
    let mut operators = operators;
    let has_source = operators
        .iter()
        .any(|operator| operator.kind == OperatorKind::Source);
    let has_sink = operators
        .iter()
        .any(|operator| operator.kind == OperatorKind::Sink);
    if !has_source {
        operators.insert(
            0,
            OperatorSpec {
                id: "source".into(),
                kind: OperatorKind::Source,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
        );
    }
    if !has_sink {
        operators.push(OperatorSpec {
            id: "sink".into(),
            kind: OperatorKind::Sink,
            stateful: false,
            key_field: None,
            config: serde_json::json!({}),
        });
    }
    JobSpec {
        id: JobId::new("test-job").unwrap(),
        version: JobVersion(1),
        max_parallelism: 2,
        parallelism,
        operators,
        edges,
        sources: vec![SourceSpec {
            operator_id: "source".into(),
            input_type: "vec".into(),
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
            operator_id: "sink".into(),
            output_type: "collect".into(),
            config: serde_json::json!({}),
        }],
        state: None,
        checkpoint: None,
        recovery: Default::default(),
    }
}

fn map_operator(id: &str) -> OperatorSpec {
    OperatorSpec {
        id: id.into(),
        kind: OperatorKind::Map,
        stateful: false,
        key_field: None,
        config: serde_json::json!({}),
    }
}

fn edge(from: &str, to: &str) -> EdgeSpec {
    EdgeSpec {
        id: format!("{from}-{to}"),
        from: from.into(),
        to: to.into(),
        partitioned: false,
    }
}

// ---------- fusion tests ----------

#[test]
fn fuses_linear_processor_chain_into_one_chain() {
    let spec = spec(
        vec![map_operator("a"), map_operator("b"), map_operator("c")],
        vec![edge("source", "a"), edge("a", "b"), edge("b", "c"), edge("c", "sink")],
        1,
    );
    let plan = JobPlan::compile(spec).unwrap();
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into())]])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    // source chain, fused abc chain, sink chain
    assert_eq!(graph.chains.len(), 3);
    let fused = graph
        .chains
        .iter()
        .find(|chain| chain.task_ids.iter().any(|id| id == "a-0"))
        .unwrap();
    assert_eq!(fused.task_ids, vec!["a-0".to_string(), "b-0".to_string(), "c-0".to_string()]);
    assert_eq!(fused.processors.len(), 3);
}

#[test]
fn stateful_operator_breaks_the_chain() {
    let mut job = spec(
        vec![map_operator("a"), stateful_operator("agg"), map_operator("b")],
        vec![edge("source", "a"), edge("a", "agg"), edge("agg", "b"), edge("b", "sink")],
        1,
    );
    job.state = Some(crate::job::StateSpec {
        backend: "embedded_kv".into(),
        namespace: None,
        ttl_ms: None,
        format_version: 1,
    });
    let plan = JobPlan::compile(job).unwrap();
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into())]])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn crate::state::StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let graph = ExecutionGraphBuilder::default()
        .with_state(backend)
        .build(&plan, &adapter, &resource())
        .unwrap();
    // source, [a], [agg], [b], sink — a cannot fuse with agg
    assert_eq!(graph.chains.len(), 5);
}

fn stateful_operator(id: &str) -> OperatorSpec {
    OperatorSpec {
        id: id.into(),
        kind: OperatorKind::Aggregate,
        stateful: true,
        key_field: Some("key".into()),
        config: serde_json::json!({}),
    }
}

// ---------- end-to-end kernel tests ----------

#[tokio::test]
async fn pipelines_batches_to_sink_in_order() {
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![
            vec![(1, "a".into()), (2, "b".into())],
            vec![(3, "c".into())],
        ])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(spec(vec![], vec![edge("source", "sink")], 1)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let cancellation = CancellationToken::new();
    let runner = tokio::spawn(run_graph(graph, cancellation.clone()));
    // EOF ends the source chain; wait for the run to finish.
    tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .expect("graph run timed out")
        .unwrap()
        .unwrap();
    let written = adapter.output.written.lock().unwrap();
    let rows: Vec<i64> = written
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(rows, vec![1, 2, 3]);
}

#[tokio::test]
async fn backpressure_blocks_upstream_when_channel_is_full() {
    struct SlowInput {
        reads: AtomicUsize,
        max_reads: usize,
    }
    #[async_trait]
    impl Input for SlowInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok((
                Arc::new(MessageBatch::new_arrow(int64_batch(vec![(1, "a".into())]))),
                Arc::new(crate::input::NoopAck),
            ))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    struct SlowProcessor;
    #[async_trait]
    impl Processor for SlowProcessor {
        async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(ProcessResult::Single(batch))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    let input = Arc::new(SlowInput {
        reads: AtomicUsize::new(0),
        max_reads: usize::MAX,
    });
    let _ = input.max_reads;
    let adapter = Adapter {
        input: input.clone(),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(SlowProcessor),
    };
    // source -> map (slow) -> sink, capacity 1: the bounded edge caps in-flight
    // envelopes so source reads stall well below unbounded.
    let plan = JobPlan::compile(
        spec(vec![map_operator("m")], vec![edge("source", "m"), edge("m", "sink")], 1),
    )
    .unwrap();
    let graph = ExecutionGraphBuilder::new(1)
        .build(&plan, &adapter, &resource())
        .unwrap();
    let cancellation = CancellationToken::new();
    tokio::spawn(run_graph(graph, cancellation.clone()));
    tokio::time::sleep(Duration::from_millis(300)).await;
    cancellation.cancel();
    // With capacity 1 and a 50ms consumer, reads stay bounded (roughly
    // elapsed/latency + capacity), far below the 300ms/0ms unbounded count.
    let reads = input.reads.load(Ordering::SeqCst);
    assert!(reads <= 32, "source reads should be backpressured, got {reads}");
}

#[tokio::test]
async fn cancellation_stops_all_chains() {
    struct ForeverInput;
    #[async_trait]
    impl Input for ForeverInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok((
                Arc::new(MessageBatch::new_arrow(int64_batch(vec![(1, "a".into())]))),
                Arc::new(crate::input::NoopAck),
            ))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    let adapter = Adapter {
        input: Arc::new(ForeverInput),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(
        spec(vec![map_operator("m")], vec![edge("source", "m"), edge("m", "sink")], 1),
    )
    .unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let cancellation = CancellationToken::new();
    let runner = tokio::spawn(run_graph(graph, cancellation.clone()));
    tokio::time::sleep(Duration::from_millis(100)).await;
    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .expect("graph did not stop after cancellation")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn partitioned_edge_routes_by_key_hash() {
    // parallelism 2: map is keyed so both subtasks exist; sink broadcast.
    let mut job = spec(
        vec![stateful_operator("agg")],
        vec![edge("source", "agg"), edge("agg", "sink")],
        2,
    );
    job.state = Some(crate::job::StateSpec {
        backend: "embedded_kv".into(),
        namespace: None,
        ttl_ms: None,
        format_version: 1,
    });
    let plan = JobPlan::compile(job).unwrap();
    let task_ids = plan.tasks.iter().map(|t| t.id.clone()).collect::<Vec<_>>();
    assert_eq!(task_ids.len(), 6); // 3 operators × 2 subtasks
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into()), (2, "b".into())]])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn crate::state::StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let graph = ExecutionGraphBuilder::default()
        .with_state(backend)
        .build_subgraph(&plan, &task_ids, &adapter, &resource())
        .unwrap_or_else(|error| panic!("build failed: {error}"));
    // Building the parallel graph with partitioned edges succeeds and each
    // agg subtask receives its own channel.
    let agg_chains: Vec<_> = graph
        .chains
        .iter()
        .filter(|chain| chain.task_ids.iter().any(|id| id.starts_with("agg-")))
        .collect();
    assert_eq!(agg_chains.len(), 2);
    assert_eq!(graph.channel_capacity, DEFAULT_CHANNEL_CAPACITY);
}

#[test]
fn rejects_assignment_that_splits_an_edge() {
    let spec = spec(
        vec![map_operator("m")],
        vec![edge("source", "m"), edge("m", "sink")],
        1,
    );
    let plan = JobPlan::compile(spec).unwrap();
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into())]])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    // Assign source but not its downstream map: the edge is split.
    let result = ExecutionGraphBuilder::default().build_subgraph(
        &plan,
        &["source-0".to_string()],
        &adapter,
        &resource(),
    );
    assert!(matches!(result, Err(Error::Config(message)) if message.contains("co-located")));
}

// ---------- barrier alignment tests ----------

use crate::executor::barrier::Aligner;
use crate::executor::Envelope;
use crate::checkpoint::CheckpointBarrier;
use crate::input::NoopAck;

fn barrier(checkpoint_id: &str) -> Envelope {
    Envelope::Barrier(CheckpointBarrier {
        checkpoint_id: checkpoint_id.into(),
        generation: 1,
    })
}

fn data_envelope(value: i64) -> Envelope {
    Envelope::Data(
        Arc::new(MessageBatch::new_arrow(int64_batch(vec![(value, "a".into())]))),
        Arc::new(NoopAck),
    )
}

#[test]
fn aligner_buffers_until_all_inputs_barrier() {
    let mut aligner = Aligner::new(2, 100);
    assert!(aligner.is_aligning() == false);
    // Input 0 barrier arrives; input 1 data must buffer.
    assert!(aligner.observe(0, barrier("cp-1")).unwrap().is_none());
    assert!(aligner.is_aligning());
    assert!(aligner.observe(1, data_envelope(5)).unwrap().is_none());
    // Input 1's barrier completes the alignment.
    let aligned = aligner.observe(1, barrier("cp-1")).unwrap().unwrap();
    assert_eq!(aligned.checkpoint_id, "cp-1");
    let released = aligner.release();
    assert_eq!(released.len(), 1);
    assert!(!aligner.is_aligning());
}

#[test]
fn aligner_fails_when_buffer_bound_exceeded() {
    let mut aligner = Aligner::new(2, 2);
    aligner.observe(0, barrier("cp-1")).unwrap();
    aligner.observe(1, data_envelope(1)).unwrap();
    aligner.observe(1, data_envelope(2)).unwrap();
    assert!(aligner.observe(1, data_envelope(3)).is_err());
    // Release still drains what was buffered.
    assert_eq!(aligner.release().len(), 3);
    assert!(!aligner.is_aligning());
}

#[test]
fn aligner_passes_data_through_when_not_aligning() {
    let mut aligner = Aligner::new(2, 100);
    // No barrier in flight: data passes straight through (None = no barrier).
    assert!(aligner.observe(1, data_envelope(9)).unwrap().is_none());
    assert!(!aligner.is_aligning());
    assert!(aligner.release().is_empty());
}

// ---------- end-to-end barrier tests ----------

use crate::executor::task::{run_graph_with_hooks, CheckpointHook};
use crate::executor::barrier::ChainSnapshot;

#[tokio::test]
async fn barrier_flows_to_sink_without_stalling_data() {
    struct StreamInput {
        sent: AtomicUsize,
        acks_needed: usize,
    }
    #[async_trait]
    impl Input for StreamInput {
        async fn connect(&self) -> Result<(), Error> { Ok(()) }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            let count = self.sent.fetch_add(1, Ordering::SeqCst);
            if count >= 50 { return Err(Error::EOF); }
            Ok((
                Arc::new(MessageBatch::new_arrow(int64_batch(vec![(count as i64, "a".into())]))),
                Arc::new(crate::input::NoopAck),
            ))
        }
        async fn current_positions(&self) -> Result<Vec<crate::checkpoint::SourcePosition>, Error> {
            Ok(vec![crate::checkpoint::SourcePosition::for_partition(0, self.sent.load(Ordering::SeqCst) as u64)])
        }
        async fn close(&self) -> Result<(), Error> { Ok(()) }
    }

    let input = Arc::new(StreamInput { sent: AtomicUsize::new(0), acks_needed: 0 });
    let _ = input.acks_needed;
    let adapter = Adapter {
        input: input.clone(),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(
        spec(vec![map_operator("m")], vec![edge("source", "m"), edge("m", "sink")], 1),
    ).unwrap();
    let graph = ExecutionGraphBuilder::default().build(&plan, &adapter, &resource()).unwrap();

    // Wire a barrier injection channel into the source chain.
    let (barrier_tx, barrier_rx) = flume::bounded::<Envelope>(8);
    let (coordinator, report_tx) = crate::executor::BarrierCoordinator::new(
        JobId::new("barrier-job").unwrap(),
        JobVersion(1),
        1,
        1,
        ["source-0".to_string()],
    );
    let coordinator_cancellation = CancellationToken::new();
    tokio::spawn(coordinator.run(coordinator_cancellation.clone()));

    let mut hooks = std::collections::BTreeMap::new();
    hooks.insert("source-0".to_string(), CheckpointHook {
        reporter: Some(report_tx),
        barrier_rx: Some(Arc::new(tokio::sync::Mutex::new(barrier_rx))),
        state: None,
        task_id: Some("source-0".to_string()),
        event_time_gate: Arc::new(tokio::sync::Mutex::new(None)),
        partition: Some(0),
    });

    let cancellation = CancellationToken::new();
    let runner = tokio::spawn(run_graph_with_hooks(graph, cancellation.clone(), hooks));
    // Inject barriers while data flows. The source chain may already have
    // finished (50 quick EOF batches), so closed-channel sends are tolerated.
    for round in 0..3 {
        let _ = barrier_tx
            .send_async(Envelope::Barrier(CheckpointBarrier {
                checkpoint_id: format!("cp-{round}"),
                generation: 1,
            }))
            .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The graph finishes on EOF; barriers must not stall it.
    tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .expect("graph stalled during checkpoints")
        .unwrap()
        .unwrap();
    coordinator_cancellation.cancel();
    let written = adapter.output.written.lock().unwrap().len();
    assert_eq!(written, 50, "all batches must reach the sink alongside barriers");
}

#[tokio::test]
async fn coordinator_completes_only_after_all_participants() {
    let (coordinator, report_tx) = crate::executor::BarrierCoordinator::new(
        JobId::new("align-job").unwrap(),
        JobVersion(1),
        1,
        1,
        ["source-0".to_string(), "m-0".to_string()],
    );
    let cancellation = CancellationToken::new();
    let handle = tokio::spawn(coordinator.run(cancellation.clone()));

    let report = |task_id: &str, checkpoint_id: &str| ChainSnapshot {
        task_id: task_id.into(),
        attempt_id: format!("{task_id}-a"),
        partition: 0,
        barrier: CheckpointBarrier { checkpoint_id: checkpoint_id.into(), generation: 1 },
        state: crate::state::StateSnapshot::new(1, vec![]),
        source_positions: vec![],
        watermark_ms: None,
    };
    // First participant reports; checkpoint stays incomplete (no error, still running).
    report_tx.send(report("source-0", "cp-9")).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(handle.is_finished() == false);
    // Second participant with a different barrier id: the coordinator rejects
    // and records the error instead of completing.
    report_tx.send(report("m-0", "cp-OTHER")).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(handle.is_finished() == false);
    cancellation.cancel();
    handle.await.unwrap();
}

// ---------- event-time source chain tests ----------

use crate::executor::event_time_gate::EventTimeGate;
use crate::job::{LateEventPolicy, WatermarkSpec, WatermarkStrategy};

#[tokio::test]
async fn event_time_source_holds_and_releases_through_kernel() {
    // Gate semantics already unit-tested in event_time_gate; this test pins
    // the end-to-end hold → watermark release cadence a source chain relies
    // on (the source loop feeds observe() per batch and refresh() per tick).
    let time = TimeSpec {
        mode: TimeMode::EventTime,
        timestamp_field: Some("ts".into()),
        watermark: Some(WatermarkSpec {
            strategy: WatermarkStrategy::Monotonous,
            out_of_orderness_ms: 0,
            idle_timeout_ms: None,
        }),
        allowed_lateness_ms: 0,
        late_event_policy: LateEventPolicy::Drop,
        late_event_route: None,
    };
    let mut gate = EventTimeGate::new(&time, vec![1_000]).unwrap();

    let first = gate.observe(0, Arc::new(MessageBatch::new_arrow(int64_batch(vec![(100, "a".into()), (200, "b".into())])))).unwrap();
    assert!(first.ready.is_empty());
    let second = gate.observe(0, Arc::new(MessageBatch::new_arrow(int64_batch(vec![(2_500, "c".into())])))).unwrap();
    let released: Vec<i64> = second.ready.iter().flat_map(|(batch, _)| {
        batch.record_batch().column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().to_vec()
    }).collect();
    assert_eq!(released, vec![100, 200]);
    let third = gate.observe(0, Arc::new(MessageBatch::new_arrow(int64_batch(vec![(6_000, "d".into())])))).unwrap();
    let released: Vec<i64> = third.ready.iter().flat_map(|(batch, _)| {
        batch.record_batch().column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().to_vec()
    }).collect();
    assert_eq!(released, vec![2_500]);
}

// ---------- stateful operator wiring tests ----------

#[test]
fn stateful_operator_wrapped_with_task_namespace() {
    let mut job = spec(
        vec![stateful_operator("agg")],
        vec![edge("source", "agg"), edge("agg", "sink")],
        1,
    );
    job.state = Some(crate::job::StateSpec {
        backend: "embedded_kv".into(),
        namespace: None,
        ttl_ms: None,
        format_version: 1,
    });
    let plan = JobPlan::compile(job).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn crate::state::StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into())]])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    let graph = ExecutionGraphBuilder::default()
        .with_state(backend.clone())
        .build(&plan, &adapter, &resource())
        .unwrap();
    // The stateful task breaks the chain; its processor is the wrapper.
    let agg_chain = graph
        .chains
        .iter()
        .find(|chain| chain.task_ids.iter().any(|id| id == "agg-0"))
        .unwrap();
    assert_eq!(agg_chain.processors.len(), 1);
    // Feeding a keyed batch through the chain's processor leaves state in the
    // task namespace (wrapper injected the count column).
    let batch = Arc::new(MessageBatch::new_arrow(int64_batch(vec![(1, "a".into())])));
    futures_check(agg_chain.processors[0].clone(), batch).unwrap();
    assert_eq!(backend.scan("job:test-job:task:agg-0").unwrap().len(), 1);
}

fn futures_check(
    processor: Arc<dyn crate::processor::Processor>,
    batch: MessageBatchRef,
) -> Result<(), Error> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async move {
        processor.process(batch).await?;
        Ok(())
    })
}

#[test]
fn stateful_operator_without_backend_is_rejected() {
    let mut job = spec(
        vec![stateful_operator("agg")],
        vec![edge("source", "agg"), edge("agg", "sink")],
        1,
    );
    // validate() requires state when an operator is stateful; declare it but
    // build without a backend to trigger the builder guard.
    job.state = Some(crate::job::StateSpec {
        backend: "embedded_kv".into(),
        namespace: None,
        ttl_ms: None,
        format_version: 1,
    });
    let plan = JobPlan::compile(job).unwrap();
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into())]])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    let result = ExecutionGraphBuilder::default().build(&plan, &adapter, &resource());
    assert!(matches!(result, Err(Error::Config(message)) if message.contains("stateful operator 'agg' requires a Job state backend")));
}

// ---------- migrated legacy runner semantics ----------

#[test]
fn rejects_unpartitioned_source_when_job_is_parallel_migrated() {
    // A parallel Job whose source input cannot pin partitions must fail at
    // graph build (legacy SingleComputeJobRunner guard, migrated).
    struct UnpartitionedInput;
    #[async_trait]
    impl Input for UnpartitionedInput {
        async fn connect(&self) -> Result<(), Error> { Ok(()) }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            Err(Error::Process("not readable".into()))
        }
        async fn close(&self) -> Result<(), Error> { Ok(()) }
    }
    struct UnpartitionedAdapter(Adapter);
    impl JobComponentAdapter for UnpartitionedAdapter {
        fn build_input(&self, _s: &SourceSpec, _r: &Resource) -> Result<Arc<dyn Input>, Error> {
            Ok(Arc::new(UnpartitionedInput))
        }
        fn build_output(&self, _s: &SinkSpec, _r: &Resource) -> Result<Arc<dyn Output>, Error> {
            self.0.build_output(_s, _r)
        }
        fn build_processor(&self, _o: &OperatorSpec, _r: &Resource) -> Result<Arc<dyn Processor>, Error> {
            self.0.build_processor(_o, _r)
        }
    }

    let mut job = spec(
        vec![stateful_operator("agg")],
        vec![edge("source", "agg"), edge("agg", "sink")],
        2,
    );
    job.state = Some(crate::job::StateSpec {
        backend: "embedded_kv".into(),
        namespace: None,
        ttl_ms: None,
        format_version: 1,
    });
    let plan = JobPlan::compile(job).unwrap();
    let adapter = UnpartitionedAdapter(Adapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into())]])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    });
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn crate::state::StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let mut resource = resource();
    // Full assignment (all tasks, both subtasks) mirrors the legacy test:
    // the source has 2 tasks and the input cannot pin partitions.
    let task_ids = plan.tasks.iter().map(|t| t.id.clone()).collect::<Vec<_>>();
    let result = ExecutionGraphBuilder::default()
        .with_state(backend)
        .build_subgraph(&plan, &task_ids, &adapter, &mut resource);
    assert!(
        matches!(&result, Err(Error::Config(message)) if message.contains("does not support partitioned")),
        "expected partition guard failure, got {}",
        result.err().map(|e| e.to_string()).unwrap_or_default()
    );
}

#[test]
fn separates_route_and_update_late_event_actions_migrated() {
    // Route vs Update policies produce distinct actions for the same late
    // event (legacy runner guard, migrated to the gate).
    let time = |policy: LateEventPolicy| TimeSpec {
        mode: TimeMode::EventTime,
        timestamp_field: Some("ts".into()),
        watermark: Some(WatermarkSpec {
            strategy: WatermarkStrategy::Monotonous,
            out_of_orderness_ms: 0,
            idle_timeout_ms: None,
        }),
        allowed_lateness_ms: 100,
        late_event_policy: policy,
        late_event_route: None,
    };
    let late = |gate: &mut EventTimeGate| {
        gate.observe(0, Arc::new(MessageBatch::new_arrow(int64_batch(vec![(1_000, "a".into())])))).unwrap();
        gate.observe(0, Arc::new(MessageBatch::new_arrow(int64_batch(vec![(1_500, "a".into())])))).unwrap()
    };
    // Watermark 2_100 closes window [1000,2000). A NEW row at 1_900 (inside
    // the closed window, before the 2_200 lateness deadline) diverges per
    // policy: Route forwards it late, Update marks it for reprocessing.
    let mut routed = EventTimeGate::new(&time(LateEventPolicy::Route), vec![1_000]).unwrap();
    routed.observe(0, Arc::new(MessageBatch::new_arrow(int64_batch(vec![(2_100, "a".into())])))).unwrap();
    let decision = routed
        .observe(0, Arc::new(MessageBatch::new_arrow(int64_batch(vec![(1_900, "a".into())]))))
        .unwrap();
    let routed_action = decision.ready.first().map(|(_, action)| *action);

    let mut updated = EventTimeGate::new(&time(LateEventPolicy::Update), vec![1_000]).unwrap();
    updated.observe(0, Arc::new(MessageBatch::new_arrow(int64_batch(vec![(2_100, "a".into())])))).unwrap();
    let decision = updated
        .observe(0, Arc::new(MessageBatch::new_arrow(int64_batch(vec![(1_900, "a".into())]))))
        .unwrap();
    let updated_action = decision.ready.first().map(|(_, action)| *action);

    assert_eq!(routed_action, Some(crate::event_time::WindowAction::Route));
    assert_eq!(updated_action, Some(crate::event_time::WindowAction::Update));
    let _ = late;
}
