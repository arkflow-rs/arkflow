//! Executor kernel unit tests: chain fusion, envelope ordering, backpressure,
//! EOS propagation, and partitioned routing.

use crate::executor::graph::{ExecutionGraphBuilder, DEFAULT_CHANNEL_CAPACITY};
use crate::executor::run_graph;
use crate::input::{fanout_ack, Ack, Input};
use crate::job::{
    EdgeSpec, JobComponentAdapter, JobId, JobPlan, JobSpec, JobVersion, OperatorKind, OperatorSpec,
    SinkSpec, SourceSpec, TimeMode, TimeSpec, WatermarkSpec, WatermarkStrategy,
};
use crate::output::Output;
use crate::processor::Processor;
use crate::{Error, MessageBatch, MessageBatchRef, ProcessResult, Resource};
use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
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
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1.clone()).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn window_batch(rows: Vec<(i64, String, i64)>, watermark: Option<i64>) -> MessageBatchRef {
    let mut fields = vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ];
    let mut columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.1.clone()).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.2).collect::<Vec<_>>(),
        )),
    ];
    if let Some(watermark) = watermark {
        fields.push(Field::new("__watermark_ms", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(vec![watermark; rows.len()])));
    }
    Arc::new(MessageBatch::new_arrow(
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
    ))
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

struct CountingAck {
    acknowledgements: Arc<AtomicUsize>,
}

#[async_trait]
impl Ack for CountingAck {
    async fn ack(&self) -> Result<(), Error> {
        self.acknowledgements.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct CountingInput {
    batches: Mutex<std::collections::VecDeque<MessageBatchRef>>,
    acknowledgements: Arc<AtomicUsize>,
}

#[async_trait]
impl Input for CountingInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        self.batches
            .lock()
            .unwrap()
            .pop_front()
            .map(|batch| {
                (
                    batch,
                    Arc::new(CountingAck {
                        acknowledgements: self.acknowledgements.clone(),
                    }) as Arc<dyn Ack>,
                )
            })
            .ok_or(Error::EOF)
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Default)]
struct CollectOutput {
    written: Mutex<Vec<RecordBatch>>,
    fail: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl Output for CollectOutput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(Error::Process("injected sink failure".into()));
        }
        self.written
            .lock()
            .unwrap()
            .push(msg.record_batch().clone());
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

struct FailingProcessor;

#[async_trait]
impl Processor for FailingProcessor {
    async fn process(&self, _batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        Err(Error::Process("injected processor failure".into()))
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

struct MultiInputAdapter {
    inputs: HashMap<String, Arc<dyn Input>>,
    outputs: HashMap<String, Arc<CollectOutput>>,
    processor: Arc<dyn Processor>,
}

impl JobComponentAdapter for MultiInputAdapter {
    fn build_input(
        &self,
        source: &SourceSpec,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        self.inputs
            .get(&source.operator_id)
            .cloned()
            .ok_or_else(|| Error::Config(format!("missing test input {}", source.operator_id)))
    }

    fn build_output(
        &self,
        sink: &SinkSpec,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        self.outputs
            .get(&sink.operator_id)
            .cloned()
            .map(|output| output as Arc<dyn Output>)
            .ok_or_else(|| Error::Config(format!("missing test output {}", sink.operator_id)))
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
    let has_window = operators
        .iter()
        .any(|operator| operator.kind == OperatorKind::Window);
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
        rebalance: None,
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
        state: has_window.then_some(crate::job::StateSpec {
            backend: "embedded_kv".into(),
            durability: crate::job::StateDurability::Ephemeral,
            root: None,
            namespace: None,
            ttl_ms: None,
            format_version: 1,
            max_pending_transactions: None,
            max_bytes: None,
        }),
        checkpoint: None,
        placement: crate::job::PlacementStrategy::Colocated,
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

fn source_spec(operator_id: &str) -> SourceSpec {
    SourceSpec {
        operator_id: operator_id.into(),
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
    }
}

fn sink_operator(id: &str, error_sink: bool) -> OperatorSpec {
    OperatorSpec {
        id: id.into(),
        kind: OperatorKind::Sink,
        stateful: false,
        key_field: None,
        config: error_sink
            .then(|| serde_json::json!({"__arkflow_error_sink": true}))
            .unwrap_or_else(|| serde_json::json!({})),
    }
}

// ---------- fusion tests ----------

#[test]
fn fuses_linear_processor_chain_into_one_chain() {
    let spec = spec(
        vec![map_operator("a"), map_operator("b"), map_operator("c")],
        vec![
            edge("source", "a"),
            edge("a", "b"),
            edge("b", "c"),
            edge("c", "sink"),
        ],
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
    assert_eq!(
        fused.task_ids,
        vec!["a-0".to_string(), "b-0".to_string(), "c-0".to_string()]
    );
    assert_eq!(fused.processors.len(), 3);
}

#[test]
fn stateful_operator_breaks_the_chain() {
    let mut job = spec(
        vec![
            map_operator("a"),
            stateful_operator("agg"),
            map_operator("b"),
        ],
        vec![
            edge("source", "a"),
            edge("a", "agg"),
            edge("agg", "b"),
            edge("b", "sink"),
        ],
        1,
    );
    job.state = Some(crate::job::StateSpec {
        backend: "embedded_kv".into(),
        durability: crate::job::StateDurability::Ephemeral,
        root: None,
        namespace: None,
        ttl_ms: None,
        format_version: 1,
        max_pending_transactions: None,
        max_bytes: None,
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
async fn window_operator_runs_inside_compiled_execution_graph_and_flushes_eos() {
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let input = Arc::new(CountingInput {
        batches: Mutex::new(std::collections::VecDeque::from([
            window_batch(vec![(1_000, "a".into(), 4)], None),
            window_batch(vec![(11_000, "z".into(), 0)], Some(10_000)),
        ])),
        acknowledgements: acknowledgements.clone(),
    });
    let output = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input,
        output: output.clone(),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(JobSpec {
        rebalance: None,
        id: JobId::new("window-runtime-job").unwrap(),
        version: JobVersion(1),
        max_parallelism: 1,
        parallelism: 1,
        operators: vec![
            OperatorSpec {
                id: "source".into(),
                kind: OperatorKind::Source,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
            OperatorSpec {
                id: "window".into(),
                kind: OperatorKind::Window,
                stateful: true,
                key_field: Some("key".into()),
                config: serde_json::json!({
                    "type": "window",
                    "kind": "tumbling",
                    "size_ms": 10_000,
                    "timestamp_field": "ts",
                    "key_field": "key",
                    "value_fields": ["value"],
                    "trigger": "watermark",
                    "watermark_field": "__watermark_ms"
                }),
            },
            sink_operator("sink", false),
        ],
        edges: vec![edge("source", "window"), edge("window", "sink")],
        sources: vec![source_spec("source")],
        sinks: vec![SinkSpec {
            operator_id: "sink".into(),
            output_type: "collect".into(),
            config: serde_json::json!({}),
        }],
        state: Some(crate::job::StateSpec {
            backend: "embedded_kv".into(),
            durability: crate::job::StateDurability::Ephemeral,
            root: None,
            namespace: None,
            ttl_ms: None,
            format_version: 1,
            max_pending_transactions: None,
            max_bytes: None,
        }),
        checkpoint: None,
        placement: crate::job::PlacementStrategy::Colocated,
        recovery: Default::default(),
    })
    .unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    run_graph(graph, CancellationToken::new()).await.unwrap();

    let sums: Vec<i64> = output
        .written
        .lock()
        .unwrap()
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("sum")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(sums, vec![4, 0]);
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn terminal_sink_acknowledges_only_after_successful_write() {
    for fail in [false, true] {
        let acknowledgements = Arc::new(AtomicUsize::new(0));
        let input = Arc::new(CountingInput {
            batches: Mutex::new(std::collections::VecDeque::from([Arc::new(
                MessageBatch::new_arrow(int64_batch(vec![(1, "a".into())])),
            )])),
            acknowledgements: acknowledgements.clone(),
        });
        let output = Arc::new(CollectOutput::default());
        output.fail.store(fail, Ordering::SeqCst);
        let adapter = Adapter {
            input,
            output,
            processor: Arc::new(PassThroughProcessor),
        };
        let plan = JobPlan::compile(spec(vec![], vec![edge("source", "sink")], 1)).unwrap();
        let graph = ExecutionGraphBuilder::default()
            .build(&plan, &adapter, &resource())
            .unwrap();
        let result = run_graph(graph, CancellationToken::new()).await;
        if fail {
            assert!(result.is_err());
        } else {
            assert!(result.is_ok());
        }
        assert_eq!(
            acknowledgements.load(Ordering::SeqCst),
            usize::from(!fail),
            "ack must follow the terminal sink result"
        );
    }
}

#[tokio::test]
async fn fanout_ack_waits_for_every_branch_and_is_idempotent() {
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let parent: Arc<dyn Ack> = Arc::new(CountingAck {
        acknowledgements: acknowledgements.clone(),
    });
    let children = fanout_ack(parent, 3);

    children[0].ack().await.unwrap();
    children[0].ack().await.unwrap();
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 0);
    children[1].ack().await.unwrap();
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 0);
    children[2].ack().await.unwrap();
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 1);
    children[2].ack().await.unwrap();
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn aborted_fanout_rejects_queued_sibling_acknowledgements() {
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let parent: Arc<dyn Ack> = Arc::new(CountingAck {
        acknowledgements: acknowledgements.clone(),
    });
    let children = fanout_ack(parent, 2);

    children[0].abort().await.unwrap();
    assert!(children[1].ack().await.is_err());
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn multi_input_chain_preserves_every_upstream_channel() {
    let left = Arc::new(VecInput::new(vec![vec![(1, "left".into())]]));
    let right = Arc::new(VecInput::new(vec![vec![(2, "right".into())]]));
    let output = Arc::new(CollectOutput::default());
    let adapter = MultiInputAdapter {
        inputs: HashMap::from([
            ("left-source".into(), left as Arc<dyn Input>),
            ("right-source".into(), right as Arc<dyn Input>),
        ]),
        outputs: HashMap::from([("sink".into(), output.clone())]),
        processor: Arc::new(PassThroughProcessor),
    };
    let job = JobSpec {
        rebalance: None,
        id: JobId::new("multi-input-job").unwrap(),
        version: JobVersion(1),
        max_parallelism: 1,
        parallelism: 1,
        operators: vec![
            OperatorSpec {
                id: "left-source".into(),
                kind: OperatorKind::Source,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
            OperatorSpec {
                id: "right-source".into(),
                kind: OperatorKind::Source,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
            map_operator("merge"),
            sink_operator("sink", false),
        ],
        edges: vec![
            edge("left-source", "merge"),
            edge("right-source", "merge"),
            edge("merge", "sink"),
        ],
        sources: vec![source_spec("left-source"), source_spec("right-source")],
        sinks: vec![SinkSpec {
            operator_id: "sink".into(),
            output_type: "collect".into(),
            config: serde_json::json!({}),
        }],
        state: None,
        checkpoint: None,
        placement: crate::job::PlacementStrategy::Colocated,
        recovery: Default::default(),
    };
    let plan = JobPlan::compile(job).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    run_graph(graph, CancellationToken::new()).await.unwrap();

    let rows: Vec<i64> = output
        .written
        .lock()
        .unwrap()
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(rows, vec![1, 2]);
}

#[tokio::test]
async fn multi_input_watermark_uses_the_slowest_upstream() {
    struct WatermarkRecorder {
        values: Arc<Mutex<Vec<i64>>>,
    }

    #[async_trait]
    impl Processor for WatermarkRecorder {
        async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
            Ok(ProcessResult::Single(batch))
        }

        async fn on_watermark(&self, watermark_ms: i64) -> Result<ProcessResult, Error> {
            self.values.lock().unwrap().push(watermark_ms);
            Ok(ProcessResult::None)
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    let left = Arc::new(VecInput::new(vec![vec![(2_000, "left".into())]]));
    let right = Arc::new(VecInput::new(vec![vec![(1_000, "right".into())]]));
    let values = Arc::new(Mutex::new(Vec::new()));
    let output = Arc::new(CollectOutput::default());
    let adapter = MultiInputAdapter {
        inputs: HashMap::from([
            ("left-source".into(), left as Arc<dyn Input>),
            ("right-source".into(), right as Arc<dyn Input>),
        ]),
        outputs: HashMap::from([("sink".into(), output)]),
        processor: Arc::new(WatermarkRecorder {
            values: values.clone(),
        }),
    };
    let event_time = || TimeSpec {
        mode: TimeMode::EventTime,
        timestamp_field: Some("ts".into()),
        watermark: Some(WatermarkSpec {
            strategy: WatermarkStrategy::Monotonous,
            out_of_orderness_ms: 0,
            idle_timeout_ms: None,
        }),
        allowed_lateness_ms: 0,
        late_event_policy: Default::default(),
        late_event_route: None,
    };
    let job = JobSpec {
        rebalance: None,
        id: JobId::new("multi-watermark-job").unwrap(),
        version: JobVersion(1),
        max_parallelism: 1,
        parallelism: 1,
        operators: vec![
            OperatorSpec {
                id: "left-source".into(),
                kind: OperatorKind::Source,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
            OperatorSpec {
                id: "right-source".into(),
                kind: OperatorKind::Source,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
            map_operator("merge"),
            sink_operator("sink", false),
        ],
        edges: vec![
            edge("left-source", "merge"),
            edge("right-source", "merge"),
            edge("merge", "sink"),
        ],
        sources: vec![
            SourceSpec {
                operator_id: "left-source".into(),
                input_type: "vec".into(),
                config: serde_json::json!({}),
                time: event_time(),
            },
            SourceSpec {
                operator_id: "right-source".into(),
                input_type: "vec".into(),
                config: serde_json::json!({}),
                time: event_time(),
            },
        ],
        sinks: vec![SinkSpec {
            operator_id: "sink".into(),
            output_type: "collect".into(),
            config: serde_json::json!({}),
        }],
        state: None,
        checkpoint: None,
        placement: crate::job::PlacementStrategy::Colocated,
        recovery: Default::default(),
    };
    let plan = JobPlan::compile(job).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    run_graph(graph, CancellationToken::new()).await.unwrap();

    let values = values.lock().unwrap();
    assert!(!values.is_empty());
    assert!(values.iter().all(|watermark| *watermark == 1_000));
}

#[tokio::test]
async fn processor_failure_uses_error_output_without_receiving_successes() {
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let input = Arc::new(CountingInput {
        batches: Mutex::new(std::collections::VecDeque::from([Arc::new(
            MessageBatch::new_arrow(int64_batch(vec![(7, "bad".into())])),
        )])),
        acknowledgements: acknowledgements.clone(),
    });
    let primary = Arc::new(CollectOutput::default());
    let errors = Arc::new(CollectOutput::default());
    let adapter = MultiInputAdapter {
        inputs: HashMap::from([("source".into(), input as Arc<dyn Input>)]),
        outputs: HashMap::from([
            ("sink".into(), primary.clone()),
            ("error-sink".into(), errors.clone()),
        ]),
        processor: Arc::new(FailingProcessor),
    };
    let job = JobSpec {
        rebalance: None,
        id: JobId::new("error-route-job").unwrap(),
        version: JobVersion(1),
        max_parallelism: 1,
        parallelism: 1,
        operators: vec![
            OperatorSpec {
                id: "source".into(),
                kind: OperatorKind::Source,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
            map_operator("fail"),
            sink_operator("sink", false),
            sink_operator("error-sink", true),
        ],
        edges: vec![
            edge("source", "fail"),
            edge("fail", "sink"),
            edge("fail", "error-sink"),
        ],
        sources: vec![source_spec("source")],
        sinks: vec![
            SinkSpec {
                operator_id: "sink".into(),
                output_type: "collect".into(),
                config: serde_json::json!({}),
            },
            SinkSpec {
                operator_id: "error-sink".into(),
                output_type: "collect".into(),
                config: serde_json::json!({}),
            },
        ],
        state: None,
        checkpoint: None,
        placement: crate::job::PlacementStrategy::Colocated,
        recovery: Default::default(),
    };
    let plan = JobPlan::compile(job).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    run_graph(graph, CancellationToken::new()).await.unwrap();

    assert!(primary.written.lock().unwrap().is_empty());
    assert_eq!(errors.written.lock().unwrap().len(), 1);
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 1);
}

/// Pooled (parallelism > 1) variant of the job above: source -> fail -> sink,
/// with the processor chain carrying a bounded worker pool.
fn pooled_failure_job_spec(parallelism: u64, with_error_sink: bool) -> JobSpec {
    let mut operators = vec![map_operator("fail"), sink_operator("sink", false)];
    let mut edges = vec![edge("source", "fail"), edge("fail", "sink")];
    if with_error_sink {
        operators.push(sink_operator("error-sink", true));
        edges.push(edge("fail", "error-sink"));
    }
    let mut job = spec(operators, edges, 1);
    if with_error_sink {
        job.sinks.push(SinkSpec {
            operator_id: "error-sink".into(),
            output_type: "collect".into(),
            config: serde_json::json!({}),
        });
    }
    job.sources[0].config = serde_json::json!({
        "__arkflow_processor_parallelism": parallelism,
    });
    job
}

/// Verification 2026-09-11 (repair-unified-runtime-review-regressions task
/// 5.2): a processor failure inside a worker pool uses the same error-only
/// routing as the single-worker path — every failed batch and its
/// acknowledgement reach the error sink exactly once, and no success reaches
/// the primary sink.
#[tokio::test]
async fn pool_processor_failure_routes_to_error_output_without_successes() {
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let batches: Vec<MessageBatchRef> = (0..4)
        .map(|value| {
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
                value,
                "bad".into(),
            )])))
        })
        .collect();
    let input = Arc::new(CountingInput {
        batches: Mutex::new(std::collections::VecDeque::from(batches)),
        acknowledgements: acknowledgements.clone(),
    });
    let primary = Arc::new(CollectOutput::default());
    let errors = Arc::new(CollectOutput::default());
    let adapter = MultiInputAdapter {
        inputs: HashMap::from([("source".into(), input as Arc<dyn Input>)]),
        outputs: HashMap::from([
            ("sink".into(), primary.clone()),
            ("error-sink".into(), errors.clone()),
        ]),
        processor: Arc::new(FailingProcessor),
    };
    let plan = JobPlan::compile(pooled_failure_job_spec(4, true)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    run_graph(graph, CancellationToken::new()).await.unwrap();

    assert!(
        primary.written.lock().unwrap().is_empty(),
        "no success reaches the primary sink"
    );
    assert_eq!(
        errors.written.lock().unwrap().len(),
        4,
        "every failed batch reaches the error sink exactly once"
    );
    assert_eq!(
        acknowledgements.load(Ordering::SeqCst),
        4,
        "every failed batch is acknowledged exactly once through the error sink"
    );
}

/// Verification 2026-09-11 (repair-unified-runtime-review-regressions task
/// 5.2): without a configured error edge, a pooled processor failure aborts
/// the delivery's acknowledgement and fails the run instead of dropping it.
#[tokio::test]
async fn pool_processor_failure_without_error_output_fails_and_aborts() {
    // Acknowledgements that observe both outcomes: ack (must stay 0) and
    // abort (must fire for the failed delivery), so the test can tell
    // "aborted" apart from "never settled". A single delivery keeps the
    // abort count deterministic.
    let acks = Arc::new(AtomicUsize::new(0));
    let aborts = Arc::new(AtomicUsize::new(0));
    #[derive(Clone)]
    struct AbortObservingAck {
        acks: Arc<AtomicUsize>,
        aborts: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl crate::input::Ack for AbortObservingAck {
        async fn ack(&self) -> Result<(), Error> {
            self.acks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn abort(&self) -> Result<(), Error> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    struct AbortObservingInput {
        batches: Mutex<std::collections::VecDeque<MessageBatchRef>>,
        ack: AbortObservingAck,
    }
    #[async_trait]
    impl Input for AbortObservingInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            self.batches
                .lock()
                .unwrap()
                .pop_front()
                .map(|batch| (batch, Arc::new(self.ack.clone()) as Arc<dyn Ack>))
                .ok_or(Error::EOF)
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    let ack = AbortObservingAck {
        acks: acks.clone(),
        aborts: aborts.clone(),
    };
    let input = Arc::new(AbortObservingInput {
        batches: Mutex::new(std::collections::VecDeque::from([Arc::new(
            MessageBatch::new_arrow(int64_batch(vec![(7, "bad".into())])),
        )])),
        ack,
    });
    let primary = Arc::new(CollectOutput::default());
    let adapter = MultiInputAdapter {
        inputs: HashMap::from([("source".into(), input as Arc<dyn Input>)]),
        outputs: HashMap::from([("sink".into(), primary.clone())]),
        processor: Arc::new(FailingProcessor),
    };
    let plan = JobPlan::compile(pooled_failure_job_spec(4, false)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let result = run_graph(graph, CancellationToken::new()).await;

    assert!(
        result.is_err(),
        "a pool delivery failure without an error edge fails the run"
    );
    assert!(primary.written.lock().unwrap().is_empty());
    assert_eq!(
        acks.load(Ordering::SeqCst),
        0,
        "the failed delivery's acknowledgement is never committed"
    );
    assert_eq!(
        aborts.load(Ordering::SeqCst),
        1,
        "the failed delivery's acknowledgement is aborted exactly once"
    );
}

/// Verification (repair-kernel-review-findings task 2.2): the siblings path
/// of pooled failure routing. A processor that fans one input out to three
/// outputs ahead of a failing processor produces `ProcessorFailure.siblings`;
/// every sibling plus the failed delivery must reach the error sink exactly
/// once and the parent acknowledgement must settle exactly once.
#[tokio::test]
async fn pool_processor_failure_routes_sibling_outputs_to_error_output() {
    struct SplittingProcessor;
    #[async_trait]
    impl Processor for SplittingProcessor {
        async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
            Ok(ProcessResult::Multiple(vec![
                batch.clone(),
                batch.clone(),
                batch,
            ]))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct MultiProcessorAdapter {
        input: Arc<dyn Input>,
        outputs: HashMap<String, Arc<CollectOutput>>,
        processors: HashMap<String, Arc<dyn Processor>>,
    }
    impl JobComponentAdapter for MultiProcessorAdapter {
        fn build_input(
            &self,
            _source: &SourceSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Input>, Error> {
            Ok(self.input.clone())
        }
        fn build_output(
            &self,
            sink: &SinkSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Output>, Error> {
            self.outputs
                .get(&sink.operator_id)
                .cloned()
                .map(|output| output as Arc<dyn Output>)
                .ok_or_else(|| Error::Config(format!("missing test output {}", sink.operator_id)))
        }
        fn build_processor(
            &self,
            operator: &OperatorSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Processor>, Error> {
            self.processors
                .get(&operator.id)
                .cloned()
                .ok_or_else(|| Error::Config(format!("missing test processor {}", operator.id)))
        }
    }

    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let input = Arc::new(CountingInput {
        batches: Mutex::new(std::collections::VecDeque::from([Arc::new(
            MessageBatch::new_arrow(int64_batch(vec![(1, "bad".into())])),
        )])),
        acknowledgements: acknowledgements.clone(),
    });
    let primary = Arc::new(CollectOutput::default());
    let errors = Arc::new(CollectOutput::default());
    let adapter = MultiProcessorAdapter {
        input: input as Arc<dyn Input>,
        outputs: HashMap::from([
            ("sink".into(), primary.clone()),
            ("error-sink".into(), errors.clone()),
        ]),
        processors: HashMap::from([
            (
                "split".into(),
                Arc::new(SplittingProcessor) as Arc<dyn Processor>,
            ),
            (
                "fail".into(),
                Arc::new(FailingProcessor) as Arc<dyn Processor>,
            ),
        ]),
    };
    let mut job = spec(
        vec![
            map_operator("split"),
            map_operator("fail"),
            sink_operator("sink", false),
            sink_operator("error-sink", true),
        ],
        vec![
            edge("source", "split"),
            edge("split", "fail"),
            edge("fail", "sink"),
            edge("fail", "error-sink"),
        ],
        1,
    );
    job.sinks.push(SinkSpec {
        operator_id: "error-sink".into(),
        output_type: "collect".into(),
        config: serde_json::json!({}),
    });
    job.sources[0].config = serde_json::json!({
        "__arkflow_processor_parallelism": 4,
    });
    let plan = JobPlan::compile(job).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    run_graph(graph, CancellationToken::new()).await.unwrap();

    assert!(primary.written.lock().unwrap().is_empty());
    assert_eq!(
        errors.written.lock().unwrap().len(),
        3,
        "the failed delivery and both siblings reach the error sink exactly once"
    );
    assert_eq!(
        acknowledgements.load(Ordering::SeqCst),
        1,
        "the parent acknowledgement settles exactly once after every sibling"
    );
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
    let plan = JobPlan::compile(spec(
        vec![map_operator("m")],
        vec![edge("source", "m"), edge("m", "sink")],
        1,
    ))
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
    assert!(
        reads <= 32,
        "source reads should be backpressured, got {reads}"
    );
}

/// Verification (repair-kernel-review-findings task 1.4): the backpressure
/// contract also holds on the pooled path. With `parallelism > 1` the worker
/// pool's result channel is bounded, so a slow downstream stops the workers,
/// which fills the submit queue and finally blocks the chain loop's reads.
/// Before the fix the result channel was unbounded: processed results piled
/// up without bound while the slow sink wrote at its own pace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_path_backpressures_source_when_downstream_is_slow() {
    let reads = Arc::new(AtomicUsize::new(0));
    struct CountingFastInput {
        reads: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl Input for CountingFastInput {
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

    struct DelayedOutput {
        written: AtomicUsize,
        delay_ms: u64,
    }
    #[async_trait]
    impl Output for DelayedOutput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn write(&self, _msg: MessageBatchRef) -> Result<(), Error> {
            tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
            self.written.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct DynAdapter {
        input: Arc<dyn Input>,
        output: Arc<DelayedOutput>,
    }
    impl JobComponentAdapter for DynAdapter {
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
            Ok(Arc::new(PassThroughProcessor))
        }
    }

    let input = Arc::new(CountingFastInput {
        reads: reads.clone(),
    });
    let output = Arc::new(DelayedOutput {
        written: AtomicUsize::new(0),
        delay_ms: 100,
    });
    let adapter = DynAdapter {
        input: input.clone(),
        output: output.clone(),
    };
    let plan = JobPlan::compile(parallel_job_spec(4)).unwrap();
    // Capacity-1 edge keeps the allowed backlog small: queue(32) + done(8) +
    // workers(4) + edge(1) = 45, comfortably inside the assertion below.
    let graph = ExecutionGraphBuilder::new(1)
        .build(&plan, &adapter, &resource())
        .unwrap();
    let cancellation = CancellationToken::new();
    let handle = tokio::spawn(run_graph(graph, cancellation.clone()));
    // ~2s with a 100ms sink: ~20 writes. An unbounded result channel lets
    // reads race ahead of writes without bound; the bounded channel caps the
    // backlog at queue(32) + done(8) + workers(4) + edge(1).
    tokio::time::sleep(Duration::from_millis(2000)).await;
    cancellation.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let reads = reads.load(Ordering::SeqCst);
    let writes = output.written.load(Ordering::SeqCst);
    assert!(
        reads.saturating_sub(writes) <= 64,
        "pooled backlog must stay bounded: reads={reads}, writes={writes}"
    );
    assert!(
        writes >= 5,
        "sanity: the slow sink must have made progress, wrote {writes}"
    );
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
    let plan = JobPlan::compile(spec(
        vec![map_operator("m")],
        vec![edge("source", "m"), edge("m", "sink")],
        1,
    ))
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
        durability: crate::job::StateDurability::Ephemeral,
        root: None,
        namespace: None,
        ttl_ms: None,
        format_version: 1,
        max_pending_transactions: None,
        max_bytes: None,
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
        .build_subgraph(&plan, &task_ids, &adapter, &resource(), None)
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
        None,
    );
    assert!(matches!(result, Err(Error::Config(message)) if message.contains("co-located")));
}

// ---------- barrier alignment tests ----------

use crate::checkpoint::CheckpointBarrier;
use crate::executor::barrier::Aligner;
use crate::executor::Envelope;
use crate::input::NoopAck;

fn barrier(checkpoint_id: &str) -> Envelope {
    Envelope::Barrier(CheckpointBarrier {
        checkpoint_id: checkpoint_id.into(),
        generation: 1,
    })
}

fn data_envelope(value: i64) -> Envelope {
    Envelope::Data(
        Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
            value,
            "a".into(),
        )]))),
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

/// A delivery buffered by alignment must stop blocking its source's barrier
/// drain (held), and re-enter the in-flight set when alignment releases it:
/// otherwise a source whose data sits in a downstream aligner can never seal
/// its own cut and every multi-input checkpoint round times out.
#[test]
fn aligner_holds_buffered_acknowledgements_until_release() {
    use crate::executor::commit::{AckTracker, TrackingAck};

    let tracker = Arc::new(AckTracker::new());
    let mut aligner = Aligner::new(2, 100);
    assert!(aligner.observe(0, barrier("cp-1")).unwrap().is_none());
    // The second input's data arrives while alignment is in flight: the
    // aligner buffers it and its acknowledgement must be excluded from the
    // drain (`blocking() == 0`).
    let inner: Arc<dyn Ack> = Arc::new(NoopAck);
    let ack = Arc::new(TrackingAck::new(tracker.clone(), inner));
    assert_eq!(tracker.blocking(), 1);
    let batch: crate::MessageBatchRef = Arc::new(crate::MessageBatch::new_arrow(int64_batch(
        vec![(1, "a".into())],
    )));
    assert!(aligner
        .observe(1, Envelope::Data(batch, ack.clone()))
        .unwrap()
        .is_none());
    assert_eq!(tracker.blocking(), 0);
    // The completing barrier releases the buffer: the held delivery re-enters
    // the in-flight set until its acknowledgement finally completes.
    assert!(aligner.observe(1, barrier("cp-1")).unwrap().is_some());
    assert_eq!(aligner.release().len(), 1);
    assert_eq!(tracker.blocking(), 1);
    futures::executor::block_on(ack.ack()).unwrap();
    assert_eq!(tracker.blocking(), 0);
}

// ---------- end-to-end barrier tests ----------

use crate::executor::barrier::ChainSnapshot;
use crate::executor::task::{run_graph_with_hooks, CheckpointHook};

#[tokio::test]
async fn barrier_flows_to_sink_without_stalling_data() {
    struct StreamInput {
        sent: AtomicUsize,
        acks_needed: usize,
    }
    #[async_trait]
    impl Input for StreamInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            let count = self.sent.fetch_add(1, Ordering::SeqCst);
            if count >= 50 {
                return Err(Error::EOF);
            }
            Ok((
                Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
                    count as i64,
                    "a".into(),
                )]))),
                Arc::new(crate::input::NoopAck),
            ))
        }
        async fn current_positions(&self) -> Result<Vec<crate::checkpoint::SourcePosition>, Error> {
            Ok(vec![crate::checkpoint::SourcePosition::for_partition(
                0,
                self.sent.load(Ordering::SeqCst) as u64,
            )])
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    let input = Arc::new(StreamInput {
        sent: AtomicUsize::new(0),
        acks_needed: 0,
    });
    let _ = input.acks_needed;
    let adapter = Adapter {
        input: input.clone(),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(spec(
        vec![map_operator("m")],
        vec![edge("source", "m"), edge("m", "sink")],
        1,
    ))
    .unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();

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
    hooks.insert(
        "source-0".to_string(),
        CheckpointHook {
            reporter: Some(report_tx),
            failure_reporter: None,
            barrier_rx: Some(Arc::new(tokio::sync::Mutex::new(barrier_rx))),
            state: None,
            task_id: Some("source-0".to_string()),
            event_time_gate: Arc::new(tokio::sync::Mutex::new(None)),
            partition: Some(0),
            metrics: None,
            finished_reporter: None,
        },
    );

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
    assert_eq!(
        written, 50,
        "all batches must reach the sink alongside barriers"
    );
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
        barrier: CheckpointBarrier {
            checkpoint_id: checkpoint_id.into(),
            generation: 1,
        },
        cut_generation: 1,
        state: crate::state::StateSnapshot::new(1, vec![]),
        source_positions: vec![],
        watermark_ms: None,
        watermark_partitions: vec![],
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
use crate::job::LateEventPolicy;

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

    let first = gate
        .observe(
            0,
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![
                (100, "a".into()),
                (200, "b".into()),
            ]))),
        )
        .unwrap();
    assert!(first.ready.is_empty());
    let second = gate
        .observe(
            0,
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
                2_500,
                "c".into(),
            )]))),
        )
        .unwrap();
    let released: Vec<i64> = second
        .ready
        .iter()
        .flat_map(|(batch, _)| {
            batch
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(released, vec![100, 200]);
    let third = gate
        .observe(
            0,
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
                6_000,
                "d".into(),
            )]))),
        )
        .unwrap();
    let released: Vec<i64> = third
        .ready
        .iter()
        .flat_map(|(batch, _)| {
            batch
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(released, vec![2_500]);
}

#[tokio::test]
async fn kernel_runner_applies_event_time_gate_and_preserves_delivery_acks() {
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let input = Arc::new(CountingInput {
        batches: Mutex::new(std::collections::VecDeque::from([
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
                100,
                "a".into(),
            )]))),
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
                2_500,
                "b".into(),
            )]))),
        ])),
        acknowledgements: acknowledgements.clone(),
    });
    let output = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: output.clone(),
        processor: Arc::new(PassThroughProcessor),
    };
    let mut job = spec(vec![], vec![edge("source", "sink")], 1);
    job.sources[0].time = TimeSpec {
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
    let plan = JobPlan::compile(job.clone()).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let gate = EventTimeGate::new(&job.sources[0].time, vec![1_000]).unwrap();
    let handle = crate::executor::kernel_handle::KernelJobRunner::spawn_with_cancellation(
        graph,
        vec![input],
        BTreeMap::new(),
        BTreeMap::from([(
            "source-0".to_string(),
            Arc::new(tokio::sync::Mutex::new(Some(gate))),
        )]),
        false,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    handle.watcher().await.unwrap().unwrap();

    let written = output.written.lock().unwrap();
    assert_eq!(
        written.len(),
        2,
        "held and EOS-flushed batches must both flow"
    );
    let rows: Vec<i64> = written
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(rows, vec![100, 2_500]);
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 2);
}

/// Verification (repair-kernel-review-findings task 2.5): the runtime
/// counter endpoint. The gate-level `late_event_rows` value must reach
/// `RuntimeMetrics.late_events` through the kernel's per-chain hooks, which
/// is the number the control plane surfaces. Lateness needs downstream
/// window timings, so the graph carries a tumbling window operator.
#[tokio::test]
async fn kernel_metrics_count_late_rows_end_to_end() {
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let input = Arc::new(CountingInput {
        batches: Mutex::new(std::collections::VecDeque::from([
            // Opens window [2000,3000) and advances the watermark to 2,500.
            window_batch(vec![(2_500, "a".into(), 1)], None),
            // Late for the closed [0,1000) window under the Drop policy.
            window_batch(vec![(100, "b".into(), 2)], None),
        ])),
        acknowledgements: acknowledgements.clone(),
    });
    let output = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: output.clone(),
        processor: Arc::new(PassThroughProcessor),
    };
    let mut job = spec(
        vec![OperatorSpec {
            id: "window".into(),
            kind: OperatorKind::Window,
            stateful: true,
            key_field: Some("key".into()),
            config: serde_json::json!({
                "kind": "tumbling",
                "size_ms": 1_000,
                "timestamp_field": "ts",
                "key_field": "key",
                "value_fields": ["value"],
                "trigger": "watermark",
                "trigger_interval_ms": 1_000
            }),
        }],
        vec![edge("source", "window"), edge("window", "sink")],
        1,
    );
    job.sources[0].time = TimeSpec {
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
    let plan = JobPlan::compile(job).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let handle = crate::executor::kernel_handle::KernelJobRunner::spawn_with_cancellation(
        graph,
        vec![input],
        BTreeMap::new(),
        BTreeMap::new(),
        false,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    handle.watcher().await.unwrap().unwrap();

    let metrics = handle.metrics().snapshot();
    assert_eq!(
        metrics.late_events, 1,
        "the runtime counter must observe exactly the one dropped late row"
    );
    assert_eq!(
        acknowledgements.load(Ordering::SeqCst),
        2,
        "the dropped row's ack is completed by the drop, the held row's by EOS flush"
    );
}

#[tokio::test]
async fn event_time_window_runs_in_graph_and_fires_on_watermark_then_eos() {
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let input = Arc::new(CountingInput {
        batches: Mutex::new(std::collections::VecDeque::from([
            window_batch(vec![(100, "a".into(), 1)], None),
            window_batch(vec![(2_500, "a".into(), 2)], None),
        ])),
        acknowledgements: acknowledgements.clone(),
    });
    let output = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: output.clone(),
        processor: Arc::new(PassThroughProcessor),
    };
    let mut job = spec(
        vec![OperatorSpec {
            id: "window".into(),
            kind: OperatorKind::Window,
            stateful: true,
            key_field: Some("key".into()),
            config: serde_json::json!({
                "kind": "tumbling",
                "size_ms": 1_000,
                "timestamp_field": "ts",
                "key_field": "key",
                "value_fields": ["value"],
                "trigger": "watermark",
                "trigger_interval_ms": 1_000
            }),
        }],
        vec![edge("source", "window"), edge("window", "sink")],
        1,
    );
    job.sources[0].time = TimeSpec {
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
    let plan = JobPlan::compile(job).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();

    // The plain local graph runner must derive the source event-time gate from
    // the JobSpec carried by the graph; callers should not need a separate
    // gate map just to make a watermark-triggered window work.
    run_graph(graph, CancellationToken::new()).await.unwrap();

    let written = output.written.lock().unwrap();
    assert_eq!(
        written.len(),
        2,
        "watermark and EOS should flush two windows"
    );
    let starts = written
        .iter()
        .map(|batch| {
            batch
                .column_by_name("window_start")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        })
        .collect::<Vec<_>>();
    assert_eq!(starts, vec![0, 2_000]);
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn late_route_is_a_real_side_branch_and_does_not_update_window() {
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let input = Arc::new(CountingInput {
        batches: Mutex::new(std::collections::VecDeque::from([
            window_batch(vec![(100, "a".into(), 1)], None),
            window_batch(vec![(2_000, "a".into(), 2)], None),
            window_batch(vec![(100, "a".into(), 99)], None),
        ])),
        acknowledgements: acknowledgements.clone(),
    });
    let primary = Arc::new(CollectOutput::default());
    let late = Arc::new(CollectOutput::default());
    let adapter = MultiInputAdapter {
        inputs: HashMap::from([("source".into(), input.clone() as Arc<dyn Input>)]),
        outputs: HashMap::from([
            ("sink".into(), primary.clone()),
            ("late-sink".into(), late.clone()),
        ]),
        processor: Arc::new(PassThroughProcessor),
    };
    let mut job = spec(
        vec![
            OperatorSpec {
                id: "window".into(),
                kind: OperatorKind::Window,
                stateful: true,
                key_field: Some("key".into()),
                config: serde_json::json!({
                    "kind": "tumbling",
                    "size_ms": 1_000,
                    "timestamp_field": "ts",
                    "key_field": "key",
                    "value_fields": ["value"],
                    "trigger": "watermark",
                    "trigger_interval_ms": 1_000
                }),
            },
            sink_operator("sink", false),
            sink_operator("late-sink", false),
        ],
        vec![edge("source", "window"), edge("window", "sink")],
        1,
    );
    job.sinks.push(SinkSpec {
        operator_id: "late-sink".into(),
        output_type: "collect".into(),
        config: serde_json::json!({}),
    });
    job.sources[0].time = TimeSpec {
        mode: TimeMode::EventTime,
        timestamp_field: Some("ts".into()),
        watermark: Some(WatermarkSpec {
            strategy: WatermarkStrategy::Monotonous,
            out_of_orderness_ms: 0,
            idle_timeout_ms: None,
        }),
        allowed_lateness_ms: 0,
        late_event_policy: LateEventPolicy::Route,
        late_event_route: Some("late-sink".into()),
    };
    let plan = JobPlan::compile(job).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    assert!(graph
        .chains
        .iter()
        .any(|chain| chain.late_event_outputs.contains_key("source-0")));

    run_graph(graph, CancellationToken::new()).await.unwrap();

    let primary_batches = primary.written.lock().unwrap();
    assert_eq!(primary_batches.len(), 2);
    let primary_counts = primary_batches
        .iter()
        .map(|batch| {
            batch
                .column_by_name("count")
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::UInt64Array>()
                .unwrap()
                .value(0)
        })
        .collect::<Vec<_>>();
    assert_eq!(primary_counts, vec![1, 1]);

    let late_batches = late.written.lock().unwrap();
    assert_eq!(late_batches.len(), 1);
    assert!(late_batches[0]
        .column_by_name("__arkflow_late_event_route")
        .is_some());
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn kernel_runner_checkpoint_barrier_collects_every_chain_snapshot() {
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
            let offset = self.reads.fetch_add(1, Ordering::SeqCst);
            if offset >= self.max_reads {
                return Err(Error::EOF);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok((
                Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
                    offset as i64,
                    "a".into(),
                )]))),
                Arc::new(crate::input::NoopAck),
            ))
        }

        async fn current_positions(&self) -> Result<Vec<crate::checkpoint::SourcePosition>, Error> {
            Ok(vec![crate::checkpoint::SourcePosition::for_partition(
                0,
                self.reads.load(Ordering::SeqCst) as u64,
            )])
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    let input = Arc::new(SlowInput {
        reads: AtomicUsize::new(0),
        max_reads: 100,
    });
    let output = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output,
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(spec(
        vec![map_operator("map")],
        vec![edge("source", "map"), edge("map", "sink")],
        1,
    ))
    .unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let cancellation = CancellationToken::new();
    let handle = crate::executor::kernel_handle::KernelJobRunner::spawn_with_cancellation(
        graph,
        vec![input.clone()],
        BTreeMap::new(),
        BTreeMap::new(),
        false,
        cancellation.clone(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let (snapshot, positions, watermarks) =
        tokio::time::timeout(Duration::from_secs(2), handle.checkpoint_snapshot())
            .await
            .expect("barrier checkpoint timed out")
            .unwrap();
    assert!(snapshot.verify());
    assert_eq!(snapshot.entries.len(), 0);
    assert_eq!(positions.len(), 1);
    assert!(positions[0].offset > 0);
    assert!(watermarks.is_empty());
    let metrics = handle.metrics().snapshot();
    assert_eq!(metrics.checkpoint_failures, 0);
    assert!(metrics.chains.values().any(|chain| chain.batches_in > 0));

    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle.watcher())
        .await
        .expect("kernel did not stop after cancellation")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn checkpoint_round_fails_when_source_positions_error() {
    struct FailingPositionsInput {
        reads: AtomicUsize,
        position_calls: AtomicUsize,
    }

    #[async_trait]
    impl Input for FailingPositionsInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            let offset = self.reads.fetch_add(1, Ordering::SeqCst);
            if self.position_calls.load(Ordering::SeqCst) >= 2 {
                return Err(Error::EOF);
            }
            Ok((
                Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
                    offset as i64,
                    "a".into(),
                )]))),
                Arc::new(crate::input::NoopAck),
            ))
        }

        async fn current_positions(&self) -> Result<Vec<crate::checkpoint::SourcePosition>, Error> {
            let call = self.position_calls.fetch_add(1, Ordering::SeqCst);
            if call >= 1 {
                return Err(Error::Process("position snapshot failed".into()));
            }
            Ok(vec![crate::checkpoint::SourcePosition::for_partition(
                0,
                self.reads.load(Ordering::SeqCst) as u64,
            )])
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    let input = Arc::new(FailingPositionsInput {
        reads: AtomicUsize::new(0),
        position_calls: AtomicUsize::new(0),
    });
    let output = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output,
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(spec(
        vec![map_operator("map")],
        vec![edge("source", "map"), edge("map", "sink")],
        1,
    ))
    .unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let cancellation = CancellationToken::new();
    let handle = crate::executor::kernel_handle::KernelJobRunner::spawn_with_cancellation(
        graph,
        vec![input.clone()],
        BTreeMap::new(),
        BTreeMap::new(),
        false,
        cancellation.clone(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // First round captures positions; the swap arms the failure, so the next
    // round must fail closed instead of sealing empty/stale positions.
    let first = tokio::time::timeout(Duration::from_secs(2), handle.checkpoint_snapshot())
        .await
        .expect("first checkpoint timed out");
    assert!(
        first.is_ok(),
        "first checkpoint round should succeed, got {:?}",
        first.err()
    );

    let second = tokio::time::timeout(Duration::from_secs(2), handle.checkpoint_snapshot())
        .await
        .expect("second checkpoint must resolve, not time out");
    assert!(
        second.is_err(),
        "checkpoint round with a failing position snapshot must fail, got {:?}",
        second.ok()
    );

    cancellation.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), handle.watcher()).await;
}

#[tokio::test]
async fn checkpoint_round_fails_when_chain_ends_without_reporting() {
    struct ErrorExitInput {
        reads: AtomicUsize,
    }

    #[async_trait]
    impl Input for ErrorExitInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            match self.reads.fetch_add(1, Ordering::SeqCst) {
                0 => Ok((
                    Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
                        0,
                        "a".into(),
                    )]))),
                    Arc::new(crate::input::NoopAck),
                )),
                // Fail mid-flight (NOT a clean EOF): the chain exits with an
                // error, which never reports a snapshot for the round the
                // caller injects while this read is blocked.
                1 => {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    Err(Error::Process("source exploded mid-flight".into()))
                }
                _ => Err(Error::EOF),
            }
        }

        async fn current_positions(&self) -> Result<Vec<crate::checkpoint::SourcePosition>, Error> {
            Ok(vec![crate::checkpoint::SourcePosition::for_partition(
                0,
                self.reads.load(Ordering::SeqCst) as u64,
            )])
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    let input = Arc::new(ErrorExitInput {
        reads: AtomicUsize::new(0),
    });
    let output = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output,
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(spec(
        vec![map_operator("map")],
        vec![edge("source", "map"), edge("map", "sink")],
        1,
    ))
    .unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let cancellation = CancellationToken::new();
    let handle = crate::executor::kernel_handle::KernelJobRunner::spawn_with_cancellation(
        graph,
        vec![input.clone()],
        BTreeMap::new(),
        BTreeMap::new(),
        false,
        cancellation.clone(),
    )
    .await
    .unwrap();
    // The chain is now blocked in its second read (150ms): inject the round
    // into that window so the error exit happens mid-round.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // The chain exits with an error while this round is in flight and never
    // reports a snapshot for it: the round must fail instead of sealing a
    // manifest that is missing a participant.
    let round = tokio::time::timeout(Duration::from_secs(2), handle.checkpoint_snapshot()).await;
    assert!(
        round.is_err() || round.unwrap().is_err(),
        "checkpoint round with a chain that ends without reporting must fail"
    );

    cancellation.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), handle.watcher()).await;
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
        durability: crate::job::StateDurability::Ephemeral,
        root: None,
        namespace: None,
        ttl_ms: None,
        format_version: 1,
        max_pending_transactions: None,
        max_bytes: None,
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
    assert_eq!(
        backend
            .scan("job:test-job:state:default:operator:agg:task:agg-0")
            .unwrap()
            .len(),
        1
    );
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
        durability: crate::job::StateDurability::Ephemeral,
        root: None,
        namespace: None,
        ttl_ms: None,
        format_version: 1,
        max_pending_transactions: None,
        max_bytes: None,
    });
    let plan = JobPlan::compile(job).unwrap();
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into())]])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    let result = ExecutionGraphBuilder::default().build(&plan, &adapter, &resource());
    assert!(
        matches!(result, Err(Error::Config(message)) if message.contains("stateful operator 'agg' requires a Job state backend"))
    );
}

// ---------- migrated legacy runner semantics ----------

#[test]
fn rejects_unpartitioned_source_when_job_is_parallel_migrated() {
    // A parallel Job whose source input cannot pin partitions must fail at
    // graph build (legacy SingleComputeJobRunner guard, migrated).
    struct UnpartitionedInput;
    #[async_trait]
    impl Input for UnpartitionedInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            Err(Error::Process("not readable".into()))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    struct UnpartitionedAdapter(Adapter);
    impl JobComponentAdapter for UnpartitionedAdapter {
        fn build_input(&self, _s: &SourceSpec, _r: &Resource) -> Result<Arc<dyn Input>, Error> {
            Ok(Arc::new(UnpartitionedInput))
        }
        fn build_output(&self, _s: &SinkSpec, _r: &Resource) -> Result<Arc<dyn Output>, Error> {
            self.0.build_output(_s, _r)
        }
        fn build_processor(
            &self,
            _o: &OperatorSpec,
            _r: &Resource,
        ) -> Result<Arc<dyn Processor>, Error> {
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
        durability: crate::job::StateDurability::Ephemeral,
        root: None,
        namespace: None,
        ttl_ms: None,
        format_version: 1,
        max_pending_transactions: None,
        max_bytes: None,
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
        .build_subgraph(&plan, &task_ids, &adapter, &mut resource, None);
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
        gate.observe(
            0,
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
                1_000,
                "a".into(),
            )]))),
        )
        .unwrap();
        gate.observe(
            0,
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
                1_500,
                "a".into(),
            )]))),
        )
        .unwrap()
    };
    // Watermark 2_100 closes window [1000,2000). A NEW row at 1_900 (inside
    // the closed window, before the 2_200 lateness deadline) diverges per
    // policy: Route forwards it late, Update marks it for reprocessing.
    let mut routed = EventTimeGate::new(&time(LateEventPolicy::Route), vec![1_000]).unwrap();
    routed
        .observe(
            0,
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
                2_100,
                "a".into(),
            )]))),
        )
        .unwrap();
    let decision = routed
        .observe(
            0,
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
                1_900,
                "a".into(),
            )]))),
        )
        .unwrap();
    let routed_action = decision.ready.first().map(|(_, action)| *action);

    let mut updated = EventTimeGate::new(&time(LateEventPolicy::Update), vec![1_000]).unwrap();
    updated
        .observe(
            0,
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
                2_100,
                "a".into(),
            )]))),
        )
        .unwrap();
    let decision = updated
        .observe(
            0,
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
                1_900,
                "a".into(),
            )]))),
        )
        .unwrap();
    let updated_action = decision.ready.first().map(|(_, action)| *action);

    assert_eq!(routed_action, Some(crate::event_time::WindowAction::Route));
    assert_eq!(
        updated_action,
        Some(crate::event_time::WindowAction::Update)
    );
    let _ = late;
}

// ---------- acknowledged-cut barrier race tests (task 1.3) ----------

/// A source whose reads park once its queue is empty instead of ending the
/// stream, so a test can hold post-barrier data back and release it while a
/// checkpoint barrier is in flight.
struct ParkingInput {
    queue: Mutex<std::collections::VecDeque<MessageBatchRef>>,
    arrived: tokio::sync::Notify,
}

impl ParkingInput {
    fn with(batches: Vec<MessageBatchRef>) -> Arc<Self> {
        Arc::new(Self {
            queue: Mutex::new(batches.into_iter().collect()),
            arrived: tokio::sync::Notify::new(),
        })
    }

    fn push(&self, batch: MessageBatchRef) {
        self.queue.lock().unwrap().push_back(batch);
        self.arrived.notify_one();
    }

    fn push_rows(&self, rows: Vec<(i64, String)>) {
        self.push(Arc::new(MessageBatch::new_arrow(int64_batch(rows))));
    }
}

#[async_trait]
impl Input for ParkingInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn crate::input::Ack>), Error> {
        loop {
            if let Some(batch) = self.queue.lock().unwrap().pop_front() {
                return Ok((batch, Arc::new(crate::input::NoopAck)));
            }
            self.arrived.notified().await;
        }
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// Multi-input stateful chain: post-barrier data released after the committed
/// snapshot must not leak into the checkpoint, and pre-barrier data whose
/// acknowledgements are still in flight must be drained into the sealed cut
/// (positions and state from one acknowledged set).
#[tokio::test]
async fn multi_input_barrier_seals_one_acknowledged_cut() {
    let left = ParkingInput::with(vec![Arc::new(MessageBatch::new_arrow(int64_batch(vec![
        (1, "a".into()),
    ])))]);
    let right = ParkingInput::with(vec![Arc::new(MessageBatch::new_arrow(int64_batch(vec![
        (2, "b".into()),
    ])))]);
    let output = Arc::new(CollectOutput::default());
    let adapter = MultiInputAdapter {
        inputs: HashMap::from([
            ("left-source".into(), left.clone() as Arc<dyn Input>),
            ("right-source".into(), right.clone() as Arc<dyn Input>),
        ]),
        outputs: HashMap::from([("sink".into(), output.clone())]),
        processor: Arc::new(PassThroughProcessor),
    };
    let mut job = JobSpec {
        rebalance: None,
        id: JobId::new("race-job").unwrap(),
        version: JobVersion(1),
        max_parallelism: 1,
        parallelism: 1,
        operators: vec![
            map_source_operator("left-source"),
            map_source_operator("right-source"),
            stateful_operator("agg"),
            sink_operator("sink", false),
        ],
        edges: vec![
            edge("left-source", "agg"),
            edge("right-source", "agg"),
            edge("agg", "sink"),
        ],
        sources: vec![source_spec("left-source"), source_spec("right-source")],
        sinks: vec![SinkSpec {
            operator_id: "sink".into(),
            output_type: "collect".into(),
            config: serde_json::json!({}),
        }],
        state: Some(crate::job::StateSpec {
            backend: "embedded_kv".into(),
            durability: crate::job::StateDurability::Ephemeral,
            root: None,
            namespace: None,
            ttl_ms: None,
            format_version: 1,
            max_pending_transactions: None,
            max_bytes: None,
        }),
        checkpoint: None,
        placement: crate::job::PlacementStrategy::Colocated,
        recovery: Default::default(),
    };
    job.operators[0] = map_source_operator("left-source");
    job.operators[1] = map_source_operator("right-source");
    job.operators[2] = stateful_operator("agg");
    let plan = JobPlan::compile(job).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn crate::state::StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let graph = ExecutionGraphBuilder::default()
        .with_state(backend.clone())
        .build(&plan, &adapter, &resource())
        .unwrap();
    let cancellation = CancellationToken::new();
    let states = BTreeMap::from([(
        "agg-0".to_string(),
        backend.clone() as Arc<dyn crate::state::StateBackend>,
    )]);
    let handle = crate::executor::kernel_handle::KernelJobRunner::spawn_with_cancellation(
        graph,
        vec![left.clone(), right.clone()],
        states,
        BTreeMap::new(),
        false,
        cancellation.clone(),
    )
    .await
    .unwrap();

    // Wait until both pre-barrier rows reached the sink and their
    // acknowledgements committed the keyed state.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while output.written.lock().unwrap().len() < 2 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        output.written.lock().unwrap().len() >= 2,
        "pre-barrier rows must reach the sink"
    );

    // Fire the barrier round, then immediately release post-barrier data so
    // it races the barrier through the channels. Creating the future without
    // polling lets the data land in the source queues first; the barrier is
    // injected once the future is awaited below.
    let checkpoint = handle.checkpoint_barrier("cp-race", 1);
    left.push_rows(vec![(3, "c".into())]);
    right.push_rows(vec![(4, "d".into())]);

    let (snapshot, _positions, _watermarks) =
        tokio::time::timeout(Duration::from_secs(10), checkpoint)
            .await
            .expect("barrier checkpoint timed out")
            .unwrap();

    // The committed snapshot contains exactly the acknowledged pre-barrier
    // mutations: a and b keyed counters, never the post-barrier c/d rows.
    let namespace = "job:race-job:state:default:operator:agg:task:agg-0";
    let keys = snapshot
        .entries
        .iter()
        .filter(|entry| entry.namespace == namespace)
        .map(|entry| String::from_utf8_lossy(&entry.key).into_owned())
        .collect::<Vec<_>>();
    assert!(
        keys.iter().any(|key| key.ends_with("utf8:a")),
        "acknowledged pre-barrier mutation for 'a' must be in the cut: {keys:?}"
    );
    assert!(
        keys.iter().any(|key| key.ends_with("utf8:b")),
        "acknowledged pre-barrier mutation for 'b' must be in the cut: {keys:?}"
    );
    assert!(
        !keys.iter().any(|key| key.ends_with("utf8:c")),
        "post-barrier mutation for 'c' leaked into the checkpoint cut: {keys:?}"
    );
    assert!(
        !keys.iter().any(|key| key.ends_with("utf8:d")),
        "post-barrier mutation for 'd' leaked into the checkpoint cut: {keys:?}"
    );
    assert!(snapshot.verify());

    cancellation.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.watcher())
        .await
        .expect("kernel did not stop after cancellation");
}

/// Verification 2026-09-11 (harden re-audit WARNING 2): a failing state
/// snapshot fails the checkpoint round through the failure reporter while
/// the data plane keeps flowing, the failure is counted, and the last valid
/// snapshot is preserved.
#[tokio::test]
async fn failed_state_snapshot_fails_the_round_and_data_keeps_flowing() {
    struct ToggleSnapshotBackend {
        inner: Arc<dyn crate::state::StateBackend>,
        fail: std::sync::atomic::AtomicBool,
    }
    impl crate::state::StateBackend for ToggleSnapshotBackend {
        fn format_version(&self) -> u32 {
            self.inner.format_version()
        }
        fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
            self.inner.get(namespace, key)
        }
        fn put_with_ttl(
            &self,
            namespace: &str,
            key: &[u8],
            value: &[u8],
            ttl_ms: Option<u64>,
            now_ms: u64,
        ) -> Result<(), Error> {
            self.inner
                .put_with_ttl(namespace, key, value, ttl_ms, now_ms)
        }
        fn update_i64(&self, namespace: &str, key: &[u8], delta: i64) -> Result<i64, Error> {
            self.inner.update_i64(namespace, key, delta)
        }
        fn delete(&self, namespace: &str, key: &[u8]) -> Result<bool, Error> {
            self.inner.delete(namespace, key)
        }
        fn purge_expired(&self, now_ms: u64) -> Result<u64, Error> {
            self.inner.purge_expired(now_ms)
        }
        fn scan(&self, namespace: &str) -> Result<Vec<crate::state::StateEntry>, Error> {
            self.inner.scan(namespace)
        }
        fn snapshot_at(&self, now_ms: u64) -> Result<crate::state::StateSnapshot, Error> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(Error::Process("injected state snapshot failure".into()));
            }
            self.inner.snapshot_at(now_ms)
        }
        fn restore(&self, snapshot: &crate::state::StateSnapshot) -> Result<(), Error> {
            self.inner.restore(snapshot)
        }
        fn metrics(&self) -> Result<crate::state::StateMetrics, Error> {
            self.inner.metrics()
        }
        fn close(&self) -> Result<(), Error> {
            self.inner.close()
        }
    }

    let input = ParkingInput::with(vec![Arc::new(MessageBatch::new_arrow(int64_batch(vec![
        (1, "a".into()),
    ])))]);
    let output = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: output.clone(),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(spec(vec![], vec![edge("source", "sink")], 1)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let backend = Arc::new(ToggleSnapshotBackend {
        inner: Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap()),
        fail: std::sync::atomic::AtomicBool::new(false),
    });
    let states = BTreeMap::from([(
        "source-0".to_string(),
        backend.clone() as Arc<dyn crate::state::StateBackend>,
    )]);
    let cancellation = CancellationToken::new();
    let handle = crate::executor::kernel_handle::KernelJobRunner::spawn_with_cancellation(
        graph,
        vec![input.clone()],
        states,
        BTreeMap::new(),
        false,
        cancellation.clone(),
    )
    .await
    .unwrap();

    // Round 1: a healthy snapshot — the last valid artifact recovery keeps.
    let (snapshot, _, _) = tokio::time::timeout(
        Duration::from_secs(5),
        handle.checkpoint_barrier("cp-good", 1),
    )
    .await
    .expect("first checkpoint timed out")
    .unwrap();
    assert!(snapshot.verify());

    // Round 2: the backend fails — the round must fail via the failure
    // reporter and the failure must be counted.
    backend.fail.store(true, Ordering::SeqCst);
    let failed = tokio::time::timeout(
        Duration::from_secs(5),
        handle.checkpoint_barrier("cp-bad", 2),
    )
    .await
    .expect("failed checkpoint timed out");
    assert!(
        failed.is_err(),
        "a failing state snapshot must fail the checkpoint round"
    );
    // Sanity check only: the metric increments on any round failure (the
    // `is_err` assertion above is what pins this round's outcome), so this
    // does not distinguish failure paths.
    assert_eq!(handle.metrics().snapshot().checkpoint_failures, 1);

    // The data plane kept running: a row pushed after the failed round still
    // reaches the sink.
    input.push_rows(vec![(2, "b".into())]);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let written_rows = || {
        output
            .written
            .lock()
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>()
    };
    while written_rows() < 2 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        written_rows() >= 2,
        "data must keep flowing through a failed checkpoint round"
    );

    // A later healthy round succeeds again; the failure was local to one
    // round rather than poisoning the checkpoint machinery.
    backend.fail.store(false, Ordering::SeqCst);
    let (recovered, _, _) = tokio::time::timeout(
        Duration::from_secs(5),
        handle.checkpoint_barrier("cp-good-2", 3),
    )
    .await
    .expect("recovery checkpoint timed out")
    .unwrap();
    assert!(recovered.verify());

    cancellation.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.watcher())
        .await
        .expect("kernel did not stop after cancellation");
}

fn map_source_operator(id: &str) -> OperatorSpec {
    OperatorSpec {
        id: id.into(),
        kind: OperatorKind::Source,
        stateful: false,
        key_field: None,
        config: serde_json::json!({}),
    }
}

/// A sink with a controlled write delay: its acknowledgements (and therefore
/// the journal applies inside them) stay in flight long enough to overlap a
/// checkpoint barrier.
struct SlowOutput {
    written: Mutex<Vec<RecordBatch>>,
    delay_ms: u64,
}

#[async_trait]
impl Output for SlowOutput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
        tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
        self.written
            .lock()
            .unwrap()
            .push(msg.record_batch().clone());
        Ok(())
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// Task 1.4: the checkpoint report waits for pre-cut state transactions. A
/// pre-barrier row whose sink write (and acknowledgement, and journal apply)
/// is still in flight when the barrier fires must land in the sealed cut —
/// the source drains the in-flight acknowledgement before sealing, so the
/// snapshot contains the row's committed mutation.
#[tokio::test]
async fn barrier_waits_for_pre_cut_state_transactions() {
    let source = ParkingInput::with(vec![Arc::new(MessageBatch::new_arrow(int64_batch(vec![
        (1, "a".into()),
    ])))]);
    let output = Arc::new(SlowOutput {
        written: Mutex::new(Vec::new()),
        delay_ms: 120,
    });
    struct SlowAdapter {
        input: Arc<dyn Input>,
        output: Arc<SlowOutput>,
        processor: Arc<dyn Processor>,
    }
    impl JobComponentAdapter for SlowAdapter {
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
    let adapter = SlowAdapter {
        input: source.clone(),
        output: output.clone(),
        processor: Arc::new(PassThroughProcessor),
    };
    let mut job = spec(
        vec![stateful_operator("agg")],
        vec![edge("source", "agg"), edge("agg", "sink")],
        1,
    );
    job.state = Some(crate::job::StateSpec {
        backend: "embedded_kv".into(),
        durability: crate::job::StateDurability::Ephemeral,
        root: None,
        namespace: None,
        ttl_ms: None,
        format_version: 1,
        max_pending_transactions: None,
        max_bytes: None,
    });
    let plan = JobPlan::compile(job).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn crate::state::StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let graph = ExecutionGraphBuilder::default()
        .with_state(backend.clone())
        .build(&plan, &adapter, &resource())
        .unwrap();
    let cancellation = CancellationToken::new();
    let states = BTreeMap::from([(
        "agg-0".to_string(),
        backend.clone() as Arc<dyn crate::state::StateBackend>,
    )]);
    let handle = crate::executor::kernel_handle::KernelJobRunner::spawn_with_cancellation(
        graph,
        vec![source.clone()],
        states,
        BTreeMap::new(),
        false,
        cancellation.clone(),
    )
    .await
    .unwrap();

    // Give the row time to reach the slow sink's write (in-flight, not yet
    // acknowledged).
    tokio::time::sleep(Duration::from_millis(20)).await;
    // Fire the barrier while the row's acknowledgement is still in flight.
    let started = std::time::Instant::now();
    let (snapshot, _positions, _watermarks) = tokio::time::timeout(
        Duration::from_secs(10),
        handle.checkpoint_barrier("cp-precut", 1),
    )
    .await
    .expect("barrier checkpoint timed out")
    .unwrap();
    // The source drained the in-flight acknowledgement before sealing: the
    // barrier round waited for the slow sink write.
    assert!(
        started.elapsed() >= Duration::from_millis(100),
        "barrier must wait for the pre-cut sink write (elapsed {:?})",
        started.elapsed()
    );
    let namespace = "job:test-job:state:default:operator:agg:task:agg-0";
    let committed = snapshot
        .entries
        .iter()
        .filter(|entry| entry.namespace == namespace)
        .map(|entry| String::from_utf8_lossy(&entry.key).into_owned())
        .collect::<Vec<_>>();
    assert!(
        committed.iter().any(|key| key.ends_with("utf8:a")),
        "the in-flight pre-cut mutation must be drained into the cut: {committed:?}"
    );

    cancellation.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.watcher())
        .await
        .expect("kernel did not stop after cancellation");
}

// ---------- resource guard / temporary lifecycle tests (tasks 2.1/2.2) ----------

struct CountingTemporary {
    connects: AtomicUsize,
    closes: AtomicUsize,
    connected: std::sync::atomic::AtomicBool,
}

impl CountingTemporary {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
            connected: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl crate::temporary::Temporary for CountingTemporary {
    async fn connect(&self) -> Result<(), Error> {
        self.connects.fetch_add(1, Ordering::SeqCst);
        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn get(
        &self,
        _keys: &[datafusion::logical_expr::ColumnarValue],
    ) -> Result<Option<MessageBatch>, Error> {
        // A processor `get` before `connect` is exactly the bug the guard
        // prevents; record it loudly.
        assert!(
            self.connected.load(Ordering::SeqCst),
            "temporary.get called before temporary.connect"
        );
        Ok(None)
    }
    async fn close(&self) -> Result<(), Error> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        self.connected.store(false, Ordering::SeqCst);
        Ok(())
    }
}

/// Task 2.2: temporary stores connect (dependency order) before any chain
/// reads input, and close at shutdown.
#[tokio::test]
async fn temporaries_connect_before_chains_and_close_at_shutdown() {
    let temporary = CountingTemporary::new();
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into())]])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(spec(vec![], vec![edge("source", "sink")], 1)).unwrap();
    let mut resource = resource();
    resource
        .temporary
        .insert("reference".into(), temporary.clone());
    let mut graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &mut resource)
        .unwrap();
    assert!(graph.temporaries.is_empty(), "builder starts clean");
    graph.temporaries = resource.temporary.values().cloned().collect();

    run_graph(graph, CancellationToken::new()).await.unwrap();
    assert_eq!(temporary.connects.load(Ordering::SeqCst), 1);
    assert_eq!(temporary.closes.load(Ordering::SeqCst), 1);
}

/// Task 2.1/2.2: a source or sink that cannot connect fails the graph start
/// before input consumption, with already-connected resources closed.
#[tokio::test]
async fn sink_connect_failure_fails_start_and_closes_temporaries() {
    struct FailingSink;
    #[async_trait]
    impl Output for FailingSink {
        async fn connect(&self) -> Result<(), Error> {
            Err(Error::Connection("injected sink connect failure".into()))
        }
        async fn write(&self, _msg: MessageBatchRef) -> Result<(), Error> {
            Ok(())
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    struct SinkAdapter {
        input: Arc<dyn Input>,
        sink: Arc<dyn Output>,
    }
    impl JobComponentAdapter for SinkAdapter {
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
            Ok(self.sink.clone())
        }
        fn build_processor(
            &self,
            _operator: &OperatorSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Processor>, Error> {
            Ok(Arc::new(PassThroughProcessor))
        }
    }

    let temporary = CountingTemporary::new();
    let adapter = SinkAdapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into())]])),
        sink: Arc::new(FailingSink),
    };
    let plan = JobPlan::compile(spec(vec![], vec![edge("source", "sink")], 1)).unwrap();
    let mut resource = resource();
    resource
        .temporary
        .insert("reference".into(), temporary.clone());
    let mut graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &mut resource)
        .unwrap();
    graph.temporaries = resource.temporary.values().cloned().collect();

    let result = run_graph(graph, CancellationToken::new()).await;
    assert!(result.is_err(), "sink connect failure must fail the start");
    // The temporary connected before the sink and was closed again by the
    // partial-startup cleanup (reverse order).
    assert_eq!(temporary.connects.load(Ordering::SeqCst), 1);
    assert_eq!(temporary.closes.load(Ordering::SeqCst), 1);
}

// ---------- Kafka/source partition assignment tests (tasks 3.1/3.4) ----------

#[derive(Default)]
struct AssignmentRecordingInput {
    assigned: Mutex<Vec<u32>>,
}

#[async_trait]
impl Input for AssignmentRecordingInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn crate::input::Ack>), Error> {
        Err(Error::EOF)
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
    fn assign_partition(&self, partition: u32) -> Result<(), Error> {
        self.assigned.lock().unwrap().push(partition);
        Ok(())
    }
    fn supports_partitioning(&self) -> bool {
        true
    }
}

struct AssignmentAdapter {
    input: Arc<AssignmentRecordingInput>,
}

impl JobComponentAdapter for AssignmentAdapter {
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
        Ok(Arc::new(CollectOutput::default()))
    }
    fn build_processor(
        &self,
        _operator: &OperatorSpec,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        Ok(Arc::new(PassThroughProcessor))
    }
}

/// Task 3.1: a single source task keeps the connector's all-partition
/// subscription — pinning it to physical partition 0 would silently drop
/// every other partition of a partition-capable source.
#[test]
fn single_source_task_keeps_all_partition_subscription() {
    let input = Arc::new(AssignmentRecordingInput::default());
    let adapter = AssignmentAdapter {
        input: input.clone(),
    };
    let plan = JobPlan::compile(spec(vec![], vec![edge("source", "sink")], 1)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    assert_eq!(
        graph
            .chains
            .iter()
            .filter(|chain| chain.is_source())
            .count(),
        1
    );
    assert!(
        input.assigned.lock().unwrap().is_empty(),
        "a single source task must not be pinned to a physical partition"
    );
}

/// Task 3.1/3.4: multiple physical source tasks receive explicit, stable
/// partitions.
#[test]
fn multiple_source_tasks_receive_explicit_partitions() {
    let input = Arc::new(AssignmentRecordingInput::default());
    let adapter = AssignmentAdapter {
        input: input.clone(),
    };
    let plan = JobPlan::compile(spec(vec![], vec![edge("source", "sink")], 2)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    assert_eq!(
        graph
            .chains
            .iter()
            .filter(|chain| chain.is_source())
            .count(),
        2,
        "parallel source produces one chain per physical task"
    );
    let mut assigned = input.assigned.lock().unwrap().clone();
    assigned.sort();
    assert_eq!(assigned, vec![0, 1], "each task pins its own partition");
}

/// Task 4.3: restoring a checkpointed watermark installs it for the source
/// task's ACTUAL physical partition; partition 0 must not be synthesized.
#[tokio::test]
async fn watermark_restore_uses_the_real_partition() {
    let input = Arc::new(VecInput::new(vec![vec![(1, "a".into())]]));
    let adapter = Adapter {
        input: input.clone(),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    let mut job = spec(vec![], vec![edge("source", "sink")], 1);
    job.sources[0].time = crate::job::TimeSpec {
        mode: crate::job::TimeMode::EventTime,
        timestamp_field: Some("ts".into()),
        watermark: Some(crate::job::WatermarkSpec {
            strategy: crate::job::WatermarkStrategy::Monotonous,
            out_of_orderness_ms: 0,
            idle_timeout_ms: None,
        }),
        allowed_lateness_ms: 0,
        late_event_policy: Default::default(),
        late_event_route: None,
    };
    let plan = JobPlan::compile(job).unwrap();
    // Pin the single source task to physical partition 3 by editing the
    // compiled plan's task partition, exercising the chain plumbing.
    let mut tasks = plan.tasks.clone();
    for task in &mut tasks {
        if task.operator_id == "source" && !task.partitions.is_empty() {
            task.partitions[0] = crate::job::PartitionSpec {
                id: 3,
                key_group: task.partitions[0].key_group.clone(),
            };
        }
    }
    let pinned_plan = crate::job::JobPlan {
        tasks,
        ..plan.clone()
    };
    let graph = ExecutionGraphBuilder::default()
        .build(&pinned_plan, &adapter, &resource())
        .unwrap();
    let cancellation = CancellationToken::new();
    let handle = crate::executor::kernel_handle::KernelJobRunner::spawn_with_cancellation(
        graph,
        vec![input.clone()],
        BTreeMap::new(),
        BTreeMap::new(),
        false,
        cancellation.clone(),
    )
    .await
    .unwrap();

    handle
        .restore_watermarks(&BTreeMap::from([("source-0".to_string(), 10_000_i64)]))
        .await
        .unwrap();
    let gates = handle.watermark_gates();
    let gate = gates.get("source-0").expect("event-time source has a gate");
    let guard = gate.lock().await;
    let gate = guard.as_ref().expect("gate initialized");
    assert_eq!(
        gate.partition_watermark(3),
        Some(10_000),
        "the restored watermark belongs to the real partition"
    );
    assert_eq!(
        gate.partition_watermark(0),
        None,
        "partition 0 must not be synthesized"
    );
    assert_eq!(gate.watermark(), Some(10_000));
    drop(guard);
    cancellation.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.watcher())
        .await
        .expect("kernel did not stop after cancellation");
}

// ---------- configured processor concurrency tests (task 6.2) ----------

struct SlowCountingProcessor {
    in_flight: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
}

#[async_trait]
impl Processor for SlowCountingProcessor {
    async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight
            .fetch_max(current, std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(30)).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(ProcessResult::Single(batch))
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

struct ConcurrencyAdapter {
    input: Arc<dyn Input>,
    output: Arc<CollectOutput>,
    processor: Arc<dyn Processor>,
}

impl JobComponentAdapter for ConcurrencyAdapter {
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

fn parallel_job_spec(parallelism: u64) -> JobSpec {
    let mut spec = spec(
        vec![map_operator("slow")],
        vec![edge("source", "slow"), edge("slow", "sink")],
        1,
    );
    spec.sources[0].config = serde_json::json!({
        "__arkflow_processor_parallelism": parallelism,
    });
    spec
}

/// Task 6.2: `pipeline.thread_num` compiles to bounded chain-level processor
/// worker concurrency — multiple deliveries process simultaneously, output
/// order is preserved, and the source partition topology (single task, no
/// partition pinning) is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_thread_num_runs_ordered_concurrent_processors() {
    let in_flight = Arc::new(AtomicUsize::new(0));
    let max_in_flight = Arc::new(AtomicUsize::new(0));
    let processor = Arc::new(SlowCountingProcessor {
        in_flight: in_flight.clone(),
        max_in_flight: max_in_flight.clone(),
    });
    let output = Arc::new(CollectOutput::default());
    let adapter = ConcurrencyAdapter {
        input: Arc::new(VecInput::new(
            (0..8).map(|value| vec![(value, "a".into())]).collect(),
        )),
        output: output.clone(),
        processor: processor.clone(),
    };

    let plan = JobPlan::compile(parallel_job_spec(4)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    // The processor chain carries the configured concurrency; the source
    // chain stays a single unpinned task.
    let processor_chain = graph
        .chains
        .iter()
        .find(|chain| !chain.processors.is_empty())
        .unwrap();
    assert_eq!(processor_chain.processor_parallelism, 4);

    run_graph(graph, CancellationToken::new()).await.unwrap();

    // Output order is preserved (reorder collector).
    let values: Vec<i64> = output
        .written
        .lock()
        .unwrap()
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(values, (0..8).collect::<Vec<_>>(), "ordered output");
    // Concurrency actually happened: with a 30ms processor and 8 batches,
    // more than one delivery was in flight at once.
    assert!(
        max_in_flight.load(Ordering::SeqCst) >= 2,
        "worker pool must process deliveries concurrently (max {})",
        max_in_flight.load(Ordering::SeqCst)
    );
}

/// Task 6.2 companion: `pipeline.thread_num` never changes the source
/// partition topology — the compiled stream stays a single source task with
/// its all-partition subscription.
#[test]
fn thread_num_does_not_change_source_partition_topology() {
    let stream = crate::stream::StreamConfig {
        id: Some("concurrent".into()),
        input: crate::input::InputConfig {
            input_type: "vec".into(),
            name: None,
            codec: None,
            config: None,
        },
        pipeline: crate::pipeline::PipelineConfig {
            thread_num: 8,
            processors: vec![],
        },
        output: crate::output::OutputConfig {
            output_type: "collect".into(),
            name: None,
            codec: None,
            config: None,
        },
        error_output: None,
        buffer: None,
        durability: None,
        state: None,
        temporary: None,
    };
    let spec = crate::executor::stream_compiler::compile_stream(&stream, 0).unwrap();
    let concurrency = spec
        .sources
        .iter()
        .find_map(|source| {
            source
                .config
                .get("__arkflow_processor_parallelism")
                .and_then(serde_json::Value::as_u64)
        })
        .or_else(|| {
            spec.operators.iter().find_map(|operator| {
                operator
                    .config
                    .get("__arkflow_processor_parallelism")
                    .and_then(serde_json::Value::as_u64)
            })
        });
    assert_eq!(
        concurrency,
        Some(8),
        "the compiler preserves the configured concurrency"
    );
    let plan = JobPlan::compile(spec).unwrap();
    let source_tasks = plan
        .tasks
        .iter()
        .filter(|task| task.operator_id == "source")
        .count();
    assert_eq!(source_tasks, 1, "the source stays a single task");
}

// ---------- ended-chain checkpoint exemption (repair-kernel-review-defects) ----------

struct BoundedPositionedInput {
    reads: AtomicUsize,
    max_reads: usize,
}

#[async_trait]
impl Input for BoundedPositionedInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        let offset = self.reads.fetch_add(1, Ordering::SeqCst);
        if offset >= self.max_reads {
            return Err(Error::EOF);
        }
        Ok((
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
                offset as i64,
                "bounded".into(),
            )]))),
            Arc::new(crate::input::NoopAck),
        ))
    }
    async fn current_positions(&self) -> Result<Vec<crate::checkpoint::SourcePosition>, Error> {
        Ok(vec![crate::checkpoint::SourcePosition::for_partition(
            0,
            self.reads.load(Ordering::SeqCst) as u64,
        )])
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

struct ForeverPositionedInput {
    reads: AtomicUsize,
}

#[async_trait]
impl Input for ForeverPositionedInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        let offset = self.reads.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok((
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
                offset as i64,
                "forever".into(),
            )]))),
            Arc::new(crate::input::NoopAck),
        ))
    }
    async fn current_positions(&self) -> Result<Vec<crate::checkpoint::SourcePosition>, Error> {
        Ok(vec![crate::checkpoint::SourcePosition::for_partition(
            0,
            self.reads.load(Ordering::SeqCst) as u64,
        )])
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// One bounded source plus one continuous source: once the bounded subtree
/// drains and its chain exits, barrier rounds must still complete with the
/// remaining live participants instead of parking forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_source_drain_keeps_checkpoints_running() {
    let bounded = Arc::new(BoundedPositionedInput {
        reads: AtomicUsize::new(0),
        max_reads: 2,
    });
    let forever = Arc::new(ForeverPositionedInput {
        reads: AtomicUsize::new(0),
    });
    let output_a = Arc::new(CollectOutput::default());
    let output_b = Arc::new(CollectOutput::default());
    let adapter = MultiInputAdapter {
        inputs: HashMap::from([
            ("source-a".into(), bounded.clone() as Arc<dyn Input>),
            ("source-b".into(), forever.clone() as Arc<dyn Input>),
        ]),
        outputs: HashMap::from([
            ("sink-a".into(), output_a.clone()),
            ("sink-b".into(), output_b.clone()),
        ]),
        processor: Arc::new(PassThroughProcessor),
    };
    let job = JobSpec {
        rebalance: None,
        id: JobId::new("test-job").unwrap(),
        version: JobVersion(1),
        max_parallelism: 2,
        parallelism: 1,
        operators: vec![
            OperatorSpec {
                id: "source-a".into(),
                kind: OperatorKind::Source,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
            map_operator("map-a"),
            sink_operator("sink-a", false),
            OperatorSpec {
                id: "source-b".into(),
                kind: OperatorKind::Source,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
            map_operator("map-b"),
            sink_operator("sink-b", false),
        ],
        edges: vec![
            edge("source-a", "map-a"),
            edge("map-a", "sink-a"),
            edge("source-b", "map-b"),
            edge("map-b", "sink-b"),
        ],
        sources: vec![source_spec("source-a"), source_spec("source-b")],
        sinks: vec![
            SinkSpec {
                operator_id: "sink-a".into(),
                output_type: "collect".into(),
                config: serde_json::json!({}),
            },
            SinkSpec {
                operator_id: "sink-b".into(),
                output_type: "collect".into(),
                config: serde_json::json!({}),
            },
        ],
        state: None,
        checkpoint: None,
        placement: crate::job::PlacementStrategy::Colocated,
        recovery: Default::default(),
    };
    let plan = JobPlan::compile(job).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let cancellation = CancellationToken::new();
    let handle = crate::executor::kernel_handle::KernelJobRunner::spawn_with_cancellation(
        graph,
        vec![bounded.clone(), forever.clone()],
        BTreeMap::new(),
        BTreeMap::new(),
        false,
        cancellation.clone(),
    )
    .await
    .unwrap();

    // Wait until the bounded subtree drained and its chain task returned.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if output_a.written.lock().unwrap().len() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("bounded source did not drain");
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The round must complete with only the live source reporting.
    let (snapshot, positions, _watermarks) =
        tokio::time::timeout(Duration::from_secs(2), handle.checkpoint_snapshot())
            .await
            .expect("checkpoint round hung after a participant chain ended")
            .unwrap();
    assert!(snapshot.verify());
    assert_eq!(
        positions.len(),
        1,
        "only the live source reports checkpoint positions"
    );
    assert!(positions[0].offset > 0);

    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle.watcher())
        .await
        .expect("kernel did not stop after cancellation")
        .unwrap()
        .unwrap();
}

// ---------- pooled tick ordering + fence liveness (repair-kernel-review-defects) ----------

/// Generates a marker batch on every tick while passing data through slowly,
/// so a tick racing in-flight pooled deliveries is observable at the sink.
struct TickMarkerProcessor {
    first_process_delay: Duration,
    delayed: AtomicUsize,
}

#[async_trait]
impl Processor for TickMarkerProcessor {
    async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        if self.delayed.fetch_add(1, Ordering::SeqCst) == 0 {
            tokio::time::sleep(self.first_process_delay).await;
        }
        Ok(ProcessResult::Single(batch))
    }
    async fn on_tick(&self) -> Result<ProcessResult, Error> {
        Ok(ProcessResult::Single(Arc::new(MessageBatch::new_arrow(
            int64_batch(vec![(-1, "tick".into())]),
        ))))
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// With `pipeline.thread_num > 1`, an idle tick that fires while an earlier
/// delivery is still inside the worker pool must not publish its generated
/// batch before that delivery (per-edge ordered delivery).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tick_output_does_not_overtake_in_flight_pooled_data() {
    struct GatedInput {
        reads: AtomicUsize,
    }
    #[async_trait]
    impl Input for GatedInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            let offset = self.reads.fetch_add(1, Ordering::SeqCst);
            match offset {
                0 => Ok((
                    Arc::new(MessageBatch::new_arrow(int64_batch(vec![(1, "a".into())]))),
                    Arc::new(crate::input::NoopAck),
                )),
                1 => {
                    // Keep the chain alive past the first 100ms ticks.
                    tokio::time::sleep(Duration::from_millis(600)).await;
                    Ok((
                        Arc::new(MessageBatch::new_arrow(int64_batch(vec![(2, "a".into())]))),
                        Arc::new(crate::input::NoopAck),
                    ))
                }
                _ => Err(Error::EOF),
            }
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    let processor = Arc::new(TickMarkerProcessor {
        first_process_delay: Duration::from_millis(400),
        delayed: AtomicUsize::new(0),
    });
    let output = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: Arc::new(GatedInput {
            reads: AtomicUsize::new(0),
        }),
        output: output.clone(),
        processor,
    };
    let mut job = spec(
        vec![map_operator("m")],
        vec![edge("source", "m"), edge("m", "sink")],
        1,
    );
    job.sources[0].config = serde_json::json!({ "__arkflow_processor_parallelism": 2 });
    let plan = JobPlan::compile(job).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();

    run_graph(graph, CancellationToken::new()).await.unwrap();

    let keys: Vec<String> = output
        .written
        .lock()
        .unwrap()
        .iter()
        .flat_map(|batch| {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|value| value.unwrap_or_default().to_owned())
                .collect::<Vec<_>>()
        })
        .collect();
    // The interval's FIRST tick fires immediately at chain startup and can
    // legitimately publish before the first delivery is even submitted —
    // nothing is in flight yet, so there is nothing to overtake. Every LATER
    // tick (the ones that fire inside the delivery's 400ms in-flight window)
    // must follow the delivery, otherwise per-edge ordering is broken.
    assert!(
        keys.iter().any(|key| key == "tick"),
        "tick output must reach the sink"
    );
    let leading_ticks = keys.iter().take_while(|key| key.as_str() == "tick").count();
    assert!(
        leading_ticks <= 1,
        "tick output overtook in-flight pooled data: {keys:?}"
    );
    assert_eq!(
        keys.get(leading_ticks).map(String::as_str),
        Some("a"),
        "the first data delivery must be published before any tick output that fired while it was in flight: {keys:?}"
    );
}

/// Stress the worker-pool control fence: rapid barrier rounds against a slow
/// pooled processor must all complete — no lost `notify_waiters` wakeup.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pooled_control_fences_survive_rapid_barriers() {
    struct EndlessSlowInput {
        reads: AtomicUsize,
    }
    #[async_trait]
    impl Input for EndlessSlowInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            let offset = self.reads.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok((
                Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
                    offset as i64,
                    "a".into(),
                )]))),
                Arc::new(crate::input::NoopAck),
            ))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    let input = Arc::new(EndlessSlowInput {
        reads: AtomicUsize::new(0),
    });
    let output = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: output.clone(),
        processor: Arc::new(SlowCountingProcessor {
            in_flight: Arc::new(AtomicUsize::new(0)),
            max_in_flight: Arc::new(AtomicUsize::new(0)),
        }),
    };
    let mut job = spec(
        vec![map_operator("m")],
        vec![edge("source", "m"), edge("m", "sink")],
        1,
    );
    job.sources[0].config = serde_json::json!({ "__arkflow_processor_parallelism": 4 });
    let plan = JobPlan::compile(job).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let cancellation = CancellationToken::new();
    let handle = crate::executor::kernel_handle::KernelJobRunner::spawn_with_cancellation(
        graph,
        vec![input],
        BTreeMap::new(),
        BTreeMap::new(),
        false,
        cancellation.clone(),
    )
    .await
    .unwrap();

    for round in 0..10 {
        tokio::time::timeout(
            Duration::from_secs(5),
            handle.checkpoint_barrier(format!("cp-stress-{round}"), 1),
        )
        .await
        .unwrap_or_else(|_| panic!("barrier round {round} stalled in the pool fence"))
        .unwrap();
    }

    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), handle.watcher())
        .await
        .expect("kernel did not stop after cancellation")
        .unwrap()
        .unwrap();
}

// ---------- remote edge graph tests (loopback full semantics) ----------

/// Input that yields one batch then blocks forever, so a chain stays alive
/// until an edge failure surfaces on its send path.
struct OneBatchThenPendingInput {
    delivered: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl Input for OneBatchThenPendingInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        if !self.delivered.swap(true, Ordering::SeqCst) {
            return Ok((
                Arc::new(MessageBatch::new_arrow(int64_batch(vec![(1, "a".into())]))),
                Arc::new(crate::input::NoopAck),
            ));
        }
        futures::future::pending::<()>().await;
        unreachable!("pending never resolves");
    }
    fn supports_partitioning(&self) -> bool {
        true
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

fn keyed_map_operator(id: &str) -> OperatorSpec {
    OperatorSpec {
        id: id.into(),
        kind: OperatorKind::Map,
        stateful: false,
        key_field: Some("key".into()),
        config: serde_json::json!({}),
    }
}

fn partitioned_edge(from: &str, to: &str) -> EdgeSpec {
    EdgeSpec {
        id: format!("{from}-{to}"),
        from: from.into(),
        to: to.into(),
        partitioned: true,
    }
}

fn remote_job_plan(partitioned: bool) -> crate::job::JobPlan {
    // source → (keyed) map → sink, parallelism 2; the source→map edge is
    // partitioned so remote subtasks exercise key-group routing.
    let spec = crate::job::JobSpec {
        rebalance: None,
        id: crate::job::JobId::new("remote-job").unwrap(),
        version: crate::job::JobVersion(1),
        max_parallelism: 2,
        parallelism: 2,
        operators: vec![
            OperatorSpec {
                id: "source".into(),
                kind: OperatorKind::Source,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
            keyed_map_operator("map"),
            OperatorSpec {
                id: "sink".into(),
                kind: OperatorKind::Sink,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
        ],
        edges: vec![
            if partitioned {
                partitioned_edge("source", "map")
            } else {
                edge("source", "map")
            },
            edge("map", "sink"),
        ],
        sources: vec![crate::job::SourceSpec {
            operator_id: "source".into(),
            input_type: "vec".into(),
            config: serde_json::json!({}),
            time: crate::job::TimeSpec {
                mode: crate::job::TimeMode::ProcessingTime,
                timestamp_field: None,
                watermark: None,
                allowed_lateness_ms: 0,
                late_event_policy: Default::default(),
                late_event_route: None,
            },
        }],
        sinks: vec![crate::job::SinkSpec {
            operator_id: "sink".into(),
            output_type: "collect".into(),
            config: serde_json::json!({}),
        }],
        state: None,
        checkpoint: None,
        placement: crate::job::PlacementStrategy::Colocated,
        recovery: Default::default(),
        ..spec(vec![], vec![], 2)
    };
    crate::job::JobPlan::compile(spec).unwrap()
}

fn task_nodes() -> BTreeMap<String, String> {
    [
        ("source-0", "node-a"),
        ("source-1", "node-a"),
        ("map-0", "node-b"),
        ("map-1", "node-b"),
        ("sink-0", "node-b"),
        ("sink-1", "node-b"),
    ]
    .into_iter()
    .map(|(task, node)| (task.to_string(), node.to_string()))
    .collect()
}

/// TCP-path remote tests run the production contract: a manager without
/// credentials can neither bind a listener nor open an edge.
fn authenticated_manager(node: &str) -> std::sync::Arc<crate::executor::remote::NetworkManager> {
    let credentials = crate::executor::remote::DataPlaneCredentials::new(node, "shuffle-secret")
        .expect("test credentials");
    let mut config = crate::executor::remote::NetworkManagerConfig::default();
    config.credentials = Some(credentials);
    config.registration_grace = Duration::from_millis(100);
    crate::executor::remote::NetworkManager::with_config(config).expect("valid test config")
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_graph_routes_data_across_nodes() {
    let plan = remote_job_plan(true);
    let manager_a = authenticated_manager("node-a");
    let manager_b = authenticated_manager("node-b");
    manager_a.spawn();
    manager_b.spawn();
    let port = manager_b
        .bind_tcp("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();

    let collector = Arc::new(CollectOutput::default());
    let adapter_b = Adapter {
        input: Arc::new(VecInput::new(vec![])),
        output: collector.clone(),
        processor: Arc::new(PassThroughProcessor),
    };
    let adapter_a = Adapter {
        input: Arc::new(VecInput::new(vec![
            vec![(1, "a".into()), (2, "b".into())],
            vec![(3, "a".into()), (4, "b".into())],
        ])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };

    let context_a = crate::executor::graph::RemoteEdgeContext {
        local_node: "node-a".into(),
        task_nodes: task_nodes(),
        node_addrs: BTreeMap::from([(
            "node-b".to_string(),
            format!("127.0.0.1:{port}").parse().unwrap(),
        )]),
        manager: manager_a.clone(),
        generation: 1,
    };
    let context_b = crate::executor::graph::RemoteEdgeContext {
        local_node: "node-b".into(),
        task_nodes: task_nodes(),
        node_addrs: BTreeMap::new(),
        manager: manager_b.clone(),
        generation: 1,
    };

    let graph_a = ExecutionGraphBuilder::default()
        .build_subgraph(
            &plan,
            &["source-0".to_string(), "source-1".to_string()],
            &adapter_a,
            &resource(),
            Some(&context_a),
        )
        .unwrap_or_else(|error| panic!("node A build failed: {error}"));
    let graph_b = ExecutionGraphBuilder::default()
        .build_subgraph(
            &plan,
            &[
                "map-0".to_string(),
                "map-1".to_string(),
                "sink-0".to_string(),
                "sink-1".to_string(),
            ],
            &adapter_b,
            &resource(),
            Some(&context_b),
        )
        .unwrap_or_else(|error| panic!("node B build failed: {error}"));

    let cancellation_a = CancellationToken::new();
    let cancellation_b = CancellationToken::new();
    let run_a = tokio::spawn(run_graph(graph_a, cancellation_a.clone()));
    let run_b = tokio::spawn(run_graph(graph_b, cancellation_b.clone()));

    // Every row routed by key to some remote map subtask and collected by a
    // sink; the broadcast edge duplicates each delivery to both sinks.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows: usize = collector
                .written
                .lock()
                .unwrap()
                .iter()
                .map(|batch| batch.num_rows())
                .sum();
            if rows >= 8 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("remote rows never arrived");

    cancellation_a.cancel();
    cancellation_b.cancel();
    tokio::time::timeout(Duration::from_secs(5), run_a)
        .await
        .expect("graph A did not stop")
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), run_b)
        .await
        .expect("graph B did not stop")
        .unwrap()
        .unwrap();
    manager_a.shutdown();
    manager_b.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_graph_fails_closed_when_downstream_unreachable() {
    let plan = remote_job_plan(false);
    let manager_a = authenticated_manager("node-a");
    manager_a.spawn();

    // Point node B's data plane at a closed port: connect retries exhaust and
    // the source chain's send path fails closed.
    let context_a = crate::executor::graph::RemoteEdgeContext {
        local_node: "node-a".into(),
        task_nodes: task_nodes(),
        node_addrs: BTreeMap::from([(
            "node-b".to_string(),
            "127.0.0.1:1".parse().unwrap(),
        )]),
        manager: manager_a.clone(),
        generation: 1,
    };
    let adapter_a = Adapter {
        input: Arc::new(OneBatchThenPendingInput {
            delivered: std::sync::atomic::AtomicBool::new(false),
        }),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    let graph_a = ExecutionGraphBuilder::default()
        .build_subgraph(
            &plan,
            &["source-0".to_string(), "source-1".to_string()],
            &adapter_a,
            &resource(),
            Some(&context_a),
        )
        .unwrap_or_else(|error| panic!("node A build failed: {error}"));

    let cancellation = CancellationToken::new();
    let runner = tokio::spawn(run_graph(graph_a, cancellation.clone()));
    let result = tokio::time::timeout(Duration::from_secs(15), runner)
        .await
        .expect("graph kept running despite a dead remote edge")
        .unwrap();
    assert!(result.is_err(), "expected the source chain to fail closed");
    cancellation.cancel();
    manager_a.shutdown();
}

#[test]
fn remote_graph_rejects_incomplete_side_edge_assignment() {
    let mut job = spec(
        vec![map_operator("map")],
        vec![edge("source", "map"), edge("map", "sink")],
        1,
    );
    job.operators.push(sink_operator("late_sink", false));
    job.sources[0].time.late_event_route = Some("late_sink".into());
    let plan = JobPlan::compile(job).unwrap();
    let manager = crate::executor::remote::NetworkManager::new(16);
    let context = crate::executor::graph::RemoteEdgeContext {
        local_node: "node-a".into(),
        // Deliberately omit late_sink-0: an Agent must not defer this failure
        // until the first late event is emitted.
        task_nodes: BTreeMap::from([
            ("source-0".to_string(), "node-a".to_string()),
            ("map-0".to_string(), "node-a".to_string()),
            ("sink-0".to_string(), "node-a".to_string()),
        ]),
        node_addrs: BTreeMap::new(),
        manager,
        generation: 1,
    };
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    let error = ExecutionGraphBuilder::default()
        .build_subgraph(
            &plan,
            &["source-0".into(), "map-0".into(), "sink-0".into()],
            &adapter,
            &resource(),
            Some(&context),
        )
        .err()
        .expect("incomplete side-edge assignment must fail graph construction");
    assert!(
        error.to_string().contains("late-event side edge")
            && error.to_string().contains("late_sink-0"),
        "expected an actionable incomplete side-edge error, got {error}"
    );
}

// ---------- openspec verification round: scenario coverage ----------

/// Scenario "多输入顶点跨远程边对齐" + "控制元素到达全部副本" at graph level:
/// every map chain has TWO remote inputs (source-0/source-1 quads); barriers
/// injected staggered across those inputs must align inside the chain (buffer
/// the lagging input's pre-barrier data), snapshot, and release — exactly the
/// `Aligner` contract, exercised through real remote edges. Barriers reach
/// BOTH map subtasks (all replicas of the partitioned edge).
#[tokio::test(flavor = "multi_thread")]
async fn remote_barriers_align_across_remote_inputs_and_reach_all_replicas() {
    let plan = remote_job_plan(true);
    let manager_a = authenticated_manager("node-a");
    let manager_b = authenticated_manager("node-b");
    manager_a.spawn();
    manager_b.spawn();
    let port = manager_b
        .bind_tcp("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();

    let collector = Arc::new(CollectOutput::default());
    let adapter_b = Adapter {
        input: Arc::new(VecInput::new(vec![])),
        output: collector.clone(),
        processor: Arc::new(PassThroughProcessor),
    };
    let context_b = crate::executor::graph::RemoteEdgeContext {
        local_node: "node-b".into(),
        task_nodes: task_nodes(),
        node_addrs: BTreeMap::new(),
        manager: manager_b.clone(),
        generation: 1,
    };
    let graph_b = ExecutionGraphBuilder::default()
        .build_subgraph(
            &plan,
            &[
                "map-0".to_string(),
                "map-1".to_string(),
                "sink-0".to_string(),
                "sink-1".to_string(),
            ],
            &adapter_b,
            &resource(),
            Some(&context_b),
        )
        .unwrap_or_else(|error| panic!("node B build failed: {error}"));

    let (snapshot_tx, mut snapshot_rx) =
        tokio::sync::mpsc::unbounded_channel::<crate::executor::barrier::ChainSnapshot>();
    let mut hooks = BTreeMap::new();
    for entry in ["map-0", "map-1"] {
        hooks.insert(
            entry.to_string(),
            crate::executor::task::CheckpointHook {
                reporter: Some(snapshot_tx.clone()),
                task_id: Some(entry.to_string()),
                ..Default::default()
            },
        );
    }

    let cancellation = CancellationToken::new();
    let runner = tokio::spawn(run_graph_with_hooks(
        graph_b,
        cancellation.clone(),
        hooks,
    ));

    // One edge per (source subtask → map subtask) quad, over real TCP.
    let transport = || {
        std::sync::Arc::new(crate::executor::remote::TcpEdgeTransport {
            addr: format!("127.0.0.1:{port}").parse().unwrap(),
            max_attempts: 5,
        }) as std::sync::Arc<dyn crate::executor::remote::EdgeTransport>
    };
    // Derive quad routing ids the same way the graph builder does — the
    // spec's `operators` list also carries source/sink entries, so positions
    // are not the obvious 0/1.
    let routing = crate::executor::graph::operator_routing_index(&plan);
    let source_op = routing["source"];
    let map_op = routing["map"];
    let quad = move |src: u32, dst: u32| crate::executor::remote::Quad {
        src_op: source_op,
        src_subtask: src,
        dst_op: map_op,
        dst_subtask: dst,
    };
    let open_edge = |src: u32, dst: u32| {
        manager_a.open_edge_deferred_for_session(
            transport(),
            quad(src, dst),
            "node-b".into(),
            plan.spec.id.to_string(),
            1,
        )
        .expect("authenticated edge")
    };
    let edges = [open_edge(0, 0), open_edge(1, 0), open_edge(0, 1), open_edge(1, 1)];

    let barrier = |checkpoint: &str| {
        Envelope::Barrier(CheckpointBarrier {
            checkpoint_id: checkpoint.into(),
            generation: 3,
        })
    };
    let data = |value: i64, key: &str| {
        Envelope::Data(
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![(value, key.into())]))),
            Arc::new(crate::input::NoopAck),
        )
    };

    // Stagger: map-0's first input gets its barrier early, its second input
    // delivers pre-barrier data first — the aligner must hold that data until
    // the second barrier lands. map-1 receives everything after.
    edges[0].sender.send_async(data(1, "a")).await.unwrap();
    edges[0].sender.send_async(barrier("c-9")).await.unwrap();
    edges[1].sender.send_async(data(2, "b")).await.unwrap();
    edges[1].sender.send_async(data(3, "a")).await.unwrap();
    edges[1].sender.send_async(barrier("c-9")).await.unwrap();
    edges[2].sender.send_async(data(4, "b")).await.unwrap();
    edges[2].sender.send_async(barrier("c-9")).await.unwrap();
    edges[3].sender.send_async(data(5, "a")).await.unwrap();
    edges[3].sender.send_async(barrier("c-9")).await.unwrap();

    // Both map replicas report the aligned snapshot for c-9.
    let mut reported = BTreeMap::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while reported.len() < 2 {
        let snapshot = match tokio::time::timeout_at(deadline, snapshot_rx.recv()).await {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => panic!("reporter channel closed early"),
            Err(_) => panic!("snapshot within timeout"),
        };
        assert_eq!(snapshot.barrier.checkpoint_id, "c-9");
        reported.insert(snapshot.task_id.clone(), snapshot.barrier.generation);
    }
    assert_eq!(
        reported.keys().collect::<Vec<_>>(),
        vec!["map-0", "map-1"],
        "both replicas of the partitioned edge observed the barrier"
    );

    // All five rows survive alignment and land in both sinks (broadcast).
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows: usize = collector
                .written
                .lock()
                .unwrap()
                .iter()
                .map(|batch| batch.num_rows())
                .sum();
            if rows >= 10 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("aligned rows never arrived");

    // End the job the production way: the peer stops and forwards Eos on
    // every quad. Chains observe Eos on ALL remote inputs and finish cleanly
    // (also pins Eos propagation across remote edges); the cancellation drain
    // path, by contrast, waits for channel closure and only completes once
    // the peer's Eos or connection death closes the inbound senders — a
    // stop-ordering property recorded in the design's as-built notes.
    for edge in &edges {
        edge.sender.send_async(Envelope::Eos).await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .expect("graph did not finish after Eos")
        .unwrap()
        .unwrap();
    manager_a.shutdown();
    manager_b.shutdown();
}

/// Scenario "下游处理失败靠超时发现": a processing failure drops the delivered
/// envelope's acknowledgement without ack or abort (the kernel's error path).
/// The upstream fan-out branch must stay pending — no spurious completion, no
/// immediate failure; discovery belongs to the source chain's barrier drain
/// timeout, whose fail-closed behavior the kernel's local-ack tests already
/// pin (the remote branch is just another `Arc<dyn Ack>` to that machinery).
#[tokio::test(flavor = "multi_thread")]
async fn downstream_processing_failure_keeps_upstream_branch_pending() {
    let quad = crate::executor::remote::Quad {
        src_op: 0,
        src_subtask: 0,
        dst_op: 1,
        dst_subtask: 0,
    };
    let upstream = crate::executor::remote::NetworkManager::new(64);
    let downstream = crate::executor::remote::NetworkManager::new(64);
    upstream.spawn();
    downstream.spawn();

    let (input_tx, input_rx) = flume::bounded::<Envelope>(64);
    downstream.register_inbound(quad, input_tx);
    let (client_side, server_side) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(server_side));
    let edge = upstream.open_edge_with_stream(Box::new(client_side), quad);

    // A counting ack so "pending" is observable.
    #[derive(Default)]
    struct NeverAck {
        acked: std::sync::atomic::AtomicBool,
    }
    #[async_trait]
    impl Ack for NeverAck {
        async fn ack(&self) -> Result<(), Error> {
            self.acked.store(true, Ordering::SeqCst);
            Ok(())
        }
    }
    let branch = Arc::new(NeverAck::default());
    edge.sender
        .send_async(Envelope::Data(
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![(1, "a".into())]))),
            branch.clone(),
        ))
        .await
        .expect("send");

    let received = tokio::time::timeout(Duration::from_secs(5), input_rx.recv_async())
        .await
        .expect("delivery within timeout")
        .expect("channel open");
    let Envelope::Data(_batch, dropped_ack) = received else {
        panic!("expected data");
    };
    // Processing failure: the envelope is consumed and its ack is dropped
    // without ack() or abort().
    drop(dropped_ack);

    // The branch stays pending: no receipt will ever arrive for it.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        !branch.acked.load(Ordering::SeqCst),
        "a dropped (failed) downstream acknowledgement must never complete the upstream branch"
    );
    // And nothing failed on the manager yet — discovery is drain-timeout
    // domain, not edge-failure domain.
    assert!(
        upstream
            .failure_receiver()
            .try_recv()
            .err()
            .is_some_and(|error| matches!(error, flume::TryRecvError::Empty)),
        "no edge failure may be reported for a processing-level failure"
    );

    upstream.shutdown();
    downstream.shutdown();
}
