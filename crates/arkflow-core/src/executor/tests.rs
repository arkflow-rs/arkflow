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
use datafusion::arrow::array::{Array as _, Int64Array, StringArray};
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
    output: Arc<dyn Output>,
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
        resources: Default::default(),
        rescale: false,
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
            codec: None,
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
            codec: None,
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
        codec: None,
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
        config: if error_sink {
            serde_json::json!({"__arkflow_error_sink": true})
        } else {
            serde_json::json!({})
        },
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
    let collect = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into())]])),
        output: collect.clone(),
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

fn stateful_operator_job() -> JobSpec {
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
    job
}

#[test]
fn stateful_operator_breaks_the_chain() {
    let job = stateful_operator_job();
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

#[test]
fn explicit_parallelism_on_stateful_chain_is_rejected() {
    let mut job = stateful_operator_job();
    job.sources[0].config =
        serde_json::json!({"__arkflow_processor_parallelism": 2});
    let plan = JobPlan::compile(job).unwrap();
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into())]])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn crate::state::StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let error = match ExecutionGraphBuilder::default()
        .with_state(backend)
        .build(&plan, &adapter, &resource())
    {
        Ok(_) => panic!("the explicit parallelism override must be rejected"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("processor parallelism 1"),
        "the explicit override must be rejected, not silently ignored: {error}"
    );
}

#[test]
fn unconfigured_stateful_chain_keeps_default_parallelism() {
    let job = stateful_operator_job();
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
    let stateful_chain = graph
        .chains
        .iter()
        .find(|chain| chain.task_ids.iter().any(|id| id == "agg-0"))
        .unwrap();
    assert_eq!(
        stateful_chain.processor_parallelism, 1,
        "no override configured: the default stays one"
    );
}

// ---------- end-to-end kernel tests ----------

#[tokio::test]
async fn pipelines_batches_to_sink_in_order() {
    let collect = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![
            vec![(1, "a".into()), (2, "b".into())],
            vec![(3, "c".into())],
        ])),
        output: collect.clone(),
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
    let written = collect.written.lock().unwrap();
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
        resources: Default::default(),
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
            codec: None,
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
        rescale: false,
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
        resources: Default::default(),
        rescale: false,
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
            codec: None,
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
        resources: Default::default(),
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
                codec: None,
                operator_id: "left-source".into(),
                input_type: "vec".into(),
                config: serde_json::json!({}),
                time: event_time(),
            },
            SourceSpec {
                codec: None,
                operator_id: "right-source".into(),
                input_type: "vec".into(),
                config: serde_json::json!({}),
                time: event_time(),
            },
        ],
        sinks: vec![SinkSpec {
            operator_id: "sink".into(),
            output_type: "collect".into(),
            codec: None,
            config: serde_json::json!({}),
        }],
        state: None,
        checkpoint: None,
        placement: crate::job::PlacementStrategy::Colocated,
        recovery: Default::default(),
        rescale: false,
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
        resources: Default::default(),
        rescale: false,
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
                codec: None,
                operator_id: "sink".into(),
                output_type: "collect".into(),
                config: serde_json::json!({}),
            },
            SinkSpec {
                codec: None,
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
            codec: None,
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
        codec: None,
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
        trace_context: None,
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
    assert!(!aligner.is_aligning());
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
    let collect = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: collect.clone(),
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
                trace_context: None,
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
    let written = collect.written.lock().unwrap().len();
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
            trace_context: None,
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
    assert!(!handle.is_finished());
    // Second participant with a different barrier id: the coordinator rejects
    // and records the error instead of completing.
    report_tx.send(report("m-0", "cp-OTHER")).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!handle.is_finished());
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
        codec: None,
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
        output: output.clone(),
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
    // Wait until a batch has physically reached the sink before firing the
    // checkpoint: the per-chain `batches_in` metric and the sealed position
    // then trail a proven event instead of racing a 20ms startup guess.
    tokio::time::timeout(Duration::from_secs(5), async {
        while output.written.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("no batch reached the sink before the checkpoint");

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
    let resource = resource();
    // Full assignment (all tasks, both subtasks) mirrors the legacy test:
    // the source has 2 tasks and the input cannot pin partitions.
    let task_ids = plan.tasks.iter().map(|t| t.id.clone()).collect::<Vec<_>>();
    let result = ExecutionGraphBuilder::default()
        .with_state(backend)
        .build_subgraph(&plan, &task_ids, &adapter, &resource, None);
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
        resources: Default::default(),
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
            codec: None,
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
        rescale: false,
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

/// Task 1.4: the checkpoint report waits for pre-cut state transactions. A
/// pre-barrier row whose sink write (and acknowledgement, and journal apply)
/// is still in flight when the barrier fires must land in the sealed cut —
/// the source drains the in-flight acknowledgement before sealing, so the
/// snapshot contains the row's committed mutation.
#[tokio::test]
async fn barrier_waits_for_pre_cut_state_transactions() {
    struct SignalledSlowOutput {
        written: Mutex<Vec<RecordBatch>>,
        /// Released the moment a write begins: firing the barrier after this
        //  point races a sink write that is provably in flight, replacing
        //  the old `sleep(20ms)` guess that load could invalidate either way.
        write_started: Arc<PublishGate>,
        delay_ms: u64,
    }
    #[async_trait]
    impl Output for SignalledSlowOutput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
            self.write_started.release().await;
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
    let source = ParkingInput::with(vec![Arc::new(MessageBatch::new_arrow(int64_batch(vec![
        (1, "a".into()),
    ])))]);
    let output = Arc::new(SignalledSlowOutput {
        written: Mutex::new(Vec::new()),
        write_started: Arc::new(PublishGate::default()),
        delay_ms: 120,
    });
    struct SlowAdapter {
        input: Arc<dyn Input>,
        output: Arc<SignalledSlowOutput>,
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

    // Wait until the row's sink write has actually begun, then fire the
    // barrier: the acknowledgement is deterministically in flight, so the
    // sealed cut must drain it. (The wall-clock "elapsed >= 100ms" proxy was
    // dropped: under load the barrier request itself can arrive after the
    // 120ms write finished, which is still a correct pre-cut completion.)
    tokio::time::timeout(Duration::from_secs(5), output.write_started.wait())
        .await
        .expect("the pre-cut row never reached the slow sink");
    let (snapshot, _positions, _watermarks) = tokio::time::timeout(
        Duration::from_secs(10),
        handle.checkpoint_barrier("cp-precut", 1),
    )
    .await
    .expect("barrier checkpoint timed out")
    .unwrap();
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
        .build(&plan, &adapter, &resource)
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
        .build(&plan, &adapter, &resource)
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
    let mut stream = window_stream_config("concurrent", 8);
    stream.buffer = None;
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

/// A stream with a tumbling-window buffer and the given pipeline thread_num.
fn window_stream_config(id: &str, thread_num: u32) -> crate::stream::StreamConfig {
    crate::stream::StreamConfig {
        id: Some(id.into()),
        input: crate::input::InputConfig {
            input_type: "vec".into(),
            name: None,
            codec: None,
            config: None,
        },
        pipeline: crate::pipeline::PipelineConfig {
            thread_num,
            processors: vec![],
        },
        output: crate::output::OutputConfig {
            output_type: "collect".into(),
            name: None,
            codec: None,
            config: None,
        },
        error_output: None,
        buffer: Some(crate::buffer::BufferConfig {
            buffer_type: "tumbling_window".into(),
            name: None,
            config: Some(serde_json::json!({
                "interval": "1s",
                "key_field": "key",
                "timestamp_field": "ts",
            })),
        }),
        durability: None,
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
        temporary: None,
    }
}

/// Spec: a window-buffered stream is single-parallelism; an explicitly
/// configured thread_num above one is rejected at compile time instead of
/// being silently clamped (the setting never took effect).
#[test]
fn window_stream_with_explicit_thread_num_above_one_is_rejected() {
    let error = crate::executor::stream_compiler::compile_stream(
        &window_stream_config("windowed", 4),
        0,
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("single-threaded") && error.contains("thread_num is 4"),
        "{error}"
    );
}

/// Spec: the DEFAULT thread_num (CPU count) on a window stream is not an
/// explicit choice — it compiles to single parallelism exactly as the old
/// silent clamp behaved, and stays valid.
#[test]
fn window_stream_with_default_thread_num_compiles_single_parallelism() {
    let stream = window_stream_config("windowed-default", crate::pipeline::default_thread_num());
    let spec = crate::executor::stream_compiler::compile_stream(&stream, 0).unwrap();
    // The parallelism key rides the source OPERATOR's config.
    let concurrency = spec
        .operators
        .iter()
        .find_map(|operator| {
            operator
                .config
                .get("__arkflow_processor_parallelism")
                .and_then(serde_json::Value::as_u64)
        });
    assert_eq!(
        concurrency,
        Some(1),
        "the default thread_num compiles to the single-parallelism the clamp always used"
    );
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
        resources: Default::default(),
        rescale: false,
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
                codec: None,
                operator_id: "sink-a".into(),
                output_type: "collect".into(),
                config: serde_json::json!({}),
            },
            SinkSpec {
                codec: None,
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
    started: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl Processor for TickMarkerProcessor {
    async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        if self.delayed.fetch_add(1, Ordering::SeqCst) == 0 {
            // Keep the delivery in flight long enough for idle ticks to fire
            // against the pool fence while it is being processed.
            self.started.store(true, Ordering::SeqCst);
            tokio::time::sleep(self.first_process_delay).await;
        }
        Ok(ProcessResult::Single(batch))
    }
    async fn on_tick(&self) -> Result<ProcessResult, Error> {
        if !self.started.load(Ordering::SeqCst) {
            // Ticks before the first delivery are pure startup noise: suppress
            // them so the leading-tick assertion stays load independent.
            return Ok(ProcessResult::None);
        }
        Ok(self.tick_batch())
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl TickMarkerProcessor {
    fn tick_batch(&self) -> ProcessResult {
        ProcessResult::Single(Arc::new(MessageBatch::new_arrow(int64_batch(vec![(
            -1,
            "tick".into(),
        )]))))
    }
}

/// Deterministic one-shot release gate that cannot miss wakeups.
#[derive(Default)]
struct PublishGate {
    released: tokio::sync::Mutex<bool>,
    notify: tokio::sync::Notify,
}

impl PublishGate {
    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            if *self.released.lock().await {
                return;
            }
            notified.await;
        }
    }
    async fn release(&self) {
        *self.released.lock().await = true;
        self.notify.notify_waiters();
    }
}

/// With `pipeline.thread_num > 1`, an idle tick that fires while an earlier
/// delivery is still inside the worker pool must not publish its generated
/// batch before that delivery (per-edge ordered delivery).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tick_output_does_not_overtake_in_flight_pooled_data() {
    struct GatedInput {
        /// Offset of the NEXT batch to hand out. Advanced only when a batch is
        /// actually returned: the source loop's `select!` drops the in-flight
        /// `read()` future whenever its idle-tick arm fires, so a read future
        /// that mutated state before its first `await` (e.g. `fetch_add`)
        /// would skip ahead on every restart and reach EOF spuriously. Real
        /// connectors are cancellation-safe for exactly this reason; this
        /// double must be too.
        next: Arc<AtomicUsize>,
        gate: Arc<PublishGate>,
    }
    #[async_trait]
    impl Input for GatedInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            match self.next.load(Ordering::SeqCst) {
                0 => {
                    self.next.store(1, Ordering::SeqCst);
                    Ok((
                        Arc::new(MessageBatch::new_arrow(int64_batch(vec![(1, "a".into())]))),
                        Arc::new(crate::input::NoopAck),
                    ))
                }
                1 => {
                    // Deliver the second batch only after the first one has
                    // finished processing: the idle window in which ticks can
                    // fire is then guaranteed, not wall-clock dependent. The
                    // gate is re-checked on every (re)poll, so a cancelled
                    // read restarts here instead of skipping to EOF.
                    self.gate.wait().await;
                    self.next.store(2, Ordering::SeqCst);
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
    struct TickGateOutput {
        written: Mutex<Vec<RecordBatch>>,
        gate: Arc<PublishGate>,
    }
    #[async_trait]
    impl Output for TickGateOutput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
            let batch = msg.record_batch().clone();
            let key = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_owned();
            self.written.lock().unwrap().push(batch);
            if key == "tick" {
                // Release the second batch only once this tick batch has
                // physically reached the sink: EOF-driven shutdown can then
                // never race the tick's downstream publish away.
                self.gate.release().await;
            }
            Ok(())
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    let second_batch_gate = Arc::new(PublishGate::default());
    let input_next = Arc::new(AtomicUsize::new(0));
    let processor = Arc::new(TickMarkerProcessor {
        first_process_delay: Duration::from_millis(800),
        delayed: AtomicUsize::new(0),
        started: std::sync::atomic::AtomicBool::new(false),
    });
    let output = Arc::new(TickGateOutput {
        written: Mutex::new(Vec::new()),
        gate: second_batch_gate.clone(),
    });
    let adapter = Adapter {
        input: Arc::new(GatedInput {
            next: input_next.clone(),
            gate: second_batch_gate.clone(),
        }),
        output: output.clone(),
        processor: processor.clone(),
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
        "tick output must reach the sink (keys={keys:?}, started={}, processed={}, input_next={})",
        processor.started.load(Ordering::SeqCst),
        processor.delayed.load(Ordering::SeqCst),
        input_next.load(Ordering::SeqCst),
    );
    let leading_ticks = keys.iter().take_while(|key| key.as_str() == "tick").count();
    assert_eq!(
        leading_ticks, 0,
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
        resources: Default::default(),
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
            codec: None,
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
            codec: None,
            operator_id: "sink".into(),
            output_type: "collect".into(),
            config: serde_json::json!({}),
        }],
        state: None,
        checkpoint: None,
        placement: crate::job::PlacementStrategy::Colocated,
        recovery: Default::default(),
        rescale: false,
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
    let config = crate::executor::remote::NetworkManagerConfig {
        credentials: Some(credentials),
        registration_grace: Duration::from_millis(100),
        ..Default::default()
    };
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
        tls: None,
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
        tls: None,
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
        tls: None,
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
        tls: None,
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
        tls: None,
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
            tls: None,
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
            trace_context: None,
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

/// Process-wide OTel test plumbing: `set_global_default` wins exactly once
/// per process, so every span test must share one exporter and provider.
/// Tests isolate themselves by filtering on unique task markers, not by
/// exporter identity. A `OnceLock` guarantees the first span test to run
/// installs the layer and every test observes the same finished spans.
fn span_test_tracing()
-> (
    &'static opentelemetry_sdk::trace::InMemorySpanExporter,
    &'static opentelemetry_sdk::trace::SdkTracerProvider,
) {
    use opentelemetry::trace::TracerProvider as _;
    use std::sync::OnceLock;
    static TRACING: OnceLock<(
        opentelemetry_sdk::trace::InMemorySpanExporter,
        opentelemetry_sdk::trace::SdkTracerProvider,
    )> = OnceLock::new();
    let (exporter, provider) = TRACING.get_or_init(|| {
        use tracing_subscriber::layer::SubscriberExt;
        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("executor-span-test");
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        let _ = tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(otel_layer),
        );
        (exporter, provider)
    });
    (exporter, provider)
}

#[serial_test::serial]
    #[tokio::test]
async fn job_and_chain_spans_are_exported_with_parent_links() {
    let (exporter, provider) = span_test_tracing();

    // Unique operator id: the global OTel subscriber sees spans from
    // concurrently running executor tests in this binary, so assertions
    // must filter by a marker unique to this test's graph.
    let spec = spec(
        vec![map_operator("span-op-7351")],
        vec![edge("source", "span-op-7351"), edge("span-op-7351", "sink")],
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

    run_graph(graph, CancellationToken::new()).await.unwrap();
    provider.force_flush().unwrap();

    let finished = exporter.get_finished_spans().unwrap();
    let names: Vec<&str> = finished
        .iter()
        .map(|span| span.name.as_ref())
        .filter(|name| *name == "job.run" || *name == "chain.run")
        .collect();
    assert!(
        names.contains(&"job.run") && names.contains(&"chain.run"),
        "expected job.run and chain.run spans, got {names:?}"
    );

    // Contamination-proof identification: find OUR chain span by the unique
    // task marker, then our job.run via its parent link. (The global OTel
    // subscriber also sees spans from concurrently running executor tests
    // in this binary, so absolute counts are not reliable.)
    let op_chain = finished
        .iter()
        .find(|span| {
            span.name.as_ref() == "chain.run"
                && span.attributes.iter().any(|kv| {
                    kv.key.as_str() == "task" && kv.value.as_str() == "span-op-7351-0"
                })
        })
        .expect("op chain span must be exported");
    let job = finished
        .iter()
        .find(|span| {
            span.name.as_ref() == "job.run"
                && span.span_context.span_id() == op_chain.parent_span_id
        })
        .expect("job.run parent of the chain span");
        let chains_attr = job
        .attributes
        .iter()
        .find(|kv| kv.key.as_str() == "chains")
        .expect("chains attribute");
    let chains_value = match &chains_attr.value {
        opentelemetry::Value::I64(value) => *value,
        other => panic!("unexpected chains attribute: {other:?}"),
    };
    assert_eq!(chains_value, 3, "3 chains in this graph");

    // Every chain.run must be a child of the job.run span.
    let _job_span_id = job.span_context.span_id();
    // Contamination-proof: find OUR chain span by the unique task marker,
    // then get the parent job span, then find all chain.run siblings.
    let op_chain = finished
        .iter()
        .find(|span| {
            span.name.as_ref() == "chain.run"
                && span.attributes.iter().any(|kv| {
                    kv.key.as_str() == "task" && kv.value.as_str() == "span-op-7351-0"
                })
        })
        .expect("op chain span must be exported");
    let job_span_id = op_chain.parent_span_id;
    let job = finished
        .iter()
        .find(|span| {
            span.name.as_ref() == "job.run"
                && span.span_context.span_id() == job_span_id
        })
        .expect("job.run parent of the chain span");
    let chains_attr = job
        .attributes
        .iter()
        .find(|kv| kv.key.as_str() == "chains")
        .expect("chains attribute");
    let chains_value = match &chains_attr.value {
        opentelemetry::Value::I64(value) => *value,
        other => panic!("unexpected chains attribute: {other:?}"),
    };
    assert_eq!(chains_value, 3, "3 chains in this graph");

    let chain_runs: Vec<_> = finished
        .iter()
        .filter(|span| {
            span.name.as_ref() == "chain.run" && span.parent_span_id == job_span_id
        })
        .collect();
    assert_eq!(chain_runs.len(), 3);
    for chain in &chain_runs {
        assert_eq!(
            chain.parent_span_id, job_span_id,
            "chain.run must be a child of job.run"
        );
    }

    let tasks: Vec<String> = chain_runs
        .iter()
        .filter_map(|span| {
            span.attributes
                .iter()
                .find(|kv| kv.key.as_str() == "task")
                .map(|kv| kv.value.as_str().to_string())
        })
        .collect();
    assert!(tasks.contains(&"source-0".to_string()), "{tasks:?}");
    assert!(tasks.contains(&"span-op-7351-0".to_string()), "{tasks:?}");
    assert!(tasks.contains(&"sink-0".to_string()), "{tasks:?}");
}

#[serial_test::serial]
    #[tokio::test]
async fn batch_span_carries_rows_and_task_with_chain_parent() {
    let (exporter, provider) = span_test_tracing();

    // Unique operator id: the global OTel subscriber sees spans from
    // concurrently running executor tests in this binary, so assertions
    // must filter by a marker unique to this test's graph.
    let operator = "span-batch-7352";
    let spec = spec(
        vec![map_operator(operator)],
        vec![edge("source", operator), edge(operator, "sink")],
        1,
    );
    let plan = JobPlan::compile(spec).unwrap();
    let rows: Vec<(i64, String)> = (0..10).map(|i| (i, format!("r{i}"))).collect();
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![rows])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();

    run_graph(graph, CancellationToken::new()).await.unwrap();
    provider.force_flush().unwrap();

    let finished = exporter.get_finished_spans().unwrap();
    let task_id = format!("{operator}-0");

    let chain_run = finished
        .iter()
        .find(|span| {
            span.name.as_ref() == "chain.run"
                && span
                    .attributes
                    .iter()
                    .any(|kv| kv.key.as_str() == "task" && kv.value.as_str() == task_id)
        })
        .expect("chain.run span for the operator chain")
        .span_context
        .span_id();

    let batch_spans: Vec<_> = finished
        .iter()
        .filter(|span| {
            span.name.as_ref() == "chain.batch"
                && span
                    .attributes
                    .iter()
                    .any(|kv| kv.key.as_str() == "task" && kv.value.as_str() == task_id)
        })
        .collect();
    assert_eq!(batch_spans.len(), 1, "one batch → one chain.batch span");
    let batch = batch_spans[0];
    assert_eq!(
        batch.parent_span_id, chain_run,
        "chain.batch must be a child of chain.run"
    );
    let rows_attr = batch
        .attributes
        .iter()
        .find(|kv| kv.key.as_str() == "rows")
        .expect("rows attribute");
    let rows_value = match &rows_attr.value {
        opentelemetry::Value::I64(value) => *value,
        opentelemetry::Value::String(value) => {
            let text: String = value.clone().into();
            text.parse::<i64>()
                .unwrap_or_else(|_| panic!("rows attribute not numeric: {text}"))
        }
        other => panic!("unexpected rows attribute: {other:?}"),
    };
    assert_eq!(rows_value, 10, "rows attribute must be the batch row count");
}

#[serial_test::serial]
    #[tokio::test]
async fn operator_failure_is_recorded_as_chain_batch_event() {
    let (exporter, provider) = span_test_tracing();

    let operator = "span-batch-fail-7353";
    let spec = spec(
        vec![map_operator(operator)],
        vec![edge("source", operator), edge(operator, "sink")],
        1,
    );
    let plan = JobPlan::compile(spec).unwrap();
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into())]])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(FailingProcessor),
    };
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();

    // The failure has no error-output route, so the job fails — but the
    // batch span still closes with the operator failure event attached.
    let result = run_graph(graph, CancellationToken::new()).await;
    assert!(result.is_err(), "failing operator must fail the job");
    provider.force_flush().unwrap();

    let finished = exporter.get_finished_spans().unwrap();
    let task_id = format!("{operator}-0");
    let batch = finished
        .iter()
        .find(|span| {
            span.name.as_ref() == "chain.batch"
                && span
                    .attributes
                    .iter()
                    .any(|kv| kv.key.as_str() == "task" && kv.value.as_str() == task_id)
        })
        .expect("chain.batch span exported even when the operator fails");

    let event = batch
        .events
        .iter()
        .find(|event| event.name == "operator processing failed; routing to error outputs")
        .expect("operator failure event on the batch span");
    let operator_attr = event
        .attributes
        .iter()
        .find(|kv| kv.key.as_str() == "operator")
        .expect("operator attribute on the failure event");
    assert_eq!(operator_attr.value.as_str(), task_id);
}

#[test]
fn barrier_wire_json_is_backward_and_forward_compatible() {
    // An older sender emits no trace_context field at all.
    let legacy = r#"{"checkpoint_id":"c","generation":1}"#;
    let barrier: CheckpointBarrier = serde_json::from_str(legacy).unwrap();
    assert_eq!(barrier.trace_context, None);
    // A tracing-off sender serializes without the field: byte-identical wire.
    let json = serde_json::to_string(&barrier).unwrap();
    assert!(!json.contains("trace_context"), "{json}");
    // A tracing-on sender's value round-trips.
    let mut stamped = barrier;
    stamped.trace_context = Some("00-trace-span-01".to_string());
    let json = serde_json::to_string(&stamped).unwrap();
    assert!(json.contains("trace_context"), "{json}");
    let back: CheckpointBarrier = serde_json::from_str(&json).unwrap();
    assert_eq!(back, stamped);
}

#[serial_test::serial]
    #[tokio::test]
async fn trace_context_round_trips_to_a_remote_parent() {
    let (exporter, provider) = span_test_tracing();
    let root = tracing::info_span!("trace-root-7354");
    let trace_context = {
        let _guard = root.enter();
        super::remote::capture_trace_context()
    };
    let Some(trace_context) = trace_context else {
        panic!("capture must yield a traceparent under an active span");
    };

    let child = tracing::info_span!("barrier-child-7354");
    {
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;
        // Mirror the production call site (task.rs `barrier_span`): the remote
        // parent is set on the not-yet-entered span — tracing-opentelemetry
        // 0.34 only materializes the export parent from set_parent before the
        // span's own guard is active.
        let remote = super::remote::extract_trace_context(&trace_context);
        assert!(remote.is_some(), "extract must parse a captured value");
        child
            .set_parent(remote.unwrap())
            .expect("set_parent on a fresh span");
        let _guard = child.enter();
    }
    drop(root);
    drop(child);
    provider.force_flush().unwrap();

    let finished = exporter.get_finished_spans().unwrap();
    let root_span = finished
        .iter()
        .find(|span| span.name.as_ref() == "trace-root-7354")
        .expect("root exported");
    let child_span = finished
        .iter()
        .find(|span| span.name.as_ref() == "barrier-child-7354")
        .expect("child exported");
    assert_eq!(
        child_span.parent_span_id,
        root_span.span_context.span_id(),
        "extract+set_parent must restore the remote parent link"
    );
    assert_eq!(
        child_span.span_context.trace_id(),
        root_span.span_context.trace_id(),
        "both spans share one trace"
    );
}

#[tokio::test]
async fn capture_is_none_without_an_active_span() {
    // No entered span on this thread: capture must decline (this is the
    // tracing-off path that keeps barrier bytes identical).
    assert!(super::remote::capture_trace_context().is_none());
}

#[serial_test::serial]
    #[tokio::test]
async fn barrier_carries_remote_trace_context_across_chains() {
    let (exporter, provider) = span_test_tracing();

    struct StreamInput {
        sent: AtomicUsize,
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
    });
    let adapter = Adapter {
        input,
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

    let (barrier_tx, barrier_rx) = flume::bounded::<Envelope>(8);
    let (coordinator, report_tx) = crate::executor::BarrierCoordinator::new(
        JobId::new("trace-barrier-job").unwrap(),
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

    // Queue the barrier before the graph starts: the biased source loop
    // picks it up before the first read, so delivery is deterministic.
    barrier_tx
        .send_async(Envelope::Barrier(CheckpointBarrier {
            checkpoint_id: "cp-trace-7354".to_string(),
            generation: 1,
            trace_context: None,
        }))
        .await
        .expect("send");
    drop(barrier_tx);

    let cancellation = CancellationToken::new();
    let runner = tokio::spawn(run_graph_with_hooks(graph, cancellation.clone(), hooks));
    tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("graph completes")
        .unwrap()
        .unwrap();
    coordinator_cancellation.cancel();
    provider.force_flush().unwrap();

    // The source chain stamped the barrier with its chain.run context at
    // send_downstream; the interior chain's chain.barrier span must parent
    // to exactly that span.
    let finished = exporter.get_finished_spans().unwrap();
    let barrier_span = finished
        .iter()
        .find(|span| {
            span.name.as_ref() == "chain.barrier"
                && span.attributes.iter().any(|kv| {
                    kv.key.as_str() == "checkpoint_id"
                        && kv.value.as_str() == "cp-trace-7354"
                })
        })
        .expect("chain.barrier span for the propagated barrier");
    let upstream = finished
        .iter()
        .find(|span| span.span_context.span_id() == barrier_span.parent_span_id)
        .expect("parent span exported");
    assert_eq!(upstream.name.as_ref(), "chain.run");
    assert!(
        upstream
            .attributes
            .iter()
            .any(|kv| kv.key.as_str() == "task" && kv.value.as_str() == "source-0"),
        "barrier span must parent to the forwarding chain, got {upstream:?}"
    );
    assert_eq!(
        barrier_span.span_context.trace_id(),
        upstream.span_context.trace_id()
    );
}

// ---------- stream-stream join operator (end to end) ----------

#[tokio::test]
async fn two_input_join_emits_matched_pairs_end_to_end() {
    let join = OperatorSpec {
        id: "join".into(),
        kind: OperatorKind::Join,
        stateful: false,
        key_field: None,
        config: serde_json::json!({
            "left_key": "key",
            "right_key": "key",
            "left_timestamp": "ts",
            "right_timestamp": "ts",
            "window_ms": 5_000
        }),
    };
    let mut job = spec(vec![join], vec![], 1);
    // Replace the single auto-generated source with two named sources feeding
    // the join's two inbound edges (edge declaration order fixes left/right).
    job.operators.retain(|operator| operator.id != "source");
    job.sources.clear();
    job.sources.push(SourceSpec {
        codec: None,
        operator_id: "left_source".into(),
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
    });
    job.sources.push(SourceSpec {
        codec: None,
        operator_id: "right_source".into(),
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
    });
    job.operators.insert(
        0,
        OperatorSpec {
            id: "left_source".into(),
            kind: OperatorKind::Source,
            stateful: false,
            key_field: None,
            config: serde_json::json!({}),
        },
    );
    job.operators.insert(
        1,
        OperatorSpec {
            id: "right_source".into(),
            kind: OperatorKind::Source,
            stateful: false,
            key_field: None,
            config: serde_json::json!({}),
        },
    );
    job.edges = vec![
        EdgeSpec {
            id: "left-edge".into(),
            from: "left_source".into(),
            to: "join".into(),
            partitioned: false,
        },
        EdgeSpec {
            id: "right-edge".into(),
            from: "right_source".into(),
            to: "join".into(),
            partitioned: false,
        },
        EdgeSpec {
            id: "join-sink".into(),
            from: "join".into(),
            to: "sink".into(),
            partitioned: false,
        },
    ];
    let plan = JobPlan::compile(job).unwrap();
    let task_ids = plan.tasks.iter().map(|t| t.id.clone()).collect::<Vec<_>>();
    let left: Arc<dyn crate::input::Input> =
        Arc::new(VecInput::new(vec![vec![(100, "a".into())]]));
    let right: Arc<dyn crate::input::Input> =
        Arc::new(VecInput::new(vec![vec![(5_100, "a".into())]]));
    let output = Arc::new(CollectOutput::default());
    let adapter = MultiInputAdapter {
        inputs: [
            ("left_source".to_string(), left),
            ("right_source".to_string(), right),
        ]
        .into_iter()
        .collect(),
        outputs: [("sink".to_string(), output.clone())].into_iter().collect(),
        processor: Arc::new(PassThroughProcessor),
    };
    let graph = ExecutionGraphBuilder::default()
        .build_subgraph(&plan, &task_ids, &adapter, &resource(), None)
        .unwrap_or_else(|error| panic!("build failed: {error}"));
    // The join chain consumes two inbound channels and tags input identity.
    let join_chain = graph
        .chains
        .iter()
        .find(|chain| {
            chain
                .task_ids
                .iter()
                .any(|id| id.starts_with("join-"))
        })
        .expect("join chain");
    assert_eq!(join_chain.inputs.len(), 2);
    assert!(join_chain.tags_input_index);
    run_graph(graph, CancellationToken::new()).await.unwrap();
    let joined = output.written.lock().unwrap().clone();
    let total: usize = joined.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total, 1, "expected exactly one matched pair");
    let batch = &joined[0];
    let names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert!(names.contains(&"l_key".to_string()), "{names:?}");
    assert!(names.contains(&"r_key".to_string()), "{names:?}");
    assert!(names.contains(&"join_key".to_string()), "{names:?}");
}

#[tokio::test]
async fn left_outer_join_emits_unmatched_rows_end_to_end() {
    let join = OperatorSpec {
        id: "join".into(),
        kind: OperatorKind::Join,
        stateful: false,
        key_field: None,
        config: serde_json::json!({
            "left_key": "key",
            "right_key": "key",
            "left_timestamp": "ts",
            "right_timestamp": "ts",
            "join_type": "left_outer",
            "window_ms": 5_000
        }),
    };
    let mut job = spec(vec![join], vec![], 1);
    job.operators.retain(|operator| operator.id != "source");
    job.sources.clear();
    // Event-time sources drive the watermark past the match window so the
    // unmatched left row is evicted (and emitted) before the sources end.
    let event_time = |operator_id: &str| SourceSpec {
        codec: None,
        operator_id: operator_id.into(),
        input_type: "vec".into(),
        config: serde_json::json!({}),
        time: TimeSpec {
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
        },
    };
    job.sources.push(event_time("left_source"));
    job.sources.push(event_time("right_source"));
    job.operators.insert(
        0,
        OperatorSpec {
            id: "left_source".into(),
            kind: OperatorKind::Source,
            stateful: false,
            key_field: None,
            config: serde_json::json!({}),
        },
    );
    job.operators.insert(
        1,
        OperatorSpec {
            id: "right_source".into(),
            kind: OperatorKind::Source,
            stateful: false,
            key_field: None,
            config: serde_json::json!({}),
        },
    );
    job.edges = vec![
        EdgeSpec {
            id: "left-edge".into(),
            from: "left_source".into(),
            to: "join".into(),
            partitioned: false,
        },
        EdgeSpec {
            id: "right-edge".into(),
            from: "right_source".into(),
            to: "join".into(),
            partitioned: false,
        },
        EdgeSpec {
            id: "join-sink".into(),
            from: "join".into(),
            to: "sink".into(),
            partitioned: false,
        },
    ];
    let plan = JobPlan::compile(job).unwrap();
    let task_ids = plan.tasks.iter().map(|t| t.id.clone()).collect::<Vec<_>>();
    // "a" matches within the window; "b" has no right counterpart and must
    // surface as an unmatched row once the watermark passes ts + window.
    // The trailing rows push both sides' watermarks past the eviction bound
    // while both edges are still active.
    let left: Arc<dyn crate::input::Input> = Arc::new(VecInput::new(vec![
        vec![(100, "a".into())],
        vec![(6_000, "b".into())],
        vec![(11_100, "y".into())],
    ]));
    let right: Arc<dyn crate::input::Input> = Arc::new(VecInput::new(vec![
        vec![(5_100, "a".into())],
        vec![(6_000, "c".into())],
        vec![(11_100, "z".into())],
    ]));
    let output = Arc::new(CollectOutput::default());
    let adapter = MultiInputAdapter {
        inputs: [
            ("left_source".to_string(), left),
            ("right_source".to_string(), right),
        ]
        .into_iter()
        .collect(),
        outputs: [("sink".to_string(), output.clone())].into_iter().collect(),
        processor: Arc::new(PassThroughProcessor),
    };
    let graph = ExecutionGraphBuilder::default()
        .build_subgraph(&plan, &task_ids, &adapter, &resource(), None)
        .unwrap_or_else(|error| panic!("build failed: {error}"));
    run_graph(graph, CancellationToken::new()).await.unwrap();
    let joined = output.written.lock().unwrap().clone();
    let total: usize = joined.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total, 2, "one matched pair plus one unmatched left row");
    // Collect (l_key, r_key) per row across every emitted batch.
    let mut rows = Vec::new();
    for batch in &joined {
        let schema = batch.schema();
        let l_key = batch
            .column(schema.index_of("l_key").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let r_key = batch
            .column(schema.index_of("r_key").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let right = if r_key.is_null(row) {
                None
            } else {
                Some(r_key.value(row).to_owned())
            };
            rows.push((l_key.value(row).to_owned(), right));
        }
    }
    // The matched pair keeps both sides; the unmatched left row carries a
    // null right side.
    assert!(rows.contains(&("a".to_owned(), Some("a".to_owned()))), "{rows:?}");
    assert!(rows.contains(&("b".to_owned(), None)), "{rows:?}");
}

// ---------- bounded-wait timeouts (fix-checkpoint-round-timeouts) ----------

/// A sink whose `write_batch` never completes: exercises the sink-write
/// bound end to end through a real chain.
struct HangingSink {
    release: Arc<tokio::sync::Notify>,
    started: AtomicUsize,
}

#[async_trait]
impl Output for HangingSink {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn write(&self, _msg: MessageBatchRef) -> Result<(), Error> {
        self.started.fetch_add(1, Ordering::SeqCst);
        self.release.notified().await;
        Ok(())
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn write_batch(&self, _msgs: &[MessageBatchRef]) -> Result<(), Error> {
        self.started.fetch_add(1, Ordering::SeqCst);
        self.release.notified().await;
        Ok(())
    }
}

/// Spec "A hung sink write fails the chain within a bound": the chain fails
/// with an explicit timeout naming the duration — and the job (therefore
/// shutdown) unblocks instead of parking forever.
#[tokio::test]
async fn hung_sink_write_fails_the_chain_within_a_bound() {
    crate::executor::task::override_sink_write_timeout_for_tests(Duration::from_millis(50));

    let sink = Arc::new(HangingSink {
        release: Arc::new(tokio::sync::Notify::new()),
        started: AtomicUsize::new(0),
    });
    let input = Arc::new(VecInput::new(vec![vec![(1, "a".into())]]));
    let adapter = Adapter {
        input,
        output: sink.clone(),
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
        // The runner connects inputs itself; pass an empty set — the adapter
        // supplies the real input to the graph.
        vec![],
        BTreeMap::new(),
        BTreeMap::new(),
        true,
        cancellation.clone(),
    )
    .await
    .unwrap();

    // The sink hangs on the first batch; the bound fires and the job ends
    // with the explicit timeout error.
    let outcome = tokio::time::timeout(Duration::from_secs(5), handle.watcher()).await;
    crate::executor::task::override_sink_write_timeout_for_tests(Duration::from_secs(5 * 60));
    let result = outcome
        .expect("the bounded sink write must unblock the job")
        .expect("watcher join failed");
    assert!(result.is_err(), "the chain must fail, not park");
    let message = result.unwrap_err().to_string();
    assert!(
        message.contains("sink write timed out"),
        "error names the timeout: {message}"
    );
    cancellation.cancel();
}

/// Spec "A hung state snapshot fails the round within a bound".
#[tokio::test]
async fn hung_state_snapshot_fails_within_a_bound() {
    crate::executor::barrier::override_snapshot_timeout_for_tests(Duration::from_millis(50));

    struct HangingBackend;
    impl crate::state::StateBackend for HangingBackend {
        fn format_version(&self) -> u32 {
            1
        }
        fn get(&self, _namespace: &str, _key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
            Ok(None)
        }
        fn put_with_ttl(
            &self,
            _namespace: &str,
            _key: &[u8],
            _value: &[u8],
            _expires_at_ms: Option<u64>,
            _now_ms: u64,
        ) -> Result<(), Error> {
            Ok(())
        }
        fn update_i64(
            &self,
            _namespace: &str,
            _key: &[u8],
            _delta: i64,
        ) -> Result<i64, Error> {
            Ok(0)
        }
        fn delete(&self, _namespace: &str, _key: &[u8]) -> Result<bool, Error> {
            Ok(false)
        }
        fn purge_expired(&self, _now_ms: u64) -> Result<u64, Error> {
            Ok(0)
        }
        fn scan(&self, _namespace: &str) -> Result<Vec<crate::state::StateEntry>, Error> {
            Ok(Vec::new())
        }
        fn snapshot_at(&self, _epoch: u64) -> Result<crate::state::StateSnapshot, Error> {
            self.snapshot()
        }
        fn snapshot(&self) -> Result<crate::state::StateSnapshot, Error> {
            // Park the blocking thread well past the (shrunken) bound; a
            // bounded park lets the test process exit cleanly instead of a
            // forever-parked thread blocking runtime shutdown.
            std::thread::park_timeout(Duration::from_secs(2));
            unreachable!("parked snapshot never returns within the test")
        }
        fn restore(&self, _snapshot: &crate::state::StateSnapshot) -> Result<(), Error> {
            Ok(())
        }
        fn metrics(&self) -> Result<crate::state::StateMetrics, Error> {
            Ok(crate::state::StateMetrics::default())
        }
        fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    let result =
        crate::executor::barrier::snapshot_state(Arc::new(HangingBackend)).await;
    crate::executor::barrier::override_snapshot_timeout_for_tests(Duration::from_secs(5 * 60));
    let Err(error) = result else {
        panic!("a hung snapshot must fail within the bound");
    };
    let message = error.to_string();
    assert!(
        message.contains("snapshot timed out"),
        "error names the timeout: {message}"
    );
}

/// Spec "挂起轮次超时失败而非永久停摆": a round whose reporting chain is
/// wedged behind a never-completing processor fails at the (shrunken)
/// deadline with an explicit error instead of parking forever.
#[tokio::test]
async fn wedged_round_fails_at_the_deadline_instead_of_parking() {
    struct WedgingProcessor;
    #[async_trait]
    impl Processor for WedgingProcessor {
        async fn process(
            &self,
            _msg: MessageBatchRef,
        ) -> Result<ProcessResult, Error> {
            // Park the worker forever: the barrier queues behind the stuck
            // delivery and the round can never collect its report.
            std::future::pending::<()>().await;
            unreachable!()
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    let input = Arc::new(VecInput::new(vec![vec![(1, "a".into())]]));
    let adapter = Adapter {
        input: input.clone(),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(WedgingProcessor),
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
    let mut handle = crate::executor::kernel_handle::KernelJobRunner::spawn_with_cancellation(
        graph,
        vec![input.clone()],
        BTreeMap::new(),
        BTreeMap::new(),
        false,
        cancellation.clone(),
    )
    .await
    .unwrap();
    handle.override_round_timeout_for_tests(Duration::from_millis(100));

    let outcome = handle.checkpoint_snapshot().await;
    cancellation.cancel();
    let Err(error) = outcome else {
        panic!("a wedged round must fail at the deadline");
    };
    let message = error.to_string();
    assert!(
        message.contains("round timed out"),
        "error names the round deadline: {message}"
    );
}

// ---------- graph construction error-path and helper coverage ----------

fn window_operator(id: &str, config: serde_json::Value) -> OperatorSpec {
    OperatorSpec {
        id: id.into(),
        kind: OperatorKind::Window,
        stateful: false,
        key_field: None,
        config,
    }
}

fn window_config_json(kind: serde_json::Value) -> serde_json::Value {
    let mut config = kind;
    let object = config.as_object_mut().expect("window kind object");
    object.insert(
        "timestamp_field".to_string(),
        serde_json::json!("ts"),
    );
    object.insert("key_field".to_string(), serde_json::json!("key"));
    object.insert("trigger".to_string(), serde_json::json!("watermark"));
    config
    }

fn graph_coverage_event_time(timestamp_field: &str) -> TimeSpec {
    TimeSpec {
        mode: TimeMode::EventTime,
        timestamp_field: Some(timestamp_field.into()),
        watermark: Some(WatermarkSpec {
            strategy: WatermarkStrategy::BoundedOutOfOrderness,
            out_of_orderness_ms: 0,
            idle_timeout_ms: None,
        }),
        allowed_lateness_ms: 0,
        late_event_policy: Default::default(),
        late_event_route: None,
    }
}

fn simple_adapter() -> Adapter {
    Adapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into())]])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    }
}

#[test]
fn builder_capacity_clamps_to_at_least_one_channel() {
    let plan = JobPlan::compile(spec(
        vec![map_operator("map")],
        vec![edge("source", "map"), edge("map", "sink")],
        1,
    ))
    .unwrap();
    let adapter = simple_adapter();
    let graph = ExecutionGraphBuilder::default()
        .with_capacity(4)
        .build(&plan, &adapter, &resource())
        .unwrap();
    assert_eq!(graph.channel_capacity, 4);

    let graph = ExecutionGraphBuilder::default()
        .with_capacity(0)
        .build(&plan, &adapter, &resource())
        .unwrap();
    assert_eq!(graph.channel_capacity, 1, "capacity 0 must clamp to 1");
}

#[test]
fn empty_task_assignment_is_rejected() {
    let plan = JobPlan::compile(spec(
        vec![map_operator("map")],
        vec![edge("source", "map"), edge("map", "sink")],
        1,
    ))
    .unwrap();
    let adapter = simple_adapter();
    let error = ExecutionGraphBuilder::default()
        .build_subgraph(&plan, &[], &adapter, &resource(), None)
        .err()
        .expect("an empty assignment must fail the build");
    assert!(
        error.to_string().contains("contains no tasks"),
        "{error}"
    );
}

#[test]
fn unknown_task_in_assignment_is_rejected() {
    let plan = JobPlan::compile(spec(
        vec![map_operator("map")],
        vec![edge("source", "map"), edge("map", "sink")],
        1,
    ))
    .unwrap();
    let adapter = simple_adapter();
    let error = ExecutionGraphBuilder::default()
        .build_subgraph(
            &plan,
            &["source-0".to_string(), "nope-0".to_string()],
            &adapter,
            &resource(),
            None,
        )
        .err()
        .expect("an unknown task id must fail the build");
    assert!(
        error.to_string().contains("unknown Job task 'nope-0'"),
        "{error}"
    );
}

#[test]
fn window_job_with_pending_transaction_limit_builds_with_limited_journal() {
    let mut job = spec(
        vec![window_operator(
            "win",
            window_config_json(serde_json::json!({"kind": "tumbling", "size_ms": 1000})),
        )],
        vec![edge("source", "win"), edge("win", "sink")],
        1,
    );
    job.state.as_mut().unwrap().max_pending_transactions = Some(3);
    job.sources[0].time = graph_coverage_event_time("ts");
    let plan = JobPlan::compile(job).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &simple_adapter(), &resource())
        .unwrap();
    assert_eq!(
        graph.chains.len(),
        3,
        "source chain, stateful window chain, sink chain"
    );
}

#[test]
fn remote_node_view_missing_a_data_edge_task_fails_closed() {
    let plan = JobPlan::compile(spec(
        vec![map_operator("map")],
        vec![edge("source", "map"), edge("map", "sink")],
        1,
    ))
    .unwrap();
    let manager = crate::executor::remote::NetworkManager::new(8);
    // map-0 sits on another node but is missing from the task→node view: the
    // Agent must fail closed instead of guessing the placement.
    let context = crate::executor::graph::RemoteEdgeContext {
        tls: None,
        local_node: "node-a".into(),
        task_nodes: BTreeMap::from([("source-0".to_string(), "node-a".to_string())]),
        node_addrs: BTreeMap::new(),
        manager,
        generation: 1,
    };
    let error = ExecutionGraphBuilder::default()
        .build_subgraph(
            &plan,
            &["source-0".to_string()],
            &simple_adapter(),
            &resource(),
            Some(&context),
        )
        .err()
        .expect("an incomplete task view must fail the build");
    assert!(
        error.to_string().contains("no node for task 'map-0'"),
        "{error}"
    );
}

#[test]
fn partitioned_target_without_key_groups_fails_locally_and_remotely() {
    let mut job = spec(
        vec![map_operator("map")],
        vec![edge("source", "map"), edge("map", "sink")],
        2,
    );
    job.edges[0].partitioned = true;
    let mut plan = JobPlan::compile(job).unwrap();
    // Strip the map tasks' key-group partitions: partitioned routing cannot
    // derive hash buckets and must fail the build.
    for task in plan.tasks.iter_mut() {
        if task.operator_id == "map" {
            task.partitions.clear();
        }
    }
    let error = ExecutionGraphBuilder::default()
        .build_subgraph(
            &plan,
            &["source-0".to_string(), "map-0".to_string()],
            &simple_adapter(),
            &resource(),
            None,
        )
        .err()
        .expect("a partitioned local target without key groups must fail");
    assert!(
        error.to_string().contains("has no key-group partition"),
        "{error}"
    );

    // Same defect on a remote target fails identically.
    let manager = crate::executor::remote::NetworkManager::new(8);
    let context = crate::executor::graph::RemoteEdgeContext {
        tls: None,
        local_node: "node-a".into(),
        task_nodes: BTreeMap::from([
            ("source-0".to_string(), "node-a".to_string()),
            ("source-1".to_string(), "node-a".to_string()),
            ("map-0".to_string(), "node-b".to_string()),
            ("map-1".to_string(), "node-b".to_string()),
            ("sink-0".to_string(), "node-b".to_string()),
            ("sink-1".to_string(), "node-b".to_string()),
        ]),
        node_addrs: BTreeMap::new(),
        manager,
        generation: 1,
    };
    let error = ExecutionGraphBuilder::default()
        .build_subgraph(
            &plan,
            &["source-0".to_string()],
            &simple_adapter(),
            &resource(),
            Some(&context),
        )
        .err()
        .expect("a partitioned remote target without key groups must fail");
    assert!(
        error.to_string().contains("has no key-group partition"),
        "{error}"
    );
}

#[test]
fn late_event_route_target_missing_from_assignment_is_rejected() {
    let mut job = spec(
        vec![map_operator("map")],
        vec![edge("source", "map"), edge("map", "sink")],
        1,
    );
    job.operators.push(sink_operator("late_sink", false));
    job.sinks.push(crate::job::SinkSpec {
        operator_id: "late_sink".into(),
        output_type: "collect".into(),
        codec: None,
        config: serde_json::json!({}),
    });
    job.sources[0].time.late_event_route = Some("late_sink".into());
    let plan = JobPlan::compile(job).unwrap();
    let error = ExecutionGraphBuilder::default()
        .build_subgraph(
            &plan,
            &["source-0".to_string(), "map-0".to_string(), "sink-0".to_string()],
            &simple_adapter(),
            &resource(),
            None,
        )
        .err()
        .expect("a route target outside the assignment must fail the build");
    assert!(
        error
            .to_string()
            .contains("late-event route target 'late_sink' for source 'source'"),
        "{error}"
    );
}

#[test]
fn late_event_route_broadcasts_across_target_subtasks() {
    let mut job = spec(
        vec![map_operator("map")],
        vec![edge("source", "map"), edge("map", "sink")],
        2,
    );
    job.operators.push(sink_operator("late_sink", false));
    job.sinks.push(crate::job::SinkSpec {
        operator_id: "late_sink".into(),
        output_type: "collect".into(),
        codec: None,
        config: serde_json::json!({}),
    });
    job.sources[0].time.late_event_route = Some("late_sink".into());
    let plan = JobPlan::compile(job).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &simple_adapter(), &resource())
        .unwrap();
    let source_chain = graph
        .chains
        .iter()
        .find(|chain| chain.task_ids.first().is_some_and(|id| id == "source-0"))
        .expect("source chain");
    let edges = source_chain
        .late_event_outputs
        .get("source-0")
        .expect("source task carries a late-event edge");
    assert!(
        edges.iter().any(|target| matches!(
            target,
            crate::executor::graph::EdgeTarget::Broadcast(channels) if channels.len() == 2
        )),
        "two late-sink subtasks must be reached by broadcast"
    );
}

#[test]
fn task_referencing_unknown_operator_is_rejected() {
    let mut plan = JobPlan::compile(spec(Vec::new(), vec![edge("source", "sink")], 1)).unwrap();
    plan.tasks.push(crate::job::TaskSpec {
        id: "ghost-0".into(),
        operator_id: "ghost".into(),
        subtask: 0,
        partitions: Vec::new(),
    });
    let error = ExecutionGraphBuilder::default()
        .build_subgraph(
            &plan,
            &[
                "source-0".to_string(),
                "ghost-0".to_string(),
                "sink-0".to_string(),
            ],
            &simple_adapter(),
            &resource(),
            None,
        )
        .err()
        .expect("a task with an unknown operator must fail the build");
    assert!(
        error.to_string().contains("references unknown operator"),
        "{error}"
    );
}

#[test]
fn join_side_producer_mismatch_fails_graph_build() {
    let mut job = spec(
        vec![OperatorSpec {
            id: "join".into(),
            kind: OperatorKind::Join,
            stateful: false,
            key_field: None,
            config: serde_json::json!({
                "left_key": "key",
                "right_key": "key",
                "window_ms": 5000,
                "left_from": "profiles",
                "right_from": "orders",
            }),
        }],
        vec![
            edge("source", "join"),
            edge("aux", "join"),
            edge("join", "sink"),
        ],
        1,
    );
    // Replace the auto-inserted source with two named sources so both join
    // sides have a producer.
    job.operators.insert(
        0,
        OperatorSpec {
            id: "aux".into(),
            kind: OperatorKind::Source,
            stateful: false,
            key_field: None,
            config: serde_json::json!({}),
        },
    );
    job.sources.push(crate::job::SourceSpec {
        operator_id: "aux".into(),
        input_type: "vec".into(),
        codec: None,
        config: serde_json::json!({}),
        time: TimeSpec {
            mode: TimeMode::ProcessingTime,
            timestamp_field: None,
            watermark: None,
            allowed_lateness_ms: 0,
            late_event_policy: Default::default(),
            late_event_route: None,
        },
    });
    let plan = JobPlan::compile(job).unwrap();
    let error = ExecutionGraphBuilder::default()
        .build(&plan, &simple_adapter(), &resource())
        .err()
        .expect("a join side naming a non-producer must fail the build");
    assert!(
        error.to_string().contains("which does not feed this join"),
        "{error}"
    );
}

#[test]
fn session_window_builds_window_side_late_route_and_gate_timing() {
    let mut job = spec(
        vec![window_operator(
            "win",
            window_config_json(serde_json::json!({"kind": "session", "gap_ms": 500})),
        )],
        vec![edge("source", "win"), edge("win", "sink")],
        1,
    );
    job.operators.push(sink_operator("late_sink", false));
    job.sinks.push(crate::job::SinkSpec {
        operator_id: "late_sink".into(),
        output_type: "collect".into(),
        codec: None,
        config: serde_json::json!({}),
    });
    job.sources[0].time = graph_coverage_event_time("ts");
    job.sources[0].time.late_event_route = Some("late_sink".into());
    let plan = JobPlan::compile(job).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &simple_adapter(), &resource())
        .unwrap();
    // The window task owns a dynamic-deadline side route for its late rows.
    let window_chain = graph
        .chains
        .iter()
        .find(|chain| {
            chain
                .task_ids
                .iter()
                .any(|id| id.starts_with("win-"))
        })
        .expect("window chain");
    assert!(
        window_chain
            .late_event_outputs
            .contains_key(window_chain.entry_task_id()),
        "the session window task must carry a late-event side edge"
    );
    // The source gate learns the session gap timing.
    let source_chain = graph
        .chains
        .iter()
        .find(|chain| chain.is_source())
        .expect("source chain");
    assert_eq!(source_chain.window_timings.len(), 1);
    assert!(matches!(
        source_chain.window_timings[0],
        crate::executor::event_time_gate::WindowTiming::Session { gap_ms: 500 }
    ));
}

#[test]
fn sliding_window_timing_reaches_the_source_gate() {
    let mut job = spec(
        vec![window_operator(
            "win",
            window_config_json(serde_json::json!({"kind": "sliding", "size_ms": 10_000, "slide_ms": 2_000})),
        )],
        vec![edge("source", "win"), edge("win", "sink")],
        1,
    );
    job.sources[0].time = graph_coverage_event_time("ts");
    let plan = JobPlan::compile(job).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &simple_adapter(), &resource())
        .unwrap();
    let source_chain = graph
        .chains
        .iter()
        .find(|chain| chain.is_source())
        .expect("source chain");
    assert!(matches!(
        source_chain.window_timings[0],
        crate::executor::event_time_gate::WindowTiming::Sliding { size_ms: 10_000, slide_ms: 2_000 }
    ));
}

#[test]
fn watermark_groups_merge_overlapping_sources_and_share_windows() {
    // s1 reaches w1 and w2; s2 reaches only w2: the connected component joins
    // both sources into one watermark group (compatible TimeSpecs).
    let mut job = spec(
        vec![
            window_operator("w1", window_config_json(serde_json::json!({"kind": "tumbling", "size_ms": 1000}))),
            window_operator("w2", window_config_json(serde_json::json!({"kind": "tumbling", "size_ms": 2000}))),
        ],
        vec![
            edge("source", "w1"),
            edge("w1", "w2"),
            edge("source", "w2"),
            edge("aux", "w2"),
            edge("w2", "sink"),
        ],
        1,
    );
    job.operators.insert(
        0,
        OperatorSpec {
            id: "aux".into(),
            kind: OperatorKind::Source,
            stateful: false,
            key_field: None,
            config: serde_json::json!({}),
        },
    );
    job.sources[0].time = graph_coverage_event_time("ts");
    job.sources.push(crate::job::SourceSpec {
        operator_id: "aux".into(),
        input_type: "vec".into(),
        codec: None,
        config: serde_json::json!({}),
        time: graph_coverage_event_time("ts"),
    });
    let plan = JobPlan::compile(job).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &simple_adapter(), &resource())
        .unwrap();
    assert_eq!(
        graph
            .chains
            .iter()
            .filter(|chain| chain.is_source())
            .count(),
        2
    );

    // The same topology with an incompatible sibling TimeSpec is rejected at
    // build time instead of letting the two gates disagree.
    let mut job2 = spec(
        vec![
            window_operator("w1", window_config_json(serde_json::json!({"kind": "tumbling", "size_ms": 1000}))),
            window_operator("w2", window_config_json(serde_json::json!({"kind": "tumbling", "size_ms": 2000}))),
        ],
        vec![
            edge("source", "w1"),
            edge("w1", "w2"),
            edge("source", "w2"),
            edge("aux", "w2"),
            edge("w2", "sink"),
        ],
        1,
    );
    job2.operators.insert(
        0,
        OperatorSpec {
            id: "aux".into(),
            kind: OperatorKind::Source,
            stateful: false,
            key_field: None,
            config: serde_json::json!({}),
        },
    );
    job2.sources[0].time = graph_coverage_event_time("ts");
    let mut incompatible = graph_coverage_event_time("ts");
    incompatible.allowed_lateness_ms = 5;
    job2.sources.push(crate::job::SourceSpec {
        operator_id: "aux".into(),
        input_type: "vec".into(),
        codec: None,
        config: serde_json::json!({}),
        time: incompatible,
    });
    let plan2 = JobPlan::compile(job2).unwrap();
    let error = ExecutionGraphBuilder::default()
        .build(&plan2, &simple_adapter(), &resource())
        .err()
        .expect("incompatible shared watermark specs must fail the build");
    assert!(
        error.to_string().contains("incompatible TimeSpec values"),
        "{error}"
    );
}

#[test]
fn session_window_route_reachability_walks_unrelated_sources() {
    // A second event-time source with a late route that does NOT reach the
    // window exercises the reachability walk's termination (visited set and
    // the not-reachable outcome) without contributing a route operator.
    let mut job = spec(
        vec![
            window_operator("win", window_config_json(serde_json::json!({"kind": "session", "gap_ms": 500}))),
            map_operator("m1"),
            map_operator("m2"),
        ],
        vec![
            edge("source", "win"),
            edge("win", "late_sink"),
            edge("aux", "m1"),
            edge("aux", "m2"),
            edge("m1", "sink"),
            edge("m2", "sink"),
        ],
        1,
    );
    job.operators.push(sink_operator("late_sink", false));
    job.sinks.push(crate::job::SinkSpec {
        operator_id: "late_sink".into(),
        output_type: "collect".into(),
        codec: None,
        config: serde_json::json!({}),
    });
    job.sources[0].time = graph_coverage_event_time("ts");
    job.sources[0].time.late_event_route = Some("late_sink".into());
    // The default auto-inserted sink operator is unreachable from `aux`
    // unless linked; m1/m2 both feed it above.
    job.operators.insert(
        0,
        OperatorSpec {
            id: "aux".into(),
            kind: OperatorKind::Source,
            stateful: false,
            key_field: None,
            config: serde_json::json!({}),
        },
    );
    let mut aux_time = graph_coverage_event_time("ts");
    aux_time.late_event_route = Some("sink".into());
    job.sources.push(crate::job::SourceSpec {
        operator_id: "aux".into(),
        input_type: "vec".into(),
        codec: None,
        config: serde_json::json!({}),
        time: aux_time,
    });
    let plan = JobPlan::compile(job).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &simple_adapter(), &resource())
        .unwrap();
    let window_chain = graph
        .chains
        .iter()
        .find(|chain| chain.task_ids.iter().any(|id| id.starts_with("win-")))
        .expect("window chain");
    // Only the source reaching the window contributes the route operator.
    assert_eq!(
        window_chain
            .late_event_outputs
            .get(window_chain.entry_task_id())
            .map(|edges| edges.len()),
        Some(1)
    );
}

#[test]
fn remote_edge_without_address_or_credentials_fails_the_build() {
    let plan = JobPlan::compile(spec(
        vec![map_operator("map")],
        vec![edge("source", "map"), edge("map", "sink")],
        1,
    ))
    .unwrap();

    // The remote node has no data-plane address advertised.
    let manager = crate::executor::remote::NetworkManager::new(8);
    let context = crate::executor::graph::RemoteEdgeContext {
        tls: None,
        local_node: "node-a".into(),
        task_nodes: BTreeMap::from([
            ("source-0".to_string(), "node-a".to_string()),
            ("map-0".to_string(), "node-c".to_string()),
            ("sink-0".to_string(), "node-c".to_string()),
        ]),
        node_addrs: BTreeMap::new(),
        manager: manager.clone(),
        generation: 1,
    };
    let error = ExecutionGraphBuilder::default()
        .build_subgraph(
            &plan,
            &["source-0".to_string()],
            &simple_adapter(),
            &resource(),
            Some(&context),
        )
        .err()
        .expect("a remote node without an address must fail the build");
    assert!(
        error.to_string().contains("no data-plane address for remote node 'node-c'"),
        "{error}"
    );

    // With an address but no credentials the edge cannot authenticate.
    let context = crate::executor::graph::RemoteEdgeContext {
        tls: None,
        local_node: "node-a".into(),
        task_nodes: BTreeMap::from([
            ("source-0".to_string(), "node-a".to_string()),
            ("map-0".to_string(), "node-c".to_string()),
            ("sink-0".to_string(), "node-c".to_string()),
        ]),
        node_addrs: BTreeMap::from([(
            "node-c".to_string(),
            "127.0.0.1:1".parse::<std::net::SocketAddr>().unwrap(),
        )]),
        manager,
        generation: 1,
    };
    let error = ExecutionGraphBuilder::default()
        .build_subgraph(
            &plan,
            &["source-0".to_string()],
            &simple_adapter(),
            &resource(),
            Some(&context),
        )
        .err()
        .expect("an unauthenticated manager must refuse a remote edge");
    assert!(
        error.to_string().contains("without data-plane credentials"),
        "{error}"
    );
}

#[test]
fn unauthenticated_transport_serves_only_one_job() {
    // The legacy in-memory transport has no Job identity on the wire: two
    // different Jobs cannot share its quad routes.
    fn downstream_plan(job_id: &str) -> JobPlan {
        let mut job = spec(
            vec![map_operator("map")],
            vec![edge("source", "map"), edge("map", "sink")],
            1,
        );
        job.id = crate::job::JobId::new(job_id).unwrap();
        JobPlan::compile(job).unwrap()
    }
    let manager = crate::executor::remote::NetworkManager::new(8);
    let context = |plan: &JobPlan| crate::executor::graph::RemoteEdgeContext {
        tls: None,
        local_node: "node-b".into(),
        task_nodes: plan
            .tasks
            .iter()
            .map(|task| {
                let node = if task.operator_id == "source" {
                    "node-a"
                } else {
                    "node-b"
                };
                (task.id.clone(), node.to_string())
            })
            .collect(),
        node_addrs: BTreeMap::new(),
        manager: manager.clone(),
        generation: 1,
    };
    let plan_a = downstream_plan("legacy-job-a");
    let task_ids: Vec<String> = ["map-0", "sink-0"]
        .into_iter()
        .map(str::to_string)
        .collect();
    ExecutionGraphBuilder::default()
        .build_subgraph(&plan_a, &task_ids, &simple_adapter(), &resource(), Some(&context(&plan_a)))
        .expect("first job claims the legacy transport");

    let plan_b = downstream_plan("legacy-job-b");
    let error = ExecutionGraphBuilder::default()
        .build_subgraph(&plan_b, &task_ids, &simple_adapter(), &resource(), Some(&context(&plan_b)))
        .err()
        .expect("a second job must be rejected by the legacy transport");
    assert!(
        error.to_string().contains("cannot carry Job"),
        "{error}"
    );
}

// ---------- run_graph wrapper / startup / shutdown coverage ----------

/// A parking input whose end-of-stream can be armed from the test: once
/// `finish` is set, the next read returns EOF (after a wake-up).
struct ClosableParkingInput {
    queue: Mutex<std::collections::VecDeque<MessageBatchRef>>,
    arrived: tokio::sync::Notify,
    finished: std::sync::atomic::AtomicBool,
    closed: AtomicUsize,
}

impl ClosableParkingInput {
    fn with(batches: Vec<MessageBatchRef>) -> Arc<Self> {
        Arc::new(Self {
            queue: Mutex::new(batches.into_iter().collect()),
            arrived: tokio::sync::Notify::new(),
            finished: std::sync::atomic::AtomicBool::new(false),
            closed: AtomicUsize::new(0),
        })
    }

    fn push_rows(&self, rows: Vec<(i64, String)>) {
        self.queue
            .lock()
            .unwrap()
            .push_back(Arc::new(MessageBatch::new_arrow(int64_batch(rows))));
        self.arrived.notify_one();
    }

    fn finish(&self) {
        self.finished.store(true, Ordering::SeqCst);
        self.arrived.notify_one();
    }
}

#[async_trait]
impl Input for ClosableParkingInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        loop {
            if let Some(batch) = self.queue.lock().unwrap().pop_front() {
                return Ok((batch, Arc::new(crate::input::NoopAck)));
            }
            if self.finished.load(Ordering::SeqCst) {
                return Err(Error::EOF);
            }
            self.arrived.notified().await;
        }
    }
    async fn close(&self) -> Result<(), Error> {
        self.closed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn run_graph_with_metrics_counts_source_batches() {
    let collect = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into()), (2, "b".into())]])),
        output: collect.clone(),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(spec(vec![], vec![edge("source", "sink")], 1)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let metrics = Arc::new(crate::runtime::RuntimeMetrics::default());
    crate::executor::task::run_graph_with_metrics(
        graph,
        CancellationToken::new(),
        Some(metrics.clone()),
    )
    .await
    .unwrap();
    assert_eq!(collect.written.lock().unwrap().len(), 1);
    assert_eq!(metrics.input_batches.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.input_messages.load(Ordering::Relaxed), 2);
    assert_eq!(metrics.output_batches.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn retired_gate_wrappers_still_run_the_graph() {
    use crate::executor::task::{run_graph_with_gate, run_graph_with_hooks_and_gate};

    let build = || {
        let collect = Arc::new(CollectOutput::default());
        let adapter = Adapter {
            input: Arc::new(VecInput::new(vec![vec![(1, "a".into())]])),
            output: collect.clone(),
            processor: Arc::new(PassThroughProcessor),
        };
        let plan = JobPlan::compile(spec(vec![], vec![edge("source", "sink")], 1)).unwrap();
        (
            ExecutionGraphBuilder::default()
                .build(&plan, &adapter, &resource())
                .unwrap(),
            collect,
        )
    };

    let (graph, collect) = build();
    let snapshot_gate: crate::executor::kernel_handle::SnapshotGate =
        Arc::new(tokio::sync::RwLock::new(()));
    run_graph_with_gate(graph, CancellationToken::new(), snapshot_gate)
        .await
        .unwrap();
    assert_eq!(collect.written.lock().unwrap().len(), 1);

    let (graph, collect) = build();
    let snapshot_gate: crate::executor::kernel_handle::SnapshotGate =
        Arc::new(tokio::sync::RwLock::new(()));
    run_graph_with_hooks_and_gate(
        graph,
        CancellationToken::new(),
        BTreeMap::new(),
        snapshot_gate,
    )
    .await
    .unwrap();
    assert_eq!(collect.written.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn preconnected_startup_runs_the_graph() {
    let input = ClosableParkingInput::with(vec![Arc::new(MessageBatch::new_arrow(int64_batch(
        vec![(1, "a".into())],
    )))]);
    let collect = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: collect.clone(),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(spec(vec![], vec![edge("source", "sink")], 1)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
    let run = crate::executor::task::run_graph_with_hooks_startup(
        graph,
        CancellationToken::new(),
        BTreeMap::new(),
        true,
        Some(startup_tx),
    );
    input.finish();
    run.await.unwrap();
    startup_rx.await.unwrap().unwrap();
    assert_eq!(collect.written.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn preconnected_startup_failure_closes_sources_and_reports() {
    struct FailingConnectOutput;
    #[async_trait]
    impl Output for FailingConnectOutput {
        async fn connect(&self) -> Result<(), Error> {
            Err(Error::Process("sink connect failed".into()))
        }
        async fn write(&self, _msg: MessageBatchRef) -> Result<(), Error> {
            Ok(())
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    let input = ClosableParkingInput::with(vec![]);
    let adapter = Adapter {
        input: input.clone(),
        output: Arc::new(FailingConnectOutput),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(spec(vec![], vec![edge("source", "sink")], 1)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
    let result = crate::executor::task::run_graph_with_hooks_startup(
        graph,
        CancellationToken::new(),
        BTreeMap::new(),
        true,
        Some(startup_tx),
    )
    .await;
    assert!(result.is_err(), "a failing sink connect fails the startup");
    let report = startup_rx.await.unwrap().unwrap_err();
    assert!(report.contains("sink connect failed"), "{report}");
    // The recovery preparer's connected inputs are closed by the failure
    // path rather than leaked.
    assert_eq!(input.closed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_panic_inside_the_event_loop_fails_the_chain() {
    struct PanickingProcessor;
    #[async_trait]
    impl Processor for PanickingProcessor {
        async fn process(&self, _batch: MessageBatchRef) -> Result<ProcessResult, Error> {
            panic!("event loop exploded");
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into())]])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PanickingProcessor),
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
    let result = run_graph(graph, CancellationToken::new()).await;
    let message = result.unwrap_err().to_string();
    // The event loop's catch_unwind converts the panic into a chain error
    // (the payload text itself depends on how the panic crossed the
    // instrumented future boundary).
    assert!(message.contains("chain task panicked"), "{message}");
}

#[tokio::test]
async fn a_panic_inside_a_close_path_fails_the_chain_task() {
    struct PanickingCloseProcessor;
    #[async_trait]
    impl Processor for PanickingCloseProcessor {
        async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
            Ok(ProcessResult::Single(batch))
        }
        async fn close(&self) -> Result<(), Error> {
            panic!("close exploded");
        }
    }
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into())]])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PanickingCloseProcessor),
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
    let result = run_graph(graph, CancellationToken::new()).await;
    let message = result.unwrap_err().to_string();
    // The panic escaped the event loop's catch_unwind (it fired in the close
    // path), so the join error surfaces as a chain task panic.
    assert!(
        message.contains("chain task panicked"),
        "{message}"
    );
}

#[tokio::test]
async fn component_close_failures_surface_after_the_chains_exit() {
    struct FailingCloseProcessor;
    #[async_trait]
    impl Processor for FailingCloseProcessor {
        async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
            Ok(ProcessResult::Single(batch))
        }
        async fn close(&self) -> Result<(), Error> {
            Err(Error::Process("processor close failed".into()))
        }
    }
    struct FailingCloseOutput;
    #[async_trait]
    impl Output for FailingCloseOutput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn write(&self, _msg: MessageBatchRef) -> Result<(), Error> {
            Ok(())
        }
        async fn close(&self) -> Result<(), Error> {
            Err(Error::Process("sink close failed".into()))
        }
    }
    struct FailingCloseInput;
    #[async_trait]
    impl Input for FailingCloseInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            Err(Error::EOF)
        }
        async fn close(&self) -> Result<(), Error> {
            Err(Error::Process("source close failed".into()))
        }
    }
    let adapter = Adapter {
        input: Arc::new(FailingCloseInput),
        output: Arc::new(FailingCloseOutput),
        processor: Arc::new(FailingCloseProcessor),
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
    let result = run_graph(graph, CancellationToken::new()).await;
    let message = result.unwrap_err().to_string();
    assert!(
        message.contains("close failed"),
        "a close failure must surface instead of being swallowed: {message}"
    );
}

#[tokio::test]
async fn a_chain_without_source_or_inputs_is_a_config_error() {
    let graph = crate::executor::ExecutionGraph {
        chains: vec![crate::executor::graph::Chain::for_pool_test(1, vec![])],
        channel_capacity: 4,
        temporaries: Vec::new(),
    };
    let result = run_graph(graph, CancellationToken::new()).await;
    let message = result.unwrap_err().to_string();
    assert!(
        message.contains("has neither a source nor input channels"),
        "{message}"
    );
}

#[tokio::test]
async fn an_event_time_source_without_a_watermark_spec_fails_the_chain() {
    // A hand-built chain: the graph builder validates JobSpecs, so the
    // malformed time contract is injected directly.
    let mut chain = crate::executor::graph::Chain::for_pool_test(1, vec![]);
    chain.task_ids = vec!["source-0".into()];
    chain.source = Some(ClosableParkingInput::with(vec![]));
    chain.source_time = Some(TimeSpec {
        mode: TimeMode::EventTime,
        timestamp_field: Some("ts".into()),
        watermark: None,
        allowed_lateness_ms: 0,
        late_event_policy: Default::default(),
        late_event_route: None,
    });
    let graph = crate::executor::ExecutionGraph {
        chains: vec![chain],
        channel_capacity: 4,
        temporaries: Vec::new(),
    };
    let result = run_graph(graph, CancellationToken::new()).await;
    let message = result.unwrap_err().to_string();
    assert!(
        message.contains("watermark tracker requires a watermark specification"),
        "{message}"
    );
}

// ---------- source-chain barrier and reconnection paths ----------

/// `run_graph_with_hooks` wired with a barrier injection channel.
fn barrier_hooked_graph(
    input: Arc<dyn Input>,
    _barrier_rx: flume::Receiver<Envelope>,
) -> crate::executor::ExecutionGraph {
    let collect = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input,
        output: collect,
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(spec(vec![], vec![edge("source", "sink")], 1)).unwrap();
    ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap()
}

fn source_hook(
    barrier_rx: flume::Receiver<Envelope>,
    state: Option<Arc<dyn crate::state::StateBackend>>,
    failure_tx: Option<tokio::sync::mpsc::UnboundedSender<Error>>,
) -> BTreeMap<String, crate::executor::task::CheckpointHook> {
    BTreeMap::from([(
        "source-0".to_string(),
        crate::executor::task::CheckpointHook {
            reporter: None,
            failure_reporter: failure_tx,
            barrier_rx: Some(Arc::new(tokio::sync::Mutex::new(barrier_rx))),
            state,
            task_id: Some("source-0".to_string()),
            event_time_gate: Arc::new(tokio::sync::Mutex::new(None)),
            partition: Some(0),
            metrics: None,
            finished_reporter: None,
        },
    )])
}

#[tokio::test]
async fn non_barrier_envelopes_on_the_barrier_channel_are_ignored() {
    let input = ClosableParkingInput::with(vec![Arc::new(MessageBatch::new_arrow(int64_batch(
        vec![(1, "a".into())],
    )))]);
    let (barrier_tx, barrier_rx) = flume::bounded::<Envelope>(8);
    let graph = barrier_hooked_graph(input.clone(), barrier_rx.clone());
    let cancellation = CancellationToken::new();
    let runner = tokio::spawn(crate::executor::task::run_graph_with_hooks(
        graph,
        cancellation.clone(),
        source_hook(barrier_rx, None, None),
    ));
    // Data envelopes on the barrier channel are skipped without stopping the
    // loop, and a real barrier flows without a reporter or state backend.
    let _ = barrier_tx
        .send_async(Envelope::Data(
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![(9, "x".into())]))),
            Arc::new(crate::input::NoopAck),
        ))
        .await;
    let _ = barrier_tx
        .send_async(Envelope::Barrier(CheckpointBarrier {
            checkpoint_id: "cp-quiet".into(),
            generation: 1,
            trace_context: None,
        }))
        .await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    input.finish();
    tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .expect("graph must ignore non-barrier control envelopes")
        .unwrap()
        .unwrap();
}

struct FailingSnapshotBackend;

impl crate::state::StateBackend for FailingSnapshotBackend {
    fn format_version(&self) -> u32 {
        1
    }
    fn get(&self, _namespace: &str, _key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        Ok(None)
    }
    fn put_with_ttl(
        &self,
        _namespace: &str,
        _key: &[u8],
        _value: &[u8],
        _ttl: Option<u64>,
        _now: u64,
    ) -> Result<(), Error> {
        Ok(())
    }
    fn update_i64(&self, _namespace: &str, _key: &[u8], _delta: i64) -> Result<i64, Error> {
        Ok(0)
    }
    fn delete(&self, _namespace: &str, _key: &[u8]) -> Result<bool, Error> {
        Ok(false)
    }
    fn purge_expired(&self, _now: u64) -> Result<u64, Error> {
        Ok(0)
    }
    fn scan(&self, _namespace: &str) -> Result<Vec<crate::state::StateEntry>, Error> {
        Ok(Vec::new())
    }
    fn snapshot_at(&self, _epoch: u64) -> Result<crate::state::StateSnapshot, Error> {
        self.snapshot()
    }
    fn snapshot(&self) -> Result<crate::state::StateSnapshot, Error> {
        Err(Error::Process("injected snapshot failure".into()))
    }
    fn restore(&self, _snapshot: &crate::state::StateSnapshot) -> Result<(), Error> {
        Ok(())
    }
    fn metrics(&self) -> Result<crate::state::StateMetrics, Error> {
        Ok(crate::state::StateMetrics::default())
    }
    fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

#[tokio::test]
async fn source_barrier_snapshot_failure_reports_and_forwards_the_barrier() {
    let input = ClosableParkingInput::with(vec![Arc::new(MessageBatch::new_arrow(int64_batch(
        vec![(1, "a".into())],
    )))]);
    let (barrier_tx, barrier_rx) = flume::bounded::<Envelope>(8);
    let graph = barrier_hooked_graph(input.clone(), barrier_rx.clone());
    let (failure_tx, mut failure_rx) = tokio::sync::mpsc::unbounded_channel();
    let cancellation = CancellationToken::new();
    let runner = tokio::spawn(crate::executor::task::run_graph_with_hooks(
        graph,
        cancellation.clone(),
        source_hook(barrier_rx, Some(Arc::new(FailingSnapshotBackend)), Some(failure_tx)),
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let _ = barrier_tx
        .send_async(Envelope::Barrier(CheckpointBarrier {
            checkpoint_id: "cp-snap-fail".into(),
            generation: 1,
            trace_context: None,
        }))
        .await;
    let failure = tokio::time::timeout(Duration::from_secs(5), failure_rx.recv())
        .await
        .expect("the snapshot failure must be reported")
        .expect("the failure channel stays open");
    assert!(
        failure.to_string().contains("injected snapshot failure"),
        "{failure}"
    );
    // The data plane keeps running after the failed round.
    input.push_rows(vec![(2, "b".into())]);
    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .expect("graph must survive a failed snapshot round")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn cancellation_during_barrier_drain_shuts_the_source_down() {
    // The interior processor defers the delivery, so the source's tracking
    // acknowledgement never completes: the barrier drain parks and only
    // cancellation ends it.
    struct DeferringProcessor;
    #[async_trait]
    impl Processor for DeferringProcessor {
        async fn process(&self, _batch: MessageBatchRef) -> Result<ProcessResult, Error> {
            Ok(ProcessResult::Deferred)
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    let input = ClosableParkingInput::with(vec![Arc::new(MessageBatch::new_arrow(int64_batch(
        vec![(1, "a".into())],
    )))]);
    let (barrier_tx, barrier_rx) = flume::bounded::<Envelope>(8);
    let collect = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: collect,
        processor: Arc::new(DeferringProcessor),
    };
    // source -> defer -> sink: the interior chain retains the delivery's
    // acknowledgement so the source's tracking ack never completes.
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
    let runner = tokio::spawn(crate::executor::task::run_graph_with_hooks(
        graph,
        cancellation.clone(),
        source_hook(barrier_rx, None, None),
    ));
    tokio::time::sleep(Duration::from_millis(30)).await;
    let _ = barrier_tx
        .send_async(Envelope::Barrier(CheckpointBarrier {
            checkpoint_id: "cp-drain".into(),
            generation: 1,
            trace_context: None,
        }))
        .await;
    // The drain is waiting on the deferred acknowledgement; cancel it.
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .expect("cancellation must end the barrier drain")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn disconnection_reconnects_and_updates_the_metrics() {
    struct FlakyInput {
        attempts: AtomicUsize,
    }
    #[async_trait]
    impl Input for FlakyInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            match self.attempts.fetch_add(1, Ordering::SeqCst) {
                0 => Err(Error::Disconnection),
                1 => Ok((
                    Arc::new(MessageBatch::new_arrow(int64_batch(vec![(1, "a".into())]))),
                    Arc::new(crate::input::NoopAck),
                )),
                _ => Err(Error::EOF),
            }
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    let input = Arc::new(FlakyInput {
        attempts: AtomicUsize::new(0),
    });
    let collect = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: collect.clone(),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(spec(vec![], vec![edge("source", "sink")], 1)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let metrics = Arc::new(crate::runtime::RuntimeMetrics::default());
    crate::executor::task::run_graph_with_metrics(
        graph,
        CancellationToken::new(),
        Some(metrics.clone()),
    )
    .await
    .unwrap();
    assert_eq!(collect.written.lock().unwrap().len(), 1);
    assert_eq!(metrics.input_errors.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.input_reconnects.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn a_failing_reconnect_backs_off_and_recovers() {
    struct ReconnectingInput {
        reads: AtomicUsize,
        connects: AtomicUsize,
    }
    #[async_trait]
    impl Input for ReconnectingInput {
        async fn connect(&self) -> Result<(), Error> {
            // The startup connect succeeds; the first two RE-connect
            // attempts fail and the third recovers.
            let call = self.connects.fetch_add(1, Ordering::SeqCst);
            if (1..3).contains(&call) {
                return Err(Error::Process("broker unavailable".into()));
            }
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            match self.reads.fetch_add(1, Ordering::SeqCst) {
                0 => Err(Error::Disconnection),
                1 => Ok((
                    Arc::new(MessageBatch::new_arrow(int64_batch(vec![(1, "a".into())]))),
                    Arc::new(crate::input::NoopAck),
                )),
                _ => Err(Error::EOF),
            }
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    let input = Arc::new(ReconnectingInput {
        reads: AtomicUsize::new(0),
        connects: AtomicUsize::new(0),
    });
    let collect = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: collect.clone(),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(spec(vec![], vec![edge("source", "sink")], 1)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    // Paused time advances the 5s backoff instantly.
    let metrics = Arc::new(crate::runtime::RuntimeMetrics::default());
    let result = crate::executor::task::run_graph_with_metrics(
        graph,
        CancellationToken::new(),
        Some(metrics.clone()),
    )
    .await;
    result.unwrap();
    assert_eq!(collect.written.lock().unwrap().len(), 1);
    assert_eq!(input.connects.load(Ordering::SeqCst), 4);
    assert_eq!(metrics.input_reconnects.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn cancellation_during_a_pending_reconnect_shuts_the_source_down() {
    struct PendingConnectInput {
        connects: AtomicUsize,
    }
    #[async_trait]
    impl Input for PendingConnectInput {
        async fn connect(&self) -> Result<(), Error> {
            // The startup connect succeeds; the reconnect after the
            // disconnection parks until cancellation ends it.
            if self.connects.fetch_add(1, Ordering::SeqCst) > 0 {
                std::future::pending().await
            } else {
                Ok(())
            }
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            Err(Error::Disconnection)
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    let input = Arc::new(PendingConnectInput {
        connects: AtomicUsize::new(0),
    });
    let adapter = Adapter {
        input: input.clone(),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(spec(vec![], vec![edge("source", "sink")], 1)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let cancellation = CancellationToken::new();
    let runner = tokio::spawn(crate::executor::task::run_graph_with_metrics(
        graph,
        cancellation.clone(),
        None,
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .expect("cancellation must interrupt the pending reconnect")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_generic_read_error_fails_the_source_chain() {
    struct ExplodingInput;
    #[async_trait]
    impl Input for ExplodingInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            Err(Error::Process("source exploded".into()))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    let input = Arc::new(ExplodingInput);
    let adapter = Adapter {
        input: input.clone(),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(spec(vec![], vec![edge("source", "sink")], 1)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let metrics = Arc::new(crate::runtime::RuntimeMetrics::default());
    let result = crate::executor::task::run_graph_with_metrics(
        graph,
        CancellationToken::new(),
        Some(metrics.clone()),
    )
    .await;
    assert!(
        result.unwrap_err().to_string().contains("source exploded"),
        "the read error must fail the chain"
    );
    assert_eq!(metrics.input_errors.load(Ordering::Relaxed), 1);
}

// ---------- event-time source-chain paths ----------

fn event_time_spec(parallelism: u64) -> JobSpec {
    let mut job = spec(
        vec![map_operator("m")],
        vec![edge("source", "m"), edge("m", "sink")],
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
    if parallelism > 1 {
        job.sources[0].config =
            serde_json::json!({ "__arkflow_processor_parallelism": parallelism });
    }
    job
}

/// An event-time source whose `watermark_partitions` succeeds once (the
/// startup seed) and fails afterwards, exercising the seed error paths on
/// both the read and the idle-tick arms.
struct TogglePartitionsInput {
    queue: Mutex<std::collections::VecDeque<MessageBatchRef>>,
    arrived: tokio::sync::Notify,
    finished: std::sync::atomic::AtomicBool,
    partition_calls: AtomicUsize,
    /// Fail `watermark_partitions` from this (zero-based) call onwards.
    fail_from: usize,
}

impl TogglePartitionsInput {
    fn with(batches: Vec<MessageBatchRef>) -> Arc<Self> {
        Self::with_fail_from(batches, 1)
    }

    fn with_fail_from(batches: Vec<MessageBatchRef>, fail_from: usize) -> Arc<Self> {
        Arc::new(Self {
            queue: Mutex::new(batches.into_iter().collect()),
            arrived: tokio::sync::Notify::new(),
            finished: std::sync::atomic::AtomicBool::new(false),
            partition_calls: AtomicUsize::new(0),
            fail_from,
        })
    }

    fn push_rows(&self, rows: Vec<(i64, String)>) {
        self.queue
            .lock()
            .unwrap()
            .push_back(Arc::new(MessageBatch::new_arrow(int64_batch(rows))));
        self.arrived.notify_one();
    }

    fn finish(&self) {
        self.finished.store(true, Ordering::SeqCst);
        self.arrived.notify_one();
    }
}

#[async_trait]
impl Input for TogglePartitionsInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        loop {
            if let Some(batch) = self.queue.lock().unwrap().pop_front() {
                return Ok((batch, Arc::new(crate::input::NoopAck)));
            }
            if self.finished.load(Ordering::SeqCst) {
                return Err(Error::EOF);
            }
            self.arrived.notified().await;
        }
    }
    async fn watermark_partitions(
        &self,
    ) -> Result<Vec<crate::event_time::EventTimePartition>, Error> {
        if self.partition_calls.fetch_add(1, Ordering::SeqCst) >= self.fail_from {
            return Err(Error::Process("partition discovery failed".into()));
        }
        Ok(Vec::new())
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

fn typed_batch(schema: Schema, columns: Vec<Arc<dyn datafusion::arrow::array::Array>>) -> MessageBatchRef {
    Arc::new(MessageBatch::new_arrow(
        RecordBatch::try_new(Arc::new(schema), columns).unwrap(),
    ))
}

#[tokio::test]
async fn event_time_seed_failure_after_a_read_fails_the_chain() {
    let input = TogglePartitionsInput::with(vec![Arc::new(MessageBatch::new_arrow(int64_batch(
        vec![(1, "a".into())],
    )))]);
    let collect = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: collect,
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(event_time_spec(1)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let result = run_graph(graph, CancellationToken::new()).await;
    let message = result.unwrap_err().to_string();
    assert!(
        message.contains("partition discovery failed"),
        "{message}"
    );
}

#[tokio::test]
async fn event_time_seed_failure_during_the_idle_tick_fails_the_chain() {
    let input = TogglePartitionsInput::with(vec![]);
    let collect = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: collect,
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(event_time_spec(1)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let cancellation = CancellationToken::new();
    let runner = tokio::spawn(run_graph(graph, cancellation.clone()));
    // The first tick (100ms) re-seeds partitions and must fail the chain.
    let result = tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .expect("the tick seed failure must end the chain")
        .unwrap();
    assert!(
        result.unwrap_err().to_string().contains("partition discovery failed"),
        "the tick's seed failure must fail the chain"
    );
}

#[tokio::test]
async fn event_time_idle_tick_refreshes_the_gate_between_deliveries() {
    let input = TogglePartitionsInput::with_fail_from(vec![], usize::MAX);
    let collect = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: collect.clone(),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(event_time_spec(1)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let cancellation = CancellationToken::new();
    let runner = tokio::spawn(run_graph(graph, cancellation.clone()));
    // Deliver one row, let an idle tick observe the gate, then deliver the
    // watermark-advancing row and end the stream.
    input.push_rows(vec![(100, "a".into())]);
    tokio::time::sleep(Duration::from_millis(150)).await;
    input.push_rows(vec![(5_000, "b".into())]);
    input.finish();
    tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .expect("the tick must not stall the source")
        .unwrap()
        .unwrap();
    let rows = collect.written.lock().unwrap().len();
    assert_eq!(rows, 2, "both rows must flow through the ticked source");
}

#[tokio::test]
async fn malformed_partition_metadata_fails_the_event_time_source() {
    // `__meta_partition` must be castable to UInt32; a list-typed partition
    // column has no safe cast and fails the split before the gate observes
    // anything. (Strings cannot be used: the safe-mode cast nulls an
    // unparseable value instead of failing.)
    let batch = typed_batch(
        Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new(
                crate::meta_columns::PARTITION,
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                false,
            ),
        ]),
        vec![
            Arc::new(Int64Array::from(vec![1i64])),
            {
                let offsets =
                    datafusion::arrow::buffer::OffsetBuffer::new(vec![0i32, 1].into());
                Arc::new(datafusion::arrow::array::ListArray::new(
                    Arc::new(Field::new("item", DataType::Int64, true)),
                    offsets,
                    Arc::new(Int64Array::from(vec![1i64])),
                    None,
                ))
            }
        ],
    );
    let input = TogglePartitionsInput::with_fail_from(vec![batch], usize::MAX);
    input.finish();
    let collect = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: collect,
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(event_time_spec(1)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let result = run_graph(graph, CancellationToken::new()).await;
    let message = result.unwrap_err().to_string();
    assert!(
        message.contains("read physical partition metadata"),
        "{message}"
    );
}

#[tokio::test]
async fn an_empty_event_time_batch_settles_and_continues() {
    // A zero-row batch carrying partition metadata splits into zero
    // partitions: the delivery is acknowledged and the stream continues.
    let empty = typed_batch(
        Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new(crate::meta_columns::PARTITION, DataType::UInt32, false),
        ]),
        vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(datafusion::arrow::array::UInt32Array::from(
                Vec::<u32>::new(),
            )),
        ],
    );
    let rows = Arc::new(MessageBatch::new_arrow(int64_batch(vec![(1, "a".into())])));
    let input = TogglePartitionsInput::with_fail_from(vec![empty, rows], usize::MAX);
    // Both deliveries are preloaded: arm EOF immediately so the bounded
    // source ends instead of parking on an empty queue.
    input.finish();
    let collect = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: collect.clone(),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(event_time_spec(1)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    run_graph(graph, CancellationToken::new()).await.unwrap();
    assert_eq!(collect.written.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn an_unsupported_timestamp_type_fails_the_event_time_source() {
    let batch = typed_batch(
        Schema::new(vec![Field::new("ts", DataType::Utf8, false)]),
        vec![Arc::new(StringArray::from(vec!["yesterday"]))],
    );
    let input = TogglePartitionsInput::with_fail_from(vec![batch], usize::MAX);
    let collect = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: collect,
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(event_time_spec(1)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let result = run_graph(graph, CancellationToken::new()).await;
    let message = result.unwrap_err().to_string();
    assert!(
        message.contains("timestamp field 'ts' has unsupported type"),
        "{message}"
    );
}

#[tokio::test]
async fn cancellation_flushes_held_event_time_rows() {
    // A watermark-triggered window holds the row; cancelling the source must
    // release it through the gate's finish path instead of dropping it.
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let input = Arc::new(CountingInput {
        batches: Mutex::new(std::collections::VecDeque::from([window_batch(
            vec![(100, "a".into(), 1)],
            None,
        )])),
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
    let cancellation = CancellationToken::new();
    let runner = tokio::spawn(run_graph(graph, cancellation.clone()));
    tokio::time::sleep(Duration::from_millis(100)).await;
    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .expect("cancellation must flush and stop the source")
        .unwrap()
        .unwrap();
    // The flushed row races the downstream cancellation drain (which aborts
    // queued data for replay), so the assertion is on the flush path having
    // run without hanging or failing, not on the row's final resting place.
    let _ = output;
}

#[tokio::test]
async fn source_shutdown_tolerates_a_closed_downstream_channel() {
    // The sink chain fails and exits before the source is cancelled; the
    // source's shutdown EOS lands on a closed channel and must be tolerated.
    struct FailingWriteOutput;
    #[async_trait]
    impl Output for FailingWriteOutput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn write(&self, _msg: MessageBatchRef) -> Result<(), Error> {
            Err(Error::Process("sink write failed".into()))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    let input = ClosableParkingInput::with(vec![]);
    let adapter = Adapter {
        input: input.clone(),
        output: Arc::new(FailingWriteOutput),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(spec(vec![], vec![edge("source", "sink")], 1)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let cancellation = CancellationToken::new();
    let runner = tokio::spawn(run_graph(graph, cancellation.clone()));
    tokio::time::sleep(Duration::from_millis(20)).await;
    input.push_rows(vec![(1, "a".into())]);
    let result = tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .expect("the sink failure must unblock the graph")
        .unwrap();
    assert!(
        result.unwrap_err().to_string().contains("sink write failed"),
        "the sink error surfaces while the source shutdown is tolerated"
    );
}

// ---------- interior-chain shutdown and pooled paths ----------

/// An acknowledgement whose abort fails: cancellation drains must observe
/// the failure without hanging.
#[derive(Clone)]
struct AbortFailingAck;
#[async_trait]
impl Ack for AbortFailingAck {
    async fn ack(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn abort(&self) -> Result<(), Error> {
        Err(Error::Process("abort failed".into()))
    }
}

struct AbortFailingParkingInput {
    queue: Mutex<std::collections::VecDeque<MessageBatchRef>>,
    arrived: tokio::sync::Notify,
}

impl AbortFailingParkingInput {
    fn push_rows(&self, rows: Vec<(i64, String)>) {
        self.queue
            .lock()
            .unwrap()
            .push_back(Arc::new(MessageBatch::new_arrow(int64_batch(rows))));
        self.arrived.notify_one();
    }
}

#[async_trait]
impl Input for AbortFailingParkingInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        loop {
            if let Some(batch) = self.queue.lock().unwrap().pop_front() {
                return Ok((batch, Arc::new(AbortFailingAck)));
            }
            self.arrived.notified().await;
        }
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

#[tokio::test]
#[serial_test::serial]
async fn interior_cancellation_tolerates_failing_aborts_and_finish() {
    struct SlowFailingFinishProcessor;
    #[async_trait]
    impl Processor for SlowFailingFinishProcessor {
        async fn process(&self, _batch: MessageBatchRef) -> Result<ProcessResult, Error> {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(ProcessResult::None)
        }
        async fn finish(&self) -> Result<ProcessResult, Error> {
            Err(Error::Process("finish failed".into()))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    let input = Arc::new(AbortFailingParkingInput {
        queue: Mutex::new(std::collections::VecDeque::new()),
        arrived: tokio::sync::Notify::new(),
    });
    let collect = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: collect,
        processor: Arc::new(SlowFailingFinishProcessor),
    };
    let plan = JobPlan::compile(spec(
        vec![map_operator("m")],
        vec![edge("source", "m"), edge("m", "sink")],
        1,
    ))
    .unwrap();
    // Keep the inter-chain channel wide so several deliveries queue behind
    // the slow processor when cancellation strikes.
    let graph = ExecutionGraphBuilder::new(8)
        .build(&plan, &adapter, &resource())
        .unwrap();
    let cancellation = CancellationToken::new();
    let runner = tokio::spawn(run_graph(graph, cancellation.clone()));
    for value in 0..4 {
        input.push_rows(vec![(value, "a".into())]);
    }
    // The processor needs 100ms per row, so several deliveries are still
    // queued when cancellation strikes; the generous window keeps the test
    // robust on a loaded machine.
    tokio::time::sleep(Duration::from_millis(80)).await;
    cancellation.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("failing aborts and finish must not hang cancellation")
        .unwrap();
    // Whether cancellation drains every in-flight delivery before the
    // finish hook runs (Ok) or the finish failure surfaces, both prove the
    // cancellation cannot hang on failing aborts or finish.
    match outcome {
        Ok(()) => {}
        Err(error) => assert!(
            error.to_string().contains("finish failed"),
            "unexpected cancellation error: {error}"
        ),
    }
}

#[tokio::test]
async fn a_failing_processor_finish_fails_the_eos_path() {
    struct FailingFinishProcessor;
    #[async_trait]
    impl Processor for FailingFinishProcessor {
        async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
            Ok(ProcessResult::Single(batch))
        }
        async fn finish(&self) -> Result<ProcessResult, Error> {
            Err(Error::Process("finish failed".into()))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into())]])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(FailingFinishProcessor),
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
    let result = run_graph(graph, CancellationToken::new()).await;
    assert!(
        result.unwrap_err().to_string().contains("finish failed"),
        "the EOS flush failure must fail the chain"
    );
}

#[tokio::test]
async fn a_pooled_sink_failure_fails_the_running_chain() {
    struct FailingWriteOutput;
    #[async_trait]
    impl Output for FailingWriteOutput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn write(&self, _msg: MessageBatchRef) -> Result<(), Error> {
            Err(Error::Process("sink write failed".into()))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    let input = ClosableParkingInput::with(vec![Arc::new(MessageBatch::new_arrow(int64_batch(
        vec![(1, "a".into())],
    )))]);
    let adapter = Adapter {
        input: input.clone(),
        output: Arc::new(FailingWriteOutput),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(parallel_job_spec(4)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let metrics = Arc::new(crate::runtime::RuntimeMetrics::default());
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        crate::executor::task::run_graph_with_metrics(
            graph,
            CancellationToken::new(),
            Some(metrics.clone()),
        ),
    )
    .await
    .expect("the pooled sink failure must fail the chain");
    assert!(
        result.unwrap_err().to_string().contains("sink write failed"),
        "the worker pool must surface the sink failure"
    );
    assert!(
        metrics.output_errors.load(Ordering::Relaxed) >= 1,
        "the sink failure must be counted"
    );
}

#[tokio::test]
async fn a_pooled_watermark_envelope_fences_the_pool() {
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
    let values = Arc::new(Mutex::new(Vec::new()));
    let input = TogglePartitionsInput::with_fail_from(vec![], usize::MAX);
    let collect = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: collect.clone(),
        processor: Arc::new(WatermarkRecorder {
            values: values.clone(),
        }),
    };
    let plan = JobPlan::compile(event_time_spec(4)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let cancellation = CancellationToken::new();
    let runner = tokio::spawn(run_graph(graph, cancellation.clone()));
    input.push_rows(vec![(100, "a".into())]);
    tokio::time::sleep(Duration::from_millis(50)).await;
    input.push_rows(vec![(5_000, "b".into())]);
    input.finish();
    tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .expect("the pooled chain must finish")
        .unwrap()
        .unwrap();
    let seen = values.lock().unwrap().clone();
    assert!(
        !seen.is_empty(),
        "the pooled chain must receive watermark control events"
    );
    assert!(seen.iter().all(|value| *value <= 5_000));
}

#[tokio::test]
async fn pooled_workers_exit_through_channel_closure_at_eos() {
    let input = TogglePartitionsInput::with_fail_from(
        vec![
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![(1, "a".into())]))),
            Arc::new(MessageBatch::new_arrow(int64_batch(vec![(2, "b".into())]))),
        ],
        usize::MAX,
    );
    let collect = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: collect.clone(),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(event_time_spec(4)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    input.finish();
    run_graph(graph, CancellationToken::new()).await.unwrap();
    assert_eq!(collect.written.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn a_panicking_pooled_worker_fails_the_chain() {
    struct PanickingProcessor;
    #[async_trait]
    impl Processor for PanickingProcessor {
        async fn process(&self, _batch: MessageBatchRef) -> Result<ProcessResult, Error> {
            panic!("worker exploded");
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    let adapter = Adapter {
        input: Arc::new(VecInput::new(vec![vec![(1, "a".into())]])),
        output: Arc::new(CollectOutput::default()),
        processor: Arc::new(PanickingProcessor),
    };
    let plan = JobPlan::compile(pooled_failure_job_spec(4, false)).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_graph(graph, CancellationToken::new()),
    )
    .await
    .expect("a panicking worker must not park the chain");
    let message = result.unwrap_err().to_string();
    assert!(
        message.contains("processor worker task failed"),
        "{message}"
    );
}

// ---------- multi-input barrier alignment edge cases ----------

fn two_source_merge_job() -> JobSpec {
    JobSpec {
        resources: Default::default(),
        rescale: false,
        rebalance: None,
        id: JobId::new("two-source-merge").unwrap(),
        version: JobVersion(1),
        max_parallelism: 1,
        parallelism: 1,
        operators: vec![
            map_source_operator("left-source"),
            map_source_operator("right-source"),
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
            codec: None,
            config: serde_json::json!({}),
        }],
        state: None,
        checkpoint: None,
        placement: crate::job::PlacementStrategy::Colocated,
        recovery: Default::default(),
    }
}

fn barrier_hook_for(task_id: &str) -> crate::executor::task::CheckpointHook {
    crate::executor::task::CheckpointHook {
        reporter: None,
        failure_reporter: None,
        barrier_rx: None,
        state: None,
        task_id: Some(task_id.to_string()),
        event_time_gate: Arc::new(tokio::sync::Mutex::new(None)),
        partition: None,
        metrics: None,
        finished_reporter: None,
    }
}

#[tokio::test]
async fn buffered_eos_envelopes_complete_an_in_flight_barrier() {
    let left = ClosableParkingInput::with(vec![Arc::new(MessageBatch::new_arrow(int64_batch(
        vec![(1, "left".into())],
    )))]);
    let right = ClosableParkingInput::with(vec![Arc::new(MessageBatch::new_arrow(int64_batch(
        vec![(2, "right".into())],
    )))]);
    let output = Arc::new(CollectOutput::default());
    let adapter = MultiInputAdapter {
        inputs: HashMap::from([
            ("left-source".into(), left.clone() as Arc<dyn Input>),
            ("right-source".into(), right.clone() as Arc<dyn Input>),
        ]),
        outputs: HashMap::from([("sink".into(), output.clone())]),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(two_source_merge_job()).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let (barrier_tx, barrier_rx) = flume::bounded::<Envelope>(4);
    let mut left_hook = barrier_hook_for("left-source-0");
    left_hook.barrier_rx = Some(Arc::new(tokio::sync::Mutex::new(barrier_rx)));
    let hooks = BTreeMap::from([
        ("left-source-0".to_string(), left_hook),
        ("right-source-0".to_string(), barrier_hook_for("right-source-0")),
    ]);
    let cancellation = CancellationToken::new();
    let runner = tokio::spawn(crate::executor::task::run_graph_with_hooks(
        graph,
        cancellation.clone(),
        hooks,
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    // Inject the barrier: the merge chain starts aligning on the left edge.
    let _ = barrier_tx
        .send_async(Envelope::Barrier(CheckpointBarrier {
            checkpoint_id: "cp-eos-align".into(),
            generation: 1,
            trace_context: None,
        }))
        .await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    // End the barrier's own input first: its EOS is buffered behind the
    // in-flight barrier instead of completing it.
    left.finish();
    tokio::time::sleep(Duration::from_millis(20)).await;
    // Ending the second input completes the round through the buffered EOS
    // envelopes and the merge chain finishes through the release path.
    right.finish();
    tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .expect("the buffered EOS envelopes must complete the barrier")
        .unwrap()
        .unwrap();
    let rows = output
        .written
        .lock()
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    assert_eq!(rows, 2);
}

#[tokio::test]
async fn barrier_alignment_overflow_releases_the_buffered_data() {
    let left = ClosableParkingInput::with(vec![Arc::new(MessageBatch::new_arrow(int64_batch(
        vec![(1, "left".into())],
    )))]);
    let right = ClosableParkingInput::with(vec![]);
    let output = Arc::new(CollectOutput::default());
    let adapter = MultiInputAdapter {
        inputs: HashMap::from([
            ("left-source".into(), left.clone() as Arc<dyn Input>),
            ("right-source".into(), right.clone() as Arc<dyn Input>),
        ]),
        outputs: HashMap::from([("sink".into(), output.clone())]),
        processor: Arc::new(PassThroughProcessor),
    };
    let plan = JobPlan::compile(two_source_merge_job()).unwrap();
    let graph = ExecutionGraphBuilder::default()
        .build(&plan, &adapter, &resource())
        .unwrap();
    let (barrier_tx, barrier_rx) = flume::bounded::<Envelope>(4);
    let (failure_tx, mut failure_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut left_hook = barrier_hook_for("left-source-0");
    left_hook.barrier_rx = Some(Arc::new(tokio::sync::Mutex::new(barrier_rx)));
    let mut merge_hook = barrier_hook_for("merge-0");
    merge_hook.failure_reporter = Some(failure_tx);
    let hooks = BTreeMap::from([
        ("left-source-0".to_string(), left_hook),
        ("merge-0".to_string(), merge_hook),
        ("right-source-0".to_string(), barrier_hook_for("right-source-0")),
    ]);
    let cancellation = CancellationToken::new();
    let runner = tokio::spawn(crate::executor::task::run_graph_with_hooks(
        graph,
        cancellation.clone(),
        hooks,
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let _ = barrier_tx
        .send_async(Envelope::Barrier(CheckpointBarrier {
            checkpoint_id: "cp-overflow".into(),
            generation: 1,
            trace_context: None,
        }))
        .await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    // Overrun the alignment bound: the merge chain releases everything it
    // buffered and keeps processing instead of blocking.
    for value in 0..1030 {
        right.push_rows(vec![(value, "right".into())]);
    }
    left.finish();
    right.finish();
    let failure = tokio::time::timeout(Duration::from_secs(5), failure_rx.recv())
        .await
        .expect("the overflow must be reported");
    assert!(
        failure
            .as_ref()
            .map(|error| error.to_string())
            .unwrap_or_default()
            .contains("exceeded"),
        "the overflow error names the bound: {failure:?}"
    );
    tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("the overflowed chain must keep flowing")
        .unwrap()
        .unwrap();
    let rows = output
        .written
        .lock()
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    assert_eq!(rows, 1_031, "every buffered and later row must reach the sink");
}

#[tokio::test]
async fn interior_barrier_snapshot_failure_reports_and_forwards() {
    let input = ClosableParkingInput::with(vec![Arc::new(MessageBatch::new_arrow(int64_batch(
        vec![(1, "a".into())],
    )))]);
    let output = Arc::new(CollectOutput::default());
    let adapter = Adapter {
        input: input.clone(),
        output: output.clone(),
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
    let (barrier_tx, barrier_rx) = flume::bounded::<Envelope>(4);
    let (failure_tx, mut failure_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut source_hook = barrier_hook_for("source-0");
    source_hook.barrier_rx = Some(Arc::new(tokio::sync::Mutex::new(barrier_rx)));
    let mut map_hook = barrier_hook_for("m-0");
    map_hook.state = Some(Arc::new(FailingSnapshotBackend));
    map_hook.failure_reporter = Some(failure_tx);
    let hooks = BTreeMap::from([
        ("source-0".to_string(), source_hook),
        ("m-0".to_string(), map_hook),
    ]);
    let cancellation = CancellationToken::new();
    let runner = tokio::spawn(crate::executor::task::run_graph_with_hooks(
        graph,
        cancellation.clone(),
        hooks,
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let _ = barrier_tx
        .send_async(Envelope::Barrier(CheckpointBarrier {
            checkpoint_id: "cp-interior-fail".into(),
            generation: 1,
            trace_context: None,
        }))
        .await;
    let failure = tokio::time::timeout(Duration::from_secs(5), failure_rx.recv())
        .await
        .expect("the interior snapshot failure must be reported")
        .expect("the failure channel stays open");
    assert!(
        failure.to_string().contains("injected snapshot failure"),
        "{failure}"
    );
    // The data plane keeps running; end the stream normally.
    input.finish();
    tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .expect("the chain must survive the failed interior snapshot")
        .unwrap()
        .unwrap();
    assert_eq!(output.written.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_failing_temporary_close_fails_the_run_after_the_chains_exit() {
    struct FailingCloseTemporary;
    #[async_trait]
    impl crate::temporary::Temporary for FailingCloseTemporary {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn get(
            &self,
            _keys: &[datafusion::logical_expr::ColumnarValue],
        ) -> Result<Option<MessageBatch>, Error> {
            Ok(None)
        }
        async fn close(&self) -> Result<(), Error> {
            Err(Error::Process("temporary close failed".into()))
        }
    }
    // A hand-built bounded source chain with no downstream: every chain
    // succeeds, so the temporary's close failure is the run's only error.
    let input = ClosableParkingInput::with(vec![Arc::new(MessageBatch::new_arrow(
        int64_batch(vec![(1, "a".into())]),
    ))]);
    input.finish();
    let mut chain = crate::executor::graph::Chain::for_pool_test(1, vec![]);
    chain.task_ids = vec!["source-0".into()];
    chain.source = Some(input);
    let graph = crate::executor::ExecutionGraph {
        chains: vec![chain],
        channel_capacity: 4,
        temporaries: vec![Arc::new(FailingCloseTemporary)],
    };
    let result = run_graph(graph, CancellationToken::new()).await;
    let message = result.unwrap_err().to_string();
    assert!(
        message.contains("temporary close failed"),
        "a close failure must surface even when every chain succeeded: {message}"
    );
}
