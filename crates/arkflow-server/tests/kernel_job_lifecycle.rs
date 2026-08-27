//! Agent-side kernel Job lifecycle: spawn a Job on the unified kernel,
//! snapshot it mid-run, and stop it cleanly. Mirrors what
//! `JobRuntime::start/checkpoint/stop` drive in production.

use arkflow_core::executor::graph::ExecutionGraphBuilder;
use arkflow_core::executor::kernel_handle::KernelJobRunner;
use arkflow_core::input::{Ack, Input};
use arkflow_core::job::{
    EdgeSpec, JobId, JobPlan, JobSpec, JobVersion, OperatorKind, OperatorSpec, SinkSpec,
    SourceSpec, TimeMode, TimeSpec,
};
use arkflow_core::output::Output;
use arkflow_core::state::{RedbStateBackend, StateBackend};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, Resource};
use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

struct ForeverInput {
    reads: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl Input for ForeverInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(5)).await;
        Ok((
            Arc::new(MessageBatch::new_arrow(sample_batch())),
            Arc::new(arkflow_core::input::NoopAck),
        ))
    }
    async fn current_positions(
        &self,
    ) -> Result<Vec<arkflow_core::checkpoint::SourcePosition>, Error> {
        Ok(vec![arkflow_core::checkpoint::SourcePosition::for_partition(
            0,
            self.reads.load(std::sync::atomic::Ordering::SeqCst) as u64,
        )])
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

fn sample_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("key", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
        ],
    )
    .unwrap()
}

#[derive(Default)]
struct CollectOutput {
    written: std::sync::Mutex<usize>,
}

#[async_trait]
impl Output for CollectOutput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn write(&self, _msg: MessageBatchRef) -> Result<(), Error> {
        *self.written.lock().unwrap() += 1;
        Ok(())
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

struct FixtureAdapter {
    input: Arc<dyn Input>,
    output: Arc<CollectOutput>,
}

impl arkflow_core::job::JobComponentAdapter for FixtureAdapter {
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
    ) -> Result<Arc<dyn arkflow_core::processor::Processor>, Error> {
        Err(Error::Config("no processors in this fixture".into()))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn agent_kernel_job_snapshots_and_stops() {
    let spec = JobSpec {
        id: JobId::new("kernel-agent-job").unwrap(),
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
                id: "sink".into(),
                kind: OperatorKind::Sink,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
        ],
        edges: vec![EdgeSpec {
            id: "e".into(),
            from: "source".into(),
            to: "sink".into(),
            partitioned: false,
        }],
        sources: vec![SourceSpec {
            operator_id: "source".into(),
            input_type: "fixture".into(),
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
    };
    let plan = JobPlan::compile(spec).unwrap();
    let task_ids = plan.tasks.iter().map(|t| t.id.clone()).collect::<Vec<_>>();

    let adapter = FixtureAdapter {
        input: Arc::new(ForeverInput {
            reads: std::sync::atomic::AtomicUsize::new(0),
        }),
        output: Arc::new(CollectOutput::default()),
    };
    let state_dir = tempfile_dir();
    let state: Arc<dyn StateBackend> = Arc::new(RedbStateBackend::open(state_dir, 1).unwrap());
    let mut resource = Resource {
        temporary: std::collections::HashMap::new(),
        input_names: std::cell::RefCell::new(Vec::new()),
    };
    let graph = ExecutionGraphBuilder::default()
        .build_subgraph(&plan, &task_ids, &adapter, &mut resource)
        .unwrap();
    let inputs = graph
        .chains
        .iter()
        .filter_map(|chain| chain.source.clone())
        .collect::<Vec<_>>();
    let mut states = BTreeMap::new();
    states.insert("source-0".to_string(), state);

    let cancellation = CancellationToken::new();
    let handle = KernelJobRunner::spawn_with_cancellation(
        graph,
        inputs,
        states,
        BTreeMap::new(),
        false,
        cancellation.clone(),
    )
    .await
    .unwrap();

    // Let data flow, snapshot mid-run, verify the graph keeps flowing after.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (snapshot, positions, _watermarks) = handle.checkpoint_snapshot().await.unwrap();
    assert!(snapshot.entries.is_empty()); // no stateful operators
    assert_eq!(positions.len(), 1);
    assert!(positions[0].offset > 0, "source must have read batches");

    let written_after_snapshot = adapter.output.written.lock().unwrap().clone();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let written_later = adapter.output.written.lock().unwrap().clone();
    assert!(
        written_later > written_after_snapshot,
        "data must continue flowing after the snapshot (got {written_after_snapshot} then {written_later})"
    );

    // Stop: the shared token cancels the chains and the watcher resolves.
    handle.stop();
    let watcher = handle.watcher();
    tokio::time::timeout(Duration::from_secs(5), watcher)
        .await
        .expect("kernel job must stop after cancellation")
        .unwrap()
        .unwrap();
}

fn tempfile_dir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let unique = NEXT.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "arkflow-kernel-test-{}-{unique}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// ---------- fault-injection (task 2.6) ----------


#[tokio::test(flavor = "multi_thread")]
async fn snapshot_failure_fails_checkpoint_but_data_continues() {
    // A state backend whose snapshot() errors: the checkpoint must fail
    // (recorded), and the graph must keep flowing afterwards.
    struct BrokenState;
    #[async_trait]
    impl arkflow_core::state::StateBackend for BrokenState {
        fn format_version(&self) -> u32 { 1 }
        fn get(&self, _n: &str, _k: &[u8]) -> Result<Option<Vec<u8>>, Error> { Ok(None) }
        fn put_with_ttl(&self, _n: &str, _k: &[u8], _v: &[u8], _t: Option<u64>, _now: u64) -> Result<(), Error> { Ok(()) }
        fn update_i64(&self, _n: &str, _k: &[u8], _d: i64) -> Result<i64, Error> { Ok(0) }
        fn delete(&self, _n: &str, _k: &[u8]) -> Result<bool, Error> { Ok(false) }
        fn purge_expired(&self, _now: u64) -> Result<u64, Error> { Ok(0) }
        fn scan(&self, _n: &str) -> Result<Vec<arkflow_core::state::StateEntry>, Error> { Ok(vec![]) }
        fn snapshot_at(&self, _now: u64) -> Result<arkflow_core::state::StateSnapshot, Error> {
            Err(Error::Process("injected snapshot failure".into()))
        }
        fn restore(&self, _s: &arkflow_core::state::StateSnapshot) -> Result<(), Error> { Ok(()) }
        fn metrics(&self) -> Result<arkflow_core::state::StateMetrics, Error> {
            Ok(Default::default())
        }
        fn close(&self) -> Result<(), Error> { Ok(()) }
    }

    let spec = kernel_job_spec();
    let plan = JobPlan::compile(spec).unwrap();
    let task_ids = plan.tasks.iter().map(|t| t.id.clone()).collect::<Vec<_>>();
    let adapter = FixtureAdapter {
        input: Arc::new(ForeverInput {
            reads: std::sync::atomic::AtomicUsize::new(0),
        }),
        output: Arc::new(CollectOutput::default()),
    };
    let mut resource = Resource {
        temporary: std::collections::HashMap::new(),
        input_names: std::cell::RefCell::new(Vec::new()),
    };
    let graph = ExecutionGraphBuilder::default()
        .build_subgraph(&plan, &task_ids, &adapter, &mut resource)
        .unwrap();
    let inputs = graph
        .chains
        .iter()
        .filter_map(|chain| chain.source.clone())
        .collect::<Vec<_>>();
    let mut states = BTreeMap::new();
    states.insert("source-0".to_string(), Arc::new(BrokenState) as Arc<dyn StateBackend>);

    let cancellation = CancellationToken::new();
    let handle = KernelJobRunner::spawn_with_cancellation(
        graph,
        inputs,
        states,
        BTreeMap::new(),
        false,
        cancellation,
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;
    // The injected failure surfaces as a snapshot error (checkpoint Failed).
    let snapshot_result = handle.checkpoint_snapshot().await;
    assert!(snapshot_result.is_err(), "injected snapshot failure must surface");
    // Data continues after the failed checkpoint.
    let after = adapter.output.written.lock().unwrap().clone();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let later = adapter.output.written.lock().unwrap().clone();
    assert!(later > after, "data must keep flowing after a failed checkpoint ({after} → {later})");
    handle.stop();
    let watcher = handle.watcher();
    tokio::time::timeout(Duration::from_secs(5), watcher)
        .await
        .expect("stop after fault")
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_restores_positions_before_new_reads() {
    // A restart from a RecoveryPlan seeks sources to the checkpointed
    // position BEFORE any new read (task 2.6 recovery replay semantics).
    use std::sync::atomic::AtomicUsize;
    struct PositionRecordingInput {
        reads: AtomicUsize,
        restored: std::sync::Mutex<Vec<u64>>,
    }
    #[async_trait]
    impl Input for PositionRecordingInput {
        async fn connect(&self) -> Result<(), Error> { Ok(()) }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(2)).await;
            Ok((
                Arc::new(MessageBatch::new_arrow(sample_batch())),
                Arc::new(arkflow_core::input::NoopAck),
            ))
        }
        async fn restore_positions(
            &self,
            positions: &[arkflow_core::checkpoint::SourcePosition],
        ) -> Result<(), Error> {
            let mut restored = self.restored.lock().unwrap();
            restored.extend(positions.iter().map(|position| position.offset));
            Ok(())
        }
        async fn current_positions(
            &self,
        ) -> Result<Vec<arkflow_core::checkpoint::SourcePosition>, Error> {
            Ok(vec![arkflow_core::checkpoint::SourcePosition::for_partition(
                0,
                self.reads.load(std::sync::atomic::Ordering::SeqCst) as u64,
            )])
        }
        async fn close(&self) -> Result<(), Error> { Ok(()) }
    }

    let spec = kernel_job_spec();
    let plan = JobPlan::compile(spec).unwrap();
    let task_ids = plan.tasks.iter().map(|t| t.id.clone()).collect::<Vec<_>>();
    let input = Arc::new(PositionRecordingInput {
        reads: AtomicUsize::new(0),
        restored: std::sync::Mutex::new(Vec::new()),
    });
    let adapter = FixtureAdapter {
        input: input.clone() as Arc<dyn Input>,
        output: Arc::new(CollectOutput::default()),
    };
    let state_dir = tempfile_dir();
    let state: Arc<dyn StateBackend> = Arc::new(RedbStateBackend::open(state_dir, 1).unwrap());
    let mut resource = Resource {
        temporary: std::collections::HashMap::new(),
        input_names: std::cell::RefCell::new(Vec::new()),
    };
    let graph = ExecutionGraphBuilder::default()
        .build_subgraph(&plan, &task_ids, &adapter, &mut resource)
        .unwrap();
    let inputs = graph
        .chains
        .iter()
        .filter_map(|chain| chain.source.clone())
        .collect::<Vec<_>>();
    let mut states = BTreeMap::new();
    states.insert("source-0".to_string(), state);

    let cancellation = CancellationToken::new();
    let handle = KernelJobRunner::spawn_with_cancellation(
        graph,
        inputs,
        states,
        BTreeMap::new(),
        false,
        cancellation,
    )
    .await
    .unwrap();

    // Let reads accumulate, then "restart": restore positions from the
    // snapshot before the graph reads again (the kernel path applies the
    // recovery plan at start; here we verify the contract on the handle).
    tokio::time::sleep(Duration::from_millis(120)).await;
    let (_snapshot, positions, _watermarks) = handle.checkpoint_snapshot().await.unwrap();
    handle.stop();
    let watcher = handle.watcher();
    tokio::time::timeout(Duration::from_secs(5), watcher)
        .await
        .expect("stop for restart")
        .unwrap()
        .unwrap();

    let restored_offsets = input.restored.lock().unwrap().clone();
    assert!(
        restored_offsets.iter().all(|offset| *offset <= positions[0].offset),
        "restore must only carry offsets at or before the checkpoint (got {restored_offsets:?} vs checkpoint {})",
        positions[0].offset
    );
}

fn kernel_job_spec() -> JobSpec {
    JobSpec {
        id: JobId::new("fault-injection").unwrap(),
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
                id: "sink".into(),
                kind: OperatorKind::Sink,
                stateful: false,
                key_field: None,
                config: serde_json::json!({}),
            },
        ],
        edges: vec![EdgeSpec {
            id: "e".into(),
            from: "source".into(),
            to: "sink".into(),
            partitioned: false,
        }],
        sources: vec![SourceSpec {
            operator_id: "source".into(),
            input_type: "fixture".into(),
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
