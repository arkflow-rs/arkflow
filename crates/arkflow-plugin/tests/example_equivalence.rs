//! Legacy-behavior equivalence regression (task 4.5): runnable example
//! configurations execute to completion on the unified kernel, and their
//! OUTPUT is checked (row values, delivery order) instead of only their exit
//! status. The durability example is additionally run with a pre-seeded WAL
//! to prove unacknowledged entries replay before new input.

use arkflow_core::config::EngineConfig;
use arkflow_core::executor::run_job;
use arkflow_core::executor::stream_adapter::StreamJobAdapter;
use arkflow_core::executor::stream_compiler::compile_stream;
use arkflow_core::output::{register_output_builder, Output, OutputBuilder, OutputConfig};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, Resource};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Batches delivered to the recording sink in write order.
static RECORDED: std::sync::Mutex<Vec<MessageBatchRef>> = std::sync::Mutex::new(Vec::new());

struct RecordingOutput;

#[async_trait::async_trait]
impl Output for RecordingOutput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
        RECORDED.lock().expect("sink recording poisoned").push(msg);
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

struct RecordingOutputBuilder;

impl OutputBuilder for RecordingOutputBuilder {
    fn build(
        &self,
        _name: Option<&str>,
        _config: &Option<serde_json::Value>,
        _codec: Option<Arc<dyn arkflow_core::codec::Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        Ok(Arc::new(RecordingOutput))
    }
}

fn setup() {
    let _ = arkflow_plugin::input::init();
    let _ = arkflow_plugin::output::init();
    let _ = arkflow_plugin::processor::init();
    let _ = arkflow_plugin::buffer::init();
    let _ = arkflow_plugin::codec::init();
    let _ = register_output_builder("__equivalence_sink", Arc::new(RecordingOutputBuilder));
}

/// Drain batches recorded since the last call.
fn take_recorded() -> Vec<MessageBatchRef> {
    std::mem::take(&mut *RECORDED.lock().expect("sink recording poisoned"))
}

fn recording_output() -> OutputConfig {
    OutputConfig {
        output_type: "__equivalence_sink".into(),
        name: None,
        codec: None,
        config: None,
    }
}

async fn run_example_on_kernel(path: &str, wal_path: &std::path::Path) -> usize {
    let mut config = EngineConfig::from_file(path).unwrap();
    for stream in &mut config.streams {
        if let Some(durability) = &mut stream.durability {
            // Keep the regression rerunnable: a previous interrupted test
            // must not turn the next run into an unbounded historical replay.
            durability.path = wal_path.to_string_lossy().into_owned();
        }
        stream.output = recording_output();
    }
    // Seed the WAL with two entries a "previous crashed run" never
    // acknowledged: replay must deliver them, in order, before the new input.
    if let Some(durability) = config
        .streams
        .iter()
        .find_map(|stream| stream.durability.clone())
    {
        let wal = arkflow_core::wal::Wal::open(&durability).unwrap();
        for (value, sensor) in [(100, "seeded-1"), (200, "seeded-2")] {
            let payload = format!(r#"{{"value": {value}, "sensor": "{sensor}"}}"#);
            let batch = Arc::new(MessageBatch::new_binary(vec![payload.into_bytes()]).unwrap());
            wal.append(&batch).await.unwrap();
        }
        wal.close().await.unwrap();
    }

    let mut completed = 0;
    for (index, stream) in config.streams.iter().enumerate() {
        let spec = compile_stream(stream, index).unwrap();
        let adapter =
            StreamJobAdapter::with_temporary(stream.durability.as_ref(), stream.temporary.clone())
                .unwrap();
        let mut resource = adapter.build_resource().unwrap();
        tokio::time::timeout(
            // The workspace runs many Arrow/DataFusion tests concurrently;
            // keep the bounded-completion assertion meaningful without
            // making scheduler contention look like a kernel hang. The
            // durability example runs 10 generate rows under group commit
            // (small, but disk-bound when the whole suite competes for IO).
            Duration::from_secs(300),
            run_job(&spec, &adapter, &mut resource, CancellationToken::new()),
        )
        .await
        .expect("bounded example must finish on the kernel")
        .unwrap_or_else(|error| panic!("{path} stream {index} failed: {error}"));
        completed += 1;
    }
    completed
}

#[tokio::test(flavor = "multi_thread")]
async fn bounded_examples_run_to_completion_on_kernel() {
    setup();
    take_recorded();
    let root = env!("CARGO_MANIFEST_DIR");
    // Bounded inline pipeline mirroring drop_output_example (which has no
    // `count`, i.e. unbounded).
    let yaml = r#"
streams:
  - id: equivalence
    input:
      type: "generate"
      context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
      interval: 1ns
      batch_size: 100
      count: 1000
    pipeline:
      thread_num: 2
      processors:
        - type: "json_to_arrow"
        - type: "sql"
          query: "SELECT count(*) AS n FROM flow WHERE value >= 10 GROUP BY sensor"
        - type: "arrow_to_json"
    output:
      type: "drop"
"#;
    let mut config: EngineConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.streams.len(), 1);
    config.streams[0].output = recording_output();
    let spec = compile_stream(&config.streams[0], 0).unwrap();
    let adapter = StreamJobAdapter::new(config.streams[0].durability.as_ref()).unwrap();
    let mut resource = adapter.build_resource().unwrap();
    tokio::time::timeout(
        Duration::from_secs(20),
        run_job(&spec, &adapter, &mut resource, CancellationToken::new()),
    )
    .await
    .expect("inline example must finish")
    .unwrap();

    // Output equivalence, not just completion: the generate input produces
    // ten 100-row batches, and the streaming SQL processor aggregates EACH
    // batch independently — ten rows of n=100, never a silently dropped or
    // duplicated batch.
    let recorded = take_recorded();
    let payloads: Vec<Vec<u8>> = recorded
        .iter()
        .flat_map(|batch| {
            batch
                .to_binary("__value__")
                .unwrap()
                .into_iter()
                .map(<[u8]>::to_vec)
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        payloads.len(),
        10,
        "one aggregate row per input batch must be delivered"
    );
    let mut counted = 0_i64;
    for payload in &payloads {
        let row: serde_json::Value = serde_json::from_slice(payload).unwrap();
        assert_eq!(row["n"], 100, "each batch aggregates its 100 rows: {row}");
        counted += row["n"].as_i64().unwrap();
    }
    assert_eq!(
        counted, 1000,
        "every generated row must be counted exactly once"
    );

    // WAL durability example (bounded count: 10, group commit), with two
    // unacknowledged entries pre-seeded into the WAL.
    let durable = format!("{root}/../../examples/durability_example.yaml");
    let wal_path = tempfile::tempdir().unwrap();
    let completed = run_example_on_kernel(&durable, wal_path.path()).await;
    assert_eq!(completed, 1, "durability example must run one stream");

    let recorded = take_recorded();
    let delivered: Vec<serde_json::Value> = recorded
        .iter()
        .flat_map(|batch| {
            batch
                .to_binary("__value__")
                .expect("binary payload")
                .into_iter()
                .map(|bytes| serde_json::from_slice(bytes).expect("JSON payload"))
        })
        .collect();
    assert_eq!(
        delivered.len(),
        12,
        "two replayed entries plus ten generated rows must be delivered"
    );
    // Replay order first, exactly as persisted.
    assert_eq!(delivered[0]["sensor"], "seeded-1", "replay order");
    assert_eq!(delivered[0]["value"], 100, "replay payload");
    assert_eq!(delivered[1]["sensor"], "seeded-2", "replay order");
    assert_eq!(delivered[1]["value"], 200, "replay payload");
    // Then the live input, unchanged by the pipeline.
    for row in &delivered[2..] {
        assert_eq!(row["sensor"], "temp_1", "live payload: {row}");
        assert_eq!(row["value"], 10, "live payload: {row}");
    }
}
