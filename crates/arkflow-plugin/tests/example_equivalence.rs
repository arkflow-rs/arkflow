//! Legacy-behavior equivalence regression (task 4.5): runnable example
//! configurations execute to completion on the unified kernel. Bounded
//! (count-limited) generate examples cover the plain pipeline and the
//! WAL-durability path (replay before new input).

use arkflow_core::config::EngineConfig;
use arkflow_core::executor::run_job;
use arkflow_core::executor::stream_adapter::StreamJobAdapter;
use arkflow_core::executor::stream_compiler::compile_stream;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn setup() {
    let _ = arkflow_plugin::input::init();
    let _ = arkflow_plugin::output::init();
    let _ = arkflow_plugin::processor::init();
    let _ = arkflow_plugin::buffer::init();
    let _ = arkflow_plugin::codec::init();
}

async fn run_example_on_kernel(path: &str, wal_path: &std::path::Path) -> usize {
    let mut config = EngineConfig::from_file(path).unwrap();
    for stream in &mut config.streams {
        if let Some(durability) = &mut stream.durability {
            // Keep the regression rerunnable: a previous interrupted test
            // must not turn the next run into an unbounded historical replay.
            durability.path = wal_path.to_string_lossy().into_owned();
        }
    }
    let mut completed = 0;
    for (index, stream) in config.streams.iter().enumerate() {
        let spec = compile_stream(stream, index).unwrap();
        let adapter =
            StreamJobAdapter::with_temporary(stream.durability.as_ref(), stream.temporary.clone())
                .unwrap();
        let mut resource = adapter.build_resource().unwrap();
        tokio::time::timeout(
            Duration::from_secs(20),
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
    let root = env!("CARGO_MANIFEST_DIR");
    // Bounded inline pipeline mirroring drop_output_example (which has no
    // `count`, i.e. unbounded) plus the WAL-durability example file.
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
    let config: EngineConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.streams.len(), 1);
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

    // WAL durability example (bounded count: 1000) — replay path included.
    let durable = format!("{root}/../../examples/durability_example.yaml");
    let wal_path = tempfile::tempdir().unwrap();
    let completed = run_example_on_kernel(&durable, wal_path.path()).await;
    assert_eq!(completed, 1, "durability example must run one stream");
}
