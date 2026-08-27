//! End-to-end: a compiled StreamConfig (generate → json_to_arrow → sql →
//! stdout/drop) runs on the unified kernel through the real plugin registry.

use arkflow_core::config::EngineConfig;
use arkflow_core::executor::stream_adapter::StreamJobAdapter;
use arkflow_core::executor::stream_compiler::compile_stream;
use arkflow_core::executor::run_job;
use std::time::Duration;
use tokio_util::sync::CancellationToken;


fn setup() {
    let _ = arkflow_plugin::input::init();
    let _ = arkflow_plugin::output::init();
    let _ = arkflow_plugin::processor::init();
    let _ = arkflow_plugin::buffer::init();
    let _ = arkflow_plugin::codec::init();
}

#[tokio::test(flavor = "multi_thread")]
async fn compiled_stream_runs_on_unified_kernel() {
    setup();
    let yaml = r#"
streams:
  - id: kernel-e2e
    input:
      type: "generate"
      context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
      interval: 1ms
      batch_size: 1
      count: 10
    pipeline:
      thread_num: 1
      processors:
        - type: "json_to_arrow"
        - type: "sql"
          query: "SELECT sum(value) as total FROM flow"
    output:
      type: "drop"
"#;
    let config: EngineConfig = serde_yaml::from_str(yaml).unwrap();
    let spec = compile_stream(&config.streams[0], 0).unwrap();
    let adapter = StreamJobAdapter::new(config.streams[0].durability.as_ref()).unwrap();
    let mut resource = arkflow_core::Resource {
        temporary: std::collections::HashMap::new(),
        input_names: std::cell::RefCell::new(Vec::new()),
    };
    let cancellation = CancellationToken::new();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        run_job(&spec, &adapter, &mut resource, cancellation),
    )
    .await
    .expect("kernel run must finish with the bounded source");
    result.expect("compiled stream should run cleanly");
}
