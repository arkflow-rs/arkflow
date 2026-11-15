//! End-to-end: a compiled StreamConfig (generate → json_to_arrow → sql →
//! recording sink) runs on the unified kernel through the real plugin
//! registry, and the OUTPUT values are checked (not just a clean exit).

use arkflow_core::config::EngineConfig;
use arkflow_core::executor::run_job;
use arkflow_core::executor::stream_adapter::StreamJobAdapter;
use arkflow_core::executor::stream_compiler::compile_stream;
use arkflow_core::output::{register_output_builder, Output, OutputBuilder, OutputConfig};
use arkflow_core::{Error, MessageBatchRef, Resource};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Delivered batches, in write order.
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
    let _ = register_output_builder("__kernel_e2e_sink", Arc::new(RecordingOutputBuilder));
}

#[tokio::test(flavor = "multi_thread")]
async fn compiled_stream_runs_on_unified_kernel() {
    setup();
    RECORDED.lock().expect("sink recording poisoned").clear();
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
    let mut config: EngineConfig = serde_yaml::from_str(yaml).unwrap();
    config.streams[0].output = OutputConfig {
        output_type: "__kernel_e2e_sink".into(),
        name: None,
        codec: None,
        config: None,
    };
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

    // The output contract: ten one-row input batches, each summed by SQL
    // into total=10. A dropped, duplicated, or unprocessed batch changes
    // the count or the values.
    let recorded = std::mem::take(&mut *RECORDED.lock().expect("sink recording poisoned"));
    let mut totals = Vec::new();
    for batch in &recorded {
        let totals_column = batch
            .record_batch()
            .column_by_name("total")
            .expect("the aggregate column must survive to the sink")
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("sum(Int64) must produce Int64");
        totals.extend(totals_column.values().iter().copied());
    }
    assert_eq!(
        totals,
        vec![10; 10],
        "every input batch must be summed exactly once"
    );
}
