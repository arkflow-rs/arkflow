//! Throughput baseline for the unified kernel (the legacy linear executor is
//! retired; the original migration comparison measured kernel 528ms vs legacy
//! 559ms on this workload — 6% faster). Run with
//! `cargo test -p arkflow-plugin --test kernel_perf_baseline -- --ignored --nocapture`.

use arkflow_core::config::EngineConfig;
use arkflow_core::executor::stream_adapter::StreamJobAdapter;
use arkflow_core::executor::stream_compiler::compile_stream;
use arkflow_core::executor::run_job;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

fn setup() {
    let _ = arkflow_plugin::input::init();
    let _ = arkflow_plugin::output::init();
    let _ = arkflow_plugin::processor::init();
    let _ = arkflow_plugin::buffer::init();
    let _ = arkflow_plugin::codec::init();
}

fn workload_yaml(count: usize) -> String {
    format!(
        r#"
streams:
  - id: perf
    input:
      type: "generate"
      context: '{{ "value": 1 }}'
      interval: 1ns
      batch_size: 1000
      count: {count}
    pipeline:
      thread_num: 1
      processors:
        - type: "json_to_arrow"
        - type: "sql"
          query: "SELECT sum(value) as total FROM flow"
    output:
      type: "drop"
"#
    )
}

async fn run_kernel(config: &EngineConfig) -> Duration {
    let started = Instant::now();
    let spec = compile_stream(&config.streams[0], 0).unwrap();
    let adapter = StreamJobAdapter::new(config.streams[0].durability.as_ref()).unwrap();
    let mut resource = arkflow_core::Resource {
        temporary: std::collections::HashMap::new(),
        input_names: std::cell::RefCell::new(Vec::new()),
    };
    run_job(&spec, &adapter, &mut resource, CancellationToken::new())
        .await
        .unwrap();
    started.elapsed()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf baseline: run explicitly with --ignored --nocapture"]
async fn kernel_throughput_baseline() {
    setup();
    let count = 200_000;
    let config: EngineConfig = serde_yaml::from_str(&workload_yaml(count)).unwrap();

    let kernel = run_kernel(&config).await;
    println!(
        "workload: generate(batch=1000, count={count}) → json_to_arrow → sql(sum) → drop");
    println!("unified kernel: {kernel:?} ({:.0} rows/s)",
        count as f64 / kernel.as_secs_f64());
}
