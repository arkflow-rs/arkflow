//! Performance baseline (task 6.4): the same generate → sql → drop workload
//! through (a) the legacy linear Stream runtime and (b) the unified kernel.
//! Run with `cargo test -p arkflow-plugin --test kernel_perf_baseline -- --ignored --nocapture`.

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

async fn run_legacy(config: &EngineConfig) -> Duration {
    let started = Instant::now();
    let mut stream = config.streams[0].build().unwrap();
    let cancellation = CancellationToken::new();
    stream.run(cancellation).await.unwrap();
    started.elapsed()
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
async fn baseline_legacy_vs_kernel() {
    setup();
    let count = 200_000;
    let config: EngineConfig = serde_yaml::from_str(&workload_yaml(count)).unwrap();

    // Warm up builders/registries, then measure both paths.
    let legacy = run_legacy(&config).await;
    let kernel = run_kernel(&config).await;
    println!(
        "workload: generate(batch=1000, count={count}) → json_to_arrow → sql(sum) → drop");
    println!("legacy Stream runtime : {legacy:?}");
    println!("unified kernel        : {kernel:?}");
    println!("ratio (kernel/legacy) : {:.2}", kernel.as_secs_f64() / legacy.as_secs_f64());
}
