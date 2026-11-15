//! Throughput baseline for the unified kernel (the legacy linear executor is
//! retired; the original migration comparison measured kernel 528ms vs legacy
//! 559ms on this workload — 6% faster). Run with
//! `cargo test -p arkflow-plugin --test kernel_perf_baseline -- --ignored --nocapture`.
//!
//! For a fuller, publicly runnable benchmark suite (more scenarios, warmup,
//! markdown/JSON reports) use `cargo run -p arkflow --release --example
//! benchmark` — see docs/docs/develop/benchmark.md.

use arkflow_core::config::EngineConfig;
use arkflow_core::executor::run_job;
use arkflow_core::executor::stream_adapter::StreamJobAdapter;
use arkflow_core::executor::stream_compiler::compile_stream;
use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Error, MessageBatchRef, ProcessResult, Resource};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// Rows that actually entered the SQL step. The baseline used to divide the
/// CONFIGURED row count by wall time; a workload that silently processed
/// fewer rows would then read as a throughput improvement.
static PROCESSED_ROWS: AtomicU64 = AtomicU64::new(0);

/// Pass-through counter fused into the decode chain at no measurable cost.
struct RowCounter;

#[async_trait::async_trait]
impl Processor for RowCounter {
    async fn process(&self, msg_batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        PROCESSED_ROWS.fetch_add(msg_batch.len() as u64, Ordering::Relaxed);
        Ok(ProcessResult::Single(msg_batch))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

struct RowCounterBuilder;

impl ProcessorBuilder for RowCounterBuilder {
    fn build(
        &self,
        _name: Option<&str>,
        _config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        Ok(Arc::new(RowCounter))
    }
}

fn setup() {
    let _ = arkflow_plugin::input::init();
    let _ = arkflow_plugin::output::init();
    let _ = arkflow_plugin::processor::init();
    let _ = arkflow_plugin::buffer::init();
    let _ = arkflow_plugin::codec::init();
    let _ = register_processor_builder("__benchmark_count_rows", Arc::new(RowCounterBuilder));
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
        - type: "__benchmark_count_rows"
        - type: "sql"
          query: "SELECT sum(value) as total FROM flow"
    output:
      type: "drop"
"#
    )
}

/// Returns `(wall, rows actually processed)`; the row count comes from the
/// in-pipeline counter rather than the configured workload size.
async fn run_kernel(config: &EngineConfig) -> (Duration, u64) {
    PROCESSED_ROWS.store(0, Ordering::SeqCst);
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
    (started.elapsed(), PROCESSED_ROWS.load(Ordering::SeqCst))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf baseline: run explicitly with --ignored --nocapture"]
async fn kernel_throughput_baseline() {
    setup();
    let count = 200_000;
    let config: EngineConfig = serde_yaml::from_str(&workload_yaml(count)).unwrap();

    let (kernel, processed) = run_kernel(&config).await;
    println!(
        "workload: generate(batch=1000, count={count}) → json_to_arrow → count → sql(sum) → drop"
    );
    println!(
        "unified kernel: {kernel:?} ({:.0} rows/s over {processed} processed rows)",
        processed as f64 / kernel.as_secs_f64()
    );
    assert_eq!(
        processed, count as u64,
        "the baseline must process every configured row"
    );
}
