/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! Public benchmark suite (issue #87): reproducible scenarios over the
//! kernel's main paths plus the JSON codec and the state backend. Every
//! scenario is self-contained — no network, no external services — so anyone
//! can reproduce the numbers with one command after cloning the repository.
//!
//! Run it via the CLI wrapper in the main crate:
//! `cargo run -p arkflow --release --example benchmark`
//!
//! The suite never asserts throughput values: machines differ, and a
//! benchmark that fails on slow hardware is useless. Regression tracking is
//! the caller's job (compare reports across commits on the same machine).

use crate::codec::schema_registry::{FetchedSchema, SchemaRegistryCodec, SchemaResolver};
use apache_avro::types::Value as AvroValue;
use apache_avro::writer::datum::GenericDatumWriter;
use apache_avro::Schema as AvroSchema;
use arkflow_core::codec::Decoder;
use arkflow_core::config::EngineConfig;
use arkflow_core::executor::run_job;
use arkflow_core::executor::stream_adapter::StreamJobAdapter;
use arkflow_core::executor::stream_compiler::compile_stream;
use arkflow_core::processor::{
    register_processor_builder, Processor, ProcessorBuilder, ProcessorConfig,
};
use arkflow_core::state::RedbStateBackend;
use arkflow_core::state::StateBackend as _;
use arkflow_core::{Bytes, Error, MessageBatch, MessageBatchRef, ProcessResult, Resource};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// One measured scenario.
#[derive(Debug, Clone)]
pub struct ScenarioResult {
    pub name: &'static str,
    pub description: &'static str,
    /// Unit of the workload: rows for stream/codec scenarios, operations for
    /// the state backend.
    pub unit: &'static str,
    pub operations: u64,
    pub wall: Duration,
}

impl ScenarioResult {
    /// Throughput in operations (or rows) per second; always finite and
    /// positive for a completed scenario.
    pub fn per_second(&self) -> f64 {
        self.operations as f64 / self.wall.as_secs_f64()
    }
}

fn setup_plugins() {
    let _ = crate::input::init();
    let _ = crate::output::init();
    let _ = crate::processor::init();
    let _ = crate::buffer::init();
    let _ = crate::codec::init();
    // The suite's row counter. It sits between decode and the workload's
    // SQL step and counts the rows the kernel actually processed. The
    // report used to divide the REQUESTED row count by wall time, so a
    // workload that processed fewer rows than requested (early exit, a
    // dropped tail batch) reported inflated throughput.
    static REGISTER: std::sync::Once = std::sync::Once::new();
    REGISTER.call_once(|| {
        let _ = register_processor_builder("__benchmark_count_rows", Arc::new(RowCounterBuilder));
    });
}

/// Rows the benchmark pipeline observed during the current scenario.
static BENCH_PROCESSED_ROWS: AtomicU64 = AtomicU64::new(0);

/// Pass-through processor that counts rows and forwards the batch unchanged.
/// Stateless, so the graph fuses it into the neighbouring chain at no
/// measurable cost.
struct RowCounter;

#[async_trait::async_trait]
impl Processor for RowCounter {
    async fn process(&self, msg_batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        BENCH_PROCESSED_ROWS.fetch_add(msg_batch.len() as u64, Ordering::Relaxed);
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

fn stream_workload(count: usize, query: &str) -> String {
    format!(
        r#"
streams:
  - id: benchmark
    input:
      type: "generate"
      context: '{{ "value": 10, "sensor": "temp_1" }}'
      interval: 1ns
      batch_size: 1000
      count: {count}
    pipeline:
      thread_num: 1
      processors:
        - type: "json_to_arrow"
        - type: "__benchmark_count_rows"
        - type: "sql"
          query: "{query}"
    output:
      type: "drop"
"#
    )
}

/// Run the compiled stream and return `(wall, rows actually processed)`. The
/// row count comes from the in-pipeline counter, not from the requested
/// `count`.
async fn run_stream(config: &EngineConfig) -> Result<(Duration, u64), Error> {
    BENCH_PROCESSED_ROWS.store(0, Ordering::SeqCst);
    let started = Instant::now();
    let spec = compile_stream(&config.streams[0], 0)?;
    let adapter = StreamJobAdapter::new(config.streams[0].durability.as_ref())?;
    let mut resource = Resource {
        temporary: std::collections::HashMap::new(),
        input_names: std::cell::RefCell::new(Vec::new()),
    };
    run_job(&spec, &adapter, &mut resource, CancellationToken::new()).await?;
    Ok((
        started.elapsed(),
        BENCH_PROCESSED_ROWS.load(Ordering::SeqCst),
    ))
}

async fn stream_scenario(
    name: &'static str,
    description: &'static str,
    count: usize,
    query: &str,
) -> Result<ScenarioResult, Error> {
    let config: EngineConfig = serde_yaml::from_str(&stream_workload(count, query))
        .map_err(|e| Error::Config(format!("benchmark workload invalid: {e}")))?;
    let (wall, operations) = run_stream(&config).await?;
    Ok(ScenarioResult {
        name,
        description,
        unit: "rows",
        operations,
        wall,
    })
}

/// Linear pipeline: JSON decode → SQL aggregation over the whole batch.
async fn linear_sql(count: usize) -> Result<ScenarioResult, Error> {
    stream_scenario(
        "linear-sql",
        "generate → json_to_arrow → count → sql(sum) → drop",
        count,
        "SELECT sum(value) as total FROM flow",
    )
    .await
}

/// Stateful keyed aggregation through the SQL engine.
async fn groupby_sql(count: usize) -> Result<ScenarioResult, Error> {
    stream_scenario(
        "groupby-sql",
        "generate → json_to_arrow → count → sql(sum group by sensor) → drop",
        count,
        "SELECT sensor, sum(value) as total FROM flow GROUP BY sensor",
    )
    .await
}

/// Filter plus projection over every row.
async fn filter_project_sql(count: usize) -> Result<ScenarioResult, Error> {
    stream_scenario(
        "filter-project-sql",
        "generate → json_to_arrow → count → sql(where + projection) → drop",
        count,
        "SELECT *, value * 2 as doubled FROM flow WHERE value >= 1",
    )
    .await
}

/// JSON codec round trip (Arrow batch → NDJSON lines → Arrow batch). Each
/// iteration moves a 1000-row batch, so the iteration count is scaled down
/// from the stream row count to keep the whole suite bounded.
async fn codec_json(count: usize) -> Result<ScenarioResult, Error> {
    let iterations = (count / 100).clamp(20, 2_000);
    let batch = sample_batch()?;
    let started = Instant::now();
    for _ in 0..iterations {
        let mut buf = Vec::new();
        let mut writer = datafusion::arrow::json::LineDelimitedWriter::new(&mut buf);
        writer
            .write(&batch)
            .map_err(|e| Error::Process(format!("benchmark encode failed: {e}")))?;
        writer
            .finish()
            .map_err(|e| Error::Process(format!("benchmark encode failed: {e}")))?;
        let _ = crate::component::json::try_to_arrow(&buf, None)?;
    }
    let wall = started.elapsed();
    Ok(ScenarioResult {
        name: "codec-json",
        description: "Arrow batch → NDJSON → Arrow batch round trips (1000-row batches)",
        unit: "batches",
        operations: iterations as u64,
        wall,
    })
}

/// JSON decode only (NDJSON buffer → Arrow batch via `try_to_arrow`). Each
/// iteration decodes the SAME 1000-row buffer, so from the second iteration
/// the exact-content schema cache serves it — this reports the cache-hit
/// decode path (the fixed-template steady state).
async fn json_decode(count: usize) -> Result<ScenarioResult, Error> {
    let iterations = (count / 100).clamp(20, 2_000);
    let row = r#"{"value":10,"sensor":"temp_1"}"#;
    let mut content = Vec::with_capacity(row.len() * 1000);
    for _ in 0..1000 {
        content.extend_from_slice(row.as_bytes());
        content.push(b'\n');
    }
    let started = Instant::now();
    for _ in 0..iterations {
        let _ = crate::component::json::try_to_arrow(&content, None)?;
    }
    let wall = started.elapsed();
    Ok(ScenarioResult {
        name: "json-decode",
        description: "NDJSON → Arrow batch decode (1000-row buffers, decode only)",
        unit: "batches",
        operations: iterations as u64,
        wall,
    })
}

fn sample_batch() -> Result<MessageBatch, Error> {
    let row = r#"{"value":10,"sensor":"temp_1"}"#;
    let mut content = Vec::with_capacity(row.len() * 1000);
    for _ in 0..1000 {
        content.extend_from_slice(row.as_bytes());
        content.push(b'\n');
    }
    let record_batch = crate::component::json::try_to_arrow(&content, None)?;
    Ok(MessageBatch::new_arrow(record_batch))
}

/// State backend put/get over a temporary redb store. Every put is a durable
/// commit (the real cost operators pay), so the operation count is scaled
/// down from the stream row count to keep the whole suite bounded.
async fn state_backend(count: usize) -> Result<ScenarioResult, Error> {
    let operations = (count / 50).clamp(200, 4_000);
    let dir = tempfile::tempdir()?;
    let backend = RedbStateBackend::open(dir.path(), 1)?;
    let started = Instant::now();
    for index in 0..operations as u64 {
        let key = index.to_be_bytes();
        backend.put("benchmark", &key, &index.to_be_bytes())?;
    }
    for index in 0..operations as u64 {
        let key = index.to_be_bytes();
        assert!(backend.get("benchmark", &key)?.is_some());
    }
    let wall = started.elapsed();
    Ok(ScenarioResult {
        name: "state-backend",
        description: "redb state backend durable put + get (per-write commit)",
        unit: "ops",
        operations: operations as u64 * 2,
        wall,
    })
}

/// Offline schema resolver for the avro-decode scenarios: pre-registers the
/// schemas the benchmark uses so the real `SchemaRegistryCodec` decode path
/// runs without any network (suite-wide self-containment contract).
struct InMemorySchemaResolver {
    schemas: HashMap<u32, FetchedSchema>,
}

#[async_trait::async_trait]
impl SchemaResolver for InMemorySchemaResolver {
    async fn fetch_schema(&self, id: u32) -> Result<FetchedSchema, Error> {
        self.schemas
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::Process(format!("benchmark resolver has no schema id {id}")))
    }
}

/// Field types cycled by the wide-schema builder: the supported leaf kinds
/// the flat Arrow mapping sees in real payloads (incl. one nullable union).
const AVRO_FIELD_TYPES: [&str; 6] = [
    r#""long""#,
    r#""string""#,
    r#""double""#,
    r#""boolean""#,
    r#"["null", "int"]"#,
    r#"{"type": "int", "logicalType": "date"}"#,
];

fn wide_avro_schema(width: usize) -> AvroSchema {
    let fields: Vec<String> = (0..width)
        .map(|i| {
            format!(
                r#"{{"name": "f_{i}", "type": {}}}"#,
                AVRO_FIELD_TYPES[i % AVRO_FIELD_TYPES.len()]
            )
        })
        .collect();
    let json = format!(
        r#"{{"type": "record", "name": "BenchmarkW{width}", "fields": [{}]}}"#,
        fields.join(",")
    );
    AvroSchema::parse_str(&json).expect("benchmark avro schema parses")
}

fn wide_avro_value(width: usize, i: u64) -> AvroValue {
    AvroValue::Record(
        (0..width)
            .map(|f| {
                let value = match f % AVRO_FIELD_TYPES.len() {
                    0 => AvroValue::Long(i as i64),
                    1 => AvroValue::String(format!("s_{}", i % 97)),
                    2 => AvroValue::Double((i % 1000) as f64 * 0.5),
                    3 => AvroValue::Boolean(i.is_multiple_of(2)),
                    4 => AvroValue::Union(1, Box::new(AvroValue::Int((i % 1_000_000) as i32))),
                    _ => AvroValue::Date((i % 30_000) as i32),
                };
                (format!("f_{f}"), value)
            })
            .collect(),
    )
}

/// Encodes one Confluent wire-format message: `[0x00][4-byte BE id][payload]`.
fn wire_message(schema: &AvroSchema, id: u32, value: AvroValue) -> Vec<u8> {
    let payload = GenericDatumWriter::builder(schema)
        .build()
        .expect("benchmark writer builds")
        .write_value_to_vec(value)
        .expect("benchmark avro encode");
    let mut message = Vec::with_capacity(5 + payload.len());
    message.push(0x00);
    message.extend_from_slice(&id.to_be_bytes());
    message.extend_from_slice(&payload);
    message
}

/// Avro decode through the real schema-registry codec path: each iteration
/// decodes a 1000-message batch of pre-encoded wire-format messages. Setup
/// (schema construction + encoding) and the schema-cache warm-up decode are
/// untimed; iterations hit the per-id cache, which is the steady-state path.
/// Iterations are scaled down from the stream row count and further scaled
/// inversely with the schema width (per-message cost grows linearly in
/// width), giving every width roughly the same time budget and keeping the
/// whole suite bounded.
async fn avro_decode(
    name: &'static str,
    description: &'static str,
    width: usize,
    count: usize,
) -> Result<ScenarioResult, Error> {
    const BATCH: usize = 1000;
    // `max(1)`: widths below 5 would otherwise divide by zero.
    let iterations = ((count / (BATCH * 2)) / (width / 5).max(1)).clamp(10, 100);
    let id = 7000 + width as u32;
    let schema = wide_avro_schema(width);
    let mut messages = Vec::with_capacity(BATCH);
    for i in 0..BATCH as u64 {
        messages.push(Bytes::from(wire_message(
            &schema,
            id,
            wide_avro_value(width, i),
        )));
    }
    let mut schemas = HashMap::new();
    schemas.insert(id, FetchedSchema::Avro(schema));
    let codec = SchemaRegistryCodec::new(None, Arc::new(InMemorySchemaResolver { schemas }), None);
    codec.decode(messages.clone()).await?;
    let started = Instant::now();
    for _ in 0..iterations {
        let _ = codec.decode(messages.clone()).await?;
    }
    let wall = started.elapsed();
    Ok(ScenarioResult {
        name,
        description,
        unit: "rows",
        operations: (iterations * BATCH) as u64,
        wall,
    })
}

/// Avro decode, 5-field schema (narrow records).
async fn avro_decode_w5(count: usize) -> Result<ScenarioResult, Error> {
    avro_decode(
        "avro-decode-w5",
        "schema_registry avro decode, 5-field schema (1000-msg decode batches)",
        5,
        count,
    )
    .await
}

/// Avro decode, 25-field schema (medium records).
async fn avro_decode_w25(count: usize) -> Result<ScenarioResult, Error> {
    avro_decode(
        "avro-decode-w25",
        "schema_registry avro decode, 25-field schema (1000-msg decode batches)",
        25,
        count,
    )
    .await
}

/// Avro decode, 100-field schema (wide records — per-message cost scales
/// with field count).
async fn avro_decode_w100(count: usize) -> Result<ScenarioResult, Error> {
    avro_decode(
        "avro-decode-w100",
        "schema_registry avro decode, 100-field schema (1000-msg decode batches)",
        100,
        count,
    )
    .await
}

/// Window journaled persistence (#25 anchor): a stream whose tumbling
/// window compiles through the production path into the journaled window
/// operator (ephemeral redb state → journal; graph.rs builds journaled
/// operators whenever a stream carries a state section). The 1h window
/// never fires during a run, so every delivered batch pays the steady-state
/// cost: keyed row assignment + journal staging for every touched key.
/// (The whole-map clone and per-batch stale scan this anchor originally
/// exercised were removed by the per-batch-cost fix — persist no longer
/// grows with the open-window count, so the remaining cross-tier growth is
/// the necessary per-dirty-key staging plus the EOF fire's one-commit-per-key.)
///
/// Key cardinality comes from the generate template: generate replicates a
/// STATIC template per read, so `keys` distinct sensor rows go into the
/// template and `batch_size` tiles it — every tier reads ~1000 rows per
/// batch with `keys` distinct live keys. Read counts are scaled so every
/// tier delivers roughly the same row budget and cross-tier wall growth
/// tracks the per-key cost (plus the EOF fire, which commits
/// one journal transaction per key — itself O(keys)).
///
/// `max_pending_transactions` scales with the key count: the journaled
/// operator stages one transaction per open window group, and the 4_096
/// default would fail the 1k tier on the first batch.
fn window_journal_workload(count: usize, keys: usize) -> String {
    // Uniform ~1000-row reads at every tier: the template carries the keys
    // (generate replicates a static template) and batch_size tiles it.
    let template_rows = keys;
    let batch_size = (1000 / keys).max(1);
    let max_txns = keys.saturating_mul(2) + 64;
    let mut template = String::with_capacity(template_rows * 40);
    for i in 0..template_rows {
        template.push_str(&format!(
            "        {{\"value\":10,\"sensor\":\"sensor_{:06}\"}}\n",
            i
        ));
    }
    format!(
        r#"
streams:
  - id: benchmark-window
    input:
      type: "generate"
      context: |
{template}      interval: 1ns
      batch_size: {batch_size}
      count: {count}
      codec:
        type: "json"
    pipeline:
      thread_num: 1
      processors:
        - type: "__benchmark_count_rows"
    buffer:
      type: "tumbling_window"
      interval: "1h"
      key_field: "sensor"
    state:
      backend: "embedded_kv"
      durability: "ephemeral"
      max_pending_transactions: {max_txns}
    output:
      type: "drop"
"#
    )
}

async fn window_journal(
    name: &'static str,
    description: &'static str,
    keys: usize,
    count: usize,
) -> Result<ScenarioResult, Error> {
    // `count` is the generate read quota (reads × batch_size); rows delivered
    // = count × template_rows, so dividing by the template size keeps the
    // row budget comparable across tiers.
    let reads = (count / keys).max(1);
    let config: EngineConfig = serde_yaml::from_str(&window_journal_workload(reads, keys))
        .map_err(|e| Error::Config(format!("benchmark workload invalid: {e}")))?;
    let (wall, operations) = run_stream(&config).await?;
    Ok(ScenarioResult {
        name,
        description,
        unit: "rows",
        operations,
        wall,
    })
}

/// Window journaled persist, 1 live key (the per-batch baseline: the persist
/// cost is the fixed full-scan + bookkeeping floor).
async fn window_journal_n1(count: usize) -> Result<ScenarioResult, Error> {
    window_journal(
        "window-journal-n1",
        "tumbling window → journaled persist, 1 live key (1000-row reads, never fires)",
        1,
        count,
    )
    .await
}

/// Window journaled persist, 100 live keys.
async fn window_journal_n100(count: usize) -> Result<ScenarioResult, Error> {
    window_journal(
        "window-journal-n100",
        "tumbling window → journaled persist, 100 live keys (1000-row reads, never fires)",
        100,
        count,
    )
    .await
}

/// Window journaled persist, 1k live keys (the EOF fire commits one journal
/// transaction per key — an O(N) journaled-window cost this tier makes
/// visible; per-batch persist itself no longer grows with the key set after
/// the per-batch-cost fix).
async fn window_journal_n1k(count: usize) -> Result<ScenarioResult, Error> {
    window_journal(
        "window-journal-n1k",
        "tumbling window → journaled persist, 1k live keys (1000-row reads, never fires until EOF)",
        1_000,
        count,
    )
    .await
}

/// SQL processor steady state with every plan cache hit (#26 anchor): the
/// end-to-end SQL scenarios dilute the per-batch `ctx.state()` clone behind
/// generate/decode work, so this scenario times `process` directly. Two
/// untimed warm-up batches build the analyzed/optimized/physical plan cache
/// for the schema; the measured loop then replays the same-schema batch, so
/// the reported wall is the cache-hit path — the per-batch fixed overhead
/// the state clone adds.
async fn sql_hot_cache(count: usize) -> Result<ScenarioResult, Error> {
    let iterations = (count / 100).clamp(20, 2_000);
    let config = ProcessorConfig {
        processor_type: "sql".to_string(),
        name: None,
        config: Some(serde_json::json!({
            "query": "SELECT sum(value) as total FROM flow"
        })),
    };
    let resource = Resource {
        temporary: HashMap::new(),
        input_names: std::cell::RefCell::new(Vec::new()),
    };
    let processor = config.build(&resource)?;
    let batch = Arc::new(sample_batch()?);
    // Untimed: plan caches are keyed by schema; two batches leave the
    // three-level cache fully warm.
    processor.process(Arc::clone(&batch)).await?;
    processor.process(Arc::clone(&batch)).await?;
    let started = Instant::now();
    for _ in 0..iterations {
        processor.process(Arc::clone(&batch)).await?;
    }
    let wall = started.elapsed();
    Ok(ScenarioResult {
        name: "sql-hot-cache",
        description: "sql processor steady state, plan cache fully warm (1000-row batches)",
        unit: "batches",
        operations: iterations as u64,
        wall,
    })
}

/// JSON decode, 200-field wide records: the narrow `json-decode` scenario
/// under-weights per-field decode work (small against the fixed decoder
/// overhead); wide records stress the per-field keyed lookups. Each
/// iteration decodes the SAME buffer, so from the second iteration the
/// exact-content schema cache serves it — this reports the wide-record
/// cache-hit decode path. Load shape taken from the `json_decode_timing`
/// oracle test (component/json.rs).
async fn json_decode_w200(count: usize) -> Result<ScenarioResult, Error> {
    // Wide rows cost far more per batch than the narrow ones; scale the
    // iteration count down to keep the tier's time budget comparable.
    let iterations = (count / 800).clamp(3, 30);
    let fields: Vec<String> = (0..200).map(|f| format!(r#""f{f:03}":{f}"#)).collect();
    let row = format!("{{{}}}", fields.join(","));
    let mut content = Vec::with_capacity(row.len() * 1000);
    for _ in 0..1000 {
        content.extend_from_slice(row.as_bytes());
        content.push(b'\n');
    }
    let started = Instant::now();
    for _ in 0..iterations {
        let _ = crate::component::json::try_to_arrow(&content, None)?;
    }
    let wall = started.elapsed();
    Ok(ScenarioResult {
        name: "json-decode-w200",
        description: "NDJSON → Arrow batch decode (1000-row × 200-field buffers, decode only)",
        unit: "batches",
        operations: iterations as u64,
        wall,
    })
}

/// Runs every scenario once (helper for measurement loops).
async fn run_each(count: usize) -> Result<Vec<ScenarioResult>, Error> {
    Ok(vec![
        linear_sql(count).await?,
        groupby_sql(count).await?,
        filter_project_sql(count).await?,
        codec_json(count).await?,
        json_decode(count).await?,
        state_backend(count).await?,
        avro_decode_w5(count).await?,
        avro_decode_w25(count).await?,
        avro_decode_w100(count).await?,
        window_journal_n1(count).await?,
        window_journal_n100(count).await?,
        window_journal_n1k(count).await?,
        sql_hot_cache(count).await?,
        json_decode_w200(count).await?,
    ])
}

/// Serializes whole suite runs. The stream scenarios count the rows the
/// kernel actually processed through one process-wide counter; two suites
/// running concurrently (the library tests do) would reset and read each
/// other's counters and report zero workload.
static SUITE_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

/// Runs the whole suite: `warmup` untimed passes followed by `runs` measured
/// passes, reporting the best (minimum wall clock) per scenario.
pub async fn run_suite(
    count: usize,
    warmup: usize,
    runs: usize,
) -> Result<Vec<ScenarioResult>, Error> {
    assert!(runs >= 1, "at least one measured run is required");
    let _suite = SUITE_LOCK.lock().await;
    setup_plugins();
    for _ in 0..warmup {
        run_each(count).await?;
    }
    let mut best: Option<Vec<ScenarioResult>> = None;
    for _ in 0..runs {
        let results = run_each(count).await?;
        best = Some(match best {
            None => results,
            Some(previous) => previous
                .into_iter()
                .zip(results)
                .map(|(old, new)| if new.wall < old.wall { new } else { old })
                .collect(),
        });
    }
    Ok(best.expect("runs >= 1"))
}

/// Human-readable markdown report.
pub fn markdown_report(results: &[ScenarioResult]) -> String {
    let mut out = String::from("# ArkFlow benchmark\n\n");
    out.push_str("| scenario | workload | unit | operations | wall | throughput |\n");
    out.push_str("| --- | --- | --- | --- | --- | --- |\n");
    for result in results {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {:?} | {:.0} {}/s |\n",
            result.name,
            result.description,
            result.unit,
            result.operations,
            result.wall,
            result.per_second(),
            result.unit
        ));
    }
    out.push_str(
        "\nBest of the measured runs on this machine; numbers are not comparable across hardware.\n",
    );
    out
}

/// Machine-readable JSON report (same scenarios and values as markdown).
pub fn json_report(results: &[ScenarioResult]) -> Result<String, Error> {
    #[derive(serde::Serialize)]
    struct Row<'a> {
        scenario: &'a str,
        description: &'a str,
        unit: &'a str,
        operations: u64,
        wall_ms: u128,
        per_second: u64,
    }
    let rows: Vec<Row> = results
        .iter()
        .map(|r| Row {
            scenario: r.name,
            description: r.description,
            unit: r.unit,
            operations: r.operations,
            wall_ms: r.wall.as_millis(),
            per_second: r.per_second() as u64,
        })
        .collect();
    serde_json::to_string_pretty(&rows)
        .map_err(|e| Error::Process(format!("benchmark report serialization failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CI-safe smoke: tiny workload, single measured run, asserting only
    /// completion and positive throughput — never performance values.
    #[tokio::test(flavor = "multi_thread")]
    async fn tiny_suite_completes_with_positive_throughput() {
        let results = run_suite(2_000, 0, 1).await.expect("suite completes");
        assert_eq!(results.len(), 14);
        for result in &results {
            assert!(result.operations > 0, "{} missing workload", result.name);
            assert!(result.per_second() > 0.0, "{} zero throughput", result.name);
        }
        for result in &results {
            println!(
                "{}: {:?} ({:.0} {}/s)",
                result.name,
                result.wall,
                result.per_second(),
                result.unit
            );
        }
        let names: Vec<&str> = results.iter().map(|r| r.name).collect();
        for expected in [
            "linear-sql",
            "groupby-sql",
            "filter-project-sql",
            "codec-json",
            "json-decode",
            "state-backend",
            "avro-decode-w5",
            "avro-decode-w25",
            "avro-decode-w100",
            "window-journal-n1",
            "window-journal-n100",
            "window-journal-n1k",
            "sql-hot-cache",
            "json-decode-w200",
        ] {
            assert!(names.contains(&expected), "missing {expected}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reports_agree_on_scenarios_and_throughput() {
        let results = run_suite(2_000, 0, 1).await.expect("suite completes");
        let markdown = markdown_report(&results);
        let json = json_report(&results).expect("json");
        for result in &results {
            assert!(
                markdown.contains(result.name) && json.contains(result.name),
                "{} missing from a report",
                result.name
            );
            let per_second = format!("{}", result.per_second() as u64);
            assert!(
                json.contains(&per_second),
                "{} throughput missing from json report",
                result.name
            );
        }
    }
}
