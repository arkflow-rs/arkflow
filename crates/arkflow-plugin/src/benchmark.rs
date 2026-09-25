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

use arkflow_core::config::EngineConfig;
use arkflow_core::executor::run_job;
use arkflow_core::executor::stream_adapter::StreamJobAdapter;
use arkflow_core::executor::stream_compiler::compile_stream;
use arkflow_core::state::RedbStateBackend;
use arkflow_core::state::StateBackend as _;
use arkflow_core::{Error, MessageBatch, Resource};
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

fn setup_plugins() -> Result<(), Error> {
    crate::input::init()?;
    crate::output::init()?;
    crate::processor::init()?;
    crate::buffer::init()?;
    crate::codec::init()?;
    Ok(())
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
        - type: "sql"
          query: "{query}"
    output:
      type: "drop"
"#
    )
}

async fn run_stream(config: &EngineConfig) -> Result<Duration, Error> {
    let started = Instant::now();
    let spec = compile_stream(&config.streams[0], 0)?;
    let adapter = StreamJobAdapter::new(config.streams[0].durability.as_ref())?;
    let mut resource = Resource {
        temporary: std::collections::HashMap::new(),
        input_names: std::cell::RefCell::new(Vec::new()),
    };
    run_job(&spec, &adapter, &mut resource, CancellationToken::new()).await?;
    Ok(started.elapsed())
}

async fn stream_scenario(
    name: &'static str,
    description: &'static str,
    count: usize,
    query: &str,
) -> Result<ScenarioResult, Error> {
    let config: EngineConfig = serde_yaml::from_str(&stream_workload(count, query))
        .map_err(|e| Error::Config(format!("benchmark workload invalid: {e}")))?;
    let wall = run_stream(&config).await?;
    Ok(ScenarioResult {
        name,
        description,
        unit: "rows",
        operations: count as u64,
        wall,
    })
}

/// Linear pipeline: JSON decode → SQL aggregation over the whole batch.
async fn linear_sql(count: usize) -> Result<ScenarioResult, Error> {
    stream_scenario(
        "linear-sql",
        "generate → json_to_arrow → sql(sum) → drop",
        count,
        "SELECT sum(value) as total FROM flow",
    )
    .await
}

/// Stateful keyed aggregation through the SQL engine.
async fn groupby_sql(count: usize) -> Result<ScenarioResult, Error> {
    stream_scenario(
        "groupby-sql",
        "generate → json_to_arrow → sql(sum group by sensor) → drop",
        count,
        "SELECT sensor, sum(value) as total FROM flow GROUP BY sensor",
    )
    .await
}

/// Filter plus projection over every row.
async fn filter_project_sql(count: usize) -> Result<ScenarioResult, Error> {
    stream_scenario(
        "filter-project-sql",
        "generate → json_to_arrow → sql(where + projection) → drop",
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
    // Each `put` commits (fsyncs); the loop is synchronous redb work, so run
    // it on a blocking thread instead of stalling the async worker for the
    // whole scenario.
    let wall = tokio::task::spawn_blocking(move || -> Result<_, Error> {
        let started = Instant::now();
        for index in 0..operations as u64 {
            let key = index.to_be_bytes();
            backend.put("benchmark", &key, &index.to_be_bytes())?;
        }
        for index in 0..operations as u64 {
            let key = index.to_be_bytes();
            assert!(backend.get("benchmark", &key)?.is_some());
        }
        Ok(started.elapsed())
    })
    .await
    .map_err(|e| Error::Process(format!("benchmark state-backend task failed: {e}")))??;
    Ok(ScenarioResult {
        name: "state-backend",
        description: "redb state backend durable put + get (per-write commit)",
        unit: "ops",
        operations: operations as u64 * 2,
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
        state_backend(count).await?,
    ])
}

/// Runs the whole suite: `warmup` untimed passes followed by `runs` measured
/// passes, reporting the best (minimum wall clock) per scenario.
pub async fn run_suite(
    count: usize,
    warmup: usize,
    runs: usize,
) -> Result<Vec<ScenarioResult>, Error> {
    if count == 0 {
        return Err(Error::Config("--count must be at least 1".to_string()));
    }
    if runs == 0 {
        return Err(Error::Config("--runs must be at least 1".to_string()));
    }
    setup_plugins()?;
    for _ in 0..warmup {
        run_each(count).await?;
    }
    let mut best: Option<Vec<ScenarioResult>> = None;
    for _ in 0..runs {
        let results = run_each(count).await?;
        best = Some(match best {
            None => results,
            Some(previous) => {
                previous
                    .into_iter()
                    .zip(results)
                    .map(|(old, new)| if new.wall < old.wall { new } else { old })
                    .collect()
            }
        });
    }
    // `runs == 0` is rejected above, so at least one measured pass ran.
    Ok(best.unwrap_or_default())
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
    /// Invalid arguments return a configuration error instead of panicking.
    #[tokio::test(flavor = "multi_thread")]
    async fn zero_runs_returns_a_config_error() {
        let error = run_suite(2_000, 0, 0).await.unwrap_err();
        assert!(error.to_string().contains("--runs"), "{error}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn zero_count_returns_a_config_error() {
        let error = run_suite(0, 0, 1).await.unwrap_err();
        assert!(error.to_string().contains("--count"), "{error}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tiny_suite_completes_with_positive_throughput() {
        let results = run_suite(2_000, 0, 1).await.expect("suite completes");
        assert_eq!(results.len(), 5);
        for result in &results {
            assert!(result.operations > 0, "{} missing workload", result.name);
            assert!(result.per_second() > 0.0, "{} zero throughput", result.name);
        }
        for result in &results {
            println!("{}: {:?} ({:.0} {}/s)", result.name, result.wall, result.per_second(), result.unit);
        }
        let names: Vec<&str> = results.iter().map(|r| r.name).collect();
        for expected in [
            "linear-sql",
            "groupby-sql",
            "filter-project-sql",
            "codec-json",
            "state-backend",
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
