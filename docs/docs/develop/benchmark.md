---
sidebar_position: 25
title: Benchmarks
description: Run ArkFlow's reproducible benchmark suite — scenarios, methodology, and reports.
---

# Benchmarks

ArkFlow ships a self-contained benchmark suite covering the kernel's main
paths. Anyone can reproduce it after cloning the repository — no network,
no external services, no datasets to download.

## Running

```bash
cargo run -p arkflow --release --example benchmark
```

Release mode is required for meaningful numbers (debug builds measure rustc,
not ArkFlow). Useful flags:

| Flag | Default | Description |
| --- | --- | --- |
| `--count` | `200000` | Stream rows per scenario. The codec and state scenarios scale their operation counts independently so the suite stays bounded. |
| `--runs` | `3` | Measured passes; the report shows the best wall clock per scenario. |
| `--warmup` | `1` | Untimed passes discarded to warm caches. |
| `--json` | off | Emit a machine-readable report instead of markdown. |

A full default run finishes in well under a minute on a laptop and prints a
markdown table: one row per scenario with workload, operations, wall time,
and throughput.

## Scenarios

| Scenario | Path exercised | Measured unit |
| --- | --- | --- |
| `linear-sql` | generate → JSON decode → SQL whole-batch aggregation → drop | rows/s |
| `groupby-sql` | generate → JSON decode → SQL keyed aggregation (stateful GROUP BY) → drop | rows/s |
| `filter-project-sql` | generate → JSON decode → SQL filter + projection → drop | rows/s |
| `codec-json` | Arrow batch → NDJSON → Arrow batch round trips (1000-row batches) | batches/s |
| `state-backend` | redb state backend durable put + get (per-write commit) | ops/s |

Stream scenarios run through the same public entry points the engine itself
uses (`compile_stream` + `run_job`), so the numbers describe the production
execution path, not a special harness.

## Methodology

- The suite performs one warmup pass and measures `--runs` passes, reporting
  the best wall clock per scenario (best-of is the standard choice for
  throughput on noisy machines).
- The benchmark never asserts performance values — a benchmark that fails on
  slow hardware is useless. Regression tracking is yours: run the same
  command on two commits of the same machine and diff the reports.
- `--json` emits the same scenarios and throughput values as the markdown
  report, so CI systems or dashboards can ingest runs.
- The state-backend scenario measures durable writes (every put commits);
  its operation count is deliberately smaller than the stream row count.

## Related

- `crates/arkflow/examples/benchmark.rs` — the CLI wrapper.
- `crates/arkflow-plugin/src/benchmark.rs` — the scenario library.
- `crates/arkflow-plugin/tests/kernel_perf_baseline.rs` — a small throughput
  guard kept as an integration test.
