---
sidebar_position: 3
description: Batch messages into tumbling or session windows and aggregate with SQL.
---

# How to: aggregate in windows

Batch messages into time windows and compute per-window aggregates with SQL.

**Prerequisites**

- ArkFlow [installed](/docs/get-started/install)
- No external services needed — the guide uses the `generate` source so you
  can run it as-is

## Choose a window type

| Buffer type | Behavior | Typical use |
|-------------|----------|-------------|
| `tumbling_window` | Fixed, non-overlapping intervals | Periodic reports, batched writes |
| `session_window` | Windows kept open while gaps stay under a timeout | User sessions |

For overlapping/sliding windows use the Job DAG `window` operator with
`kind: sliding` (event-time) — see
[Event-time semantics](../distributed-jobs.md#event-time-semantics).
The legacy `sliding_window` **buffer** is rejected by the stream compiler:
its row-count windows cannot be represented by the event-time window
operator.

## Configuration

A complete, CI-validated configuration ships as
[`examples/case_telemetry_windows.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/case_telemetry_windows.yaml):

```yaml validate=full
streams:
  - input:
      type: generate
      context: '{ "sensor": "temp_1", "value": 21.5, "ts": 1757000000000 }'
      interval: 100ms
      batch_size: 1

    buffer:
      type: tumbling_window
      interval: 10s

    state:
      backend: embedded_kv
      durability: ephemeral

    pipeline:
      processors:
        - type: json_to_arrow
        - type: sql
          query: |
            SELECT
              sensor,
              count(*) AS samples,
              min(value) AS min_value,
              max(value) AS max_value,
              avg(value) AS avg_value
            FROM flow
            GROUP BY sensor
        - type: arrow_to_json

    output:
      type: stdout
```

The flow is: messages accumulate in the **buffer** (the window), and every
`interval` the whole window is released into the pipeline as one batch,
where SQL computes the aggregate. Switching to `session_window` only
changes the `buffer` block (`session_window` takes `timeout`).

## Run and verify

```bash
./target/release/arkflow --config windows.yaml
```

Expected result: roughly every 10 seconds, one summary line per sensor with
`samples` around 100 (10 s of 100 ms messages), plus the min/max/avg for the
window.

## Troubleshooting

- **Nothing is emitted** — window buffers only release when the interval
  elapses; wait one full interval.
- **Aggregate over string JSON fields fails** — make sure `json_to_arrow`
  decoded the payload first and the query references the decoded column
  names.
- **Joining two sources** — a window buffer with a legacy `join` field fails
  compilation. Stream-stream joins are a Job DAG feature: run a job with a
  `join` operator (exactly two inbound edges) — see
  [Stream-stream join](../distributed-jobs.md#stream-stream-join).
