---
sidebar_position: 12
description: End-to-end case — IoT sensor telemetry batched into windows and aggregated per sensor.
---

# Case: IoT telemetry with windowed aggregation

A factory floor has hundreds of temperature sensors publishing readings
every few seconds. Writing every raw reading to storage is wasteful; the
monitoring dashboard only needs per-sensor statistics every 10 seconds.

## Requirements

- Aggregate, don't store: min/max/avg/count per sensor per interval
- Handle hundreds of sensors (group by sensor key)
- Swappable source: development uses `generate`, production uses MQTT or
  Kafka with the identical downstream configuration

## Architecture

```
┌─────────┐ readings ┌──────────────────┐  window flush  ┌───────────┐
│ Sensors │─────────▶│ tumbling_window  │───────────────▶│ SQL group │──▶ stdout / DB
└─────────┘          │ buffer (10 s)    │  one batch     │ by sensor │
                     └──────────────────┘                └───────────┘
```

The window **buffer** accumulates raw readings and releases one batch per
interval; the SQL processor computes the aggregate over the released batch.

## Configuration

Validated example: [`examples/case_telemetry_windows.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/case_telemetry_windows.yaml)

```yaml validate=full
streams:
  - id: telemetry-window-agg
    input:
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
      thread_num: 4
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

## Run and expected outcome

```bash
./target/release/arkflow --config examples/case_telemetry_windows.yaml --validate
./target/release/arkflow --config examples/case_telemetry_windows.yaml
```

About every 10 seconds, one summary line per sensor appears — with a 100 ms
input interval, `samples` is close to 100 and min/max/avg reflect the window.

## Trade-offs and variations

- **Late readings** matter if sensors retry: window emission and lateness
  handling are described in
  event-time and lateness handling in [streaming jobs](/docs/build/jobs) and the
  [window buffer references](/docs/components/buffers/tumbling_window).
- Replace `generate` with `mqtt` and keep the rest identical — the source
  produces the same JSON payload shape (see
  [MQTT input reference](/docs/components/inputs/mqtt)).
- Point the same aggregate at InfluxDB instead of stdout by switching the
  output (see [InfluxDB output reference](/docs/components/outputs/influxdb)).
