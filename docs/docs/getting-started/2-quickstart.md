---
sidebar_position: 2
---

# Quickstart

This guide runs a tiny pipeline that generates synthetic JSON readings, filters
them with SQL, and prints the result to the console.

## 1. Create a config

Save this as `config.yaml`:

```yaml
logging:
  level: info

streams:
  - input:
      type: "generate"
      context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
      interval: 1s
      batch_size: 10
    buffer:
      type: "memory"
      capacity: 10
      timeout: 10s
    pipeline:
      thread_num: 4
      processors:
        - type: "json_to_arrow"
        - type: "sql"
          query: "SELECT * FROM flow WHERE value >= 10"
    output:
      type: "stdout"
    error_output:
      type: "stdout"
```

What this does:

- `generate` emits one batch of synthetic JSON every second.
- `json_to_arrow` parses the raw bytes into Arrow columns so SQL can read them.
- `sql` keeps only rows where `value >= 10`.
- `stdout` writes each batch to the console; `error_output` captures failures.

## 2. Validate

```bash
./target/release/arkflow --config config.yaml --validate
```

## 3. Run

```bash
./target/release/arkflow --config config.yaml
```

## Where to go next

- [Concepts](../concepts/1-architecture.md) — understand streams, pipelines, and data flow.
- [Configuration reference](../configuration/1-top-level.md) — the full top-level YAML structure.
- [Components](../reference/component-inventory.md) — pick a real input and output.
