---
sidebar_position: 3
description: Write and run streaming jobs — from a local YAML job to a Hub-managed distributed deployment.
---

# Streaming jobs

Every ArkFlow workload runs through the same unified execution kernel, which
provides pipeline parallelism with backpressure, asynchronous barrier
checkpoints, event-time/watermark processing, keyed state, and columnar
window operators. You reach that kernel through three configuration levels:

| Mode | Where it is declared | Where it runs | Use it for |
| --- | --- | --- | --- |
| Stream | `streams:` | Single process | Linear pipelines and existing configs; add `durability` for crash safety. |
| Local job | `jobs:` | Single process | Stateful DAG jobs with explicit time, state, checkpoint, and recovery settings. |
| Distributed job | Hub API / console | Multiple compute nodes | Horizontal scale, savepoints, centralized operations. |

All three share the same job specification, so a job you debug locally can be
submitted to the Hub unchanged. Streams compile into the same kernel as jobs —
see [Distributed jobs](./distributed-jobs.md) for the runtime
contract.

## Write a local job

A job is a directed acyclic graph: `operators` connected by `edges`, with
component `sources` and `sinks` attached to the graph's `source` and `sink`
operators:

```yaml
streams: []
jobs:
  - id: sensor-window-job
    version: 1
    parallelism: 1
    max_parallelism: 128

    operators:
      - id: source
        kind: source
      - id: sink
        kind: sink
    edges:
      - id: e1
        from: source
        to: sink
        partitioned: true

    sources:
      - operator_id: source
        input_type: generate
        config:
          type: generate
          context: '{ "sensor": "temp_1", "value": 10, "ts": 1757000000000 }'
          interval: 100ms
          batch_size: 10
        time:
          mode: event_time
          timestamp_field: ts
          watermark:
            strategy: bounded_out_of_orderness
            out_of_orderness_ms: 2000
            idle_timeout_ms: 60000
          allowed_lateness_ms: 5000
          late_event_policy: update

    sinks:
      - operator_id: sink
        output_type: stdout

    state:
      backend: embedded_kv
      ttl_ms: 3600000
      format_version: 1
    checkpoint:
      interval_ms: 30000
      retention: 3
      object_store_uri: "file://./data/checkpoints"
    recovery: latest_checkpoint
```

This example is maintained as [`examples/jobs_local.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/jobs_local.yaml).
Field-by-field reference: [Top-level configuration → job](../reference/configuration.md#job).

Key points:

- `parallelism` (default `1`) sets task parallelism; `max_parallelism`
  (default `128`) bounds the key-group space used by `partitioned` edges.
- `partitioned: true` routes by key-group ownership, so the same key always
  reaches the same downstream task regardless of which source partition or
  upstream subtask delivered it.
- Operator `kind` is one of `source`, `map`, `filter`, `aggregate`,
  `window`, `join`, `sink`, `udf`. Intermediate kinds are primarily produced
  by the streaming SQL compiler and the console DAG orchestrator today.
- Event-time sources declare `mode`, a `timestamp_field`, watermark
  parameters, and a late-event policy (`drop`, `route`, or `update`).

### Validate and run

Validation performs the same deep build checks as startup — unknown
components, unsupported state backends, and illegal graph edges fail before
anything runs:

```bash
./target/release/arkflow --config jobs.yaml --validate
./target/release/arkflow --config jobs.yaml
```

`./target/release/arkflow schema` emits the JSON Schema for the whole
configuration, including all `jobs` fields, for editor completion.

## Distribute a job across nodes

A Hub plus two compute nodes is enough to run a job on multiple machines.
Start from the maintained examples:

```bash
./target/release/arkflow --config examples/control_plane_hub.yaml   # the Hub
./target/release/arkflow --config examples/control_plane_node.yaml  # node-a; repeat with node-b
```

Each node config sets `health_check.hub_url`, `node_id`, `node_token`, and
`agent_lease_ttl_ms`; the node registers with the Hub and reports heartbeat
and observation state.

### Submit flow

All routes are Bearer-authenticated (`health_check.api_token` on the Hub):

```bash
H=http://127.0.0.1:8080/api/v1
A="Authorization: Bearer operator-secret"
```

1. **Validate first.** Validation builds the real plan against the target
   nodes and rejects unsupported operators or state backends before anything
   is created:

   ```bash
   curl -s -H "$A" -X POST $H/jobs/validate \
     -d '{"spec": <JobSpec>, "node_ids": ["node-a", "node-b"]}'
   ```

2. **Submit stopped, inspect, then run.** Creation is a desired-state write
   and returns `202 Accepted`:

   ```bash
   curl -s -H "$A" -X POST $H/jobs \
     -d '{"spec": <JobSpec>, "desired_state": "stopped"}'

   curl -s -H "$A" $H/jobs/sensor-window-job/plan     # explain the physical plan
   curl -s -H "$A" $H/jobs/sensor-window-job/detail   # assignments and convergence

   curl -s -H "$A" -X PUT $H/jobs/sensor-window-job/desired-state \
     -d '{"state":"running"}'
   ```

   `PUT desired-state` supports the same `If-Match` generation guard and
   `Idempotency-Key` dedup as streams (see
   [HTTP API v1](../reference/api.md)).

3. **Checkpoint and savepoint.** Both write checksummed recovery artifacts to
   the job's `object_store_uri`; a checkpoint requires every planned task to
   participate in the same valid cut:

   ```bash
   curl -s -H "$A" -X POST $H/jobs/sensor-window-job/checkpoints -d '{}'
   curl -s -H "$A" -X POST $H/jobs/sensor-window-job/savepoints  -d '{}'
   ```

4. **Upgrade and roll back.** Upgrades require the job to be stopped and
   converged, and select a completed savepoint with a compatible state format;
   rollback restores the previous version:

   ```bash
   curl -s -H "$A" -X POST $H/jobs/sensor-window-job/upgrades -d '{"spec": <v2>}'

   curl -s -H "$A" -X POST \
     $H/jobs/sensor-window-job/upgrades/<upgrade_id>/rollback
   ```

The console Job workbench drives the same flow visually: build the DAG in the
orchestrator, validate, diff versions, publish, then observe watermark lag,
checkpoint duration, and recovery progress on the Runtime page.

### Failure and recovery

The Hub fences task attempts with job generations: a node that loses
reachability is re-placed onto other nodes, and when it returns it receives a
stop command instead of running a duplicate. On restart, the job restores
state, source positions, and per-partition watermarks from the recovery
artifact selected by its `recovery` policy (`latest_checkpoint`,
`latest_savepoint`, or `fail`). Recovery is rejected up front when the
artifact's state format is incompatible with the job version, so a bad
restore cannot corrupt state. Kill-and-restart recovery is exercised
end-to-end by the two-node smoke test in `crates/arkflow-server/tests/`.

## What scales — and what does not

Task placement keeps operator chains co-located: an assignment never splits
an edge between two nodes. The practical consequences:

- **Scales well**: running a job's source partitions across nodes (for
  example, a 20-partition Kafka topic consumed half by each of two nodes),
  independent subtasks, and many jobs per fleet.
- **Does not apply**: cross-node network shuffle. An operator's intermediate
  data stays on its node. Computations that need a shuffle across the whole
  stream should pass through an external system instead — for example,
  re-partition by key into an intermediate Kafka topic and run a second job
  on it.

This keeps the operational model light (one Hub, N nodes, no data-plane
mesh) while covering the parallel-data workloads ArkFlow targets.

## Checklist

- [ ] `--validate` passes on the configuration.
- [ ] `parallelism`/`max_parallelism` match the source's real parallelism
      (for Kafka, the partition count).
- [ ] Stateful operators declare a `state` backend; `checkpoint` points at a
      persistent `object_store_uri`.
- [ ] Distributed jobs validate against the target `node_ids` before the
      first submit.
- [ ] Upgrade path tested: savepoint → stop → upgrade → verify → (rollback).
