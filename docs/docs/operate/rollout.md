---
sidebar_position: 5
description: Roll a streaming job out to a fleet through the control plane Hub.
---

# How to: roll out jobs with the control plane

Run the same declarative job you debugged locally on a fleet of compute
nodes, managed by the control plane Hub.

**Prerequisites**

- ArkFlow [installed](/docs/get-started/install)
- Understand the [job configuration shape](/docs/build/jobs) — start with
  the local job example `examples/jobs_local.yaml`

## The pieces

```
┌─────────┐   submit    ┌─────┐   assign    ┌───────┐   run   ┌──────┐
│  You    │────────────▶│ Hub │────────────▶│ Agent │────────▶│ Kernel│
└─────────┘             └─────┘             └───────┘         └──────┘
```

- **Hub** — the control plane server. Stores desired job state, leases nodes,
  assigns jobs, and reconciles.
- **Agent** — runs on every compute node. Receives assignments and drives the
  unified execution kernel.
- A job validated locally runs unchanged on the fleet because both paths use
  the same `JobSpec`.

## 1. Debug the job locally

```bash
./target/release/arkflow --config examples/jobs_local.yaml --validate
./target/release/arkflow --config examples/jobs_local.yaml
```

Expected result: the validation passes and the job runs locally — this is
the exact `JobSpec` the Hub will distribute.

## 2. Start the Hub and register agents

`examples/control_plane_hub.yaml` configures a Hub with its API and
health-check endpoints; `examples/control_plane_example.yaml` shows an
agent-side deployment configuration:

```bash
# terminal 1 — the Hub
./target/release/arkflow --config examples/control_plane_hub.yaml

# terminal 2+ — one Agent per compute node
./target/release/arkflow --config examples/control_plane_example.yaml
```

Expected result: each agent appears as a leased node in the Hub's fleet API
(`/api/v1`). A node that goes silent keeps appearing in the fleet but stops
receiving new assignments until its lease is refreshed.

## 3. Submit the job and verify

Submit the job spec through the control plane HTTP API. Desired state is
stored by the Hub; agents pick up assignments, run the job through the
kernel, and report observed state. Watch reconciliation by killing an agent:
the Hub notices the stale lease and reassigns the job to a healthy node.

Operator details — deployment, targeting a specific node, and the full
HTTP API — are in the [control plane overview](./control-plane/overview.md)
and the [operations guide](./control-plane/operations.md).

## Troubleshooting

- **Job stays pending** — no healthy agent holds a lease; check agent logs
  and the fleet listing.
- **Version conflicts on update** — job versions are monotonic; submit with
  `version` incremented.
