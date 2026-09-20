---
sidebar_position: 6
description: Deploy, observe, troubleshoot, and operate ArkFlow.
---

# Operate ArkFlow

This section is for operators responsible for reliable streaming workloads.

## Run it

- [Deploy on Kubernetes](./kubernetes.md)
- [Roll out changes](./rollout.md)

## The control plane

Operating many nodes as a fleet:

- [Control plane overview](./control-plane/overview.md) — Hub, Agents, and the desired-state model
- [Control plane deployment](./control-plane/deploy.md)
- [Web console](./control-plane/console.md)
- [Control plane operations](./control-plane/operations.md)
- [Reconciliation and rollout](./control-plane/reconciliation.md)
- [HTTP API reference](../reference/api.md)

## Keep it healthy

- [Observability](./observability.md) — metrics, events, logging, and alerts
- [Recovery runbook](./recovery.md) — WAL replay, checkpoints, and rollback

## Background reading

Before changing throughput or concurrency, read
[backpressure and ordering](/docs/build/backpressure),
[WAL optimization](/docs/build/wal), and
[delivery semantics](/docs/build/delivery-semantics).
