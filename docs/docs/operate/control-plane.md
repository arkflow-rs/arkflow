---
title: Control Plane
description: Operate a fleet of ArkFlow nodes with the Hub — deployment, the web console, rollout orchestration, and recovery.
sidebar_position: 5
---

# Control Plane

The control plane turns a set of ArkFlow nodes into an operated fleet: a
**Hub** holds desired state and brokers commands, **Agents** run job slices
and report observations, and a **web console** gives operators one pane of
glass.

1. [Overview](./control-plane/overview.md) — architecture and the desired-state model.
2. [Deployment](./control-plane/deploy.md) — run the Hub, agents, and console.
3. [Web console](./control-plane/console.md) — a tour of every console view.
4. [Operations](./control-plane/operations.md) — day-2 operator workflows.
5. [Reconciliation and rollout](./control-plane/reconciliation.md) — how changes propagate safely.

Every console action is a documented HTTP route — see the
[API reference](../reference/api.md).
