---
sidebar_position: 2
---

# Why ArkFlow

ArkFlow is a single-binary, configuration-driven stream processor for teams
that need more structure than a stateless router without adopting a
distributed streaming database.

| Product shape | Data model | State and operations | Best fit |
|---|---|---|---|
| ArkFlow | Arrow batches with SQL, VRL, and Python | Single-node windows, input WAL, control-plane operations | Durable real-time integration and ETL |
| Vector | Row-oriented observability events | Routing and transforms; not a general SQL engine | Logs and metrics routing |
| Benthos/Bento | Configuration-driven messages | Stateless pipeline model; durability normally delegated to brokers | Lightweight connectors and routing |
| Flink | Distributed stream computation | Cluster state, checkpoints, and event-time processing | Stateful distributed analytics |
| RisingWave | Streaming database and SQL materialized views | Distributed storage and compute | Durable continuously maintained views |

## The trade-off

Choose ArkFlow when a single process, columnar batches, SQL joins, and
ack-gated input durability match the deployment boundary. Choose Flink or
RisingWave when cluster-wide state, elastic scaling, or database-grade
materialized views are required.

This is an architecture comparison, not a benchmark claim. See [delivery
semantics](../concepts/4-delivery-semantics.md), [window semantics](../concepts/8-windows.md),
and the component reference for implementation contracts.
