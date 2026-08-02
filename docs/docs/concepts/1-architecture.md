---
sidebar_position: 1
---

# Architecture

ArkFlow is a stream processing engine: it reads data from sources, transforms
it through a chain of processors, and writes it to sinks — all defined in a
single YAML configuration file. This page describes the runtime concepts that
the rest of the documentation builds on.

## Engine

The **engine** is the top-level process. It loads a configuration and runs one
or more **streams** concurrently, plus an HTTP health-check / control-plane
server. Each stream is independent; they share only the process and its
component registry (inputs, outputs, processors, buffers, codecs).

## Stream

A **stream** is the unit of data processing. It wires together:

```
Input → Buffer → [Processor → Processor → …] → Output
                                              ↘ Error output (optional)
```

- **Input** — reads raw messages from a source (Kafka, MQTT, HTTP, file, …).
- **Buffer** — holds batches between the input and the processors. It can be a
  simple in-memory queue or a windowing strategy (tumbling, sliding, session),
  optionally joining multiple sources.
- **Pipeline** — an ordered list of **processors** applied to each batch, run by
  `thread_num` worker tasks in parallel.
- **Output** — writes each processed batch to a sink. An optional
  `error_output` receives batches a processor failed on.
- **Durability** — an optional per-stream write-ahead log (WAL) that persists
  messages at the input boundary so they survive crashes (see
  [Delivery semantics](./4-delivery-semantics.md)).

## Message model

Data flows through a stream as a `MessageBatch` — a thin wrapper around an
[Apache Arrow](https://arrow.apache.org/) `RecordBatch`. Columnar Arrow gives
the engine high throughput and lets SQL processors run directly on the data
without reserializing. Inputs may also attach standardized **metadata columns**
(see [Metadata](./3-metadata.md)).

## Concurrency model

Each stream runs as a set of cooperative async tasks on the Tokio multi-threaded
runtime:

- one **input worker** reading from the source,
- several **processor workers** (configurable via `pipeline.thread_num`),
- one **output worker** writing to the sink with ordered delivery.

They communicate through `flume` channels and shut down together via a
`CancellationToken`. A `TaskTracker` waits for every task to finish on shutdown.

Continue to [Backpressure & ordered delivery](./2-backpressure-ordering.md).
