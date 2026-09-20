---
sidebar_position: -1
title: ArkFlow documentation
description: Build durable, high-throughput stream pipelines in Rust — and operate them as a fleet.
---

# ArkFlow documentation

ArkFlow is a high-performance stream processing engine written in Rust. It
consumes from any source, processes with SQL / Python / VRL, and delivers to
any sink — with write-ahead-log durability, checkpointed state, and an
optional control plane for running a fleet.

<div class="row af-cards">
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--lg" href="/docs/get-started/install">
      <div class="card__header"><h3>🚀 Get Started</h3></div>
      <div class="card__body">
        Install the binary, run your first pipeline in five minutes, and make
        it survive a crash.
      </div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--lg" href="/docs/build/streams">
      <div class="card__header"><h3>🔧 Build</h3></div>
      <div class="card__body">
        Streams, jobs, and DAGs; the component catalog; durability and
        delivery semantics; task-focused recipes.
      </div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--lg" href="/docs/sql">
      <div class="card__header"><h3>🗄️ SQL</h3></div>
      <div class="card__body">
        The full SQL language surface: data types, SELECT, aggregates, window
        functions, and user-defined functions.
      </div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--lg" href="/docs/operate/overview">
      <div class="card__header"><h3>🛠️ Operate</h3></div>
      <div class="card__body">
        Kubernetes deployment, the control plane and web console,
        observability, and a recovery runbook.
      </div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--lg" href="/docs/reference/cli">
      <div class="card__header"><h3>📖 Reference</h3></div>
      <div class="card__body">
        CLI, the complete HTTP API, configuration schema, and the generated
        component inventory.
      </div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--lg" href="/docs/develop/kernel">
      <div class="card__header"><h3>⚙️ Develop</h3></div>
      <div class="card__body">
        The unified execution kernel, plugin authoring, and performance notes
        from the source.
      </div>
    </a>
  </div>
</div>

## Pick your path

| You are... | Start with |
|------------|------------|
| Evaluating ArkFlow | [Quickstart](/docs/get-started/quickstart) → [Architecture](/docs/build/architecture) |
| Writing a pipeline | [Streams](/docs/build/streams) → [Components](./components/) → [Recipes](./build/recipes/) |
| Migrating from Kafka Connect / Flink | [Compatibility policy](./reference/compatibility.md) → [Delivery semantics](/docs/build/delivery-semantics) |
| Running in production | [Kubernetes](./operate/kubernetes.md) → [Observability](./operate/observability.md) → [Recovery](./operate/recovery.md) |
| Running a fleet | [Control plane](./operate/control-plane/overview.md) → [Web console](./operate/control-plane/console.md) |
| Extending the engine | [Writing a plugin](./develop/plugins.md) → [The kernel](./develop/kernel.md) |
