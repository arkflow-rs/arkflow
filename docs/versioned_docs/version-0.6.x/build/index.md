---
sidebar_position: 0
title: Build with ArkFlow
description: Streams, jobs, components, and the durability model — everything you need to build pipelines.
---

# Build with ArkFlow

A pipeline in ArkFlow is a declarative YAML description of data flowing from
an **input**, through **processors** (and optionally **buffers**), to an
**output** — all backed by a columnar (Apache Arrow) data model. The same
description compiles to one unified execution kernel whether it runs as a
single stream, a local job DAG, or a slice of a distributed job.

<div class="row af-cards">
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/docs/build/streams">
      <div class="card__header"><h3>Streams</h3></div>
      <div class="card__body">The pipeline anatomy: input → processors → output, error outputs, and codecs.</div>
    </a>
  </div>
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/docs/build/jobs">
      <div class="card__header"><h3>Jobs</h3></div>
      <div class="card__body">Multi-step DAGs with event time, keyed state, and checkpoints — locally or distributed.</div>
    </a>
  </div>
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/docs/build/distributed-jobs">
      <div class="card__header"><h3>Distributed jobs</h3></div>
      <div class="card__body">How the control plane assigns, co-locates, and recovers job slices across a fleet.</div>
    </a>
  </div>
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/docs/build/delivery-semantics">
      <div class="card__header"><h3>Durability & delivery</h3></div>
      <div class="card__body">At-least-once by default; exactly-once for Kafka outputs; WAL and checkpoint mechanics.</div>
    </a>
  </div>
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/docs/build/architecture">
      <div class="card__header"><h3>Architecture</h3></div>
      <div class="card__body">The columnar data model, metadata columns, and how a config becomes a running graph.</div>
    </a>
  </div>
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/docs/build/recipes/1-kafka-to-sql">
      <div class="card__header"><h3>Recipes</h3></div>
      <div class="card__body">Task how-tos and end-to-end cases, each backed by a CI-validated example.</div>
    </a>
  </div>
</div>

The full component catalog lives under **Components** in the sidebar, and the
SQL dialect is documented separately under **SQL**.
