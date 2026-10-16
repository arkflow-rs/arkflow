---
sidebar_position: 0
title: Develop ArkFlow
description: Extend the engine or work on it — the kernel, plugin authoring, and performance notes.
---

# Develop ArkFlow

Everything you need to extend ArkFlow or hack on the engine itself.

<div class="row af-cards">
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/docs/develop/kernel">
      <div class="card__header"><h3>The unified execution kernel</h3></div>
      <div class="card__body">Graphs, fused chains, barriers, state journal, and the commit frontier.</div>
    </a>
  </div>
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/docs/develop/plugins">
      <div class="card__header"><h3>Writing a plugin</h3></div>
      <div class="card__body">Trait + builder + registration, and the machine-checked docs contract.</div>
    </a>
  </div>
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/docs/develop/s3-wal-performance">
      <div class="card__header"><h3>S3 WAL backend performance</h3></div>
      <div class="card__body">Design notes and benchmarks for remote WAL durability.</div>
    </a>
  </div>
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/docs/contribute">
      <div class="card__header"><h3>Contribute</h3></div>
      <div class="card__body">Workspace layout, commands, and the openspec workflow for behavior changes.</div>
    </a>
  </div>
</div>

## Repository map

| Crate | Contents |
|-------|----------|
| `crates/arkflow-core` | Engine abstractions and the unified execution kernel (`src/executor/`), traits, `MessageBatch`. |
| `crates/arkflow-plugin` | Every plugin: `input/`, `output/`, `processor/`, `buffer/`, `codec/`, `wal/`. |
| `crates/arkflow` | The binary; component `init()` order. |
| `crates/arkflow-server` | Control plane: Hub, Agent, storage, HTTP API. |
| `console/` | The Vite + React web console. |

Behavioral specs live in `openspec/specs/` — when you change kernel,
durability, or control-plane behavior, change the spec with it.
