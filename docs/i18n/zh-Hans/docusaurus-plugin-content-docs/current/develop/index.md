---
sidebar_position: 0
title: 开发 ArkFlow
description: 扩展引擎或参与引擎开发——内核、插件编写与性能笔记。
---

# 开发 ArkFlow

扩展 ArkFlow 或参与引擎本身开发所需的一切。

<div class="row af-cards">
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/zh-Hans/docs/develop/kernel">
      <div class="card__header"><h3>统一执行内核</h3></div>
      <div class="card__body">图、融合链、屏障、状态日志与提交前沿。</div>
    </a>
  </div>
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/zh-Hans/docs/develop/plugins">
      <div class="card__header"><h3>编写插件</h3></div>
      <div class="card__body">trait + builder + 注册,以及机器校验的文档契约。</div>
    </a>
  </div>
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/zh-Hans/docs/develop/s3-wal-performance">
      <div class="card__header"><h3>S3 WAL 后端性能</h3></div>
      <div class="card__body">远程 WAL 持久化的设计笔记与基准测试。</div>
    </a>
  </div>
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/zh-Hans/docs/contribute">
      <div class="card__header"><h3>参与贡献</h3></div>
      <div class="card__body">工作区布局、命令,以及行为变更的 openspec 工作流。</div>
    </a>
  </div>
</div>

## 仓库地图

| Crate | 内容 |
|-------|----------|
| `crates/arkflow-core` | 引擎抽象与统一执行内核(`src/executor/`)、各 trait、`MessageBatch`。 |
| `crates/arkflow-plugin` | 所有插件:`input/`、`output/`、`processor/`、`buffer/`、`codec/`、`wal/`。 |
| `crates/arkflow` | 二进制;组件 `init()` 顺序。 |
| `crates/arkflow-server` | 控制平面:Hub、Agent、存储、HTTP API。 |
| `console/` | Vite + React Web 控制台。 |

行为规格位于 `openspec/specs/`——当你修改内核、持久化或控制平面行为时,请同步修改对应规格。
