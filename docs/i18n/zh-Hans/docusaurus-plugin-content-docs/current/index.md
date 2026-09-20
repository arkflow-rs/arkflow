---
sidebar_position: -1
title: ArkFlow 文档
description: 用 Rust 构建持久、高吞吐的流式流水线——并以机群(Fleet)方式运维。
---

# ArkFlow 文档

ArkFlow 是一个用 Rust 编写的高性能流处理引擎。它从任意数据源消费数据,用 SQL / Python / VRL 进行处理,再投递到任意下游——具备预写日志(WAL)持久化、检查点状态,以及用于运行机群的可选控制平面。

<div class="row af-cards">
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--lg" href="/zh-Hans/docs/get-started/install">
      <div class="card__header"><h3>🚀 快速上手</h3></div>
      <div class="card__body">
        安装二进制文件,五分钟内运行你的第一条流水线,并让它经受住崩溃的考验。
      </div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--lg" href="/zh-Hans/docs/build/streams">
      <div class="card__header"><h3>🔧 构建</h3></div>
      <div class="card__body">
        流、作业与 DAG;组件目录;持久化与投递语义;面向任务的实战配方。
      </div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--lg" href="/zh-Hans/docs/sql">
      <div class="card__header"><h3>🗄️ SQL</h3></div>
      <div class="card__body">
        完整的 SQL 语言能力:数据类型、SELECT、聚合、窗口函数和用户自定义函数(UDF)。
      </div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--lg" href="/zh-Hans/docs/operate/overview">
      <div class="card__header"><h3>🛠️ 运维</h3></div>
      <div class="card__body">
        Kubernetes 部署、控制平面与 Web 控制台、可观测性,以及一份恢复手册。
      </div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--lg" href="/zh-Hans/docs/reference/cli">
      <div class="card__header"><h3>📖 参考</h3></div>
      <div class="card__body">
        CLI、完整的 HTTP API、配置 Schema,以及自动生成的组件清单。
      </div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--lg" href="/zh-Hans/docs/develop/kernel">
      <div class="card__header"><h3>⚙️ 开发</h3></div>
      <div class="card__body">
        统一执行内核、插件编写,以及来自源码的性能笔记。
      </div>
    </a>
  </div>
</div>

## 选择你的路径

| 你是…… | 从这里开始 |
|------------|------------|
| 正在评估 ArkFlow | [快速入门](/zh-Hans/docs/get-started/quickstart) → [架构](/zh-Hans/docs/build/architecture) |
| 正在编写流水线 | [流](/zh-Hans/docs/build/streams) → [组件](./components/) → [实战配方](./build/recipes/) |
| 正从 Kafka Connect / Flink 迁移 | [兼容性策略](/zh-Hans/docs/reference/compatibility) → [投递语义](/zh-Hans/docs/build/delivery-semantics) |
| 正在生产环境运行 | [Kubernetes](/zh-Hans/docs/operate/kubernetes) → [可观测性](/zh-Hans/docs/operate/observability) → [恢复](/zh-Hans/docs/operate/recovery) |
| 正在运行机群 | [控制平面](/zh-Hans/docs/operate/control-plane/overview) → [Web 控制台](/zh-Hans/docs/operate/control-plane/console) |
| 正在扩展引擎 | [编写插件](/zh-Hans/docs/develop/plugins) → [执行内核](/zh-Hans/docs/develop/kernel) |
