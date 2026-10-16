---
sidebar_position: 0
title: 用 ArkFlow 构建
description: 流、作业、组件与持久化模型——构建流水线所需的一切。
---

# 用 ArkFlow 构建

ArkFlow 中的一条流水线,是对数据从**输入**经过**处理器**(以及可选的**缓冲**)流向**输出**这一过程的声明式 YAML 描述——底层由列式(Apache Arrow)数据模型支撑。同一份描述会编译到统一的执行内核上,无论它以单条流、本地作业 DAG,还是分布式作业切片的形式运行。

<div class="row af-cards">
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/zh-Hans/docs/build/streams">
      <div class="card__header"><h3>流(Streams)</h3></div>
      <div class="card__body">流水线解剖:输入 → 处理器 → 输出、错误输出与编解码器。</div>
    </a>
  </div>
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/zh-Hans/docs/build/jobs">
      <div class="card__header"><h3>作业(Jobs)</h3></div>
      <div class="card__body">带事件时间、键控状态与检查点的多步骤 DAG——本地或分布式运行。</div>
    </a>
  </div>
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/docs/build/distributed-jobs">
      <div class="card__header"><h3>分布式作业</h3></div>
      <div class="card__body">控制平面如何在机群中分配、共置与恢复作业切片。</div>
    </a>
  </div>
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/docs/build/delivery-semantics">
      <div class="card__header"><h3>持久化与投递</h3></div>
      <div class="card__body">默认至少一次;Kafka 输出可精确一次;WAL 与检查点机制。</div>
    </a>
  </div>
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/zh-Hans/docs/build/architecture">
      <div class="card__header"><h3>架构</h3></div>
      <div class="card__body">列式数据模型、元数据列,以及配置如何变成运行中的图。</div>
    </a>
  </div>
  <div class="col col--6 margin-bottom--md">
    <a class="card padding--md" href="/docs/build/recipes/1-kafka-to-sql">
      <div class="card__header"><h3>实战配方</h3></div>
      <div class="card__body">任务指南与端到端案例,每个都有 CI 校验过的示例支撑。</div>
    </a>
  </div>
</div>

完整的组件目录见侧边栏的**组件**部分,SQL 方言在 **SQL** 部分单独成册。
