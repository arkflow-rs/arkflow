---
slug: components
title: 组件
description: ArkFlow 流中可用的输入、输出、处理器、缓冲、编解码器与临时存储组件。
sidebar_position: 5
---

# 组件

ArkFlow 内置六类共 41 个注册组件。本节的每个页面都会与实时组件注册表进行机器校验:这里的类型名称、配置 schema 与示例不会与二进制悄然漂移。

<div class="row af-cards">
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--md" href="/zh-Hans/docs/components/inputs/kafka">
      <div class="card__header"><h3>输入</h3><span class="af-badge af-badge--feature">input</span></div>
      <div class="card__body">从 Kafka、HTTP、MQTT、NATS、Pulsar、Redis、SQL、文件、WebSocket、Modbus 等数据源消费数据。</div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--md" href="/zh-Hans/docs/components/buffers/tumbling_window">
      <div class="card__header"><h3>缓冲</h3><span class="af-badge af-badge--feature">buffer</span></div>
      <div class="card__body">滚动、滑动与会话窗口——感知事件时间,原生列式。</div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--md" href="/zh-Hans/docs/components/processors/sql">
      <div class="card__header"><h3>处理器</h3><span class="af-badge af-badge--feature">processor</span></div>
      <div class="card__body">使用 SQL、Python UDF、VRL、Protobuf 转换以及批处理来变换数据。</div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--md" href="/zh-Hans/docs/components/outputs/sql">
      <div class="card__header"><h3>输出</h3><span class="af-badge af-badge--feature">output</span></div>
      <div class="card__body">投递到 SQL 数据库、Kafka(精确一次)、HTTP、Redis、InfluxDB、MongoDB 等下游。</div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--md" href="/zh-Hans/docs/components/codecs/json">
      <div class="card__header"><h3>编解码器</h3><span class="af-badge af-badge--feature">codec</span></div>
      <div class="card__body">解码传输格式:JSON、Protobuf、Debezium CDC 事件以及 Schema Registry 载荷。</div>
    </a>
  </div>
  <div class="col col--4 margin-bottom--md">
    <a class="card padding--md" href="/zh-Hans/docs/components/temporary/redis">
      <div class="card__header"><h3>临时存储</h3><span class="af-badge af-badge--feature">temporary</span></div>
      <div class="card__body">基于 Redis 的查找表,可在流水线内做数据富化。</div>
    </a>
  </div>
</div>

## 从 CLI 发现组件

也可以直接通过二进制查询注册表——参见 [CLI 参考](/zh-Hans/docs/reference/cli):

```bash
arkflow components list             # 所有组件,按类型分组
arkflow components list --kind codec --format json
arkflow components show input kafka # 查看单个组件的 schema + 示例
```

自动生成的[组件清单](/zh-Hans/docs/reference/component-inventory)是同一份数据的表格呈现。
