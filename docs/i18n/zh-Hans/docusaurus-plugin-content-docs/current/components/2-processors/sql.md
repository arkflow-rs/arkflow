---
components: [processor/sql]
description: ArkFlow 文档页面。
---

# SQL

SQL 处理器以 DataFusion 作为查询引擎,对传入的消息批次执行 SQL 查询。每个批次都会注册为一张临时表(默认名为 `flow`,若设置了 `table_name` 则使用该名称),从而可以在 SQL 中对它进行过滤、投影、与临时数据源连接或聚合。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `sql` |
| query | string | yes | — | 要对传入批次执行的 SQL 查询语句。 |
| table_name | string | no | `flow` | 在查询中引用传入批次所用的表名。 |
| temporary_list | array&lt;object&gt; | no | — | 可在查询中引用的额外临时数据源。 |

### `temporary_list` 条目

每个条目将一个外部数据源注册为一张命名表。

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| name | string | yes | — | 注册到引擎的临时数据源名称。 |
| table_name | string | yes | — | 在 SQL 查询中引用该数据源所用的表名。 |
| key | object | yes | — | 用于在临时数据源中查找数据的键。 |

### `key`

标签联合(`type` 字段选择具体变体,采用 snake_case 形式):

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `expr` \| `value` |
| expr | string | no | — | 对批次逐行求值的表达式(当 `type: expr` 时使用)。 |
| value | string | no | — | 用作键的静态字符串值(当 `type: value` 时使用)。 |

## 性能与语义

- **计划缓存。** SQL 语句对每个 processor 是固定的,因此其分析与优化后的计划按工作者上下文缓存,只要输入批的 schema 不变就复用——跳过逐批的重新分析与重新优化,执行时始终读取刚注册的那一批。
- **时间函数保持新鲜。** `now()`、`current_date`、`current_time`、`current_timestamp` 在计划优化时本会被折叠为常量。包含它们的查询跳过优化计划缓存,每一批都重新优化,因此每一批观察到的都是它被处理时的时间。
- **上下文池。** 查询在少量 DataFusion `SessionContext` 组成的池(四个上下文)上执行,并发的流水线工作者可以并行执行;工作者在拿到空闲上下文前最多等待 10 秒即失败,上下文用后总会归还池中——反复失败不会耗尽池。

## 示例

### 基础 SQL 查询

```yaml validate=fragment wrap=processors
- type: "sql"
  query: "SELECT id, name, age FROM flow WHERE age > 18"
  table_name: "flow"
```

### 使用临时数据源的 SQL 查询

```yaml validate=fragment wrap=stream
temporary:
  - name: user_profiles
    type: "redis"
    mode:
      type: single
      url: redis://127.0.0.1:6379
    redis_type:
      type: "string"
    codec:
      type: "json"

pipeline:
  processors:
    - type: "sql"
      query: "SELECT u.id, u.name, p.title FROM flow u JOIN profiles p ON u.id = p.user_id"
      table_name: "flow"
      temporary_list:
        - name: "user_profiles"
          table_name: "profiles"
          key:
            type: "expr"
            expr: "user_id"
```
