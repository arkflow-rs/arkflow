---
components: [output/kafka]
description: ArkFlow 文档页面。
---

# Kafka

Kafka 输出(Output)基于 librdkafka 向 Apache Kafka 主题(Topic)生产消息(Message)。它支持按键分区、压缩、可配置的确认机制,以及可选的精确一次(exactly-once)事务性生产。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | 固定值 `"kafka"` |
| brokers | `array<string>` | yes | — | Kafka broker 地址列表。 |
| topic | object | yes | — | 目标主题(表达式;见下文)。 |
| key | object | no | — | 用于分区的消息键(表达式;见下文)。 |
| client_id | string | no | — | 客户端标识符。 |
| compression | string | no | — | `none`、`gzip`、`snappy`、`lz4` 之一。 |
| acks | string | no | — | 确认级别:`0`、`1` 或 `all`。 |
| value_field | string | no | — | 用作消息载荷的记录字段。 |
| exactly_once | boolean | no | `false` | 启用精确一次事务性生产(L2)。 |
| transactional_id | string | no | — | 稳定的事务 ID;当 `exactly_once` 为 `true` 时必填。 |
| security | object | no | — | SASL 认证与 TLS 设置;完全省略即为明文。见[安全配置](#安全配置)。 |

### 安全配置

`security` 块在 Kafka 输入与输出中形状完全相同。生效协议取显式声明的 `protocol`;未声明时按子块存在性推断:`sasl`+`tls` 皆有 → `sasl_ssl`,仅 `sasl` → `sasl_plaintext`,仅 `tls` → `ssl`,皆无 → `plaintext`。

| 字段 | 类型 | 描述 |
|-------|------|-------------|
| protocol | string | `plaintext` \| `ssl` \| `sasl_plaintext` \| `sasl_ssl`。显式声明优先于推断;与已提供的子块矛盾属于配置错误。 |
| sasl.mechanism | string | `plain` \| `scram-sha-256` \| `scram-sha-512` |
| sasl.username / sasl.password | string | 凭据;`plain`/`scram-*` 机制下必填且非空 |
| tls.ca | string | 校验 broker 的 CA 证书:文件路径**或内联 PEM 文本**(通过 `-----BEGIN` 标记自动识别) |
| tls.cert / tls.key | string | mTLS 客户端证书与私钥:文件路径或内联 PEM |
| tls.key_password | string | 客户端私钥的保护口令 |
| tls.insecure_skip_verify | boolean | `true` 时关闭 broker 证书校验——仅限开发/测试环境 |

:::warning
`sasl.username`、`sasl.password` 与 `tls.*` 的值以明文存储在配置中。请妥善保护配置文件与控制面的存储。
:::

配置不一致会在配置校验阶段(任何流启动之前)快速失败:`sasl_*` 协议缺少 `sasl` 块、SCRAM 凭据缺失、或显式 `plaintext` 协议与 `sasl`/`tls` 块并存,均会被拒绝。

```yaml validate=fragment wrap=output
output:
  type: "kafka"
  brokers:
    - "broker1.example.com:9094"
  topic:
    type: "value"
    value: "events-out"
  security:
    protocol: sasl_ssl
    sasl:
      mechanism: scram-sha-512
      username: arkflow
      password: change-me
    tls:
      ca: /etc/arkflow/certs/ca.crt
      cert: /etc/arkflow/certs/client.crt
      key: /etc/arkflow/certs/client.key
```

### 表达式对象

`topic` 与 `key` 是 `Expr<String>` 对象,具有以下形态之一:

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `value`(静态)或 `expr`(SQL 表达式)。 |
| value | string | yes (`value`) | 静态字符串值。 |
| expr | string | yes (`expr`) | 对每条消息求值的 SQL 表达式。 |

## 示例

### 静态主题与键

```yaml validate=fragment wrap=output
output:
  type: "kafka"
  brokers:
    - "localhost:9092"
  topic:
    type: "value"
    value: "my-topic"
  key:
    type: "value"
    value: "my-key"
  client_id: "my-client"
  compression: "snappy"
  acks: "1"
```

### 通过 SQL 表达式动态指定主题

```yaml validate=fragment wrap=output
output:
  type: "kafka"
  brokers:
    - "localhost:9092"
  topic:
    type: "expr"
    expr: "concat('1','x')"
  acks: "all"
  value_field: "message"
```

### 精确一次生产

```yaml validate=fragment wrap=output
output:
  type: "kafka"
  brokers:
    - "localhost:9092"
  topic:
    type: "value"
    value: "events"
  exactly_once: true
  transactional_id: "arkflow-events-tx"
  acks: "all"
```

## 注意事项

- 当 `exactly_once: true` 时,`transactional_id` 必须为非空且跨重启保持稳定的值,以便 broker 能隔离(fence)过期的生产者 epoch(即僵尸隔离,zombie fencing)。否则,构建器将拒绝该配置。
- 启用精确一次后,每个已确认的消息批次(Batch)都在一个 Kafka 事务内生产(开始 → 发送 → 提交)。失败时事务中止,该批次被重放。
- 端到端投递语义契约参见[精确一次处理](/zh-Hans/docs/build/exactly-once)。
