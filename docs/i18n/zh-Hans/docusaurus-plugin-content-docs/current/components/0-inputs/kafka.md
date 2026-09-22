---
components: [input/kafka]
sidebar_label: Kafka
---

# Kafka

Kafka 输入(Input)使用消费者组(consumer group)从一个或多个 Apache Kafka 主题(Topic)消费消息(Message)。只有在下游输出确认写入之后,偏移量(offset)才会推进(`enable.auto.offset.store=false`),从而在崩溃时保证至少一次(at-least-once)投递。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 常量值 `"kafka"` |
| brokers | array&lt;string&gt; | yes | — | Kafka broker 地址列表,如 `["host1:9092","host2:9092"]` |
| topics | array&lt;string&gt; | yes | — | 要订阅的主题列表 |
| consumer_group | string | yes | — | 消费者组 ID,用于偏移量协调与负载均衡 |
| client_id | string | no | — | 客户端 ID,用于监控和日志 |
| start_from_latest | boolean | no | `false` | 为 `true` 时忽略已提交的偏移量,从最新的消息开始消费 |
| fetch_min_bytes | integer | no | — | broker 响应一次拉取请求所需的最小字节数 |
| fetch_max_bytes | integer | no | — | 单次拉取请求返回的最大字节数 |
| fetch_max_partition_bytes | integer | no | — | 单次拉取中每个分区返回的最大字节数 |
| fetch_wait_max_ms | integer | no | — | broker 在响应前等待足够数据累积的最长时间(毫秒) |
| security | object | no | — | SASL 认证与 TLS 设置;完全省略即为明文。见[安全配置](#安全配置) |

## 安全配置

`security` 块在 Kafka 输入与输出中形状完全相同。生效协议取显式声明的 `protocol`;未声明时按子块存在性推断:

| `sasl` 块 | `tls` 块 | 推断出的协议 |
|--------------|-------------|-------------------|
| 无 | 无 | `plaintext`(默认) |
| 有 | 无 | `sasl_plaintext` |
| 无 | 有 | `ssl` |
| 有 | 有 | `sasl_ssl` |

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

```yaml validate=fragment wrap=input
input:
  type: "kafka"
  brokers:
    - "broker1.example.com:9094"
  topics:
    - "events"
  consumer_group: "arkflow"
  start_from_latest: false
  security:
    protocol: sasl_ssl
    sasl:
      mechanism: scram-sha-256
      username: arkflow
      password: change-me
    tls:
      ca: /etc/arkflow/certs/ca.crt
```

凡是证书路径的位置都接受内联 PEM——配置集中管理、本地无文件时很有用:

```yaml validate=fragment wrap=input
input:
  type: "kafka"
  brokers:
    - "broker1.example.com:9094"
  topics:
    - "events"
  consumer_group: "arkflow"
  start_from_latest: false
  security:
    sasl:
      mechanism: scram-sha-512
      username: arkflow
      password: change-me
    tls:
      ca: |
        -----BEGIN CERTIFICATE-----
        MIID...peer CA certificate...
        -----END CERTIFICATE-----
```

## 示例

```yaml validate=fragment wrap=input
input:
  type: "kafka"
  brokers:
    - "localhost:9092"
  topics:
    - "my_topic"
  consumer_group: "my_consumer_group"
  client_id: "my_client"
  start_from_latest: false
```

```yaml validate=fragment wrap=input
input:
  type: "kafka"
  brokers:
    - "kafka1:9092"
    - "kafka2:9092"
  topics:
    - "topic1"
    - "topic2"
  consumer_group: "app1_group"
  start_from_latest: true
  fetch_min_bytes: 1
  fetch_max_bytes: 52428800
  fetch_max_partition_bytes: 1048576
  fetch_wait_max_ms: 500
```

## 说明

- 消息会自动携带 `__meta_source`、`__meta_partition`、`__meta_offset`、`__meta_key`、`__meta_timestamp`、`__meta_ingest_time` 等元数据列,以及扩展列 `__meta_ext.topic`。
- 只有在调用 `ack()` 时(下游写入成功后)才通过 `store_offset` 推进偏移量,并结合周期性自动提交,实现至少一次投递。
