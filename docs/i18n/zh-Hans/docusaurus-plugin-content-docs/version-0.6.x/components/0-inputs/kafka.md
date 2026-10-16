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
| fetch_min_bytes | integer | no | — | broker 响应一次拉取请求所需的最小字节数。默认(`1`)对延迟友好:broker 一有数据立即返回。调大则让 broker 等数据凑量(最多等 `fetch_wait_max_ms`)——吞吐杠杆,但安静时段会给首条消息引入取数等待。 |
| fetch_max_bytes | integer | no | — | 单次拉取请求返回的最大字节数 |
| fetch_max_partition_bytes | integer | no | — | 单次拉取中每个分区返回的最大字节数 |
| fetch_wait_max_ms | integer | no | — | broker 为凑够 `fetch_min_bytes` 而等待的最长时间(毫秒)。仅在 `fetch_min_bytes` 大于 1 时有意义;`fetch_min_bytes: 1` 时 broker 立即响应,该设置不会触发。 |
| batch_max_rows | integer | no | `1024` | 单次 read 聚合进一个批的最大消息数。小于 1 的值钳制为 1。调小它**不是**延迟优化——攒批从不等待;更小的值只是缩小失败后至少一次语义的重投单位。 |
| batch_max_bytes | integer | no | `8388608` (8 MiB) | 单次 read 批内累计 payload 字节数的**软上界**，按完整 payload 计数：使累计首次达到上限的那条 payload 照常计入，单条 payload 本身可以超过该上限；首条消息必定纳入。小于 1 的值钳制为 1。 |
| security | object | no | — | SASL 认证与 TLS 设置;完全省略即为明文。见[安全配置](#安全配置) |
| transactional_offsets | boolean | no | `false` | L3 精确一次:为配对 Kafka 输出的 `offset_commit_group` 注册本消费者组以在事务内提交位点。此时 `ack()` 只推进内存 frontier——broker 组位点仅随输出的事务前进。 |

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

## 吞吐 vs 延迟

攒批本身对延迟是中性的:首条消息阻塞等待,已缓冲的消息立即排空——从不为凑批等待,低流量时批就是单条。吞吐/延迟的取舍在 **broker 侧 fetch 参数**与**批量上界**上:

**延迟优先:保持默认即可。** `fetch_min_bytes` 默认 `1`,broker 一有数据立即响应(该设置下 `fetch_wait_max_ms` 不会触发);批量上界无需调小——更小的 `batch_max_rows` 只缩小失败后的重投单位,不会降低延迟。

```yaml validate=fragment wrap=input
input:
  type: "kafka"
  brokers:
    - "localhost:9092"
  topics:
    - "events"
  consumer_group: "latency-group"
  start_from_latest: false
  # Defaults are latency-friendly: fetch_min_bytes=1, batch_max_rows=1024
```

**吞吐优先:放大批量上界,让 broker 攒数据。** 更大的 `fetch_min_bytes` 让 broker 每次返回更多数据(最多等 `fetch_wait_max_ms`),本地队列被填满后一次 `read()` 能聚出更大的批。代价:安静时段的首条消息最多等 `fetch_wait_max_ms`;失败的批整段重投(至多 `batch_max_rows` 条);内存随 `batch_max_rows × 单条消息大小` 增长。

```yaml validate=fragment wrap=input
input:
  type: "kafka"
  brokers:
    - "localhost:9092"
  topics:
    - "events"
  consumer_group: "throughput-group"
  start_from_latest: false
  batch_max_rows: 8192
  batch_max_bytes: 67108864
  fetch_min_bytes: 1048576
  fetch_max_bytes: 104857600
  fetch_max_partition_bytes: 8388608
```

持续负载下,更大的批**不会**推高端到端延迟——排队等待占主导,更高的吞吐更快清空积压。真正变大的是单批处理步长(当前批处理完才开始下一次 `read()`),表现为单次分发处理时间变长,而不是到达→落库变慢。

## 说明

- 一次 `read()` 会把多条消息聚合成一个批:首条消息阻塞等待,随后**不等待**地排空客户端已缓冲的消息,受 `batch_max_rows` / `batch_max_bytes` 双上界约束。低流量时批为单条、零额外延迟——不会为凑批而等待。
- 消息会自动携带 `__meta_source`、`__meta_partition`、`__meta_offset`、`__meta_key`、`__meta_timestamp`、`__meta_ingest_time` 等元数据列,以及扩展列 `__meta_ext.topic`。多消息批内每一行携带**各自消息**的 `__meta_partition`/`__meta_offset`/`__meta_key`/`__meta_timestamp`/`__meta_ext` 值;`__meta_ingest_time` 为整批一个时间戳。`__meta_key` 与 `__meta_timestamp` 仅在批内至少一行有值时以 nullable 列出现(缺值的行为 NULL)——全批皆无时不出现在 schema 中,与逐条路径形状一致。
- 每条 Kafka record header 会作为 `header_<key>` 条目写入 `__meta_ext` 映射列。重复的 header key 会保留全部值:首次出现使用 `header_<key>`,后续出现附加位置后缀(`header_<key>_2`、`header_<key>_3`…)。值按 UTF-8 lossy 解码(非法字节替换为 U+FFFD),无值的 header 映射为空字符串。
- 确认按 `(topic, partition)` 的连续 offset 段进行:批的 ack 把提交位点推进到段内最后一个 offset,补偿(`undo`)则回退到段首——至少一次语义下整批作为一个单元重投。墓碑消息(null payload,如 compacted topic)在批外结算,不进入数据批。
- 只有在调用 `ack()` 时(下游写入成功后)才通过 `store_offset` 推进偏移量,并结合周期性自动提交,实现至少一次投递。
- 声明 `transactional_offsets: true` 后,`ack()` 只推进内存 frontier 并跳过 `store_offset`:broker 组位点折入配对事务性 Kafka 输出的事务内提交(`offset_commit_group` 指名本输入的 `consumer_group`),消除「提交后崩溃」的重复窗口。输出把事务内提交钳制到该 frontier,其他分支仍在结算的记录不会被跳过。本输入的 `undo()` 补偿同样只回退内存 frontier——组的 broker 位点只有唯一写者:配对输出的事务。配对是进程内的,要求单主题订阅,且在启动期校验:没有任何输出认领本组的配置启动即失败(配置错误)——参见[精确一次处理](/zh-Hans/docs/build/exactly-once)。
