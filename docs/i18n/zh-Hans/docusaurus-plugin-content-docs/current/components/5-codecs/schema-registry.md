---
components: [schema_registry]
sidebar_label: Schema Registry
---

# Schema Registry

`schema_registry` 编解码器(Codec)在运行时通过 Confluent Schema Registry 解析内嵌的 schema id,从而解码 Confluent 线路格式(wire format)的消息。每个 schema 版本(id)至多获取一次,并按编解码器实例缓存,因此同一条流内支持多版本 schema 演进:混合多个 schema 版本的批次会先归一到字段名并集 schema(某版本缺失的列以 null 填充)再合并;同一列在不同版本中类型不同时会显式报错(错误信息含列名与两个类型),绝不猜测转换。**Protobuf** 与 **Avro** 两种 subject 均受支持,依据注册表返回的 `schemaType` 进行分发。可选的 subject 兼容性门控会在某个 subject 的注册兼容级别低于配置的最低要求时让流快速失败。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 固定值 `"schema_registry"` |
| registry_url | string | yes | — | Confluent Schema Registry 根 URL,如 `http://localhost:8081` |
| message_type | string | conditional | — | 完全限定的 Protobuf 消息类型。Protobuf schema 必填;Avro 则省略。 |
| subject | string | no | — | 用于兼容性门控的注册表 subject。 |
| min_compatibility | string | no | — | `none`、`backward`、`forward` 或 `full`(仅限小写;其他值在配置加载时被拒绝)。在首条解码消息上强制执行的 subject 最低兼容级别。需要配置 `subject`。 |
| auth | object | no | — | 注册表认证配置 |
| auth.type | string | yes (if `auth`) | — | 认证方式:`basic` 或 `bearer` |
| auth.username | string | no | — | `basic` 模式的用户名 |
| auth.password | string | no | — | `basic` 模式的密码 |
| auth.token | string | no | — | `bearer` 模式的令牌 |

## 示例

Protobuf 主题:

```yaml validate=fragment wrap=codec
codec:
  type: schema_registry
  registry_url: http://localhost:8081
  message_type: com.example.User
```

带兼容性门控的 Avro 主题:

```yaml validate=fragment wrap=codec
codec:
  type: schema_registry
  registry_url: http://registry:8081
  subject: orders-value
  min_compatibility: backward
```

Bearer 形式:

```yaml validate=fragment wrap=codec
codec:
  type: schema_registry
  registry_url: http://registry:8081
  message_type: com.example.User
  auth:
    type: bearer
    token: ${SR_TOKEN}
```

参见 `examples/schema_registry.yaml` 与 `examples/schema_registry_avro.yaml`。

## 语义

### 线路格式

```
[0x00 magic][4-byte big-endian schema id][payload]
```

编解码器校验 magic 字节,拆分出 id 与负载,然后使用该 id 从注册表解析 schema。

### 工作流程

1. 解析 Confluent 线路格式(magic + id + payload)。
2. 按 id 解析 schema(`GET {registry}/schemas/ids/{id}`),并按 id 缓存。分发目标来自响应中的 `schemaType`:
   - `PROTOBUF` → 构建 `MessageDescriptor`,并通过扁平的 Protobuf→Arrow 映射解码负载。当 `schemaType` **缺失**(旧版 registry)时按 schema 文本内容判定:JSON 对象按 Avro 解码,其余按 Protobuf;两种解析都失败时错误会列出两次尝试。
   - `AVRO` → 解析 Avro writer schema,并通过扁平的 Avro→Arrow 映射解码负载。
3. 将请求中的单行批次合并为一个列式批次。

Schema 解析被抽象在可插拔的 `SchemaResolver` trait 之后(生产环境使用 `RestSchemaResolver`,测试使用内存实现),因此无需真实注册表即可对线路格式 / 缓存 / 多版本逻辑进行单元测试。

### Avro → Arrow 映射

扁平映射与 Protobuf 一致:顶层 record 字段成为列。支持的类型:

| Avro 类型 | Arrow 类型 |
|-----------|------------|
| `null` | Null |
| `boolean` | Boolean |
| `int` | Int32 |
| `long` | Int64 |
| `float` | Float32 |
| `double` | Float64 |
| `bytes`, `fixed` | Binary |
| `string`, `enum` | Utf8 |
| `uuid` | Utf8 |
| `date` | Date32 |
| `time-millis` / `time-micros` | Time32(ms) / Time64(µs) |
| `timestamp-millis` / `timestamp-micros` | Timestamp(ms/µs, UTC) |
| `local-timestamp-*` | Timestamp (no timezone) |
| `decimal` | Decimal128 (precision ≤ 38) |
| `["null", T]` 联合类型 | nullable T |

嵌套的 record、array、map 以及多于两个分支的联合类型会被显式报错拒绝,而不是被静默展平。

### Subject 兼容性门控

配置了 `subject` 与 `min_compatibility` 后,首条解码消息会触发一次 `GET {registry}/config/{subject}?defaultToGlobal=true` 请求。subject 的注册级别会被排序(`NONE` < `BACKWARD`/`FORWARD` 含其 `_TRANSITIVE` 变体 < `FULL` 含 `FULL_TRANSITIVE`);如果低于配置的最低级别,流会失败并报错,错误信息中会指明 subject、实际级别与要求。**通过**的判定结果会在编解码器生命周期内永久缓存(该配置端点至多被访问一次);**失败**的判定则按不小于 30 秒的最小间隔在后续解码中重试——间隔内的解码直接返回上次错误、不发请求——因此 registry 的瞬时故障不会在恢复后让编解码器永久失效。这样可以在流水线处捕获兼容性策略的退化(例如 subject 被切换为 `NONE`),而不是静默解码不兼容的未来版本。

## 说明 / 非目标

- 仅限解码侧:它按 id 解析 schema;从不注册新 schema,编码则输出按行分隔的 JSON(与注册表无关)。
- 注册表响应缺少 `schemaType` 时按内容判定:schema 文本为 JSON 对象则按 Avro 解码(旧版 registry 的 Avro 主题常省略该字段),否则按 Protobuf。旧 registry 的 Avro 流与 Protobuf 流都能正常工作。
- 不执行 Debezium 信封展平——负载按其注册 schema 的声明进行映射;嵌套的信封结构(`source`、`before`)会被扁平映射拒绝。
- 不支持 Protobuf schema 引用(imports)——仅支持单文件 schema。
- Avro 解码仅使用 writer schema(不做 reader-schema 解析),与 Protobuf 路径一致。
- 本地 schema 兼容性推导(reader/writer schema 解析检查、`POST /compatibility`)不在范围内;门控读取注册表的 subject 配置,它才是兼容性的权威来源。
