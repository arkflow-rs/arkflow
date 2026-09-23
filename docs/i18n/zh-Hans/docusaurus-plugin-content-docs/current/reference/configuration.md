---
sidebar_position: 1
---

# 顶层配置

一份 ArkFlow 配置描述整个引擎:日志、健康检查 / 控制平面服务器、要运行的流列表,以及由统一内核(unified kernel)执行的可选流式 `jobs`。文件格式由扩展名决定——`.yaml`/`.yml`、`.json` 或 `.toml` 均可接受。

```yaml validate=full
logging:
  level: info

health_check:
  enabled: true
  address: "127.0.0.1:8080"

streams:
  - id: orders
    input:
      type: memory        # any registered input — see the component pages
    pipeline:
      processors: []      # optional processors
    output:
      type: drop          # any registered output

jobs: []      # optional declarative streaming jobs, see "job" below
```

## 顶层字段

| 字段 | 类型 | 必填 | 默认值 | 描述 |
|-------|------|----------|---------|-------------|
| `streams` | array&lt;[stream](#stream)&gt; | 是* | — | 要运行的流。 |
| `jobs` | array&lt;[job](#job)&gt; | 否 | `[]` | 由统一内核运行的声明式流作业(DAG + 时间 + 状态 + 检查点)。 |
| `logging` | object | 否 | 见下文 | 日志配置。 |
| `health_check` | object | 否 | 见下文 | 健康检查与控制平面服务器。 |

\* `streams` 与 `jobs` 都默认为空列表;仅包含作业的配置是合法的(声明 `streams: []` 或直接省略)。

## `logging`

| 字段 | 类型 | 必填 | 默认值 | 描述 |
|-------|------|----------|---------|-------------|
| `level` | string | 否 | `info` | 日志级别:`debug`、`info`、`warn`、`error`。 |
| `file_path` | string | 否 | — | 把日志写入该文件而不是 stdout。 |
| `format` | string | 否 | `plain` | 日志格式:`plain` 或 `json`。 |

## `health_check`

运行一个带有 `/health`、`/readiness` 与 `/liveness` 端点的 HTTP 服务器(对 Kubernetes 有用)。当设置了 `hub_url` 时,同一服务器还承载可选的控制平面 API 与 Hub Agent(参见[控制平面](/zh-Hans/docs/operate/control-plane/overview))。

| 字段 | 类型 | 必填 | 默认值 | 描述 |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | 否 | `true` | 启动健康检查 / 控制平面服务器。 |
| `address` | string | 否 | `127.0.0.1:8080` | 监听地址。 |
| `health_path` | string | 否 | `/health` | 整体健康端点路径。 |
| `readiness_path` | string | 否 | `/readiness` | 就绪端点路径。 |
| `liveness_path` | string | 否 | `/liveness` | 存活端点路径。 |
| `api_prefix` | string | 否 | `/api/v1` | 带版本的控制平面 API 前缀。 |
| `api_token` | string | 否 | — | 保护控制平面操作与配置的可选 Bearer 令牌。 |
| `cors_origins` | array&lt;string&gt; | 否 | `[]` | 允许调用控制 API 的浏览器来源。为空则拒绝跨域调用。 |
| `hub_url` | string | 否 | — | 计算节点 Agent 模式使用的 Hub URL。缺省即为独立(standalone)模式。 |
| `node_id` | string | 否 | — | 本进程向其 Hub 上报的稳定身份。 |
| `node_token` | string | 否 | — | 共享的节点注册凭据。绝不会包含在报告中。 |
| `agent_lease_ttl_ms` | integer | 否 | `15000` | 计算节点向其 Hub 通告的租约时长(毫秒)。 |
| `agent_session_ttl_ms` | integer | 否 | `3600000` | Hub 签发的 Agent 会话凭据的硬性生命周期(毫秒);到期后 Agent 会透明地重新注册。 |
| `data_port` | integer | 否 | — | 用于跨节点 shuffle 的数据面监听端口。缺省时节点不运行网络数据面,也绝不会通告 `network_shuffle` 能力。参与 `split` 放置的每个节点都要把它与 `data_host` 一起设置。 |
| `data_host` | string | 否 | — | 对等端用于访问本节点数据面的可路由主机(例如局域网 IP)。`split` 放置要求它与 `data_port` 一同设置。 |
| `observability` | object | 否 | 见下文 | 进程级 Prometheus 指标与健康探针(参见[可观测性](/zh-Hans/docs/operate/observability))。 |

### `health_check.observability`

导出进程级可观测性端点。即使 `health_check.enabled` 为 `false`,它们依然可用——纯数据面部署(无控制平面 API、无 Hub)仍会暴露指标与探针。监听器默认绑定环回地址;生产环境请设置显式地址,或依赖主机/防火墙策略。启用控制平面服务器时,由其路由器提供相同端点,不会启动第二个监听器。

| 字段 | 类型 | 必填 | 默认值 | 描述 |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | 否 | `true` | 启动可观测性监听器(除非控制平面服务器已在提供这些端点)。 |
| `address` | string | 否 | `127.0.0.1:8081` | 监听地址。 |
| `metrics_path` | string | 否 | `/metrics` | Prometheus 暴露端点(格式 0.0.4)。 |
| `ready_path` | string | 否 | `/ready` | 就绪探针:引擎完成启动所配置的流与作业后即成功。 |
| `live_path` | string | 否 | `/live` | 存活探针:进程运行期间即成功。 |

### 独立 Hub 的启动安全

独立的 `arkflow-server` 控制平面默认 fail-closed。在把 Hub 绑定到环回地址之外之前,请将 `ARKFLOW_HUB_STORAGE` 设为持久化的 SQLite 路径,为操作员 API 设置 `ARKFLOW_OPERATOR_TOKEN`,为 Agent 注册设置 `ARKFLOW_NODE_TOKEN`。仅限明确的本地开发场景:在保持 `ARKFLOW_HUB_ADDRESS` 为环回地址的同时设置 `ARKFLOW_HUB_INSECURE_LOCAL=1`,这才允许易失状态与省略凭据。Hub 在绑定监听器之前会先恢复持久化状态,因此恢复错误会让服务器不可用,而不是提供一份残缺视图。

:::note
启用控制平面服务器时,`/ready` 与 `/live` 也会挂载在服务器地址上,与旧的 `/health`、`/readiness`、`/liveness` 端点并列(后者保持原有语义)。
:::

## Secret 引用

敏感值(密码、令牌、密钥材料)不必明文写进配置文件。任何**字符串值**都可以内嵌引用,在配置物化为 `EngineConfig` 时解析一次——文件配置(`--config`)、`--validate` 以及经控制平面下发的配置均是如此。存储/下发的配置内容保持引用原文;只有运行中的进程在内存里持有解析出的值。

| 语法 | 含义 |
|--------|---------|
| `${env:VAR}` | 环境变量 `VAR` 的值;未设置则报错。 |
| `${env:VAR:-default}` | `VAR` 的值;未设置**或为空**时使用 `default`(`${env:VAR:-}` 允许显式空值)。 |
| `${file:/path}` | 读取 `/path` 文件内容并剥离尾部换行(例如挂载的 Kubernetes Secret)。 |
| `${secret:NAME}` | 环境变量 `ARKFLOW_SECRET_<NAME>` 的值(名字逐字映射)——为凭据提供独立命名空间。支持 `:-` 默认值。在 Hub 分发部署中,Hub 会在 rollout 分发时预解析这些引用,Agent 无需持有密钥环境。 |
| `$${` | 字面 `${` 的转义。 |

规则与保证:

- 引用可以出现在字符串值的任意位置,包括嵌套 map/array(`host=${env:HOST};port=${env:PORT}` 可行)。
- 未知 scheme 的 `${...}` 原样保留(向前兼容);键名与非字符串值绝不会被改动。
- 解析结果不会被重扫:密钥值里若含 `${...}` 保持字面(防注入)。
- 解析错误只会指明配置路径与引用本身——绝不会包含密钥值。

```yaml validate=full
logging:
  level: info

health_check:
  api_token: "${env:ARKFLOW_API_TOKEN:-}"

streams: []
```

若 `ARKFLOW_API_TOKEN` 未设置,进程会在启动时失败,错误形如 `Failed to resolve secret reference at health_check.api_token: environment variable 'ARKFLOW_API_TOKEN' is not set (reference: ${env:ARKFLOW_API_TOKEN})`。

:::note
引用在所有配置物化处都是严格解析的。在 Hub–Agent 部署中,引用节点本地密钥的配置必须在密钥可解析的位置校验/应用(独立单进程部署不受影响)。中心化密钥存储将在后续 Hub 版本中提供。
:::

## stream

`streams` 中的每个条目都是一条独立的处理管道。流字段的深入文档见[组件](./component-inventory.md)一节;其结构为:

| 字段 | 类型 | 必填 | 默认值 | 描述 |
|-------|------|----------|---------|-------------|
| `id` | string | 否 | `stream-<index>` | 稳定的流标识符(必须唯一;用于 WAL 身份与 Hub 上报)。 |
| `input` | object | 是 | — | [输入](./component-inventory.md)组件(数据源)。 |
| `pipeline` | object | 是 | — | 处理器管道。 |
| `output` | object | 是 | — | 输出组件(sink)。 |
| `error_output` | object | 否 | — | 接收处理器处理失败批次的输出。 |
| `buffer` | object | 否 | — | 输入与处理器之间的缓冲 / 窗口策略。 |
| `durability` | object | 否 | — | 流级 WAL 持久化(参见[投递语义](/zh-Hans/docs/build/delivery-semantics))。 |
| `state` | object | 否 | — | 旧式窗口的状态契约。不可恢复的窗口请声明 `durability: ephemeral`;需要持久状态则使用带检查点(checkpoint)的 `jobs` 条目。 |
| `temporary` | array&lt;object&gt; | 否 | — | 用于 join 的临时存储表。 |

### `pipeline`

| 字段 | 类型 | 必填 | 默认值 | 描述 |
|-------|------|----------|---------|-------------|
| `thread_num` | integer | 否 | `1` | 处理器工作任务数量。 |
| `processors` | array&lt;object&gt; | 是 | — | 处理器组件的有序列表。 |

## job

`jobs` 中的每个条目都是一个声明式流作业:带有显式事件时间、状态、检查点与恢复设置的算子 DAG。作业在本地与流一样通过统一内核运行;Hub 向计算节点分发的也是同样的作业结构(参见[分布式作业](/zh-Hans/docs/build/distributed-jobs))。

```yaml validate=full
jobs:
  - id: local-job
    version: 1
    parallelism: 1
    max_parallelism: 128
    operators:
      - { id: source, kind: source }
      - { id: sink, kind: sink }
    edges:
      - { id: e1, from: source, to: sink, partitioned: true }
    sources:
      - operator_id: source
        input_type: generate
        config: { type: generate, context: '{"value": 1}', interval: 1s, batch_size: 10 }
        time:
          mode: processing_time
    sinks:
      - operator_id: sink
        output_type: stdout
    recovery: latest_checkpoint
```

| 字段 | 类型 | 必填 | 默认值 | 描述 |
|-------|------|----------|---------|-------------|
| `id` | string | 是 | — | 稳定的作业标识符;在 `jobs` 中必须唯一。 |
| `version` | integer | 是 | — | 作业版本;恢复时的状态格式兼容性依据它评估。 |
| `parallelism` | integer | 否 | `1` | 默认任务并行度。 |
| `max_parallelism` | integer | 否 | `128` | 用于键组(key-group)分区的上限。 |
| `operators` | array&lt;object&gt; | 是 | — | DAG 节点:`id`、`kind`(`source`、`map`、`filter`、`aggregate`、`window`、`sink`、`udf`);`join` 已保留,但在存在分布式多输入运行时之前会被拒绝。 |
| `edges` | array&lt;object&gt; | 否 | `[]` | DAG 边:`id`、`from`、`to`、`partitioned`(按键组路由,而非同子任务)。 |
| `sources` | array&lt;object&gt; | 否 | `[]` | 把组件输入附加到 `source` 算子:`operator_id`、`input_type`、`config`、`time`。 |
| `sinks` | array&lt;object&gt; | 否 | `[]` | 把组件输出附加到 `sink` 算子:`operator_id`、`output_type`、`config`。 |
| `state` | object | 否 | — | `backend`(例如 `embedded_kv`)、`durability`(默认 `durable`,或显式 `ephemeral`)、可选的稳定 `root`(或 `ARKFLOW_STATE_ROOT`)、`namespace`、`ttl_ms`、`format_version`、`max_pending_transactions`(正数;默认 4096;当窗口看到非常高的每窗口键基数时应调大,因为每个打开的窗口组或未确认的输出都会占用一个事务)、可选的正数 `max_bytes` 活动状态预算。有状态算子必需;持久状态还要求 `checkpoint`。 |
| `checkpoint` | object | 否 | — | `interval_ms`、`retention`、`object_store_uri`(例如 `file://...` 或 `s3://...`)。 |
| `recovery` | string | 否 | `latest_checkpoint` | `latest_checkpoint`、`latest_savepoint` 或 `fail`。 |

### `time`(source 的事件时间声明)

| 字段 | 类型 | 必填 | 默认值 | 描述 |
|-------|------|----------|---------|-------------|
| `mode` | string | 是 | — | `event_time` 或 `processing_time`。 |
| `timestamp_field` | string | 否 | — | 当 `mode: event_time` 时作为事件时间戳读取的字段。 |
| `watermark` | object | 否 | — | `strategy`(`bounded_out_of_orderness`(默认)或 `monotonous`)、`out_of_orderness_ms`、`idle_timeout_ms`。 |
| `allowed_lateness_ms` | integer | 否 | `0` | 超过水印(Watermark)多久的迟到事件仍被接受。 |
| `late_event_policy` | string | 否 | `drop` | 迟到事件的处理方式:`drop`、`route` 或 `update`。 |
| `late_event_route` | string | 否 | — | 当策略为 `route` 时接收被路由迟到事件的算子。 |

:::note
中间算子类别(`map`、`filter`、`aggregate`、`window`、`join`、`udf`)目前主要由流式 SQL 编译器与控制台 DAG 编排器生成。部署前务必运行 `--validate`:它执行与启动时相同的深度构建检查,并显式拒绝不受支持的算子或状态后端。`./target/release/arkflow schema` 会输出权威的 JSON Schema(包含 `jobs` 字段),供编辑器补全使用。
:::

## 运行前先校验

务必先校验配置:

```bash
./target/release/arkflow --config config.yaml --validate
```

或者输出完整的 JSON Schema 并让编辑器指向它,以获得字段级补全:

```bash
./target/release/arkflow schema > arkflow.schema.json
```

文档附带一份预生成的 schema:[`/config-schema.json`](/config-schema.json);编辑器配置参见 [IDE 自动补全](./ide-schema.md)。
