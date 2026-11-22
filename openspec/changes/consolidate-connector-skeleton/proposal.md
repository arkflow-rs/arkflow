## Why

2026-11 代码审查（`CODE-REVIEW-2026-11-15.md` §5）的根因 1 是"拷贝熵：兄弟组件复制后各自漂移"，治理动作 3 即"连接器共享骨架"。`output/payload.rs` + `ensure_row_aligned`（批次 1）证明了共享模块形态可行，但骨架的其余部分仍只完成一半（HEAD 8ffbc386 逐一核实）：

- **`vector_util` 收编不全**：`truncate_body` 已 7 处共享，但 `processor/embedding.rs:383-387`、`processor/llm.rs:486-491`、`processor/lookup.rs:967-971` 仍各持一份逐字符相同副本；`build_http_client`（timeout + loopback 免代理）有 4 份 inline 副本（`embedding.rs:260-275`、`llm.rs:397-411`、`output/milvus.rs:441-454`、`lookup.rs:930-947`）——其中 milvus output 用着共享 `truncate_body` 却不用共享 client 构建，是漂移的活标本；URL 基址拼接 4 份、bearer 头循环 8 份逐字重复。
- **RetryUtils 零生产调用 + 三份手写重试**：`pulsar/common.rs:118-170` 的 `RetryUtils` 有测试、零调用者；`input/pulsar.rs:223-334` 注释自曝"the policy was validated and then never applied"，手写了同语义退避循环；`output/qdrant.rs:180-228` 与 `output/milvus.rs:183-260` 是互相复制的重试循环（4xx 短路注释逐字相同）；`embedding`/`llm`/`vector_search`/`milvus_search` 四个 AI 处理器对 429/5xx **零重试**（单发失败即批失败→Job 重启循环）。
- **退避溢出缺陷仍在**：`output/qdrant.rs:227` 与 `output/milvus.rs:259` 的 `100 * 2u64.pow(attempt - 1)` 无封顶无 checked，`retry_count` 是用户可配 u32——配置 ≥64 即 debug panic / release 回绕。
- **通道容量魔数**：`flume::bounded(1000)` 7 处（`input/mqtt.rs:83`、`nats.rs:105`、`websocket.rs:77`、`pulsar.rs:114`、`redis.rs:185,186`、`http.rs:111`）vs 内核 `DEFAULT_CHANNEL_CAPACITY = 1024`（`executor/graph.rs:14`）。

## What Changes

- **共享基建**（plugin crate 内，全部 `pub(crate)`，无外部 API 变化）：新建 `src/retry.rs`——`RetryUtils` 从 `pulsar/common.rs` 迁出并扩展（退避封顶 `max_delay_ms` + checked 指数、可选 jitter、HTTP 可重试分类：429/5xx/传输错误可重试，4xx 确定性单次失败）；新建 `src/http_util.rs`——收编 `build_http_client`、`truncate_body`、URL 基址拼接、bearer 头应用；`vector_util` 收缩为向量专属（`extract_vectors`/`append_column`/`escape_identifier`/test_support）。
- **采用方收编**：qdrant/milvus output 的复制循环、pulsar input 的手写循环全部替换为共享 `RetryUtils`（**默认值不变**：qdrant 3、milvus 0——默认值统一是行为变更，不在本 change）；pulsar input 的 `RetryConfig` 从"校验后弃用"变为实际生效。
- **AI 处理器补重试**：embedding/llm/vector_search/milvus_search 新增可选 `retry_count`（默认 3，0=关闭），暂态失败按共享退避重试；确定性失败（4xx、维度不一致、非 Utf8 列）行为不变；重试耗尽后错误透传语义不变。**行为变化**：处理器失败前的耗时与请求次数变化，进 CHANGELOG。
- **通道容量对齐**：7 处 `bounded(1000)` → 共享常量 1024，与内核对齐；新增卫生钉扎防回漂。
- **溢出修复**：任意可配置的 `retry_count` 不再产生 panic/回绕（封顶退避）。

## Capabilities

### New Capabilities

- `connector-resilience`: 组件级弹性纪律——共享重试实现（封顶退避、可重试分类）、AI/向量/发布类组件的采用义务、组件内 delivery 通道容量与内核对齐。

### Modified Capabilities

- `ai-vector-ingestion`: 新增 AI 处理器（embedding/llm/vector_search/milvus_search）暂态失败有界重试需求与 `retry_count` 配置面。
- `pulsar-io`: 新增"input 接收退避 SHALL 由 RetryConfig 经共享实现治理"需求（修复配置后弃用的缺陷）。

## Impact

- `crates/arkflow-plugin`：新模块 `retry.rs`/`http_util.rs`；`vector_util.rs` 收缩；采用方 9 个文件（qdrant/milvus output、pulsar input、embedding/llm/vector_search/milvus_search、mqtt/nats/websocket/redis/http input 的通道行）。
- 配置面：4 个 AI 处理器新增可选 `retry_count` 字段 → 组件 schema 再生成（`ARKFLOW_REGENERATE_DOCS=1` snapshot 测试）+ 组件文档页（EN+zh）+ CHANGELOG。
- `pulsar/common.rs` 保留薄 re-export 或更新调用点（crate 内部，无外部影响）。
- 用户可见差异：AI 处理器默认对暂态失败重试 3 次（失败路径耗时变长、成功率变高）；pulsar input 配置的重试策略开始生效；通道容量 1000→1024。
- 不改内核、不改 `Error` 变体（归 `specify-connector-error-contract`）。

## Non-goals

- 不统一 Ack 体系（core 4 种 + 插件 4 种本地类型：KafkaAck frontier/NatsAck enum/PulsarAck/MqttAck 语义各异，收编是独立大工程）。
- 不把 `input/redis.rs` 的自建 client 收进共享 `component::redis`（连接语义重构，非复制粘贴）。
- 不统一 milvus 应答信封处理（`code==0` 判败是 milvus 特有语义，不是拷贝漂移）。
- 不统一 4 个测试 mock 变体（各服务响应语义不同，测试形态差异是有意的）。
- 不统一 qdrant/milvus output 的重试**默认值**（qdrant 3 / milvus 0 是已发布的用户可见默认，变更需独立决策）。
- 不让组件内 delivery 通道容量可配置（对齐 1024 常量即可，不加配置面）。
