## Context

`output/payload.rs` + `codec_helper::ensure_row_aligned`（6 个 output 共用，批次 1）是共享骨架的正面样板；本 change 把同样的形态推广到重试、HTTP 工具与通道容量。现状（HEAD 8ffbc386 核实，file:line 见 proposal）：`RetryUtils`（`pulsar/common.rs:118-170`：`max_attempts`/`initial_delay_ms`/`backoff_multiplier`/`max_delay_ms` 封顶，泛型 `E: Display`，**无 jitter、无状态码分类、零生产调用**）；qdrant/milvus 各自的复制循环在 `attempt` 上裸算 `2u64.pow`；四个 AI 处理器零重试；`vector_util` 的通用工具（client 构建/截断）与向量专属能力混居，导致 lookup 这类非向量组件要么复制要么误用向量模块。

## Goals / Non-Goals

见 proposal。设计要点：收编是**无行为变化的机械替换**（输出默认值不动），唯一的行为增量是 AI 处理器重试与 pulsar RetryConfig 生效——两者都是 spec 场景显式声明的修复。

## Decisions

### D1 — 模块布局：`retry.rs` + `http_util.rs`，`vector_util` 收缩

- `src/retry.rs`：`RetryUtils`/`RetryConfig` 从 `pulsar/common.rs` 整体迁入并扩展：结构体增 `jitter: f64`（0=关，默认 0 保持现行为）；新增 `is_retryable_status(u16) -> bool`（429、5xx、以及 reqwest 传输/连接错误→true；其余 4xx→false）；退避计算改为 `checked` + `max_delay_ms` 封顶（见 D3）。`pulsar/common.rs` 更新为 re-export（`input/pulsar.rs:21` 的既有 `pub use` 路径不断）。
- `src/http_util.rs`：`build_http_client`（自 `vector_util.rs:208` 迁入）、`truncate_body`（自 :198 迁入）、`join_url_base(base, path)`、`apply_headers(builder, headers)`。全 `pub(crate)`；同 crate 内直接改调用点，不留兼容 re-export。
- `vector_util.rs` 保留 `extract_vectors`/`append_column`/`escape_identifier`/`test_support::MockApi`。
- **否决**：把通用工具留在 `vector_util` 并让 lookup 继续引用——模块名撒谎，且是下次复制漂移的温床；一个大 `connector_util.rs`——职责过宽，重试与 HTTP 工具的变更节奏不同。

### D2 — AI 处理器重试的配置面：单字段 `retry_count`，默认 3

四个处理器新增可选 `retry_count: u32`（默认 3，0=关闭），退避参数用 `RetryConfig` 内置默认（100ms 起、×2、封顶 5s、无 jitter），**不暴露**完整退避配置——与 qdrant/milvus output 的既有 `retry_count` 字段名与语义对齐（用户心智一致），schema 面最小。重试判定：HTTP 429/5xx/传输错误→重试；4xx/维度不一致/列类型错误→立即失败（确定性）。重试耗尽→错误透传（状态码+响应体片段），与既有「API 错误透传」场景兼容（`ai-vector-ingestion` 该场景不冲突，无需 MODIFIED）。
**否决**：完整 `retry` 配置块（initial/multiplier/jitter 全暴露）——四个处理器各加 5 字段的 schema/文档成本与用户价值不成比例；需要时后续再加。

### D3 — 溢出修复的语义：封顶优先于精确指数

退避 = `min(initial × multiplier^(attempt-1), max_delay_ms)`，幂运算用 `saturating_pow`；`retry_count` 不设上限校验（配置 64、1000 都合法，只是延迟封顶在 5s）。**否决**：给 `validate_retry_config` 加 `max_attempts <= 63` 上限——把实现缺陷转嫁给用户配置约束，且与"封顶"语义重复。qdrant/milvus 既有的「瞬时失败按退避重试」场景（100ms/200ms）不受影响（小 attempt 不触顶）。

### D4 — pulsar input 接收循环：策略与延迟调度走共享实现（实施期修订）

实施核实发现：`retry_config` 字段（max_attempts/initial_delay/multiplier/max_delay）在当前代码中**已经生效**（接收循环实际使用，"validated but never applied" 是历史注释），真正的分裂只是延迟调度为手写乘法。且该循环有两个不能被 `retry_with_backoff` 整体包裹的语义：idle 锁预算超时 `continue` 不消耗重试预算、退避 sleep 与取消 select 竞争。因此接入方式为：新增 `retry::backoff_after`（与共享实现同一 cap/jitter 调度），循环形态保留——策略与延迟调度自此单一来源，取消安全语义不变（`input-cancellation-safety` 不回退）。**偏离原设计**（原计划整体替换为 `retry_with_backoff`）的原因如上；spec 场景"配置的重试策略实际生效"与"退避期间取消立即返回"均不受影响。`validate_retry_config`（common.rs）保留。

### D5 — 通道容量：共享常量 + 卫生钉扎

plugin crate 新增 `pub(crate) const DELIVERY_CHANNEL_CAPACITY: usize = 1024;`（附注释指向内核 `executor/graph.rs` `DEFAULT_CHANNEL_CAPACITY`）；7 处 `bounded(1000)` 替换。卫生测试（可挂在 `tests/connector_error_contract.rs` 同层或独立文件）扫描 `src/input/` 禁止 `bounded(1000)` 字面量与裸 `1000` 容量字面量，防止回漂。
**否决**：把内核常量 `pub` 出来直接引用——内核容量未来计划可配置化（`channel_capacity`），插件钉死具体值反而更稳；用测试断言两常量相等即可表达"对齐"意图。

### D6 — 测试策略

- `retry.rs` 单测：分类表（429/500/503/连接错误→true；400/401/404→false）、封顶（attempt=64 无 panic 且延迟 == max_delay_ms）、jitter 边界。
- 采用方行为测试复用既有 mock 基建：qdrant/milvus 经 `MockApi` 断言 503 两次后成功（既有场景不动）；4 个处理器各加 429 重试用例；pulsar input 断言 RetryConfig.delay 实际作用于重试间隔。
- 结构钉扎：`grep` 级测试禁止 `2u64.pow` 于 `output/`（防复制循环回潮）与 `bounded(1000)`。

## Risks / Trade-offs

- [AI 处理器默认重试改变失败路径耗时（最多 ~3 次退避）] → 正是修复目标（429 即批失败→Job 重启循环代价更高）；`retry_count: 0` 一键回退旧行为；CHANGELOG 与文档显式声明。
- [pulsar RetryConfig 生效是行为变化] → 对从未配置重试的部署零影响（默认值就是旧手写循环的近似参数）；配置过的部署获得期待已久的行为。
- [机械收编触及 9+ 文件可能引入搬运笔误] → 收编 commit 保持纯移动（函数体逐字符不变），行为变更分离在独立 commit；行为测试全绿后再合并。
- [`http_util` 与 `retry` 可能吸引后续组件继续收敛（mongodb/influxdb/http output 的纯 timeout client）] → 本 change 只收编**逐字符同语义**副本；纯 timeout 版语义不同（无 loopback 免代理），留待有真实需求时决策。

## Migration Plan

无数据/配置迁移。三个行为变化（处理器默认重试、pulsar RetryConfig 生效、通道容量 1000→1024）进 CHANGELOG；文档 EN+zh 同步。回滚 = revert 单 PR。

## Open Questions

无。
