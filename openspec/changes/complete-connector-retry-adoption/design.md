## Context

共享重试骨架（`retry.rs`：`RetryConfig`/`retry_with_backoff`/`retry_with_backoff_classified`/`is_retryable_status`/`advance_backoff`/`sleep_duration`；`http_util.rs`：`send_with_retry` 等）已随 `consolidate-connector-skeleton` 落地，采用方样板两种：qdrant/milvus 式（`retry_with_backoff_classified` + 自定义分类闭包，保组件自有错误文案）与 `send_with_retry`（AI 处理器，接受共享文案）。本 change 的 5 处收编对象都有既定错误文案与测试钉扎，收编原则与骨架 change 一致：**机械替换保语义，行为增量必须 spec 声明**。分诊细节（行号、循环形态、测试钉扎点）见 proposal 的 Why 一节。

## Goals / Non-Goals

见 proposal。设计要点：5 处收编中 mqtt 是零行为变化（调度逐字节复现），influxdb/http 的行为增量是「4xx 分类」（修复目标本身），kafka 两处是「曲线形状」（线性→指数，低风险路径）；另将卫生测试从「可被写法变体绕过」修复为「按形态匹配」。

## Decisions

### D1 — influxdb/http 走 qdrant 式 classified，不走 `send_with_retry`

两处 output 的错误文案是既有测试钉扎的组件契约（influxdb `test_flush_server_error_retains_buffered_lines` pin 500 文案；http 的 `server_error_is_retried_then_reported` pin 503 行为、错误走 `"HTTP Request Failed: Status code {}, response: {}"` + 64KiB 分块截断）。`send_with_retry` 的共享文案（`"{component} request failed/returned …"` + 512 字符截断）会改变这些文本。采用 qdrant/milvus 先例：自定义分类闭包（transport→retryable、429/5xx→retryable、其余→deterministic）+ `retry_with_backoff_classified`，成功/失败路径的错误字符串逐字符保留。**否决** `send_with_retry`：文案迁移属于 `specify-connector-error-contract` 已稳定的错误契约，本 change 不动它。

### D2 — influxdb `retry_count` 保持「总尝试数」语义

`max_attempts = retry_count.max(1)`（现状 394/398 行语义），不采用 qdrant 的 `+1` 映射；schema 已诚实声明 "Number of HTTP attempts per flush"。**否决**统一为「重试次数」语义：会把已发布配置的 `retry_count: 3` 从 3 次尝试静默变为 4 次，需要弃用周期与独立决策（Non-goal 记录）。本 change 对该字段的唯一变化是行为分类与封顶。

### D3 — http `retry_count` 增 serde default 3

原字段无 default（缺失即配置拒绝），与 AI 处理器（可选默认 3）、qdrant（默认 3）不一致。加 `#[serde(default = "default_retry_count")]`：非破坏（既有配置不受影响），语义保持「重试次数」（`max_attempts = retry_count + 1`，qdrant 式）。schema 重生成后展示默认值。

### D4 — mqtt 走 `retry_with_backoff`（非 classified）+ 常量 RetryConfig

现状 `reconnect()` 对一切失败重试满 3 次（含 TLS 配置类确定性错误），`retry_with_backoff`（全重试）语义精确吻合；换 classified 反而是行为变化。`RetryConfig { max_attempts: 3, initial_delay_ms: 1000, backoff_multiplier: 2.0, max_delay_ms: 4000 }` 复现现调度：sleep 1s、2s（cap 4s 永不触顶）。`RECONNECT_ATTEMPTS` 常量与错误文案（`"MQTT output reconnect failed after 3 attempts: …"`）保留；循环内的自定义 `warn!` 被共享实现日志取代——日志文案统一是骨架 change 既定取舍，不算错误契约。修正 188 行注释为真实序列。

### D5 — kafka `settle_tombstone` 走 pulsar 式保循环（D4 教训的镜像）

该循环的「sleep 前 `close.is_cancelled()` 检查」是 detached 任务的取消安全语义，整体 `retry_with_backoff` 包裹会丢失它（close 后多睡一轮）。保留 `for` 循环形态与自定义 `warn!`，仅把手写 `200*(attempt+1)` 换成共享 `advance_backoff`/`sleep_duration` + 常量 `RetryConfig { max_attempts: 4, initial_delay_ms: 200, backoff_multiplier: 2.0, max_delay_ms: 5000 }`——延迟序列 200/400/600/800 → 200/400/800/1600（**行为变化，spec 声明**）。**否决**整体包裹（丢取消检查）与新增配置字段（内部结算细节，配置面收益低——Non-goal）。

### D6 — kafka restore seek：只换延迟公式，不动 deadline 形态

该 loop 是「60s deadline 内等 assignment + seek 重试」混合体，attempts 无上限（受 deadline 约束），与 `max_attempts` 语义不匹配。仅把手写 `(200ms × attempt).min(1s)` 换成共享 `sleep_duration`（本地常量 `RetryConfig { initial_delay_ms: 200, max_delay_ms: 1000, backoff_multiplier: 2.0, .. }`）——序列 200/400/600/800/1000… → 200/400/800/1000（封顶提前一档，恢复路径低风险）。

### D7 — nats 清理：删死 sleep + Disconnection 收敛单发

删除 296/308 两处 sleep 及其失真注释；288-297 的批内 `while let` 逐条错误分支改为「发一次 `Delivery::Err(Disconnection)` 后 break」，对齐 spawned loop（346-356）既有形态。行为变化：错误批次更快结束、Disconnection 不重复报（引擎对重复 Disconnection 的处理是幂等重连，收敛是纯改善）。结构测试 `jetstream_fetch_loop_pins_a_bounded_expires`（735-763）只钉 `.expires(500ms)`，不触这些行。

### D8 — 卫生针按形态匹配，不做通用循环检测器

- **pow 针**：扫描前对每行做数字字面量下划线归一（`2_u64` → `2u64`），needle 从字面 `"2u64.pow"` 扩为正则 `\.(saturating_|checked_|wrapping_)?pow\(`；扫描范围 `output/` + `input/`（production section）。input/ 当前 pow 命中为零（kafka 两处是线性乘法），扩展是防回漂。
- **采用方针**：新增 `RETRY_ADOPTERS` 文件清单（本 change 后的共享实现采用方：qdrant/milvus/pulsar input/http/influxdb/mqtt/kafka input + AI 四处理器），断言每个文件 production section 含 `crate::retry` 引用——兑现 `retry.rs:22` 的注释承诺，注释同步指向真实测试名。
- **移除** `is_retryable_status` 的过时 `#[allow(dead_code)]` 与过时注释。
- **否决**通用「attempt+sleep 手写循环检测器」：output/kafka 的 flush ticker、input/kafka 的 assignment 轮询、窗口定时器都是合法 sleep，形态检测误报率不可接受；pow 族 + 采用方清单已覆盖本次全部真实违例。目录递归与 `production_section` 截断策略重写维持 Non-goal。

### D9 — 元数据与文档门禁随 change 交付

vector_search/milvus_search 的 `register_processor_metadata` schema JSON 补 `retry_count`（对齐 llm/embedding 的 "Defaults to 3; 0 disables" 措辞）→ `ARKFLOW_REGENERATE_DOCS=1` 重生成快照；influxdb 组件页（EN+zh）核对重试语义措辞；CHANGELOG 记录三处行为变化（influxdb/http 4xx 分类、kafka 结算曲线、nats 错误路径）。

## Risks / Trade-offs

- [4xx 不再重试改变 influxdb/http 失败路径耗时] → 正是修复目标（404 重试 3 次是纯浪费且推迟错误暴露）；CHANGELOG 与文档显式声明；既有 503 重试测试必须保绿。
- [kafka 两处曲线指数化，尾部落点后移（800→1600ms）] → 均为有界路径（结算 detached 任务 / 60s deadline），at-least-once 语义与取消安全不变；spec 场景声明。
- [机械替换 5 文件可能引入搬运笔误] → 收编保持错误文案、断言、日志逐字符不变（mqtt/kafka 保循环形态连日志一起保）；既有测试（influx 500 文案、http 503、mqtt 重连、kafka 结构测试）全绿为准。
- [卫生针扫 input/ 未来误伤] → 针只匹配 pow 族调用形态，不匹配裸 sleep；合法数学 pow（如有）可走 `// contract-allow: pow` 行标记豁免（沿用 EOF 针的标记先例）。
- [influxdb attempts 语义与全仓「重试次数」语义并存] → schema 文案已诚实，Non-goal 记录统一需独立决策；避免在本 change 里夹带破坏性语义迁移。

## Migration Plan

无数据/配置迁移。http output `retry_count` 必填→可选为纯放宽。三个行为变化（4xx 分类 ×2、kafka 曲线、nats 错误路径）进 CHANGELOG；文档 EN+zh 同步。回滚 = revert 单 PR。

## Open Questions

无。
