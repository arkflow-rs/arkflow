## Why

v1.0 就绪度审查 P2 内核批（`openspec/CODE_REVIEW_2026-09-29.md`「三处 watermark 驱动的无界缓冲」）。调研后聚焦其共同根因与最直接的爆炸面：

- **watermark 聚合门冻结（根因放大器）**：链内多输入的 watermark 聚合要求**每个**活跃输入都上报过 watermark 才转发最小值（`crates/arkflow-core/src/executor/task.rs` 的 `Envelope::Watermark` 臂，`upstream_watermarks.contains_key` 全员门）。混入一个从不发 watermark 的源（处理时间源、静默分区）→ 聚合 watermark 永不推进 → 下游一切 watermark 驱动的释放/触发/清理全部冻结。
- **event-time gate `held` 无界（`event_time_gate.rs:79-84`，push 于 `:732`）**：窗口未开的行持续压入 `held`，只在 watermark 推进时释放。上述冻结发生时 `held` 随输入速率无界增长——静默内存炸弹。每个 held 批的 ack 处于 `mark_held` 挂起态，泄漏面同时包含消息数据。
- window 聚合缓冲与 join by_key 的总量上界问题与本根因同源（watermark 冻结时清理不触发；join 已有每键容量驱逐，剩余风险是 distinct key 基数），本变更不展开（见 Non-goals）。

## What Changes

- **空闲输入排除**（task.rs 聚合门）：记录每个输入最近一次 watermark 到达时刻；超过空闲阈值（默认 5 分钟，内核常量）仍未上报的活跃输入从"全员已上报"门与最小值计算中排除（与已结束输入同待遇）。空闲输入后续再上报时重新加入；转发值与上次转发值取 max 保持单调。把聚合决策提取为纯函数并单测。
- **gate held 行数上限**（event_time_gate.rs）：`held` 累计行数超过上限（默认 1M 行，内核常量）时，从最旧开始驱逐——`abort` 其 ack（对齐引擎取消语义：数据不确认、重启后经 WAL 重放）并以节流 warn 记录累计驱逐行数。
- 文档：事件时间相关文档页（en/zh）补"空闲输入排除"与"held 上限驱逐"语义。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `event-time-processing`: 新增两个需求——多输入 watermark 聚合的空闲输入排除（含单调性），event-time gate 持有量的有界驱逐（abort + 重放语义）。

## Impact

- `crates/arkflow-core/src/executor/task.rs`：watermark 聚合改为带到达时刻的纯函数决策（`BTreeMap<usize, i64>` → `(i64, Instant)`），转发单调钳制。
- `crates/arkflow-core/src/executor/event_time_gate.rs`：held 行数上限 + 最旧驱逐 + 节流 warn。
- 行为变化：静默输入 5 分钟后不再冻结聚合（其后续迟到数据走既有 late 策略）；held 超 1M 行时最旧行被 abort（重启重放）而非无限持有。
- 既有 watermark 语义（慢输入约束 min、结束输入豁免）不变。

## Non-goals

- 不给空闲阈值/上限做配置面（内核常量 + 文档；配置化涉及 job/stream 配置管线扩展，另行立项）。
- 不做 window 聚合缓冲与 join by_key 的总量上界（需要 fire 路径重构；空闲排除已消除其冻结主因，总量上界独立立项）。
- 不改 watermark 生成/对齐/ barrier 交互；不做字节级（只按行数）上限。
- 不处理 checkpoint round 超时批（批次 D 另一项）。
