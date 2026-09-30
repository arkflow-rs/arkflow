## Context

三个无界位点共享一个根因：task.rs 的 watermark 聚合要求全员上报，静默输入冻结一切下游推进。join by_key 已有每键容量驱逐（前序修复），window 缓冲总量上界需 fire 路径重构——本变更修根因（空闲排除）+ 最直接的爆炸面（gate held），其余见 Non-goals。

## Goals / Non-Goals

**Goals:** 空闲输入排除（含单调钳制与恢复语义）；gate held 行数上限（abort 驱逐 + 节流告警）；聚合决策提为可单测纯函数。

**Non-Goals:** 配置面、window/join 总量上界、字节级上限、checkpoint 超时批（见 proposal）。

## Decisions

1. **空闲排除用"最近到达时刻"而非"数据到达"判定。** watermark 是聚合的门控信号；一个持续发数据但从不发 watermark 的源（处理时间源）正是要排除的对象——按数据到达豁免会把这类源永远留在门内，等于没修。到达时刻随 watermark 信封更新，纯本地状态。
2. **转发单调钳制（max）。** 空闲源恢复时其水位大概率落后于已推进的聚合值；watermark 回退会破坏下游单调假设（窗口重复触发等）。钳制后该源的迟到行自然落入 late 策略——数据有出口，语义可解释。
3. **驱逐 = abort 而非 late 路由或静默丢弃。** gate 的 held ack 处于 `mark_held` 挂起态；abort 是引擎取消路径的标准结算（不确认、WAL 游标不推进），保持 at-least-once。备选"强制按 late 路由"需要在 gate 内重放决策逻辑且 late 策略为 Drop 时同样是丢——复杂度高收益低；备选"静默丢"违反可观测性。【CR 修正（B1/B2）：abort 波及整个 fan-out 组——组内仍被下游持有的兄弟确认在停顿解除时会失败并重启任务（rico­chet），且重放行落后于已恢复水位、按 late 策略处置（Drop 即丢）。detach 式新结算原语要么让父 ack 越过未处理行推进游标（静默丢数据）要么永挂（barrier 卡死）——故显式接受 ricochet，以 `fanout_abort_poisons_sibling_acknowledgements` 测试钉住，warn/spec/docs 如实声明。】
4. **上限与阈值用内核常量（5min / 1M 行）。** 常量选择：5min 远大于正常 watermark 间隔（秒级），假排除风险小；1M 行 × 行均几百字节 ≈ 数百 MB 量级的兜底界，正常运行远达不到。配置化涉及 job/stream 配置管线，另行立项。
5. **聚合决策提为自由函数 `effective_watermark(...)`。** `handle_envelope` 的 Watermark 臂逻辑复杂、难以单测；纯函数（输入：watermarks+到达时刻、ended、输入数、now、阈值 → Option<i64>）可对三种场景各写确定性单测，`handle_envelope` 只做状态维护与转发。
6. **节流 warn：每驱逐事件计数 + 10s 时间节流。**【CR 修正：初版设计写"不引入时间节流器"，实现引入了 10s 节流（`WARN_INTERVAL`）以防空转贴限路径刷屏——以实现为准。】驱逐只在冻结+超限的病态路径发生；每次事件记累计行数与后果语义。
7. **【CR 增补】持有队列用 `VecDeque`。** 初版 `Vec::remove(0)` 在贴限路径每次驱逐 O(n) memmove（~1M 条目），恰好塌方在本特性驯服的病态场景——改为 `VecDeque::pop_front` O(1)。

## Risks / Trade-offs

- [慢但活跃的源（间隔 >5min 的合法 watermark）被误排除 → 聚合提前推进 → 其后续行被判 late] → 阈值取远超正常间隔的 5min；late 策略可配路由兜底；文档明示；配置化列为后续。
- [abort 驱逐导致重启重放、可能重复处理] → 与引擎取消语义一致（at-least-once），优于无界增长；节流 warn 让运维可见。
- [空闲排除改变既有"慢输入约束 min"的语义边界] → 未超阈值时逐位不变（单测钉住）；超过阈值的场景此前是冻结（更坏），新语义是显式权衡。
- [`BTreeMap<usize, i64>` → `(i64, Instant)` 波及 `handle_envelope` 多个调用点] → 纯机械类型改写，编译器兜底。

## Migration Plan

内核行为变更，无配置面。回滚即 revert。验证：新增单测 + workspace 全量（既有 watermark/gate 测试群回归）。

## Open Questions

无。
