## 1. 空闲输入排除（task.rs）

- [x] 1.1 `upstream_watermarks` 值改 `(i64, std::time::Instant)`，Watermark 信封到达时更新；提取纯函数 `effective_watermark(watermarks, ended, input_count, now, idle_timeout) -> Option<i64>`（排除超时空闲与已结束输入；非空闲活跃输入须全员已上报）
- [x] 1.2 `Envelope::Watermark` 臂改用该函数，转发值与上次转发值取 max（单调钳制），其余语义不变
- [x] 1.3 单测三场景：静默源超阈值不再冻结；空闲源恢复上报且低水位被钳制不回退；全员阈值内上报行为与旧语义一致

## 2. gate held 上限（event_time_gate.rs）

- [x] 2.1 `held` 累计行数上限常量（1,048,576）+ 结构内累计驱逐计数；Hold push 后超限则从最旧驱逐，逐批 `release_held` + `abort` ack，节流 warn（含本次与累计行数）
- [x] 2.2 `finish()`/释放路径对驱逐后状态保持一致；单测：构造超限 held 断言最旧被驱逐、ack abort、计数与 warn；未超限时释放路径不变（既有测试回归）

## 3. 文档（en + zh-Hans）

- [x] 3.1 事件时间文档页补：空闲输入排除（5 分钟阈值、恢复语义、单调性）与 held 上限驱逐（abort + 重放）语义

## 4. 验证

- [x] 4.1 `cargo test -p arkflow-core` 全绿
- [x] 4.2 `cargo test --workspace --all-targets` 全绿
- [x] 4.3 `cargo clippy --workspace --all-targets` 零新增告警
- [x] 4.4 `pnpm docs:check` 通过
- [x] 4.5 `openspec validate fix-kernel-watermark-stall-bounds` 通过

## 5. CR 修复（2026-09-29 第一轮）

- [x] 5.1 驱逐告警加 10 秒节流（`WARN_INTERVAL`）：持续贴着上限运行的管线原先每个驱逐事件一条 warn（可能每秒上千条），违背 spec 的"节流"要求；累计计数保持精确，仅日志限频
- [x] 5.2 机械核对 held_rows 计数器 4 个 take 点全部清零（push/清零闭环无遗漏）；CR 顺手消掉 1 条 clippy 风格告警

## 6. CR 修复（2026-09-29 第二轮，接手会话）

- [x] 6.1 B5：`held` 改 `VecDeque`（`pop_front` O(1)），消除贴限病态路径上每次驱逐 ~1M 条目的 O(n) memmove
- [x] 6.2 B1：驱逐 warn 文案如实声明语义（abort 波及整个 fan-out 投递、兄弟确认可能失败重启、重放行按 late 策略处置）；新增 `fanout_abort_poisons_sibling_acknowledgements` 钉住 ricochet 行为（input/mod.rs）
- [x] 6.3 6.2 的补强：驱逐测试升级为带可观察 ack 的端到端断言（最旧投递被 abort 且从不确认、幸存投递保持挂起、水位推进后释放的是幸存者数据）——修正 2.2 的过度声明（原先只断言计数器）
- [x] 6.4 B2：spec 场景/docs en/zh/design 的"重启后经 WAL 重放"改为如实表述（重放行落后于已恢复水位、按 late 策略处置，Drop 即丢；abort 波及整投递）；spec 增补"驱逐毒化同组兄弟确认"场景
- [x] 6.5 测试缺口：`clamp_forwarded_watermark` 提为具名函数 + 单测（初版零覆盖的核心承诺）；`default_timeout_unfreezes_a_silent_input_only_past_the_threshold` 用真实默认常量钉住 5 分钟阈值的两侧语义
- [x] 6.6 工件一致性：design 决策 6 改为与实现一致（10s 节流已引入）、新增决策 7（VecDeque）、决策 3 补 B1/B2 修正；`.openspec.yaml` 日期修正
- [x] 6.7 CR 二轮：修复 6.4 的静默失败（spec 替换锚不匹配未生效——补"驱逐毒化同组兄弟确认"场景与 B2 如实表述，本次以 Edit 精确落地并验证）；`take_held_acknowledgements` 一并排空 `pending_eviction_acks`（离 runtime 嵌入者的 pending abort 不再滞留卡 barrier）；修正驱逐测试的误导注释（held 行不重灌 tracker）；warn 措辞补"同投递内排队兄弟可能立即失败"
- [x] 6.8 CR 三轮（CodeRabbit 三 Major）：①驱逐 spawn 结算加 outstanding 上界（默认 1024，测试可注入），超界观测显式 fail-closed——持续溢出+慢结算不再无界累积游离任务与滞留 ack；②spawn 结算失败时 ack 回推共享重试队列（abort_held/finish/take 三路排空），不再"警告后丢弃"；③finish 成功路径排空离 runtime 队列的驱逐 ack。三个行为各有测试钉住（fail-closed / 失败保留+重试浮出 / 离 runtime finish 结算）
