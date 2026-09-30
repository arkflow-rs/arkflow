# event-time-processing Specification

## Purpose
TBD - created by archiving change add-distributed-stateful-streaming-runtime. Update Purpose after archive.
## Requirements
### Requirement: Sources SHALL declare time semantics

Each event-time Job source SHALL declare an event timestamp expression or explicitly select processing time, together with a watermark strategy when event time is used. A nullable or invalid timestamp routed by policy SHALL carry a marker distinct from a late-event marker.

#### Scenario: Validate an event-time source

- **WHEN** a Job selects event-time processing without a valid timestamp expression or watermark strategy
- **THEN** validation fails before deployment with the source and missing configuration identified

#### Scenario: Route an invalid timestamp

- **WHEN** a nullable timestamp cannot be converted to an event timestamp and the invalid-event policy routes the row
- **THEN** the routed row contains a dedicated invalid-timestamp marker and is not labeled only as a late event

### Requirement: Watermarks SHALL reflect partition progress
The runtime SHALL track watermark progress by the complete physical input identity (topic, when present, and partition), SHALL seed all known assigned partitions before data arrives, SHALL exclude only partitions that are configured idle, and SHALL advance an operator watermark from the minimum active upstream progress. Event-time gates that feed the same downstream window SHALL use the same downstream watermark frontier. Session windows SHALL use dynamic per-key boundaries owned by the window operator, and processing-time windows SHALL not be event-time gated.

#### Scenario: One partition becomes idle
- **WHEN** an input partition is marked idle according to the configured policy
- **THEN** it does not permanently hold back the operator watermark, and the observation identifies the topic and partition that became idle

#### Scenario: An unobserved assigned partition is slow
- **WHEN** one assigned partition has not emitted a row while another partition advances
- **THEN** the unobserved partition remains active until the idle timeout and the operator watermark does not advance past its progress

#### Scenario: Topics reuse a partition number
- **WHEN** topic A partition 0 and topic B partition 0 report different progress
- **THEN** they maintain independent watermark entries and neither can advance the other topic's windows

#### Scenario: Multiple source edges feed one window
- **WHEN** two event-time source edges feed the same downstream window
- **THEN** late classification and window closure use their shared slowest active frontier rather than a source-local watermark

#### Scenario: Processing-time window receives event-time input
- **WHEN** an event-time source feeds a window whose trigger is processing time
- **THEN** the source event-time gate forwards the row immediately and the processing-time trigger remains responsible for emission

#### Scenario: A slower partition is first observed
- **WHEN** partition 0 reports watermark 2,000 and partition 1 is then first observed at 1,000
- **THEN** the active watermark reflects the minimum active progress and does not remain at 2,000 solely because that value was previously observed

#### Scenario: One connector task multiplexes partitions
- **WHEN** a single source task consumes physical partitions 0 and 1 and their deliveries carry those partition identities
- **THEN** event-time progress for partition 0 cannot advance or close windows on behalf of partition 1

#### Scenario: Restore a non-zero partition watermark
- **WHEN** a source subtask assigned to physical partition 3 restores a checkpointed watermark
- **THEN** the watermark is installed for partition 3 and no synthetic partition 0 participates in the calculation

### Requirement: Windows SHALL define lateness behavior
Event-time windows SHALL define closure, allowed lateness, late-event handling, and emitted result behavior. A late Update within the allowed-lateness deadline SHALL modify the already emitted window result rather than create an unrelated partial window. For sliding windows, each row SHALL be classified against every containing window membership rather than a single latest window end. Runtime metrics SHALL count each late or invalid row regardless of whether the policy drops, routes, or updates it. Session windows SHALL retain and merge dynamic per-key session boundaries; a late row that bridges an emitted session SHALL produce an update rather than a new initial result. Expired-window cleanup SHALL NOT reclaim an emitted buffer that still owes an unemitted correction, including at end-of-stream; the correction SHALL be emitted before the buffer's state is reclaimed.

#### Scenario: A late event arrives within allowed lateness
- **WHEN** an event arrives after the window watermark but before the allowed-lateness deadline
- **THEN** the runtime updates or emits the window result according to the Job policy, including the dynamic session membership for session windows

#### Scenario: A late event exceeds allowed lateness
- **WHEN** an event arrives after the allowed-lateness deadline
- **THEN** the runtime routes or drops it according to the configured late-event policy and records the outcome

#### Scenario: Update targets an already emitted window
- **WHEN** a late event marked for Update arrives before the window's allowed-lateness deadline
- **THEN** the runtime reopens the retained closed aggregate, emits a complete corrected result with an update marker, and acknowledges the input only after that result is written

#### Scenario: A sliding row belongs to multiple windows
- **WHEN** a row belongs to sliding windows ending at 5 and 9 and the first membership has already expired
- **THEN** the first membership is classified as late independently and is not reintroduced as a new on-time aggregate merely because the later membership remains open

#### Scenario: Dropped rows are counted
- **WHEN** a batch contains multiple late rows and the policy drops them, including rows with invalid timestamps
- **THEN** the late-event metric increases by the number of affected rows, not by one per action group

#### Scenario: End-of-stream emits pending corrections before reclaim

- **WHEN** a session window's emitted buffer was merged or extended by a late update and the source reaches end-of-stream before the watermark advances past the merged end
- **THEN** the pending correction is emitted before the expired-buffer cleanup reclaims the buffer, and neither the corrected rows nor the new rows are lost

### Requirement: Event timestamps SHALL accept supported Arrow units
An event-time source SHALL accept Int64 timestamps and Arrow timestamp columns in seconds, milliseconds, microseconds, or nanoseconds, normalize them to checked millisecond values, and reject overflow or unsupported types before processing.

#### Scenario: Normalize an Arrow timestamp column
- **WHEN** the configured timestamp field is a nullable `TimestampSecond`, `TimestampMillisecond`, `TimestampMicrosecond`, or `TimestampNanosecond` column
- **THEN** the gate converts each non-null value to milliseconds, preserves null information, and applies the same window-boundary rules to the normalized value

#### Scenario: Timestamp conversion overflows
- **WHEN** a timestamp value cannot be represented in milliseconds
- **THEN** event-time processing fails with an actionable field/type error and does not silently wrap the value

### Requirement: Event-time gates SHALL classify current rows consistently
The gate SHALL evaluate current rows against a watermark that is consistent with the batch's recorded progress. Rows that become late because the batch advances the watermark SHALL receive the configured Drop, Route, or Update action, while future rows MAY remain held. At end-of-stream the gate SHALL classify each held row per window membership against that window's real lateness deadline derived from the last observed watermark; a sentinel end-of-stream watermark SHALL NOT by itself exclude memberships that are still within their deadline.

#### Scenario: A batch contains a future row and an old row
- **WHEN** a first batch contains event times `[2100, 100]` for a `[0,1000)` window and the batch advances the watermark beyond that window
- **THEN** the old row is classified by the configured late-event policy and only the future row remains held for a later window

#### Scenario: End-of-stream does not truncate live sliding memberships
- **WHEN** the source ends while a held sliding row has one membership past its deadline and one membership still within its deadline
- **THEN** the still-live membership receives the row under the same Update/Drop/Route policy as mid-stream, and the emitted aggregates are not permanently truncated by the end-of-stream classification

### Requirement: Invalid event timestamps SHALL not be held indefinitely
A null or otherwise invalid event timestamp SHALL NOT be retained as an ordinary held event because it cannot produce a window end. The runtime SHALL route it to the configured invalid/late side output when one exists, otherwise drop and acknowledge it.

#### Scenario: Null timestamp with a route configured
- **WHEN** a nullable timestamp column contains a null row and the Job has a late-event route
- **THEN** the row is sent to that side output with an invalid-timestamp marker and its acknowledgement is completed after the route write

#### Scenario: Null timestamp without a route
- **WHEN** a nullable timestamp column contains a null row and no side output is configured
- **THEN** the row is dropped, the invalid/late metric is incremented, and its acknowledgement is completed without retaining it in the gate

### Requirement: Sliding windows SHALL include every containing window
For a sliding window, the runtime SHALL enumerate every valid window start whose interval contains the event timestamp, including when the window size is not divisible by the slide.

#### Scenario: Non-divisible sliding window
- **WHEN** `size=5`, `slide=2`, and an event has timestamp 4
- **THEN** the event contributes to windows starting at 4, 2, and 0, and no containing start is omitted because of integer division truncation

### Requirement: Sliding-window exclusions accumulate across releases

When a held row belongs to multiple sliding windows that close on different watermark advances, the runtime SHALL merge newly closed memberships into the existing exclusion metadata. Re-evaluating a held row SHALL NOT discard exclusions already recorded.

#### Scenario: A held row sees two successive window closures

- **WHEN** a held row is first marked for an ending-5 window and later the ending-9 window also closes before release
- **THEN** the final row carries both exclusions and is not reintroduced into either already-emitted window

### Requirement: Window state and source acknowledgement share rollback semantics

For a fired window backed by staged state, state finalization and source/WAL acknowledgement SHALL form one retryable processing unit. If any source acknowledgement fails, the runtime SHALL conditionally compensate or retain the state transaction so replay cannot double-count or lose the window update. Every window operator construction path SHALL register its fired-buffer writes with that retryable unit: a path that writes the backend before the output acknowledgement SHALL roll back the backend state, not only its in-memory buffers, when the acknowledgement fails.

#### Scenario: Replay after a failed fired-window acknowledgement

- **WHEN** a fired window is emitted successfully but its source acknowledgement fails before the durable cut completes
- **THEN** replaying the source row restores the pre-commit state or resumes the same staged transaction rather than applying a second aggregate

#### Scenario: A direct-construction path rolls back its backend writes

- **WHEN** a window operator built without the state journal emits a fired buffer, its acknowledgement fails, and the process replays the source rows before the next emission
- **THEN** the replay does not double-count into the emitted aggregate because the backend's emitted state was rolled back together with the in-memory buffers

### Requirement: Event-time compatibility markers remain row-local

Sliding-window late-membership metadata SHALL be attached and merged per input row. A row whose open membership remains eligible SHALL still be processed for that membership even when another containing membership has already closed.

#### Scenario: Mixed open and closed memberships

- **WHEN** a late row belongs to one closed sliding window and one still-open sliding window
- **THEN** the closed membership is excluded or routed according to policy while the open membership receives the row exactly once

### Requirement: Timestamped rows with a NULL key SHALL follow an explicit policy

A row with a convertible event timestamp but a NULL key SHALL NOT be silently skipped by keyed window aggregation. The runtime SHALL count it in the invalid/late metrics, SHALL route it to the late/invalid side output when one is configured, and SHALL otherwise drop it and complete its acknowledgement — mirroring the invalid-timestamp policy. The row's delivery SHALL NOT be acknowledged while it is silently unaccounted.

#### Scenario: NULL key with a side output configured

- **WHEN** a windowed Job receives a row whose timestamp converts but whose key column is NULL and a late/invalid side output is configured
- **THEN** the row is routed to that side output with an invalid marker, counted in the metrics, and acknowledged only after the route write

#### Scenario: NULL key without a side output

- **WHEN** a windowed Job receives a row whose key column is NULL and no side output is configured
- **THEN** the row is dropped, the invalid/late metric is incremented, and the acknowledgement completes without the row being counted into any aggregate

### Requirement: 多输入 watermark 聚合 SHALL 排除空闲输入

链内 watermark 聚合 SHALL 记录每个输入最近一次 watermark 到达时刻：活跃输入中超过空闲阈值（默认 5 分钟）未上报者，SHALL 被排除出"全员已上报"门与最小值计算（与已结束输入同待遇），使聚合 watermark 不因单个静默源而冻结。被排除输入后续再上报 watermark 时 SHALL 重新加入计算；转发给下游的 watermark SHALL 与上次转发值取最大以保持单调。空闲输入重新加入后因其水位较低而被钳制的行，SHALL 按既有 late 策略处理。

#### Scenario: 静默源不再冻结聚合

- **WHEN** 多输入链的一个输入在超过空闲阈值的时间里未产生任何 watermark，其余输入持续上报
- **THEN** 聚合 watermark 基于其余输入推进，下游窗口触发与状态清理不再冻结

#### Scenario: 空闲源恢复上报

- **WHEN** 被排除的输入恢复上报且其 watermark 低于当前已转发值
- **THEN** 转发值保持单调不回退；该输入重新参与后续聚合，其间迟到行按 late 策略处置

#### Scenario: 未超阈值前语义不变

- **WHEN** 所有活跃输入都在阈值内上报过 watermark
- **THEN** 聚合行为与既有语义逐位一致（全员门 + 最小值）

### Requirement: event-time gate 的持有量 SHALL 有界

event-time gate 因等待 watermark 而持有的行 SHALL 有累计行数上限（默认 1,048,576）：超限时 SHALL 从最旧的持有批开始驱逐并以其 ack 的 abort 结算，节流日志 SHALL 记录累计驱逐规模与后果语义。abort 结算 SHALL 波及该投递的整个 fan-out 组——组内仍被下游持有的兄弟确认在停顿解除（或其在组内排队结算）时 SHALL 失败并按 at-least-once 重启任务（驱逐是防无界增长的最后兜底，接受该 ricochet）；驱逐后重启重放的行 SHALL 落后于已恢复水位并按既有 late 策略处置（Drop 策略即丢弃）。上限未触达时持有语义不变（watermark 推进即释放）。

#### Scenario: 超限驱逐最旧持有

- **WHEN** `held` 累计行数超过上限且 watermark 仍未推进
- **THEN** 最旧的持有批被移除且其 ack 被 abort（从不确认），内存占用回落到上限内，节流 warn 记录累计驱逐行数与 abort 的波及语义

#### Scenario: 驱逐毒化同组兄弟确认

- **WHEN** 被驱逐批与仍在下游持有的兄弟切片同属一个 fan-out 投递，停顿解除后兄弟尝试确认
- **THEN** 兄弟确认失败（fan-out 已 abort）、父确认不发生，投递整体回滚——该失败机制有测试钉住；任务级重启后果由引擎既有失败路径承担（at-least-once）

#### Scenario: 未超限时无驱逐

- **WHEN** 持有行数在上限内且 watermark 推进
- **THEN** 既有释放路径逐位不变，不发生驱逐

