## MODIFIED Requirements

### Requirement: 状态 SHALL 有界并随 watermark 逐出(outer 侧发射)

每侧每 key 缓冲 SHALL 受 `max_per_key` 上限约束(超限先逐出最旧);当链级 watermark 推进到 `timestamp + window_ms + ttl_ms` 之后,该行 SHALL 被逐出(匹配窗口已闭合)。逐出时,`join_type` 标记为 outer 的侧中**从未匹配**的行 SHALL 作为未匹配行发射(对侧列全 null);已匹配过的行 SHALL NOT 再作为未匹配行发射;inner 模式或 inner 侧 SHALL 维持只丢弃。`max_per_key` 容量逐出在 outer 侧 SHALL 同样发射未匹配行——该发射无 watermark 保证(行仍可能随后匹配,产生「未匹配 + 匹配」双发),属 at-least-once 附属语义,SHALL 在组件文档声明。未匹配发射由链级 watermark 驱动:处理时间模式(无 watermark)下未匹配行 SHALL 不发射(活性限制,文档前置声明)。未匹配发射还需要对侧 schema 以构造 null 列:对侧尚未产生任何批次时,被逐出行 SHALL 暂存于每侧以 `max_per_key` 为界的待发队列(超限丢最旧并告警),待对侧 schema 已知后的下一个发射点补发;至作业关闭仍无法发射的暂存行 SHALL 不发射并丢弃。

容量逐出(区别于 watermark 闭合逐出)SHALL 可观测:inner 侧与 outer 侧的每次容量逐出 SHALL 产生 warn 级日志(至少含侧别、key、当前缓冲深度与 `max_per_key`,可节流但节流窗口内 SHALL 累计次数并在下一次日志中体现),MUST NOT 静默丢弃;watermark 闭合逐出属正常语义,无日志要求。

#### Scenario: watermark 闭合窗口

- **WHEN** 一行已缓冲且 watermark 越过其匹配上界
- **THEN** 该行被逐出,之后到达的对侧同 key 行不再与其匹配

#### Scenario: 每 key 容量上限

- **WHEN** 某侧某 key 的缓冲超过 `max_per_key`
- **THEN** 最旧的行被逐出,缓冲大小不超过上限

#### Scenario: left outer 未匹配行随淘汰发射

- **WHEN** `join_type = left_outer`,某左行从未匹配且 watermark 越过 `ts + window_ms + ttl_ms`
- **THEN** 该行以右侧列全 null 的形态发射一次,随后从缓冲移除

#### Scenario: 已匹配行不作为未匹配发射

- **WHEN** `join_type = full_outer`,某左行曾与右行匹配,随后 watermark 越过其匹配上界
- **THEN** 该行被逐出且不再发射(匹配对已在匹配时发射)

#### Scenario: 容量逐出在 outer 侧发射

- **WHEN** `join_type = left_outer` 且左侧某 key 超过 `max_per_key`,最旧左行被容量逐出
- **THEN** 该行以未匹配形态发射;若其后对侧同 key 行到达并落入窗口界内,匹配对照常发射(双发由 at-least-once 契约覆盖)

#### Scenario: inner 容量逐出可观测

- **WHEN** `join_type = inner` 某侧某 key 超过 `max_per_key`,最旧行被容量逐出
- **THEN** 该逐出产生带侧别/key/深度/上限字段的 warn 日志(持续倾斜下按节流窗口累计),不再静默丢弃
