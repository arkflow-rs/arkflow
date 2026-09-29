# stream-join-operator Specification

## Purpose

统一执行内核的 keyed interval join 算子：双输入侧别、相等键匹配、事件时间窗口界、有界状态与重放恢复语义。

## Requirements

### Requirement: Join 算子 SHALL 以声明的生产者区分两侧

Join 算子所在链 SHALL 恰好持有两条入边。侧别 SHALL 由 `left_from`/`right_from`(上游算子 id)在图构建期解析为通道索引——通道顺序是内核内部细节,用户以生产者身份声明侧别而非位置;省略时回退 0/1。每个声明的生产者 SHALL 恰好贡献一个通道(即该上游算子喂入本链的子任务数为 1);上游并行度 > 1 的侧 SHALL 在图构建期以指明「将上游算子 parallelism 设为 1」的错误拒绝,而非运行期失败。链路循环 SHALL 为含 join 算子的链的每个数据批次追加 `__meta_input_index`(UInt32)列;join 算子 SHALL 依据该列路由批次到对应侧缓冲,缺失该列或索引既非左也非右即失败(运行期校验保留为纵深防御)。

#### Scenario: 两侧批次正确路由

- **WHEN** 左源批次(input 0)与右源批次(input 1)先后到达 join 链
- **THEN** 两个批次分别进入左右缓冲,且右侧到达时对已缓冲的左侧同 key 行完成匹配

#### Scenario: 缺失输入标记即失败

- **WHEN** 一个不含 `__meta_input_index` 的批次被送入 join 算子
- **THEN** 处理以配置错误失败,指明需要内核输入打标

#### Scenario: 多子任务侧在构建期拒绝

- **WHEN** 某侧声明的上游算子以并行度 > 1 喂入该 join
- **THEN** 图构建以指明该侧需单子任务(parallelism = 1)的错误失败,作业不启动

### Requirement: 匹配语义 SHALL 为相等键 + 事件时间窗口界

一行左侧与一行右侧 SHALL 在两侧 join key 相等且事件时间差绝对值 ≤ `window_ms` 时匹配。匹配对 SHALL 即时发射——该行为与 `join_type` 无关。`join_type` SHALL 取值 `inner`(缺省)| `left_outer` | `right_outer` | `full_outer`;未声明时 SHALL 缺省 `inner` 且行为与既有 inner 语义逐位一致。事件时间列缺省回退 `__meta_timestamp`(纳秒归一化为毫秒)。

#### Scenario: 窗口内匹配扇出

- **WHEN** 左侧同 key 两行的时间戳均落在右行的 `window_ms` 界内
- **THEN** join 输出两行匹配对

#### Scenario: 窗口外不匹配

- **WHEN** 两侧时间差超过 `window_ms`
- **THEN** 不产生输出行

#### Scenario: join_type 缺省向后兼容

- **WHEN** 既有配置未声明 `join_type`
- **THEN** 算子以 inner 语义运行,匹配发射与淘汰丢弃行为与变更前逐位一致

### Requirement: 状态 SHALL 有界并随 watermark 逐出(outer 侧发射)

每侧每 key 缓冲 SHALL 受 `max_per_key` 上限约束(超限先逐出最旧);当链级 watermark 推进到 `timestamp + window_ms + ttl_ms` 之后,该行 SHALL 被逐出(匹配窗口已闭合)。逐出时,`join_type` 标记为 outer 的侧中**从未匹配**的行 SHALL 作为未匹配行发射(对侧列全 null);已匹配过的行 SHALL NOT 再作为未匹配行发射;inner 模式或 inner 侧 SHALL 维持只丢弃。`max_per_key` 容量逐出在 outer 侧 SHALL 同样发射未匹配行——该发射无 watermark 保证(行仍可能随后匹配,产生「未匹配 + 匹配」双发),属 at-least-once 附属语义,SHALL 在组件文档声明。未匹配发射由链级 watermark 驱动:处理时间模式(无 watermark)下未匹配行 SHALL 不发射(活性限制,文档前置声明)。未匹配发射还需要对侧 schema 以构造 null 列:对侧尚未产生任何批次时,被逐出行 SHALL 暂存于每侧以 `max_per_key` 为界的待发队列(超限丢最旧并告警),待对侧 schema 已知后的下一个发射点补发;至作业关闭仍无法发射的暂存行 SHALL 不发射并丢弃。

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

### Requirement: 输出 SHALL 为双侧前缀宽表(outer 侧可空)

join 输出 SHALL 由左侧原始列(前缀 `l_`)、右侧原始列(前缀 `r_`)与 `join_key` 组成;每侧 schema SHALL 在流内保持稳定,变更即以配置错误失败。outer 模式下,可能在未匹配发射中全为 null 的侧字段 SHALL 在输出 schema 中强制 `nullable = true`(`left_outer` 为 `r_*` 侧、`right_outer` 为 `l_*` 侧、`full_outer` 两侧);未匹配行的对侧列 SHALL 全为 null;inner 模式输出 schema SHALL 保持既有形态(不引入 nullable 漂移)。

#### Scenario: 输出列命名

- **WHEN** 一对 key 为 `a` 的左右行匹配
- **THEN** 输出行包含 `l_*`、`r_*` 与 `join_key = a` 列

#### Scenario: left outer 未匹配行的 null 装配

- **WHEN** `join_type = left_outer` 的一行未匹配左行被发射
- **THEN** 输出行包含 `l_*` 原值列、全 null 的 `r_*` 列与 `join_key`,且 `r_*` 字段在 schema 中声明为 nullable

### Requirement: 状态 SHALL 由 checkpoint 重放重建

join 算子 SHALL NOT 维护独立状态快照;恢复时源回退到已确认 checkpoint cut,两侧缓冲与行的已匹配标记由输入重放确定性重建(at-least-once 语义,下游需容忍重复)。未匹配发射的正确性 SHALL 由链级 watermark 的逐边最小值聚合保证:行被淘汰时对侧 watermark 必已越过匹配界,因此重放重建不会产生「假未匹配」(已发射未匹配后又出现本应匹配的对侧行)——迟到超过 `window + ttl` 的行除外,该情形属既有迟到契约范围。

#### Scenario: 恢复后缓冲重建

- **WHEN** 含 join 的作业从 checkpoint 恢复
- **THEN** 重放窗口内的输入重建两侧缓冲,窗口界内的匹配继续产生

#### Scenario: outer 重放的未匹配确定性

- **WHEN** 含 `join_type = left_outer` 的作业崩溃后从 checkpoint cut 重放,重放期间某左行的匹配右行在 cut 之后
- **THEN** 重放重演与首次执行相同的匹配与淘汰顺序,已匹配的左行不会被作为未匹配行发射

### Requirement: Join DAG 校验 SHALL 检查元数与配置

Job DAG 中的 Join 算子 SHALL 要求恰好两条入边(多不可路由、少则一侧饥饿),其配置 SHALL 通过 `JoinOperatorConfig::validate`(非空 key、非负 window/ttl、max_per_key ≥ 1;声明的 `left_from`/`right_from` 必须真实喂入该 join;`join_type` 必须为四个合法值之一)。含 join 的链 SHALL 以单线程执行(`processor_parallelism = 1`),避免池并发交错跨 checkpoint 切口更新缓冲。

#### Scenario: 入边元数校验

- **WHEN** Join 算子的入边数不等于 2
- **THEN** 校验以指明元数的错误失败

#### Scenario: 配置校验

- **WHEN** Join 配置缺少 key 或 window_ms 为负
- **THEN** 校验失败并指明字段

#### Scenario: join_type 非法值校验

- **WHEN** `join_type` 声明为四个合法枚举值之外的值
- **THEN** 配置反序列化或校验以指明合法取值的错误失败
