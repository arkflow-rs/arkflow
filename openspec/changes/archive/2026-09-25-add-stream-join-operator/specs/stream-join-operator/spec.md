# stream-join-operator Specification

## Purpose

统一执行内核的 keyed interval join 算子：双输入侧别、相等键匹配、事件时间窗口界、有界状态与重放恢复语义。

## Requirements

### Requirement: Join 算子 SHALL 以声明的生产者区分两侧

Join 算子所在链 SHALL 恰好持有两条入边。侧别 SHALL 由 `left_from`/`right_from`（上游算子 id）在图构建期解析为通道索引——通道顺序是内核内部细节，用户以生产者身份声明侧别而非位置；省略时回退 0/1。链路循环 SHALL 为含 join 算子的链的每个数据批次追加 `__meta_input_index`（UInt32）列；join 算子 SHALL 依据该列路由批次到对应侧缓冲，缺失该列或索引既非左也非右即失败。

#### Scenario: 两侧批次正确路由

- **WHEN** 左源批次（input 0）与右源批次（input 1）先后到达 join 链
- **THEN** 两个批次分别进入左右缓冲，且右侧到达时对已缓冲的左侧同 key 行完成匹配

#### Scenario: 缺失输入标记即失败

- **WHEN** 一个不含 `__meta_input_index` 的批次被送入 join 算子
- **THEN** 处理以配置错误失败，指明需要内核输入打标

### Requirement: 匹配语义 SHALL 为相等键 + 事件时间窗口界

一行左侧与一行右侧 SHALL 在两侧 join key 相等且事件时间差绝对值 ≤ `window_ms` 时匹配。匹配对 SHALL 即时发射（inner join）；每侧每 key 的全部候选都会参与匹配（扇出）。事件时间列缺省回退 `__meta_timestamp`（纳秒归一化为毫秒）。

#### Scenario: 窗口内匹配扇出

- **WHEN** 左侧同 key 两行的时间戳均落在右行的 `window_ms` 界内
- **THEN** join 输出两行匹配对

#### Scenario: 窗口外不匹配

- **WHEN** 两侧时间差超过 `window_ms`
- **THEN** 不产生输出行

### Requirement: 状态 SHALL 有界并随 watermark 逐出

每侧每 key 缓冲 SHALL 受 `max_per_key` 上限约束（超限先逐出最旧）；当链级 watermark 推进到 `timestamp + window_ms + ttl_ms` 之后，该行 SHALL 被逐出（匹配窗口已闭合）。

#### Scenario: watermark 闭合窗口

- **WHEN** 一行已缓冲且 watermark 越过其匹配上界
- **THEN** 该行被逐出，之后到达的对侧同 key 行不再与其匹配

#### Scenario: 每 key 容量上限

- **WHEN** 某侧某 key 的缓冲超过 `max_per_key`
- **THEN** 最旧的行被逐出，缓冲大小不超过上限

### Requirement: 输出 SHALL 为双侧前缀宽表

join 输出 SHALL 由左侧原始列（前缀 `l_`）、右侧原始列（前缀 `r_`）与 `join_key` 组成；每侧 schema SHALL 在流内保持稳定，变更即以配置错误失败。

#### Scenario: 输出列命名

- **WHEN** 一对 key 为 `a` 的左右行匹配
- **THEN** 输出行包含 `l_*`、`r_*` 与 `join_key = a` 列

### Requirement: 状态 SHALL 由 checkpoint 重放重建

join 算子 SHALL NOT 维护独立状态快照；恢复时源回退到已确认 checkpoint cut，两侧缓冲由输入重放确定性重建（at-least-once 语义，下游需容忍重复）。

#### Scenario: 恢复后缓冲重建

- **WHEN** 含 join 的作业从 checkpoint 恢复
- **THEN** 重放窗口内的输入重建两侧缓冲，窗口界内的匹配继续产生

### Requirement: Join DAG 校验 SHALL 检查元数与配置

Job DAG 中的 Join 算子 SHALL 要求恰好两条入边（多不可路由、少则一侧饥饿），其配置 SHALL 通过 `JoinOperatorConfig::validate`（非空 key、非负 window/ttl、max_per_key ≥ 1；声明的 `left_from`/`right_from` 必须真实喂入该 join）。含 join 的链 SHALL 以单线程执行（`processor_parallelism = 1`），避免池并发交错跨 checkpoint 切口更新缓冲。

#### Scenario: 入边元数校验

- **WHEN** Join 算子的入边数不等于 2
- **THEN** 校验以指明元数的错误失败

#### Scenario: 配置校验

- **WHEN** Join 配置缺少 key 或 window_ms 为负
- **THEN** 校验失败并指明字段
