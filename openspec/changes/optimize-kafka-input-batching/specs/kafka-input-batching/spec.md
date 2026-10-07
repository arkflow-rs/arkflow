## ADDED Requirements

### Requirement: Kafka input 一次 read SHALL 聚合多行为一批

`KafkaInput::read()` SHALL 以「首条阻塞认领 + 无挂起点排空」的形式聚合多条 Kafka 消息为一个多行 `MessageBatch` 返回：首条消息经阻塞 `recv()` 认领（read 的首个 await，此前的取消不产生任何认领副作用）；其后 SHALL 仅以非阻塞方式取走客户端已缓冲的消息，认领与返回之间 SHALL NOT 引入新的可挂起 await 点。聚合 SHALL 受 `batch_max_rows`（默认 1024，最小 1）与 `batch_max_bytes`（默认 8 MiB，按 payload 字节累计，最小 1）约束，任一满足即停止排空。排空中途遇到可重试接收错误时，已认领消息 SHALL 照常组批返回；遇到不可重试错误 SHALL 立即上抛（与既有逐条路径的错误语义一致）。

#### Scenario: 积压时聚合多行

- **WHEN** 消费者本地队列已缓冲多条消息且吞吐充足
- **THEN** 一次 `read()` 返回包含至多 `batch_max_rows` 行、payload 累计至多 `batch_max_bytes` 的单批，而不是每条一批

#### Scenario: 低流量时无额外延迟

- **WHEN** 队列中仅有一条消息（或为空）
- **THEN** 批为单行（或阻塞等待首条），排空探测立即返回，不为凑批引入任何等待

#### Scenario: 排空期间的取消不丢已认领消息

- **WHEN** 源链在 `read()` pending 时因 idle tick/barrier/取消丢弃该 future
- **THEN** 若首条尚未认领，重新发起的 `read()` 观察到相同流状态；若认领后组批正在进行，组批路径无可挂起点、不被打断，消息不丢失

#### Scenario: 排空遇可重试错误保留已认领消息

- **WHEN** 排空循环中 `recv` 返回可重试错误（如暂时性断连）
- **THEN** 已认领消息组批正常返回，错误在下一轮 `read()` 的阻塞认领处按既有 `Disconnection` 重连路径报告

### Requirement: 批 SHALL 携带逐行对齐的源元数据

批量 read SHALL 为批内每一行附着该行对应消息的源元数据：`__meta_partition`（UInt32）与 `__meta_offset`（UInt64）逐行给出；`__meta_key`（Binary）与 `__meta_timestamp`（Timestamp(ns)）当且仅当批内至少一行存在该值时出现，出现时 SHALL 为 nullable 且缺失行填 NULL；`__meta_ext` SHALL 逐行携带该行的 topic（`topic` 键）与消息头（`header_<key>` 键，重复键位置后缀）；`__meta_ingest_time` SHALL 每 read 批取一次。解码行数与 payload 的映射 SHALL 精确：整批解码的行数等于 payload 数时按位对齐；否则 SHALL 回退为逐 payload 解码并按 payload→行集映射附着元数据，skip 模式丢弃的 payload 不产生行，fail 模式首错即上抛。

#### Scenario: 混批的 key/timestamp 列形状

- **WHEN** 一批中部分消息带 key/CreateTime 时间戳、部分不带
- **THEN** 对应列存在且为 nullable，缺失行取 NULL；全批均无该值时列不出现在 schema 中（与逐条路径的列存在性形状一致）

#### Scenario: skip 模式丢行后元数据仍对齐

- **WHEN** 批内某 payload 解码失败且 codec 配置为 skip 模式
- **THEN** 回退路径逐 payload 解码后，每个幸存行的 `__meta_offset/__meta_partition` 精确等于其来源消息的值，被跳过消息不占行

#### Scenario: WAL 回放按行覆盖判定兼容

- **WHEN** 批量 read 产生的多行批经 WAL 持久化后回放
- **THEN** 逐行读取 `__meta_partition/__meta_offset/__meta_ext.topic` 的覆盖判定与逐条路径语义一致，未确认批整批重投后每行恰好对应其原位置

### Requirement: 批的确认 SHALL 保持连续 frontier 与段语义

批量 read 的 Ack SHALL 按 `(topic, partition)` 分组：每组确认携带该分区内连续 offset 段 `[first, last]`，ack 的位置语义为排他 next-offset（`last+1`），SHALL 经 `CommitFrontier` 连续推进且 broker `store_offset` 只写最高连续 next-offset；undo SHALL 将分区 frontier 与 broker 位点整体回退到段首 `first`（整段重投）。每分区段 SHALL 以段首 offset 锚定 delivery。批内 null-payload（tombstone）SHALL 维持既有的后台结算语义（不阻塞 source loop），与段 ack 的覆盖幂等共存。`transactional_offsets`（L3）下段 ack SHALL 与逐条路径一致地抑制本地 `store_offset`，仅推进内存 frontier 供事务 output 提交。

#### Scenario: 段 ack 连续推进 checkpoint

- **WHEN** 一个分区段的批数据被下游成功处理并确认
- **THEN** frontier 与 checkpoint 位置推进到 `last+1`，与逐条确认全部段内消息后的结果一致

#### Scenario: 段 undo 整段重投

- **WHEN** 段 ack 的补偿（undo）被触发
- **THEN** 分区位点回退到段首，at-least-once 语义下整段（而非仅段尾一条）被重投

#### Scenario: 段内 tombstone 幂等结算

- **WHEN** 批内某分区段包含 null-payload 消息且其位置落在段覆盖范围内
- **THEN** tombstone 的后台结算与段 ack 的推进幂等共存，frontier 不因双重结算跳过或回退；不产生数据行的 tombstone 不进入数据批

#### Scenario: 乱序 fan-out 不跳段

- **WHEN** 一个批经 fan-out 拆分后子确认乱序完成
- **THEN** 暴露的 checkpoint 位置仍只推进到最后一个连续确认的边界，未完成的段不暴露

### Requirement: 批量边界 SHALL 可配置且可回退逐条

`KafkaInputConfig` SHALL 提供 `batch_max_rows` 与 `batch_max_bytes` 字段：缺省分别为 1024 行与 8 MiB；小于 1 的取值 SHALL 在构建期被拒绝或钳制为 1（实现择一并写入文档）；`batch_max_rows: 1` SHALL 恢复逐条出批粒度（批内实现统一，无单独的逐条代码路径）。字段 SHALL 进入组件 JSON Schema 与组件文档（en/zh）。

#### Scenario: 默认配置聚合生效

- **WHEN** 配置未指定批量字段
- **THEN** 聚合以 1024 行 / 8 MiB 为上界工作，无需显式开启

#### Scenario: 逐条回退

- **WHEN** `batch_max_rows` 配置为 1
- **THEN** 每次 `read()` 恰好返回一行（无 codec 多行展开时），行为粒度与批量化之前的逐条路径一致

#### Scenario: 非法边界值

- **WHEN** `batch_max_rows` 或 `batch_max_bytes` 配置为 0
- **THEN** 构建期给出明确错误或钳制为 1（与文档声明一致），不产生无限排空或零行批
