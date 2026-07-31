## Context

ArkFlow 当前无 CDC 能力（见 `proposal.md` Why）。但补齐 CDC 的基础设施已就位：

- Kafka input 已支持配置 codec 解码（`crates/arkflow-plugin/src/input/kafka.rs:62`、`kafka.rs:198-200` `apply_codec_to_payload`）。
- Kafka input 的 offset 是 ack-gated 的（`kafka.rs:135-148`：`enable.auto.offset.store=false`，仅在 `KafkaAck::ack()` 内 `store_offset()`），与 `openspec/specs/input-durability` 的 ack-gated source-commit 一致。
- codec 注册机制成熟（`crates/arkflow-plugin/src/codec/json.rs:63` `register_codec_builder("json", …)`），JSON→Arrow 解析可复用 `component::json::try_to_arrow`（见 `processor/json.rs:73`）。

因此本 change 是**纯增量、零侵入**：新增一个 codec，挂到 Kafka input 现有 codec 接入点，位点完全复用现有 ack-gated offset。

## Goals / Non-Goals

**Goals:**
- 以 `debezium_json` codec 形式提供 Debezium Envelope JSON → 列式 Arrow 的解析。
- 经 Kafka input 现有 codec 接入点接入，不新增 input 类型、不改 input 代码。
- 一份解析逻辑覆盖 Debezium 支持的全部数据库（MySQL/PostgreSQL/MongoDB/SQLServer 等）。
- 复用 ack-gated Kafka offset 作为 CDC 位点，保持 at-least-once。

**Non-Goals:**
- binlog / PG 逻辑复制直连（未来独立 input）。
- Schema Registry 与 schema 演进治理（方向② Change 2）。
- 端到端 exactly-once（方向② Change 3）。
- Debezium Avro/Protobuf 格式（随 Change 2）。

## Decisions

### 决策 1：放 codec 层，而非 processor 层
**选择**：新增 `debezium_json` **codec**，由 Kafka input 经 `apply_codec_to_payload`（`kafka.rs:198-200`）调用。

**Alternatives**：
- *processor*（如 `debezium` processor）：需先让 input 把 Debezium 字节作为 binary MessageBatch 传入 pipeline，再由 processor 展平。优点是转换逻辑可与其他 processor 组合；缺点是配置更长（input + pipeline 两处）、且把「这是一个 CDC 流」的语义下沉到 pipeline，不如在 input 层声明直观。
- *新 input 类型*（`type: debezium_kafka`）：重复实现 Kafka 消费逻辑，违背 surgical changes。

**理由**：codec 接入点已现成、配置最简（`input.codec.type: debezium_json` 一处）、CDC 是 input 流的性质；processor 方案的两步处理无额外收益。

### 决策 2：Envelope 展平策略——after 扁平为顶层列 + CDC 元列 + before 保留
Debezium Envelope：`{ before, after, op, source, ts_ms }`。

**选择**：输出列 = `after` 的字段**扁平化为顶层列**（主数据行）＋ 顶层元列 `op`（`c`/`u`/`d`/`r`）、`ts_ms`、`source` 关键字段（`source_db`、`source_table` 等）＋ `before`/`source` 作为 **JSON 文本列（Utf8）**保留完整对象。

> 实现发现（2026-07-31）：原计划把 `before`/`source` 作为 `StructArray` 列，但 Arrow JSON Reader 的单遍 schema 推断无法处理同一 batch 内 `null` 与 object 混合（`before` 在 insert 行为 null、update 行为 object，报 `expected null got {...}`）。故改为 JSON 文本列（稳定 Utf8，null 值序列化为 `"null"` 字面量），下游用 DataFusion JSON 函数查询；最常用的 `source_db`/`source_table` 仍为可直接 SQL 的标量顶层列。

**Alternatives**：
- *双扁平 `before_*` / `after_*`*：列数翻倍、大多数下游只关心新值，冗余。
- *完全不展平（整体作为一个 Struct/JSON 列）*：DataFusion SQL 无法直接 `SELECT name WHERE op='u'`，违背列式 SQL 的价值。

**理由**：after 扁平让下游 SQL 可直接用业务字段；`before` 作为 StructArray 保留变更前值且不爆炸列数；`op`/`ts_ms`/`source_*` 顶层列便于过滤与路由。

### 决策 3：DELETE 与 snapshot 的处理
- `op = "d"`（delete）：`after` 为 null → 顶层业务列取自 `before`，`op="d"` 标识删除（下游可据此做删除同步）。
- `op = "r"`（snapshot/read）：当作普通行，`after` 即初始值。
- `op = "c"/"u"`：`after` 提供新值。

### 决策 4：位点管理不新增，复用 ack-gated Kafka offset
CDC 位点即 Kafka offset，已由 `KafkaAck::ack()`（`kafka.rs:135-148`）经 `input-durability` 的 ack-gated source-commit 推进。**本 change 不引入任何位点代码**。

### 决策 5：复用 `component::json` 解析，无新依赖
Envelope 内 `before`/`after`/`source` 为 JSON 对象，复用 `component::json::try_to_arrow`（`processor/json.rs:73` 已用）做 JSON→Arrow，不引入新 crate。

## Risks / Trade-offs

- **[依赖外部 Debezium + Kafka Connect]** → 与「单二进制轻量」有张力。缓解：文档明确部署架构（Debezium Server / Kafka Connect → Kafka → ArkFlow）；零依赖直连作为未来独立 input 的 Non-goal。
- **[schema 漂移（DDL 加列）]** → 同一 batch 内 Arrow schema 由推断一致；跨 batch 的 schema 演进不在本 change 范围。缓解：留给方向② Change 2（Schema Registry）。
- **[DELETE 时 after=null 的列对齐]** → 展平时需用 before 的字段并补 null 对齐 schema。缓解：决策 3 明确取自 before，并在 specs 中以 scenario 约束。
- **[Avro/Protobuf 格式不支持]** → Non-goal；JSON 先行，Avro 随 Schema Registry（Change 2）。

## Migration Plan

- 纯新增 codec + 注册，**无破坏性变更**，现有配置与行为不受影响。
- 部署：用户在 Kafka input 下增加 `codec: { type: debezium_json }` 即启用。
- 回滚：移除该 codec 配置或回退注册提交即可，无状态/数据迁移。

## Open Questions

- `source` 字段提取范围：仅 `db`/`table`/`ts_ms` 子集，还是全量保留为 StructArray？——倾向「关键字段子集 + 同时保留完整 `source` StructArray 列」，实现时定。
- Envelope 缺失字段（如无 `transaction`）的容错策略——按「字段缺失即 null」处理，specs 约束。
