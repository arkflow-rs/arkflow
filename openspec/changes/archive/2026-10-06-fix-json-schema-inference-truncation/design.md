# Design: fix-json-schema-inference-truncation

## Context

单调用点修复（见 proposal Why 节）：`crates/arkflow-plugin/src/component/json.rs:28` 的 `infer_json_schema(..., Some(1))` 只读首条记录，导致跨记录类型漂移被静默截断、后续新字段被静默丢弃。arrow-json 的推断 API 本身支持全量扫描，截断/丢列均来自"推断样本 = 首条"这一前提。

## Goals / Non-Goals

**Goals:**

- 合并解码的 schema 由全量记录推断：类型并集取宽、字段取并集、null 放宽 nullable。
- 单消息路径（skip 逐条、Kafka 逐消息）零变化。
- 既有 skip-widens 回归测试修正为真正守护 skip/fail 分化。

**Non-Goals:** 见 proposal Non-goals（显式 schema 声明、on_error 语义、Avro/Protobuf、推断上限均不动）。

## Decisions

### D1 — `Some(1)` → `None`，一次改动修复两类静默错误

`infer_json_schema(&mut cursor, None)` 扫描全量记录。arrow-json 推断本身即按并集语义合并：Int64 列见到 Float64 token 升格 Float64、新字段追加新列、null 放宽 nullable。**无需自研合并逻辑**——截断与丢列的根因只是样本量，不是合并规则。

备选「保持 Some(1) + 解码侧安全强转」被否：需要在 `ReaderBuilder` 与 arrow-json 的 token 回退链之外再造一层值转换，且无法找回"首行没有的字段"（列根本不存在，无从强转）。

### D2 — 推断-解码的 schema 一致性

`try_to_arrow` 现有结构已经用**同一个** `inferred_schema` 做推断结果与 Reader 输入（`json.rs:26-52`），全量推断后 Reader 按全量并集 schema 解码每条记录，`concat_batches` 也以该 schema 为准——schema 单一来源，无需其他改动。`fields_to_include` 投影发生在推断之后（`json.rs:29-37`），与全量推断正交，行为不变。

### D3 — 测试修正与新增

- 新增合并解码回归（`codec/json.rs` 与 `processor/json.rs` 各一组）：`{"v":1}`/`{"v":1.5}`/`{"v":2.9,"tag":"x"}` → Float64 `[1.0,1.5,2.9]` + `tag` 列保留；全整数列仍 Int64。
- 修正 `test_json_codec_skip_widens_types_like_fail_mode`（`codec/json.rs:361`）：skip 路径产出 Float64（不截断），fail 路径对混型批维持整批报错或按全量推断成功——以实际分化行为重写断言与文档串，删除名不副实的"两者一致"断言。
- 单消息路径抽样回归：`on_error: skip` 逐条解码与 Kafka 逐消息解码输出与既有 golden 一致。

## Risks / Trade-offs

- **推断成本 O(batch)**：与解码同阶，批大小受通道容量与 codec 批参数约束；无额外内存峰值（`Cursor` 在既有 `&[u8]` 上流式扫描）。大批场景若实测成为热点，后续可加推断字节预算——但预算会重新引入盲区，故本期不做（Non-goal）。
- **类型放宽的下游影响**：原本被静默截断为 Int64 的列变为 Float64——依赖旧（错误）类型的下游 SQL/输出 schema 会看到类型变化。这是把静默错误变为正确值，属预期修复；在 docs 任务中说明。

## Migration Plan

单 PR。无配置迁移；下游若依赖被截断的 Int64 类型，需自行适配（docs 说明）。

## Open Questions

（无）
