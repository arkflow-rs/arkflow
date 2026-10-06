## Why

2026-09-16 CR 遗留项①（Avro 路径 `GenericDatumReader` 每消息重建未缓存）经 2026-10-06 源码核实后定性升级：schema_registry 解码热路径每条消息存在三层叠加成本——L1 `resolve_cached` 命中路径对 `CachedSchema` 的整树深拷贝（apache-avro `Schema` 无 Arc：`Vec<RecordField>` + `BTreeMap` lookup + String 全递归克隆）、L2 `GenericDatumReader::build()` 每消息重建名字表、L3 每字段每消息一个全新 Arrow array 分配。CR 定下的纪律是「先出基准再立项」；本变更交付未被阻塞的两件事：消灭 L1（显然正确、零语义风险），并建立 Avro 解码基准为 L2/L3 的后续决策提供数据。

## What Changes

- schema 缓存从 `DashMap<u32, CachedSchema>` 改为 `DashMap<u32, Arc<CachedSchema>>`，消灭每消息深拷贝（L1）。protobuf 侧 `MessageDescriptor` 本身廉价克隆，统一 Arc 包装无害。
- 公开基准套件（`arkflow_plugin::benchmark`）新增 `avro-decode` 场景：预编码 Confluent wire-format 消息经**离线 in-memory `SchemaResolver`** 走真实 `SchemaRegistryCodec::decode` 路径；矩阵为 schema 宽度（5/25/100 字段）× 批大小，产出与其他场景一致的 markdown/JSON 报告行。
- `docs/docs/develop/benchmark.md` 与 zh-Hans 对应页的场景清单同步更新。
- `openspec/PLANNING.md` 修正 2026-09-16 CR 遗留记录：②（subject URL 编码）③（多版本 schema 合并）已由 PR #1284 修复，随本变更闭环划掉；①的探索结论（API 约束与成本栈）落档。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `benchmark-suite`: 「场景覆盖内核主路径」requirement 扩展——基准 SHALL 覆盖 Avro schema-registry 解码路径（离线、可复现、与其他场景同报告格式）。

## Impact

- `crates/arkflow-plugin/src/codec/schema_registry.rs`：`cache` 字段类型与 `resolve_cached` 返回值改 `Arc<CachedSchema>`；调用点（decode 循环）适配借用。
- `crates/arkflow-plugin/src/benchmark.rs`：新增 avro-decode 场景函数并注册进 `run_suite`（离线 resolver、预编码数据、宽度×批大小矩阵）。
- `docs/docs/develop/benchmark.md` + zh-Hans 对应页：场景清单。
- 无公开 API、行为、依赖变化（apache-avro 版本不动，不引入新 crate）；`schema-registry-integration` spec 的「按 schema id 缓存」契约（同 id 不重复请求 registry）语义不变。
