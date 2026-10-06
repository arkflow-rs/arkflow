## 1. Arc 缓存（消灭 L1 每消息深拷贝）

- [x] 1.1 `schema_registry.rs`：`cache` 改 `DashMap<u32, Arc<CachedSchema>>`，`resolve_cached` 返回 `Arc<CachedSchema>`（miss 路径 insert clone 一次不变），decode 循环以 `&*cached` 借用调用 `avro_to_arrow` / `protobuf_to_arrow`，签名与错误文案零变化
- [x] 1.2 既有 schema_registry / avro_arrow 测试全绿（零行为变更验证）：codec 90 passed + avro 22 passed

## 2. avro-decode 基准场景

- [x] 2.1 `benchmark.rs`：离线 `InMemoryResolver`（实现 `SchemaResolver`）+ 宽度 5/25/100 的 schema 构造器（混合叶类型 int/long/string/double/boolean + nullable union）+ setup 阶段 `GenericDatumWriter` 预编码 Confluent wire-format 消息（不计入测量）
- [x] 2.2 三个场景 `avro-decode-w5` / `avro-decode-w25` / `avro-decode-w100` 注册进 `run_suite`：每轮 `decode` 批大小 1000 消息，warmup + 多轮取最优沿用现有 harness 语义；复核默认参数总时长在数十秒预算内——首轮等迭代数下 w100 单轮 10.2s 超预算，按 design 预案改为按宽度反比缩放迭代数（等时间预算），三场景合计每轮 ~1.9s，整套回归预算内
- [x] 2.3 基准 smoke：极小行数下三个场景有限耗时、正吞吐、报告含场景名（对齐 delta spec 两个 Scenario）——`tiny_suite_completes_with_positive_throughput` 与 `reports_agree_on_scenarios_and_throughput` 双测通过（8 场景）

## 3. 验证与文档

- [x] 3.1 门禁：`cargo test -p arkflow-plugin` 全绿（redis_cluster 首跑失败为已知环境性端口占用——残留 testcontainers 容器占 16379，`docker rm -f` 后单跑 2/2 通过，与本次改动无关）；`cargo clippy --workspace --all-targets` 0 告警（新代码一处 `manual_is_multiple_of` 已修）；`cargo fmt --all` clean；`pnpm docs:check` 通过（142 页/49 组件）
- [x] 3.2 release 实跑一次 benchmark 留档（变更后数据；如需"前"对照在父提交检出跑同参数），数据记入 tasks 备注
  - 留档（2026-10-06，本机 macOS arm64，默认参数 count=200k/runs=3/warmup=1，best-of）：`avro-decode-w5` 208,193 rows/s（100k rows, 480ms）｜`avro-decode-w25` 45,415 rows/s（20k rows, 440ms）｜`avro-decode-w100` 9,830 rows/s（10k rows, 1.02s）。首轮（等迭代数）数据 208,368 / 44,654 / 9,846 rows/s，跨轮一致。既有场景对照：linear-sql 874k、groupby-sql 769k、filter-project-sql 807k rows/s、codec-json 1,303 batches/s、state-backend 362 ops/s。
  - **Arc 化前后 A/B 实测（2026-10-06 补测，release 测试二进制，count=400k 单轮 ×3 取最优，唯一变量为 `schema_registry.rs` 的 HEAD 版 vs Arc 版）**：w5 173,278 → 207,166 rows/s（**+19.6%**）｜w25 37,302 → 45,554 rows/s（**+22.1%**）｜w100 8,134 → 9,806 rows/s（**+20.6%**）。两侧各自 3 轮内部波动 <1%，信号可信；深拷贝（`Vec<RecordField>` + `BTreeMap` lookup + String 递归克隆）实际占每消息成本约六分之一，高于探索阶段的先验估计（5-10%）。
- [x] 3.3 `docs/docs/develop/benchmark.md` 与 zh-Hans 对应页：场景清单补 avro-decode 三场景（en/zh）
- [x] 3.4 `openspec/PLANNING.md`：修正 2026-09-16 CR 遗留记录——②（subject URL 编码）③（多版本 schema 合并）标注已由 PR #1284 修复；①标注随本变更闭环，落档成本栈 L1-L3 与上游 API 约束结论
- [x] 3.5 契约复核：delta spec 每个 Scenario 均有测试证据；tasks 勾选
  - 「全部场景产出有限正吞吐」→ `benchmark::tests::tiny_suite_completes_with_positive_throughput`（8 场景断言 operations>0 且 per_second>0，含三个 avro 场景名）+ `reports_agree_on_scenarios_and_throughput`（markdown/JSON 双报告含全部场景名与吞吐）。
  - 「Avro 解码场景自包含」→ 场景实现仅用 `InMemorySchemaResolver`（无 reqwest client 构造、无网络路径），由上述 smoke 走真实 `SchemaRegistryCodec::decode` 覆盖。
