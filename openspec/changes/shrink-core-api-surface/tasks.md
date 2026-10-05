# Tasks: shrink-core-api-surface

## 1. Dead code 与 trait 签名（小步先行，独立可验证）

- [x] 1.1 删除 `Error` 的 `LockTimeout`/`InvalidConfig` dead variants（`crates/arkflow-core/src/lib.rs`），全 workspace 确认零构造零匹配
- [x] 1.2 5 个 builder trait（`input/mod.rs`、`output/mod.rs`、`processor/mod.rs`、`codec/mod.rs`、`buffer/mod.rs`）`name: Option<&String>` → `Option<&str>`；core 内实现桩与调用点（`runtime.rs`、`control_plane.rs`、`job_runner_adapter.rs`、`stream_adapter.rs` 等）同步（`as_ref()`→`as_deref()`、`cloned()`→`map(str::to_string)`）
- [x] 1.3 arkflow-plugin 全部 builder 实现（约 60 处）与调用点同步替换；`cargo check -p arkflow-plugin --all-targets` 通过
- [x] 1.4 `Temporary::get` 签名 `&[ColumnarValue]` → `&[String]`：core trait、plugin `temporary/redis.rs`（删 `get_key` ScalarValue 解包）、`processor/sql.rs`（求值结果/字面量统一转 `Vec<String>`，null/非 UTF-8 键 fail-closed）、core 测试桩（`resource_guard.rs`、sql.rs 测试 `StaticTemporary`）

## 2. `HealthCheckConfig` → `NodeConfig` 改名拆分

- [x] 2.1 `config.rs`：拆分为 `NodeConfig` + `HealthEndpointsConfig`/`ControlApiConfig`/`AgentConfig`/`DataPlaneConfig`（serde flatten，逐字段 default 原样），`EngineConfig.node` 字段 `#[serde(rename = "health_check")]`，`DeprecatedHubUrl` 哨兵与 `validate_hub_urls`（移入 `AgentConfig`）语义不变
- [x] 2.2 config.rs 内既有 serialize/deserialize/round-trip 测试扩充：断言 YAML 键集（16 个历史键全保留、哨兵不序列化）、空对象默认值、`EngineConfig` 的 `health_check` 键名兼容
- [x] 2.3 全 workspace `HealthCheckConfig` 引用更新（arkflow-server `lib.rs`/`agent.rs`/`bootstrap.rs` 及集成测试约 40 处 → `NodeConfig`/`config.node.xxx`）；`cargo check --workspace --all-targets` 通过

## 3. `resolve_candidate_payload` 迁出 core

- [x] 3.1 envelope 函数 + 6 个单测（yaml/toml/json、无 secret 短路、缺失报错、escape 重扫、verbatim）迁至 `crates/arkflow-server/src/hub/secret_dispatch.rs`；core `secret.rs` 以 `pub fn resolve_secret_references` 暴露 secret-only 遍历原语（设计 D5 修正：语法遍历属 `secret-references` 能力域留在 core，envelope 契约属 Hub 移至 server）；`hub/placement.rs` 调用点改 `super::secret_dispatch`
- [x] 3.2 `cargo test -p arkflow-server`（迁移单测）+ `cargo test -p arkflow-core secret`（确认 core secret 测试无缺失覆盖）

## 4. pub 面修剪（最大块，最后做）

- [x] 4.1 审计清单执行：139 个 core 外零引用 item 批量降 `pub(crate)`；编译器多轮裁决回退 40+ 个假阴性（返回类型/再导出路径使用者：FrontierSnapshot、JobMetricsRegistry、TracingConfig、ParseOutcome 等）
- [x] 4.2 降级暴露的死代码清除：删除彻底死亡项（control.rs 四类型、error_helpers 两映射、META_COLUMN_PREFIX、remote 旧 frame 函数、run_job 兼容壳 run_job_tasks/run_job_with_hooks/run_job_with_checkpoints/shared_resource、filter_rows、TaskAttemptController、JobState/JobDesiredState/JobConvergenceState、JobCommand、bounded_job_channel、task_for_key、registered_wal_store_count、ChainSnapshot 三个 written-not-read 字段）；测试专用项 `#[cfg(test)]` 门控（run_graph 族、BarrierCoordinator、KeyedCounter/WindowAccumulator、window persist_buffers 族、state_journal 观测访问器、StatefulOperator::new、TimestampExtractor/EventTimeMetrics）；executor/wal mod.rs 再导出相应收缩（保留 AckAdvance/CommitFrontier/Envelope/Chain/ExecutionGraphBuilder/run_job/StreamJobAdapter/WalStore 等 8 项 pub 再导出）
- [x] 4.3 复跑审计：core 顶层 pub item 318 → **221**（-97，-30%）；`pub(crate)` 101 处；workspace 0 error 0 warning

## 5. 收口与验证

- [x] 5.1 `cargo fmt --all`、`cargo clippy --workspace --all-targets -- -D warnings` 全绿
- [x] 5.2 `cargo test --workspace --all-targets --no-fail-fast`：29 个测试二进制 ok（含 examples_validate、docs_inventory_snapshot、docs_snippets_validate、core executor 全量、server 全量、secret_dispatch 迁移单测 6/6）；7 个失败均为外部服务依赖的集成测试（kafka_eos/mongodb_io/mqtt_io/nats_io/pulsar_io/redis_cluster/redis_input，失败模式为 broker 容器启动超时或 TCP Connection refused，本机无对应服务，未触达任何被改代码路径）
- [x] 5.3 `pnpm docs:check` 全绿（142 页/49 组件）；YAML 面不变证据：`docs/static/config-schema.json` 零改动（git diff 空）、`docs_inventory_snapshot` 测试 ok、config.rs 新增键集/哨兵/`health_check` 键名兼容 round-trip 测试
- [x] 5.4 `openspec/CODE_REVIEW_2026-09-29.md` P3 第一条与批次 C 行划掉注明落地 change；`openspec/PLANNING.md` §9.3 批次 C 行同步划掉
