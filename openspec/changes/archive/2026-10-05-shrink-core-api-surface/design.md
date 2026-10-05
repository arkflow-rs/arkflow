# Design: shrink-core-api-surface

## Context

批次 C 的全部变更都是 Rust API 面层面的，消费者只有自家 workspace 的三个 crate（plugin/server/bin）加各 crate 集成测试，因此 breaking 成本为零——这正是「v1.0 semver 前一次性做完」的含义。约束：

- YAML/JSON 配置形状与 JSON Schema 必须逐字节不变（用户面与 `component-registry-export` 的 schema 门禁不动）。
- 执行内核行为零变化（`unified-execution-kernel` spec 保护路径只许可见性级触碰）。
- 审计脚本基于词法 grep，存在假阴性/假阳性，**以 `cargo check --workspace --all-targets` 为最终准绳**。

## Goals / Non-Goals

**Goals:**

- core 顶层 pub item 只保留跨 crate 实际消费的部分。
- builder trait `name: Option<&str>`；`Temporary::get(&self, keys: &[String])`。
- 删除 dead Error variants；`HealthCheckConfig` → `NodeConfig`（Rust 拆分、YAML 不变）；Hub 专属 secret 分发 helper 迁至 server。

**Non-Goals:** 见 proposal Non-goals（YAML 形状、Error 分类收紧、方法级清扫、模块导出重排）。

## Decisions

### D1. pub 修剪以「脚本审计 + 编译器裁决」执行，不逐个人工判定

审计脚本（已完成）：提取 core 全部顶层 `pub (async )?(fn|struct|enum|trait|type|const|static)` 名字，对 `arkflow-plugin/src`、`arkflow-server/src`、`arkflow/src` 与四个 crate 的 `tests/` 目录做词边界 grep，得 139 个零外部引用 item（清单随任务附带）。执行方式：批量改为 `pub(crate)` → `cargo check --workspace --all-targets` → 对编译报错 item（grep 漏掉的消费者，如经 re-export 或同名碰撞）回退为 `pub`。**为何不用 rustdoc JSON**：需要 nightly 工具链且对 `pub use` re-export 的判定同样要人工归类，收益不抵复杂度。

替代方案否决：`#[doc(hidden)]` 保留 pub——不减少面，只是藏起来，semver 冻结后仍是负担。

### D2. builder trait `Option<&String>` → `Option<&str>`，实现侧统一机械替换

trait 定义 5 处 + core 内测试桩（`runtime.rs`、`control_plane.rs`、`job_runner_adapter.rs`、`stream_adapter.rs` 等 `impl ... for` 桩）+ plugin 60 处实现签名。调用点模式：`name.as_ref()` → `name.as_deref()`；实现内 `name.map(String::clone)`/`name.cloned()` → `name.map(str::to_string)`。逐文件 sed 后以编译错误清单收敛，不做语义改动。

### D3. `Temporary::get` 键类型定为 `&[String]`

唯一生产实现（redis temporary）与唯一生产调用方（sql processor）实际语义都是「一个键表达式求值出 N 个 UTF-8 字符串键，批量查 Redis 再 codec 解码」。`ColumnarValue` 只是运输壳。sql 侧新增本地转换：`Expr` 求值结果（`ColumnarValue::Scalar(Utf8)` → 1 个键；`Array(Utf8)` → 逐行收集）与 `Expr::Value` 字面量统一产出 `Vec<String>`。**为何不是 `&[&str]**：异步 trait 生命周期参数（`async_trait` + `&'a [&'a str]`）在 dyn 兼容性上徒增复杂度，且调用方持有所有权 Vec 更自然。

### D4. `HealthCheckConfig` 改名拆分：serde(flatten) 保持 YAML 逐字节兼容

```
EngineConfig.node: NodeConfig   // #[serde(rename = "health_check")]
NodeConfig {
    #[serde(flatten)] health: HealthEndpointsConfig,      // enabled,address,health_path,readiness_path,liveness_path
    #[serde(flatten)] control_api: ControlApiConfig,      // api_prefix,api_token,cors_origins
    #[serde(flatten)] agent: AgentConfig,                 // hub_urls,node_id,node_token,agent_lease_ttl_ms,agent_session_ttl_ms
    #[serde(flatten)] data_plane: DataPlaneConfig,        // data_port,data_host
    observability: ObservabilityConfig,                   // 原样
    #[serde(default, skip_serializing)] hub_url: DeprecatedHubUrl,  // 哨兵原样
}
```

每个子结构字段保留原有逐字段 `#[serde(default = ...)]`，空 map 反序列化等价现状；flatten 下具名字段 `hub_url` 仍优先命中哨兵、报错语义不变。round-trip 既有测试（config.rs 内 serialize/deserialize 组）作为兼容门禁。**序列化键序会变**（具名字段先于 flatten 字段）——经核实 schema 为手写（`component/mod.rs`）且 Hub 存原文不重序列化，无观察者。`validate_hub_urls` 移入 `AgentConfig`。

替代方案否决：(a) YAML section 一并改名/嵌套重组——用户可见 breaking，5 个示例 + 10 个文档页 + 未知用户配置全要动，成本收益不成比例（proposal Non-goals 已记录）；(b) 仅改名不拆分——「命名大杂烩」的根因（一个类型管五个域）不解决。

### D5. `resolve_candidate_payload` 迁移目标：`arkflow-server/src/hub/` 新模块

envelope 处理（候选载荷 format/content/content_verbatim 契约）与 6 个相关单测迁移到 `hub/secret_dispatch.rs`（调用方 `placement.rs` 同目录就近）。secret-only 遍历留在 core：实现中发现 `resolve_secret_only_*` 是 `${secret:}` 语法的遍历器（`secret-references` 能力域，与 `resolve_env` 等语法机制紧耦合），整体搬移需要在 server 复制 env 解析语义；改为 core 以 `pub fn resolve_secret_references` 导出语法原语（净 pub 面 −1/+1），server 只拥有 Hub 分发特有的 envelope 契约。core `secret.rs` 保留通用配置解析（`resolve_document`/`resolve_value`，被 core 配置加载使用）。**为何不放 server 根 lib**：避免加大 6.7k 行巨石（P3 结构债另有立项），放 `hub/` 与唯一调用方同域。`secret-references` spec 行为零变化，仅实现位置迁移，故无 delta。

## Risks / Trade-offs

- [审计 grep 假阴性漏掉消费者] → 编译器裁决：批量收紧后全 workspace `cargo check --all-targets`（含 tests），报错项回退 `pub`；最终 `cargo test --workspace` 兜底。
- [serde(flatten) 与未来 `deny_unknown_fields`/schemars 自动生成不兼容] → 当前两者均未使用；在 `NodeConfig` doc 注释注明「flatten 是 YAML 兼容手段，勿叠加 deny_unknown_fields」。
- [`Option<&str>` 机械替换在存字符串的 impl 里漏改] → 全量 clippy `-D warnings` 门禁已存在，`clippy::needless_borrow`/类型不匹配会逐个暴露。
- [139 处可见性改动误伤 executor 内部逻辑] → 变更仅限 `pub`→`pub(crate)` 前缀，diff 审查按文件过一遍；不触碰任何函数体。

## Migration Plan

单 PR 落地，无部署迁移（无用户可见行为变化）。回滚 = revert 单提交。

## Open Questions

无——YAML section 是否在 v1.0 前改名由维护者另行决策（proposal Non-goals），不阻塞本变更。
