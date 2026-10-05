# Design: refactor-storage-delegation

## Context

P3 结构债中最大的单项样板消除。逐臂审计结论（2026-10-05，HEAD=33781eb8）：dispatch 62 个非 Fenced 臂与 ControlPlaneStore 66 个两臂委托 100% 符合固定约定；actor 代理方法模板统一但需逐操作元数据；`StorageCommand` 为私有枚举、无外部构造点。

## Goals / Non-Goals

**Goals**：六层委托收敛为一张声明表 + 一个宏；DB schema 版本号建立（新库旧进程防护）；行为逐位不变。

**Non-Goals**：不改 `StorageBackend` trait 本体（手写契约）；不改 FIFO actor 架构、fence 语义、SQL；不做版本化迁移框架（今日仅版本 1，盖章/校验机制到位即可，演进路径留待真正改 schema 时使用）；不把 PG 契约测试接进 CI（既有 Non-goal）。

## Decisions

### 1. 操作表语法（每字段显式类型 + 传递模式标记）

```rust
storage_ops! {
    ("upsert_job", UpsertJob, upsert_job) JobRecord fenced {
        job: JobRecord as v,
    }
    ("update_job", UpdateJob, update_job) Option<JobRecord> fenced {
        job_id: String as s,
        desired_state: Option<String> as o,
        generation: Option<u64> as v,
    }
    ("list_intents", ListIntents, list_intents) Vec<IntentRecord> plain {
        node_id: Option<String> as q,
    }
}
```

字段模式四选一（由审计确定的既有约定，非新设计）：
- `as s`：变体字段 `String`；dispatch 传 `&field`；trait 参数 `&str`；代理参数 `impl Into<String>`、构造 `.into()`
- `as o`：`Option<String>`；dispatch `field.as_deref()`；trait `Option<&str>`；代理参数 `Option<String>` 按值
- `as q`：同 `o`，但代理参数 `Option<impl Into<String>>`、构造 `.map(Into::into)`（既有 4 个查询方法的风格；不统一到 `o` 是因为裸 `None` 在 `impl Into` 下无法推断，会破坏既有调用点）
- `as v`：任意类型按值；trait/代理参数同类型

每操作元数据：label 字符串、变体名、方法名（`ReadHubLeaseSnapshot`→`hub_lease_snapshot` 重命名靠两个字段都显式声明解决）、返回类型、`fenced|plain`（fenced 分类是语义属性不可推导——`recover_rollouts`/`recover_job_upgrades` 走 fenced、三个 lease 方法走 plain，逐项照抄现状）、可选 `#[doc]` 属性转发到代理方法。宏对所有生成的代理方法统一加 `#[allow(clippy::too_many_arguments)]`（现状仅 2 个方法需要，统一无害）。

### 2. 宏生成物

单次展开生成五层：枚举变体 + `Fenced`（手写保留于宏体内）、`nack()` 全臂 + Fenced 递归、`label()` 全臂、`dispatch()` 全臂 + Fenced 分支（手写保留）、actor 代理方法。**实现期调整：`impl StorageBackend for ControlPlaneStore` 委托层不宏化、保持手写**——`#[async_trait]` 属性宏不处理嵌套 `macro_rules!` 调用生成的方法（最小复现实证：内层宏生成的 `async fn` 逃过脱糖 → E0195），宏内镜像 async-trait 脱糖需要逐引用参数分配 `'lifeN` 生命周期（宏无计数能力，单生命周期合并实测也不被接受）。该层 738 行两臂委托按原样保留，新增操作成本从 6 处编辑降为 3 处（表项 + trait 声明 + 委托臂）。代理方法签名经 tt-muncher 逐字段累积生成（函数签名位置不允许宏调用，这是唯一可行的生成路径）。

### 3. schema 版本

- 常量 `SCHEMA_VERSION: u32 = 1`。
- SQLite：`ensure_schema_version(conn)` 在 DDL 后执行——`PRAGMA user_version` 读；`> CURRENT` → `StorageError::Config`（指明库版本与二进制版本）；否则 `PRAGMA user_version = 1` 盖章。幂等（重开不重复报错）。
- PostgreSQL：DDL 增加 `CREATE TABLE IF NOT EXISTS cp_schema_meta (key TEXT PRIMARY KEY, value BIGINT NOT NULL)`；`ensure_schema_version` 读 `schema_version` 键，同上语义（事务内 upsert 盖章）。
- migrate 工具：复制前校验源库 `user_version`/meta ≤ CURRENT；复制数据含 meta 表；完成后目标库版本 = CURRENT。
- 测试：SQLite 盖章/新版本拒绝/重开幂等三测；PG 走 `ARKFLOW_TEST_POSTGRES_URL` 门控同套断言。

## Risks / Trade-offs

- 宏正确性风险由编译器穷尽性 + 全量回归兜底（任何漏臂 = 编译错误；任何语义漂移 = 契约测试失败）。
- 宏调试体验差于普通代码 → 宏体注释逐层说明展开形态，表项每行自含全部信息。
- 版本校验是新增的启动失败路径（库新于二进制时）——这正是防护目标，错误信息带两侧版本号与升级指引。

## Migration Plan

无数据迁移（版本 1 盖章即完成）。部署顺序无约束：旧二进制 + 已盖章 1 的库正常启动（1 ≤ 1）。
