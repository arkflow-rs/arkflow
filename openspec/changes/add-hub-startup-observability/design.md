# Design: add-hub-startup-observability

## Context

`arkflow-server` bin（`crates/arkflow-server/src/bin/arkflow-server.rs`，152 行）手写 env 解析、无 subscriber；引擎侧 `arkflow_core::cli::init_logging(&EngineConfig)`（`crates/arkflow-core/src/cli/mod.rs:353-413`）+ `shutdown_otel_tracing()`（`:471-479`）+ 私有 `build_otel_layer`（`:419-461`，`OTEL_PROVIDER: OnceLock`）已生产可用。`ServerConfig`（`api/mod.rs:120`）有完整 serde 与 `observability` 字段但 bin 不读；`HubConfig`（`hub.rs:74`）/`HubHaConfig`（`hub/leadership.rs:18`）无 serde。secret 解析器 `resolve_value` 是 pub（`arkflow-core/src/secret.rs:70`，env:/file:/secret: 三 scheme；`contains_reference`/`resolve_document` 是 pub(crate) 不可用）。helm control-plane chart 纯 env 配置 Hub（`deploy/charts/arkflow/templates/hub-deployment.yaml:38-64`）。workspace `tracing-subscriber`（根 `Cargo.toml:40`）features 仅 `["std","fmt","json"]`。

用户已确认：配置文件全量覆盖；新增少量 env；支持 RUST_LOG。

## Goals / Non-Goals

**Goals:**
- Hub 进程启动即有结构化日志（默认 console/info），OTel 可选导出，优雅退出 flush。
- `--config` 全量启动配置文件 + env 覆盖 + secret 引用，helm env-only 路径逐字段等价回归。
- 引擎行为不变（RUST_LOG unset 时逐字节）；RUST_LOG 成为引擎与 Hub 共同支持的标准面。
- 未知参数显式失败（exit 2）。

**Non-Goals:**
见 proposal（OIDC 进文件、per-flag 覆盖、热重载、helm chart 改动、clap、独立 metrics 监听器、sampler）。

## Decisions

### D1. core 切面：`init_logging_from(&LoggingConfig, &TracingConfig)`，`build_otel_layer` 保持私有

`init_logging` 实际只读 `config.logging` 与 `config.node.observability.tracing` 两片段，签名取最小。函数体整体搬入新 pub 函数，`init_logging` 变薄壳——引擎调用点零改动、行为不变。server 只经 `init_logging_from` 间接复用 OTel 构建；`build_otel_layer` 的既有单测（cli/mod.rs:714-724、828-858）直接测它，不必为 server 开 pub。备选「server 构造完整 `EngineConfig` 调 `init_logging`」被否：`EngineConfig` 无 Default 且语义是引擎配置，让 Hub 伪构一个引擎配置是类型谎言。

### D2. RUST_LOG 在 core `init_logging_from` 内实现（引擎也获得该能力）

- server bin 无法「外层再包一层 EnvFilter」：`try_init` 后全局 subscriber 已定，server 自建 subscriber 意味着重复 fmt/otel 四象限矩阵，是最差路径。
- `docs/docs/operate/kubernetes.md:66,148` 已向用户承诺 `RUST_LOG=info`，core 内实现让该承诺成真（对引擎与 Hub 同时成立），是文档失真的正向修复而非副作用。
- 实现：私有泛型 `build_subscriber<F>(logging, tracing, filter)`，`F: tracing_subscriber::layer::Filter<Registry> + Clone + Send + Sync + 'static`——现有 fmt 四臂矩阵改 `.with_filter(filter.clone())`，otel layer 同 filter，避免把矩阵复制成八臂。`RUST_LOG` 有值 → `EnvFilter::try_new`（**Err 则 eprintln 告警并回退静态级别**，不 panic）；无值 → `LevelFilter::from(level)`，路径逐字节不变。
- `ARKFLOW_LOG_LEVEL` 不进 core：由 server 的 env 覆盖阶段写入 `logging.level`，否则引擎会静默获得一个新 env 面。
- 依赖前提：根 `Cargo.toml` tracing-subscriber features 加 `"env-filter"`。

### D3. 配置文件结构：`HubStartupConfig`（flatten server + hub + ha），`HubConfig` 本体不加 serde

```rust
// crates/arkflow-server/src/startup_config.rs（新，库内以便测试）
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HubStartupConfig {
    #[serde(flatten)] pub server: ServerConfig,
    #[serde(default)]   pub hub: HubFileConfig,
    #[serde(default)]   pub ha: HubHaConfig,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HubFileConfig { pub operator_token: Option<String> }
```

- `HubConfig` 8 字段中 7 个与 `ServerConfig` 重叠（现有 bin 从 ServerConfig 复制的路径保留），文件里 `hub:` 块只需 `operator_token` 一个独占字段——独立 `HubFileConfig` 不产生两处默认值漂移。
- `HubHaConfig` 补 `Serialize, Deserialize` derive + 容器级 `#[serde(default)]`（缺字段走既有手写 Default，lease_ttl_ms 缺省 15000 语义不变；保留手写 Default 不 derive）。
- `ServerConfig` 加 `#[serde(default)] pub logging: arkflow_core::config::LoggingConfig`；`from_engine` 加 `logging: config.logging.clone()`（引擎路径无行为变化）。
- 备选「`ServerConfig` 直接 flatten 进文件顶层 + `hub`/`ha` 平级」即本方案；备选「包一层 `server:` 嵌套键」被否：与 `ServerConfig` 字段名对齐更直观，迁移成本最低。

### D4. 加载与防泄漏：本地 `load_startup_config`，复用 `resolve_value`

`resolve_document`/`contains_reference` 是 `pub(crate)`，server 用不了；但 `resolve_value` 是 pub 且支持全部三 scheme。加载器（~30 行，零 core 改动）：

```
读文件 → 按扩展名分派（yaml/yml/json/toml；未知扩展名报错）
      → parse 成 serde_json::Value（格式语法错误在此阶段带行列信息）
      → content 含 "${" 时 resolve_value(&mut value)
      → 未知顶层键检查（见 D5）
      → from_value::<HubStartupConfig>
```

含引用时解析后失败用固定消息（"validation failed after secret resolution"），**绝不回显已解析值**——`from_value` 的 serde 类型错误会内嵌解析出的 secret，复用 core `parse_engine_config`（`config.rs:429-444`）的既有模式与注释理由。无引用时保留详细错误。备选「在 core 开泛型公共文件加载助手」被否：server 已有 serde_yaml/toml/serde_json 三依赖，本地实现更小且不给 core 增面。

### D5. 未知顶层键检查：手写键集对比（flatten 与 deny_unknown_fields 编译期不兼容）

加载后把文件顶层键集与 `serde_json::to_value(HubStartupConfig::default())` 的键集求差（序列化天然枚举含 flatten 的全部真实字段名，字段增删不失同步），差集非空 → 报错列出未知键。否则 `adress:` 这类拼写错误被 flatten 静默吞掉。

### D6. env 覆盖：注入式 `apply_env_overrides`，逐一平移现有 presence-check 语义

```rust
pub fn apply_env_overrides(cfg: &mut HubStartupConfig, env: &dyn Fn(&str) -> Option<String>);
```

- bin 传 `|n| std::env::var(n).ok()`，测试传 map 闭包。
- 语义：env 在且值合法 → 覆盖底值；env 在且值非法 → stderr 告警 + **保留底值**（文件值；无文件时即默认——与现状「告警 + 回退默认」仅在文件设置了该字段时有差异，属有意细化）。布尔限 `1/true/yes|0/false/no`；approval_ttl/ha lease 的 `>=1000` 地板沿用现有静默过滤（无新告警）：低于地板的 env 值**不生效、保留底值**——「被拒绝的 env 值」不能比「未设置的 env 值」更强势。
- 现有 bin 的 `env_rate_limit_config`（bin:10-50，含非法布尔 warn 文案）与全部 ARKFLOW_* 变量逐一平移进来，另读入 4 个新 env（ARKFLOW_LOG_LEVEL → `logging.level`、ARKFLOW_OTEL_TRACING_ENABLED/ENDPOINT/SERVICE_NAME → `observability.tracing.*`）。
- **回归等价是硬约束**：无文件时「默认为底 + env 覆盖」必须与今日逐字段构建完全等价（含告警文案与回退对象），设专门回归测试。

### D7. 启动序与 flush 落点

```
main → run():
  解析参数（migrate/backup/restore/--help 不变；serve 路径只认 --config <FILE>，
  其余首参数含未知 flag → 用法提示 exit 2）
  → [--config?] load_startup_config（此时无 subscriber，错误走 stderr）
  → apply_env_overrides
  → arkflow_core::cli::init_logging_from(&server.logging, &server.observability.tracing)
  → validate_ha_config（其 SQLite tracing::warn 从此可见）
  → HubConfig 组装（operator_token 取 hub.operator_token，其余从 server 复制）
  → serve_from_config
main → arkflow_core::cli::shutdown_otel_tracing()（覆盖 serve 返回与 ? 早退两条终态；
       migrate/backup/restore 路径未 init otel，flush 为无害 no-op）
```

文件必须先于 init_logging 加载（logging 配置在文件里）。

### D8. service_name 默认保持复用（`"arkflow"`）

给 Hub 做特化默认需要「检测到默认值就改写」的魔法突变，可预测性差。文档建议共享 collector 的用户显式设 `observability.tracing.service_name: arkflow-hub`（或 env）。

### D9. `ServerConfig.enabled` 在 Hub bin 路径不被读取

serve 无条件启动（现状即如此，env 路径同样不读）。文件使其首次可被用户设置——文档标注「arkflow-server 忽略 enabled」，不做新 fail 语义（诚实边界写进 spec 场景外的文档义务，不进 Requirement）。

## Risks / Trade-offs

- [flatten 使 from_value 类型错误丢失行列信息] → 文档如实写「字段级错误无行号，格式语法错误有行号」；语法错误在 Value 阶段已带行列。
- [EnvFilter/OTEL_PROVIDER 全局 once（try_init 首建为准）] → 测试沿用 core 既有「直接构建不装全局」模式（cli/mod.rs tests:714-724）；跨进程测试用 `tests/cli_logging.rs` 独立进程模式。
- [引擎获得 RUST_LOG 是行为面变化] → proposal 列为有意变更；RUST_LOG 未设置路径逐字节不变；kubernetes.md 同 PR 校正；风险仅为环境里游离的 RUST_LOG 提升引擎日志量，可接受。
- [无 --config 时回归等价] → 「默认为底 + env 覆盖」与今日逐字段构建的等价性（尤其非法布尔/数字的告警文案、回退对象、TTL 静默地板）设专门回归测试（阶段 3）。
- [resolve 后防泄漏] → had_refs 分支固定消息，注释引用 core 既有模式。
- [exit 2 是行为修复] → 今天 `--confg` 静默起服务，修复后显式拒绝——退出码表与 CHANGELOG 写明；migrate/backup/restore 自身解析不受影响。
- [validate_ha_config 错误消息仍引 env 变量名（文件用户看到 ARKFLOW_HUB_HA_ADVERTISE_URL 字样）] → 小改措辞提及字段形式，不改逻辑。
- [otel layer 受有效级别过滤（引擎现状）] → level=info 时 debug span 不导出，文档提一句。

## Migration Plan

- 纯增量发布，无存储/协议迁移。现有 env-only 部署（含 helm）行为逐字段等价（回归测试钉死）；文档提供 `--config` 文件示例与优先级说明。
- 回滚 = 还原二进制即可；配置文件与 env 可并存，回滚后文件被忽略不报错（老二进制不认识该 flag 会 exit 2——回滚说明里写明需去掉 `--config` 参数）。
