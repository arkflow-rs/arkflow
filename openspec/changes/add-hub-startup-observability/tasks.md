# Tasks: add-hub-startup-observability

## 1. core 日志重构

- [x] 1.1 根 `Cargo.toml`：tracing-subscriber features 加 `"env-filter"`
- [x] 1.2 `crates/arkflow-core/src/cli/mod.rs`：抽出 `pub fn init_logging_from(&LoggingConfig, &TracingConfig)`（函数体整体搬入），`init_logging` 变薄壳；引擎行为不变（既有测试全绿）
- [x] 1.3 私有泛型 `build_subscriber<F: Filter<Registry> + Clone + Send + Sync + 'static>` 收拢 fmt 四象限矩阵 + otel layer 同 filter
- [x] 1.4 RUST_LOG 分支：`EnvFilter::try_new`（失败 eprintln 告警回退静态级别）；未设置走原 `LevelFilter` 路径
- [x] 1.5 core 单测：filter 决策（合法 RUST_LOG / 非法回退 / 未设置）；既有 `otel_layer_is_absent_when_tracing_is_disabled`、`shutdown_without_provider_is_a_no_op`、exporter 隔离单测保持绿
- [x] 1.6 `crates/arkflow-core/tests/cli_logging.rs`：独立进程测试 RUST_LOG=warn 生效 + 未设置回归不变

## 2. 配置文件层

- [x] 2.1 `crates/arkflow-server/src/api/mod.rs`：`ServerConfig` 加 `#[serde(default)] pub logging: LoggingConfig`；`from_engine` 映射；`Default` 补字段
- [x] 2.2 `crates/arkflow-server/src/hub/leadership.rs`：`HubHaConfig` 加 `Serialize, Deserialize` + 容器级 `#[serde(default)]`（保留手写 Default）
- [x] 2.3 新建 `crates/arkflow-server/src/startup_config.rs`：`HubStartupConfig`（flatten server + hub + ha）、`HubFileConfig`；`lib.rs` 挂模块并 pub use
- [x] 2.4 `load_startup_config(path)`：扩展名分派 yaml/yml/json/toml（未知扩展名报错）→ `serde_json::Value` → 含 `${` 时 `arkflow_core::secret::resolve_value` → 未知顶层键检查 → `from_value`；含引用时解析后失败固定消息防泄漏
- [x] 2.5 未知顶层键检查：文件键集对 `to_value(HubStartupConfig::default())` 键集求差，非空报错列出
- [x] 2.6 测试 `crates/arkflow-server/tests/startup_config.rs`：三格式全块解析、缺字段默认、`${env:X}`/`${secret:X}` 解析、解析后类型错误固定消息不含 secret 串、未知键报错列出、不存在文件/未知扩展名/坏 yaml 错误文案

## 3. env 覆盖层

- [x] 3.1 `apply_env_overrides(&mut HubStartupConfig, &dyn Fn(&str) -> Option<String>)`：现有全部 ARKFLOW_* 变量逐一平移（含 `env_rate_limit_config` 的告警文案与回退语义）+ 4 个新 env（ARKFLOW_LOG_LEVEL → logging.level；ARKFLOW_OTEL_TRACING_ENABLED/ENDPOINT/SERVICE_NAME → observability.tracing）
- [x] 3.2 语义：env 在且合法覆盖；在且非法 stderr 告警保留底值；布尔限 `1/true/yes|0/false/no`；approval_ttl/ha lease ≥1000 静默地板
- [x] 3.3 测试：env 胜文件、非法布尔/数字告警且保留文件值、env 缺席保留文件值、无文件时与现状默认逐字段等价（helm 兼容回归）、ARKFLOW_LOG_LEVEL 覆盖文件 logging.level

## 4. bin 接线

- [x] 4.1 `bin/arkflow-server.rs`：serve 路径参数解析只认 `--config <FILE>`；未知 flag/陌生位置参数/缺值 → 用法提示 + exit 2；USAGE 文本更新
- [x] 4.2 启动序：load → apply_env_overrides → `init_logging_from` → validate_ha_config（错误措辞提及配置字段形式）→ HubConfig 组装（operator_token 取 hub 块）→ serve_from_config
- [x] 4.3 main 收进 `async fn run()`；main 统一 `shutdown_otel_tracing()`（覆盖 serve 返回与早退）
- [x] 4.4 测试 `crates/arkflow-server/tests/cli_flags.rs`（CARGO_BIN_EXE）：`--help` exit 0 含 --config 文案；`--confg x` exit 2；`--config` 缺值 exit 2；`--config 不存在.yaml` exit 2；陌生位置参数 exit 2

## 5. 冒烟集成

- [x] 5.1 `crates/arkflow-server/tests/bootstrap.rs` 追加：最小 insecure_local yaml → `load_startup_config` + env 覆盖 → `serve_from_config(127.0.0.1:0)` + cancellation（复用既有 193-225 模式）
- [x] 5.2 tracing 默认关闭惰性测试：默认 ServerConfig 的 `observability.tracing.enabled == false`，无全局 provider 副作用

## 6. 文档与收尾

- [x] 6.1 `docs/docs/reference/cli.md` + zh：删 "has no flags"；`--config` 节（格式/覆盖范围/示例/secret 引用/未知键拒绝）；env 表加 4 新变量 + RUST_LOG；优先级链（RUST_LOG > ARKFLOW_LOG_LEVEL > logging.level > 默认）与文件-vs-env 总则；退出码表补 usage error 2；标注 `enabled` 被 arkflow-server 忽略
- [x] 6.2 `docs/docs/operate/control-plane/deploy.md` + zh：配置文件部署节（完整示例、env-only 与文件混用建议、helm env-only 不需改 chart）
- [x] 6.3 `docs/docs/operate/observability.md` + zh：Hub 进程日志节（ARKFLOW_LOG_LEVEL/文件 logging 块）；Hub OTel 子节（observability.tracing/ARKFLOW_OTEL_*、service_name 建议 arkflow-hub、退出 flush、级别过滤提示、TraceLayer span 可见）
- [x] 6.4 `docs/docs/operate/kubernetes.md` + zh：RUST_LOG=info 表述校正为引擎与 Hub 均真实生效
- [x] 6.5 `CHANGELOG.md` Unreleased：Added 条目（--config、新 env、RUST_LOG 引擎侧成真、exit 2 行为修复、Hub 日志/OTel）
- [x] 6.6 验证：`cargo test -p arkflow-core -p arkflow-server` → `cargo test --workspace` → clippy/fmt → `pnpm docs:check`
- [x] 6.7 真实运行验证：本地文件起 Hub（日志可见）；env-only 老方式回归等价；OTel collector（如可用）收 span + Ctrl-C flush
