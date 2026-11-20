# Proposal: add-hub-startup-observability

## Why

`arkflow-server`（Hub 控制面二进制）没有任何 tracing subscriber 初始化：crate 内 53 处 `tracing::` 调用全部落空（如选主 `hub/leadership.rs:144`、SQLite HA 开发警告 `bootstrap.rs:356`、HA standby `api/mod.rs:995`），挂在路由上的 `TraceLayer`（`api/mod.rs:499,861`）span 同样无人消费——运维在生产环境看不到 Hub 的任何结构化日志。同时该二进制只接受环境变量（`bin/arkflow-server.rs:102-135` 手写解析），`api_prefix`、`cors_origins`、`session_ttl_ms`、`observability.*` 等字段根本没有配置入口（`api/mod.rs:120-172` 的 `ServerConfig` 已带 serde 派生与 `observability` 字段，但 bin 路径从不读它，`..ServerConfig::default()` 收尾）。另外两个既有失真：`docs/docs/operate/kubernetes.md:66,148` 向用户承诺 `RUST_LOG=info`，但实现从未支持过 RUST_LOG；serve 路径对未知参数（含拼错的 `--confg`）静默按启动 Hub 处理（`bin/arkflow-server.rs:76-95`）。

引擎侧已有完整可复用设施：`arkflow_core::cli::init_logging(&EngineConfig)`（`crates/arkflow-core/src/cli/mod.rs:353-413`，fmt 层四象限矩阵 + `build_otel_layer` OTLP http-json 失败隔离）与 `shutdown_otel_tracing()`（`:471-479`）。本 change 把它开放给 Hub 二进制复用，并闭合配置面缺口。这是控制面补齐 roadmap（openspec/PLANNING.md §11.2-8、§11.4 P2「server 二进制可观测」）的既定项。

## What Changes

- **core 日志入口重构**：`init_logging(&EngineConfig)` 拆出 `pub fn init_logging_from(&LoggingConfig, &TracingConfig)`（函数体整体搬入），`init_logging` 变薄壳——引擎调用点（`crates/arkflow/src/main.rs:29`）零改动。在 `init_logging_from` 内实现 `RUST_LOG`（`EnvFilter`，解析失败告警并回退静态级别；未设置时走原 `LevelFilter` 路径逐字节不变）——**引擎二进制因此也获得 RUST_LOG 支持**（有意变更，kubernetes.md 的既有承诺成真）。workspace `tracing-subscriber` 增加 `env-filter` feature。
- **Hub 启动配置文件**：`arkflow-server` 新增 `--config <FILE>`（YAML/JSON/TOML 按扩展名分派），承载全量启动配置——`ServerConfig` 全部字段（含 `logging`、`observability`）+ `hub.operator_token` + `ha` 块。支持 secret 引用 `${env:VAR}`/`${file:PATH}`/`${secret:NAME}`（复用 `arkflow_core::secret::resolve_value`）；secret 解析后的类型错误用固定消息防泄漏（对齐 core `parse_engine_config` 模式，`config.rs:429-444`）；未知顶层键报错列出（flatten 无法 deny_unknown_fields，手写键集对比防漂移）。
- **env 覆盖语义**：文件为底、显式设置的 env 覆盖文件值、缺省回落内置默认（与今日 helm env-only 路径逐字段等价，专门回归测试钉死）。非法 env 值告警并保留底值（平移现有 presence-check 模式）。新增 4 个 env：`ARKFLOW_LOG_LEVEL`、`ARKFLOW_OTEL_TRACING_ENABLED`、`ARKFLOW_OTEL_ENDPOINT`、`ARKFLOW_OTEL_SERVICE_NAME`。级别优先级：`RUST_LOG` > `ARKFLOW_LOG_LEVEL` > 文件 `logging.level` > 默认 `info`。
- **OTel 导出**：Hub 启动时初始化 subscriber（默认 console/info），`observability.tracing` 启用时经 OTLP/HTTP-JSON 批量导出（默认关、exporter 构建失败告警后继续运行不阻塞启动）；优雅退出（Ctrl-C 或 serve 返回）时 `shutdown_otel_tracing()` flush 尾部 span。
- **参数解析修复（BREAKING 行为修复）**：serve 路径只认 `--config <FILE>`；未知 flag、陌生位置参数、`--config` 缺值 → stderr 用法提示 + exit 2（与 migrate/backup/restore 既有用法错误退出码一致）。今天拼错参数会静默启动 Hub。
- **文档**（en/zh 成对）：cli.md（删除 "has no flags"、新 flag/env 表、优先级链、退出码）、deploy.md（配置文件部署节）、observability.md（Hub 进程日志/OTel 节）、kubernetes.md（RUST_LOG 表述校正）、CHANGELOG。

## Capabilities

### New Capabilities

（无——全部归宿到两个既有 capability）

### Modified Capabilities

- `control-plane-deployment`: ADDED「Hub startup configuration file」Requirement（--config 契约：格式、覆盖范围、env 优先级、secret 引用、未知键拒绝、用法错误退出码）；MODIFIED 既有「The Hub server CLI surface SHALL be documented in the CLI reference」Requirement（文档义务扩展至 --config、4 个新 env、RUST_LOG、优先级链与退出码表）。
- `control-plane-observability`: ADDED「Hub process logging and OTel trace export」Requirement（启动即有 subscriber、tracing 默认关、exporter 失败隔离、优雅退出 flush、既有 metrics/health 端点行为不变）。

## Impact

- **代码**：`crates/arkflow-core/src/cli/mod.rs`（重构 + RUST_LOG）；`crates/arkflow-server/src/{api/mod.rs, hub/leadership.rs, startup_config.rs(新), lib.rs, bin/arkflow-server.rs}`；根 `Cargo.toml`（tracing-subscriber features）。
- **API/接口面**：`ServerConfig` 新增 `logging` 字段（serde default，序列化面 additive）；`HubHaConfig` 加 serde derive；`arkflow-server` CLI 新增 `--config` flag 与 4 个 env；未知参数从「静默 serve」变为 exit 2（行为修复，写入 CHANGELOG）。
- **部署**：helm chart 不改——「无文件时默认为底 + env 覆盖」与今日 env-only 路径逐字段等价；文档说明 env 用法不变。
- **测试**：core 单测/独立进程测试、server startup_config/env 覆盖矩阵、二进制级 cli_flags（CARGO_BIN_EXE）、bootstrap 冒烟。
- **依赖**：仅 workspace `tracing-subscriber` 增加 `env-filter` feature；server 不新增直接 OTel 依赖（经 arkflow-core 传递）。

## Non-goals

- OIDC 配置不进文件（`ARKFLOW_OIDC_*` env 面保持现状）。
- 不做 per-flag CLI 覆盖（`--config.addr` 式）与配置热重载。
- 不改 helm chart（env-only 路径天然回归等价）。
- 不引入 clap 等第三方 CLI 解析库（延续手写解析）。
- 不给 Hub 新增独立 8081 metrics 监听器（已有认证的 `/api/v1/metrics`）。
- `ServerConfig.enabled` 字段在 Hub bin 路径不被读取（serve 无条件启动）——文档明示，不做新 fail 语义。
- 不做 sampler/采样率配置（`TracingConfig` 三字段保持不变）。
