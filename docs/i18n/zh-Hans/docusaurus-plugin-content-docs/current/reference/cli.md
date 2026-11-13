---
sidebar_position: 10
title: CLI 参考
description: "`arkflow` 的全部命令、标志(flag)与退出行为。"
---

# CLI 参考

`arkflow` 二进制是一个单一可执行文件:它从 YAML 配置运行流与作业,并提供用于发现组件注册表的命令。

```bash
arkflow [OPTIONS] [COMMAND]
```

## 运行引擎

| 选项 | 描述 |
|--------|-------------|
| `-c, --config <FILE>` | YAML 配置文件路径。除发现类子命令外必填。 |
| `-v, --validate` | 校验配置后退出,不启动引擎。 |
| `--version` | 打印版本并退出。 |

```bash
# 运行配置中定义的流与作业
./target/release/arkflow --config config.yaml

# 深度校验一份配置:解析、流 id、声明的作业图、
# 重复 id、算子引用以及配置规则
./target/release/arkflow --config config.yaml --validate
```

`--validate` 不止于反序列化:它检查流 id 的唯一性,校验每一条声明的作业规格(图结构与算子引用),并运行引擎的配置规则检查。校验失败会打印 `Invalid configuration: ...` 并以非零状态退出,因此该标志适用于 CI 流水线与准入钩子。

## `components list`

按类别分组列出所有已注册组件。

```bash
arkflow components list [--kind KIND] [--format FORMAT]
```

| 选项 | 描述 |
|--------|-------------|
| `-k, --kind <KIND>` | 按组件类别过滤:`input`、`output`、`processor`、`buffer`、`codec`、`temporary`。 |
| `-f, --format <FORMAT>` | `text`(默认,对齐列)或 `json`(机器可读的注册表导出)。 |

```bash
$ arkflow components list --kind codec
codec:
  json        JSON codec for parsing and serializing message payloads
  protobuf    Protobuf codec using a compiled .proto descriptor
  ...
```

`json` 格式与生成[组件清单](./component-inventory.md)所用的注册表导出完全相同,因此脚本与文档永远不会和二进制不一致。

## `components show`

打印单个组件的配置 schema。

```bash
arkflow components show <KIND> <NAME> [--format FORMAT]
```

| 参数 | 描述 |
|----------|-------------|
| `<KIND>` | 组件类别(必填)。 |
| `<NAME>` | 已注册组件的类型名(必填)。 |
| `-f, --format <FORMAT>` | `text`(默认)或 `json`。 |

```bash
$ arkflow components show input kafka
kafka: Kafka input component for consuming messages from Kafka topics
kind: input
config_optional: no

Example:
{ ... pretty-printed config example ... }

Config schema:
{ ... JSON Schema for the component config ... }
```

未知名称会失败并列出合法备选项,例如 `Unknown input type: kafaka. Available input types: file, generate, http, kafka, ...`。

## `schema`

打印整个引擎配置的 JSON Schema(draft 2020-12)。把它交给你的编辑器即可获得 YAML 自动补全与悬停文档——参见 [IDE 自动补全](./ide-schema.md)。

```bash
arkflow schema > arkflow.schema.json
```

## 退出行为

| 调用方式 | 行为 |
|------------|----------|
| `components ...`、`schema` | 打印结果并立即退出;不读取任何配置文件。 |
| `--validate` | 校验配置,记录日志 `The config is validated.`,随后退出且不启动引擎。 |
| `--config <FILE>`(默认) | 校验配置,随后启动引擎并阻塞直至关闭。 |
| 缺少 `--config` 且无子命令 | 错误:`missing --config <FILE> (or run a subcommand: components, schema)`。 |

## `arkflow-server`

`arkflow-server` 二进制运行控制面 Hub。启动配置来自环境变量与可选的完整配置文件(`--config <FILE>`);`migrate`、`backup`、`restore` 子命令负责存储数据库管理。

### `--config <FILE>` {#config-file}

`--config` 从 YAML、JSON 或 TOML 文件(按扩展名分派)加载完整启动配置。文件覆盖全部启动字段,包括环境变量无法设置的项(`api_prefix`、health 路径、`cors_origins`、`session_ttl_ms`、`lease_ttl_ms`、日志与可观测性):

```yaml validate=foreign reason="arkflow-server --config startup file; parsed by the server binary, not the engine parser"
# hub.yaml — full arkflow-server startup configuration
address: 0.0.0.0:8080
tls_cert: /etc/arkflow/hub.crt
tls_key: /etc/arkflow/hub.key
hub_storage: postgres://user:pass@db:5432/arkflow
node_token: ${env:ARKFLOW_NODE_TOKEN}
cors_origins:
  - https://console.example.com
logging:
  level: info
  format: json
observability:
  tracing:
    enabled: true
    endpoint: http://127.0.0.1:4318/v1/traces
    service_name: arkflow-hub
hub:
  operator_token: ${secret:OPERATOR_TOKEN}
ha:
  enabled: true
  lease_ttl_ms: 15000
  advertise_url: https://hub.example.com
```

规则:

- **优先级**:显式设置的环境变量覆盖文件值;未设置的变量回落到文件值,再回落到内置默认。仅用环境变量的部署行为与之前完全一致。
- **secret 引用**:`${env:VAR}`、`${file:PATH}`、`${secret:NAME}` 在文件加载时解析(与引擎配置同一语法)。secret 解析之后的校验错误绝不回显已解析的值。
- **未知顶层键会启动失败**并指名键——`adress:` 这类拼写错误会被捕获,而不是被静默忽略。
- `enabled` 字段被接受但被忽略:独立 Hub 被调用时总是启动服务。
- 格式语法错误带行列信息;字段级类型错误不带(secret 解析后整体校验)。

### 启动环境变量 {#startup-environment-variables}

| 环境变量 | 默认值 | 说明 |
|----------|---------|-------------|
| `ARKFLOW_HUB_ADDRESS` | `127.0.0.1:8080` | Hub API 监听地址。 |
| `ARKFLOW_OPERATOR_TOKEN` | — | 供人工/管理员访问 API 的 operator token。支持 `role\|token` 凭据格式;参见 [Hub 认证](../operate/control-plane/overview.md)与[用户与凭证](../operate/control-plane/users-and-credentials.md)。 |
| `ARKFLOW_NODE_TOKEN` | — | Agent 向 Hub 认证使用的共享 token。 |
| `ARKFLOW_HUB_INSECURE_LOCAL` | 关 | 设为 `1`/`true`/`yes` 以放宽校验,仅限本地开发。 |
| `ARKFLOW_HUB_STORAGE` | — | 存储后端描述:SQLite 路径或 PostgreSQL URL。启用 HA 时必须设置,托管身份 API(用户/凭证/会话)也需要它。 |
| `ARKFLOW_HUB_TLS_CERT` / `ARKFLOW_HUB_TLS_KEY` | — | Hub 监听器的 TLS 证书与私钥路径。 |
| `ARKFLOW_HUB_HA_ENABLED` | 关 | 设为 `1`/`true`/`yes` 启用基于租约的 HA。要求设置 `ARKFLOW_HUB_STORAGE`;多实例 HA 使用 PostgreSQL URL(SQLite 路径仅记录一条仅供开发的警告)。 |
| `ARKFLOW_HUB_HA_LEASE_TTL_MS` | `15000` | 租约 TTL(毫秒);低于 1000 会被拒绝。 |
| `ARKFLOW_HUB_HA_HOLDER_ID` | — | 显式租约持有者身份;缺省时由主机名、PID 与启动时间自动生成。 |
| `ARKFLOW_HUB_HA_ADVERTISE_URL` | — | 带主机的绝对 `http(s)://` URL,Agent 通过它访问该 Hub 实例(多 Hub 发现)。 |
| `ARKFLOW_LOG_LEVEL` | `info` | Hub 日志级别(`trace`/`debug`/`info`/`warn`/`error`)。 |
| `ARKFLOW_OTEL_TRACING_ENABLED` | 关 | 设为 `1`/`true`/`yes` 以经 OTLP/HTTP-JSON 导出 Hub span。 |
| `ARKFLOW_OTEL_ENDPOINT` | `http://127.0.0.1:4318/v1/traces` | OTLP/HTTP-JSON span 导出端点。 |
| `ARKFLOW_OTEL_SERVICE_NAME` | `arkflow` | 导出 span 的 Resource `service.name`。当 collector 同时接收引擎 trace 时建议显式设置(如 `arkflow-hub`)。 |

`RUST_LOG` 环境变量设为可解析的 `EnvFilter` 指令(如 `RUST_LOG=info` 或
`arkflow=debug`)时,在引擎与 Hub 中都覆盖静态日志级别。优先级:
`RUST_LOG` > `ARKFLOW_LOG_LEVEL` > `logging.level`(文件)> 默认 `info`。
无法解析的值在 stderr 告警并回落到静态级别。Hub 的追踪行为见
[可观测性](../operate/observability.md#tracing)。

不在上述基础项内的身份相关变量——OIDC 联邦相关(`ARKFLOW_OIDC_*`,含
`ARKFLOW_OIDC_MANAGED_USERS_ONLY`)——见
[部署](../operate/control-plane/deploy.md#oidc-jwt-federation)。
托管用户目录及其凭证见[用户与凭证](../operate/control-plane/users-and-credentials.md)。

### 用法错误 {#usage-errors}

serve 路径的未知标志或多余参数、缺失的 `--config` 取值、不可读/无法解析/扩展名未知的配置文件,都会向 stderr 打印消息并以**退出码 `2`** 退出——未知参数绝不会静默启动 Hub。

### `migrate` 子命令

将 Hub 存储数据库从 SQLite 迁移到 PostgreSQL——把单实例 Hub(SQLite)迁移到多实例 HA(PostgreSQL)时走这条路。

```bash
arkflow-server migrate --from sqlite:<path> --to postgres://<url>
```

成功时打印逐表行数报告(`<table>: <rows> rows` ... `migration complete: <n> rows total`)并以 `0` 退出。

| 退出码 | 行为 |
|-----------|----------|
| `0` | 迁移完成。 |
| `2` | 用法错误:缺少 `--from`/`--to`,或 `--from` 不以 `sqlite:` 开头,或 `--to` 不以 `postgres://`/`postgresql://` 开头。 |
| 非零 | 迁移失败;错误打印到 stderr。 |

## 相关页面

- [组件清单](./component-inventory.md) — 由 CLI 读取的同一注册表生成。
- [顶层配置](./configuration.md) — `--config` 接受的内容。
- [IDE 自动补全](./ide-schema.md) — 在编辑器中使用 `arkflow schema`。
