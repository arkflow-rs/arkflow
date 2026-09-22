## Why

企业集成安全方向刚落地 Kafka SASL/SSL（`crates/arkflow-plugin/src/kafka_security.rs`，2026-09-23 合入 #1246），但 SASL `username`/`password` 与 TLS 私钥（`key`、`key_password`）只能明文写进 YAML 配置文件——配置文件会被提交到 Git、由 Hub 存储并在 Console 中展示，明文凭据随之扩散。PLANNING.md 7.3-2 已将「Secret 引用机制」列为方向⑤的延伸项，为 Hub 阶段 4 Secret Manager 铺路。当前引擎没有任何机制（全仓库无 `env:`/`file:` 引用解析，`EngineConfig::from_file`（`crates/arkflow-core/src/config.rs:211`）与 `ConfigCandidate::parse`（`crates/arkflow-core/src/configuration.rs:128`）直接把文本反序列化为 `EngineConfig`，字符串值原样进入组件）。

## What Changes

- 配置文件（YAML/JSON/TOML）中的字符串值新增三种 secret 引用语法，在配置物化为 `EngineConfig` 时解析：
  - `${env:VAR}` — 读取环境变量，未设置则报错；
  - `${env:VAR:-default}` — 读取环境变量，未设置或为空时使用默认值；
  - `${file:/path}` — 读取文件内容（去除尾部换行），文件不可读则报错；
- 转义语法 `$${` 表示字面 `${`，含未知 scheme 的 `${...}` 保持原样（向前兼容未来 `secret:` scheme）；
- 解析只作用于字符串值（递归覆盖嵌套 map/array），不改动键名与非字符串值；
- 解析失败的错误信息只包含引用表达式与配置路径（如 `streams[0].input...sasl.password`），绝不包含解析出的明文；
- Hub 存储与 Console 展示的配置内容保持引用原文，明文只在消费端（运行进程）内存中出现。

## Capabilities

### New Capabilities

- `secret-references`: 配置字符串值的 secret 引用语法、解析时机、错误语义与不泄漏保证。

### Modified Capabilities

<!-- 无既有能力的需求级变更：configuration-management 约束配置版本存储格式，本变更不改变存储格式（引用作为普通字符串存储）。 -->

## Impact

- `crates/arkflow-core/src/config.rs`：`EngineConfig::from_file` 解析链路增加引用解析步骤；无引用的配置保持现有直接反序列化路径（保留 YAML 行号级错误信息）。
- `crates/arkflow-core/src/configuration.rs`：`ConfigCandidate::parse` 增加同一解析步骤（Hub 校验端点与 Agent `apply_configuration` 共用）。
- 新增 `crates/arkflow-core/src/secret.rs`（或同层模块）：引用扫描与解析实现 + 单测。
- 无新增外部依赖（`std::env`/`std::fs` 足够）。
- 文档：配置参考页新增语法说明；新增示例 YAML（注册 manifest，标注 `validate: false` 或用默认值使其可离线校验）。
- `config-schema.json` 不变（字段仍为 string）。

## Non-goals

- 不实现 Hub `secret:` scheme 与中心化 Secret Manager（Hub 阶段 4 另行立项）；本变更只保证语法向前兼容。
- 不做 Hub 侧「跳过未解析引用」的宽松校验模式；引用在配置物化处严格解析（单进程部署 Hub/Agent 同机，环境变量可用；多节点场景要求执行节点可解析，约束写入文档）。
- 不做加密配置文件、KMS/Vault 集成、运行时密钥轮换。
- 不改动组件级配置结构（`security` 等字段形状不变，仅值来源更灵活）。
