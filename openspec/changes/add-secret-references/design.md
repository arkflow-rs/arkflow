## Context

配置物化有两个入口：`EngineConfig::from_file`（`crates/arkflow-core/src/config.rs:211`，文件路径：CLI 启动、`--validate`）与 `ConfigCandidate::parse`（`crates/arkflow-core/src/configuration.rs:128`，Hub 校验端点与 Agent `apply_configuration` 共用）。两者都是「文本 → serde 直接反序列化 → EngineConfig」，字符串值原样进入组件。Kafka SASL/SSL（#1246）落地后，`security.sasl.username/password`、`tls.key/key_password` 等敏感值只能明文内联。

约束：仓库单执行内核、surgical change、无新增依赖；`from_file` 现有 YAML 错误带行列号，不能整体降级。

## Goals / Non-Goals

**Goals:**
- 字符串值支持 `${env:VAR}`、`${env:VAR:-default}`、`${file:/path}`，物化时解析；`$${` 转义字面 `${`。
- 错误只含引用表达式与配置路径，不含解析明文；Hub 存储内容保持引用原文。
- 三种格式（YAML/JSON/TOML）一套语义。

**Non-Goals:**
- Hub `secret:` scheme / Secret Manager / 运行时轮换 / KMS 集成（阶段 4）。
- Hub 侧宽松校验（跳过不可解析引用）。

## Decisions

1. **语法形态：子串扫描，引用可嵌入更大字符串**（`password: "${env:PW}"` 与 `dsn: "host=${env:H};user=${env:U}"` 均可）。备选「整值匹配才解析」更保守但反直觉且无法覆盖 dsn 拼接场景。行业先例（Docker Compose、Benthos）均为子串插值 + 转义。
2. **解析时机：Value 树遍历，非原文文本替换**。文本替换会让含 `#`/换行/引号的密钥内容破坏 YAML 结构（PEM 必然如此）。实现：文本先解析为格式 Value → 转 `serde_json::Value`（serde round-trip，三种格式共用一个遍历器）→ 递归解析字符串值 → `serde_json::from_value::<EngineConfig>`。
3. **双通道保住错误行号**：原文不含 `${`/`$${` 时走现有直接反序列化路径（行列号错误不变，覆盖存量配置 100% 场景）；含引用时才走 Value 通道（该通道类型错误无行列号，是可接受的局部退化，错误信息补 JSON 路径定位）。
4. **已知 scheme 才解析；未知 `${...}` 原样保留**。理由：滚动升级窗口内，新版 Hub 下发的含 `${secret:...}` 配置必须能被旧版 Agent 解析（同仓库 `bearer_auth` legacy 回退的兼容哲学）。拼写错误的代价（如 `${envs:X}` 静默原样）由文档与 `--validate` 输出缓解（validate 会回显已解析配置的结构性错误，引用未解析时可肉眼发现）。
5. **`env:` 默认值语义对齐 Compose**：`:-` 在未设置**或为空**时生效；`${env:VAR:-}` 显式允许空串。`file:` 无默认值语法。
6. **`file:` 尾部换行剥离**：去除内容尾部所有 `\n`/`\r`（Docker secrets 惯例，密码文件编辑器普遍补尾换行）；PEM 场景尾换行无害。
7. **解析结果不重扫**：`${env:A}` 的值里若含 `${...}` 保持字面。防注入、防递归，行为可预测。
8. **作用域：仅字符串值**（递归含 array/map 元素），键名与非字符串值不动。所有组件字段一视同仁——不做按字段白名单（SQL/VRL/Python 程序文本中出现 `${env:X}` 的误伤概率极低，需要字面量时用 `$${`，同 Compose 的取舍）。
9. **错误模型**：复用 `Error::Config`，消息格式 `Failed to resolve secret reference at <json-path>: <原因> (reference: <引用原文>)`；`file:` 的 IO 错误只带 kind 与路径，不带内容。

## Risks / Trade-offs

- [Value 通道丢失行列号（仅含引用的配置）] → 双通道 + 错误带 JSON 路径；无引用配置零影响。
- [Hub 校验端点会严格解析引用，多节点部署时 Hub 侧需能解析（环境变量存在）] → 文档明示约束；单进程部署（主流形态）无影响。真正的多节点密钥治理属阶段 4。
- [serde round-trip 对 YAML 非字符串键报错提前] → EngineConfig 结构本就要求字符串键，现有此类配置已在反序列化失败，仅错误文案变化。
- [明文进入进程内存后经 tracing 泄漏] → 本变更范围内没有任何新增日志点；错误信息不含值（有测试断言）。

## Migration Plan

纯增量：存量配置（不含 `${`）行为逐字节不变；新语法可选采用。回滚 = revert 提交。

## Open Questions

无。
