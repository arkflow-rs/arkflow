# Tasks: add-secret-references

## 1. 引用解析模块

- [x] 1.1 新建 `crates/arkflow-core/src/secret.rs`：`contains_reference(&str) -> bool`（识别 `${`/`$${` 序列）；在 `lib.rs` 挂载模块
- [x] 1.2 实现字符串扫描器：`$${` → 字面 `${`；已知 scheme（`env:`/`file:`）子串解析；未知 `${...}` 原样保留；解析结果不重扫
- [x] 1.3 实现 `env:` 语义：未设置报错；`:-` 默认值在未设置或空串时生效；`${env:VAR:-}` 允许显式空串
- [x] 1.4 实现 `file:` 语义：读文件并剥离尾部 `\n`/`\r`；IO 错误只带路径与错误类别
- [x] 1.5 实现 `resolve_value(&mut serde_json::Value, path)` 递归遍历（map/array/字符串），错误携带 JSON 路径与引用原文、不含解析值
- [x] 1.6 模块单测：全部语法角落（转义、未知 scheme、默认值三态、子串嵌入、嵌套结构、不重扫）与错误路径（含「错误信息不含明文」断言）

## 2. 物化链路接入

- [x] 2.1 `EngineConfig::from_file`：双通道——原文无 `${`/`$${` 走现有直接反序列化（行列号不变）；含引用走 Value 通道（解析为格式 Value → `serde_json::Value` → 解析 → `from_value`）
- [x] 2.2 `ConfigCandidate::parse`：同一双通道接入（Hub 校验与 Agent apply 共用）
- [x] 2.3 集成单测：YAML/JSON/TOML 三格式含引用的物化；无引用配置的行列号错误回归；env/file 错误路径

## 3. 文档与示例

- [x] 3.1 配置参考文档新增 secret 引用语法说明（语法表、转义、约束、错误语义；yaml 块按规范加 validate 分类标记）
- [x] 3.2 新增 `examples/kafka_input_sasl_ssl_secrets.yaml`（用 `${env:...:-default}` 保证离线可校验），注册 `docs/reference/example-manifest.json`
- [x] 3.3 确认 `config-schema.json`/inventory 无需变更（字段仍为 string）；若有漂移用 `ARKFLOW_REGENERATE_DOCS=1` 重新生成

## 4. 全量验证

- [x] 4.1 `cargo test --workspace --all-targets` 全绿
- [x] 4.2 `cargo clippy --workspace --all-targets` 无新告警
- [x] 4.3 `pnpm docs:check` 通过
- [x] 4.4 对照 specs/secret-references 场景清单逐条核对；`openspec validate add-secret-references` 通过
