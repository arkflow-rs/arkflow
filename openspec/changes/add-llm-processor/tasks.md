# Tasks: add-llm-processor

## 1. llm processor 实现

- [ ] 1.1 新建 `crates/arkflow-plugin/src/processor/llm.rs`：`LlmProcessorConfig`（serde default 齐全）+ builder 构建期校验（必填非空、concurrency ≥ 1）+ metadata schema 注册；`processor/mod.rs` init 接线
- [ ] 1.2 实现列提取与消息构造：Utf8/LargeUtf8 校验、null 行报错、`{{value}}` 模板替换、system/user 消息三态、temperature/max_tokens 缺省省略
- [ ] 1.3 实现有界并发请求：`futures_util` `stream::buffered(concurrency)` 保序回填、Bearer 鉴权、headers、loopback 代理绕过、非 2xx/缺 content 错误（状态码 + 截断响应体）
- [ ] 1.4 mock server 测试：行序回填与原始列保持、消息三态请求体断言、可选参数省略断言、并发保序（延迟扰动）、鉴权头/无鉴权、429 透传、缺 content、null 输入、空 batch 直通、构建期校验

## 2. 文档与示例

- [ ] 2.1 组件文档页 `docs/docs/components/2-processors/llm.md`（components front matter + validate 分类标记 yaml 块，链接用绝对 /docs 路由）
- [ ] 2.2 双 README `README_COMPONENTS:processor` 清单各加一条
- [ ] 2.3 示例 `examples/llm_rewrite_pipeline.yaml`（memory → json_to_arrow → llm → stdout，api_key 用 `${env:...:-}`），注册 example-manifest
- [ ] 2.4 `ARKFLOW_REGENERATE_DOCS=1` 重新生成 inventory/config-schema 与 component-inventory.md

## 3. 全量验证与归档

- [ ] 3.1 `cargo test --workspace --all-targets` 全绿；`cargo clippy --workspace --all-targets` 无新告警；`pnpm docs:check` 通过
- [ ] 3.2 对照 specs/llm-processor 场景逐条核对；`openspec validate add-llm-processor` 通过
- [ ] 3.3 同步主 spec `openspec/specs/llm-processor/spec.md`、归档 change、更新 PLANNING.md 7.3-3 进展、提交
