# Tasks: add-milvus-output

## 1. milvus output 实现

- [ ] 1.1 新建 `crates/arkflow-plugin/src/output/milvus.rs`：`MilvusOutputConfig`（serde default 齐全）+ builder 构建期校验 + metadata schema 注册；`output/mod.rs` init 接线
- [ ] 1.2 实现行对象装配：向量列提取（Fixed/List Float32）、payload 打包（arrow-json 逐行 JSON，可禁用）、id 列（Int64/Int32/Utf8，null 报错；缺省省略 id 键）
- [ ] 1.3 实现 REST v2 upsert：`POST {url}/v2/vectordb/entities/upsert`、Bearer 鉴权、headers、loopback 代理绕过、HTTP 2xx + code!=0 判败、非 2xx 透传
- [ ] 1.4 mock server 测试：路径与请求体形状（全字段/auto-id/禁 payload/多行）、code!=0 判败、code=0 成功、非 2xx、鉴权头两态、null 向量、空 batch、构建期校验

## 2. 文档与示例

- [ ] 2.1 组件文档页 `docs/docs/components/3-outputs/milvus.md`（最低版本、DDL/dynamic field 说明、validate 分类标记、绝对路由链接）
- [ ] 2.2 双 README `README_COMPONENTS:output` 清单各加一条
- [ ] 2.3 示例 `examples/embedding_milvus_pipeline.yaml`，注册 example-manifest
- [ ] 2.4 `ARKFLOW_REGENERATE_DOCS=1` 重新生成 inventory/config-schema 与 component-inventory.md

## 3. 全量验证与归档

- [ ] 3.1 `cargo test --workspace --all-targets` 全绿；`cargo clippy --workspace --all-targets` 无新告警；`pnpm docs:check` 通过
- [ ] 3.2 对照 specs/milvus-output 场景逐条核对；`openspec validate add-milvus-output` 通过
- [ ] 3.3 同步主 spec `openspec/specs/milvus-output/spec.md`、归档 change、更新 PLANNING.md（milvus 完成 + OTel 评估结论）、提交
