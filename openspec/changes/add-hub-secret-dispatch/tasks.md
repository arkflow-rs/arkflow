# Tasks: add-hub-secret-dispatch

## 1. 预解析实现

- [x] 1.1 `secret.rs`：`resolve_candidate_payload`（仅 secret: 解析、format→json、无引用返回 None）+ 单测（yaml+env 混合、无引用、纯 secret、解析失败）
- [x] 1.2 `hub.rs` `reconcile_rollouts`：payload 获取后经预解析，失败置 target failed（错误含引用）
- [x] 1.3 hub 集成测试：含 secret 引用的版本分发 → intent payload 已解析且 env:/file: 保持；密钥未设置 → target failed

## 2. 文档与全量验证

- [x] 2.1 secret-references 文档节（en/zh）补充多节点分发语义
- [x] 2.2 `cargo test --workspace --all-targets` 全绿；clippy 无新告警；`pnpm docs:check` 通过
- [x] 2.3 对照场景核对；`openspec validate add-hub-secret-dispatch` 通过；同步主 spec、归档、更新 PLANNING.md、提交
