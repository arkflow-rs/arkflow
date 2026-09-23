# Tasks: add-tls-support-matrix

## 1. 测试

- [ ] 1.1 pulsar common 单测：pulsar:// 与 pulsar+ssl:// URL 校验均通过、非法 scheme 拒绝

## 2. 文档

- [ ] 2.1 部署文档新增 TLS 支持矩阵节（全部网络组件逐一列出，en/zh）；pulsar 组件页补 pulsar+ssl:// 说明

## 3. 验证与收尾

- [ ] 3.1 `cargo test --workspace --all-targets` 连续 2 轮全绿；clippy 无新告警；`pnpm docs:check` 通过
- [ ] 3.2 `openspec validate add-tls-support-matrix` 通过；归档、更新 PLANNING.md、提交推送
