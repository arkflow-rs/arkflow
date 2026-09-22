# Tasks: add-secret-scheme

## 1. 解析器扩展

- [ ] 1.1 `secret.rs`：`secret:` scheme（`ARKFLOW_SECRET_<NAME>` 逐字映射、`:-` 默认值、未设置报错含引用与路径、不含值）
- [ ] 1.2 单测：secret 解析、默认值三态、未设置错误路径（不泄漏断言）、与 env:/file: 混用

## 2. 文档与全量验证

- [ ] 2.1 secret-references 文档节（en/zh）补充 `secret:` scheme 与命名空间约定
- [ ] 2.2 `cargo test --workspace --all-targets` 全绿；clippy 无新告警；`pnpm docs:check` 通过
- [ ] 2.3 对照场景核对；`openspec validate add-secret-scheme` 通过；同步主 spec、归档、更新 PLANNING.md、提交
