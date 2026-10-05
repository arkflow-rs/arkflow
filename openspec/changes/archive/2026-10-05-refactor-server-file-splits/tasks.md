# Tasks: refactor-server-file-splits

- [x] 1. lib.rs 源码区 → `src/api/`（mod.rs 承载三个 router builder + 共享 state；handler 按路由域分模块；测试 → `api/tests.rs`），公共入口不变
- [x] 2. agent.rs → `src/agent/`（config/checkpoint/kernel/resources/session/commands/mod/tests），字符串命令协议不动，`crate::agent::X` 路径不变
- [x] 3. 验证：`cargo test -p arkflow-server` 全绿 + clippy + fmt
- [x] 4. openspec validate + 归档
