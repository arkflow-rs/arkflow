## 1. 索引唯一性

- [x] 1.1 `RuntimeManager` 新增 `next_index: AtomicU64`（初值 0）；`register` 从 `fetch_add` 取索引替代 `entries.len()`
- [x] 1.2 测试：register→remove→register 断言索引严格递增（b={≠}c 且 c>b）——replace_config 的碰撞面由此消除

## 2. stop/restart 竞态

- [x] 2.1 `restart` 第一个锁块合并"置 Restarting + token 取消 + handle 取出"为一个原子块
- [x] 2.2 `stop` 的 match 增加 `Restarting => return Ok(())`（restart 的内部 stop 阶段已履行 stop 意图，文档说明）
- [x] 2.3 测试：并发 stop + restart 锁级测试——stop 在 Restarting 时 early-return Ok、restart 原子取 handle 完成自身周期（start 因无插件注册而 Failed，但绝不出现 stop Ok + Running 复活形态）

## 3. 验证

- [x] 3.1 `cargo test -p arkflow-core` 全绿
- [x] 3.2 `cargo test --workspace --all-targets` 全绿
- [x] 3.3 `cargo clippy --workspace --all-targets` 零新增告警
- [x] 3.4 `openspec validate fix-runtime-manager-races` 通过
