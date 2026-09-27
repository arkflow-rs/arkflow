# Tasks: add-job-rescale

- [x] 1.1 `JobSpec.rescale` 显式声明字段（serde default false）。
- [x] 1.2 `RescaleContext`：routing_key_bytes（窗口/Stateful 双编码白名单）+ redistribute（key_group → 新任务命名空间）。
- [x] 1.3 守卫分支 + `restore_local_snapshot` 重分布接线。
- [x] 1.4 测试：stateful/窗口键重分布按 key-group 落位、未知编码拒绝、守卫默认不变。
- [x] 1.5 `cargo test -p arkflow-core` 全绿（509）。
