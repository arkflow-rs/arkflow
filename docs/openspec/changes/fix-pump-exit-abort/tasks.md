# Tasks: fix-pump-exit-abort

## 1. 实现

- [ ] 1.1 `pump_edge` 退出路径统一：`pending.abort_all()` 从 `if result.is_err()` 改为无条件调用
- [ ] 1.2 验证编译通过且既有测试不受影响
