## 1. 三个有界等待

- [x] 1.1 `kernel_handle.rs`：round collect select 增加 deadline 臂（默认 `CHECKPOINT_ROUND_TIMEOUT = 10min`；句柄 `AtomicU64` 毫秒 + `#[cfg(test)]` 注入口；超时错误点名时长）
- [x] 1.2 `task.rs`：sink 写包 `SINK_WRITE_TIMEOUT = 5min` timeout，超时映射 Fatal（错误点名时长；ack 按既有失败路径结算）
- [x] 1.3 `barrier.rs`：`snapshot_state` join 包 `SNAPSHOT_TIMEOUT = 5min` timeout（后台 blocking 结果迟到即丢弃，无副作用）

## 2. 测试

- [x] 2.1 round 超时：注入极小 deadline + 永不报告的参与者 → 显式超时错误（含时长字样），上一有效 checkpoint 不受影响
- [x] 2.2 sink 超时：挂死 sink（await 永不完成）驱动真实 chain → Fatal 错误含 "timed out"，chain 终止（shutdown 路径随之返回）
- [x] 2.3 快照超时：挂死 StateBackend 的 `snapshot_state` → 显式超时错误
- [x] 2.4 迟到报告吸收：既有 stale-report 吸收路径（kernel_handle.rs 的 "ignoring stale checkpoint report" 分支）被 wedged_round 测试的取消路径间接覆盖（chain 取消后 finished 通知先于报告到达，drain 分支按既有语义吸收）；专项目测试并入后续 barrier 优先级 change

## 3. 验证

- [x] 3.1 `cargo test -p arkflow-core --lib executor` 全绿
- [x] 3.2 `cargo test --workspace --all-targets` 全绿
- [x] 3.3 `cargo clippy --workspace --all-targets` 零新增告警
- [x] 3.4 `openspec validate fix-checkpoint-round-timeouts` 通过
