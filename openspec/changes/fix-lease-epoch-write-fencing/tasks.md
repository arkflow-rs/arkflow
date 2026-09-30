## 1. 存储层围栏基座

- [x] 1.1 `StorageError::StaleLeader { claimed_epoch, current_epoch }`（thiserror）+ `hub_problem` 映射 503 `stale_leader`
- [x] 1.2 `StorageBackend::current_lease_epoch()` trait 方法 + SQLite/Postgres 实现 + `ControlPlaneStore` 分派臂
- [x] 1.3 `StorageCommand::Fenced { claimed_epoch, command: Box<StorageCommand> }` 变体；actor 循环体提为 `dispatch(store, command)` 自由函数；`Fenced` 臂执行前校验（None/一致→执行，不一致→nack StaleLeader，读失败→nack 错误）
- [x] 1.4 机械生成 `StorageCommand::nack(error)`（61 个带 response 变体各一行臂；穷尽性由编译器保证）

## 2. 句柄与接线

- [x] 2.1 `StorageActor` 句柄增加 `leadership_epoch: Arc<AtomicU64>`（start 创建、`pub fn leadership_epoch()` 取引用）与 `send_fenced`（发送时捕获声明值、包 Fenced 信封）
- [x] 2.2 38 个变更类 wrapper 方法改经 `send_fenced`（脚本白名单改造：20 读 + 3 租约直通，双向断言 61 全覆盖无交集）
- [x] 2.3 `run_election_tick` 计算出新 Leadership 后同步句柄 epoch（Leader{epoch}→epoch，其他→0）；promote→recovery 顺序下 recovery 写携带新 epoch

## 3. 测试

- [x] 3.1 契约测试（in-memory 后端 actor 级：无租约直通/claim 匹配通过/standby 与旧 leader 双向拒绝/接管后无持久化副作用/新 leader 恢复写入；current_lease_epoch 断言；Postgres 的 current_lease_epoch 为单查询，随既有 PG 门控套件补）:acquire(A)→epoch1 后 claimed=1 写通过、claimed=0 写被拒；模拟过期 takeover(B)→epoch2 后 claimed=1 拒 / claimed=2 通；`current_lease_epoch` 与租约返回一致
- [x] 3.2 actor 级测试：句柄 `leadership_epoch` 写入后 wrapper 正常路径不受影响；无租约行时任意 claimed 直通；租约三操作经句柄不受围栏影响
- [x] 3.3 `hub_problem` 映射断言（StaleLeader→503 stale_leader）

## 4. 验证

- [x] 4.1 `cargo test -p arkflow-server` 全绿
- [x] 4.2 `cargo test --workspace --all-targets` 全绿
- [x] 4.3 `cargo clippy --workspace --all-targets` 零新增告警
- [x] 4.4 `openspec validate fix-lease-epoch-write-fencing` 通过
