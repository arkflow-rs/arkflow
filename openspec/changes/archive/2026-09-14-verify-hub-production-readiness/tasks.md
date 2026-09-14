## 1. 舰队 harness

- [x] 1.1 新建 `crates/arkflow-server/tests/fleet_readiness.rs`:Hub 侧 fixture(loopback `TcpListener:0` + `serve_hub` 任务 + 共享 `ControlPlaneStore`,HubConfig 参数注入)与 agent 侧 fixture(N 个真实 `agent::run` 任务,NodeAgentConfig 间隔/TTL 注入,共享 reqwest Client),参数常量集中于文件头部
- [x] 1.2 断言辅助:命令全部终态(轮询 `hub.operation`/`commands` 超时判定)、节点注册表全量可查询、有界表 SQL COUNT 收敛断言(operations 终态/processed outbox/terminal attempts/audit,允许 bound + 单轮余量)
- [x] 1.3 RSS 采样器:`#[cfg(target_os = "linux")]` 读 `/proc/self/statm`,其余平台回退 `ps -o rss= -p <pid>`;不可用时返回 None 并跳过斜率断言(记录而非失败)

## 2. 测试

- [x] 2.1 `staircase_ci`(N=8→32 阶梯):每梯级 Job churn → 全部命令终态 + 注册表可查询 + 历史表收敛;记录各梯级派发 p50/p99 与 RSS(输出到测试日志)
- [x] 2.2 `soak_ci`(N=16,session_ttl=2s,~2min):持续 churn + Hub 重启风暴 ×3(同 store 重建)+ agent 全灭重生 ×1;断言恢复收敛、p99 无漂移、RSS 斜率 ≈ 0(余量以首轮实测校准并注释依据)
- [x] 2.3 `staircase_full`(#[ignore],N=25→64→128→256):同 2.1 断言,产出容量数字
- [x] 2.4 `soak_one_hour`(#[ignore],N=64,session_ttl=5s,≥60min):重启风暴 ×10、agent 重生 ×3、checkpoint 风暴;断言同 2.2

## 3. 验证与文档

- [x] 3.1 全量 `cargo test --workspace --all-targets` 与 `cargo clippy --workspace --all-targets` 通过(确认 CI 预算 < 3 分钟;只提交本 change 文件,防 fmt 搅动)
- [x] 3.2 手动运行 `#[ignore]` 完整版,把容量数字(knee 位置、p99@256、RSS 平台)回填 PLANNING.md 5.2-9 与「下一步」注记
- [x] 3.3 `/opsx:verify` 校验后 `/opsx:archive` 归档

> **⚠ 实施发现(2026-09-13,soak_ci 抓获)**:Hub 重启会楔死 job 生命周期——`Hub::with_storage` 不从 SQLite 恢复终端态 skip 记忆与操作映射(`hub.rs:506-510`),重启后 reconcile 对 agent 已在运行的 job 重新派发 `job_start`;agent 侧 `JobRuntime::start`(`agent.rs:404-427`)先取消并 await 旧内核,而旧内核 WAL-safe teardown 在负载下挂起 → start 守护互斥量被永久持有 → 后续一切 start 阻塞 → 租约过期 → 重试预算耗尽 → 操作永久停在 Dispatched/Queued。staircase(无重启)已通过;soak 的重启场景在此缺陷修复前无法关闭。详见 2.2 任务注记与修复 change。
