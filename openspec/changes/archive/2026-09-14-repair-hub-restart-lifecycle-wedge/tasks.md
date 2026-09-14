## 1. Hub 侧启动恢复

- [x] 1.1 `hub.rs` 新增 `restore_persisted_operations()`:`list_operations(None)` → 反序列化 `operation_json` → 插入内存映射(去重、`MAX_OPERATIONS` 封顶、不可解析行 WARN 跳过),返回恢复条数
- [x] 1.2 `serve_hub` 在绑定监听前调用(仅 `has_storage()` 时),恢复条数打 info 遥测

## 2. Agent 侧 teardown 有界化

- [x] 2.1 `JobRuntime::start` 等待前内核 handle 加 `KERNEL_TEARDOWN_JOIN_TIMEOUT`(10s):超时 WARN(含 job_id)后继续新 start;正常路径行为不变

- [x] 2.3 实施新增(fleet harness 排障发现):`build_agent_client` 对 loopback Hub 禁用系统代理(本机拦截代理致 register 400)并加 connect/total 超时(死监听 backlog 连接致 agent 永久挂起——`compute-node-agent` spec「Hub 暂时不可用 → 有界退避」的缺口)
- [x] 2.4 实施新增:同代重复 `job_start` 幂等成功(不再取消/重启健康内核);`deliver_result` 投递失败仍缓存终态结果(spec 重放路径缺口——此前投递 401 会丢结果直至预算耗尽)

## 3. 测试

- [x] 3.1 Hub 测试:写入持久操作(Succeeded + Queued)→ 重建 `Hub::with_storage` + `restore_persisted_operations` → 断言内存映射含两条、reconcile 不为 Succeeded 的 (node, job, generation) 重复派发
- [x] 3.2 Agent 测试:伪造前内核 handle(挂起不退出)→ 冗余 start 在超时后仍能完成或终态失败,不再无限阻塞
- [x] 3.3 集成回归:`two_node_job_smoke` 不回归;`fleet_readiness::soak_ci`(重启场景)通过——本 change 的验收标志
- [x] 3.4 全量 `cargo test --workspace --all-targets` 与 `cargo clippy --workspace --all-targets` 通过(防 fmt 搅动:只提交本 change 文件)

## 4. 收尾

- [x] 4.1 回到 `verify-hub-production-readiness` 继续 apply:soak_ci/staircase_full/soak_one_hour,容量数字回填 PLANNING.md
- [x] 4.2 `/opsx:verify` 后 `/opsx:archive`
