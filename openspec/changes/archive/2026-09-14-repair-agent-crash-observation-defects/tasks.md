## 1. Agent 侧核心修复(crates/arkflow-server/src/agent.rs)

- [x] 1.1 `await_previous_teardown` 改为返回 `Option<Result<(), arkflow_core::Error>>`(join 超时为 `None`,正常结束为 `Some(result)`),同步更新现有 `wedged_previous_teardown_does_not_block_beyond_the_bound` 测试断言 `None` 返回
- [x] 1.2 `JobRuntime::start` 同代幂等分支收窄为「同代且 `!task.handle.is_finished()`」;移除该分支注释中已过时的措辞
- [x] 1.3 替换路径:teardown join 之后对被移除条目调用 `state.close()`(`Ok`/`Err`/`None` 三种结果均关闭,健康同代早退分支不受影响)
- [x] 1.4 替换路径拿到 `Some(Err(e))` 时 park `(job_id, 旧generation, Err(e.to_string()))` 进 `pending_observations`(`Ok(())` 与 `None` 不 park);注释说明锁定顺序(starts → pending_observations 无反向路径)

## 2. 确定性测试(crates/arkflow-server/src/agent.rs `mod tests`)

- [x] 2.1 同代 + 已死 kernel:构造已完成且返回 `Err` 的 `JoinHandle` 填入 `tasks`,调用同代 `start()`,断言不早退、park 含该崩溃观测、新 kernel 注册成功
- [x] 2.2 gen bump + 已死 kernel:更高代 `start()` 替换已死条目,断言崩溃观测被 park(不被静默吞掉)且新代正常启动
- [x] 2.3 优雅替换不产生崩溃观测:旧 kernel 以 `Ok(())` 退出后被 gen bump 替换,断言 park 为空
- [x] 2.4 健康 kernel 同代幂等不回归:运行中的 kernel 收到同代重投仍早退 `Ok`(确认 1.2 收窄未误伤设计意图)

## 3. 次要修复

- [x] 3.1 `build_agent_client` loopback 判定:去方括号后 `parse::<IpAddr>()` + `is_loopback()`,`localhost` 保留字面量匹配;补 127.0.0.2 / [::1] / 非回环地址的解析单测
- [x] 3.2 `tests/fleet_readiness.rs`:`staircase_ci`/`soak_ci` 入口检查 `ARKFLOW_SKIP_FLEET=1`,命中打印 WARN 后返回;文件头注释写明开关用途与"仅限本地"

## 4. 验证与收尾

- [x] 4.1 `cargo test -p arkflow-server --lib` 全绿(含新增单测)
- [x] 4.2 `cargo clippy -p arkflow-server --all-targets` 无新告警
- [x] 4.3 `cargo test -p arkflow-server --test two_node_job_smoke` 集成回归通过
- [x] 4.4 `cargo test -p arkflow-plugin --test docs_inventory_snapshot` 与 `cargo test -p arkflow --test examples_validate` 确认无文档面影响(预期零改动,跑一遍兜底)
