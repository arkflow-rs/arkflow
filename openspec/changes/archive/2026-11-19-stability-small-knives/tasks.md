# Tasks: Stability Small Knives

## 1. 控制面 API（jobs.rs / mod.rs / rollouts / approvals）

- [x] 1.1 mutation 响应脱敏：jobs.rs 全部 mutation 端点（create/update/upgrade/rollback/restore/recovery/trigger/desired-state/actions）响应记录过 `redact_record_fields`；补 mutation 响应脱敏断言测试
- [x] 1.2 GET /jobs 分页：`PageQuery` + `page_items` 信封化，rollouts 与 config approvals 列表同样处理；补分页参数测试（默认页、越界页、page_size clamp）
- [x] 1.3 console 适配：api.ts `jobs()`/rollouts/approvals 读 `.items`，features 调用点随之更新；console 检查通过
- [x] 1.4 每请求超时：local 与 hub 两处栈加 `HandleErrorLayer` + `TimeoutLayer`，Elapsed → `problem(408, "request_timeout")`；补超时响应形状测试
- [x] 1.5 docs 同步：控制面 API 文档（EN + zh-Hans）记录分页信封、BREAKING 变更与超时行为

## 2. 存储与 HA（postgres_methods / nodes）

- [x] 2.1 PG `renew_hub_lease` 改单条 `UPDATE ... RETURNING epoch`；Lost 分支语义不变
- [x] 2.2 契约测试补两后端并发续约场景（SQLite 现状 + PG 新实现同断言）
- [x] 2.3 hub/nodes.rs register：`get_node_labels().await` 移出 nodes 写锁。不阻塞属性为结构性保证（存储层无注入缝），label 重同步正确性由既有 node_labels API 测试钉扎

## 3. 内核与 WAL（core）

- [x] 3.1 `Wal` 失败自由列表：PerEntry 追加 store 失败归还 seq、append 先 pop 自由列表再 `fetch_add`；补"失败归还后重用且不双重分配"单测
- [x] 3.2 Job 深校验临时 state backend：`validate_local_job` 不再打开真实 durable root（临时目录同型 with_temporary）；补"运行中 Job 状态库不被校验加锁"测试
- [x] 3.3 agent kernel stop/stop_all：join 加 `KERNEL_TEARDOWN_JOIN_TIMEOUT` 有界等待，超时 warn 后 detach；补楔死内核不阻塞后续 start 的测试

## 4. 插件（kafka output）

- [x] 4.1 kafka output close()：30s flush 移入 `spawn_blocking`；确认现有 close 测试仍绿

## 5. 门禁与收尾

- [x] 5.1 `cargo fmt` + `cargo clippy --workspace --all-targets` 全绿
- [x] 5.2 `cargo test --workspace --all-targets` 全绿（含 docs_inventory_snapshot 如注册有变）（#1372 合并树上 CI 全绿，本地复验 fmt/clippy/docs:check）
- [x] 5.3 `pnpm docs:check` 全绿（EN/zh 链接、README 一致性、YAML 分类）
- [x] 5.4 openspec verify + 归档准备（归档 PR 另行）
