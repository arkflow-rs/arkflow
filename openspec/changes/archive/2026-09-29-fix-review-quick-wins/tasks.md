## 1. remote 投递去重竞态修复

- [x] 1.1 `executor/remote.rs` 入站投递路径实现 per-route-key 投递锁（`Arc<tokio::sync::Mutex<()>>`，横跨"查 delivered → send_async → 更新 delivered"），`delivered.insert` 改为单调 max 更新；锁条目随会话清理一并移除
- [x] 1.2 回归测试：同一会话键两条连接先后服务（旧投递在本地通道反压中、新连接重放同一 seq）——该 seq 至多投递一次，且迟到的旧投递完成不把水位改小（后续更高 seq 重放仍被去重）

## 2. Kafka L3 行级 topic 校验

- [x] 2.1 核实 kafka input 对外产出 `__meta_topic` 列（`input/kafka.rs` 元数据装配处），确认列名常量
- [x] 2.2 `output/kafka.rs::transactional_offsets_for_batches` 增读可选 `__meta_topic` 列：列存在时逐行校验等于 group topic，任何异源行（含位点元数据非空）使 write_batch 以显式数据错误失败；列缺失维持现状
- [x] 2.3 单测：异 topic 行报错（错误信息含冲突 topic 名）、同 topic 行通过、无 `__meta_topic` 列时维持既有推导

## 3. SQL processor context 池泄漏

- [x] 3.1 `processor/sql.rs` 归还改为 RAII guard（Drop 兜底释放，成功路径显式 release），覆盖临时表路径与缓存计划路径的全部 `?` 早退
- [x] 3.2 `context_pool.rs::acquire` 加 10s 截止时间：超时返回带诊断的错误（池大小/占用说明），并在超时时打 warn 日志
- [x] 3.3 单测：连续 N>N_pool 次处理错误后，后续批次仍能正常获取 context 执行（池不被耗尽、不忙等挂死）

## 4. HTTP input bind 失败可见

- [x] 4.1 `input/http.rs`：bind 从 spawn 任务移入 `connect()` 直接 await（失败同步返回、去掉 `expect` panic 路径），accept 循环错误经既有消息通道送 Err 使 read 可见，connected 只在 bind 成功后置位
- [x] 4.2 单测：端口被占用时 `connect()` 返回明确 bind 错误且无 panic

## 5. join 容量驱逐可观测

- [x] 5.1 `executor/join.rs` 容量驱逐（inner 与 outer 侧）加节流 warn（1s 窗口累计，字段：侧别、key 截断 64 字节、当前深度、`max_per_key`、窗口累计次数）；watermark 逐出不日志
- [x] 5.2 单测：`max_per_key` 超限时驱逐仍发生且行为不变（缓冲不超上限），日志路径可被测试捕获（tracing subscriber 断言 warn 与关键字段）

## 6. 文档（en + zh-Hans 双树）

- [x] 6.1 Kafka output 组件文档（`docs/docs/components/output/kafka.md` 与 zh 对应页）exactly-once 节补 L3 fail-closed 语义：批次混入非 group topic 的 Kafka 位点行时 write_batch 显式失败
- [x] 6.2 join 相关文档页补容量驱逐可观测性说明（warn 字段与节流语义）
- [x] 6.3 http input 组件文档补 bind 失败 fail-fast 行为说明

## 7. 验证

- [x] 7.1 针对性测试通过：`cargo test -p arkflow-core`（remote/join）与 `cargo test -p arkflow-plugin`（kafka/sql/http）
- [x] 7.2 `cargo test --workspace --all-targets` 全绿
- [x] 7.3 `cargo clippy --workspace --all-targets` 零新增告警
- [x] 7.4 `pnpm docs:check`（docs 目录）通过；`openspec validate fix-review-quick-wins` 通过
