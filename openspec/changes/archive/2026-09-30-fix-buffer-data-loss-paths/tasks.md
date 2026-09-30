## 1. batch processor 排空与关闭

- [x] 1.1 实现 `finish()`：EOS 时排空不足 `count` 的部分批次（复用 flush 合并逻辑），返回合并批次走常规下发路径
- [x] 1.2 实现 `on_tick()`：缓冲非空且 `last_batch_time.elapsed() >= timeout_ms` 时刷新部分批次，不依赖新消息到达
- [x] 1.3 `close()` 去掉 `batch.clear()`，改为纯资源释放；`finish()` 之后仍有残留（取消等异常路径）时记录包含条数的 warn
- [x] 1.4 batch 单测：finish 排空部分批次；无新消息到达时 on_tick 超时刷新；flush 合并失败（异构 schema 批次）时缓冲保留可重试；改写 `test_batch_processor_close`（现断言 close 后清空，语义已反转为保留）

## 2. memory buffer 失败路径与背压

- [x] 2.1 `process_messages()` 改为先克隆合并、成功后才清除队列：合并失败返回错误且队列条数与顺序不变
- [x] 2.2 `write()` capacity 背压：持有条数达到 capacity 时在 `select!` 中等待"排空通知或 close"，读者排空后 `notify_waiters()` 释放写者；等待不得发生在写锁内
- [x] 2.3 flush/close token 分离：`flush()` 仅 `notify_waiters()` 不再 cancel；`close()` 保持终止语义；`read()` 在关闭后先排空余量再返回 None（保持现状）
- [x] 2.4 删除 `ArrayAck`，合并批次的组合 ack 改用 `arkflow_core::input::VecAck`
- [x] 2.5 memory 单测：合并失败后队列保留可重试；capacity 满载 `write` 阻塞、读者排空后放行（`test_memory_buffer_capacity_limit` 改并发读者形态）；close 释放等待中的 write 不挂死；flush 之后超时释放仍然工作；VecAck 组合 ack 子项失败时反向 undo（计数 mock ack 断言补偿调用）

## 3. 文档（en + zh-Hans）

- [x] 3.1 更新 `docs/docs/components/1-buffers/memory.md` 与 zh-Hans 对应页：capacity 满载阻塞（背压）语义、flush 与 close 行为描述
- [x] 3.2 更新 `docs/docs/components/2-processors/batch.md` 与 zh-Hans 对应页：部分批次在 timeout 到期即刷新（无需新消息）、EOS/优雅关停时排空

## 4. 验证

- [x] 4.1 `cargo test -p arkflow-plugin` 全绿
- [x] 4.2 `cargo test --workspace --all-targets` 全绿（`two_node_job_smoke` 在重负载并行下超时抖动一次，串行两轮复跑全绿，其 job 不使用 batch/memory 组件，与本次改动无关）
- [x] 4.3 `cargo clippy --workspace --all-targets` 零新增告警
- [x] 4.4 `pnpm docs:check` 通过（若文档改动涉及 yaml 块保持 validate 标记）
- [x] 4.5 `openspec validate fix-buffer-data-loss-paths` 通过

## 5. CR 修复（2026-09-30）

- [x] 5.1 `MemoryBufferBuilder::build` 拒绝 `capacity: 0`（serde 不强制 JSON schema 的 minimum:1，零容量会使 `write()` 永久阻塞）+ 构建期拒绝测试
- [x] 5.2 memory.md（en/zh）补注：统一内核下 stream 配置的 `buffer: {type: memory}` 编译为 no-op（`stream_compiler.rs:82`，有 `memory_buffer_is_noop` 专测），页面语义为组件契约，适用于注册表直接使用场景
