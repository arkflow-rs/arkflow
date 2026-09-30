## 1. 代际生命周期

- [x] 1.1 引入 `Generation { token: CancellationToken, tracker: TaskTracker }`（`tokio::sync::Mutex<Option<Generation>>` 持有）；`connect()` 先 take 旧代 → cancel → `wait()` → 子 input `connect()` → 建新代 spawn 读任务 → `tracker.close()`
- [x] 1.2 `close()` 复用代际收尾（取消当前代并等待）后逐个关闭子 input，语义与现状一致
- [x] 1.3 重连时 `try_recv` 清理上一代残留的 `Msg::Err`，保留 `Msg::Message`（顺序不变）

## 2. 错误路径与有界通道

- [x] 2.1 读任务错误路径统一：任意子 input 错误单次 `send_async(Msg::Err(e))` 后 return，删除"非 Disconnection 继续循环"分支
- [x] 2.2 内部通道改 `flume::bounded(1024)`；发送仅在 select 了 cancellation 的任务内进行（现状结构即满足，确认无锁内发送）

## 3. 测试

- [x] 3.1 重连不叠加：用一个可控 mock 子 input 记录活跃 read 并发数——两次 `connect()` 后并发读数恒为 1，且第二次 connect 期间旧代先退出
- [x] 3.2 子错误单次上浮：mock 子 input 连续返回同一错误，断言消费者只收到一次 Err、任务退出（无热循环）
- [x] 3.3 有界背压：capacity 打满后子任务发送阻塞、消费者恢复后放行；关闭释放阻塞中的发送不挂死
- [x] 3.4 重连清理：通道预置一条 Err + 若干 Message，`connect()` 后 Err 消失、Message 按序可读

## 4. 文档（en + zh-Hans）

- [x] 4.1 更新 `docs/docs/components/0-inputs/multiple_inputs.md` 与 zh-Hans 对应页：断连重连行为（重启一代读任务）、子错误上浮语义、内部缓冲有界（1024）与背压

## 5. 验证

- [x] 5.1 `cargo test -p arkflow-plugin` 全绿
- [x] 5.2 `cargo test --workspace --all-targets` 全绿
- [x] 5.3 `cargo clippy --workspace --all-targets` 零新增告警
- [x] 5.4 `pnpm docs:check` 通过
- [x] 5.5 `openspec validate fix-multiple-inputs-lifecycle` 通过

## 6. CR 修复（2026-09-30）

- [x] 6.1 修正 multiple_inputs.md（en/zh）Notes 中错误的子流语义断言：实际行为是任一子输入 `EOF` 结束整个合并输入（`task.rs:665` 源链 EOS 路径）、任一子输入 `Disconnection` 触发整个输入重连——并非"该子流结束、其余继续"
- [x] 6.2 测试 3.3 补非空转断言（`consumed > 0`），防读者任务未启动时背压断言空洞通过

## 7. CR 第二轮修复（2026-09-30）

- [x] 7.1 空 `inputs` 数组构建期拒绝：schema 补 `minItems: 1`、`new()` 显式校验 + 拒绝测试；空数组旧代码会静默部署一个永久挂起的输入（零读任务、sender 永活）
- [x] 7.2 再生成 schema 快照（`ARKFLOW_REGENERATE_DOCS=1 docs_inventory_snapshot`）：`component-inventory.json` 与 `config-schema.json` 同步 `minItems`
