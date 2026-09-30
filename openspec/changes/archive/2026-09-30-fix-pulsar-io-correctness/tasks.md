## 1. output 修复（P1-7）

- [x] 1.1 producer 改为按 topic 缓存：`connect()` 只建 client；`write()` 求值 topic 后 get-or-build producer（`with_topic`），`close()` 清空缓存
- [x] 1.2 发送改为 `send_non_blocking(...).await?` + await `SendFuture` 直至 `CommandSendReceipt`，错误映射上浮；删除被忽略的 `_topics` 参数用法
- [x] 1.3 topic `Vec` 语义对齐 kafka（逐消息 `v[i]`）但越界返回 `Error::Config` 而非 panic

## 2. input 修复（P1-11）

- [x] 2.1 消费循环的 `next()` 用 `tokio::time::timeout(CONSUMER_LOCK_BOUND=100ms, ...)` 包裹：超时释放锁、回到外层 select 重查取消后继续；消息就绪时零额外延迟

## 3. 端到端测试（tests/pulsar_io.rs，参照 kafka_eos.rs）

- [x] 3.1 testcontainers 拉起 `apachepulsar/pulsar:3.3.3` standalone（固定宿主端口 6650、BrokerLease 式容器卫生、清扫遗留容器）
- [x] 3.2 output 端到端：write N 条 → 独立 consumer 验收全量；含 Scalar 与 Vec 两种 topic 表达式
- [x] 3.3 可靠投递回归：connect 成功后停止 broker → write 返回 Err（不 fire-and-forget）
- [x] 3.4 input 端到端：produce → input read 验收；ack 后同订阅新消费者不重投
- [x] 3.5 ack 活锁回归：read 交付一条后消息流停止 → ack 在有界时间（如 3s）内完成

- [x] 3.6 （实现补充）output 回执等待 30s 上界（`WRITE_RECEIPT_TIMEOUT`），超时映射 Connection 错误——e2e broker-down 场景驱动
- [x] 3.7 （实现补充）input 新订阅显式 `InitialPosition::Earliest`，消除 pulsar-rust 默认 Latest 的积压跳过

## 4. 文档与验证

- [x] 4.1 核对 pulsar input/output 组件文档页（en/zh）与实现语义，必要时同步（write 可靠性、topic 表达式）
- [x] 4.2 `cargo test -p arkflow-plugin --test pulsar_io` 全绿（本机 Docker）
- [x] 4.3 `cargo test --workspace --all-targets` 全绿
- [x] 4.4 `cargo clippy --workspace --all-targets` 零新增告警
- [x] 4.5 `pnpm docs:check` 与 `openspec validate fix-pulsar-io-correctness` 通过

## 5. CR 修复（2026-09-30）

- [x] 5.2 实装 output 的 `value_field`（此前解析后从不读取、文档却声称生效）：指定列的逐行值作为载荷（Binary/Utf8 支持、其余 fail-closed，优先于 codec）；e2e 补验证场景
- [x] 5.1 producer 缓存改 per-topic `Arc<Mutex<Producer>>`：map 锁只守 get-or-build，发送走 per-topic 锁——原先 map 锁横跨入队+30s 回执等待，单个 topic 卡住会把其他 topic 的发送一起阻塞（部分失效爆炸半径）；e2e 5/5 复验
