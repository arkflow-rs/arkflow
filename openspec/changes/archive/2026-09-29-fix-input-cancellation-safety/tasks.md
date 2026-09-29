## 1. 共享投递类型与解码前移辅助

- [x] 1.1 `input/codec_helper.rs` 新增 `pub(crate) enum Delivery { Data(MessageBatchRef, Arc<dyn Ack>), Err(Error) }` 与 `decode_delivery(payload, codec, input_name, ack) -> Delivery`（解码 + set_input_name + 配对 ack；失败返 Err），含单元测试（有/无 codec、解码失败）
- [x] 1.2 契约测试框架件（`#[cfg(test)]` 共享）：`GateCodec`（entered/release Notify 门控解码）与 `cancel_during_decode_then_expect_delivery` 取消循环 helper

## 2. 五个通道型 input 改造（每个：生产端解码 + read 单 await 化 + 内联单测）

- [x] 2.1 mqtt：eventloop 任务内 decode + MqttAck 构造前移，通道改 `Delivery`；read 收敛（无 broker 无法注入通道越过连接门，分支语义由共享模式测试与全量套件覆盖）
- [x] 2.2 pulsar：consumer 任务内 decode + `PulsarAck::new` 前移（read 不再取 consumer 锁）；通道改 `Delivery`
- [x] 2.3 nats：Regular/JetStream 两模式的 decode + ack 前移（含 connect 内首拉与 spawned 循环两处）；通道改 `Delivery`
- [x] 2.4 redis：decode 前移（NoopAck；pubsub 同步回调经 Handle::try_current spawn 解码）；通道改 `Delivery`
- [x] 2.5 websocket：decode 前移 + Ping/Pong/Close 过滤移入后台任务、删除 read 递归；通道改 `Delivery`；端到端契约测试（本地 ws 服务端，见 3.2）
- [x] 2.6 generate：副作用重排（上限读检查 → 构造 → 解码 await → `fetch_add` → 返回），取消不再消耗配额（gate 场景配额断言由共享 probe 模式覆盖——generate 无通道，取消即重生成，配额推进已在最后一个 await 之后）

## 3. 端到端契约测试

- [x] 3.1 模式级 fake input：框架有效性双证——故意违例实现（认领后 await 解码）被框架抓出；合规同构实现通过
- [x] 3.2 websocket 端到端：本地 tokio-tungstenite 服务端推消息 + GateCodec + 取消循环，断言恰好一次送达
- [x] 3.3 http 对照组：同一框架对合规 http input 零误报

## 4. 验证

- [x] 4.1 针对性测试全绿：`cargo test -p arkflow-plugin --lib input::`
- [x] 4.2 `cargo test --workspace --all-targets` 全绿（27 套件 exit 0；two_node_job_smoke::split_job 首轮在满并行负载下超时一次，隔离 3/3 <1s 通过、复跑全量绿——属 harden-test-stability 已记载的该测试负载 flake 类）
- [x] 4.3 `cargo clippy --workspace --all-targets` 零新增告警
- [x] 4.4 `openspec validate fix-input-cancellation-safety` 通过
