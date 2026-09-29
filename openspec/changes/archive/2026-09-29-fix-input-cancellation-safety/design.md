## Context

契约（`arkflow-core/src/input/mod.rs:263-276`）要求 `read()` 在引擎 `tokio::select!` 的随时丢弃下不丢已认领数据。六个 input 的违例同源：`recv_async()` 认领 → `apply_codec_to_payload(...).await` 解码（pulsar 另有 consumer 锁 await 构造 ack；websocket 另有递归跳控制帧）。引擎源链 idle tick 每 100ms 触发（`executor/task.rs` 源循环），取消落在认领与返回之间即静默丢消息——常规路径而非罕见路径。

## Goals / Non-Goals

**Goals:**
- 六个 input 的 `read()` 达成单数据 await 点形态：认领（recv）返回即构造完成，无任何后续可被取消的 await。
- 错误语义保持等价：decode 失败仍使 read 返回 Err（经通道传递），引擎失败路径不变。
- 契约回归防线：门控 codec 测试框架 + 两处端到端用例 + 共享模式单元断言。

**Non-Goals:**
- pulsar 消费锁活锁（P1-11）、multiple_inputs（P1-8）、非列出的其他 input。
- codec 批量化解码、broker 级集成测试基建。
- MessageBatch/ack 的公共 API 变更（Delivery 为 plugin 内部 pub(crate)）。

## Decisions

**D1 — 解码与 ack 构造前移到生产端，通道改载成品投递。**
新增共享类型（`codec_helper.rs`）：

```rust
pub(crate) enum Delivery {
    Data(MessageBatchRef, Arc<dyn Ack>),
    Err(Error),
}
pub(crate) async fn decode_delivery(
    payload: &[u8], codec: &Option<Arc<dyn Codec>>,
    input_name: Option<String>, ack: Arc<dyn Ack>,
) -> Delivery
```

各 connector 的后台接收任务（本就存在：mqtt eventloop / pulsar consumer / nats / redis / ws reader）在发送前完成解码 + ack 构造 + `set_input_name`；decode 失败发 `Delivery::Err`。`read()` 收敛为「连接检查（锁获取，无副作用，可安全丢弃）→ select(recv, cancellation) → 解包返回」。*备选否决*：peek 式通道（flume 不支持）；取消时回填消息（async fn 内无法感知取消）；把 decode 变同步（async-codec-contract 变更已确立 IO 型 codec 必须 async）。

**D2 — 通道容量与背压不变。** 通道仍为 `flume::bounded(1000)`；decode 前移后通道内存占用从原始字节变为 Arrow 批（通常更大），但容量语义与背压推导不变——满时生产端 `send_async` 等待，反压到 connector。解码失败不发 Err 之外的数据，无额外占用。

**D3 — websocket 控制帧过滤移入生产端，消除 read 递归。** Ping/Pong/Frame 分支在生产任务里 `continue`；`Message::Close` 视作 EOF。read 递归（`websocket.rs:159/166`）删除——递归本身也是取消窗口。

**D4 — pulsar 的 ack 构造（含 `self.consumer.read().await`）随解码一并前移。** 生产任务本就持有 consumer；`PulsarAck::new(message, consumer)` 在生产端构造后随 Delivery 下发。read() 不再触碰 consumer 锁——顺带收窄（但不修完）P1-11 的锁争用面：`PulsarAck::ack` 的执行路径仍需锁，活锁本体留给独立 change。

**D5 — generate 重排副作用顺序。** 先（读）检查 count 上限 → 构造负载（同步）→ 解码（await，此刻仍无副作用，可安全丢弃）→ `fetch_add` 推进配额 → 返回。被取消的 decode 不再消耗配额。

**D6 — 契约测试框架三层。**
1. **共享框架**（`codec_helper.rs` 测试模块 + `#[cfg(test)]` 可复用件）：`GateCodec`（`decode` 先 `entered.notify_one()` 再等 `release`，构造确定性"解码中"窗口）；`cancel_during_decode_then_read` 断言循环：启动 read → 等 entered → drop read future（模拟 select 丢弃）→ release → 再次 read 必须恰好取回该消息。对**模式级 fake input**（与改造后 read 同构的最小实现）跑通框架，并对故意违例实现断言框架能抓到（测试框架自身的有效性证明）。
2. **端到端**：websocket——本地起 tokio-tungstenite 服务端（已是依赖）推 1 条消息，门控 codec + 取消循环；http——axum 服务端 POST（合规对照，证明框架对合规实现零误报）。
3. **单元断言**：五个通道型 input 的内联测试直接向通道预投 `Delivery` 并断言 read 解包语义（Err 透传 / EOF / cancellation 分支）。

mqtt/pulsar/nats/redis 无 broker 基建，端到端留给后续（proposal Non-goals 已声明）。

## Risks / Trade-offs

- [通道载 Arrow 批使等量消息下内存增大] → 上界仍由 bounded(1000) 约束；原始字节通常 ≥ 解码后批（JSON→列式压缩），预期中性。
- [decode 前移使解码错误从"read 同步报错"变为"经通道异步报错"] → 错误种类与引擎处理路径不变；通道断开（close 后残留 Err）行为与今日 EOF 路径一致。
- [pulsar 生产端持锁构造 ack 与 P1-11 的交错] → 只前移构造、不改变锁的持有范围；活锁修复独立立项，本变更不使其恶化。
- [框架对 broker 型 input 覆盖不足] → 以三层框架 + 模式统一（五处 read 同构，审查面收敛到 codec_helper 一处）补偿；有 broker 基建后补 E2E。
- [六个文件同时动，回归面大] → 每个 input 独立可测（内联单测 + 既有测试），全量 workspace 门禁兜底。

## Migration Plan

纯内部行为修复，无配置/协议变更，直接合入。回滚 = revert。
