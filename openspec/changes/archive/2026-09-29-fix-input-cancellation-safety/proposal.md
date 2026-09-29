## Why

2026-09-29 代码审查（`openspec/CODE_REVIEW_2026-09-29.md` P1-5）发现 6 个 input 系统性违反 `Input::read` 的取消安全契约。契约原文（`crates/arkflow-core/src/input/mod.rs:263-276`）：引擎源循环把 `read()` 与控制事件（idle tick、barrier、cancellation）放在同一个 `tokio::select!` 里——**源链 idle tick 每 100ms 触发一次**，pending 的 read future 随时被丢弃重来；实现方必须保证"第一个 await 之前无可观察副作用"，被丢弃的 read 不得丢失已认领的数据。

六个违例实现共享同一个复制粘贴模式：`receiver.recv_async().await`（从通道弹出消息 = 认领，副作用已发生）→ `apply_codec_to_payload(...).await`（codec 解码，又一个可被取消的 await）。取消落在两点之间时消息已弹出但 ack 从未构造——**静默丢失，at-least-once 语义被击穿**：

- MQTT：`input/mqtt.rs:198-209`（`set_manual_acks(true)` + 默认 clean session，broker 不重投——丢即永久丢失）
- Pulsar：`input/pulsar.rs:232-243`（decode await 之后还有第三个 await：`:247` 取 `self.consumer.read().await` 构造 ack）
- NATS：`input/nats.rs:360-370`
- Redis：`input/redis.rs:406-414`（List 模式消息由后台任务 `blpop` 弹出，取消即永久丢失）
- WebSocket：`input/websocket.rs:149-174`（另以递归 `self.read().await` 跳过 Ping/Pong，叠加取消窗口）
- generate（P2 级）：`input/generate.rs:72-93`——`count.fetch_add`（配额副作用）在 decode await 之前执行，取消消耗配额但不产消息

HTTP input（`input/http.rs`）是合规对照：recv 之后只有同步 `set_input_name`。file/sql input 的 `try_next()` 原子完成，亦合规。

## What Changes

- 新增共享投递类型 `Delivery`（Data/Err）与 `decode_delivery` 辅助函数（`input/codec_helper.rs`）：**codec 解码与 ack 构造前移到各 connector 的后台接收任务**，通道改载解码后的 `(MessageBatchRef, Arc<dyn Ack>)`——`read()` 收敛为单一数据 await 点（recv），recv 返回即完成，按构造即取消安全。
- 五个通道型 input（mqtt/pulsar/nats/redis/websocket）按上述模式改造；websocket 的 Ping/Pong 控制帧过滤移入后台任务（消除 read 递归）；pulsar 的 ack 构造（含 `consumer.read().await`）移入后台任务。
- generate input 重排：先构造并解码负载（无可副作用的 await），最后才 `fetch_add` 计数——decode 被取消不再消耗配额。
- 新增**契约测试框架**：门控 codec（decode 阻塞在 Notify 上，可确定性制造"decode 进行中"窗口）+ 引擎形态的取消循环（drop read future 后重试），断言消息不丢不重。对 websocket（本地起真实 ws 服务端）与 http（合规对照）做端到端契约测试，对共享 `decode_delivery` 模式与各 input 的 read 形态做单元断言。

## Capabilities

### New Capabilities

- `input-cancellation-safety`: 随引擎发行的通道型 input 的 `read()` 取消安全契约——认领与解码的顺序不变量、单 await 点形态、以及回归测试框架。

### Modified Capabilities

（无——现有 specs 不覆盖 connector 级 read 语义；本变更为纯正确性修复，无配置面/用户可见行为变化。）

## Impact

- `crates/arkflow-plugin/src/input/codec_helper.rs`（Delivery + decode_delivery + 测试框架）
- `crates/arkflow-plugin/src/input/{mqtt,pulsar,nats,redis,websocket,generate}.rs`（生产端解码 + read 收敛）
- `crates/arkflow-plugin/src/input/websocket.rs`（控制帧过滤前移、去递归）
- 契约测试：`codec_helper` 内共享框架 + websocket/http 端到端用例
- 无配置面变更、无 wire/协议变更、无破坏性变更；decode 失败的错误语义不变（仍以 read 错误 surfaced，引擎行为一致）。

## Non-goals

- 不修 P1-11（pulsar consumer 锁跨 `next().await` 的活锁）、P1-8（multiple_inputs 重连任务翻倍/热循环/无界通道）——各自独立立项。
- 不为 mqtt/pulsar/nats/redis 搭建本地 broker 做端到端契约测试（无测试 broker 基建）；这四个 input 的保障来自共享模式 + 单元断言，端到端覆盖留给有 broker 基建后的补强。
- 不改动合规的 http/file/sql input 的行为（http 仅作为契约测试对照组）。
- 不做 codec 解码批量化（仍逐消息解码，仅前移位置）。
