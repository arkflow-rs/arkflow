## Context

pulsar crate 固定 6.x（本地解析 6.8.0）。其 API 形态决定了修复形态：

- `ProducerBuilder` 无 topic 必然 `Error::Custom("topic not set")`（producer.rs:1062）；`send` 已 `#[deprecated]` 且本质是 SendFuture 的包装；可靠发送的标准姿势是 `send_non_blocking(...).await?` 返回 `SendFuture`，**继续 await 它**拿 `CommandSendReceipt`（producer.rs:415-421 文档示例即此）。`send_non_blocking` 要求 `&mut self`。
- `Consumer::ack(&mut self)`（consumer/mod.rs:113）与 `next()`（Stream，`&mut self`）都必须独占 consumer——共享互斥锁不可避免，能修的是**锁的持有时长**。
- Consumer 的 `next()` 是 Stream poll 语义：取消只发生在 poll 之间，已从内部通道取出的消息随完成的 poll 返回，不会因 timeout 取消而丢失——这使 timeout 包裹成为安全的锁上界手段。

## Goals / Non-goals

**Goals:** output 拿回执才报成功、按 topic 建 producer；input 的锁持有有界；真实 broker 端到端 + 回归测试。

**Non-goals:** 重连策略、批量优化、事务语义、TLS e2e（见 proposal）。

## Decisions

1. **producer 按 topic 缓存（`Mutex<HashMap<String, PulsarProducer>>`），connect() 只建 client。** 备选"connect 期解析一次 topic 建单 producer"——topic 是表达式（支持 {field} 占位），逐批可变，单 producer 无法承载；备选"每次 write 重建 producer"——producer 创建含 broker 往返（lookup/握手），逐批重建不可接受。缓存按需建、复用，`close()` 清空。锁用 tokio Mutex（构建 producer 是异步操作）。
2. **可靠发送 = await SendFuture。** 不用已 deprecated 的 `send`；`send_non_blocking` 拿 SendFuture 后立刻 await——语义上仍是"逐条等回执"，但保留了 6.x 官方推荐的 API 形态。
3. **topic Vec 越界返回 `Error::Config`。** kafka output 现状是 `&v[i]` 直接 panic（`output/kafka.rs:280,489`）；pulsar 侧修复采用 `v.get(i).ok_or(Config)`，不为对齐而复制 panic。kafka 侧的同类问题不属本变更（记录到 CR 清单的既有问题）。
4. **input 锁上界用 `timeout(CONSUMER_LOCK_BOUND, next())`，常量 100ms。** 超时 → 释放锁 → 回到外层 select（先查 cancelled 再继续 next）——ack 最坏等待 ≈ 100ms + ack 本身；消息流动时 next 立即就绪，零额外延迟。备选 `try_next()` 轮询会在空闲期引入固定轮询间隔的延迟/空转；备选"ack 独立通道"重构面大且 pulsar 6.x 无免锁 ack API。
5. **e2e 参照 `kafka_eos.rs` 模式**：testcontainers `GenericImage` + 固定宿主端口 6650（Pulsar standalone 会做 broker lookup 并回 advertised 地址，随机映射端口会破坏二次连接）+ BrokerLease 式容器卫生（清扫遗留容器、末位释放时移除）。测试不 `#[ignore]`——Docker 可用即跑，与 kafka_eos 同纪律。

6. **（实现补充）回执等待 30s 上界。** e2e 首跑暴露：broker 停止后 pulsar 客户端内部重连、挂起中的回执永不 resolve，write 无限挂起——对回执 await 包 `tokio::time::timeout(WRITE_RECEIPT_TIMEOUT=30s)`，超时映射 `Error::Connection`。健康 broker 回执毫秒级，30s 余量充分。
7. **（实现补充）input 显式 Earliest。** e2e 暴露 pulsar-rust `ConsumerOptions` 默认 "latest"（`options.rs:25-28` 文档自述），新订阅跳过积压。显式 Earliest 对齐 kafka input 语义；行为变化（新订阅现在消费积压）已在 proposal 标注。

## Risks / Trade-offs

- [100ms 锁上界给空闲期 ack 引入最坏 ~100ms 延迟] → 相比"无限等待"是严格改进；常量可后续提为配置（无当前需求）。
- [write 等回执后延迟升高（每条一次 broker 往返的确认）] → 这是"可靠输出"的本义；此前的低延迟建立在假成功上。
- [producer 缓存在 topic 极多时无上限] → 与 kafka output 的 topic 空间同数量级；真实部署 topic 集有限，暂不设 LRU（记录为后续观察项）。
- [testcontainers 拉取 apachepulsar/pulsar 镜像在 CI 首跑较慢] → 与 cp-kafka 同量级；固定 tag（`3.3.3`）保证可复现。
- [timeout 取消 next() 的安全性] → Stream poll 语义决定取消只发生在 poll 间（见 Context）；P1-5 契约测试框架已覆盖 pulsar read 的取消安全，本次不改变 read 路径结构。

## Migration Plan

无配置面变化。回滚即 revert。e2e 需本机/CI Docker。

## Open Questions

无。
