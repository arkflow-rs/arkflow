# Design: add-remote-failed-receipt

## Context

下游处理失败时，该交付的 `RemoteAck` 会被本地 error 路径 `abort()`（`Ack::abort` 默认 `undo()`）。v1 未覆写 `RemoteAck::abort`，上游 pending 分支只能等 barrier drain 超时。

## Decisions

1. **复用 abort 而非新增回调**：失败信号天然走 `Ack::abort`——远程边只需把它镜像为 `Failed` 帧，与 Acked/Held/Released 同通道同格式。
2. **上游即时中止**：`apply(Failed)` 移除 pending 并 spawn `branch.abort()`（与 Acked 的 ack 同样移出读循环，补偿可能阻塞在 journal/WAL undo）。
3. **drain 超时保留**：Failed 帧本身可能丢失（连接断开），超时路径仍是兜底——fail-closed 语义不变，只是常见路径加速。
4. **幂等**：重复 Failed（重试投递）命中已移除的 seq → 既有「未知 seq 丢弃」路径。

## Risks / Trade-offs

- 线协议新增变体：serde 枚举按 tag 序列化，旧内核读到新 tag 报错断连——与既有「非法帧关闭连接」策略一致；混布升级顺序约束已在 spec。
