# Design: fix-remote-replay-ack-semantics

## 决策

1. **删除镜像回执而非加 acked_seq 判定**：即使按 acked_seq 判定，帧仍可能绑在已死连接的回执通道上（RemoteAck 持连接级 tx），ack 会失败并把链打死。根因是回执的生命周期绑在连接上——把它提升到会话级后，镜像回执的存在理由（补偿丢回执）整体消失，语义回归基线："上游只被处理完成推进"。
2. **转发任务重试而非丢弃**：会话队列有界（沿用 max_receipt_queue），转发失败原条目重试到新连接——回执 at-least-once，上游 `apply` 幂等（未知/已完成 seq 丢弃），无需去重。
3. **回执丢失窗口的兜底**：写往已死 socket 且对端未收 → 上游 pending 悬置 → 既有 drain 超时 → checkpoint 轮失败（fail-closed）→ 从上个 sealed cut 重放。与 v1 规格「Held 帧丢失安全降级」同一模式。

## 风险

- 转发任务在无连接期间 20ms 轮询重试（有界队列背压 RemoteAck.ack，链自然反压）。
