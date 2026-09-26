# Tasks: add-remote-failed-receipt

- [x] 1.1 `ReceiptKind::Failed` + `RemoteAck::abort` 镜像发送。
- [x] 1.2 `PendingReceipts::apply` Failed 分支：移除 + 异步 branch.abort。
- [x] 1.3 测试：中止即时性、不产生虚假 ack、重复幂等、往返序列化。
- [x] 1.4 `cargo test -p arkflow-core` 全绿。
