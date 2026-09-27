# Tasks: fix-remote-replay-ack-semantics

- [x] 1.1 SessionReceiptRoute + 会话级转发任务 + 连接换槽。
- [x] 1.2 RemoteAck 改发会话队列；删除镜像回执；delivered_seq 发送成功后推进。
- [x] 1.3 会话清理三路径（确认丢失、remove_job_session、连接退出清槽）。
- [x] 1.4 重放保留按 reconnectable 门控。
- [x] 1.5 重连测试改按正确语义断言并通过；core 509 绿。
