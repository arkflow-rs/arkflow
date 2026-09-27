# Tasks: add-remote-transparent-reconnect

- [x] 1.1 `PendingReceipts` 重放日志（register 携带批次、replay_snapshot）。
- [x] 1.2 监督循环：`attach_stream_with_redial` + `run_edge_connection` + `replay_pending`；预算/退避/会话存活检查。
- [x] 1.3 接收端 `delivered_seq` 去重 + 镜像 Acked 补发。
- [x] 1.4 接收端宽限：`session_registrations` 代数、延迟报告、注册表保留与确认清理。
- [x] 1.5 泵语义：可重连时不中止分支；排空后读循环存活（回归修复）。
- [x] 1.6 配置字段 + validate + remove_job_session 清理。
- [x] 1.7 测试：重放恢复（含去重与镜像回执断言）、预算耗尽 fail-closed、既有套件回归；core 506 绿。
