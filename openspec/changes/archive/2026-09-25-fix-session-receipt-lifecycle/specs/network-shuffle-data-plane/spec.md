# network-shuffle-data-plane 变更（Delta）

## MODIFIED Requirements

### Requirement: 回执经会话级路由跨越连接更替

回执 SHALL 发往按会话键存在的有界会话队列，由常驻转发任务写往当前服务该会话的连接；连接更替时写失败的原条目 SHALL 原样重试直至新连接接槽，不得丢弃。RemoteAck SHALL 持会话队列而非连接通道。连接退出清槽 SHALL 以通道身份为守卫——只清除本连接安装的槽，不得清掉重连替代连接已安装的槽。转发任务 SHALL 同时监视会话撤销与节点数据面关停，二者任一发生即退出。确认丢失与 Job 会话移除 SHALL 撤销路由并取消转发任务。

#### Scenario: 替代连接的槽不被旧连接清理清除

- **WHEN** 透明重连的新连接已安装回执槽，随后旧连接的退出清理运行
- **THEN** 新连接的槽保持有效，其回执照常转发

#### Scenario: 数据面关停停止转发任务

- **WHEN** 节点数据面关停（manager shutdown）
- **THEN** 所有会话转发任务退出，不驻留、不强持 manager
