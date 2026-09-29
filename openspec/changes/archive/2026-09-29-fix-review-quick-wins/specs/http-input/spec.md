## ADDED Requirements

### Requirement: HTTP 监听器失败 SHALL 同步可见

HTTP input 的监听器生命周期 SHALL 使 bind 失败在 `connect()` 同步返回明确错误（含失败原因），MUST NOT 在后台任务中 panic 或吞掉错误后仍报告连接成功；listener 任务在运行期异常退出 SHALL 反映为后续 `read()` 的错误（进入引擎的失败/重连路径），不得让 input 停留在"已连接但永无数据"的静默状态。

#### Scenario: 端口占用使 connect 失败

- **WHEN** 配置端口已被占用且引擎调用 `connect()`
- **THEN** connect 返回明确的 bind 失败错误，input 不进入 connected 状态，无后台任务 panic

#### Scenario: listener 运行期退出可见

- **WHEN** 已建立的 HTTP listener accept 循环因错误退出
- **THEN** 后续 read 以错误返回（进入引擎处理路径），而不是无限阻塞在空通道上
