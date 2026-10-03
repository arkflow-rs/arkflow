## ADDED Requirements

### Requirement: Console SHALL 有全局 Error Boundary

Console 的 app 路由出口 SHALL 被 Error Boundary 包裹：任何组件 render 异常（API 形状漂移、未预期的 null 等）SHALL 显示一个含错误摘要与重试按钮的错误页，MUST NOT 卸载整棵 React 树导致白屏。

#### Scenario: Render 错误不白屏

- **WHEN** 任意子组件在 render 阶段抛出异常
- **THEN** 用户看到错误页面（含错误信息摘要与 Reload 按钮），而非空白页

### Requirement: 操作等待 SHALL 诚实报告长时操作

`waitForOperation` 的轮询上限 SHALL 足以覆盖正常服务端操作时长（默认 60 秒）；超时错误 SHALL 提示操作可能仍在服务端执行并建议刷新查看结果，MUST NOT 暗示操作已失败。

#### Scenario: 超过旧上限的操作不误报失败

- **WHEN** 一个服务端操作执行 10 秒（旧 7.5 秒上限内未完成）
- **THEN** 等待不超时（新 60 秒上限内完成），用户看到真实结果

#### Scenario: 超时提示不暗示失败

- **WHEN** 等待超过 60 秒上限
- **THEN** 错误信息提示操作可能仍在执行并建议刷新查看（而非 "Operation timed out"）
