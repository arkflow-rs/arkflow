## ADDED Requirements

### Requirement: Server module reorganization SHALL preserve the API surface and agent entry points
控制面服务端内部模块结构的重组（handler 按路由域拆分到 `api/`、Agent 按关注点拆分到 `agent/`）SHALL 保持对外行为与公共入口不变：`router`/`hub_router`/`observability_router`/`serve`/`serve_hub` 与 `agent::run` 等公共函数的路径与签名 SHALL 不因内部重组而变化，全部路由（本地 API 与 Hub API）的 HTTP 契约 SHALL 原样保持，Agent 命令分发的字符串协议 SHALL 逐字不变。

#### Scenario: handler 拆分后路由契约不变
- **WHEN** lib.rs 中的 handler 被重组进 `api/` 子模块
- **THEN** 本地控制面 API 与 Hub API 的全部路由路径、方法、请求/响应形状与鉴权行为不变，既有 API 契约测试全部通过

#### Scenario: Agent 拆分后命令协议不变
- **WHEN** agent.rs 被重组进 `agent/` 目录模块
- **THEN** Hub 下发的字符串命令（job_start/job_stop/job_checkpoint/validate_configuration 等）的匹配与执行语义逐字不变
