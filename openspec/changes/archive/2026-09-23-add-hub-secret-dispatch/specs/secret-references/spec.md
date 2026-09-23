# secret-references 变更（Delta）

## ADDED Requirements

### Requirement: Hub 分发时的 secret 引用预解析

多节点部署中，Hub SHALL 在 rollout 分发构建 intent payload 时，对配置 content 中的 `${secret:...}` 引用按 **Hub 进程**的 `ARKFLOW_SECRET_<NAME>` 环境解析，解析后的 payload 以 JSON 文本承载 content 且 `format` 置为 `json`；content 中不含 `secret:` 引用时 payload SHALL 原样派发。Hub 存储的配置版本内容 SHALL 保持引用原文。`env:`/`file:` 引用 SHALL NOT 被预解析（节点本地语义）。

#### Scenario: 分发 payload 预解析

- **WHEN** 配置版本 content 含 `${secret:db_pass}`，Hub 进程环境设置 `ARKFLOW_SECRET_db_pass=s3cret`，rollout 分发至节点
- **THEN** intent payload 的 content 中不含 `${secret:`，对应位置为 `s3cret`，且 `format` 为 `json`

#### Scenario: secret 未设置时目标分发失败

- **WHEN** `ARKFLOW_SECRET_db_pass` 未设置且 rollout 分发含该引用的配置
- **THEN** 该 rollout target 置为 failed，错误指明引用，Agent 不收到半解析配置

#### Scenario: env 引用不被预解析

- **WHEN** 配置同时含 `${secret:A}` 与 `${env:LOCAL}`
- **THEN** 分发 payload 中 `${secret:A}` 被解析，`${env:LOCAL}` 保持原样由 Agent 物化时解析
