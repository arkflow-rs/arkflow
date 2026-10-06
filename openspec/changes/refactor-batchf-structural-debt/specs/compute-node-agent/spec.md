## ADDED Requirements

### Requirement: Command operations SHALL be a closed, strongly-typed set with byte-identical wire format

Hub→Agent 命令的 `operation` 域 SHALL 以封闭枚举（`AgentOperation`）建模：已知操作（stream 生命周期 start/stop/restart、配置 validate/diff/apply/rollback_configuration、Job 的 start/restart/stop/checkpoint/savepoint/checkpoint_commit/savepoint_commit）在序列化与反序列化两个方向上 SHALL 与既有字符串值逐字节相同；不在集合内的值 SHALL 原样保留于未知回退变体中（无损往返），并在分发时进入既有 fail-closed 失败路径且错误文案包含原始操作串。Agent 侧命令分发 SHALL 以对该枚举的穷尽 `match` 表达，新增操作变体时编译器 SHALL 拒绝遗漏分发分支的实现。

#### Scenario: 已知操作的 wire 字节不变

- **WHEN** 任一已知操作（如 `job_checkpoint_commit`）构造为 `AgentCommand` 并序列化为 JSON
- **THEN** `operation` 字段的字符串值与重构前的字面量逐字节相同，旧版 Hub/Agent 可原样反序列化

#### Scenario: 未知操作 fail-closed 且保留原串

- **WHEN** Agent 收到 `operation` 为未知字符串（如来自更新版本 Hub 的 `job_teleport`）的命令
- **THEN** 命令以 `Failed` 终态结束，错误消息包含该原始字符串，Agent 会话不中断

#### Scenario: 分发穷尽性

- **WHEN** 向 `AgentOperation` 枚举新增一个变体而未更新 Agent 分发 match
- **THEN** workspace 编译失败（非穷尽 match），遗漏在编译期而非运行期暴露

### Requirement: Command payloads SHALL parse through typed constructors with stable error messages

Job 与配置类命令的 payload 解析 SHALL 收敛到类型化构造器（结构体 + 显式解析函数），分发逻辑 SHALL 只消费结构体字段；payload 缺字段/类型错误的用户可见错误消息 SHALL 与既有文案逐字节相同。

#### Scenario: payload 缺字段报错文案不变

- **WHEN** `job_checkpoint` 命令的 payload 缺少 `checkpoint_id`
- **THEN** Agent 返回的失败原因与重构前逐字节相同（如 `missing checkpoint_id`）

#### Scenario: payload 解析成功后字段直接可用

- **WHEN** `job_start` 命令的 payload 合法
- **THEN** 分发处理函数从类型化结构体直接取得 `JobPlan`、assignments、recovery 与 split placement 字段，无内联 JSON 挖掘
