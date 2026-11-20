## ADDED Requirements

### Requirement: 从配置面移除的键 SHALL 在存量配置中显式拒绝
组件配置面移除某字段后，构建路径 SHALL 检测存量配置仍携带该键的情况并返回指名该键与移除原因的显式错误：MUST NOT 依赖 serde 对未知键的静默丢弃，使"配置已失效"对用户不可见。首个落地实例：`pulsar` 的 `ca_file`（驱动无自定义 CA API，从未生效）。

#### Scenario: 存量配置携带 ca_file 启动报错
- **WHEN** pulsar 组件配置包含 `ca_file` 键
- **THEN** 构建返回显式错误，错误信息指名 `ca_file`、说明驱动无自定义 CA API 并指导移除该键；MUST NOT 静默忽略

#### Scenario: schema 不再宣告 ca_file
- **WHEN** 生成 pulsar 组件的配置 schema 或 `arkflow schema` 输出
- **THEN** 输出中不含 `ca_file` 字段
