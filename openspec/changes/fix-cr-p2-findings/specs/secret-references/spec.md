## ADDED Requirements

### Requirement: 节点侧分发配置的落盘语义

Hub 分发已完成 `${secret:...}` 预解析的配置时，payload SHALL 同时携带原引用文本（`content_verbatim`）；节点 SHALL 以 verbatim 文本持久化配置版本（版本存储/历史不落已解析明文），并仅将已解析副本用于内存中的校验与运行。`content_verbatim` 缺失时（旧 Hub 或本地 API 直提）节点行为 SHALL 与现状一致。节点回滚含 `${secret:...}` 引用的版本时，物化失败 SHALL 返回指明引用路径的错误——此类版本的回滚 SHALL 通过控制面重新解析完成。

#### Scenario: 分发版本以引用文本落盘

- **WHEN** Hub 分发含 `${secret:db_pass}` 的配置且节点应用成功
- **THEN** 节点版本存储中该版本的 content 为 `${secret:db_pass}` 引用原文，不含解析明文；运行中的配置为已解析副本

#### Scenario: 缺失 verbatim 时行为不变

- **WHEN** 节点收到不含 `content_verbatim` 的 apply 载荷（本地 API 或旧 Hub）
- **THEN** 版本存储持久化载荷的 content，行为与引入该字段前一致

#### Scenario: 节点回滚引用版本报可定位错误

- **WHEN** 节点回滚 content 含 `${secret:db_pass}` 且节点环境未设置对应变量的版本
- **THEN** 物化失败，错误信息包含配置路径与 `${secret:db_pass}` 引用原文，不包含任何明文
