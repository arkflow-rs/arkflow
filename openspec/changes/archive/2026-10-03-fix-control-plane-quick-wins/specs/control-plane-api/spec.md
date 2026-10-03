## ADDED Requirements

### Requirement: hub_problem SHALL 映射存储故障为服务端错误

`hub_problem` 的 HTTP 状态映射 SHALL 把存储层故障（`StorageUnavailable`、`Storage(_)`）映射为 503 `storage_unavailable`（服务端问题，可重试），而非 400 `agent_request_rejected`（客户端问题）；`GenerationConflict` SHALL 映射为 409 `generation_conflict`（语义冲突）。

#### Scenario: 存储故障返回 503

- **WHEN** hub_problem 收到 StorageUnavailable 或 Storage(_) 错误
- **THEN** 响应状态码为 503，错误码为 "storage_unavailable"

#### Scenario: 世代冲突返回 409

- **WHEN** hub_problem 收到 GenerationConflict 错误
- **THEN** 响应状态码为 409，错误码为 "generation_conflict"

### Requirement: ${file:} 引用 SHALL 限制为绝对路径且不含遍历

`${file:path}` 的路径解析 SHALL 拒绝包含 `..` 组件的路径（防止目录遍历）与相对路径（防止依赖 CWD 的不确定行为）；仅接受不含 `..` 的绝对路径。拒绝时错误信息 SHALL NOT 回显文件路径或内容。

#### Scenario: 合法绝对路径通过

- **WHEN** ${file:/etc/app/secrets.txt} 引用被解析
- **THEN** 文件内容被读取并替换引用

#### Scenario: 含 .. 的路径被拒绝

- **WHEN** ${file:/etc/../etc/shadow} 引用被解析
- **THEN** 解析以显式错误失败（错误不回显路径）

#### Scenario: 相对路径被拒绝

- **WHEN** ${file:~/.ssh/id_rsa} 或 ${file:relative/path} 引用被解析
- **THEN** 解析以显式错误失败
