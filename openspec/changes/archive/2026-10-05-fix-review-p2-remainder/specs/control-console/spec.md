# control-console Delta

## ADDED Requirements

### Requirement: Console requests SHALL time out and SSE reconnects SHALL back off
Console 的 API 请求层 SHALL 对每个请求施加默认超时（30 秒，AbortSignal）并在超时时给出可读错误；调用方传入的中止信号 SHALL 与超时信号合并生效。SSE 事件流断线重连 SHALL 使用指数退避（1 秒起步、上限 30 秒），并在成功重新建立后重置退避；连接黑洞 MUST NOT 使 UI 无限期冻结。

#### Scenario: 连接黑洞在超时后报错
- **WHEN** API 请求因服务端不可达而挂起
- **THEN** 请求在 30 秒超时后以可读错误失败，UI 不永久冻结

#### Scenario: SSE 断线指数退避
- **WHEN** SSE 连接断开且连续重连失败
- **THEN** 重连间隔按 1s → 2s → 4s … 指数增长直至 30 秒上限；连接成功后间隔重置回 1 秒

## MODIFIED Requirements

### Requirement: Configuration publishing and rollback
The Web Console SHALL allow an operator to inspect configuration versions, review validation results, publish a valid configuration, and request rollback to a prior version. When comparing a version against another, the Console SHALL use the selected version's direct predecessor (by version sequence) as the default diff baseline; when no predecessor exists (first version), the diff action SHALL be disabled instead of comparing against an arbitrary version.

#### Scenario: Publish valid configuration
- **WHEN** the operator submits a validated configuration
- **THEN** the console displays the API application result, affected Streams, and any recovery failure

#### Scenario: Diff baseline is the direct predecessor
- **WHEN** the operator opens the version comparison view with three or more versions present
- **THEN** the default baseline is the version immediately preceding the selected one by sequence, not an arbitrary other version

#### Scenario: First version disables diff
- **WHEN** only one version exists and it is selected
- **THEN** the diff action is disabled (no predecessor to compare against)
