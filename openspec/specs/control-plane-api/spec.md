# control-plane-api Specification

## Purpose
TBD - created by archiving change add-control-plane. Update Purpose after archive.
## Requirements
### Requirement: Unified control HTTP server
The system SHALL serve health checks and control-plane routes from one configurable HTTP server and SHALL report listener binding failures to the Engine startup path.

#### Scenario: Server starts with configured address
- **WHEN** the Engine starts with control HTTP enabled and an available address
- **THEN** health routes and `/api/v1` routes are reachable on that address

#### Scenario: Server bind fails
- **WHEN** the configured address cannot be bound
- **THEN** Engine startup returns an error instead of reporting readiness or panicking in a detached task

### Requirement: Versioned system and Stream APIs
The system SHALL expose `GET /api/v1/system`, `GET /api/v1/status`, `GET /api/v1/streams`, and `GET /api/v1/streams/{id}` with JSON responses containing stable Stream IDs, lifecycle state, timestamps, errors, and available metrics.

#### Scenario: List running Streams
- **WHEN** a client requests `GET /api/v1/streams`
- **THEN** the response includes every configured Stream exactly once with its current state

#### Scenario: Unknown Stream
- **WHEN** a client requests a Stream ID that is not registered
- **THEN** the API returns HTTP 404 with the standard error envelope

### Requirement: Component and schema discovery
The system SHALL expose registered component metadata and the generated full Engine configuration JSON Schema through versioned API endpoints.

#### Scenario: Discover components
- **WHEN** a client requests the component catalogue
- **THEN** the response groups registered input, output, processor, buffer, and codec metadata with descriptions, schemas, and examples when available

#### Scenario: Retrieve configuration schema
- **WHEN** a client requests the Engine schema
- **THEN** the response contains the same registered-component-aware schema used by the CLI schema command

### Requirement: Standard API errors

Control API failures SHALL use a consistent JSON error envelope containing an error code, human-readable message, and optional field or Stream ID context. A failed conditional write, such as a generation or version conflict, SHALL return a non-2xx conflict status carrying the expected and observed values, and SHALL NOT be reported as a generic invalid request.

#### Scenario: Invalid request

- **WHEN** a client submits an invalid Stream ID or malformed request body
- **THEN** the response contains a non-2xx status and the standard error envelope

#### Scenario: Concurrent mutation conflicts with a conditional write

- **WHEN** a request carrying an expected generation targets a resource whose generation has since advanced
- **THEN** the response is a precondition-failed envelope naming the expected and observed generation, and the stored resource keeps the newer state

### Requirement: Job upgrade and rollback SHALL fence the whole written record

A Job upgrade or rollback SHALL write the Job through a conditional update that fences the generation the handler read, and SHALL NOT write the recovery/checkpoint pointer, which is owned by the checkpoint path: a checkpoint moves it without bumping the generation, so a handler copying its earlier read back would regress recovery to a pointer retention may already have deleted. The version and spec a rollback restores SHALL be written, and artifact selection SHALL filter recovery candidates by job version and state format so a pointer the restored version cannot use is reported instead of silently ignored.

#### Scenario: A checkpoint lands during a rollback

- **WHEN** a checkpoint report moves the Job's recovery pointer after a rollback handler read the Job and before it writes
- **THEN** the conditional write leaves the newer pointer untouched, and the Job's recovery selection never regresses to the pointer the handler read

#### Scenario: Concurrent desired-state change

- **WHEN** a desired-state change bumps the Job's generation between a rollback handler's read and its write
- **THEN** the write fails with a conflict, the newer desired state is kept, and the operator can retry against the fresh generation

### Requirement: Upgrade and rollback SHALL validate state compatibility equally

An upgrade and a rollback SHALL both validate the selected artifact's state format against the Job's declared state format before accepting the request, and SHALL reject an incompatible target with an explicit error. A rollback SHALL NOT be accepted when the target version's state format the Job cannot restore.

#### Scenario: Rollback to an incompatible state format

- **WHEN** an operator requests a rollback to a Job version whose declared state format differs from the artifact the Job would restore
- **THEN** the API rejects the request with the state-format incompatibility error and leaves the Job unchanged, instead of accepting it and degrading to a stateless restart

#### Scenario: Rollback to a compatible state format

- **WHEN** the requested target version declares the same state format as the Job's current artifact
- **THEN** the rollback is accepted and the Job's recovery selection uses the restored version's spec

### Requirement: Atomic job upgrade mode and orchestration conflicts
The job upgrade endpoint SHALL accept an atomic mode that performs savepoint, version commit, and recovery start as one supervised orchestration, returning 202 with an orchestration id and phase, plus endpoints to read one orchestration's status and to invoke its actions (pause, resume, cancel, rollback). While an orchestration is non-terminal for a Job, job-level mutations — desired-state changes, job actions, and further upgrades — SHALL be rejected with `orchestration_in_progress`, without affecting the running orchestration. The existing stopped-mode upgrade and rollback contracts SHALL remain available and unchanged.

#### Scenario: Atomic upgrade is initiated
- **WHEN** the operator POSTs an atomic-mode upgrade for a running Job that passes validation and exclusivity
- **THEN** the response is 202 carrying the orchestration id and current phase, and the stop-the-world preconditions do not apply to this mode

#### Scenario: Desired-state change conflicts with an active orchestration
- **WHEN** the operator PUTs a desired-state change for a Job with a non-terminal orchestration
- **THEN** the request fails with `orchestration_in_progress` and the orchestration continues unaffected

#### Scenario: Orchestration status and actions are addressable
- **WHEN** the operator GETs the orchestration status or POSTs an action for it
- **THEN** the status exposes phase, savepoint reference, deadline, and last error, and actions follow terminal-state rejection and paused-only resume rules

#### Scenario: Stopped-mode upgrade is unchanged
- **WHEN** the operator POSTs a stopped-mode upgrade without an active orchestration
- **THEN** the request is governed by the existing stopped-and-converged preconditions and fencing behavior

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


### Requirement: Server module reorganization SHALL preserve the API surface and agent entry points
控制面服务端内部模块结构的重组（handler 按路由域拆分到 `api/`、Agent 按关注点拆分到 `agent/`）SHALL 保持对外行为与公共入口不变：`router`/`hub_router`/`observability_router`/`serve`/`serve_hub` 与 `agent::run` 等公共函数的路径与签名 SHALL 不因内部重组而变化，全部路由（本地 API 与 Hub API）的 HTTP 契约 SHALL 原样保持，Agent 命令分发的字符串协议 SHALL 逐字不变。

#### Scenario: handler 拆分后路由契约不变
- **WHEN** lib.rs 中的 handler 被重组进 `api/` 子模块
- **THEN** 本地控制面 API 与 Hub API 的全部路由路径、方法、请求/响应形状与鉴权行为不变，既有 API 契约测试全部通过

#### Scenario: Agent 拆分后命令协议不变
- **WHEN** agent.rs 被重组进 `agent/` 目录模块
- **THEN** Hub 下发的字符串命令（job_start/job_stop/job_checkpoint/validate_configuration 等）的匹配与执行语义逐字不变
