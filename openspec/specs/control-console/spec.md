# control-console Specification

## Purpose
TBD - created by archiving change add-control-plane. Update Purpose after archive.
## Requirements
### Requirement: Console system dashboard
The Web Console SHALL display Engine state, Stream counts by lifecycle state, aggregate activity metrics, and recent errors using the Control API.

#### Scenario: Open dashboard
- **WHEN** the operator opens the console against a reachable Engine
- **THEN** the dashboard renders current system and Stream summary data and indicates API errors clearly

### Requirement: Stream inspection and controls
The Web Console SHALL provide Stream list and detail views with topology summary, state, metrics, recent errors, and start/stop/restart actions.

#### Scenario: Restart from Stream detail
- **WHEN** the operator confirms a restart action
- **THEN** the console submits the versioned API command, shows the operation state, and refreshes the Stream status

### Requirement: Schema-driven configuration editing
The Web Console SHALL provide configuration text editing and SHALL use the API-provided JSON Schema and component metadata for validation, completion, and component guidance.

#### Scenario: Invalid configuration feedback
- **WHEN** the operator edits an invalid configuration and requests validation
- **THEN** the console displays structured validation errors with their configuration paths and does not offer a publish action as successful

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

### Requirement: Safe display of secrets
The Web Console SHALL render redacted values returned by the API and SHALL NOT expose plaintext credentials in browser logs, URLs, or client-side error messages.

#### Scenario: View configured connector credentials
- **WHEN** the operator opens a connector configuration containing credentials
- **THEN** the console displays redaction markers rather than secret values

### Requirement: Credential material SHALL NOT enter the image build context or the repository
The console build chain SHALL prevent accidental credential spread: the `console/` Docker build context SHALL exclude `.env*` files (and `node_modules`, `dist`) via `.dockerignore`; repository ignore rules SHALL cover `.env` and `.env.*` while keeping `.env.example` tracked. Injecting the static token on a controlled build SHALL go through the explicit `ARG VITE_API_TOKEN` channel rather than a `.env` file placed in the build directory. The example configuration SHALL warn that the static token is inlined into the public JavaScript bundle, is for trusted networks only, and that production deployments should use OIDC.

#### Scenario: Local .env stays out of the image
- **WHEN** a `.env` file containing `VITE_API_TOKEN` exists in `console/` and a Docker image build runs
- **THEN** the file is excluded from the build context and appears in no image layer

#### Scenario: Static token injection is explicit
- **WHEN** a controlled build needs the static token
- **THEN** it is passed via `--build-arg VITE_API_TOKEN=...` (the Dockerfile declares the `ARG`), leaving an auditable build command with no dependency on files in the build directory

#### Scenario: A real .env is never committed
- **WHEN** a developer creates `console/.env` (or any `.env.*` other than `.env.example`) and runs `git add`
- **THEN** ignore rules keep the credential out of the repository

#### Scenario: The example file carries the exposure warning
- **WHEN** someone consults `console/.env.example`
- **THEN** it states that the static token is inlined into the public bundle, is for trusted networks only, and that production should use OIDC

### Requirement: console 依赖升级保持构建与测试契约

console 的开发依赖升级与 lockfile 再生成 SHALL 保持既有构建与测试契约：`tsc -b && vite build` 成功、vitest 全部用例通过；lockfile 再生成 SHALL 仅改变 devDependencies 邻域的解析结果，SHALL NOT 引入新的运行时依赖（dependencies 面不变）。

#### Scenario: 升级后构建与测试全绿

- **WHEN** vitest 升至修复版（^4.1.11）并重新生成 lockfile 后执行 `npm run build` 与 `npm test`
- **THEN** TypeScript 编译与 Vite 构建成功，全部 vitest 用例通过

#### Scenario: 运行时依赖面不变

- **WHEN** 对比升级前后的 console lockfile
- **THEN** `dependencies`（运行时）声明的解析保持不变，差异仅出现在 devDependencies 及其传递闭包

### Requirement: Console requests SHALL time out and SSE reconnects SHALL back off
Console 的 API 请求层 SHALL 对每个请求施加默认超时（30 秒，AbortSignal）并在超时时给出可读错误；调用方传入的中止信号 SHALL 与超时信号合并生效。SSE 事件流断线重连 SHALL 使用指数退避（1 秒起步、上限 30 秒），并在成功重新建立后重置退避；连接黑洞 MUST NOT 使 UI 无限期冻结。

#### Scenario: 连接黑洞在超时后报错
- **WHEN** API 请求因服务端不可达而挂起
- **THEN** 请求在 30 秒超时后以可读错误失败，UI 不永久冻结

#### Scenario: SSE 断线指数退避
- **WHEN** SSE 连接断开且连续重连失败
- **THEN** 重连间隔按 1s → 2s → 4s … 指数增长直至 30 秒上限；连接成功后间隔重置回 1 秒

