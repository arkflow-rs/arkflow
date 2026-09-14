## Why

ArkFlow 目前由 `Engine::run()` 一次性构建并启动全部 Stream（`crates/arkflow-core/src/engine/mod.rs:210-288`），运行时只保存全局健康状态，无法通过接口查看单条 Stream、获取运行指标或执行启停/重启操作。现有 HTTP 服务仅提供健康、就绪和存活检查（`crates/arkflow-core/src/engine/mod.rs:91-139`），而组件注册表和配置 JSON Schema 已经具备驱动控制台编辑器的基础（`crates/arkflow-core/src/component/mod.rs:20-28,299-355`）。

现在补齐单节点控制面，可以把 ArkFlow 从配置文件驱动的进程提升为可观察、可管理的流处理服务，同时保留现有 WAL 恢复和至少一次投递语义。

## What Changes

- 增加统一的 Control API Server，与现有 health/readiness/liveness 接口共用 HTTP Server 和生命周期。
- 为 Stream 引入稳定 ID、运行状态、独立取消令牌、任务监督和基础运行指标。
- 增加 Stream 查询、启动、停止和重启接口；单条 Stream 的操作不得影响其他 Stream。
- 增加配置读取、校验、版本保存、应用和回滚接口；配置应用采用先校验/构建、再替换运行实例的策略。
- 暴露组件注册表和完整 JSON Schema，支持控制台动态展示组件配置。
- 增加独立的 React Web Console，提供 Dashboard、Stream 列表/详情、配置编辑和组件浏览功能。
- 增加 Prometheus 指标、最近运行错误和基础事件查询。
- 对配置 API 的敏感字段进行脱敏；控制面默认面向单节点本地或受保护网络部署。

### Non-goals

- 不实现多节点集群、节点发现、共识或远程集群编排。
- 不保证控制面操作提供 exactly-once 语义；现有 WAL 和输出端语义保持不变。
- 不实现任意组件对象的原地热替换；配置变化通过 Stream 停止、构建和重新启动完成。
- 不在本变更中实现完整 OIDC、RBAC、多租户和审计平台。
- 不实现复杂的可视化 DAG 拖拽编排。

## Capabilities

### New Capabilities

- `control-plane-api`: 提供系统、Stream、组件、Schema、指标和运行事件的 HTTP API。
- `stream-runtime-control`: 管理具名 Stream 的生命周期、状态、任务监督和基础运行指标。
- `configuration-management`: 提供配置校验、版本、应用、回滚和敏感字段脱敏。
- `control-console`: 提供独立 Web 控制台，消费 Control API 完成观察和管理操作。

### Modified Capabilities

- None.

## Impact

- 影响 `arkflow-core` 的 Engine、StreamConfig 和运行时生命周期模型。
- 新增或启用 `arkflow-server` crate，承载 Control API 和统一 HTTP 路由。
- 新增独立 `console/` 前端工程及其构建/部署配置。
- 需要扩展配置 Schema、Stream 配置序列化兼容逻辑和 Prometheus 指标依赖。
- 需要新增 API、生命周期、配置回滚、WAL 重启和前端端到端测试。
