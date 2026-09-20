---
sidebar_position: 2
---

# 控制平面部署

以 `cargo run -p arkflow-server --bin arkflow-server` 运行 Hub,并用 `health_check.hub_url`、`node_id` 与
`node_token` 启动每个计算节点。然后用 `cd console && npm ci && npm run build` 构建控制台,
并通过受保护的反向代理提供 `console/dist`。开发用 Vite 服务器把 `/api` 与 `/metrics` 代理到
`127.0.0.1:8080`;生产环境应保持同源路径,仅在 API 前缀不同时才设置 `VITE_API_BASE`。
仅当 ArkFlow 监听器配置了运维凭据时,才在受控的构建环境中设置 `VITE_API_TOKEN`。
兼容凭据可以是原始令牌(admin),也可以是 `principal|role|secret`,例如
`readonly|viewer|viewer-secret`;viewer 凭据可以读取资源与审计历史,但不能变更 Stream、节点或灰度发布。

自带的 `console/Dockerfile` 构建静态资源并通过 Nginx 提供服务。其 `/api/` 与 `/metrics`
位置代理到 `arkflow-hub:8080` 服务;请将其部署在带有 TLS 和认证层的私有网络上。
不要把 API 或携带令牌的控制台直接暴露在公共互联网上。ArkFlow 的默认绑定地址仅限本地。

## 从以健康检查为中心的控制台迁移

旧 UI 把后端当作健康与聚合流监控使用。面向资源的控制台使用 `/api/v1/system`、`/nodes`、`/streams`、
`/operations`、`/events`、`/configuration` 与 `/components`。生命周期请求是异步的,必须按操作 ID 轮询。
现有的 `/health`、`/readiness`、`/liveness`、`/metrics`、`/status` 与 `/config*`
路由作为兼容别名保留,但新的集成应使用资源端点。配置版本、操作、审计记录与有界事件历史在 Hub
模式下是持久的。灰度发布位于 `/api/v1/rollouts`;使用 action 端点来暂停、恢复、取消或创建回滚。
经过认证的 `/api/v1/events/stream` SSE 端点支持过滤与 `Last-Event-ID`;客户端在收到 `resync`
事件后必须重新加载 REST 快照。

对于反向代理,请不加缓冲地透传 `Authorization`、`X-Correlation-ID` 与 SSE 的 `text/event-stream`
响应。不要把凭据放在查询参数中。

## 升级混合机群

Agent 只在 `Authorization: Bearer` 头中出示会话凭据。Hub 仍接受旧版查询参数,以便旧版本的
Agent 在 Hub 升级后继续轮询;但升级后的 Agent 要求 Hub 是相同或更高版本:滚动升级时,先升级
Hub,再升级任何 Agent。Agent 重连使用随机化退避,因此 Hub 重启不会引发同步的重注册风暴。
Hub 会话 TTL(`health_check.agent_session_ttl_ms`,默认一小时)限制了泄露的会话凭据可认证的时长;
TTL 到期后 Agent 会透明地重新注册,因此请把它保持在最长预期命令(例如一次耗时的检查点)之上并留有余量,
以避免结果重复提交。
