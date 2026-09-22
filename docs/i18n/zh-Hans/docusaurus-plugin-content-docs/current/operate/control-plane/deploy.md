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

### OIDC JWT 联邦

除了(或配合)静态运维凭据,Hub 还接受由组织 OIDC 身份提供方签发的 bearer JWT。通过环境变量配置:

| 变量 | 必填 | 说明 |
|----------|----------|-------------|
| `ARKFLOW_OIDC_ISSUER` | 是 | 令牌签发方,必须与令牌的 `iss` claim 一致。 |
| `ARKFLOW_OIDC_AUDIENCE` | 是 | Hub 期望的 `aud` claim。 |
| `ARKFLOW_OIDC_JWKS_URL` | 否 | JWKS 端点;缺省为 `{issuer}/.well-known/jwks.json`。 |
| `ARKFLOW_OIDC_ROLE_CLAIM` | 否 | 携带角色的 claim;缺省为 `roles`。 |
| `ARKFLOW_OIDC_SCOPES_CLAIM` | 否 | 携带资源 scope 的 claim;缺省为 `scopes`。 |
| `ARKFLOW_OIDC_CLIENT_ID` | 登录流 | 浏览器授权码流的 OAuth2 client id。 |
| `ARKFLOW_OIDC_CLIENT_SECRET` | 登录流 | OAuth2 client secret。 |
| `ARKFLOW_OIDC_REDIRECT_URI` | 登录流 | 已注册的重定向 URI,例如 `https://hub.example.com/api/v1/auth/oidc/callback`。 |

三个 client 变量齐备时,Hub 会在启动时通过 discovery 获取 IdP 的
授权/令牌端点,并提供浏览器登录流:

- `GET /api/v1/auth/oidc/login` —— 302 跳转到身份提供方,携带绑定
  HttpOnly cookie 的随机 `state`。
- `GET /api/v1/auth/oidc/callback?code&state` —— 用 code 换取 id_token,
  沿用与 bearer JWT 相同的验证管线,创建 8 小时服务端会话,并写入
  HttpOnly 的 `arkflow_session` cookie。
- `GET /api/v1/auth/oidc/logout` —— 删除服务端会话并清除 cookie。

浏览器会话与其他凭据走同一套 RBAC 模型(角色来自角色 claim)。会话
保存在内存中:重启 Hub 会强制所有人重新登录,也没有吊销列表——建议在
身份提供方侧签发短时令牌。未配置 client 变量时 Hub 保持仅 bearer 模式,
登录路由不存在。

claims 映射到既有 RBAC 模型:`sub` 作为主 id;角色 claim 接受数组或单个字符串(`admin`、`operator`、`viewer`,取最高匹配角色);scope claim(数组或逗号分隔)沿用与静态凭据相同的 `type=id` 文法,例如 `["node=node-a", "stream"]`。仅接受非对称算法(ES256/RS256);签名、有效期、签发方与受众全部校验,JWKS 带缓存并在出现未知 key id 时按需刷新。建议在身份提供方侧签发短时令牌——Hub 没有吊销列表。配置了静态凭据时它仍然生效,因此自动化用的应急令牌可以保留,人工访问则迁移到身份提供方。

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
