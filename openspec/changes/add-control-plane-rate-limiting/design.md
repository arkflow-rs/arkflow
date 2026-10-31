# Design: add-control-plane-rate-limiting

## Context

两条 API 面均为 axum + tower middleware 洋葱结构：本地 `router()`（`api/mod.rs:409`，`route_layer` 挂 `local_auth_middleware`）与 Hub `hub_router()`（`api/mod.rs:618`，`operator_auth_middleware` + standby allowlist + agent 豁免清单 `OPERATOR_AUTH_EXEMPT`）。凭证呈现已统一：`bearer()`（`api/mod.rs:1160`）把 Authorization header 与 `arkflow_session` cookie 归一为可选 token 串（cookie → `session:<id>`）；agent 面经 `bearer_session_token()`（`agent_endpoints.rs:60`）+ 旧通道 query token。指标渲染有 `storage_families()`（`metrics.rs:264`）先例。`sha2` 已是 workspace 依赖且 `arkflow-server` 已引用。

## Goals / Non-Goals

**Goals:**
- 任意凭证身份的请求速率有上界；超限请求不进 handler，拿到可重试语义（429 + Retry-After）。
- 默认开启且默认限值对合法负载（含 Agent 默认 1s 轮询 `api/mod.rs:338`）有 ≥2 个数量级富余；`enabled: false` 行为逐字节回退。
- 被拒请求可观测（Prometheus counter），桶表内存有界。

**Non-Goals:**
- 资源配额、IP 限速、401 暴破防护、分布式限速、OIDC 多会话聚合、运行时热更、429 审计（见 proposal Non-goals）。

## Decisions

- **D1 自研令牌桶，零新增依赖**：`governor`/`tower::limit` 要么引入新依赖栈，要么是并发数限制而非速率桶。自研约 80 行：每桶 `last_refill: Instant` + `tokens: f64`，惰性补给 `tokens = min(burst, tokens + elapsed*rps)`；`Mutex<HashMap<Key, Bucket>>`。速率参数是部署配置不是热点调参，精度够用；确定性测试友好（可注入时钟）。
- **D2 分桶键 = `bearer()` 输出的 SHA-256 截断（16 字节 hex）**：不落凭证原文（日志/崩溃转储不含凭据）；静态/结构化凭证 1 凭证 1 桶（= per-principal，因为凭证与 principal 1:1）；OIDC 浏览器会话 cookie 归一为 `session:<id>` 后同键处理（= per-session，聚合为 Non-goal）；无凭证请求（insecure_local/loopback）共享 `anonymous` 桶。agent 面单独键空间：`node:` 前缀 + `bearer_session_token()` 或旧通道 query token 的哈希——两通道同一节点同桶（legacy agent 不因升级窗口被双计）。
- **D3 中间件位置 = 认证层内侧、版本化前缀内**：本地挂 `.route_layer`（认证之后，复用同一分桶逻辑）；Hub 挂 operator 认证层内侧 + **agent 路由独立覆盖**——agent 路由豁免 operator 认证（`OPERATOR_AUTH_EXEMPT`），限速中间件按路径前缀切换键空间（`/agent/` → node 桶，其余 → principal 桶），豁免清单（健康探针/metrics/OIDC 登录）不经过限速器，与今日认证豁免边界逐位一致。
- **D4 429 = problem+json `rate_limited` + `Retry-After`（向上取整秒）**：复用既有 `problem()` 助手形状；不审计、不计入操作历史。
- **D5 桶表有界**：常量 `MAX_BUCKETS = 1024`（256 节点上限 × 4 富余）。满时新 key 落入共享 catch-all 桶（按 `principal_rps` 计），使伪造海量凭证的洪泛退化为共享慢通道而非内存增长；既有桶惰性淘汰（重置时刻超过 `burst/rps` × 2 未活动即回收，随请求路径顺带清扫，不 spawn 后台任务）。
- **D6 配置节 serde default 双向兼容**：`ServerConfig.rate_limit: RateLimitConfig { enabled: bool = true, ... }`，本地 YAML 暴露为 `health_check.rate_limit`（`control_api` 经 serde flatten 并入 `health_check`，无独立嵌套层）。Hub 与本地面同构读取；默认值论证：人机交互 ≤1 rps，脚本 ≤10 rps，100 rps 已是 10× 富余；node 面覆盖 poll(1s)+heartbeat+report+per-job observations，500 rps 富余 2 个数量级。
- **D7 指标**：限速器持 `AtomicU64`（plane × key_kind 三态：`principal`/`node`/`catch_all`），`metrics.rs` 新 family `arkflow_control_plane_rate_limit_rejections_total`，低基数标签封闭。

## Risks / Trade-offs

- **令牌桶精度**：f64 计数在极低速率下有浮点误差——限速语义是上界而非精确计量，可接受；测试用毫秒级注入时钟断言边界。
- **Mutex 热点**：所有请求过同一把桶表锁——桶操作是 O(1) 纳秒级，相对认证（OIDC JWKS/存储 actor 往返）可忽略；256 节点 × 数 rps 的真实规模远离锁竞争区。
- **catch-all 桶误伤**：桶表满时新凭证与攻击者共享慢桶——`MAX_BUCKETS=1024` 下正常部署（≤256 凭证 + 会话）触不到；触发即意味着正在被伪造凭证洪泛，慢比崩好。
- **默认开启改变行为**：限值富余 2 个数量级，唯一可感知场景是既有部署存在 >100 rps/凭证 的负载——那正是本变更要拦的负载；`enabled: false` 是逃生门，文档写明。
