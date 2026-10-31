# Proposal: add-control-plane-rate-limiting（控制面 per-principal/per-node 请求限速）

## Why

控制面对请求频率没有任何防护：两条 API 面（本地 standalone `crates/arkflow-server/src/api/mod.rs:409`、Hub `api/mod.rs:618`）仅有请求体大小限制（本地 1MB `api/mod.rs:464`、Hub 4MB `api/mod.rs:793`）与内存硬上限（`MAX_NODES=256`、`MAX_COMMANDS_PER_NODE=128`、`MAX_OPERATIONS=1024`，`hub.rs:59-61`），全仓 grep `rate.?limit|throttle` 在 `arkflow-server` 源码零命中。一个失控行回环（脚本 bug、被攻陷的凭证、失控的轮询）可以以任意速率冲击控制面——每个请求都要走认证解析（OIDC 路径含 JWKS 校验，`hub/operator.rs:50`）与存储 actor 往返，且 mutation 会放大为 Hub 命令派发。PLANNING 11.2-3 与 11.4 将其列为 P1「企业入场券」；用户已拍板**砍掉配额半边**（per-principal 资源配额不做），本变更只做请求限速。

## What Changes

- **进程内令牌桶限速中间件**：新增 tower 中间件，挂在两条 API 面的版本化前缀内（本地 `.route_layer` 内层、Hub 认证层内层），零新增依赖。按**凭证身份**分桶（SHA-256 截断为 key，不落原文）：
  - Hub operator 面：静态/结构化凭证按凭证分桶（1 凭证 = 1 principal），OIDC 浏览器会话按会话分桶（聚合单用户多会话为 Non-goal）；
  - Hub agent 面：按节点凭证分桶（1 节点 = 1 桶）——agent 路由豁免 operator 认证（`api/mod.rs:551`），是 Hub 上最该有洪泛护栏的面；
  - 本地面：按 bearer 凭证分桶，未配置凭证（loopback/insecure_local）共享单桶。
- **429 拒绝**：problem+json（`rate_limited`）+ `Retry-After`（向上取整秒）；被拒请求不进 handler、不计审计（噪声）。
- **桶容量有界**：追踪桶数上限（常量，溢出 key 共享一个 catch-all 桶），防高频伪造凭证把桶表打爆成内存 DoS。
- **配置**：`health_check.rate_limit`（control_api 节 flatten 并入 health_check）/ Hub 侧同构节——`enabled`（默认 `true`）、`principal_rps`+`principal_burst`（默认 100/200）、`node_rps`+`node_burst`（默认 500/1000，须覆盖默认 1s 轮询 `api/mod.rs:338` 下 register/heartbeat/report/commands/observations 的合法速率，富余 2 个数量级）。默认开启 + 富余默认值 = 正常部署无感；`enabled: false` 一键回退旧行为。
- **指标**：`arkflow_control_plane_rate_limit_rejections_total{plane,key_kind}` counter，接入既有 metrics 渲染（`metrics.rs` storage_families 先例）。
- **边界**：健康探针、`/metrics`、OIDC 登录流在外层路由或豁免清单（`api/mod.rs:551`），保持现状不限额（监控与准入通道不被限速器误伤）。

## Non-goals

- **per-principal 资源配额**（每 principal 可建 Job/Stream/节点数上限）——用户已拍板砍掉，另行评估。
- **按 IP 限速 / 401 暴破防护**——需要可信代理头语义（X-Forwarded-For），单机直连语义下不可靠，留待真实需求信号。
- **跨实例分布式限速**——单进程令牌桶；多 Hub 共享限速状态属 HA 后续。
- **OIDC 单用户多会话聚合成一个桶**——按会话分桶已封住单会话失控；跨会话聚合需要身份目录。
- **动态调参/管理 API**——限速参数是部署配置，不做运行时热更。
- **限速 429 审计**——攻击噪声不进审计流。

## Capabilities

### New Capabilities

- `control-plane-rate-limiting`: 控制面请求限速契约——分桶身份、令牌桶语义、429 响应形状、桶容量有界、配置开关与默认值、指标出口。

## Impact

- `crates/arkflow-server/src/`：`api/`（新 `rate_limit.rs` 中间件模块 + 两条路由接线）、`api/mod.rs`（`ServerConfig` 增 `rate_limit` 节，serde default 双向兼容）、`metrics.rs`（counter 出口）。
- 依赖：零新增（SHA-256 用既有 `sha2`，无则 std 哈希替代）。
- 兼容性：默认开启但限值远超合法负载；`enabled: false` 逐字节回退；响应新增 429 状态码属契约扩展。
- 文档：`docs/docs/control-plane/http-api-v1.md` 429 语义、部署/运维页 en/zh、CHANGELOG。
