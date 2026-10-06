## ADDED Requirements

### Requirement: 独立引擎控制平面 SHALL 应用启动护栏与 default-deny 认证
standalone 引擎的控制 API SHALL 在启动前校验绑定与凭据组合：绑定地址非回环且未配置 `api_token` 时 SHALL 拒绝启动，除非配置显式声明 `insecure_local`（沿用 Hub 既有语义）。运行期认证 SHALL default-deny：全部 API 端点（读与写）经统一认证中间件——配置了 `api_token` 时所有请求要求有效 Bearer token，未配置且回环绑定时中间件直通。handler 内散落的自查 SHALL 由中间件统一取代。两类豁免是**有意的**且不属于"全部 API 端点"：健康探针（`/health`、`/readiness`、`/liveness` 及其短别名）与顶层 `/metrics`（Prometheus 抓取端点，区别于 `/api/v1/metrics`）保持无认证——后者文档中已注明"勿暴露到非受信网络"。

#### Scenario: 非回环无 token 拒绝启动

- **WHEN** standalone 引擎配置 `health_check.address: "0.0.0.0:8080"` 且未设 `control_api.api_token`
- **THEN** 启动以明确的配置错误失败，文案指明非回环绑定必须设置 token 或显式 `insecure_local`

#### Scenario: 显式 insecure_local 可启动

- **WHEN** 非回环绑定 + 未设 token + 显式 `insecure_local`
- **THEN** 启动进行并输出显著警告（对齐 Hub 同语义的警告形态）

#### Scenario: 设 token 后读端点同样要求认证

- **WHEN** 配置了 `api_token`，请求 GET `/api/v1/streams`（或 events/system/metrics 任一读端点）且不带 Bearer token
- **THEN** 返回 401；带正确 token 返回 200（读写一致，无裸读端点）

#### Scenario: 回环无 token 保持零摩擦

- **WHEN** 默认回环绑定且未设 token
- **THEN** 本地读写请求照常通过（本地开发体验不变）

#### Scenario: 错误 token 常量时间拒绝

- **WHEN** 携带错误 Bearer token 的请求
- **THEN** 返回 401，比较保持常量时间（既有 `ct_eq` 实现不变）
