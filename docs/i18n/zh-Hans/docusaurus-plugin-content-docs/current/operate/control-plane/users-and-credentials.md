---
sidebar_position: 8
title: 用户与凭证
description: 管理 Hub 用户目录、签发与轮换 Bearer 凭证、绑定 OIDC subject，以及吊销会话。
---

# 用户与凭证

Hub 可以维护自己的**用户目录**：具名用户(带角色与资源 scope)、按用户签发的
不透明 Bearer **凭证**，以及持久化的浏览器**会话**。它位于单一静态 operator
凭证与完整 OIDC 联邦之间的中间层：让每个自然人或自动化拥有各自可吊销的身份，
而不必让所有调用方共用 `ARKFLOW_OPERATOR_TOKEN`。

该目录是 Hub 模式的能力，需要持久化存储(`ARKFLOW_HUB_STORAGE`)。易失
(volatile)Hub 无处保存用户，因此整个管理 API 返回 `503
identity_store_unavailable`，托管凭证也永远无法通过认证。未配置该能力的部署
行为完全不变：静态 operator 凭证仍是优先路径。

## 概念

| 概念 | 含义 |
|------|------|
| **用户(User)** | 稳定身份 `user_id`，拥有单一 `role`(`admin`、`operator` 或 `viewer`)、可选的资源 `scopes`、`display_name`、可选的已绑定 OIDC subject，以及 `disabled` 标记。 |
| **凭证(Credential)** | 签发给某个用户的不透明 Bearer token。Hub 只保存哈希；明文仅在签发或轮换时出现一次。凭证可带 `label` 与可选过期时间。 |
| **会话(Session)** | 由 OIDC 授权码流建立的浏览器登录。Hub 具备持久化存储时会话落盘(`cp_sessions`)，因此能跨 Hub 重启存活并可被集中吊销。 |
| **Scope** | `{resource_type, resource_id?}` 对，用于收窄用户可操作的对象。角色是全局的，scope 在其上做细化。 |
| **吊销阈值** | 用户上的 `tokens_valid_after_ms`。`iat` 早于该阈值的联邦令牌会被拒绝——这就是 JWT 吊销机制。 |

用户 ID 会出现在 URL 与审计记录中：1–64 个字符，仅小写字母、数字、`.`、
`_`、`-`，且以小写字母或数字开头。`display_name` 与凭证 `label` 最长 128 字符。
每个用户只持有一个角色(不按用户设置角色集合)；`admin` 是唯一持有
`manage_users` 动作的角色，因此只有 admin 能管理目录。

目录有上限：最多 **1024 个用户**，每用户最多 **16 个活跃凭证**(已吊销的不再计入)。
每个 OIDC subject 最多绑定到一个用户。

## 认证解析顺序

请求携带 Bearer 凭证时，Hub 按以下顺序解析：

1. **静态 operator 凭证**(`ARKFLOW_OPERATOR_TOKEN`)——兼容/应急(break-glass)
   路径。匹配即胜出；不匹配则落到下一层(格式错误的结构化凭证从不匹配)。
2. **托管凭证**——任何以 `ark_` 开头的 token 按 `ark_<credential_id>_<secret>`
   解析并查询用户目录。托管 token **绝不回退**到 OIDC：未知、已吊销、已过期、
   用户已禁用或存储不可达的查询一律硬性 `401`。Hub 保存
   `SHA-256(credential_id ‖ 0x00 ‖ secret)`，并以常量时间比较。
3. **浏览器会话**——`arkflow_session` cookie 解析为持久化会话(易失 Hub 上则
   解析为内存映射)；已吊销、已过期、用户已禁用、用户已删除的会话都会被拒绝。
4. **OIDC JWT**——校验来自所配置身份提供方的 Bearer JWT，再按下文的托管目录
   规则映射。

所有身份查询失败都是**默认拒绝(fail-closed)**：凭证解析期间发生存储错误会拒绝
请求，而不是跳过认证。

## 引导与应急访问

系统没有内置的首个用户。用下列任一方式引导：

- **静态 operator 凭证(推荐)。** 以 `ARKFLOW_OPERATOR_TOKEN` 启动 Hub，再用它
  创建第一个 admin 用户并签发凭证：

  ```bash
  export HUB=https://hub.example.com
  export BOOTSTRAP="$ARKFLOW_OPERATOR_TOKEN"

  curl -fsS -X POST "$HUB/api/v1/users" \
    -H "Authorization: Bearer $BOOTSTRAP" -H 'Content-Type: application/json' \
    -d '{"user_id":"alice","display_name":"Alice","role":"admin","scopes":[]}'

  curl -fsS -X POST "$HUB/api/v1/users/alice/credentials" \
    -H "Authorization: Bearer $BOOTSTRAP" -H 'Content-Type: application/json' \
    -d '{"label":"alice-laptop"}'
  ```

  请把静态凭证保留为**应急(break-glass)**凭证：只要它存在，「最后一个启用的
  admin」不变量就会被豁免，因此即使一次误操作本会锁死所有目录 admin，你仍能恢复。

- **OIDC 引导。** 在没有静态凭证时，**尚未绑定**目录用户的联邦 subject 保持
  按 claim 映射角色(见下文)。因此一个 `admin` subject 可以创建首批用户并完成
  绑定；此后对这些 subject 而言目录就是权威来源。

如果移除静态凭证，最后 admin 不变量即开始强制执行：Hub 会以
`409 last_admin_required` 拒绝禁用、删除或降级仅剩的启用 admin。除此之外没有
其他进入方式(只能做存储层恢复)——因此要么保留静态凭证，要么保证始终存在至少
一个启用的 admin 用户。

在**易失 Hub** 上管理 API 不可用(`identity_store_unavailable`)；在 Hub 拥有
持久化存储之前请使用静态 operator 凭证。

## 签发、轮换与吊销凭证

凭证按用户管理；明文 token **仅**在签发/轮换响应中返回，之后无法再读取。

| 操作 | 路由 | 行为 |
|------|------|------|
| 列出元数据 | `GET /api/v1/users/{id}/credentials` | `credential_id`、`label`、各时间戳、过期与吊销信息——绝非 token 或哈希。用户不存在时返回 `404 user_not_found`。 |
| 签发 | `POST /api/v1/users/{id}/credentials` | `201` 返回 `{credential, token}`。可选 `label` 与 `expires_at_ms`。 |
| 轮换 | `POST /api/v1/credentials/{credential_id}/rotate` | `200` 返回新的 `{credential, token}`；旧凭证在同一次存储变更中吊销。对已吊销凭证轮换会被拒绝。 |
| 吊销 | `DELETE /api/v1/credentials/{credential_id}` | 幂等。返回 `200 {"revoked": true\|false}`；重复调用保留首次吊销时间戳。 |

像使用静态凭证一样，用 `Authorization: Bearer <token>` 携带 token。吊销立即
生效：该 token 的下一个请求即 `401`。`last_used_at_ms` 在本版本中为 `null`——
使用时间戳只是响应形状中预留的字段，尚未记录。

建议：

- 每个客户端/主机签发独立凭证，这样吊销一个不会影响其他；`label` 写明其所在位置。
- 定期轮换，并在怀疑泄露时立即轮换。轮换在单次存储变更中吊销旧凭证并创建新凭证，
  因此旧 token 立即失效。
- 为短时自动化设置 `expires_at_ms` 以限制暴露窗口；过期凭证的行为与已吊销相同。
- 吊销一个凭证**不会**吊销该用户的其他凭证。

## OIDC 绑定与吊销 {#oidc-binding-and-revocation}

配置 OIDC 后(参见 [OIDC JWT 联邦](./deploy.md#oidc-jwt-federation))，可以通过
在用户记录上设置 `oidc_subject` 把目录登录绑定到身份提供方的 subject(它在目录内
唯一；`PATCH` 传入空字符串可清除绑定)。一旦绑定，目录即为权威来源：

- **角色与 scope 来自目录**，而非 token 的 claim。编辑用户会立即改变其下一个请求
  的授权结果。
- **已禁用**的绑定用户即便持有有效 JWT 也会被 `401` 拒绝。
- token 的 `iat` 会与 `tokens_valid_after_ms` 比较；早于该阈值(含 60 秒时钟偏斜
  容差)的令牌会被拒绝。这就是 `POST /api/v1/users/{id}/revoke-tokens` 无需等待
  过期即可使已签发 JWT 失效的方式。
- 绑定用户的 token **缺少 `iat`** 时会被拒绝——没有签发时间 Hub 无法评估吊销，
  因此默认拒绝。

未绑定的 OIDC subject 保持纯 claim 映射(角色 claim 与 scope claim)，所以目录是
可选启用的。设置 `ARKFLOW_OIDC_MANAGED_USERS_ONLY=true` 可要求每个联邦 subject
都必须绑定到目录用户；未绑定的 subject 将收到 `401`。默认值为 `false`，保留
claim 映射行为。

`POST /api/v1/users/{id}/revoke-tokens` 做两件事：把 `tokens_valid_after_ms`
提升到当前时间(使更早的联邦令牌失效)，并删除该用户所有已保存的浏览器会话。它返回
`{"revoked_sessions": n}`。它**不会**吊销托管凭证——托管凭证可单独吊销与轮换，
请对它们使用 `DELETE /api/v1/credentials/{credential_id}` 或轮换。

禁用用户、删除用户与 `revoke-tokens` 都立即生效：会话在每个请求上都要经过目录
解析，没有需要刷新的缓存。删除用户还会同时删除其凭证与会话。

## 浏览器会话持久化

OIDC 登录流会创建 8 小时的服务端会话 cookie(`arkflow_session`)。Hub 具备持久化
存储时会话行落盘，把运行故事从早先版本改变为：

- **重启幸存**：Hub 重启不再强制所有人重新登录；cookie 对同一存储继续有效。
- **集中吊销**：登出删除会话行；禁用/删除用户或调用 `revoke-tokens` 会使其会话在
  下一个请求即失效。
- **维护**：Hub 的周期性维护任务清扫过期与已吊销的会话行，表容量保持有界。
- **HA**：共用同一数据库的多个 Hub 实例共享会话表；在 leader 上创建的会话可在任何
  能服务 operator 流量的实例上解析(standby 仍返回 `503 hub_standby`)。
- **备份/恢复有意排除 `cp_sessions`**：会话 id 是活跃的 Bearer secret，灾难恢复后
  让旧登录复活是安全反模式。恢复后用户需重新登录。

在易失 Hub(无持久化存储)上，会话仍像以前一样保存在内存：重启 Hub 即登出所有人。

## HA、备份与恢复

- **HA**：目录位于共享控制面数据库中，因此基于租约的 HA 部署里每个 Hub 实例都能
  看到相同的用户、凭证与会话。身份变更与其他控制面写入一样受 fencing 保护(陈旧
  leader 收到 `503 stale_leader`)。
- **备份**：`cp_users` 与 `cp_credentials` 已纳入 backup/migrate/restore 的表
  顺序，因此灾难恢复快照会保留用户、角色、scope、OIDC 绑定、吊销阈值与凭证哈希。
  `cp_sessions` 有意排除(见上)。快照文件不含凭证明文(只有哈希)，但包含用户元数据
  与审计历史——请按数据库同等的密级保管。
- **Schema**：托管身份是存储 schema **v5**。打开旧库会自动创建身份表并写入新版本
  标记；被更新版本二进制标记过的数据库会在启动时被拒绝。

## 审计

身份变更以标量元数据记入 Hub 审计日志——绝不包含 token 材料或 secret 哈希。被接受的
变更会产生以下记录：

| 动作 | 触发时机 |
|------|----------|
| `user.create` | 创建用户。 |
| `user.update` | `PATCH /users/{id}` 修改用户。 |
| `user.delete` | 删除用户。 |
| `user.revoke_tokens` | 吊销联邦令牌与会话。 |
| `credential.create` | 签发凭证。 |
| `credential.rotate` | 轮换凭证(记录新凭证 id)。 |
| `credential.revoke` | 吊销凭证。 |

被拒绝的变更同样会审计:以同一动作写入 `outcome=rejected`,记录真实 actor、目标
用户/凭证,并把稳定的问题码作为 `failure_code`(例如 `last_admin_required`、
`credential_limit`、`user_not_found`、`oidc_subject_conflict`)。尚未到达身份存储就
失败的变更——例如用户 id 非法、角色未知等校验错误——同样记为 rejected。而授权拒绝
(缺少凭证,或主体没有 `manage_users`)由通用授权拒绝路径审计。可在
`GET /api/v1/audit` 查询审计轨迹。

## 错误码

| 状态 | 错误码 | 何时出现 |
|------|--------|----------|
| `400` | `invalid_user` | 用户 id 格式错误、角色未知、display name/label 过长、scope 无法解析，或 `PATCH` 为空。 |
| `400` | `invalid_credential` | 对已吊销凭证执行轮换。 |
| `401` | `unauthorized` | 没有有效凭证。 |
| `403` | `forbidden` | 已认证，但主体缺少 `manage_users`(仅 admin 持有)。 |
| `404` | `user_not_found` / `credential_not_found` | 用户或凭证不存在——包括对不存在的用户列出凭证。 |
| `409` | `last_admin_required` | 在没有静态 operator 凭证时，最后一个启用的 admin 将被禁用、删除或降级。 |
| `409` | `user_limit` | 已有 1024 个用户。 |
| `409` | `credential_limit` | 该用户已有 16 个活跃凭证。 |
| `409` | `oidc_subject_conflict` | 该 OIDC subject 已绑定到其他用户。 |
| `503` | `identity_store_unavailable` | Hub 没有持久化存储。 |

完整路由表、请求/响应形状与 `manage_users` 动作见
[HTTP API 参考](/zh-Hans/docs/reference/api#users-and-credentials)。

## 控制台

Web 控制台在独立的**用户**页(`/users`)暴露同一套能力,该页仅在 Hub 模式下对拥有
`manage_users` 的主体出现在导航中:列出/创建/编辑/禁用/删除用户,查看凭证元数据,
签发凭证并一次性展示明文,轮换、吊销,以及吊销用户的全部令牌。运行期 Bearer 凭证在
**设置**页输入;当 API 返回 `401` 且未配置 OIDC 登录流时,控制台 shell 会提示输入
token——交互细节与 token 存储边界见
[Web 控制台](./console.md#users-and-credentials)。

## 密级与运维提示

- **明文只有一次。** 把签发的 token 当作密码：立即复制到目标端的 secret store。
  Hub 无法再次展示；一旦丢失就轮换。
- **只存哈希。** 凭证哈希为 `SHA-256(credential_id ‖ 0x00 ‖ secret)`；被窃取的
  数据库里没有可直接使用的 Bearer token。会话行保存的是 cookie 值的哈希，而非
  cookie 本身。
- **TLS 比以往更重要。** Bearer 凭证每个请求都会过网络；请在 Hub 或可信代理处
  终止 TLS，且不要把凭证放进查询参数。
- **身份默认拒绝。** 认证期间的存储故障会拒绝请求，而不会静默降级为无认证或
  仅静态 token 模式。
- **静态 token 卫生。** 若保留 `ARKFLOW_OPERATOR_TOKEN` 作为应急凭证，请存入
  secret manager，并在用于恢复后轮换；它拥有全部权限并绕过目录。
- **引导窗口风险最高。** 创建首个 admin、用其托管凭证验证登录，然后再决定保留
  还是移除静态凭证。

## 相关页面

- [控制平面概览](./overview.md) —— Hub 架构与认证层次。
- [部署](./deploy.md) —— OIDC 配置、存储、备份/恢复。
- [HTTP API 参考](/zh-Hans/docs/reference/api#users-and-credentials) —— 路由与
  问题码。
- [Web 控制台](./console.md) —— 管理界面。
