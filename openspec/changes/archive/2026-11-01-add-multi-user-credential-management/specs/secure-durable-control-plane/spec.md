# Delta Spec: secure-durable-control-plane

## ADDED Requirements

### Requirement: Managed identity SHALL require durable storage

用户目录与托管凭证 SHALL 只存在于 Hub 的 durable storage：未配置 durable storage 的 volatile Hub MUST NOT 接受用户/凭证的创建或修改，SHALL 以稳定的 `identity_store_unavailable` 错误响应，且 SHALL NOT 在进程内建立可认证的托管身份。托管身份的访问判定（禁用、吊销、过期）SHALL 读取共享持久状态，使吊销在 Hub 重启与选主切换后仍然有效；身份写操作 SHALL 经租约 epoch 围栏，stale leader MUST NOT 写入。secure 模式（非回环绑定）的启动护栏 SHALL 保持既有语义：静态 operator 凭证仍为必需的 bootstrap/break-glass，OIDC 配置 MUST NOT 替代它。

#### Scenario: Volatile Hub refuses identity administration

- **WHEN** 未配置 durable storage 的 Hub 收到用户创建或凭证签发请求
- **THEN** 返回 503 `identity_store_unavailable`，且之后不存在可用该身份认证的凭证

#### Scenario: Revocation survives restart and failover

- **WHEN** 管理员吊销某条凭证或某用户令牌后 Hub 重启或由另一实例接任 leader
- **THEN** 被吊销的凭证与会话仍然无法通过认证

#### Scenario: Secure startup guard still requires the static credential

- **WHEN** 非回环 Hub 未配置静态 operator 凭证
- **THEN** 启动仍以既有的缺失凭据错误失败，即使持久库中已有启用状态的 Admin 用户

#### Scenario: Identity writes are fenced

- **WHEN** 已失去租约的 Hub 尝试写入用户或凭证
- **THEN** 存储层以 stale-leader 语义拒绝该写入，身份记录不变
