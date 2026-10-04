# hub-oidc-auth Delta

## ADDED Requirements

### Requirement: JWKS SHALL refresh periodically and revoke absent kids
Hub SHALL 按可配置间隔（默认 1 小时）周期性整表刷新 JWKS 缓存；刷新成功后，不在新表中的 known kid SHALL 自然吊销（后续校验按未知 kid 处理并 401）。刷新失败时 SHALL 保留旧表继续服务（provider 故障 MUST NOT 打掉 Hub）并记录 warn 日志；既有「未知 kid 触发即时刷新（节流保留）」语义保持不变。

#### Scenario: 密钥轮换后旧 kid 被吊销
- **WHEN** IdP 轮换签名密钥且 Hub 的周期刷新成功拉取到不含旧 kid 的新 JWKS
- **THEN** 旧 kid 签发的令牌在校验时按未知 kid 处理（触发即时刷新后仍不存在则 401）

#### Scenario: 刷新失败不打掉 Hub
- **WHEN** 周期刷新时 IdP 暂时不可达
- **THEN** Hub 保留旧 JWKS 表继续校验既有 kid，并记录刷新失败 warn 日志

#### Scenario: 未配置 OIDC 时零行为变化
- **WHEN** Hub 未启用 OIDC 认证
- **THEN** 不启动周期刷新任务，行为与既有零配置兼容语义一致
