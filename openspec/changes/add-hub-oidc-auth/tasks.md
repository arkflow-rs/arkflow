# Tasks: add-hub-oidc-auth

## 1. OIDC 验证器

- [ ] 1.1 workspace `Cargo.toml` 新增 `jsonwebtoken = "9"`；`arkflow-server` 引用
- [ ] 1.2 新建 `crates/arkflow-server/src/oidc.rs`：`OidcAuthenticator`（from_env、JWKS TTL 缓存 + kid 未命中刷新、算法白名单 ES256/RS256、claims → OperatorPrincipal 映射含 roles/scopes 解析）
- [ ] 1.3 Hub 接线：`Hub` 持有可选验证器 + `Hub::with_oidc`；`operator_principal`/`operator_authorized`/`operator_can`/`operator_can_scope` 异步化并回退 OIDC；lib.rs handler 与 hub.rs 测试调用点补 `.await`
- [ ] 1.4 装配：standalone `bin/arkflow-server.rs` 与单进程装配点接 `OidcAuthenticator::from_env()`
- [ ] 1.5 测试（内嵌 ES256 测试密钥 + mock JWKS）：三角色映射、多角色取高、scopes 映射、kid 未命中刷新、过期/错 aud/错 iss/坏签名/HS256 拒绝、无 sub 拒绝、静态凭据优先、未配置零行为变化

## 2. 文档

- [ ] 2.1 控制面文档（operate/control-plane）新增 OIDC 节（环境变量表、claim 约定、短时令牌建议、静态凭据共存）

## 3. 全量验证与归档

- [ ] 3.1 `cargo test --workspace --all-targets` 全绿；`cargo clippy --workspace --all-targets` 无新告警
- [ ] 3.2 对照 specs/hub-oidc-auth 场景逐条核对；`openspec validate add-hub-oidc-auth` 通过
- [ ] 3.3 同步主 spec `openspec/specs/hub-oidc-auth/spec.md`、归档 change、更新 PLANNING.md（RBAC 已实现修正 + OIDC 完成）、提交
