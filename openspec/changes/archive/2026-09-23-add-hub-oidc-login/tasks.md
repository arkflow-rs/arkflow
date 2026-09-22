# Tasks: add-hub-oidc-login

## 1. Federation 与登录流实现

- [x] 1.1 `oidc.rs` 扩展 `OidcFederation`：from_env（async discovery、client 凭据三件套判定、降级告警）+ 会话表（OsRng 随机 id、8h TTL 惰性清理）+ create/resolve_session API；Hub 持有 `Arc<OidcFederation>`（原 authenticator 字段迁移）
- [x] 1.2 登录路由：`GET /auth/oidc/login`（state cookie + 302 授权端点）、`GET /auth/oidc/callback`（state 校验 → code 换 id_token → 验证 → 会话 → cookie → 303）、`GET /auth/oidc/logout`（清会话 + cookie）；仅登录流启用时注册
- [x] 1.3 会话参与授权：`bearer()` 回退 `arkflow_session` cookie（`session:` 前缀，返回类型改 Option<String>，调用点机械更新）、`operator_principal` 会话解析分支
- [x] 1.4 mock IdP 全链路测试：login 重定向与 state cookie、callback 全链路（discovery→token 交换→id_token 验证→会话）、会话参与授权、state 不匹配 401、logout、仅 bearer 模式路由不存在、未配置零行为变化

## 2. 文档

- [x] 2.1 控制面部署文档 OIDC 节扩展登录流（新增环境变量表、会话语义、Console 入口说明，en/zh）

## 3. 全量验证与归档

- [x] 3.1 `cargo test --workspace --all-targets` 全绿；`cargo clippy --workspace --all-targets` 无新告警
- [x] 3.2 对照 specs/hub-oidc-auth（delta）场景逐条核对；`openspec validate add-hub-oidc-login` 通过
- [x] 3.3 同步主 spec `openspec/specs/hub-oidc-auth/spec.md`（合并 delta）、归档 change、更新 PLANNING.md、提交
