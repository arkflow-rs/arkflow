## Context

The Hub has a durable SQLite storage actor, but the standalone binary only uses it when `ARKFLOW_HUB_STORAGE` is set. It also derives authorization from optional tokens, so missing secrets become an implicit legacy-admin/local-trust mode even when the address is externally reachable. The server already knows its bind address before opening the listener, which is enough to enforce a safe startup policy without changing the in-process `Hub::new` test constructor.

## Goals / Non-Goals

**Goals:**

- Refuse unsafe external startup when storage or required credentials are absent.
- Preserve a deliberately explicit insecure-local mode for development and unit tests.
- Restore durable state before binding the listener or reporting readiness.
- Keep constant-time token comparison and existing session authentication.

**Non-Goals:**

- Multi-Hub HA, leader election, or distributed consensus.
- Certificate-based control-plane authentication.
- Changes to Agent session-token rotation.

## Decisions

1. **Determine security mode from bind address plus explicit opt-in.**
   Loopback addresses may run with `insecure_local: true`; non-loopback addresses require operator token, node token, and durable storage. A missing token is never converted to an Admin principal in secure mode. This is safer than a global default because local test routers can still construct `Hub::new` directly.

2. **Add explicit `insecure_local` and storage path/startup validation to `ServerConfig`.**
   The standalone binary reads `ARKFLOW_HUB_INSECURE_LOCAL`; config-file users can set the equivalent field. The validation checks `SocketAddr::ip().is_loopback()`, storage presence, and both token classes before `serve_hub` calls recovery or binds `TcpListener`.

3. **Keep storage recovery before readiness.**
   `serve_hub` opens the configured `ControlPlaneStore`, calls the existing persisted-state and operation restoration, and only then binds the listener. If storage recovery fails, startup fails rather than serving an incomplete in-memory view.

4. **Make missing credentials unauthorized, not legacy Admin, in secure mode.**
   `HubConfig` receives the explicit local-insecure flag. The authorization methods preserve legacy behavior only when that flag is true; otherwise `operator_principal(None)` and node registration without a configured token fail closed.

## Risks / Trade-offs

- [Risk] Existing deployments bind `0.0.0.0` without tokens. → They fail at startup with migration guidance instead of becoming accidentally public Admin endpoints.
- [Risk] Test helpers expect `Hub::new` to be unauthenticated. → Keep the constructor's explicit test semantics and enforce the policy at the standalone server boundary; add an explicit insecure config for integration servers.
- [Risk] SQLite storage is a single-writer/single-Hub design. → Document it as restart durability only and keep HA as a separate capability.
- [Risk] A storage path can be writable but empty after operator error. → Treat successful initialization as a valid new control-plane store, but expose readiness and audit events so operators can detect an unexpected empty database.

## Migration Plan

1. Add fields with safe local defaults and startup validation.
2. Set `ARKFLOW_HUB_STORAGE`, `ARKFLOW_OPERATOR_TOKEN`, and `ARKFLOW_NODE_TOKEN` before enabling external binds.
3. Verify restart restores Jobs/checkpoint pointers and readiness stays false during recovery.
4. Use `ARKFLOW_HUB_INSECURE_LOCAL=1` only for local development; never set it on externally bound addresses.
