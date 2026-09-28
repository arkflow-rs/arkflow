## 1. Storage lease contract

- [x] 1.1 Add `cp_hub_lease` DDL + `try_acquire_hub_lease` / `renew_hub_lease` / `release_hub_lease` to the SQLite backend with CAS semantics (epoch bump on takeover, self-acquire idempotent)
- [x] 1.2 Add the same DDL and three methods to the PostgreSQL backend (SQLSTATE-mapped errors, BIGINT casts)
- [x] 1.3 Extend `StorageBackend` trait + `ControlPlaneStore` dispatch + `StorageActor` public wrappers; keep migrate_tool untouched (lease row is runtime state)
- [x] 1.4 Contract tests: expiry takeover, refusal while held, renew success/lost, self-acquire idempotence, release immediate expiry — parameterized SQLite offline + PG behind `ARKFLOW_TEST_POSTGRES_URL`

## 2. Leadership state machine

- [x] 2.1 Add `HubHaConfig` (enabled/lease_ttl_ms/holder_id) to `HubConfig` with defaults (disabled, 15s, generated holder id); env plumbing (`ARKFLOW_HUB_HA_*`) in the server binary
- [x] 2.2 Add `Leadership` state (Disabled/Standby/Leader with epoch+since) and `is_leader()` on `Hub`; gate sweep/reconcile/maintenance tick bodies on leadership in `serve_hub`
- [x] 2.3 Implement the lease task loop (ttl/3 cadence): standby `try_acquire` → promote; leader `renew` → continue or step down; graceful shutdown releases the lease
- [x] 2.4 Implement promote(): clear + reload durable view (recover_persisted_state / restore_persisted_operations made overwrite-safe), clear node registry + placement_order, then flip to Leader; on failure release and stay standby
- [x] 2.5 Startup validation: `ha.enabled` without storage fails before bind; SQLite backend with HA logs a warning

## 3. HTTP surface

- [x] 3.1 Standby gate middleware on `hub_router`: allowlist health/readiness/liveness/metrics, everything else 503 `hub_standby` with Retry-After; Disabled passes through untouched
- [x] 3.2 readiness payload gains `ha { enabled, role, epoch }`; standby readiness is 503 with role
- [x] 3.3 Metrics: leadership role gauge, epoch gauge, transitions counter; transition emits HubEvent + tracing

## 4. Tests

- [x] 4.1 Unit/integration: standby rejects mutating + agent routes (503, no side effects) while health endpoints answer; periodic loops idle on standby (no dispatch after N ticks)
- [x] 4.2 Failover test on one shared SQLite store: hub A leader → release-on-shutdown → hub B promotes within one probe, serves recovered jobs; expired-lease takeover path (A's loop stopped, lease expires, B acquires) also covered
- [x] 4.3 Stale-memory overwrite test: demoted hub re-promotes and its older in-memory entries are replaced by durable state
- [x] 4.4 `cargo test -p arkflow-server` green; full `cargo test --workspace --all-targets` + clippy clean

## 5. Docs

- [x] 5.1 Update `docs/docs/` HA/deployment page + zh-Hans counterpart: stage-2 lease election config, LB/readiness guidance, NTP assumption, PG-only production note, SQLite dev-only note
- [x] 5.2 Run `pnpm docs:check` and fix any anchors/sidebar issues
