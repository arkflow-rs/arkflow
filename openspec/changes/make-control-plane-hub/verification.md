# Verification record

## Verified

- Hub registration, authenticated sessions, lease expiry, reconnect, bounded
  per-node command queues, duplicate-command idempotency, and node isolation are
  covered by `arkflow-server` tests.
- A real Hub process and Agent process were started locally. The Hub observed
  `node-a` as online, aggregated its `demo-events` Stream and metrics, accepted
  a node-targeted restart, and recorded the operation as succeeded with queued,
  dispatched, acknowledged, and terminal timestamps.
- A second run where the Agent process was terminated demonstrated stale lease
  handling and `node_unavailable` reconciliation.
- Node configuration is reported through the Agent after redaction; the Hub
  configuration endpoint never exposes the configured node or API tokens.
- Rust server/core checks and console typecheck, tests, and production build
  pass. Existing plugin warnings are unrelated to this change.

## Deliberate limitations

- Hub registry, operation, event, and metric state is in-memory and is rebuilt
  from node reports after Hub restart.
- The current automated end-to-end smoke uses one live process pair; multi-node
  behavior is covered by Hub isolation tests and can be extended with a
  process-level two-node harness.
- Configuration version history remains durable on each node; the Hub exposes
  the current redacted snapshot and dispatches apply/rollback commands.
