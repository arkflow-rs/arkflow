## 1. Hub protocol and domain models

- [x] 1.1 Define versioned agent DTOs for registration, session, heartbeat, full report, command, acknowledgement, and result.
- [x] 1.2 Add node identity, lease status, capability, and `(node_id, resource_id)` key types without Axum dependencies.
- [x] 1.3 Define Hub operation states for dispatch, acknowledgement, running, terminal, timeout, and node-unavailable outcomes.
- [x] 1.4 Add serialization/redaction tests for agent reports and secret-bearing configuration.

## 2. Hub node registry and command broker

- [x] 2.1 Implement a bounded `NodeRegistry` with register, heartbeat, report, reconnect, stale detection, and removal operations.
- [x] 2.2 Implement per-node authenticated sessions and lease expiration with configurable TTL.
- [x] 2.3 Implement a bounded command broker with per-node queues, idempotency keys, expiry, acknowledgement, and result reconciliation.
- [x] 2.4 Aggregate node Stream, operation, event, and metrics snapshots while preserving node identity and last-seen metadata.
- [x] 2.5 Add Hub tests for duplicate node IDs, stale leases, reconnects, duplicate commands, queue bounds, and node isolation.

## 3. Agent protocol and compute-node integration

- [x] 3.1 Add `NodeAgentConfig` with Hub URL, node ID, credentials, heartbeat/report intervals, and standalone switch.
- [x] 3.2 Implement the Agent registration, heartbeat, full-report, command-poll, acknowledgement, and result HTTP loops.
- [x] 3.3 Dispatch supported commands to the local `ControlPlane` and make command execution idempotent by Hub operation ID.
- [x] 3.4 Reconnect with bounded backoff, send a full report after reconnect, and expose disconnected/draining state.
- [x] 3.5 Refactor `arkflow` startup so Agent mode does not bind the Hub API listener; retain explicit standalone mode.
- [x] 3.6 Add Agent tests for registration failure, expired commands, duplicate delivery, reconnect, graceful shutdown, and redaction.

## 4. Hub HTTP API

- [x] 4.1 Add authenticated `/api/v1/agent/register`, `/heartbeat`, `/report`, `/commands`, and `/commands/{id}/result` endpoints.
- [x] 4.2 Change `/nodes` to return fleet resources with online/stale/offline state and lease metadata.
- [x] 4.3 Add node filters and target validation to Streams, operations, events, configuration, and metrics endpoints.
- [x] 4.4 Route lifecycle/configuration mutations through the command broker and reject ambiguous no-target mutations.
- [x] 4.5 Add contract tests for authentication, correlation, pagination, stale nodes, duplicate IDs, command dispatch, and compatibility health routes.

## 5. Fleet console

- [x] 5.1 Add typed Hub/Agent API models and centralized node selection/query state.
- [x] 5.2 Implement fleet overview with node health, lease age, capabilities, Stream totals, and stale/disconnected banners.
- [x] 5.3 Scope Runtime, Configuration, Events, Components, and Operations views by selected node and show target identity.
- [x] 5.4 Add operation dispatch/ack/running/terminal/node-unavailable feedback and disable unsafe actions for stale nodes.
- [x] 5.5 Add frontend tests for node switching, stale nodes, target validation, permission errors, operation polling, and redaction.

## 6. Deployment and migration

- [x] 6.1 Add Hub and compute-node configuration examples, startup commands, credentials, and lease settings.
- [x] 6.2 Update Docker/Nginx/reverse-proxy deployment so only the Hub is exposed to the console.
- [x] 6.3 Document standalone-to-Agent migration, rollback, compatibility health endpoints, and security posture.
- [x] 6.4 Add an end-to-end smoke test with one Hub and two nodes covering registration, aggregation, targeted lifecycle, reconnect, and graceful shutdown.

## 7. Verification

- [x] 7.1 Run formatter, core/server/agent tests, and workspace checks; distinguish pre-existing plugin warnings.
- [x] 7.2 Run console typecheck, component tests, and production build.
- [x] 7.3 Verify WAL replay and local data-plane behavior are unchanged when Hub connectivity is lost.
- [x] 7.4 Review all Hub/Agent requirements against implementation and record remaining limitations before archive.
