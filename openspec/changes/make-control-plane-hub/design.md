## Context

ArkFlow currently constructs a local `ControlPlane` from each Engine and passes
that facade directly to `arkflow-server`. That is useful for standalone mode,
but it means the server can only see the process in which it is running. The
target architecture has one Hub service and many independent compute-node
processes, with the console talking only to the Hub.

The design must work with the existing Tokio runtime, preserve the Stream/WAL
data plane, avoid introducing a database requirement, and allow a node to
reconnect without losing its local runtime. The first implementation targets a
single Hub and many nodes; it does not attempt distributed consensus.

## Goals / Non-Goals

**Goals:**

- Give every compute node a stable identity, authenticated session, lease, and
  last-known resource snapshot in the Hub.
- Let nodes report system, Stream, operation, event, and metrics state to the
  Hub and let the Hub dispatch node-targeted commands.
- Aggregate read APIs across nodes while making mutating requests explicit
  about their target node.
- Detect stale/disconnected nodes and expose that state to operators.
- Keep standalone local mode available as a migration and development tool.

**Non-Goals:**

- Consensus, failover election, or scheduling a Stream onto another node.
- Sending Stream data through the Hub.
- Durable historical telemetry or a mandatory external database.

## Decisions

1. **Use an HTTP pull command channel for the first protocol.**

   Nodes register with `POST /api/v1/agent/register`, periodically send
   `POST /api/v1/agent/heartbeat` and `POST /api/v1/agent/report`, then poll
   `GET /api/v1/agent/commands` and acknowledge with
   `POST /api/v1/agent/commands/{id}/result`. This uses existing Axum and
   Reqwest infrastructure, survives proxies, and is easier to debug than
   introducing WebSockets. WebSockets remain a future optimization.

   Result calls include the current node session (`node_id` and
   `session_token`); the Hub verifies command ownership before mutating an
   operation. Targeted configuration apply/rollback uses the same command
   broker with a bounded JSON payload, and node configuration reports are
   redacted before they enter Hub state.

2. **Separate Hub state from node-local execution.**

   `arkflow-server` owns `NodeRegistry`, leases, pending commands, aggregated
   snapshots, and Hub operation records. `arkflow-core` retains
   `RuntimeManager` and executes commands locally. The node Agent is a thin
   client loop that serializes local snapshots and delegates command actions to
   the local `ControlPlane`.

3. **Use a signed-in-practice bearer session, not shared unauthenticated
   endpoints.**

   Registration requires a configured node token. The Hub returns a session
   token; all subsequent agent calls require that token and the node ID. The
   external operator API continues to use its separate operator token. Tokens
   are compared in constant time and never included in logs or snapshots.

4. **Model leases explicitly.**

   Each node has `last_seen`, `lease_ttl_ms`, `status` (`online`, `stale`,
   `offline`, `draining`), capabilities, and the latest report. A heartbeat
   refreshes the lease; a Hub timer marks a node stale after TTL and fails
   undispatched operations with a node-unavailable outcome. Re-registration
   replaces the session only after authenticating the node identity.

5. **Keep command state at the Hub and execution state at the node.**

   A Hub operation progresses through `queued -> dispatched -> acknowledged ->
   running -> succeeded|failed|timed_out|node_unavailable`. The node report
   carries the local operation ID and correlation ID so the Hub can reconcile
   after reconnect. Duplicate command delivery is safe because the node uses
   an idempotency key `(hub_operation_id, action, resource_id)`.

6. **Make resource targeting explicit.**

   `GET /api/v1/nodes` and collection resources aggregate by default. Stream
   detail and mutation routes accept `node_id` (query or path depending on the
   resource); a mutation without a target is rejected when more than one node
   is online. The UI always carries the selected node in its query state.

7. **Keep standalone mode as an adapter.**

   The current local router can be constructed around a local facade for
   development, but normal startup uses `--hub-url` and runs the Agent instead
   of binding a second HTTP control listener. This allows a rolling migration:
   start a Hub, point nodes at it, then remove standalone listeners.

## Risks / Trade-offs

- **[Risk]** HTTP polling adds command latency. → Bound polling to a short
  interval, expose dispatch timestamps, and leave a streaming transport seam.
- **[Risk]** Hub memory is lost on restart. → Document this limitation, make
  nodes re-register and report full state, and retain configuration versions on
  the configured durable store.
- **[Risk]** A stale node may still process a command after its lease expires.
  → Commands carry expiry and idempotency data; the node rejects expired
  commands and the Hub marks the result as uncertain rather than claiming
  success.
- **[Risk]** Multiple nodes can use duplicate human-readable Stream IDs. → Use
  `(node_id, stream_id)` as the resource key everywhere in the Hub API.
- **[Risk]** Agent credentials can be misconfigured. → Fail closed on
  registration, provide clear problem codes, and keep standalone mode explicit.

## Migration Plan

1. Deploy the Hub with operator authentication and node registration enabled.
2. Add node credentials and `hub_url`/`node_id` to each compute-node config.
3. Start nodes in Agent mode; verify registration, heartbeat, and full reports.
4. Switch the console API base to the Hub and validate node selection and
   targeted lifecycle operations.
5. Keep standalone mode during rollout; remove direct node listeners after all
   clients use Hub routes.
6. Roll back by stopping Agent mode and starting the existing standalone local
   server. Local Stream/WAL state remains on the node.

## Open Questions

- Should the command channel move to WebSockets once the HTTP protocol is
  proven, or is bounded polling sufficient for expected fleet size?
- Which persistent store, if any, should retain node audit history beyond the
  bounded in-memory operational window?
