## Context

ArkFlow currently supports a local Engine control server and a Hub server. They expose overlapping resources, but the local `/nodes` response and the Hub node collection are not interchangeable, and metrics are mounted at a different path from the versioned API routes. The React console currently compensates by inspecting response shapes, which makes failures silent and prevents reliable node-scoped administration.

The change crosses the HTTP adapter, the typed browser client, and the console state model. Existing Rust domain and agent contracts should remain the source of truth for runtime behavior.

## Goals / Non-Goals

**Goals:**

- Provide a stable envelope for collections and a consistent node identity field in local and Hub responses.
- Make all console requests use the configured API base and preserve selected node context.
- Make lifecycle and configuration mutations observable, validation-gated, and safe on stale/unavailable nodes.
- Keep compatibility aliases and redacted configuration behavior.

**Non-Goals:**

- Changing stream lifecycle internals or the Agent protocol.
- Adding a new authentication or authorization provider.
- Replacing the existing console UI framework.

## Decisions

1. **Normalize at the HTTP boundary.** Add response adapters/helpers in `arkflow-server` so local and Hub handlers return the same `Page<T>` and resource fields. This is preferred over frontend shape detection because API consumers other than the console need the same contract.

2. **Keep `/api/v1` as the public base, including metrics.** Mount metrics under the configured API prefix while retaining the existing root health endpoints. This is preferred over special-casing the browser client and keeps proxy/auth behavior uniform.

3. **Represent local mode as a synthetic node collection.** The local server returns one stable local node resource and the Hub returns registered nodes. This preserves the console's node selector without adding a second frontend mode switch.

4. **Use explicit client state for validation and operations.** The console will retain the latest validation result, refuse publish while dirty/unvalidated/invalid, and poll mutations through the operation endpoint. This is preferred over trusting a successful POST alone because configuration and Hub commands are asynchronous.

5. **Treat server problem envelopes as typed data.** The client will normalize non-2xx responses into an `ApiError` with status and correlation ID, while redacting authorization and configuration content from displayed errors.

## Risks / Trade-offs

- [Compatibility] Clients relying on the old local `/nodes` singleton may break → retain a compatibility `/node` alias and document the collection contract for `/nodes`.
- [Proxy deployment] Moving metrics under the API prefix may affect existing probes → expose both paths during the migration and test both.
- [Stale data] A node can become stale between refresh and mutation → server rechecks availability and the console displays the returned conflict without retrying.
- [Polling load] Operation polling adds requests during mutations → use bounded polling with backoff and stop at terminal state.

## Migration Plan

1. Add server response/route compatibility and contract tests.
2. Update the typed client and console state handling.
3. Run Rust and console test suites against local and Hub fixtures.
4. Keep compatibility routes for one release; rollback is reverting the client to the old paths while retaining the additive server aliases.

