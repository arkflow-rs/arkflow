# ArkFlow control-plane console

Start the backend from the repository root:

```bash
cargo run -p arkflow -- --config path/to/arkflow.yaml
```

In another terminal start the development console:

```bash
cd console
npm ci
npm run dev
```

The Vite proxy forwards `/api` and the compatibility root `/metrics` to
`127.0.0.1:8080`; the console uses the canonical `/api/v1/metrics` route.

A protected Hub answers `401` until the console sends a Bearer credential. The
console prompts for one when no OIDC browser login is configured, stores it in
`sessionStorage` (this tab only, never `localStorage`), and Settings exposes the
same control to switch or clear it. `VITE_API_TOKEN` is only an optional
build-time default: Vite inlines it into the public bundle, so prefer the
runtime entry or OIDC. Serve `dist/` behind the same-origin authenticated proxy
or provide a token at runtime.

The canonical control resources are under `/api/v1`. `GET /api/v1/nodes`
always returns a paginated collection in both local and Hub mode; the local
singleton compatibility endpoint is `/api/v1/node`. Node-scoped mutations
must include the target node in the URL, and stale/offline Hub nodes reject
mutations with `node_unavailable`.

The Rollouts view creates bounded configuration rollouts and exposes per-node
progress, audit history, and pause/resume/cancel/rollback actions. The Console
also opens `/api/v1/events/stream` with the configured Authorization header;
SSE reconnects send `Last-Event-ID`, while REST polling remains a slower
snapshot fallback when the stream is unavailable.

For local Hub development, use the same value as `ARKFLOW_OPERATOR_TOKEN`:

```bash
VITE_API_TOKEN=operator-secret npm run dev
```

Vite reads this value at startup, so restart the dev server after changing it.
The Settings page also accepts the token at runtime, so a dev server started
without `VITE_API_TOKEN` can be authenticated from the UI after the first 401.

Hub mode adds a Users panel (admin / `manage_users`): manage roles, scopes and
OIDC bindings, issue or rotate per-user credentials (the plaintext token is
shown once), revoke a single credential, and revoke all of a user's
sessions/tokens.

## Checks

CI runs the full gate for every change under `console/`
(`.github/workflows/console.yml`):

```bash
npm run typecheck    # tsc --noEmit
npm run format:check # prettier --check (format with: npm run format)
npm test             # vitest
npm run build        # tsc -b && vite build
```

## UI conventions

- Navigation deep-links through `?page=` (and `?node_id=` for the node
  filter), so views survive reloads and can be bookmarked.
- Live SSE events coalesce into one debounced snapshot refresh
  (`REFRESH_DEBOUNCE_MS`) instead of triggering a full fan-out per event;
  polling cadences live as constants in `src/api.ts` and the Settings view
  derives its copy from them.
- List panels fetch the first server page and state the server-side total
  when it exceeds what is shown (streams, operations, events).
- Destructive actions still use native `window.confirm`; job and version
  plans render in an in-app modal rather than `window.alert`.
