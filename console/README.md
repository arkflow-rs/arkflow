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
`127.0.0.1:8080`; the console uses the canonical `/api/v1/metrics` route. For a
protected deployment, set `VITE_API_TOKEN` only during a controlled build and
serve `dist/` behind the same-origin authenticated proxy.

The canonical control resources are under `/api/v1`. `GET /api/v1/nodes`
always returns a paginated collection in both local and Hub mode; the local
singleton compatibility endpoint is `/api/v1/node`. Node-scoped mutations
must include the target node in the URL, and stale/offline Hub nodes reject
mutations with `node_unavailable`.

For local Hub development, use the same value as `ARKFLOW_OPERATOR_TOKEN`:

```bash
VITE_API_TOKEN=operator-secret npm run dev
```

Vite reads this value at startup, so restart the dev server after changing it.
