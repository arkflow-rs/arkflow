# ArkFlow Helm Chart

Installs the [ArkFlow](https://github.com/arkflow-rs/arkflow) stream processing engine and, in control-plane mode, the full distributed runtime.

## Install

```bash
helm install my-arkflow oci://ghcr.io/arkflow-rs/charts/arkflow \
  --values my-values.yaml
```

`appVersion` tracks the engine release published to `ghcr.io/arkflow-rs/arkflow`; the chart `version` bumps independently.

## Modes

| `mode` | Renders | Use for |
|---|---|---|
| `standalone` (default) | 1-replica Deployment (`Recreate`), ClusterIP Service (HTTP) | a self-contained pipeline |
| `agent` | same workload + data-plane port, headless Service | joining a control-plane Hub |
| `control-plane` | Hub (`arkflow-server` image) + Console + shared node-credential Secret, optional in-chart agent | the distributed runtime in one release |

## Values philosophy

- `config` is a **pass-through YAML document** rendered byte-for-byte into a ConfigMap mounted at `/app/etc/config.yaml`. Engine schema evolution never requires chart changes.
- Secrets are **never values**: reference them in `config` as `${env:VAR}` and feed `env` from Kubernetes Secrets.
- `health_check.address` must bind a pod-reachable address (`0.0.0.0:8080`) — the engine default is loopback, which kubelet probes cannot reach.
- Standalone runs **exactly one replica** (`Recreate` strategy). A second replica consumes the same sources twice. Scale-out is a control-plane concern.

## Agent mode

Node identity defaults to the pod name (`ARKFLOW_NODE_ID` via the downward API; engine-native fallback), or set `agent.nodeId` for a fixed identity. Supply the registration/bootstrap token as `ARKFLOW_NODE_TOKEN` from a Secret, set `agent.tls.existingSecret` to mount CA + client/data-plane certificates for Hub mTLS and cross-node shuffle, and enable `persistence.enabled: true` so enrolled per-node `arkn_` credentials (`ARKFLOW_NODE_CREDENTIAL_FILE`) survive restarts. Note that `arkn_` credentials stay bound to their enrolled `node_id`: when using persistence in a Deployment whose Pod name changes on replacement, set `agent.nodeId` (for a single pinned identity), use a StatefulSet, or clear the stale `node_credential` file before starting a new `node_id` (while bootstrap is enabled). The data-plane port (`agent.dataPort`) is exposed through a headless Service; the engine refuses to serve the data plane without credentials (see the TLS matrix docs).

## Control-plane mode

Hub + Console + optional in-chart agent:

- Hub storage defaults to SQLite on the persistence PVC; set `controlPlane.hub.storage.postgresURL` to switch (also the HA prerequisite) and skip the PVC. The Hub Deployment stays single-replica `Recreate`.
- The bootstrap node credential (`ARKFLOW_NODE_TOKEN`) is generated into a kept Secret on first install, or point `controlPlane.nodeToken.existingSecret` at your own.
- Mutual TLS (`controlPlane.tls.enabled: true`) is **enabled by default**: on first install the chart generates and keeps a Fleet CA (10y) and Hub/Agent leaf certs (365d, preserved across upgrades; `clientAuth: agent_required` — `required` is rejected because kubelet probes and Console do not present client certs), and configures Hub HTTPS, Console CA verification (`proxy_ssl_verify on`), and Agent client/data-plane mounts (`hub.key` excluded). Bring your own rotating certs (e.g. `cert-manager`) with `controlPlane.tls.existingSecret`, rotate before the 365-day leaf expiry by deleting `<release>-tls`, running `helm upgrade` to regenerate the Secret (certificates are generated during Helm rendering, not at pod startup), and then restarting Hub, Console, and Agent pods, or set `controlPlane.tls.enabled=false` to opt out to plaintext HTTP.
- The Console proxies `/api` and `/metrics` to the in-chart Hub Service (same-origin contract).

See `values-controlplane.yaml` for a complete example.

## More

See the deployment docs (`docs/docs/operate/helm.md` in the repository) for the full guide.
