---
description: Install ArkFlow with the official Helm chart.
---

# Helm Chart

The official chart installs the ArkFlow engine from the images CI publishes
to ghcr.io. It renders the raw manifests described in
[Kubernetes](./kubernetes.md) with the safety defaults baked in: single
replica, `Recreate` updates, wired probes, and pass-through configuration.

## Install

```bash
helm install my-arkflow oci://ghcr.io/arkflow-rs/charts/arkflow \
  --values my-values.yaml
```

The chart `version` bumps independently; `appVersion` tracks the engine
release tag, so `helm upgrade` picks the engine version through the image tag.

## Modes

| `mode` | Renders | Use for |
|---|---|---|
| `standalone` (default) | 1-replica Deployment (`Recreate`), ClusterIP Service (HTTP) | a self-contained pipeline |
| `agent` | same workload + data-plane port, headless Service | joining a control-plane Hub |
| `control-plane` | Hub + Console + shared credential (+ optional in-chart agent) | the distributed runtime in one release |

## Pass-through configuration

The whole engine configuration document is supplied as the `config` value and
rendered byte-for-byte into a ConfigMap mounted at `/app/etc/config.yaml`.
Engine schema evolution never requires chart changes; `arkflow --validate`
stays the authority on validity.

```yaml validate=foreign reason="Helm values file"
mode: standalone

config: |
  health_check:
    address: "0.0.0.0:8080"
  streams:
    # ... full engine configuration document
```

:::warning Bind a reachable address

The engine's `health_check.address` defaults to `127.0.0.1:8080`, which
kubelet probes cannot reach. Always bind a pod-reachable address such as
`0.0.0.0:8080` in the chart-supplied config.

:::

## Secrets

The chart never receives secret values. Reference them inside `config` with
the engine's `${env:VAR}` expansion and feed the variables from Kubernetes
Secrets:

```yaml validate=foreign reason="Helm values file"
config: |
  health_check:
    address: "0.0.0.0:8080"
  outputs:
    - type: kafka
      brokers: ["${env:KAFKA_BROKERS}"]

env:
  - name: KAFKA_BROKERS
    valueFrom:
      secretKeyRef:
        name: my-release-secrets
        key: brokers
```

## Single-replica semantics

Standalone mode runs exactly one replica and updates with the `Recreate`
strategy: the engine is stateful, and two instances would consume the same
sources twice. `helm upgrade` therefore never runs old and new pods
concurrently. Scaling out is a control-plane concern, not a standalone-release
concern. An `unsafe.allowMultipleReplicas` opt-in exists for operators who
know their sources are partition-assigned; it is unsupported for
partition-unaware sources.

## Agent mode

`mode: agent` joins the engine to a control-plane Hub:

```yaml validate=foreign reason="Helm values file"
mode: agent

agent:
  dataPort: 9090
  tls:
    existingSecret: arkflow-agent-tls

persistence:
  enabled: true

config: |
  health_check:
    address: "0.0.0.0:8080"
    hub_urls: ["https://arkflow-hub:8080"]
    # node_id omitted: defaults to the pod name (ARKFLOW_NODE_ID)

env:
  - name: ARKFLOW_NODE_TOKEN
    valueFrom:
      secretKeyRef:
        name: arkflow-fleet
        key: node_token
```

- Node identity defaults to the pod name via the downward API; the engine
  reads `ARKFLOW_NODE_ID` natively when the config sets no `node_id`.
- Supply the bootstrap credential via `ARKFLOW_NODE_TOKEN` from a Secret, and
  enable `persistence.enabled: true` so the chart wires
  `ARKFLOW_NODE_CREDENTIAL_FILE` (`/app/data/node_credential`) and retains the
  Hub-enrolled `arkn_` credential across restarts.
- Set `agent.tls.existingSecret` (`ca.crt`, `tls.crt`, `tls.key`) to mount CA
  and client/data-plane certificates for Hub mTLS (`ARKFLOW_AGENT_TLS_*`) and
  cross-node shuffle (`ARKFLOW_DATA_PLANE_TLS_*`). Without TLS or a data-plane
  secret, the engine refuses split shuffle — see the
  [TLS matrix](./tls-matrix.md).

## Control-plane mode

`mode: control-plane` renders the full distributed runtime in one release:
the Hub (`arkflow-server` image), the Console (static UI behind a same-origin
nginx proxy), a bootstrap node-credential Secret, Fleet CA + Hub/Agent mTLS
certificates, and an optional in-chart agent joining the in-chart Hub.

```yaml validate=foreign reason="Helm values file"
mode: control-plane

controlPlane:
  # nodeToken unset => the chart generates and KEEPS a Secret across
  # upgrades and uninstalls (delete it manually when certain).
  # tls.enabled defaults to true (mTLS for Hub, Agent, and data plane).
  hub:
    storage:
      # Default: SQLite on the persistence PVC. A postgres:// URL switches
      # the Hub to Postgres (HA prerequisite) and skips the PVC.
      sqlitePath: /var/lib/arkflow/hub.sqlite
  console:
    service:
      type: ClusterIP
  agent:
    enabled: true
    replicas: 1

config: |
  health_check:
    address: "0.0.0.0:8080"
    hub_urls: ["https://<release-name>-hub:8080"]
```

Storage, mTLS, and availability contract:

- **Default storage**: SQLite at `controlPlane.hub.storage.sqlitePath` on the
  persistence PVC; the Hub Deployment is single-replica with `Recreate`
  updates (never two Hubs against one store).
- **Postgres**: set `controlPlane.hub.storage.postgresURL` (env
  `${env:...}` references allowed) to skip the PVC; Hub HA lease election
  additionally needs `controlPlane.hub.ha.enabled` and Postgres storage.
- **Migration** between backends stays a manual `arkflow-server migrate`
  operation (stop the Hub first) — see the
  [control-plane deployment](./control-plane/deploy.md) page.
- **mTLS enabled by default (`controlPlane.tls.enabled: true`)**: on first
  install the chart generates and keeps (`helm.sh/resource-policy: keep`) a
  Fleet CA, Hub server cert, and Agent client/data-plane cert in
  `<release>-tls` (or point `controlPlane.tls.existingSecret` at your own),
  configures Hub `ARKFLOW_HUB_TLS_{CA,CERT,KEY}` +
  `ARKFLOW_HUB_TLS_CLIENT_AUTH=agent_required` with `HTTPS` probes, switches
  `ARKFLOW_HUB_UPSTREAM` on the Console to `https://`, and wires both
  `ARKFLOW_AGENT_TLS_*` and `ARKFLOW_DATA_PLANE_TLS_*` onto the in-chart Agent.
  Set `controlPlane.tls.enabled=false` (and use `http://` in `hub_urls`) to opt
  out to plaintext HTTP.

The shared bootstrap credential feeds `ARKFLOW_NODE_TOKEN` on both the Hub and
the in-chart agent; the chart generates one into a Secret on first install
(`helm.sh/resource-policy: keep`) unless `controlPlane.nodeToken.existingSecret`
points at your own.

The Console resolves its proxy target at container start from
`ARKFLOW_HUB_UPSTREAM` — the chart sets it to `https://<release>-hub:8080`
(or `http://` when TLS is disabled), and the image default
(`http://arkflow-hub:8080`) preserves standalone-console behavior outside the
chart.

## Roadmap

The chart is the Kubernetes delivery surface: standalone/agent engine
installs, and the control-plane umbrella above. A CRD/operator is
explicitly deferred until a real GitOps demand signal exists — and when
built, it will only translate CRs into Hub API calls, never make scheduling
decisions.
