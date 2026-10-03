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

config: |
  health_check:
    address: "0.0.0.0:8080"
    hub_urls: ["http://arkflow-hub:8080"]
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
- The registration credential arrives as `ARKFLOW_NODE_TOKEN` from a Secret.
- The data-plane shuffle port is exposed through a headless Service. The
  engine refuses to serve the data plane without fleet credentials — see the
  [TLS matrix](./tls-matrix.md) before relying on split placement.

## Control-plane mode

`mode: control-plane` renders the full distributed runtime in one release:
the Hub (`arkflow-server` image), the Console (static UI behind a same-origin
nginx proxy), a shared node-credential Secret, and an optional in-chart
agent joining the in-chart Hub.

```yaml validate=foreign reason="Helm values file"
mode: control-plane

controlPlane:
  # nodeToken unset => the chart generates and KEEPS a Secret across
  # upgrades and uninstalls (delete it manually when certain).
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
    hub_urls: ["http://<release-name>-hub:8080"]
```

Storage and availability contract:

- **Default**: SQLite at `controlPlane.hub.storage.sqlitePath` on the
  persistence PVC; the Hub Deployment is single-replica with `Recreate`
  updates (never two Hubs against one store).
- **Postgres**: set `controlPlane.hub.storage.postgresURL` (env
  `${env:...}` references allowed) to skip the PVC; Hub HA lease election
  additionally needs `controlPlane.hub.ha.enabled` and Postgres storage.
- **Migration** between backends stays a manual `arkflow-server migrate`
  operation (stop the Hub first) — see the
  [control-plane deployment](./control-plane/deploy.md) page.

The shared node credential feeds `ARKFLOW_NODE_TOKEN` on both the Hub and
the in-chart agent; the chart generates one into a Secret on first install
(`helm.sh/resource-policy: keep`) unless `controlPlane.nodeToken.existingSecret`
points at your own.

The Console resolves its proxy target at container start from
`ARKFLOW_HUB_UPSTREAM` — the chart sets it to the in-chart Hub Service, and
the image default (`http://arkflow-hub:8080`) preserves standalone-console
behavior outside the chart.

## Roadmap

The chart is the Kubernetes delivery surface: standalone/agent engine
installs, and the control-plane umbrella above. A CRD/operator is
explicitly deferred until a real GitOps demand signal exists — and when
built, it will only translate CRs into Hub API calls, never make scheduling
decisions.
