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

## Roadmap

The chart is the first delivery layer of the Kubernetes story: an umbrella
chart for the control plane (Hub + Console) follows, and a CRD/operator is
explicitly deferred until a real GitOps demand signal exists — and when
built, it will only translate CRs into Hub API calls, never make scheduling
decisions.
