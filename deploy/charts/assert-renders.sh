#!/usr/bin/env sh
# Render gates for the arkflow chart. Requires: helm on PATH, python3.
# Local usage without installing helm:
#   helm() { docker run --rm -v "$PWD:/work" -w /work alpine/helm:3.16.4 "$@"; }
#   ./deploy/charts/assert-renders.sh
set -eu
CHART=deploy/charts/arkflow
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

fail() { echo "ASSERT-FAIL: $1" >&2; exit 1; }
assert_contains() { grep -qF "$2" "$1" || fail "$3 (missing: $2)"; }
assert_not_contains() { ! grep -qF "$2" "$1" || fail "$3 (unexpected: $2)"; }

echo "== helm lint =="
helm lint "$CHART"

echo "== standalone render =="
helm template assert-std "$CHART" > "$TMP/std.yaml"
assert_contains "$TMP/std.yaml" "type: Recreate" "standalone: Recreate strategy"
assert_contains "$TMP/std.yaml" "replicas: 1" "standalone: single replica"
assert_not_contains "$TMP/std.yaml" "name: data" "standalone: no data-plane port"
assert_not_contains "$TMP/std.yaml" "clusterIP: None" "standalone: no headless service"
assert_contains "$TMP/std.yaml" "path: /health" "standalone: liveness on /health"
assert_contains "$TMP/std.yaml" "path: /readiness" "standalone: readiness on /readiness"

echo "== agent render =="
helm template assert-agent "$CHART" -f "$CHART/values-agent.yaml" > "$TMP/agent.yaml"
assert_contains "$TMP/agent.yaml" "containerPort: 9090" "agent: data-plane port"
assert_contains "$TMP/agent.yaml" "clusterIP: None" "agent: headless service"
assert_contains "$TMP/agent.yaml" "name: ARKFLOW_NODE_ID" "agent: pod-name identity env"
assert_contains "$TMP/agent.yaml" "fieldPath: metadata.name" "agent: downward API source"
assert_contains "$TMP/agent.yaml" "type: Recreate" "agent: Recreate strategy"

echo "== unsafe multi-replica opt-in =="
helm template assert-multi "$CHART" --set unsafe.allowMultipleReplicas=true --set unsafe.replicas=3 \
  > "$TMP/multi.yaml"
assert_contains "$TMP/multi.yaml" "replicas: 3" "unsafe: honored when opted in"

echo "== control-plane render (mTLS enabled by default) =="
helm template arkflow "$CHART" -f "$CHART/values-controlplane.yaml" > "$TMP/cp.yaml"
assert_contains "$TMP/cp.yaml" "name: arkflow-hub" "cp: hub deployment+service"
assert_contains "$TMP/cp.yaml" "name: arkflow-console" "cp: console deployment+service"
assert_contains "$TMP/cp.yaml" "kind: PersistentVolumeClaim" "cp: default SQLite PVC"
assert_contains "$TMP/cp.yaml" "value: \"/var/lib/arkflow/hub.sqlite\"" "cp: sqlite storage env"
assert_contains "$TMP/cp.yaml" "helm.sh/resource-policy: keep" "cp: kept secret"
assert_contains "$TMP/cp.yaml" "name: arkflow-tls" "cp: generated TLS secret"
assert_contains "$TMP/cp.yaml" "name: ARKFLOW_HUB_TLS_CA" "cp: hub CA env"
assert_contains "$TMP/cp.yaml" "name: ARKFLOW_HUB_TLS_CLIENT_AUTH" "cp: hub client auth env"
assert_contains "$TMP/cp.yaml" "value: \"agent_required\"" "cp: default agent_required client auth"
assert_contains "$TMP/cp.yaml" "scheme: HTTPS" "cp: hub probes use HTTPS"
assert_contains "$TMP/cp.yaml" "name: ARKFLOW_HUB_UPSTREAM" "cp: console upstream env"
assert_contains "$TMP/cp.yaml" "value: \"https://arkflow-hub:8080\"" "cp: console targets https in-chart hub"
assert_contains "$TMP/cp.yaml" "name: ARKFLOW_HUB_PROXY_SSL" "cp: console proxy SSL verification env"
assert_contains "$TMP/cp.yaml" "proxy_ssl_verify on" "cp: console enables proxy_ssl_verify"
assert_contains "$TMP/cp.yaml" "name: hub-ca" "cp: console mounts CA-only volume"
assert_contains "$TMP/cp.yaml" "name: arkflow-agent" "cp: in-chart agent workload"
assert_contains "$TMP/cp.yaml" "name: ARKFLOW_NODE_TOKEN" "cp: shared credential env"
assert_contains "$TMP/cp.yaml" "name: ARKFLOW_AGENT_TLS_CERT" "cp: agent client cert env"
assert_contains "$TMP/cp.yaml" "name: ARKFLOW_DATA_PLANE_TLS_CERT" "cp: data-plane cert env"
assert_contains "$TMP/cp.yaml" "clusterIP: None" "cp: agent headless service"
python3 - "$TMP/cp.yaml" <<'EOF'
import sys
docs = open(sys.argv[1]).read().split("\n---")
agent = next((d for d in docs if "app.kubernetes.io/component: engine" in d and "kind: Deployment" in d), "")
assert agent, "cp: missing engine Deployment"
assert 'key: "ca.crt"' in agent and 'key: "agent.crt"' in agent and 'key: "agent.key"' in agent, "cp: agent-tls items projection incomplete"
assert "hub.key" not in agent and "hub.crt" not in agent, "cp: hub TLS key/cert must never be mounted in agent deployment"
console = next((d for d in docs if "app.kubernetes.io/component: console" in d and "kind: Deployment" in d), "")
assert console, "cp: missing console Deployment"
assert 'key: "ca.crt"' in console and "hub.key" not in console, "cp: console must mount only ca.crt"
EOF

echo "== control-plane mTLS opt-out =="
helm template arkflow "$CHART" -f "$CHART/values-controlplane.yaml" \
  --set controlPlane.tls.enabled=false > "$TMP/cpnotls.yaml"
assert_not_contains "$TMP/cpnotls.yaml" "name: arkflow-tls" "cp-no-tls: no generated TLS secret"
assert_not_contains "$TMP/cpnotls.yaml" "name: ARKFLOW_HUB_TLS_CERT" "cp-no-tls: no hub TLS env"
assert_not_contains "$TMP/cpnotls.yaml" "name: ARKFLOW_AGENT_TLS_CERT" "cp-no-tls: no agent TLS env"
assert_not_contains "$TMP/cpnotls.yaml" "name: ARKFLOW_HUB_PROXY_SSL" "cp-no-tls: no console SSL env"
assert_not_contains "$TMP/cpnotls.yaml" "scheme: HTTPS" "cp-no-tls: probes stay HTTP"
assert_contains "$TMP/cpnotls.yaml" "value: \"http://arkflow-hub:8080\"" "cp-no-tls: console uses http upstream"

echo "== control-plane rejects unsupported clientAuth=required =="
if helm template arkflow "$CHART" -f "$CHART/values-controlplane.yaml" \
  --set controlPlane.tls.clientAuth=required >"$TMP/cpreq.yaml" 2>"$TMP/cpreq.err"; then
  fail "cp-required: expected clientAuth=required to fail chart render"
fi
assert_contains "$TMP/cpreq.err" "clientAuth=required" "cp-required: explicit error message"

echo "== agent existingSecret TLS, fixed nodeId, and node credential persistence =="
helm template my-agent "$CHART" -f "$CHART/values-agent.yaml" \
  --set agent.tls.existingSecret=my-agent-tls --set agent.nodeId=fixed-edge-01 --set persistence.enabled=true > "$TMP/agenttls.yaml"
assert_contains "$TMP/agenttls.yaml" "secretName: my-agent-tls" "agent-tls: mounts existing TLS secret"
assert_contains "$TMP/agenttls.yaml" "value: \"fixed-edge-01\"" "agent-tls: fixed nodeId override honored"
assert_contains "$TMP/agenttls.yaml" "name: ARKFLOW_AGENT_TLS_CA" "agent-tls: agent CA env"
assert_contains "$TMP/agenttls.yaml" "name: ARKFLOW_AGENT_TLS_CERT" "agent-tls: agent cert env"
assert_contains "$TMP/agenttls.yaml" "name: ARKFLOW_DATA_PLANE_TLS_CERT" "agent-tls: data-plane cert env"
assert_contains "$TMP/agenttls.yaml" "name: ARKFLOW_NODE_CREDENTIAL_FILE" "agent-tls: persisted credential file env"
assert_contains "$TMP/agenttls.yaml" "value: \"/app/data/node_credential\"" "agent-tls: credential file inside PVC mount"

echo "== control-plane with postgres disables PVC =="
helm template arkflow "$CHART" -f "$CHART/values-controlplane.yaml" \
  --set controlPlane.hub.storage.postgresURL="postgres://u:p@db:5432/hub" > "$TMP/cppg.yaml"
assert_not_contains "$TMP/cppg.yaml" "kind: PersistentVolumeClaim" "cp-pg: no PVC"
assert_contains "$TMP/cppg.yaml" "value: \"postgres://u:p@db:5432/hub\"" "cp-pg: postgres storage env"
assert_contains "$TMP/cppg.yaml" "type: Recreate" "cp-pg: hub still Recreate single replica"

echo "== control-plane without agent renders no engine workload =="
helm template arkflow "$CHART" --set mode=control-plane > "$TMP/cpnoagent.yaml"
assert_not_contains "$TMP/cpnoagent.yaml" "kind: ConfigMap" "cp-noagent: no engine configmap"
assert_not_contains "$TMP/cpnoagent.yaml" "app.kubernetes.io/component: engine" "cp-noagent: no engine workload"
assert_contains "$TMP/cpnoagent.yaml" "kind: Secret" "cp-noagent: node token secret still rendered"

echo "== user-supplied node credential =="
helm template arkflow "$CHART" --set mode=control-plane \
  --set controlPlane.nodeToken.existingSecret=my-fleet --set controlPlane.nodeToken.existingSecretKey=tk > "$TMP/cpuser.yaml"
assert_not_contains "$TMP/cpuser.yaml" "node-token" "cp-user: no generated secret"
assert_contains "$TMP/cpuser.yaml" "name: my-fleet" "cp-user: hub references user secret"
assert_contains "$TMP/cpuser.yaml" "key: tk" "cp-user: user secret key"

echo "== config rendered byte-for-byte =="
python3 "$(dirname "$0")/extract-config.py" "$TMP/std.yaml" "$TMP/std-config.yaml"
python3 - "$TMP/std-config.yaml" <<'EOF'
import re, sys
doc = open(sys.argv[1]).read()
expected = open("deploy/charts/arkflow/values.yaml").read()
m = re.search(r"^config: \|\n((?:  .*\n|\n)+?)^\S", expected, re.M)
assert m, "config value not found in values.yaml"
want = "".join(line[2:] for line in m.group(1).splitlines(keepends=True))
assert doc.rstrip("\n") == want.rstrip("\n"), "rendered config differs from values.config input"
print("byte-for-byte: OK")
EOF

echo "ALL RENDER ASSERTIONS PASSED"
