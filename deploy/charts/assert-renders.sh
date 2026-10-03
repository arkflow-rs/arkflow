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

echo "== control-plane render =="
helm template arkflow "$CHART" -f "$CHART/values-controlplane.yaml" > "$TMP/cp.yaml"
assert_contains "$TMP/cp.yaml" "name: arkflow-hub" "cp: hub deployment+service"
assert_contains "$TMP/cp.yaml" "name: arkflow-console" "cp: console deployment+service"
assert_contains "$TMP/cp.yaml" "kind: PersistentVolumeClaim" "cp: default SQLite PVC"
assert_contains "$TMP/cp.yaml" "value: \"/var/lib/arkflow/hub.sqlite\"" "cp: sqlite storage env"
assert_contains "$TMP/cp.yaml" "helm.sh/resource-policy: keep" "cp: kept node-token secret"
assert_contains "$TMP/cp.yaml" "name: ARKFLOW_HUB_UPSTREAM" "cp: console upstream env"
assert_contains "$TMP/cp.yaml" "value: \"http://arkflow-hub:8080\"" "cp: console targets in-chart hub"
assert_contains "$TMP/cp.yaml" "name: arkflow-agent" "cp: in-chart agent workload"
assert_contains "$TMP/cp.yaml" "name: ARKFLOW_NODE_TOKEN" "cp: shared credential env"
assert_contains "$TMP/cp.yaml" "clusterIP: None" "cp: agent headless service"

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
