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

echo "== config rendered byte-for-byte =="
python3 - "$TMP/std.yaml" <<'EOF'
import sys, re
rendered = open(sys.argv[1]).read()
block = re.search(r"  config\.yaml: \|\n((?:    .*\n|\n)+?)\n?---", rendered)
assert block, "config.yaml block not found"
doc = "".join(line[4:] if line.startswith("    ") else "" for line in block.group(1).splitlines(keepends=True))
expected = open("deploy/charts/arkflow/values.yaml").read()
m = re.search(r"^config: \|\n((?:  .*\n|\n)+?)^\S", expected, re.M)
assert m, "config value not found in values.yaml"
want = "".join(line[2:] for line in m.group(1).splitlines(keepends=True))
assert doc.rstrip("\n") == want.rstrip("\n"), "rendered config differs from values.config input"
print("byte-for-byte: OK")
EOF

echo "ALL RENDER ASSERTIONS PASSED"
