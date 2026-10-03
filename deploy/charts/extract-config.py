#!/usr/bin/env python3
"""Extract the rendered engine config document from `helm template` output.

Usage: extract-config.py <rendered-manifests.yaml> <out-config.yaml>

Pulls the `config.yaml` block out of the rendered ConfigMap and unindents
it, so the result can be piped straight into `arkflow --validate`.
"""
import re
import sys

rendered_path, out_path = sys.argv[1], sys.argv[2]
rendered = open(rendered_path).read()
block = re.search(r"  config\.yaml: \|\n((?:    .*\n|\n)+?)\n?---", rendered)
assert block, f"config.yaml block not found in {rendered_path}"
doc = "".join(
    line[4:] if line.startswith("    ") else ""
    for line in block.group(1).splitlines(keepends=True)
)
open(out_path, "w").write(doc)
