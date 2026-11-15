#!/usr/bin/env python3
"""Extract the rendered engine config document from `helm template` output.

Usage: extract-config.py <rendered-manifests.yaml> <out-config.yaml>

Pulls the `config.yaml` literal block out of the rendered ConfigMap and
unindents it, so the result can be piped straight into `arkflow --validate`.

The block is delimited by YAML's block-scalar rules (a dedent ends it), not
by the next document separator: a ConfigMap rendered as the LAST document
has no `---` after it.
"""
import re
import sys

rendered_path, out_path = sys.argv[1], sys.argv[2]
with open(rendered_path) as handle:
    lines = handle.readlines()

for index, line in enumerate(lines):
    match = re.match(r"^(\s*)config\.yaml: \|\s*$", line)
    if not match:
        continue
    content_indent = len(match.group(1)) + 2
    body = []
    for line in lines[index + 1 :]:
        if not line.strip():
            # Blank lines belong to the scalar until the dedent.
            body.append("")
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent < content_indent:
            break
        body.append(line[content_indent:].rstrip("\n"))
    # Drop trailing blank lines the block separator added.
    while body and body[-1] == "":
        body.pop()
    with open(out_path, "w") as handle:
        handle.write("\n".join(body) + "\n")
    sys.exit(0)

raise SystemExit(f"config.yaml block not found in {rendered_path}")
