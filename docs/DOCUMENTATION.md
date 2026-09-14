# Documentation system

ArkFlow uses Docusaurus. The maintained documentation is organized into five
audience-facing areas: get started, build, SQL, operate, and reference, plus
a develop area for contributors. The unversioned tree is the primary site
(`lastVersion: 'current'`); released snapshots hang off `/docs/<version>` and
are switched with the version dropdown.

## Source of truth

The component reference is generated from the engine registry, not maintained by hand:

- `reference/component-inventory.json` is a **generated artifact** (format version 2) exported from the Rust component registry. Never edit it by hand.
- `static/config-schema.json` is the engine configuration JSON Schema, generated the same way and published as a site asset for [IDE auto-completion](docs/reference/ide-schema.md).
- A Rust snapshot test keeps both files in sync with the code. When the registry or schema changes, `cargo test --workspace` fails until you regenerate:

```bash
ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot
```

- Each page under `docs/docs/components/` declares the components it documents in front matter. A bare name takes its kind from the page's directory; use `kind/name` when a page needs to be explicit:

```yaml
---
components: [arrow_to_json, json_to_arrow]   # or: [temporary/redis]
---
```

`pnpm docs:check` validates ownership bidirectionally: an inventory entry with no declaring page is an error, and a page declaring an unknown or wrong-kind component is an error.

## Local workflow

```bash
cd docs
pnpm install --frozen-lockfile
pnpm docs:check
pnpm build
```

`docs:check` is the same command used by CI. It validates front matter, internal Markdown links, component inventory coverage against the generated registry export, example manifests, and generated-reference markers. `pnpm components:generate` regenerates the component table on `docs/reference/component-inventory.md` from the committed inventory; `pnpm components:check` verifies the table is reproducible.

Example YAML files under `examples/` that are registered in `reference/example-manifest.json` are deep-validated by a Rust workspace test (the same checks as `--validate`). An example that cannot be validated offline carries an explicit `"validate": false` entry with a reason in the manifest.

## Version policy

The unversioned `docs/docs/` tree is the next/current development documentation. Versioned trees are release snapshots and are changed only for release corrections or explicit backports; generation and ownership checks apply to the unversioned tree only. A release checklist must verify links, examples, inventory, build output, and the version dropdown before publication.

When a page moves, keep a redirect or compatibility stub until the route migration is announced. When a feature is unavailable in a versioned tree, add a compatibility note that points to the supported version.

## Release checklist

- [ ] `ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot` is a no-op (inventory and schema asset current).
- [ ] `pnpm docs:check` passes.
- [ ] `pnpm build` passes with no new broken-link warnings.
- [ ] Quickstart and release-sensitive examples are validated by the workspace example test.
- [ ] Supported versions and version dropdown are correct.
- [ ] The build command, lockfile, and result are recorded in CI artifacts.
