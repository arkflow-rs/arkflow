# Documentation system

ArkFlow uses Docusaurus. The maintained documentation is organized around reader intent: tutorials and quickstarts, pipeline how-to guides, concepts, component/configuration reference, operations, and contribution guidance.

## Local workflow

```bash
cd docs
pnpm install --frozen-lockfile
pnpm docs:check
pnpm build
```

`docs:check` is the same command used by CI. It validates front matter, internal Markdown links, component inventory coverage, example manifests, and generated-reference markers. Run `pnpm components:generate` after changing the canonical inventory; `pnpm components:check` verifies that the committed table is reproducible.

## Version policy

The unversioned `docs/docs/` tree is the next/current development documentation. Versioned trees are release snapshots and are changed only for release corrections or explicit backports. A release checklist must verify links, examples, inventory, build output, and the version dropdown before publication.

When a page moves, keep a redirect or compatibility stub until the route migration is announced. When a feature is unavailable in a versioned tree, add a compatibility note that points to the supported version.

## Release checklist

- [ ] `pnpm docs:check` passes.
- [ ] `pnpm build` passes with no new broken-link warnings.
- [ ] Component/configuration inventory is current.
- [ ] Quickstart and release-sensitive examples are validated.
- [ ] Supported versions and version dropdown are correct.
- [ ] The build command, lockfile, and result are recorded in CI artifacts.
