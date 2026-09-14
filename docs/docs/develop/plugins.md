---
sidebar_position: 21
title: Writing a plugin
description: Build and register a new component — the trait, the builder, registration, and the documentation contract.
---

# Writing a plugin

Every ArkFlow component — input, output, processor, buffer, codec, or
temporary table — follows the same four-step pattern. This page walks it with
a real, minimal example: the `drop` output
(`crates/arkflow-plugin/src/output/drop.rs`).

## 1. Implement the component trait

Each kind has a trait in `arkflow-core` (`Input`, `Output`, `Processor`,
`Buffer`, `Codec`, plus the temporary-table interface). They are
`async_trait` and revolve around `MessageBatch` — ArkFlow's Arrow
`RecordBatch` wrapper:

```rust
use arkflow_core::output::{Output, OutputBuilder};
use arkflow_core::{Error, MessageBatchRef, Resource};
use async_trait::async_trait;
use std::sync::Arc;

struct DropOutput;

#[async_trait]
impl Output for DropOutput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn write(&self, _: MessageBatchRef) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}
```

Data in and out is columnar. If your component deals in raw bytes, use the
codec layer rather than converting to rows yourself.

## 2. Implement the builder

The builder turns YAML config into a component instance. It receives the
component name, the optional JSON config, the optional codec, and the
resource pool:

```rust
struct DropOutputBuilder;

impl OutputBuilder for DropOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        _: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        Ok(Arc::new(DropOutput))
    }
}
```

Parse config with `serde_json::Value` + `serde` into your own options struct
so unknown fields fail fast, and return `Error` with a message that names the
offending field.

## 3. Register and wire the init

Registration happens in the plugin crate's `init()`:

```rust
pub fn init() -> Result<(), Error> {
    register_output_builder("drop", Arc::new(DropOutputBuilder))?;
    register_output_metadata(ComponentMetadata::unit(
        "drop",
        "Discards all messages. Useful for performance benchmarks and dead-end pipelines.",
    ))
}
```

Then wire it up the chain:

1. Call your module's `init()` from the kind's `init()` in
   `crates/arkflow-plugin/src/mod.rs`.
2. That kind `init()` is already invoked from `crates/arkflow/src/main.rs` —
   no binary change needed for a standard plugin.

The type-name you register (`"drop"` above) is the exact string users write
in the `type:` field of their YAML.

## 4. Fulfill the documentation contract

Docs are machine-checked against the registry; a plugin PR is not done until
these pass:

1. **Regenerate the inventory** — the snapshot test fails until you run:
   ```bash
   ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot
   ```
   This refreshes `docs/reference/component-inventory.json` and
   `docs/static/config-schema.json` from your `ComponentMetadata` (description,
   config schema, config example), so keep them meaningful.
2. **Declare page ownership** — add or update a page under
   `docs/docs/components/` with front matter naming your component:
   ```yaml
   ---
   components: [output/my_component]
   ---
   ```
   An inventory entry with no declaring page fails `pnpm docs:check`, and a
   page declaring an unknown component fails too.
3. **Add an example YAML** under `examples/` and register it in
   `docs/reference/example-manifest.json`. Examples are deep-validated
   offline by a workspace test; an example that cannot validate offline needs
   an explicit `"validate": false` plus a reason.
4. **Update the READMEs** — `README.md` and `README_zh.md` component lists
   must use exact registry type-names and stay in parity (CI-checked).

## Checklist

- [ ] Trait implemented (`async_trait`, `MessageBatch` in/out).
- [ ] Builder parses config defensively; errors name the field.
- [ ] Registered with the exact YAML `type:` name + metadata with description, schema, example.
- [ ] `init()` wired through the kind's `init()` in the plugin `mod.rs`.
- [ ] Inventory regenerated (`ARKFLOW_REGENERATE_DOCS=1 ...`).
- [ ] Component page with `components:` ownership front matter.
- [ ] Example YAML registered and validating.
- [ ] `README.md` / `README_zh.md` lists updated in parity.
- [ ] `cargo test --workspace` and `cd docs && pnpm docs:check` green.

## Related pages

- [The unified execution kernel](./kernel.md) — where your component runs.
- [Component inventory](../reference/component-inventory.md) — the generated registry table.
- [IDE auto-completion](../reference/ide-schema.md) — the config schema your metadata feeds.
