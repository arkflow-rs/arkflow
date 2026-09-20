---
sidebar_position: 2
---

# IDE auto-completion

ArkFlow publishes the JSON Schema for the engine configuration so editors can
validate configurations and offer field-level completion while you type. The
schema embeds every registered component's configuration schema, so completion
works per `type:` — pick `kafka`, and your editor offers `brokers`, `topics`,
and `consumer_group`.

## Download the schema

The schema for the current development version is published with the
documentation:

- [`config-schema.json`](/config-schema.json) — full engine configuration
  schema (also embedded as a site asset)

It is generated from the engine itself and snapshotted by CI: if the engine's
configuration surface changes, the committed asset must be regenerated or the
build fails. You can also emit it locally at any time:

```bash
./target/release/arkflow schema > arkflow.schema.json
```

## Editor setup

### Visual Studio Code (YAML extension)

Install the [YAML extension](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml),
then associate the schema with ArkFlow configuration files. Either add a
modeline comment at the top of the configuration file:

```yaml validate=full
# yaml-language-server: $schema=https://arkflow-rs.com/config-schema.json
```

Or map it globally in your user or workspace `settings.json`:

```json
{
  "yaml.schemas": {
    "https://arkflow-rs.com/config-schema.json": ["arkflow.yaml", "arkflow.yml"]
  }
}
```

### JetBrains IDEs

Enable JSON Schema mappings under **Settings → Languages & Frameworks →
Schemas and DTDs → JSON Schema Maps** (or use the
[File Watchers plugin](https://www.jetbrains.com/help/idea/using-file-watchers.html)
with a YAML schema provider), pointing at
`https://arkflow-rs.com/config-schema.json`.

### Offline use

Point your editor at a locally generated schema instead of the published URL:

```bash
./target/release/arkflow schema > arkflow.schema.json
```

```yaml validate=full
# yaml-language-server: $schema=./arkflow.schema.json
```

## What the schema covers

- The top-level structure: `logging`, `health_check`, `streams`, and `jobs`.
- Every registered component per kind (input, output, processor, buffer,
  codec, temporary) as a `type`-discriminated union, including each
  component's individual configuration fields and examples.
- The declarative `jobs` structure executed by the unified kernel.

The authoritative listing of registered components is the
[component inventory](./component-inventory.md); each component page
under [Components](/docs/components/inputs/kafka) documents behavior in depth.
