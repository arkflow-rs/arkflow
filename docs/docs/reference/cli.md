---
sidebar_position: 10
title: CLI reference
description: Every `arkflow` command, flag, and exit behavior.
---

# CLI reference

The `arkflow` binary is a single executable that runs streams and jobs from a
YAML configuration and ships discovery commands for the component registry.

```bash
arkflow [OPTIONS] [COMMAND]
```

## Run the engine

| Option | Description |
|--------|-------------|
| `-c, --config <FILE>` | Path to the YAML configuration file. Required unless a discovery subcommand is used. |
| `-v, --validate` | Validate the configuration and exit without starting the engine. |
| `--version` | Print the version and exit. |

```bash
# Run streams and jobs defined in the config
./target/release/arkflow --config config.yaml

# Deep-validate a config: parsing, stream ids, declared job graphs,
# duplicate ids, operator references, and configuration rules
./target/release/arkflow --config config.yaml --validate
```

`--validate` goes beyond deserialization: it checks stream id uniqueness,
validates every declared Job spec (graph structure and operator references),
and runs the engine's configuration rule checks. Validation failures print
`Invalid configuration: ...` and exit with a non-zero status, which makes the
flag suitable for CI pipelines and admission hooks.

## `components list`

List every registered component, grouped by kind.

```bash
arkflow components list [--kind KIND] [--format FORMAT]
```

| Option | Description |
|--------|-------------|
| `-k, --kind <KIND>` | Filter by component kind: `input`, `output`, `processor`, `buffer`, `codec`, `temporary`. |
| `-f, --format <FORMAT>` | `text` (default, aligned columns) or `json` (machine-readable registry export). |

```console
$ arkflow components list --kind codec
codec:
  json        JSON codec for parsing and serializing message payloads
  protobuf    Protobuf codec using a compiled .proto descriptor
  ...
```

The `json` format is the same registry export that backs the generated
[component inventory](./component-inventory.md), so scripts and the docs can
never disagree with the binary.

## `components show`

Print the configuration schema for one component.

```bash
arkflow components show <KIND> <NAME> [--format FORMAT]
```

| Argument | Description |
|----------|-------------|
| `<KIND>` | Component kind (required). |
| `<NAME>` | Registered component type name (required). |
| `-f, --format <FORMAT>` | `text` (default) or `json`. |

```console
$ arkflow components show input kafka
kafka: Kafka input component for consuming messages from Kafka topics
kind: input
config_optional: no

Example:
{ ... pretty-printed config example ... }

Config schema:
{ ... JSON Schema for the component config ... }
```

Unknown names fail with the list of valid alternatives, e.g.
`Unknown input type: kafaka. Available input types: file, generate, http, kafka, ...`.

## `schema`

Print the JSON Schema (draft 2020-12) for the whole engine configuration.
Feed it to your editor for YAML auto-completion and hover docs — see
[IDE auto-completion](./ide-schema.md).

```bash
arkflow schema > arkflow.schema.json
```

## Exit behavior

| Invocation | Behavior |
|------------|----------|
| `components ...`, `schema` | Prints results and exits immediately; no config file is read. |
| `--validate` | Validates the config, logs `The config is validated.`, exits without starting the engine. |
| `--config <FILE>` (default) | Validates the config, then starts the engine and blocks until shutdown. |
| Missing `--config` without a subcommand | Error: `missing --config <FILE> (or run a subcommand: components, schema)`. |

## Related pages

- [Component inventory](./component-inventory.md) — generated from the same registry the CLI reads.
- [Top-level configuration](./configuration.md) — what `--config` accepts.
- [IDE auto-completion](./ide-schema.md) — use `arkflow schema` in your editor.
