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

```bash
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

```bash
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

## `arkflow-server`

The `arkflow-server` binary runs the control-plane Hub. It has no flags; all
startup configuration comes from environment variables, plus the single
`migrate` subcommand for storage schema migration.

### Startup environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ARKFLOW_HUB_ADDRESS` | `127.0.0.1:8080` | Listen address for the Hub API. |
| `ARKFLOW_OPERATOR_TOKEN` | — | Operator token for human/admin API access. Supports the `role\|token` credential format; see [Hub authentication](../operate/control-plane/overview.md). |
| `ARKFLOW_NODE_TOKEN` | — | Shared token agents use to authenticate to the Hub. |
| `ARKFLOW_HUB_INSECURE_LOCAL` | off | Set to `1`/`true`/`yes` to relax checks for local development only. |
| `ARKFLOW_HUB_STORAGE` | — | Storage backend spec: a SQLite path or a PostgreSQL URL. Required when HA is enabled. |
| `ARKFLOW_HUB_TLS_CERT` / `ARKFLOW_HUB_TLS_KEY` | — | TLS certificate and key paths for the Hub listener. |
| `ARKFLOW_HUB_HA_ENABLED` | off | Set to `1`/`true`/`yes` to enable lease-based HA. Requires `ARKFLOW_HUB_STORAGE` to be set; use a PostgreSQL URL for multi-instance HA (a SQLite path only logs a development-only warning). |
| `ARKFLOW_HUB_HA_LEASE_TTL_MS` | `15000` | Lease TTL in milliseconds; values below 1000 are rejected. |
| `ARKFLOW_HUB_HA_HOLDER_ID` | — | Explicit lease holder identity; by default one is generated from hostname, PID, and boot time. |
| `ARKFLOW_HUB_HA_ADVERTISE_URL` | — | Absolute `http(s)://` URL with a host that agents use to reach this Hub instance (multi-Hub discovery). |

### `migrate` subcommand

Migrates the Hub storage database from SQLite to PostgreSQL — the path you
take when moving a single-instance Hub (SQLite) to multi-instance HA
(PostgreSQL).

```bash
arkflow-server migrate --from sqlite:<path> --to postgres://<url>
```

On success it prints a per-table row report (`<table>: <rows> rows` ...
`migration complete: <n> rows total`) and exits `0`.

| Exit code | Behavior |
|-----------|----------|
| `0` | Migration completed. |
| `2` | Usage error: missing `--from`/`--to`, or `--from` not starting with `sqlite:`, or `--to` not starting with `postgres://`/`postgresql://`. |
| non-zero | Migration failed; the error is printed to stderr. |

## Related pages

- [Component inventory](./component-inventory.md) — generated from the same registry the CLI reads.
- [Top-level configuration](./configuration.md) — what `--config` accepts.
- [IDE auto-completion](./ide-schema.md) — use `arkflow schema` in your editor.
