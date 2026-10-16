---
title: Versioning and upgrade guide
description: How ArkFlow versions releases, what the v1.0 surface freeze covers, and how to upgrade a deployment.
---

# Versioning and upgrade guide

This page covers the **product** versioning policy and upgrade path. For how
the documentation site itself snapshots versions, see
[Compatibility and version policy](./compatibility.md); for the release-by-release
change list, see the [CHANGELOG](https://github.com/arkflow-rs/arkflow/blob/main/CHANGELOG.md)
in the repository.

## Versioning policy

ArkFlow follows [Semantic Versioning](https://semver.org/) starting at v1.0:

- **MAJOR** — breaking changes to any stable surface listed below.
- **MINOR** — new components, configuration keys, and features, fully
  backward compatible.
- **PATCH** — bug fixes and internal changes with no configuration impact.

Before v1.0 (0.x releases), minor versions may carry breaking changes; every
such change is called out at the top of the changelog section for that
release.

### What the v1.0 freeze covers

After v1.0, the following surfaces are covered by the compatibility promise:

- The **YAML configuration surface** — existing keys keep their names and
  semantics; the generated JSON Schema (`arkflow schema`) reflects the stable
  surface.
- The **CLI contracts** of both binaries: `arkflow` (run, `--validate`,
  `components`, `schema`) and `arkflow-server` (startup environment variables
  and the `migrate` subcommand).
- The **component registry type names** — every `input`/`output`/`processor`/
  `buffer`/`codec`/`wal` type string keeps working across minor upgrades.
- The **public API of the published crates** (`arkflow-core`, `arkflow-plugin`)
  for plugin authors.
- The **metrics and tracing label vocabulary** (`arkflow_job_*` metric names
  and label keys) so dashboards survive upgrades.

Behavior contracts captured in the operational specs (delivery semantics,
checkpoint/recovery guarantees) are part of the same promise: a minor upgrade
must not silently weaken an at-least-once or exactly-once guarantee.

## Release artifacts

Each tagged release publishes on GitHub Releases:

- Binary archives for Linux (amd64, arm64) and macOS (amd64, arm64), each
  containing `arkflow`, `arkflow-server`, LICENSE, and README, with a SHA-256
  checksum.
- Container images via the existing Docker tag channel and Helm charts from
  the repository.

## Upgrading a deployment

1. **Read the changelog first.** Entries tagged as breaking changes list the
   exact keys or flags you must adjust. The upgrade notes at the top of each
   release section are the authoritative list.
2. **Validate your configuration against the new binary** before swapping it
   in:

   ```bash
   arkflow --config config.yaml --validate
   ```

   The validator deep-checks streams, job graphs, and configuration rules,
   catching removed or renamed keys before they take effect in production.
3. **Back up state directories** — the WAL directory, checkpoint/state stores,
   and the Hub storage database — so you can roll back the binary and replay.
4. **Single-node engine:** stop the process gracefully (SIGTERM triggers the
   WAL close-drain path), replace the binary, and start it again; buffered
   input is replayed from the WAL on startup.
5. **Hub/Agent fleets:** upgrade the Hub first, then agents. Agents report a
   compatibility status against the Hub; keep an eye on it during the rollout
   and consult the [control-plane operations guide](../operate/control-plane/operations.md)
   for drain and maintenance procedures.
6. **Storage schema migrations** (Hub): run `arkflow-server migrate` when a
   release notes it — see the [CLI reference](./cli.md) for the subcommand
   contract.

If a release introduces an incompatible checkpoint or state format, the
release notes state it explicitly and the recovery path rejects old snapshots
with a precise error instead of misreading them.
