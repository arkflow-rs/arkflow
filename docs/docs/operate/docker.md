---
description: Run ArkFlow as a Docker container.
---

# Docker

The repository ships a multi-stage [`docker/Dockerfile`](https://github.com/arkflow-rs/arkflow/blob/main/docker/Dockerfile)
that builds the release binary with `cargo-chef` layer caching and runs it on
`debian:bookworm-slim`. Release-tagged builds are published by CI as container
images; the sections below cover running the engine, persisting state, and
building your own image.

## Run the engine

The image entrypoint is `arkflow --config /app/etc/config.yaml`, so mounting
your configuration at that path is all a single-node deployment needs:

```bash
docker run -d --name arkflow \
  -p 8080:8080 \
  -v $PWD/config.yaml:/app/etc/config.yaml:ro \
  -v arkflow-data:/app/data \
  ghcr.io/arkflow-rs/arkflow:0.5.0   # pin a released tag (or digest); do not float :latest in production
```

- **`-p 8080:8080`** exposes the HTTP health server (`/health`, `/readiness`,
  `/liveness` — see [Operate overview](../operate/overview.md)) for container
  health checks.
- **`-v arkflow-data:/app/data`** persists everything the default relative
  paths write under the working directory (`/app`): WAL directories, embedded
  state roots, and `file://` checkpoint stores. Point `durability.path`,
  `state.root`, and `checkpoint.object_store_uri` at paths under this volume
  so a recreated container recovers instead of starting cold.

### Distributed jobs

A node participating in a `split` placement must publish a routable data
plane: set `health_check.data_port` and `health_check.data_host` (the LAN
address peers dial, not `0.0.0.0`), and publish the port with
`-p <data_port>:<data_port>`. Nodes without `data_port` keep the co-located
placement behavior and never advertise the `network_shuffle` capability —
see [Distributed Jobs](../build/distributed-jobs.md).

## Validate before containerizing

The image carries the full binary, so a misconfigured deployment can be caught
before it enters orchestration:

```bash
docker run --rm \
  -v $PWD/config.yaml:/app/etc/config.yaml:ro \
  ghcr.io/arkflow-rs/arkflow:0.5.0 \
  /app/arkflow --config /app/etc/config.yaml --validate
```

## Build your own image

The Dockerfile needs the full toolchain headers (clang, protobuf-compiler,
OpenSSL, libcurl, python3-dev for the Python UDF processor); the
`cargo-chef` planner/builder split keeps dependency rebuilds out of the
per-change path:

```bash
git clone https://github.com/arkflow-rs/arkflow.git
cd arkflow
docker build -f docker/Dockerfile -t arkflow:local .
```

:::note
The release profile uses fat LTO; the build needs ample disk and memory
(BuildKit cache plus link-time optimization). CI prunes the local image cache
and adds swap before building for the same reason.
:::

## Control plane and console

The Hub and its console ship separately from the engine container — their
deployment model (binary flags, SQLite/PostgreSQL storage, static console
assets) is covered in
[Control plane deployment](./control-plane/deploy.md).
