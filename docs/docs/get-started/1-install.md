---
sidebar_position: 1
---

# Install

## Prerequisites

- **Rust ≥ 1.88** (toolchain, for building from source)
- **Protobuf compiler** (`protoc`) — required at build time for protobuf codecs

```bash
# macOS
brew install protobuf
# Debian/Ubuntu
sudo apt-get install protobuf-compiler
export PROTOC=$(which protoc)
```

## Build from source

```bash
git clone https://github.com/arkflow-rs/arkflow.git
cd arkflow

# Optimized release build
cargo build --release

# (optional) run the test suite
cargo test
```

The binary is at `./target/release/arkflow`.

## Run

```bash
# Run with a config file
./target/release/arkflow --config config.yaml

# Validate a config without starting the engine
./target/release/arkflow --config config.yaml --validate
```

## Component discovery & schema

ArkFlow exposes CLI commands that list every registered component and print its
configuration schema. These power editor auto-completion and are handy when
writing a config.

```bash
# List every input / output / processor / buffer / codec
./target/release/arkflow components list
./target/release/arkflow components list --kind input

# Print the config schema for one component (text or JSON)
./target/release/arkflow components show input kafka
./target/release/arkflow components show processor sql --format json

# Emit a complete JSON Schema for the whole engine config
./target/release/arkflow schema > arkflow.schema.json
```

Point your editor's YAML language server at the generated schema for
field-level completion and validation.

Continue to [Quickstart](./2-quickstart.md).
