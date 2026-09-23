---
components: [wasm]
description: ArkFlow documentation page.
---

# WASM

The WASM processor runs each row through a sandboxed WebAssembly module
powered by [wasmtime](https://wasmtime.dev/). Write your transform in any
language that compiles to `wasm32` (Rust, C/C++, AssemblyScript, TinyGo, ...)
and ArkFlow executes it inside a fuel-metered, memory-bounded sandbox.

## Data contract

For every batch the host serializes the rows as newline-delimited JSON and
hands each row to the guest one at a time:

- the host writes the row's JSON bytes into linear memory;
- the guest's transform function runs and returns the output location packed
  as `(ptr << 32) | len`;
- the host reads the output bytes back and parses them as the transformed
  JSON row.

The output rows are assembled into a new columnar batch, so a stream keeps
flowing as Arrow regardless of what the guest does. Rows pass through in
order; one row produces exactly one output row.

## Guest module ABI

A guest module must be self-contained (no imports; WASI is not available in
v1) and must export:

| Export | Signature | Purpose |
| --- | --- | --- |
| `memory` | memory | The linear memory the host and guest share. |
| `alloc` | `alloc(len: i32) -> ptr: i32` | Reserve at least `len` writable bytes. |
| `dealloc` | `dealloc(ptr: i32, len: i32)` | Release a previous allocation. |
| `transform` (configurable) | `transform(in_ptr: i32, in_len: i32) -> i64` | Transform one JSON row. |

Missing exports are rejected when the component starts, so a misconfigured
module fails fast instead of breaking the first record.

## Configuration

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `path` | string | yes | - | Path to the module file: `.wasm` binary or `.wat` text (handy for small helpers). |
| `function` | string | no | `transform` | Name of the exported transform function. |
| `fuel` | integer | no | `10000000` | Per-row fuel budget. Exhaustion fails the row with an error instead of hanging. |

The engine also caps guest linear memory growth at 256 MiB per instance.

```yaml validate=fragment wrap=processors
- type: "wasm"
  path: "examples/wasm/echo.wat"
  function: "transform"
```

## Examples

```yaml validate=full
logging:
  level: info
streams:
  - input:
      type: "generate"
      context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
      interval: 1ns
      batch_size: 1
      count: 10
    pipeline:
      thread_num: 2
      processors:
        - type: "json_to_arrow"
        - type: "wasm"
          path: "examples/wasm/echo.wat"
    output:
      type: "stdout"
```

See `examples/wasm/echo.wat` for a complete minimal guest in WebAssembly text
format: it allocates space, copies the input row and returns it unchanged.
