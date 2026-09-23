---
components: [wasm]
description: ArkFlow 文档页面。
---

# WASM

WASM 处理器在由 [wasmtime](https://wasmtime.dev/) 驱动的沙箱中逐行执行用户提供的 WebAssembly 模块。你可以用任何能编译到 `wasm32` 的语言(Rust、C/C++、AssemblyScript、TinyGo 等)编写变换逻辑,ArkFlow 在带 fuel 计量与内存上限的沙箱中运行它。

## 数据契约

对每个批次,宿主先把行序列化为按行分隔的 JSON,再逐行交给 guest:

- 宿主把该行的 JSON 字节写入线性内存;
- guest 的变换函数执行并把输出位置打包为 `(ptr << 32) | len` 返回;
- 宿主读回输出字节,解析为变换后的 JSON 行。

全部输出行随后重组为新的列式批次,因此无论 guest 做了什么,流始终保持 Arrow 数据模型。行按顺序通过,一行恰好产出一行输出。

## guest 模块 ABI

guest 模块必须自包含(无导入;v1 不提供 WASI),并导出:

| Export | 签名 | 用途 |
| --- | --- | --- |
| `memory` | memory | 宿主与 guest 共享的线性内存。 |
| `alloc` | `alloc(len: i32) -> ptr: i32` | 预留至少 `len` 字节可写空间。 |
| `dealloc` | `dealloc(ptr: i32, len: i32)` | 释放先前分配。 |
| `transform`(可配置) | `transform(in_ptr: i32, in_len: i32) -> i64` | 变换一行 JSON。 |

缺失导出会在组件启动时被拒绝,配置错误的模块会快速失败,而不是在首条记录处出错。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `wasm` |
| path | string | yes | — | 模块文件路径:`.wasm` 二进制或 `.wat` 文本(适合小型 helper)。 |
| function | string | no | `transform` | 导出的变换函数名。 |
| fuel | integer | no | `10000000` | 每行的 fuel 预算;耗尽会使该行以错误失败,而不是挂起宿主。 |

引擎还会把 guest 线性内存增长限制在每实例 256 MiB 以内。

```yaml validate=fragment wrap=processors
- type: "wasm"
  path: "examples/wasm/echo.wat"
  function: "transform"
```

## 示例

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

完整的最小 guest 示例见 `examples/wasm/echo.wat`(WebAssembly 文本格式):它分配空间、拷贝输入行并原样返回。
