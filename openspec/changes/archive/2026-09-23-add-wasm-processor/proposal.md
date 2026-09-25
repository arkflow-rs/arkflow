## Why

PLANNING 7.3 方向④（issue #88 的插件化落地）：沙箱化用户变换是流处理引擎的生态标配（Fluvio SmartModules、Redpanda wasm transforms）。WASM processor 让用户以任意可编译到 wasm32 的语言编写逐行变换，宿主沙箱执行。与 ONNX/candle 相比，wasmtime 方案可用内联 wat 完全离线测试（无原生库、无模型资产），是当前可闭环的最优候选。

## What Changes

- 新 processor `wasm`：加载 wasm 模块（二进制或 wat 文本，`Module::new` 通吃），对每个 batch 逐行调用导出函数 `transform(in_ptr, in_len) -> i64`（返回 `(ptr<<32)|len` 打包），guest 需导出 `memory`、`alloc(len)->ptr`、`dealloc(ptr,len)`。
- 数据桥：宿主用 Arrow JSON LineDelimitedWriter 把 batch 序列化为 NDJSON 行 → 逐行 JSON 字节进出 guest → 输出 NDJSON 经 `try_to_arrow` 重组 batch。V1 不做 WASI、不做列式 IPC ABI。
- 沙箱：wasmtime `consume_fuel` + 可配置 fuel 上限（默认 10M/行）；Config limiter 限制 wasm 线性内存增长。
- 配置：`path`（必填）、`function`（默认 `transform`）、`fuel`（可选）。注册 metadata schema + 示例（examples/wasm/echo.wat）+ 文档页 + README en/zh。

## Capabilities

### New Capabilities

- `wasm-processor`: WASM 沙箱化逐行变换 processor 的 ABI、桥接与沙箱保证。

## Impact

- `crates/arkflow-plugin/`：新增 `processor/wasm.rs`；`Cargo.toml`（workspace 增 wasmtime）；`processor/mod.rs` init。
- `examples/wasm/echo.wat` + `examples/wasm_processor.yaml` + `docs/reference/example-manifest.json`。
- 文档：`docs/docs/components/2-processors/wasm.md`（en）+ zh；README en/zh `README_COMPONENTS:processor` 列表。

## Non-goals

- 不做 WASI、不做 Arrow IPC 列式 ABI、不做多实例池化（每 processor 一个 instance）。
- 不做引擎本体编译到 wasm32（issue #88 字面诉求不可行，本变更是其插件化替代）。
- 不做 ONNX/candle（无离线测试路径，缓行另立项）。
