# wasm-processor 变更（Delta）

## ADDED Requirements

### Requirement: wasm 逐行变换 ABI

processor SHALL 从 `path` 加载 wasm 模块（二进制或 wat 文本），要求 guest 导出 `memory`、`alloc(len)->ptr`、`dealloc(ptr,len)` 与变换函数（默认名 `transform`，可用 `function` 配置）。对 batch 的每一行，宿主 SHALL 以 JSON 字节调用 `transform(in_ptr, in_len) -> i64`（打包 `(ptr<<32)|len`），回收输出字节并解析为 JSON 行；全部行处理完后 SHALL 重组为单个输出 batch。guest trap 或输出非法 JSON SHALL 以 `Error::Process` 失败（数据面可见，不静默丢弃）。

#### Scenario: echo 模块保真往返

- **WHEN** 一个输出输入字节原样的 guest 模块处理含 3 行的 batch
- **THEN** 输出 batch 的行内容与输入逐行一致

#### Scenario: guest trap 快败

- **WHEN** guest 函数执行 trap（如 unreachable）
- **THEN** process 返回 Error::Process 且包含 trap 信息

### Requirement: fuel 与内存沙箱

wasmtime 引擎 SHALL 启用 fuel 计量，每行调用前设置 fuel 上限（`fuel` 配置，默认 10_000_000）；耗尽 SHALL 以 Error::Process 失败。引擎 SHALL 通过 Store limiter 限制 guest 线性内存（默认上限 256 MiB），超出即分配失败快败。

#### Scenario: 死循环被 fuel 截停

- **WHEN** guest 函数包含无限循环
- **THEN** 调用以 fuel 耗尽错误失败，宿主不挂起

### Requirement: 注册与文档一致性

processor SHALL 以 `wasm` 类型名注册 builder 与 metadata schema，并在 README en/zh 与组件文档中保持一致（CI 校验）。

#### Scenario: 注册表一致性

- **WHEN** 运行 registry_consistency 与 docs:check
- **THEN** builder、metadata、README 列表与文档页归属一致
