# Tasks: add-wasm-processor

## 1. 实现

- [x] 1.1 workspace 增加 wasmtime 依赖（default-features=false, cranelift+wat）
- [x] 1.2 `processor/wasm.rs`：ABI 桥（NDJSON ↔ guest）、fuel/内存沙箱、config 解析
- [x] 1.3 注册 builder + metadata schema + mod.rs init

## 2. 测试与文档

- [x] 2.1 wat 测试模块：echo 往返、常量输出、死循环 fuel 截停、trap 快败、缺导出报错
- [x] 2.2 示例 yaml + echo.wat + manifest + 文档页 en/zh + README 列表
- [x] 2.3 全量验证（test×2 + clippy + docs:check）+ 归档 + PLANNING + 推送
