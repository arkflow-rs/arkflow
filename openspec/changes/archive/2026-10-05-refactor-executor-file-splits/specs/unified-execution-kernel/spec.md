## ADDED Requirements

### Requirement: Internal module reorganization SHALL preserve the kernel's public execution surface

执行内核内部模块结构的重组（文件/目录拆分、内部可见性调整、测试专用兼容壳删除）SHALL 保持 `arkflow_core::executor` 对外导出的执行入口集合与语义不变：`run_job`、`ExecutionGraphBuilder`、`Envelope`、`Chain`、`StreamJobAdapter` 等公共项的路径与签名 SHALL 不因内部重组而变化，内核全部测试 SHALL 在重组后原样通过。

#### Scenario: 内部文件拆分后外部引用不受影响

- **WHEN** executor 内部实现从单文件重组为目录模块（如 `remote.rs` → `remote/`、`window.rs` → `window/`）
- **THEN** crate 外部对 `arkflow_core::executor::*` 公共项的既有 `use` 路径无需修改即可编译

#### Scenario: 测试专用兼容壳删除不改变执行路径

- **WHEN** 仅供测试使用的 no-op 兼容壳（如带 gate 参数但不使用 gate 的 `run_graph_*` 包装）被删除且测试改用真实入口
- **THEN** 生产执行路径与测试断言语义不变，全部内核测试通过
