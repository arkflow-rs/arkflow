## ADDED Requirements

### Requirement: Hub submodule boundaries SHALL be explicit

`hub/` 目录的每个子模块 SHALL 以显式 `use` 清单声明其对父模块、兄弟模块与 crate 路径的依赖；`use super::*` glob 依赖与父模块仅为测试可达性而设的 glob 再导出 SHALL 不存在。内部导入结构的治理 SHALL 不改变 `crate::hub::*` 对外导出的公共项集合（`AgentCommand`、`Hub`、`CommandMetrics` 等公共类型的路径与可见性）。

#### Scenario: 子模块不再 glob 依赖父模块

- **WHEN** 在 `hub/` 任一子模块中搜索 `use super::*`
- **THEN** 无命中；该子模块引用的每个父级/兄弟符号都有显式 use 声明

#### Scenario: 内部导入治理不改变公共面

- **WHEN** hub 子模块导入显式化重构合入
- **THEN** crate 外部（api/agent/bootstrap/bin 与 arkflow 二进制）对 `crate::hub::*` 公共项的既有引用无需修改即可编译，server 全量测试通过
