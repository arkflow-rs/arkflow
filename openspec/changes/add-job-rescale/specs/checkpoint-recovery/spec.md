# checkpoint-recovery 变更（Delta）

## MODIFIED Requirements

### Requirement: 恢复工件的任务集兼容性 SHALL 在恢复前校验

带状态恢复在选择恢复工件后 SHALL 比对工件 `task_attempts` 记录的任务集与当前编译计划的任务集；不一致（并行度或算子拓扑变更导致）SHALL 以显式配置错误失败，错误信息 SHALL 说明 keyed 状态尚不能跨并行度重分布，并给出恢复原并行度或以新 checkpoint/savepoint 重置状态两条出路——**除非 JobSpec 声明 `rescale: true`**：此时恢复 SHALL 按声明的重分布语义执行（见下）。任务集一致时行为与现状逐位一致；无状态作业无恢复工件可比对，不受影响。

#### Scenario: 变更并行度后恢复显式失败

- **WHEN** 一个有状态作业在并行度 1 下产出 checkpoint 后以并行度 2 重启并尝试恢复，且未声明 rescale
- **THEN** 恢复在状态还原前以配置错误失败，错误指明任务集差异（移除/新增任务）与两条出路，不出现空状态静默继续

#### Scenario: 拓扑不变时零行为变化

- **WHEN** 一个作业以与封存时相同的任务集恢复
- **THEN** 校验通过，恢复流程与现状逐位一致

#### Scenario: 无状态作业变更并行度

- **WHEN** 一个无状态作业变更并行度重启
- **THEN** 不触发该校验，作业按新并行度正常编译运行

## ADDED Requirements

### Requirement: 声明 rescale 的恢复 SHALL 按 key-group 重分布 keyed 状态

`rescale: true` 的 Job 在恢复工件任务集与当前计划不一致时 SHALL 重分布快照条目：从条目命名空间解析算子，从状态键按算子编码白名单（窗口：跳过 8 字节 window_start 取 utf8 键；StatefulOperator：剥类型前缀，整数大端/utf8/binary 原样；`null:<tag>` 哨兵整条）还原**路由哈希输入**，以 `key_group_for_key(输入, max_parallelism)` 计算归属，把条目命名空间重写为新 plan 中拥有该 key-group 的任务。键与值逐字节保留。无法识别的键编码 SHALL 显式失败。

#### Scenario: stateful 条目按新归属落位

- **WHEN** 并行度 1 的快照以并行度 4 声明 rescale 恢复
- **THEN** 每个键的条目出现在新 plan 中拥有其 key-group 的任务命名空间下，键值不变

#### Scenario: 窗口条目剥除窗口起点后归属

- **WHEN** 一个窗口状态条目（window_start + 键）参与重分布
- **THEN** 归属按用户键（不含 window_start）计算，条目键保留完整原编码

#### Scenario: 未知编码拒绝

- **WHEN** 条目键不符合任何白名单编码
- **THEN** 恢复以显式错误失败，不执行猜测性归属
