# exactly-once-output 变更（Delta）

## MODIFIED Requirements

### Requirement: Kafka 输出 SHALL 支持事务内源位点提交（L3）

声明 `transactional_offsets` 的输入不得进入分区指派（assign）模式：显式指派的消费者不加入组、无法提供组元数据，`assign_partition` SHALL 以明确配置错误拒绝该组合。（其余 L3 语义不变。）

#### Scenario: 分区指派与 L3 组合被拒绝

- **WHEN** 一个声明 `transactional_offsets` 的 Kafka 输入被图构建期分区指派
- **THEN** assign_partition 以明确的配置错误失败，指明 L3 需要 subscribe 模式的单读者输入
