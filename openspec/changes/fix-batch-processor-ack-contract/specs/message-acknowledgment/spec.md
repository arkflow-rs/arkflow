## ADDED Requirements

### Requirement: 缓冲型处理器投递 SHALL 延迟结算 ack 至发射确认
以内存缓冲聚合多条输入投递的处理器（`batch`）SHALL 经 `process_with_ack` 持有投递 ack：缓冲期间返回 `Deferred`（不结算 ack、不提交源位点）；触发 flush 发射合并批时 SHALL 以替换 ack 携带全部暂存 ack，下游写出确认成功后一并结算，失败/中止时按组合 ack 语义补偿（undo/abort 传导至每个暂存 ack）。处理器链取消或关闭时，未发射的暂存 ack SHALL 被 abort；仅当输入源的确认模式与配置保证该投递在恢复后重放时，才由源重放（如 Kafka 已确认 offset 之前的消息；QoS 0 的 MQTT 等至多一次投递无此保证）。

#### Scenario: 缓冲期间源位点不前进

- **WHEN** `batch {count: 1000}` 收到 1 条消息（未触发 flush）且进程崩溃，且输入源的确认模式与配置保证未确认投递在重启后重放
- **THEN** 该消息的 ack 未结算，源位点未提交，重启后消息重放（不丢失）

#### Scenario: flush 发射确认后统一结算

- **WHEN** 缓冲满批 flush，合并批被下游 output 成功写出
- **THEN** 该批内全部暂存投递的 ack 恰好各结算一次

#### Scenario: 下游写失败补偿

- **WHEN** flush 后合并批写出失败
- **THEN** 组合 ack 的失败语义（undo/abort）传导至每个暂存 ack，无一被错误确认

#### Scenario: 关闭时未发射投递被 abort

- **WHEN** 处理器所在链取消/关闭且缓冲中仍有未发射投递，且输入源的确认模式与配置保证该未确认投递在重启时重放
- **THEN** 暂存 ack 被 abort（而非 ack），恢复后重放
