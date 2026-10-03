# columnar-window-operators Delta

## ADDED Requirements

### Requirement: Window aggregation buffer entry bound
窗口算子的 keyed 聚合缓冲（以 (window, key) 为条目）SHALL 有可配置的条目上限（`max_buffered_keys`，默认 65536）。达到上限时系统 SHALL 按最老 window-start 优先驱逐整条聚合缓冲直至回到上限内，并 MUST 以节流告警（含驱逐条目数、当前深度、上限）显式声明被驱逐窗口的数据丢失。上限检查 MUST 位于所有窗口族（tumbling/sliding/session）共用的缓冲插入路径。

#### Scenario: 高基数 key 触发驱逐并告警
- **WHEN** 某窗口的 distinct key 数使缓冲条目总数超过 `max_buffered_keys`
- **THEN** 最老 window-start 的条目被优先驱逐、缓冲回到上限内，且节流告警记录驱逐量与上限

#### Scenario: 正常负载不受影响
- **WHEN** 缓冲条目总数低于上限
- **THEN** 不发生驱逐、不产生告警，聚合与触发语义与既有行为一致

#### Scenario: 驱逐后的窗口不再参与触发
- **WHEN** 被驱逐条目所属窗口随后到达触发条件
- **THEN** 该窗口无聚合结果可发射（数据已在驱逐时显式计为丢失），其余窗口照常触发
