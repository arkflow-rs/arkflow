# Design: fix-l3-registry-lifecycle

弱引用的存活锚点是输入持有的 `SharedMetadata` 强克隆（构造时注册、connect 时填充）。同 group 双输入覆盖行为维持（后注册胜出）——与 librdkafka 同组双消费者的既有约束一致，属无效配置，不做额外检测。
