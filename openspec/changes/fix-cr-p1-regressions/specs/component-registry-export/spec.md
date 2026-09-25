## ADDED Requirements

### Requirement: Component kind initialization SHALL propagate registration failures

每个组件 kind 的 `init()`（input/output/processor/buffer/codec/wal 等）SHALL 将注册结果持久化在进程级单次初始化存储中并向调用方传播：任一 builder 或元数据注册失败时 `init()` SHALL 返回该错误；重复调用 SHALL 幂等返回首次结果（成功或失败）而不重试注册。注册错误 SHALL NOT 被静默丢弃，SHALL NOT 使进程以残缺组件目录继续运行而对外报告初始化成功。初始化编排方（如 `arkflow_plugin::initialize`）SHALL 在首个 kind 失败时短路停止后续初始化并向上传播错误。

#### Scenario: 注册失败使启动失败

- **WHEN** 某个组件 builder 或其元数据注册返回错误（例如注册名冲突或元数据构造失败）
- **THEN** 该 kind 的 `init()` 返回该错误，编排初始化以配置错误失败，进程不进入组件残缺的运行状态

#### Scenario: 初始化幂等且失败可重复观察

- **WHEN** `init()` 首次调用失败后被再次调用
- **THEN** 第二次调用不重试注册，直接返回与首次相同的错误

#### Scenario: 成功路径保持幂等

- **WHEN** 所有注册成功且 `init()` 被多次调用
- **THEN** 注册表只构建一次，后续调用立即返回成功，组件目录完整
