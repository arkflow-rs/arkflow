# Purpose

Define the minimality policy for arkflow-core's public Rust API surface: visibility follows cross-crate consumption, builder signatures use borrowed string slices, public traits do not leak internal engine types, Hub-only helpers live in arkflow-server, and config Rust types stay honest without changing the serialized configuration shape.

## Requirements

### Requirement: arkflow-core 公共 API 面 SHALL 保持最小

arkflow-core 对 workspace 内其他 crate（arkflow-plugin、arkflow-server、arkflow）不需要的顶层 item SHALL NOT 保持 `pub` 可见性（用 `pub(crate)` 收紧）。组件 builder trait 的 `name` 参数 SHALL 使用 `Option<&str>` 而非 `Option<&String>`。新增公共 item 的 pull request SHALL 以「跨 crate 消费者是否存在」为准绳评估可见性。

#### Scenario: 零外部引用的 item 收紧

- **WHEN** arkflow-core 中某个顶层 pub item（类型/函数/trait/常量）在 arkflow-plugin、arkflow-server、arkflow 及各 crate 集成测试中均无引用
- **THEN** 该 item 的可见性为 `pub(crate)` 或更收紧，公共 API 面不包含它

#### Scenario: builder trait 命名参数用 &str

- **WHEN** 一个插件为新组件实现任一 builder trait（InputBuilder/OutputBuilder/ProcessorBuilder/CodecBuilder/BufferBuilder）
- **THEN** 该 trait 的 `name` 参数类型为 `Option<&str>`，实现无需为名字参数借用 String

### Requirement: 公共 trait 签名 SHALL NOT 泄漏内部计算引擎类型

arkflow-core 公共 trait 的方法签名 SHALL NOT 暴露 DataFusion 内部求值类型（如 `ColumnarValue`/`ScalarValue`）；与插件交换的数据 SHALL 使用 core 自有类型（如 `MessageBatch`、基础标准库类型）。`Temporary` 的键参数 SHALL 为字符串切片类型。

#### Scenario: Temporary 查询键为字符串

- **WHEN** 一个 Temporary 实现（例如 Redis lookup）收到查询键
- **THEN** 键以 `&[String]`（或等价字符串切片类型）传入，实现无需依赖 DataFusion 类型解包标量

### Requirement: Hub 专属逻辑 SHALL NOT 住在 arkflow-core

仅被 arkflow-server（Hub/Agent 控制面）使用的辅助函数与类型 SHALL 定义在 arkflow-server 内；arkflow-core SHALL 只保留引擎、通用配置与 secret 引用语法定义。分发候选载荷的 envelope 处理（`resolve_candidate_payload`：format/content/content_verbatim 契约）SHALL 住在 arkflow-server；secret-only 引用遍历作为 core `secret` 语法原语保留。既有 `secret-references` 能力定义的 Hub 分发预解析行为 SHALL 保持不变，仅实现位置迁移。

#### Scenario: 分发预解析实现位于 server

- **WHEN** 维护者在代码中查找 Hub 分发配置的 secret 预解析入口（`resolve_candidate_payload` envelope 处理）
- **THEN** 它定义在 arkflow-server crate 内，arkflow-core 不再导出该函数

### Requirement: Error 变体 SHALL 无死代码，配置类型名实相符

`arkflow_core::Error` SHALL NOT 含全工作区零构造的变体；新增错误分类 SHALL 先有构造点再进 enum。进程级配置的 Rust 类型命名 SHALL 反映其内容（健康端点、控制 API、Agent 模式、数据面、可观测性），且类型改名/拆分 SHALL NOT 改变 YAML/JSON 配置形状（serde 字段名与哨兵行为逐字节兼容）。

#### Scenario: dead variant 被移除

- **WHEN** `Error` enum 中的某变体在工作区任何 crate（含测试）中零构造
- **THEN** 该变体不存在于 enum 中，匹配处（若有）编译失败暴露

#### Scenario: 配置类型改名不破坏既有 YAML

- **WHEN** 用户加载一个使用 `health_check:` section（含 hub_urls、observability 等字段）的既有配置文件
- **THEN** 反序列化行为与本变更前逐字节一致，包括对已废弃键的哨兵报错语义；仅 Rust 类型名与内部分组发生变化
