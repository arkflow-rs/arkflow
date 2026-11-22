## Why

内核按 `Error` 变体分支决定流的命运（`EOF`=优雅结束 / `Disconnection`=重连 / 其余=致命，`crates/arkflow-core/src/executor/task.rs:772-875`），但这份契约只写在 `task.rs:818-824` 的注释里，`openspec/specs/connector-recovery-contract/spec.md` 只覆盖了 5 个局部需求（Purpose 还是 TBD 占位）。连接器侧因此按作者口味选变体：同一个"未连接"guard 被映射为 `Connection(msg)` 12 处、裸 `Disconnection` 3 处、`Process` 1 处；连接器共使用 8 种变体、持 6 个互不共享的错误分类器。2026-11 代码审查（`CODE-REVIEW-2026-11-15.md` §5 动作 2）将其列为三根因之二："Error 变体是未治理的契约"。

批次 1（`fix-silent-data-loss`，#1367）修了 file/sql EOF 误映射与 NATS 订阅结束，但**逐连接器打补丁的漏网模式已被证实**：Pulsar 作为无界订阅源，`Ok(None)` 仍映射为 `EOF`（`crates/arkflow-plugin/src/input/pulsar.rs:290-301,350-356`）→ 服务端正常关流会让内核按优雅结束广播 Eos，静默终结整个 Job——这正是批次 1 给 NATS 修掉的同一缺陷，因缺乏跨连接器一致性测试而漏网。其余核实仍存的错位：

- `input/pulsar.rs:179-188` 死代码 `_consumer_builder`；
- `input/redis.rs:99-115` `is_temporary_redis_error` 子串匹配过宽（含 `"invalid"` 即判永久）；
- `output/sql.rs` 写路径 begin/execute/commit/close 全部→`Process`（:70-182，DB 断连=致命且不可辨别）、连接失败→`Config`（:688-709）、裸 `Disconnection` guard（:600,611）；`output/pgvector.rs` 同病（:199-214，guard :142,150）；
- `output/redis.rs:173` publish→`Process`；`output/nats.rs:195,208,225` publish→`Process`（mqtt/kafka 兄弟均映射连接族变体）；
- `input/multiple_inputs.rs:103` 重连期间通道关闭重包成 `Process` 字符串——内核有专用 `InputChannelClosed`（`lib.rs:122`）却未使用；
- 裸 `Disconnection` guard 3 处（`input/redis.rs:535`、`output/mongodb.rs:138`、`output/sql.rs:600,611`）与主流 `Connection(msg)` 不一致。

## What Changes

- **契约成文**：把内核三分法（source 读循环：`EOF`=优雅结束、`Disconnection`=重连、其余=致命；sink 写路径一律致命、组件内有界自重连是组件行为）写成 `connector-recovery-contract` 的需求与场景，并补写该 spec 的 Purpose（现为 TBD）。
- **修剩余错位**（全部为变体重映射，不改内核运行时行为，`task.rs` 分支逻辑不动）：
  - Pulsar 无界源 `Ok(None)`→`Disconnection`（两处），删除死 `_consumer_builder`；
  - Redis input 分类器收紧（"invalid" 一词不再判定永久错误，改为结构性错误码/类别判断）；
  - SQL/pgvector output 写路径区分暂态断连（→`Disconnection`）与确定性失败（→`Process`）；连接失败 `Config`→`Connection`；
  - Redis/NATS output publish 失败→连接族变体（对齐 mqtt/kafka 兄弟）；
  - `multiple_inputs.rs` 通道关闭→`Error::InputChannelClosed`；
  - "未连接"guard 按位置定语义：output 侧与 source 建连期的 guard（`output/mongodb.rs:138`、`output/sql.rs:600,611`）统一为 `Connection(msg)`；source 接收路径的会话丢失 guard（`input/redis.rs:535` 等）保持 `Disconnection`——内核重连是恢复路径，改 `Connection` 会让 broker 断连变成致命（回归）。
- **跨连接器一致性测试**：新增按契约遍历 input/output 的钉扎测试（无界源禁止 `Ok(None)`→`EOF`、guard 变体统一、写路径暂态/确定性二分），使此类错位在 CI 阶段漏网成为不可能——这是批次 1 漏掉 Pulsar 的直接教训。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `connector-recovery-contract`: 新增内核错误变体总契约（EOF/Disconnection/Connection/其余 的 source 语义与 sink 语义）、"未连接"guard 统一变体、output 写路径暂态/确定性分类、Pulsar/Redis/multiple_inputs 的具体归类；并将 Purpose 从 TBD 补为正式陈述。

## Impact

- `crates/arkflow-core`: `lib.rs` Error doc comments 补语义（`EOF`/`Disconnection` 现无文档注释）；`task.rs:818-824` 注释指向 spec（不改逻辑）。
- `crates/arkflow-plugin`: `input/{pulsar,redis,multiple_inputs}.rs`、`output/{sql,pgvector,redis,nats}.rs` 的错误映射；新增一致性测试文件。
- 文档：pulsar/redis/sql/pgvector/nats 组件页（EN+zh）补错误行为说明（对齐批次 1 给 mqtt/nats 补的先例）。
- 用户可见差异：上述组件的失败错误消息变体/文本变化；`multiple_inputs` 在子源重连期间通道关闭从"Job 致命失败"变为按 EOS 处理——行为变化在 spec 场景中显式声明。
- 不改配置 schema、不改 `Error` 枚举成员、不改内核重连机制。

## Non-goals

- 不给内核增加 sink 级重连/重试机制（output 错误在内核仍是致命；把 mqtt 式组件内有界自重连推广到 sql/pgvector 是后续独立 change）。
- 不重构 `Error` 枚举（13 个变体成员保持不变，只治理映射纪律）。
- 不动数据面 remote 边的字符串嗅探分类器（`manager.rs` `is_deterministic_protocol_error`/`is_unexpected_eof`）——已由 `harden-data-plane-frame-auth` 治理，属另一契约域。
- 不动 codec/processor 处理路径的错误语义（schema_registry/lookup 暂态失败的处理不在本 change）。
- 不统一 6 个既有连接器分类器的实现（收编归 `consolidate-connector-skeleton` change，本 change 只钉扎输出变体的契约）。
- Ack 体系（core 4 种 + 插件 4 种本地类型）不在本 change。
