## Context

内核三分法契约目前只存在于 `crates/arkflow-core/src/executor/task.rs:818-824` 的注释：source 读循环 `EOF`→优雅结束（:772-817）、`Disconnection`→`source.connect()` 重连（:825-867，失败退避 5s）、其余→致命（:868-875）；sink 写路径**一律致命**（:2965-2983），`InputChannelClosed` 在 interior chain 按 EOS 处理（:1521-1544，产生于 :2749）。`Error` 枚举 13 个变体（`lib.rs:80-123`），其中 `Disconnection`/`EOF` 是**无载荷 unit 变体**、无文档注释——错误消息丢失全部上下文（Display 只有 "Connection lost"/"EOF"）。

`openspec/specs/connector-recovery-contract/spec.md` 已有 5 个需求（批次 1 产物），但未覆盖：内核总契约、"未连接"guard 变体纪律、output 写路径分类、分类器判定方式，且 Purpose 是 TBD。连接器侧现状（HEAD 8ffbc386 逐一核实）：无界源 Pulsar `Ok(None)`→`EOF` 违反既有需求 4；redis 分类器子串过宽；sql/pgvector/redis/nats output 错位；guard 轮盘 `Connection(msg)` ×12 / 裸 `Disconnection` ×3 / `Process` ×1；无跨连接器一致性测试（批次 1 漏 Pulsar 的直接原因）。

## Goals / Non-Goals

**Goals**：契约成文（spec + `lib.rs` doc comments）；修剩余错位（变体重映射，`task.rs` 逻辑不动）；建立结构性钉扎测试让同类错位在 CI 漏网不可能。

**Non-Goals**：见 proposal。特别地：不改 `Error` 枚举成员与载荷形态；不给内核加 sink 重连；不收编 6 个既有分类器的实现（归 `consolidate-connector-skeleton`）。

## Decisions

### D1 — 契约的规范表述：按"读路径/写路径/guard"三位置分别定义，而非一个全局规则

- **source 读路径**（内核强制）：`EOF`=有界源自然耗尽；`Disconnection`=已建立会话的接收失败，重连有帮助（内核唯一会触发重连的变体）；`Connection`=建连期/状态永久错误（致命——按既有注释，无限重试 `Connection` 会 wedge 关停）；其余致命。
- **sink 写路径**（内核一律致命，变体是**文档级语义**）：`Disconnection`=暂态传输失败、换新连接重试可能成功；`Process`=确定性失败（语法/约束/编码）；`Connection`=运行期建连失败。组件内有界自重连（mqtt output，既有需求 2）是组件行为，不依赖内核。
- **guard**：按位置——source 接收路径的会话丢失 guard → `Disconnection`；source 建连期与 output 侧 guard → `Connection(msg)`。
- `InputChannelClosed`：interior chain 通道关闭专用，组件不得重包成 `Process` 字符串。
- **否决的替代**：①全局统一 guard 为 `Connection`——会使 mqtt/websocket/redis input 的 broker 断连变致命（回归，且违背既有注释"reconnecting connectors normalize transient receive errors to Disconnection"）；②把 `Disconnection` 改成带载荷变体以保留上下文——枚举签名变更，波及内核 match 与全部调用点，超出"治理映射纪律"的范围（记为后续候选）。
- 落点：spec 需求 + `lib.rs:99-110` 给 `Disconnection`/`EOF` 补 doc comment + `task.rs:818-824` 注释追加指向 spec 的一行。

### D2 — sql/pgvector 写路径分类的实现：复用 `input/codec_helper.rs` 的既有分类器

`classify_stream_error` + `sqlstate_is_retryable`（`crates/arkflow-plugin/src/input/codec_helper.rs:31-103`）与 output 同 crate 可直接引用。sqlx 传输族错误→`Disconnection`；SQLSTATE 在可重试集（如 57P01）→`Disconnection`；语法/约束/标识符类→`Process`。**运行期**建连失败（server 不可达）`Config`→`Connection`；**构建期**的非法 URL/驱动参数保持 `Config`（"配置错"与"连不上"是不同故障域）。pgvector 同规则。
**否决**：为 output 新写第三套分类器——正是本 change 要消灭的形态；实现收敛归 change B。

### D3 — 一致性测试：结构性 tripwire + 行为钉扎两层

跨连接器语义测试不可行（input 无 bounded/unbounded 元数据，加元数据是 schema 面变更）。采用两层：

1. **结构性 tripwire**（新文件 `crates/arkflow-plugin/tests/connector_error_contract.rs`，文本扫描自身源码）：
   - **R-EOF**：`src/input/*.rs` 中每个 `Error::EOF` 出现点，要么文件在有界源 allowlist `{file.rs, sql.rs, memory.rs, multiple_inputs.rs}`，要么所在行带 `// contract-eof: teardown` 标记（teardown 路径：receiver 关闭/取消——mqtt:238,243、nats:401,406、websocket:191,195、pulsar:393,398 等需补标记）；`src/output/*.rs` 禁止 `Error::EOF`。该规则今天即红（pulsar:290-301,350-356 无标记），修复后绿——这正是 tripwire 的验收方式。
   - **R-GUARD**：`src/output/*.rs` 中 `Error::Disconnection` 仅允许出现在写路径暂态分类的 allowlist 文件 `{sql.rs, pgvector.rs}`（guard 误用如 mongodb:138 → 改 `Connection(msg)` 后绿）。
   - **R-CHANNEL**：`src/input/multiple_inputs.rs` 必须出现 `Error::InputChannelClosed`（防回退到 `Process` 重包）。
2. **行为钉扎**：pulsar mock 流结束→`Disconnection`（对齐批次 1 `nats.rs:764-788` 先例）；redis 分类器单测（"invalid" 字样不再判永久）；sql/pgvector 分类器单测（离线函数级——CI 无 PG，先例 `input/sql.rs:591-757`）。

**否决**：纯逐连接器行为测试——正是漏掉 Pulsar 的模式；注册表加 bounded 元数据——扩大 schema 面，收益不成比例。

### D4 — `multiple_inputs.rs` 通道关闭改 `InputChannelClosed` 是显式行为变化

现状 :103 重包 `Process` → 内核致命；改后按 EOS 处理（task.rs:1521-1544）。语义正确（兄弟链已关，整链自然结束），与内核对 `InputChannelClosed` 的既有处理一致。风险：若某子源在重连中短暂关通道，此前表现为 Job 失败、改后表现为等待重连——这是修复而非回归，spec 场景显式钉扎。

### D5 — redis 分类器收紧的判定方式

`is_temporary_redis_error`（`input/redis.rs:99-115`）从子串匹配改为**结构性判定**：redis ErrorKind/错误类别枚举优先（认证/权限/命令不存在→永久；IO/超时/断连→暂态），子串只允许白名单精确短语而非 `"invalid"` 这类单词。方向：宁可误判暂态（内核 5s 退避重连、可恢复）也不误判永久（Job 死亡）——与 D4 同一哲学。

## Risks / Trade-offs

- [结构性 tripwire 可被宏/字符串拼接绕过] → 接受：tripwire 的价值是把静默漂移变成可评审 diff，不是语义证明；配合行为钉扎层。
- [redis 收紧后部分原先"永久"的错误进入 5s 退避重连循环] → 内核重连有退避且可恢复；比误判永久导致 Job 死亡好。spec 场景钉扎典型错误归类。
- [output 变体是文档级语义、今天不改变运行时] → 明示于 spec（sink 致命语义写进需求）；价值在错误消息/审计诚实与未来 sink 重连的接缝，不虚称立即的可用性收益。
- [CI 无 PG，sql/pgvector 分类只能函数级测试] → 沿用 `input/sql.rs` 分类器单测先例；真实传输分类由 `classify_stream_error` 的既有集成测试背书。

## Migration Plan

无配置/数据迁移。行为变化两处（pulsar 流结束不再优雅终止 Job；multiple_inputs 通道关闭不再致命）写入 CHANGELOG 与组件文档（EN+zh 锁步）。`connector-recovery-contract` 的 TBD Purpose 在归档时从 proposal 回填（#1382 先例）。

## Open Questions

无——分歧点均已在 D1-D5 内定盘。
