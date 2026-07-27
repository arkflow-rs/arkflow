## Why

`Stream::do_input` 在 `loop` 之前跑 WAL recovery，但 recovery 失败的两条路径都被静默吞掉——`read_after_cursor` 整体返回 `Err` 时只 `error!` 后继续读新 input（`crates/arkflow-core/src/stream/mod.rs:199`），replay 循环中 `forward` 失败时只 `break` 出 `if` 块后继续读新 input（`crates/arkflow-core/src/stream/mod.rs:192-197`）。结果是 WAL 已经损坏或下游 channel/buffer 不可用时，stream 看起来"正常运行"，但实际违反 commit `b31220b`（feat: add durable input WAL with crash recovery）承诺的 at-least-once 契约，且没有任何上层报警。

挖了 redb 2.6.3 源码后确认 redb 在 `Database::open` 时已经走完整棵 B-tree 校验 xxh3 128-bit checksum（`redb/src/db.rs:783 do_repair`），运行时再次损坏的概率极低，但一旦发生应该让 stream 启动失败而不是降级运行。

## What Changes

- WAL recovery 从 `Stream::do_input` 顶部抽到 `Stream::run` 开头（`tracker.spawn` 之前），让 recovery 失败能直接 `?` 上抛给 Engine。
- recovery 中 `read_after_cursor` 返回 `Err` 时，`Stream::run` 返回 `Err`，stream 不进入运行态。
- recovery 中 `forward` 失败时，`Stream::run` 返回 `Err`，stream 不进入运行态。
- 更新 `input-durability` capability spec，补一条 "recovery 失败时 stream 启动失败" 的契约。
- 新增回归测试：模拟 WAL 读取失败、模拟 recovery forward 失败，验证 `Stream::run` 返回 `Err`。
- **BREAKING（行为）**：WAL 损坏或 recovery 阶段下游不可用时，stream 行为从"继续运行+log"变为"启动失败"。生产用户遇到 WAL 损坏（小概率）的恢复路径需要人工介入或回滚到 checkpoint。

## Non-goals

- 不改 `read_after_cursor` 内部"ENTRIES 表不存在 → `Ok(空)`"的语义。redb 在 `Database::open` 时已校验整棵树，能 open 成功就意味着表完整，`Ok(空)` 只在 fresh database 第一次起时触发，是正确行为。
- 不改 `read_after_cursor` 中 `iter()` 中途失败导致已读条整批丢弃的语义。触发条件需 "open 时校验通过 + 运行时再次损坏"，实际概率极低；本 change 让外层 fail-fast 后即使发生也不会静默继续。
- 不修复 `group-commit` / `periodic` 的已知丢失窗口（cursor 推进与 entry 落盘是两条独立 redb 事务，crash 时 cursor 可能领先 ENTRIES）。这是 commit `b31220b` 和归档 change `add-input-durability` design.md 已经文档化的权衡，需要完全 durable 时使用 `per-entry`。
- 不引入"WAL 损坏后降级为非 durable 继续运行"的可配置策略。如果将来有用户需要这种韧性，再开单独的 change 讨论 `recovery_policy: FailFast | Continue`。
- 不改 WAL 文件格式、序列化格式、配置格式或 sync 策略语义。
- 不改 Engine 层（`cli.rs`）对 `Stream::run` 错误的处理——已经会让进程退出，本 change 只依赖现有行为。

## Capabilities

### New Capabilities

无。

### Modified Capabilities

- `input-durability`: 新增 "recovery 失败时 stream 启动失败" 的契约——`read_after_cursor` 返回 `Err` 或 replay 阶段 forward 失败时，`Stream::run` SHALL 返回 `Err`，stream 不进入运行态。

## Impact

- 代码：
  - `crates/arkflow-core/src/stream/mod.rs`：把 recovery 块从 `do_input` 移到 `Stream::run`；`do_input` 删掉 recovery 块；新增的 recovery 流程在 `tracker.spawn(Self::do_input(...))` 之前。
  - `crates/arkflow-core/src/stream/mod.rs` 测试模块：新增 2 个回归测试。
- Spec：
  - `openspec/specs/input-durability/spec.md`：新增 1 条 requirement（recovery 失败 fail-fast），2 个 scenario。
- 运行时行为：WAL 损坏（罕见）或 recovery 阶段下游不可用时，进程会因 stream 启动失败而退出，依赖 k8s/systemd 重启策略。
- 性能：recovery 抽到 `run` 开头只是控制流重组，不引入额外开销。
- 兼容性：无配置 / 文件格式变更；行为变更需在 changelog 强调。
