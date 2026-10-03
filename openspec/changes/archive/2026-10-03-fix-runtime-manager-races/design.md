## Context

runtime.rs 的 RuntimeManager 管理本地 YAML stream 的生命周期（register/start/stop/restart/replace_config）。两个已核实缺陷：索引复用（entries.len() 在 remove 后被重用，导致 job id / 状态命名空间冲突）与 stop/restart 竞态（restart 在两个锁块之间释放锁，并发 stop 可插入并触发流复活）。

## Goals / Non-Goals

**Goals:** 注册索引全局唯一（remove 后不复用）；stop/restart 的生命周期命令原子性（stop 返回 Ok 时流不 Running）。

**Non-Goals:** 锁架构重写、replace_config 语义变更、WAL 锁残留（见 proposal）。

## Decisions

**D1 — 索引改用全局单调 AtomicU64。**
`next_index: AtomicU64`（RuntimeManager 字段），`register` 用 `fetch_add` 取索引。remove 后重注册拿到新值——索引在**全注册史**内唯一，彻底消除 job id / 状态命名空间 / 路径碰撞。初始化从 0 起与现有行为对齐（首次注册拿到与 len 相同的值）。*备选否决*：用 entries 的 max(index)+1（需遍历，且 remove 后仍可能回缩到被 remove 的值区间——只要索引不删除就不复用，但 max+1 在全部 remove 后回 0，仍碰撞史上的目录名）。

**D2 — restart 的状态转换与 handle 取出合并为一个锁块。**
原代码两个锁块之间释放锁是竞态窗口：并发 stop 看到 `Restarting` 后在第二个锁块取走 handle（第一个已空）→ join → 置 Stopped → 返回 Ok → restart 侧 wait_result Ok → start → 流复活。修复：restart 的第一个锁块同时完成"状态置 Restarting + token 取消 + handle 取出"——stop 后续看到 `Restarting` 时 handle 已被 restart 取走，stop 走 `None => Ok(())` 路径，不会抢到 handle 也不会把状态置 Stopped（stop 的 `Created | Stopped => return Ok(())` 不覆盖 Restarting；`Restarting` 走 `_ => {}` 到 Stopping 转换——但 handle 已 None，`Stopping` 转换后 `handle.take()` 返回 None → `wait_result Ok` → 置 Stopped → **这仍有问题**：stop 把状态置为 Stopped，restart 随后又 start → 流 Running。更完整的修复：**stop 的 Stopping 分支加 `Restarting` 到 early-return**——`Restarting` 意味着 restart 已经在管理生命周期，stop 不应干预；或 restart 在锁块内置 `Stopping`（而非 `Restarting`）使 stop early-return。

选定：restart 的锁块将状态置为 `Restarting` + 取 handle；stop 的 match 增加 `Restarting => return Ok(())`（restart 正在管理该流的生命周期，stop 的语义由 restart 的内部 stop 阶段完成——restart 最终调用 start，所以 stop 的意图"停止"与 restart 的意图"重启"冲突时，后发起者胜出但至少不产生中间态复活）。这保证 stop 返回 Ok 时流要么 Stopped（stop 自己停的）要么 Restarting→Running（restart 赢了，但 stop 诚实地返回 Ok 而非错误——语义可接受：stop 请求的是"停止当前运行"，restart 的内部 stop 已完成该动作，后续 start 是 restart 的新意图）。

**D3 — 测试策略。**
索引唯一性：`register→remove→register` 断言新 index > 旧 index；或者用 `replace_config` 全量替换后断言存活流的 job spec id 不冲突（`compile_stream` 的 `stream-{index}` 唯一）。stop/restart：构造真实 runner，发起 restart 后立刻并发 stop，断言最终态不是"stop Ok 但 Running"（需要观察 start 后的 cancellation token 或 metrics）。

## Risks / Trade-offs

- [AtomicU64 起始值与既有 len() 不一致] → 从 0 起，首次注册对齐；存量运行时索引在重启后可能不同——但重启后 index-derived 名本来就重算，唯一性约束只要求同进程注册史内唯一。
- [stop 在 Restarting 时 early-return 而非执行完整 stop] → restart 的内部 stop 阶段已做了 token 取消 + handle join——stop 的"停止当前任务"意图已由 restart 履行。文档明示。

## Migration Plan

无配置/格式变更。回滚 = revert。legacy 无 id 流在 replace_config 后拿到新 index（不同于旧 index）——但这正是修复目的（旧 index 可能碰撞）；显式 id 流零变化。
