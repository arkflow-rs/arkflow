# Design: refactor-p3-misc-batch

## Context

六个互相独立的小型结构修复，全部来自 `openspec/CODE_REVIEW_2026-09-29.md` P3 杂项清单（2026-10-05 逐项核实仍存在）。目标是以最低风险收掉这一批，为后续 storage 宏化与文件拆分批次开路。

## Goals / Non-Goals

**Goals**：失败可重试的 init；checkpoint 对象存储操作不再每操作一线程一 runtime；`q()` 重写零重复计算；watcher 事件驱动唤醒 + 泄漏兜底；文档句修复；console 死常量/数据层/linter 三项收口。

**Non-Goals**：不改 `CheckpointStore` trait 契约与错误语义；不改 Hub-Agent 命令协议；不引入 console 之外的 lint 工具链（rust 侧 clippy/fmt 已有）；不做 kernel watcher 的完整生命周期重构（批次 3/4 范围外，catch_unwind 不变量不动）。

## Decisions

### 1. plugin init：成功闩锁 + 每 kind 断点续跑

实现期核实：各 kind 的 `init()` **并非各自幂等**（重跑报 "Temporary type already registered"），原顶层 `OnceLock` 是唯一幂等屏障。因此除顶层 `Mutex<Option<()>>`（成功才落位）外，为七个 kind 各设一个成功闩锁：重试链跳过已成功的 kind、从首个失败处续跑——部分失败后重试既不重复注册也不中断。失败路径不落任何状态，天然可重试；顶层 `Mutex` 锁内无重活。测试：`init_step` 以 fn 指针注入「先失败后成功」的步骤，验证失败不缓存、成功后短路、init fn 不再被调。

### 2. SharedCheckpointStore：常驻 worker 线程

`block_on` 的存在理由是 `CheckpointStore` 为同步 trait、调用方可能在 runtime 之外。方案：`static CHECKPOINT_WORKER: OnceLock<Worker>`，Worker 持有单个 OS 线程 + 该线程上的 current-thread runtime + `mpsc::sync_channel`（有界，容量 64，背压显式化）。每次操作发 `BoxFuture` 命令 + `oneshot` 回执，worker 循环 `runtime.block_on`。put/get/delete 错误字符串逐位保留（`checkpoint object store: ...` 前缀）。线程 panic 时回执侧得到 `RecvError`，映射为现有 "checkpoint object store thread panicked" 错误——但 worker 死亡不可恢复，故 worker 循环内逐命令 catch_unwind（对齐 storage actor 的 panic 隔离先例），单命令 panic 不杀 worker。

### 3. `q()` 缓存

`OnceLock<Mutex<HashMap<String, String>>>`；miss 时重写并插入。调用点 29 处全部包装常量字面量（postgres.rs 12 + postgres_methods.rs 17），缓存条目数 = 常量数，有界。返回 `String` 签名不变，调用点零改动。

### 4. kernel watcher：watch 通道无损唤醒

`Completion` 升级为 `Arc<CompletionSlot>`（结果槽 `Mutex<Option<Arc<Result>>>` + `watch::Sender<()>` 版本通道）。`complete()` 写槽后 `send(())`；`watcher()` **先订阅再检查**（先查后订会漏唤醒——晚订阅的 receiver 从当前版本起步，看不到已发生的变更），循环 = 查槽 → `changed().await`。`watch` 的版本记忆保证「检查与等待之间落地的完成」不丢失，替代原 50ms 轮询。**实现期调整**：原计划的 abort-on-drop 句柄包装会改动 `watcher()` 的公共返回类型（`KernelJobHandle` 是 batch C 刚收紧的公共 API），且泄漏场景已被 catch_unwind 不变量（completion 必然 resolve）覆盖——收益边际，故守卫部分取消，仅保留唤醒化；`watcher()` 签名不变，agent.rs 消费点零改动。测试：多观察者一次 resolution 全部唤醒 + 晚创建的 watcher 经首查立即返回。

### 5. console

- 常量接线：`queries.ts` 两处 `refetchInterval: 5_000` 改引用 api.ts 导出常量。
- JobVersions：`queries.ts` 新增 `useJobVersions(jobId)`（queryKey `["job-versions", jobId]`）与 `useRollbackJobUpgrade()`（useMutation，成功后 invalidate job-versions 与 job-detail）；`features/jobs.tsx` 的内联 useEffect/useState 整体替换。
- ESLint：devDependencies 加 `eslint`、`typescript-eslint`、`eslint-plugin-react-hooks`、`eslint-config-prettier`；flat config（`eslint.config.js`）启用 recommended + react-hooks + prettier 收尾；`lint` script = `eslint .`；`console.yml` 增 gate。存量零 error 为验收线（个别误报用行内 disable 并注明理由）。

## Risks / Trade-offs

- watcher 返回类型变化触及 agent.rs 消费点——包装 Deref 保证用法兼容，编译器兜底。
- checkpoint worker 为进程级单例，`#[cfg(test)]` 下多测试共享——channel 容量 64 + 每命令 catch_unwind，测试间无状态残留（worker 无自身状态）。
- ESLint 引入 ~5 个 devDependencies（仅 console，不进产品构建）。

## Migration Plan

纯内部行为，无配置/协议迁移。init 重试语义变化只在"此前已失败"的进程内可观测（原为永久失败，现为可恢复）——严格放宽，无回归面。
