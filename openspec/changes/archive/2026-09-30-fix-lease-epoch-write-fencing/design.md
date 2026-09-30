## Context

租约 CAS（takeover epoch+1）正确，但写路径无 epoch 谓词：旧 leader 在 ≤ ttl/3 的感知窗口内可继续写。存储访问全部经单写者 actor（FIFO，`mpsc`）——天然的单一校验咽喉。61 个 handle 方法 = 20 读 + 3 租约操作 + 38 变更。

## Goals / Non-Goals

**Goals:** 旧 leader 的写在存储层被显式拒绝（`StaleLeader`）；standalone（无租约行）零行为变化；租约三操作与读不受影响；双后端契约一致。

**Non-Goals:** 见 proposal（阶段 3、即时 step-down、公共签名变更）。

## Decisions

**D1 — 信封变体 + 执行期校验，而非 45 个方法加参。**
`StorageCommand::Fenced { claimed_epoch: u64, command: Box<StorageCommand> }`。围栏的包装方法（38 个）经 `send_fenced` 发送：**发送时**从句柄的 `Arc<AtomicU64>` 捕获当前声明 epoch；actor 收到后在**执行前**调 `store.current_lease_epoch()` 比对——`None`（无租约行，standalone）或 `Some(current) == claimed` 才执行，否则 `command.nack(StaleLeader)`。TOCTOU 关键点（CodeRabbit 复审后修正）：校验必须与写**同锁串行**，而非仅"执行期检查"——每个 Hub 进程各有自己的 actor，跨进程的 takeover 可以插进"检查后、写入前"。实现为守卫协议：PostgreSQL 在独立事务中对租约行取 `FOR SHARE` 并横跨整个变更（takeover 的 UPDATE 阻塞到守卫提交）；SQLite 持有开放的 `BEGIN IMMEDIATE`（写锁）横跨变更，内部 `immediate_transaction` 以 SAVEPOINT 嵌套。由此任何通过校验的写在线性序上先于使其失效的 takeover。*备选否决*：给 `StorageBackend` 全部写方法加 epoch 参数（45 处签名 + 双后端 SQL 谓词，且逐条 SQL 加 `WHERE` 易漏）；在 Hub 内存层判 leader（TOCTOU 不闭合，正是本缺陷根因）。

**D2 — `nack` 机械生成，61 变体均有 `response` 字段。**
拒绝需向内层命令的 oneshot 回错误——response 类型各异但 `Err(err)` 对任意 `Result<T, _>` 可推断。为 61 个真实变体各生成一行臂 `X { response, .. } => { let _ = response.send(Err(error)); }`（`Fenced` 自身无 response，排除）。脚本从枚举源生成，纯机械。

**D3 — 38/20/3 分类按名字显式列举（脚本内白名单），不做启发式。**
读（get_*/list_*/operational_aggregates）与租约（try_acquire/renew/release_hub_lease）直通；其余 38 个变更全部围栏。分类错误的两面风险：漏围（写未受保护）或多围（读被误拒）——白名单双向断言（脚本校验 61 全覆盖、无交集）。

**D7 — UNFENCED 哨兵（CodeRabbit 复审补）**：句柄初值 `u64::MAX`（HA 禁用，不围栏——残留租约行不影响）；`enter_election`（HA 启用）置 0 进入参与态；Disabled 转换不清写哨兵。"无租约行直通"与"UNFENCED 直通"是两个独立的豁免面。

**D4 — epoch 接线：选举 tick 写句柄原子量。**
`StorageActor::start` 创建 `leadership_epoch: Arc<AtomicU64>`（初值 0），`pub fn leadership_epoch()` 取引用。`run_election_tick` 在计算出新的 `Leadership` 后同步：`Leader { epoch } → store(epoch)`，其他 → `store(0)`。语义：standby/未启用（0）在**有租约行**时被 fence（standby 不得写，standby 中间件之外的内部路径同样被拦）；无租约行（standalone）永远直通。promote 路径：acquire 成功（tick 内）→ epoch 已写入 → 随后的 recovery 写（recover_* 亦在 38 个围栏方法内）携带新 epoch 通过。*顺序约束*：先 acquire 后 recovery 的现有顺序（leadership.rs promotion 先重载持久状态）天然满足——tick 先更新 epoch 再返回，recovery 由后续 tick/调用发起。

**D5 — `current_lease_epoch` 为后端 trait 方法，非命令。**
actor 持有 `ControlPlaneStore` 直呼（不经通道，无自嵌套）；SQLite/Postgres 各一条 `SELECT epoch FROM cp_hub_lease`。契约测试断言 acquire/takeover 后该值与租约返回一致。

**D6 — 错误与映射。**
`StorageError::StaleLeader { claimed_epoch, current_epoch }`；`hub_problem` 新臂 → **503** `stale_leader`（与 standby 503 家族一致；不是 4xx）。reconcile 现有错误处理已记录失败 tick——无需逐调用点改造。

## Risks / Trade-offs

- [nack 的 61 臂是样板] — 脚本生成 + 编译器穷尽性检查；后续新增命令变体时 `nack` 缺臂编译失败，倒逼分类（防漏围）。
- [围栏粒度为"整个命令"，细粒度 CAS（generation/幂等键）仍在 SQL 内] — 双保险关系：generation CAS 防并发语义冲突，epoch fence 防旧 leader 写；两者互补不互斥。
- [actor 每个围栏命令多一次 epoch SELECT] — SQLite 内存级单行读（µs）；Postgres 一跳 RTT，控制面 QPS 低；如成瓶颈，后续可在 actor 内缓存租约 epoch 并由租约命令更新（Non-goal）。
- [读不围栏] — 旧 leader 的读可能短暂陈旧，但 standby 中间件与 reconcile 的写保护才是正确性面；读围栏会破坏 standby 的维护清扫。

## Migration Plan

无配置/存储格式变更。未启用 HA（无租约行）逐位不变；启用 HA 的双实例在滚动升级期间：新代码 leader 正常，旧代码 leader（无围栏）写不被拒——围栏保护的是"新代码存储层拒旧写"，混布期建议先升 standby 后升 leader（与既有 Hub 混布顺序约束一致）。回滚 = revert。
