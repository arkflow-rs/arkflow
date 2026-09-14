## Context

- 验证基础:`two_node_job_smoke.rs` 已验证「真实 `agent::run` 循环 + loopback `serve_hub` + 共享 `ControlPlaneStore` + agent kill/restart」模式(18s 内 2 节点含 checkpoint/恢复);harness 按同型放大即可获得 HTTP/序列化/重连退避/会话 TTL 的全部真实路径。
- 压力模型(2026-09-13 探索结论):每 agent 稳态 ≈2.5 req/s(heartbeat lease/3、report 2s、poll 1s);主导写放大是 report 的 `cp_stream_observed` × S streams;`MAX_NODES=256` 满载理论写速 ~1280 行/s 对单写者 StorageActor;绝对会话 TTL(默认 1h)在加速时钟下可压成持续 re-register 负载。
- 有界面(已由 `bound-control-plane-storage-history` 闭合):operations/processed-outbox/terminal-attempts 24h+4096 双界、audit 30d/100k、checkpoint 记录 24h、events 2048、内存侧 MAX_OPERATIONS/MAX_EVENTS/commands-128-per-node;清扫挂独立 60s 维护任务。
- 既有容量常量:`MAX_NODES=256`(hub.rs:26)。

## Goals / Non-Goals

**Goals:**

- 把「长稳与规模上限」从口号变成可判定的自动化测试:有界性收敛、中断恢复、满员舰队功能完备——三类硬断言进 CI 或 `#[ignore]` 套件。
- 产出环境相关的容量数字(延迟百分位、RSS 平台、knee 位置)并文档化,供容量规划引用。
- 保持 CI 预算:门禁版合计新增 < 3 分钟。

**Non-Goals:**

- 绝对性能门禁(环境相关,只记录)。
- 生产代码变更(缺陷走独立修复)。
- 状态后端替代方案、network shuffle、WAL/S3 长稳、v1 合入。

## Decisions

### D1:真实 loopback 舰队,不用进程内直调

在「进程内直调 Hub 方法(快但无 HTTP/序列化)」与「真实 `agent::run` + loopback(慢但全保真)」之间选后者:`two_node_job_smoke` 已证明该模式可行且 2 节点仅 18s;验证的价值恰在真实路径(重连退避 jitter、Bearer 认证、会话 TTL 过期 401→re-register、report 写放大)。加速手段不是缩短断言而是**注入小间隔**:测试构造 `NodeAgentConfig`(heartbeat/report/poll 间隔)与 `HubConfig`(`poll_interval_ms`、`lease_ttl_ms`、`session_ttl_ms`)直接可调——命令到达终态的判定在 Hub 侧 API 上做,与 agent 实现细节解耦。

被否决:mock agent(直调 Hub 方法)——测不出认证、退避、HTTP 序列化与单写者争锁的真实形态,「验证」名不副实。

### D2:双层测试矩阵,`#[ignore]` 承载重负载

| 测试 | 舰队 | 时长 | 门禁 |
| --- | --- | --- | --- |
| `staircase_ci` | N=8→32 阶梯 | <60s | CI |
| `soak_ci` | N=16,TTL=2s,重启风暴×3 | ~2min | CI |
| `staircase_full`(ignore) | N=25→256 全阶梯 | ~10-20min | 手动 |
| `soak_one_hour`(ignore) | N=64,TTL=5s,重启×10/重生×3 | ≥60min | 手动 |

理由:CI 门禁保证回归可发现(短 soak 的收敛断言在小规模下与 256 满员同构——有界性不随 N 改变,只随累计操作量改变);完整版给出容量数字但不阻塞 CI。两版共享同一 fixture 与断言函数,只差参数。

### D3:硬断言只限正确性与有界性;延迟/RSS 只记录

CI 可门禁的(环境无关):
1. **功能完备**:每梯级/每轮注入后,派发的全部命令在超时内到达终态(`HubOperationState` 终态集合),注册表全部节点可查询;
2. **有界收敛**:静默期后,`cp_operations` 终态行、processed outbox、cp_attempts 终态行、cp_audit_events 均收敛到各自 bound(直接 SQL COUNT 断言 ≤ bound + 一个梯级余量);
3. **中断恢复**:Hub 重启(同 `ControlPlaneStore` 重建 Hub + `serve_hub`)与 agent 全灭重生后,期望态重新收敛;
4. **RSS 斜率**:soak 后半程 RSS 线性回归斜率 ≈ 0(允许一个小的绝对余量,吸收 allocator 抖动)。

只记录不门禁的:命令派发 p50/p99(从 Hub 侧派发→终态的实测耗时统计)、各 N 的 RSS 绝对值、knee 位置——写入 PLANNING.md。

被否决:p99 相对退化门禁(如 256 级 ≤ 10×32 级)——共享 runner 的 CI 噪声足以击穿任何紧阈值;松阈值又测不出真退化,不如记录。

### D4:重启风暴 = 同 store 重建 Hub;agent 重生 = 真实进程重启语义

Hub 重启:丢弃 `Hub` 与 `serve_hub` 任务,用**同一个** `ControlPlaneStore` clone 重建——精确模拟真实重启(持久态保留、内存态全失:session token 清空 → 256 agent 全量 401 → jitter 化 re-register 洪峰,恰好压测 `harden-agent-session-credentials` 的风暴路径)。agent 重生:abort 旧 `agent::run` 任务,以同 `node_id` + 同 `boot_id` 重启(走既有 re-register 保留语义)。

### D5:RSS 采样零依赖

Linux 读 `/proc/self/statm`;其他平台(本机 macOS、CI Linux)回退 `ps -o rss= -p <pid>`(每样本一次子进程,1s 级采样率可接受)。采样函数 `#[cfg]` 分派,无法获取时返回 None,断言降级为跳过并记录——验证不因平台缺能力而假失败。

### D6:会话 TTL churn 纳入 soak 负载模型

soak 的 `session_ttl_ms` 设为秒级(短版 2s/长版 5s),使 re-register 成为持续背景负载(每 agent 每数秒一次注册全路径:upsert_node + cursor 重置 + reconcile),与 Job churn、重启风暴叠加。这是 `harden-agent-session-credentials` 落地后新增的稳态负载形态,必须进长稳模型。

## Risks / Trade-offs

- [CI 时长被 soak_ci 拖长] → 硬预算 2min;若超,降到 N=8/60s 并在 PR 记录(参数集中于一个 const 区)。
- [共享 runner 上 RSS 斜率误报] → 斜率断言只设在 soak(非 staircase),余量取平台期典型噪声的量级(实现时以首轮实测校准,常量注释记录依据)。
- [加速时钟下 report 写放大压垮单写者导致超时假失败] → report 间隔取 200ms(真实 2s 的 1/10)而非无节制压频;终态判定超时给足(秒级);失败时输出当前 outbox/operations 快照辅助定位。
- [`#[ignore]` 版本年久失修] → CI 版与完整版共享断言函数(仅参数不同),完整版退化会被 CI 版的共享代码编译期覆盖;另在 tasks 中约定归档前手动跑一次完整版并回填 PLANNING.md 数字。
- [port 冲突/资源耗尽(256 agent × 每秒循环)] → loopback `TcpListener:0` 随机端口;agent 任务共享一个 reqwest Client(连接池复用);N=256 档仅存在于 `#[ignore]` 版本。

## Migration Plan

纯新增测试设施,无部署/回滚问题。CI 若因 runner 波动出现 soak_ci 抖动,参数(时长/舰队规模/余量)集中于文件头部常量区,单点调整。

## Open Questions

- 无。容量数字在归档前的真实运行后回填 PLANNING.md(tasks 已含该步)。
