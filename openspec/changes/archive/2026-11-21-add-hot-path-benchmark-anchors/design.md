## Context

三个热路径性能 P1 修复前的验收基准缺失。仓库已有自建套件：`crates/arkflow-plugin/src/benchmark.rs`（`run_suite(count, warmup, runs)` best-of-N 取最小 wall；`ScenarioResult{operations, wall}` → rows/s）、CLI 入口 `crates/arkflow/examples/benchmark.rs`、CI 观测工作流（benchmark.yml：main push / 手动 / `benchmark` label PR，markdown 进 job summary + JSON artifact 90 天）。

关键可见性约束：`StateJournal` 是 `pub(crate)`（state_journal.rs:191）、`persist_buffers_for_fired` 私有——arkflow-plugin 无法直接微基准窗口持久化内部；`SqlProcessor` 私有但 `arkflow_core::processor::ProcessorConfig::build`（processor/mod.rs:126）公共注册表可用 `{"type": "sql", "query": ...}` 构造。

## Goals / Non-Goals

**Goals:**
- #25/#26/#27 三个热路径各有一个可复现、无外部依赖、计入 CI 报告的基准场景
- #25 的场景能直接呈现"每批成本随键基数增长"的曲线（O(全键) 代价可视化）
- 验收协议明确：修复 commit 与锚点 commit 的 JSON 报告同机同命令 diff

**Non-Goals:**
- 不改三个热路径本体；不引入 criterion；不加吞吐断言；CI 工作流不改

## Decisions

**D1 不引入 criterion，沿用 ScenarioResult + best-of-N 约定。** 现有 9 场景全是一个 async fn + `run_each` 一行的模式；CI 观测闭环（benchmark.yml + artifact 契约）已存在。criterion 引入 dev-dep + 第二套报告格式，与"一条命令可复现、报告格式稳定"的既有 spec 契约冲突。GitHub 2 vCPU runner 上 best-of-N Instant 计时对"前后对比"足够。

**D2 #25 走生产流路径，不走 in-core 微基准。** stream YAML（generate（json codec 直出 Arrow）→ tumbling_window buffer（`key_field: sensor`）→ drop）+ `state: {backend: embedded_kv, durability: ephemeral}` 段：生产编译路径（graph.rs:1110 对有 state 的窗口一律 `with_journal_and_late_event_policy`）保证 operator 必然 journaled，全部公共 API。generate 复制静态模板，键基数由模板承载（N 行不同 sensor 的 NDJSON + batch_size 平铺，三档统一 ~1000 行/批）。窗口 size 拉大（1h）运行期不 fire，稳态 = 每批按键分配 + journal persist。键基数三档 N=1/100/1_000 沿 avro-decode-w5/w25/w100 的"宽度矩阵"命名先例（`window-journal-n1/n100/n1k`）。
*实现期修正：顶档原定 10_000，实测 EOF fire 对 10k 存活键做 10k 次逐键 journal 落盘提交（~15-18ms/键）达 178s——收缩为 1_000（三个数量级的曲线仍成立）；`max_pending_transactions` 随键数放大（默认 4_096 会被 1k+ 键的逐键事务击穿）。EOF fire 的 O(N) 逐键提交本身也是 journaled 窗口的真实成本，n1k 档使其可见；纯"每批 persist"的隔离测量留给 in-core `#[ignore]` 计时测试（可选后续）。*
*风险：默认行数下套件时长 → 窗口档 fire 提交数只随键数（与行数无关），三档合计 ~15s；实测见任务 5.1。*

**D3 #26 直接计时 process 循环，不走端到端流场景。** linear-sql 场景里 generate/JSON 解码摊薄克隆占比，看不出 `ctx.state()` 浪费。经 `ProcessorConfig::build("sql")` 构造（公共注册表，无需改可见性），untimed 预热两批使 analyzed/optimized/physical 三级 plan 缓存全命中，计时 K 批同 schema 稳态。单线程即可（pool 并发不改变单批克隆成本）。

**D4 #27 以 wide 变体增强既有锚点。** `json-decode` 场景已计无缓存推断路径；新增 `json-decode-w200`（200 列负载，取自 component/json.rs:221 计时测试的 wide 负载形态）——宽 schema 下推断在每批成本中占比更高，对"推断零缓存"更灵敏。

**D5 验收协议 = JSON 报告 diff。** 同机同命令 `cargo run -p arkflow --release --example benchmark -- --json`，锚点 commit vs 修复 commit 的报告逐场景对比；PR 修复时打 `benchmark` label 让 CI 产出带提交短 SHA 的 artifact（90 天保留）。吞吐不断言，人工对比。

## Risks / Trade-offs

- [ephemeral redb 落盘在 CI 磁盘慢导致抖动] → best-of-N 取最小 wall 吸收；场景名独立成行，报告可读。
- [窗口场景易被 generate 吞吐掩蔽（每批成本 < 生成成本）] → generate batch_size 1000 + 大窗口不 fire 保证每批都走 persist 路径；若 N=1 档差异过小属预期（锚点价值在增长曲线而非绝对值）。
- [DataFusion 升级改变 SQL 缓存行为] → 锚点意义恰在捕捉此类漂移；场景名稳定，报告跨版本可读。

## Migration Plan

无状态迁移。场景为纯新增，回滚即 revert。

## Open Questions

无。
