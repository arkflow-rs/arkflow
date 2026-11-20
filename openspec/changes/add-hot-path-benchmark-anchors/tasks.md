## 1. window-journal 场景（#25 锚点）

- [x] 1.1 `benchmark.rs` 新增流式负载变体：generate（json codec 直出 Arrow，NDJSON 模板承载 N 个不同键）→ tumbling_window buffer（`key_field: sensor`，size 拉大运行期不触发 fire）→ drop，附 `state: {backend: embedded_kv, durability: ephemeral, max_pending_transactions: 随键数}` 段使生产编译路径构建 journaled 窗口 operator
- [x] 1.2 以 N=1/100/1_000 三档注册 `window-journal-n1` / `window-journal-n100` / `window-journal-n1k` 进 `run_each`（沿 avro-decode-w* 命名先例；顶档原定 10k，实测 EOF fire 逐键提交 ~178s 收缩为 1k）；默认行数满足"数十秒内完成"契约
- [x] 1.3 smoke 测试：极小行数下三档场景返回有限正吞吐、报告含全部场景名

## 2. sql-hot-cache 场景（#26 锚点）

- [x] 2.1 经 `ProcessorConfig::build("sql")` 构造处理器，untimed 预热两批使三级 plan 缓存全命中，计时同 schema 1000 行批 × K 的稳态循环；注册进 `run_each`
- [x] 2.2 smoke 测试：场景返回有限正吞吐

## 3. json-decode-w200 变体（#27 增强）

- [x] 3.1 以 200 列宽负载新增 `json-decode-w200` 场景（取 `component/json.rs:221` 计时测试的 wide 负载形态），注册进 `run_each`；迭代上限 30 档位控制默认行数下的时长
- [x] 3.2 smoke 测试：场景返回有限正吞吐

## 4. 文档

- [x] 4.1 `docs/docs/develop/benchmark.md` 场景表补漏列的既有 `json-decode` 行，并新增 `window-journal-n*` / `sql-hot-cache` / `json-decode-w200` 四行
- [x] 4.2 zh-Hans 对应页（docs/i18n/zh-Hans/.../develop/benchmark.md）同步上述表格变更

## 5. 收尾验证

- [x] 5.1 CLI 端到端验证：`cargo run -p arkflow --example benchmark -- --count 极小值` 全套件跑通且报告含全部场景（本机 libduckdb 在 release profile 编译失败——已知限制，`--release` 的时长与数字验证依赖 CI benchmark.yml）
- [x] 5.2 `openspec validate --all --strict` 通过
