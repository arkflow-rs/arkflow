## Context

P1（#1251）/ P2（#1252，经 #1253 重新落地 main）之后，代码审查遗留 7 项 P3。关键现状：

- `vector_search.rs:146` 拼 `{base}/collections/{collection}/points/search`、`qdrant.rs:165` 拼 `{base}/collections/{collection}/points?wait=true`——集合名含 `/`、`?`、`#`、空格或 `%` 时产生错误请求；pgvector 走 SQL 绑定（P2 已转义标识符），milvus 的集合名在请求体（`collectionName`）不受影响。
- `embedding.rs:131-138` `embed_all` 对 `texts.chunks(batch_size)` 逐块 `await`，块间零并发。
- `pgvector_search.rs:190-191` `query_as::<(String, Option<String>, f64)>`：id 列为 NULL 时 sqlx 在解码阶段报 `UnexpectedNullError` 类错误，不含行上下文。
- `qdrant.rs:357` `random_uuid_v4` 函数体首行多 8 空格缩进。
- `benchmark.rs:57-63` `setup_plugins` 用 `let _ =` 丢弃五个 `init()` 的错误（P1 之后 init 会真实失败并返回错误，静默丢弃会让后续场景以"unknown component"误导性失败）；`state_backend`（193-215）在 async fn 里同步循环数千次 redb durable put/get（每写一次 fsync 提交）；`run_suite`（234）`assert!(runs >= 1)`、（253）`best.expect("runs >= 1")`；`examples/benchmark.rs:38-41` 对 `--count` 等缺失值 `expect` panic。

## Goals / Non-Goals

**Goals:**

- 七项全部落地，各带回归测试（基准相关以单测覆盖纯逻辑路径）。
- 行为零变化（除明确修复点：URL 编码使特殊集合名从错误请求变为正确请求；`run_suite` 参数错误从 panic 变为 `Err`）。

**Non-Goals:**

- 不动测量口径/默认参数；不新增配置项。
- `secret.rs` 同步文件读取维持 P2 记录的已接受权衡。

## Decisions

**D1 — 路径段编码放 `vector_util`，最小百分号编码**
新增 `vector_util::encode_path_segment(name) -> String`：对 RFC 3986 `pchar` 之外的字节做 `%XX` 大写十六进制编码（字母/数字/`-._~` 保留，其余全部编码——保守正确，无需依赖新 crate）。`vector_search::search` 与 `qdrant::upsert` 的集合名先编码再拼 URL。编码后的 URL 对常规名（字母数字连字符下划线点）逐字节不变，既有测试的 URL 断言不受影响；特殊名测试断言 `%2F` 等编码输出。

**D2 — embedding 分块流水化：`buffered(concurrency)` + `try_collect` + 顺序拼接**
`embed_all` 改为对块索引 `stream::iter(...).map(|chunk| embed_chunk(chunk)).buffered(concurrency).try_collect()`；`embed_chunk` 返回 `Vec<Vec<f32>>`，完成后按块顺序展平拼接（`buffered` 保序，无需重排）。内存上界 = concurrency × batch_size 个向量，与现单块驻留同量级。`concurrency` 默认 1 时退化为顺序行为（默认值不变，规格场景不受影响）。

**D3 — pgvector_search NULL id：解码前移 + 定向报错**
`search_row` 的解码类型改为 `(Option<String>, Option<String>, f64)`；`rows_to_matches` 入口校验 `id` 为 `None` 的行，返回 `Error::Process("pgvector_search processor: id column '{id_column}' is NULL for a matched row")`。备选（SQL 端 `WHERE id IS NOT NULL`）静默丢行改变语义，弃用。

**D4 — benchmark 参数与错误处理**
- `setup_plugins() -> Result<(), Error>`：五个 `init()` 用 `?` 链接（P1 后错误真实传播），`run_suite` 开头 `setup_plugins()?`。
- `run_suite` 校验：`count == 0 || runs == 0 || warmup 参数溢出` → `Err(Error::Config(...))`，删除 `assert!` 与 `expect`。
- `state_backend` 的 put/get 循环整体包进 `tokio::task::spawn_blocking`（闭包内持 `RedbStateBackend`；返回 `Result<_, Error>` 透传）——async worker 不再被 fsync 阻塞；测量口径（操作数/计时区间）不变。
- 示例入口：`next_arg(args, "--count")` 助手，缺失值输出 `usage` 提示并 `std::process::exit(2)`；未知参数同样报错退出（替代仅 eprintln 后继续）。

**D5 — qdrant 缩进**：`random_uuid_v4` 函数体统一 4 空格缩进（纯格式）。

## Risks / Trade-offs

- [D1 保守编码改变极少数现有集合名的 URL 字节] 保留集（unreserved）之外的字节本就语义危险；常规名逐字节不变，既有 URL 断言测试保持通过。
- [D2 embedding 并发改变请求到达时序] 有界（默认 1 = 现状），服务商限流场景不受默认影响；失败短路语义与其他处理器一致。
- [D4 state_backend 移入阻塞线程] 计时包含 spawn_blocking 调度开销（µs 级，相对数千次 fsync 可忽略）；测量语义不变。

## Migration Plan

纯修复，无迁移；回滚 = revert。实现顺序：D1 → D2 → D3 → D5 → D4，每组附回归测试。

## Open Questions

（无。）
