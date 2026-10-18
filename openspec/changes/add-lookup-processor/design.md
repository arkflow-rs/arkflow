# Design — add-lookup-processor

## Context

processor 链的组件契约：`Processor::process(&self, msg: MessageBatchRef) -> Result<ProcessResult>`，enrichment 形态返回 `ProcessResult::Single(new_batch)`（`crates/arkflow-core/src/lib.rs:192-196`）。两个直接先例覆盖了本 change 需要的全部机制：

- **llm processor**（`crates/arkflow-plugin/src/processor/llm.rs`）：`futures_util` 有界保序并发逐行 HTTP、`timeout_ms`/`concurrency` 配置面、secret 引用由配置加载层全局解析（组件内零工作）；
- **embedding processor**（`crates/arkflow-plugin/src/processor/embedding.rs:123,306`）：批后追加列（构造 `ArrayRef` + `RecordBatch::try_new`）。

内核侧 keyed interval join（`crates/arkflow-core/src/executor/join.rs`）是双流事件时间形态，不覆盖「按 key 查外部维表当前值」的处理时间 enrichment。

## Goals / Non-Goals

**Goals:**

- 流每行按 key 列查外部维表（HTTP 后端），命中的字段作为新列按行对齐追加。
- 进程内缓存：TTL + 容量上界 + 负缓存（未命中也缓存，防穿透）。
- 批内 key 去重后再发请求；有界保序并发。
- 失败与未命中语义显式可配，默认 fail-closed。

**Non-Goals:**

- 内核版本化 temporal join（CDC 维表流按事件时间匹配）——另行立项。
- JDBC / Redis / gRPC 后端——`LookupBackend` trait 预留扩展点，具体后端各自小 change。
- 维表变更主动失效（CDC 驱动的缓存失效/刷新）。
- 批量查询端点协议（POST keys 数组）——v1 用逐键 URL 模板。
- 分布式/跨进程缓存；浮点类型 key；非 equi 条件。

## Decisions

### D1 — 处理器形态，不入 Job DAG join 算子

维表 lookup 是处理时间的逐行 enrichment，不消费 watermark、不参与 barrier 状态（缓存可全量重建，丢失只会造成瞬时穿透）。做成 processor：零内核改动、YAML 单流与 Job 链内均可直接使用。**替代方案**（内核 lookup 算子）被否：需要新的 DAG 资源类型与命令面，收益只有「链级指标归属」，不值得。

### D2 — key 规范化为字符串；支持 Utf8 / Int64 / UInt64 / Bool key 列

缓存键与 URL 模板替换统一用字符串形态（`i64→"42"` 可逆无损；浮点显式配置报错，避免 `1.5` 的 locale/精度歧义）。多 key 列按列序以 `\u{1f}` 连接为复合键。**替代方案**（`LookupKey` 枚举携带原始类型传后端）被否：HTTP 模板替换终归是字符串，枚举徒增后端实现负担。

### D3 — 缓存：手写 TTL + 容量近似 LRU，不引新依赖

`Mutex<HashMap<Arc<str>, (Instant, Option<Arc<Value>>)>>` + 插入序 `VecDeque` 驱逐队列：读命中把键移到队尾（近似 LRU），容量超限时从队首驱逐，驱逐前先清扫已过期项。`max_entries` 默认 100_000，`ttl_ms` 默认 60_000，负缓存与命中同 TTL（`negative_ttl_ms` 可单独调小）。**替代方案**（引入 moka）被否：依赖面与其 async 特性（需要 tokio runtime 把手）对 v1 过重；spec 只钉语义（TTL 内命中、容量上界、未命中缓存）不钉算法，后续可无痛换实现。

### D4 — HTTP 后端：URL 模板 + `columns` 类型化映射

- `url` 含 `{{key}}` 占位符，每个 distinct key 一次 GET（headers/method 可配）。
- `columns: [{name, path, type}]`：`path` 为响应 JSON 的 dot-path；`type` ∈ `utf8|int64|float64|bool`（默认 `utf8`），取不到 path 或类型不匹配 → 该格 null。列类型在构建期即固定（不随首行推断），保证流式 schema 稳定。
- target 列与输入批既有列（含 `__meta_`）重名 → 构建期报错。
- 非法 key 字符（路径注入面，如 `/`、`?`、`#`、空白与控制符）→ 构建期可选 `encode_key: true`（默认 true，percent-encode）防 URL 结构被 key 值改写。

**替代方案**（整对象扁平化为 `l_*` 前缀列，如内核 join 的 `l_*`/`r_*` 风格）被否：维表字段集合随响应漂移，违反流式 schema 稳定性；显式 columns 契约更诚实。

### D5 — 失败/未命中语义

- 后端请求失败（传输错误/超时/非 2xx）：默认整批 `Err`（fail-closed，链路错误路径既有行为）；可配 `on_backend_error: null` 降级为该键未命中。
- 单键未命中：默认该行新列全 null；可配 `on_missing: discard` 丢行（处理性丢行在 at-least-once 契约下合法，vrl 先例；spec 钉明输出行数 ≤ 输入行数）。
- 不做部分失败重试（v1 单次尝试 + 超时）；重试语义留给后端实现自行演进。

### D6 — 并发与对齐

批内 distinct keys 经 `stream::iter(...).map(...).buffered(concurrency)`（默认 8）保序收集；解析完成后按「key → 行索引列表」映射写回各列 builder。每批请求数 = distinct key 数，重复 key 不重复请求。

### D7 — 注册与文档义务照清单走

`register_processor_builder("lookup", ...)` + `ComponentMetadata::with_schema`（JSON Schema 与文档页、示例一致）；`docs/docs/components/2-processors/lookup.md` + zh-Hans、example YAML + manifest 登记、双语 README 清单——全部是 `pnpm docs:check` 与 workspace 测试已机械化的 CI 义务。secret 引用（headers 值、URL 中的 `${env:...}`）由配置加载层解析，组件内不处理（与 llm 同）。

## Risks / Trade-offs

- [缓存冷启动时每 distinct key 一次串发请求，首批延迟高] → 有界并发 + 批内去重；文档给出 TTL/容量调参建议；不做异步预热（Non-goal）。
- [手写缓存实现的正确性风险] → 语义（TTL 命中、上界、负缓存、并发下无 panic）由针对性单测钉住；算法可替换。
- [维表响应体过大导致内存压力] → 缓存按条目数计上界；`columns` 显式映射天然只保留所需字段（响应整体不入缓存——仅映射后的行对象入缓存）。**修正**：缓存值为「映射后各列的 JSON 片段」而非原始响应，`columns` 在解析期一次应用。
- [`on_missing: discard` 丢行与下游 ack 语义混淆] → spec 明确：丢弃是处理决策而非失败，ack 随批正常推进。
- [HTTP 后端限流]（维表服务 429）→ v1 视为后端错误走 D5 语义；不做退避重试（Non-goal 文档声明）。

## Migration Plan

纯新增组件：无配置迁移、无数据迁移。回滚 = 从 `processor/mod.rs` 的 `init()` 移除一行注册（或升级前版本二进制）。

## Open Questions

无阻塞性开放问题。JDBC/Redis 后端、批量端点、CDC 缓存失效按 Non-goals 顺序留给后续 change。
