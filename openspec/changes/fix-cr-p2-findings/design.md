## Context

P1 变更（`fix-cr-p1-regressions`，PR #1251）修复了关停语义与安全边界的 7 个 P1；本变更处理代码审查发现的 18 项 P2。关键现状（file:line 以 `fix-cr-p1-regressions` 分支为准）：

- `secret.rs:141-170` `resolve_secret_only_string` 逐 token 扫描，`secret:` 替换值原样拼入输出；节点端 `parse_engine_config` 再扫描全文 → 值内 `${...}` 被二次展开，违反既有「解析结果不重扫」REQUIREMENT。
- `resolve_candidate_payload`（secret.rs:79-118）返回的 envelope 只有 `content`（已解析）与 `format: json`；`agent.rs:2294` 将整个 payload 反序列化为 `ConfigCandidate` 后交 `ControlPlane::apply_configuration`（control_plane.rs:393-410），后者无条件 `version_store.save_with_parent(candidate)` → 已解析明文落盘 `.arkflow/config-history/`。
- `configuration.rs:102-112` `load` 用 `self.root.join(format!("{id}.json"))`，id 来自 `configuration_diff`/`rollback_configuration` 的查询参数，未过滤 `..`/分隔符。
- `configuration.rs:288-318` `redact_secrets` 仅按键名匹配，pgvector `url: "postgres://user:pass@host/db"` 原样返回。
- `oidc.rs:122-138` `decoding_key`：已知 kid 未命中时在持有 `cache.lock().await` 的情况下 `fetch_keys().await`（5s 超时），所有并发认证排队。
- `oidc.rs:309-317` `authorization_redirect` 无 `code_challenge`/`nonce`；`exchange_code`（320-346）无 `code_verifier`。`lib.rs:2836-2839` login 用 `expect("OS randomness")`；`oidc.rs:353` `create_session` 同。`lib.rs:2845/2897` cookie `Max-Age=28800` 硬编码（`SESSION_TTL` = 8h，oidc.rs:195），无 `Secure`。
- `cli/mod.rs:372-390` OTel layer 无 `with_filter`；`build_otel_layer`（394-432）中 `global::set_tracer_provider(provider)` 后无任何 shutdown 挂钩。
- 六个向量文件（processor/{vector_search,milvus_search,pgvector_search}、output/{milvus,pgvector,qdrant}）存在成串复制：`extract_vectors` ×6（~60 行）、`extract_ids` ×3、`truncate_body` ×6、loopback 客户端构造 ×5、逐字节 mock 服务器 ×5（~90 行）；`extract_vectors` 对 `FixedSizeList`/`List` 内层元素不做 `is_null` 校验。
- `milvus_search.rs:179-185` 逐行 `search(vector).await` 串行；兄弟文件用 `buffered(concurrency)`；模块文档（17-21 行）宣称批量 v2 请求。
- llm/vector_search/pgvector_search 的 `buffered(n).collect::<Vec<Result<_>>>()` 不短路；`futures::TryStreamExt::try_collect` 可短路并丢弃在途 future。
- `output/milvus.rs:148-201` 单次 `send().await` 无重试；qdrant.rs（同提交）有 `retry_count` + 100ms×2ⁿ 退避可移植。
- `output/pgvector.rs:199-223`（P1 后为 build_insert）多行单语句绑定：每行 1–3 个参数，>~21.8k 行触发 Postgres 65535 上限整批失败。
- `output/milvus.rs:125-139`/`qdrant.rs:122-145` 整批序列化进单个请求体（Milvus/Qdrant REST 有请求体上限），payload 经 JSON 文本往返 3-4 次驻留内存。
- `input/kafka.rs:473-484` header 值 `from_utf8(v).ok().unwrap_or("")` 静默丢数据；`insert` 语义使重复 key 相互覆盖。
- `mqtt_tls.rs:70-74/107/120` 在 async `connect()` 内 `std::fs::read` PEM。

## Goals / Non-Goals

**Goals:**

- 18 项 P2 全部落地；秘密不泄漏/不重扫/不落盘明文三类保证闭环。
- 向量插件去重后行为与现状逐字节一致（除明确列出的修复点），由既有测试矩阵（含 `#[ignore]` 真库集成测试）守护。
- 全部修复带回归测试；`cargo test --workspace` 与 clippy 基线不回退。

**Non-Goals:**

- P3 项、`secret.rs` 同步读改异步（见 proposal Non-goals）。
- 不重写 Hub/agent 协议：`content_verbatim` 为可选字段的最小扩展。

## Decisions

**D1 — Hub 派发值转义：在替换点转义，不碰节点解析器（A1，合规修复）**
`resolve_secret_only_string` 中 `resolve_env(...)` 的返回值在拼入 `output` 前 `replace("${", "$${")`。节点解析器把 `$${` 还原为字面 `${`，最终内存值与秘密原文一致；`env:`/`file:` 未替换 token 原样保留可解析性。备选（payload 打标记 instruct 节点跳过重扫）改线协议且破坏「单一解析点」模型，弃用。

**D2 — 节点落盘 verbatim：`ConfigCandidate.content_verbatim` 可选字段（A2）**
`ConfigCandidate` 增加 `#[serde(default, skip_serializing_if = "Option::is_none")] content_verbatim: Option<String>`。`resolve_candidate_payload` 把原 content 放入该字段（序列化自动带出）；agent 反序列化自动继承；`ControlPlane::apply_configuration` 保存版本时 `StoredConfigVersion.content = verbatim.unwrap_or(content)`，validate/apply 仍用已解析 content。回滚含 `${secret:}` 的版本时节点端解析报「未设置引用」错误（指明路径），语义正确：hub 派发配置的回滚应走控制面重新解析。增量字段向后兼容（旧 hub/新 node、新 hub/旧 node 均不炸）。

**D3 — 版本 id 校验：白名单字符集（A3）**
`ConfigVersionStore::load`/`rollback_configuration` 入口校验：非空、≤128 字符、仅 `[A-Za-z0-9._-]`、不含 `..`（生成 id 为 `<ms>-<seq>` 天然满足）。`save` 的 id 为生成值不需校验，但 `load` 统一守卫。

**D4 — URL userinfo 脱敏：只掩密码段（A4）**
`redact_secrets` 对 String 值扫描 `://`：其后到下一个 `/`（或串尾）之间若含 `:`，将该段 `user:pass` 的 pass 替换为 `******`（保留 scheme/user/host 可用性）；无 userinfo 的 URL 不动。嵌套结构递归不变。

**D5 — PKCE+nonce：state cookie 承载三元组（B1）**
login 生成 `state`/`verifier`(64B hex)/`nonce`(16B hex)；cookie 值 `state.verifier.nonce`（'.' 分隔，cookie 值合法）；授权 URL 追加 `code_challenge=S256(verifier)&code_challenge_method=S256&nonce`。callback：ct_eq 校验 query state 与 cookie 第一段 → `exchange_code(code, verifier)`（表单加 `code_verifier`）→ 新增 `federation.verify_nonce(&id_token, &nonce)`（base64 解 payload 比对 nonce claim；签名/iss/aud 仍由 `authenticate` 全量验证，nonce 预检只做早停）→ 建会话。IdP 不支持 PKCE 时（未知参数必须忽略，RFC 6749 §3.1.2.2）行为不变、callback 因 id_token 无 nonce claim 而失败——在文档标注要求 IdP 支持。

**D6 — cookie 属性：条件 Secure + TTL 派生（B2）**
`fn session_cookie(value, max_age) -> String` 统一构造；`Secure` 仅当 `login.redirect_uri` 以 `https://` 开头（http 本地开发不受影响）；`Max-Age` 取 `SESSION_TTL.as_secs()`；state cookie `Max-Age=600` 保持常量、logout 清除串不变。

**D7 — JWKS 刷新去锁：双检 + AtomicBool 去重（B3）**
`decoding_key`：锁内查命中即时返回（无 await）→ 锁内查节流期返回 None → `refreshing.compare_exchange` 胜者释放锁后 fetch → 锁内写回。败者直接返回 None（401，客户端重试），消除合法认证排队；节流窗口防打点。

**D8 — RNG 失败不 panic（B4）**
`hub_oidc_login` 的 `expect` 改 `match` → 503 `problem("oidc_random_unavailable")`；`create_session` 返回 `Option<String>`，callback None → 500。oidc.rs:245 `eprintln!` → `tracing::warn!`。

**D9 — OTel 过滤与关停（C1/C2）**
`level_filter` Clone 一份给 otel layer `.with_filter(...)`。`build_otel_layer` 将 provider clone 存入 `cli/mod.rs` 的 `static OTEL_PROVIDER: OnceLock<SdkTracerProvider>`，新增 `pub fn shutdown_otel_tracing()`（`shutdown()` 消费；opentelemetry_sdk 0.28 的 `SdkTracerProvider: Clone`，实现时核对 `set_tracer_provider` 签名）。调用点：`crates/arkflow/src/main.rs` 的优雅关停路径（engine stop 后）；server bin 如复用同一 init 一并挂钩。备选（Drop guard/panic hook）覆盖不了正常退出路径，弃用。

**D10 — `vector_util` 共享模块（D6+D5，去重先行）**
新建 `crates/arkflow-plugin/src/vector_util.rs`：`extract_vectors`（含内层 `is_null` 校验：报 `Error::Process` 含列名/行号/槽位）、`extract_ids`、`truncate_body`、`build_http_client(timeout_ms)`（loopback 判定→no_proxy）、`escape_identifier`、`#[cfg(test)] pub mod test_support`（共享 MockApi）。六个文件逐个迁移并删除本地副本；`milvus`/`qdrant` 的 payload JSON 文本往返（`extract_payloads`）在去重 PR 内保持行为不变（不顺手优化），仅收敛签名。顺序：先去重，再在共享层落地 D2/D5 修复，避免 6 处重复改。

**D11 — 批处理语义统一（D1/D2/D3/D4/D8）**
- fail-fast：四个处理器 `buffered(n)` 后 `try_collect`（`futures::TryStreamExt`），短路并 drop 在途。
- milvus_search：与兄弟一致 `buffered(concurrency)`（concurrency 配置项缺失则加默认 4，对齐 vector_search）；模块文档改述实际行为。
- milvus 输出：移植 qdrant 的 `retry_count` + 100ms×2ⁿ 退避循环（可重试判定：连接错误与 5xx）。
- pgvector 分块：`binds_per_row = id?1:0 + 1 + payload?1:0`；`rows_per_chunk = max(1, 65000 / binds_per_row)`，顺序执行、错误即停（at-least-once 输出重投递，upsert 模式幂等）。
- milvus/qdrant 请求分块：行数 > 1000 时按 1000 行/请求切片，逐片走各自重试循环；≤1000 行保持单请求（`milvus-output` 既有「批量多行单请求」场景对 3 行仍成立）。

**D12 — 杂项（E1/E2）**
Kafka header：`String::from_utf8_lossy(v).to_string()`；重复键第 n 次（n≥2）写 `header_<key>_<n>`。mqtt_tls：三处 `std::fs::read` 包 `tokio::task::spawn_blocking`（连接建立本身是低频路径，不加缓存）。

## Risks / Trade-offs

- [D2 节点回滚 hub 派发版本会因 `${secret:}` 未解析而报错] → 错误消息指明引用路径；文档标注"hub 派发配置的回滚走控制面"。对照现状（明文落盘）为有意交换。
- [D5 要求 IdP 支持 PKCE S256 与 nonce] → RFC 强制忽略未知参数不破坏旧 IdP 的 code 换取，但缺 nonce claim 的 id_token 会被拒；docs 的 OIDC 页面标注前置条件。
- [D7 刷新竞争败者返回 None] → 竞争窗口内该请求 401，客户端重试即命中新缓存；节流窗口（既有）限制重试频率。
- [D9 `set_tracer_provider` 签名随版本变化] → 实现时以 0.28 实际 API 为准（clone before set / guard），shutdown 失败仅 warn 不影响退出码。
- [D10 大面积去重可能夹带行为漂移] → 迁移为纯移动 + 测试矩阵回归（单测 + `#[ignore]` 集成测试），diff 审查按"先纯移动、后修复"两段提交。
- [D11 分块/重试放大对后端的请求量] → 1000 行/请求 × 既有 timeout 与 retry 上限有界；at-least-once 语义不变。

## Migration Plan

纯加固/修复，无数据迁移；`content_verbatim` 与 PKCE 均为增量兼容。部署顺序无约束（新旧混布兼容）。回滚 = revert。实现顺序：D10 去重 → D 组其余 → A 组 → B 组 → C 组 → E 组，每组独立提交。

## Open Questions

（无——opentelemetry 0.28 API 细节在实现时以 cargo doc 为准，不构成设计分歧。）
