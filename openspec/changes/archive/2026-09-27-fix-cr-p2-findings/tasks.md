## 1. 向量插件去重与批处理语义（arkflow-plugin，D 组）

- [x] 1.1 新建 `vector_util.rs`：迁移 `extract_vectors`（增加内层 `is_null` 校验，报错含列名/行号/槽位）、`extract_ids`、`truncate_body`、`build_http_client`（loopback→no_proxy）、`escape_identifier`、共享 `#[cfg(test)]` MockApi；六个文件逐个改为引用共享实现并删除本地副本（纯移动优先，独立提交段）
- [x] 1.2 四个处理器（llm/vector_search/milvus_search/pgvector_search）`buffered` 循环改 `try_collect` 短路 + 回归测试（首错不再发起剩余请求）
- [x] 1.3 `milvus_search` 改 `buffered(concurrency)` 并发、修正模块文档失实（宣称批量、实为逐行）
- [x] 1.4 `output/milvus.rs` 移植 qdrant 的 `retry_count` + 100ms×2ⁿ 退避（连接错误与 5xx 可重试）+ 回归测试（mock 503 两次后成功）
- [x] 1.5 `output/pgvector.rs` INSERT 按绑定上限分块（`rows_per_chunk = 65000 / binds_per_row`）+ SQL 文本断言测试
- [x] 1.6 `output/milvus.rs`/`qdrant.rs` 整批请求体按 1000 行切片（≤1000 行单请求不变）+ 分片断言测试
- [x] 1.7 `pgvector` 输出与 `pgvector_search` SQL 标识符转义 `"` → `""` + 回归测试

## 2. 秘密卫生（arkflow-core / arkflow-server，A 组）

- [x] 2.1 `resolve_secret_only_string`：替换值中 `${` → `$${` 转义（合规「解析结果不重扫」）+ 回归测试（秘密值含 `${env:X}` 文本时节点解析为字面量）
- [x] 2.2 `ConfigCandidate.content_verbatim` 可选字段；`resolve_candidate_payload` 携带原 content；`ControlPlane::apply_configuration` 以 verbatim 落盘版本、以解析内容运行 + 回归测试（分发后版本存储不含明文）
- [x] 2.3 `ConfigVersionStore::load` 校验 id（空/长度/`[A-Za-z0-9._-]`/`..`）+ 路径穿越回归测试
- [x] 2.4 `redact_secrets` 覆盖 URL userinfo 密码段（`scheme://user:pass@` → pass 掩码，无 userinfo 不动）+ 回归测试
- [x] 2.5 `oidc.rs` `eprintln!` → `tracing::warn!`

## 3. OIDC 会话加固（arkflow-server，B 组）

- [x] 3.1 PKCE S256 + nonce：login 生成三元组写 state cookie（`state.verifier.nonce`），授权 URL 加 `code_challenge`/`code_challenge_method`/`nonce`；callback 以 verifier 换码、校验 nonce claim 后建会话 + 回归测试（重定向参数断言、nonce 不匹配 401）
- [x] 3.2 cookie 属性统一构造：`Secure`（https redirect_uri 时）+ `Max-Age` 从 `SESSION_TTL` 派生，删除 28800 硬编码 + 回归测试
- [x] 3.3 JWKS 刷新移出缓存锁（双检 + AtomicBool 去重），缓存命中锁内即时返回 + 回归测试
- [x] 3.4 RNG 失败不 panic：login 返回 503、`create_session` 返回 Option、callback 返回 500

## 4. 可观测性（arkflow-core，C 组）

- [x] 4.1 OTel 导出层 `.with_filter(level_filter)`（Clone 过滤器）
- [x] 4.2 provider 存入 `OnceLock` 并新增 `shutdown_otel_tracing()`；挂接 `crates/arkflow/src/main.rs`（及 server bin 如适用）优雅关停路径

## 5. 杂项（E 组）

- [x] 5.1 Kafka header：`from_utf8_lossy` 替代静默置空 + 重复键 `header_<key>_<n>` 后缀 + 回归测试
- [x] 5.2 `mqtt_tls` 三处 PEM 读取包 `spawn_blocking`

## 6. 验证与收尾

- [x] 6.1 `cargo test --workspace --all-targets` 通过
- [x] 6.2 `cargo clippy --workspace --all-targets` 无新增告警（与 P1 分支基线对比）
- [x] 6.3 `openspec validate fix-cr-p2-findings` 通过；docs OIDC 页面补 PKCE/nonce 前置条件说明
