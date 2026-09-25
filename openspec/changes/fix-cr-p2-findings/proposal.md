## Why

对 #1247 的代码审查在 P1 之外还发现 18 项 P2 问题，分属四块：秘密卫生（Hub 解析值被节点二次扫描、节点明文落盘、版本存储路径穿越、URL 凭证漏脱敏）、OIDC 会话加固（无 PKCE/nonce、cookie 缺 `Secure` 且 TTL 双份硬编码、JWKS 刷新持锁拖慢合法认证、RNG 失败 panic）、可观测性成本（OTel 层无级别过滤、provider 从不 shutdown 丢失尾部 span）、AI/向量插件健壮性（约 700 行跨 6 文件的复制粘贴、milvus_search 串行逐行请求且文档失实、无 fail-fast、milvus 输出无重试、pgvector 超 65535 绑定参数上限即整批失败、SQL 标识符未转义、内层 null 未校验、整批单请求体）以及两处杂项（Kafka header 二进制值被静默置空且重复键相互覆盖、mqtt_tls 在 async 路径同步读 PEM）。

P1 变更（`fix-cr-p1-regressions`）已修复关停语义与安全边界的主要缺陷；本变更处理剩余 P2，消除秘密泄漏面、会话加固缺口与批量数据路径的健壮性缺口。

## What Changes

**秘密卫生**
- Hub 派发路径：`resolve_secret_only_string` 替换出的值中 `${` SHALL 转义为 `$${`，使节点解析器视为字面量——合规既有"解析结果不重扫"REQUIREMENT（实现修复，无 delta）。
- 节点落盘：`ConfigCandidate` 增加可选 `content_verbatim`；Hub 派发载荷携带原引用文本，节点版本存储持久化 verbatim 版本，已解析副本仅驻内存（新增 `secret-references` ADDED REQUIREMENT）。
- `ConfigVersionStore::load` 校验版本 id（拒绝空、路径分隔符与 `..`），消除路径穿越（新增 `configuration-management` ADDED REQUIREMENT）。
- 脱敏覆盖 URL userinfo：字符串值形如 `scheme://user:password@` 时密码段替换为 `******`（MODIFIED `Secret redaction`）。
- `oidc.rs:245` 的 `eprintln!` 改为 `tracing::warn!`。

**OIDC 会话加固**（`hub-oidc-auth` 两处 MODIFIED）
- 授权码流增加 PKCE（S256 `code_verifier`/`code_challenge`）与 `nonce`：state cookie 携带三元组，callback 校验 state 后以 verifier 换码、校验 id_token 的 nonce claim。
- 会话与 state cookie：redirect_uri 为 https 时附加 `Secure`；`Max-Age` 从 `SESSION_TTL` 派生，删除 28800 硬编码。
- JWKS 未知 kid 触发的刷新移出缓存锁：命中键在锁内即时返回；刷新以原子标志去重，合法认证不再排队在 5s HTTP 之后。
- RNG 失败不再 `expect` panic：login 返回 503、callback 返回 500。

**可观测性**（`data-plane-tracing` ADDED）
- OTel 导出层挂接与日志层相同的级别过滤，开启追踪不再导出全部依赖的 DEBUG/TRACE。
- `SdkTracerProvider` 在优雅关停路径 `shutdown()`，批量导出的尾部 span 不再静默丢失。

**插件健壮性**（`milvus-output`/`pgvector-output` 两处 MODIFIED；其余为实现层修复）
- 新增共享 `vector_util` 模块：收敛 6 份 `extract_vectors`、3 份 `extract_ids`、6 份 `truncate_body`、5 份 loopback 客户端构造、共享 `#[cfg(test)]` mock 服务器；`extract_vectors` 增加内层 null 校验（列名/行号/槽位）。
- 四个搜索/LLM 处理器的 `buffered` 循环改 `try_collect` 短路：首错不再跑完剩余全部行。
- `milvus_search` 改 `buffered(concurrency)` 并发并修正模块文档（宣称批量请求、实为逐行）。
- `milvus` 输出补 `retry_count` + 指数退避（对齐 qdrant/http 惯例）；milvus/qdrant 整批单请求体改为按行数分块（`milvus-output` MODIFIED：小批仍单请求）。
- pgvector INSERT 按绑定参数上限分块（MODIFIED `SQL 生成与 upsert 语义`）；pgvector 输出与 `pgvector_search` 的 SQL 标识符转义内嵌 `"`。
- `mqtt_tls` 的 PEM 读取经 `spawn_blocking`。

**杂项**
- Kafka header：`from_utf8_lossy` 替代静默置空；重复键追加序号后缀不再相互覆盖。

## Capabilities

### New Capabilities

（无。）

### Modified Capabilities

- `secret-references`: ADDED——节点侧分发配置的落盘语义（verbatim 持久化、已解析副本仅驻内存）。
- `configuration-management`: MODIFIED `Secret redaction`（URL userinfo 密码段脱敏）；ADDED 版本 id 校验（拒绝路径分隔符与 `..`）。
- `hub-oidc-auth`: MODIFIED `浏览器授权码登录流`（PKCE S256 + nonce、Secure cookie、Max-Age 派生、RNG 失败不 panic）；MODIFIED `令牌验证与失败语义`（JWKS 刷新不得在缓存锁内进行，命中键不受影响）。
- `data-plane-tracing`: ADDED——OTel 导出层级别过滤与关停冲刷。
- `milvus-output`: MODIFIED `milvus output 的配置与请求语义`（`retry_count` + 退避；大批量按行分块，小批仍单请求）。
- `pgvector-output`: MODIFIED `SQL 生成与 upsert 语义`（按绑定参数上限分块；标识符转义）。

实现合规/实现层修复（无 delta，设计与任务中标注）：Hub 派发值转义（`解析结果不重扫` 已禁止重扫）、milvus_search 并发与文档、fail-fast、内层 null、vector_util 去重、qdrant 分块（无 qdrant 规格）、Kafka header、mqtt_tls、eprintln。

## Impact

- 代码：`crates/arkflow-core/src/{secret.rs,configuration.rs,control_plane.rs,cli/mod.rs}`、`crates/arkflow-server/src/{oidc.rs,lib.rs,hub.rs,agent.rs}`、`crates/arkflow-plugin/src/{vector_util.rs(新),processor/{llm,vector_search,milvus_search,pgvector_search}.rs,output/{milvus,pgvector,qdrant}.rs,input/kafka.rs,mqtt_tls.rs}`、`crates/arkflow/src/main.rs`（shutdown 挂钩）及对应测试。
- 兼容性：`content_verbatim` 为可选字段（serde default + skip_serializing_if），线格式向后兼容；PKCE/nonce 对不支持 PKCE 的 IdP 属行为变更（RFC 6749 授权服务器必须忽略未知参数，S256 为业界默认）；cookie `Secure` 仅在 https redirect_uri 下附加，http 部署不受影响。
- P2 变更基于 P1 变更分支（`fix/cr-p1-regressions`），hub.rs/lib.rs/pgvector.rs 存在同区域交叠，PR 堆叠、按序合并。

## Non-goals

- 不修复 P3（集合名 URL 编码、qdrant 注释勘误、embedding 串行分块、benchmark 参数校验等）。
- 不将 `secret.rs` 的同步 `std::fs` 读取改为异步（同步 API 面贯穿配置层，改造非手术式；文件小、为可接受的权衡，见 design Risks）。
- 不改变 OIDC bearer 联邦的验证算法白名单与会话 TTL 语义。
- 不引入新的向量后端行为差异：所有六文件共享同一 `vector_util` 语义。
