## Why

对 #1247（feat: AI backends, observability, benchmarks & security）的代码审查发现 7 个 P1 级缺陷，集中在关停语义、错误吞噬和安全边界三块：

1. **WAL 停靠 ack 在 close 后无限等待**：`crates/arkflow-core/src/wal/mod.rs:562-570` 从 parked-ack 的 `select!` 中删除了 `close.cancelled()` 分支，只剩 `notified.await`。在途投递的 ack future 被 drop（链路取消）时无人 `notify_waiters`，停靠的 ack 永久挂起；`WalAck` 存在于 detached spawn 任务中，泄漏 `Arc<Wal>` 并锁住 redb 文件，导致同路径重启失败。注释声称的 "drain window" 从未实现。
2. **pump 退出无条件 `abort_all` 丢弃迟到回执**：`crates/arkflow-core/src/executor/remote.rs:2494-2497` 在包括干净关闭在内的所有退出路径上清空 pending map，使回执读循环专门设计的迟到回执窗口（`remote.rs:1868-1875`）成为死代码——每次优雅排水都把已投递分支 abort → WAL undo → 重启后重复回放。
3. **Hub 秘密预解析失败使 rollout 永久卡死**：`crates/arkflow-server/src/hub.rs:2514-2527` 中 `resolve_candidate_payload` 失败时直接返回 `Err`，绕过 intent/attempt 失败状态机；outbox 30s 租约到期后无限重领重失败，无重试上限、无 failure_class，rollout 永远停在 "applying"。
4. **组件注册错误被静默吞掉**：`crates/arkflow-plugin/src/{processor,output,input,buffer,codec}/mod.rs` 的 `INIT.get_or_init(|| { let _ = register_components(); }); Ok(())` 丢弃注册错误且 `OnceLock` 使残缺注册表永久化，`lib.rs` 的错误传播链失效。
5. **未认证 `/configuration/validate` 执行秘密解析**：`crates/arkflow-server/src/lib.rs:3294` 无 `authorized()` 检查（同路由的 apply/draft 均有），而 `ConfigCandidate::parse` 已开始解析 `${env:}`/`${file:}`——控制面绑非回环地址时向未认证调用者暴露环境变量/文件存在性探测与全量组件构造（CPU DoS）。
6. **解析错误泄露已解析秘密值**：`crates/arkflow-core/src/config.rs:274` 的 `serde_json` 类型错误将违规值内嵌进消息（如 `invalid type: string "<resolved-secret>", expected u64`），违背 `secret-references` 规格的错误不泄露保证。
7. **pgvector 输出并发 close 竞态 panic**：`crates/arkflow-plugin/src/output/pgvector.rs:141-158` 在两次加锁之间 `expect("pool checked above")`，与 `close()` 竞态时 panic 而非返回 `Error::Connection`。

这些问题影响生产可靠性（挂死、句柄泄漏、重复回放、静默残缺注册表）与安全边界（未认证探测、秘密泄露），应在合入后尽快修复。

## What Changes

- 恢复 WAL parked-ack 在 close 请求后的有界等待（实现文档中承诺的 drain window），超时后走 pending-error 路径返回可重试错误，杜绝无限挂起与 `Arc<Wal>` 泄漏。
- pump 仅在错误/强制关闭路径上 `abort_all`；干净的通道关闭路径交由回执读循环拥有最终 abort，保留迟到回执窗口、避免优雅排水触发重复回放。
- Hub 派发时秘密预解析失败 SHALL 路由到既有 intent/rollout 失败状态机（标记目标失败并记录错误），不再无限重试。
- 各组件 kind 的 `init()` 将注册结果存入 `OnceLock` 并向调用方传播错误；`lib.rs` 的错误链恢复生效。
- `/configuration/validate` 端点要求认证，且验证路径不做 `env:`/`file:` 引用的实际解析（仅语法校验），消除未认证探测面。
- `config.rs` 解析错误复用值无关的固定消息（与 `parse_with_secret_references` 一致），确保已解析秘密值不出现在任何错误输出中。
- pgvector 输出以单次持锁完成连接检查与查询，或以 `ok_or_else` 替代 `expect`，竞态时返回 `Error::Connection`。

## Capabilities

### New Capabilities

（无——全部为既有能力的缺陷修复。）

### Modified Capabilities

- `network-shuffle-data-plane`: pump 取消与 void-write 语义 REQUIREMENT 变更：上游通道关闭的干净退出路径 SHALL NOT abort 已注册分支，迟到回执窗口由同连接的回执读循环拥有（wire 失败与 shutdown 路径的 abort 语义不变）。
- `configuration-management`: Configuration validation REQUIREMENT 变更：验证端点 SHALL 与应用端点同等要求操作者授权。
- `component-registry-export`: 新增 REQUIREMENT：各组件 kind 的 `init()` SHALL 传播注册错误，一次失败的注册 SHALL NOT 被静默丢弃或永久化为残缺注册表。

其中四个修复（WAL drain 窗口、Hub 预解析失败进入失败状态机、解析错误不泄露秘密明文、pgvector 并发 close 不 panic）为**实现合规修复**——`input-durability`（优雅关闭时的 parked 确认 drain，30s 窗口）、`secret-references`（Hub 分发预解析的 target-failed 场景、错误不泄漏保证）、`pgvector-output`（输入校验与错误语义）的既有 REQUIREMENT 已写明目标行为，本次仅使实现合规，不改动这些规格。

## Impact

- 代码：`crates/arkflow-core/src/wal/mod.rs`、`crates/arkflow-core/src/executor/remote.rs`、`crates/arkflow-server/src/hub.rs`、`crates/arkflow-server/src/lib.rs`、`crates/arkflow-core/src/config.rs`、`crates/arkflow-plugin/src/{processor,output,input,buffer,codec}/mod.rs`、`crates/arkflow-plugin/src/output/pgvector.rs` 及对应测试。
- 无 API/依赖/配置格式变更；全部为缺陷修复，无破坏性变更。
- 涉及持久化与 ack 语义的修复需回归 `input-durability`、`checkpoint-recovery`、`distributed-job-runtime` 相关测试。

## Non-goals

- 不修复 CR 中的 P2/P3 问题（OIDC PKCE/cookie 加固、OTel filter、milvus 重试、向量工具去重等）——留待后续独立变更。
- 不改变 at-least-once 语义、WAL 存储格式或回执协议线格式。
- 不重构 Hub 状态机，仅将预解析失败接入既有失败路径。
- 不新增任何用户可见配置项。
