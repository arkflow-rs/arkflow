## Why

`S3Store::write_manifest` 执行裸 `client.put(...)`，没有任何 `If-Match` / ETag 协调（`crates/arkflow-plugin/src/wal/s3.rs:876-889`）。从 PR #1186 起，parallel PUT worker 数为 1–8，每条 PUT 完成后都会回到 `on_complete` 回调并触发 `seal_active_segment → write_manifest`。当多个 worker 同时完成时，多个 task 会并发修改 manifest：晚到者覆盖先到者的 cursor / sealed_segments 写入，**早到者的变更永久丢失**。

后果：已 ack 的 cursor 比实际落后 N 条；recovery 时把那些视为"未 ack"重新 replay，下游出现**重复消费**。多实例同 bucket 部署会必坏（每个实例的 cursor 互相覆盖）。这是一个回归 bug——非新引入的功能性问题，#1183 引入 S3 backend 时按单 PUT worker 设计（隐性单写者），#1186 改成多 worker 后打开了 race 窗口，#1191（comprehensive WAL optimization）也没修。

立即修这个 race，是为了让后续 B1（retention）/ B2（tiered storage prefix 分层）在安全的 manifest 写路径上推进，而不至于在一个会丢 cursor 的底层上继续叠加复杂度。

## What Changes

- **改 `S3Store::write_manifest`**，改为 ETag 协调的闭包写入器 `write_manifest_with_etag`：读 base + ETag，运行 caller 的 mutator，再用 `PutMode` 提交（manifest 已存在用 `Update { e_tag }`、首次创建用 `Create`/if-none-exists）；条件失败（`Precondition` = ETag 不匹配，或 `AlreadyExists` = 首次写入竞争落败）时重新读 manifest + 叠加本次改动 + 再 PUT；最多重试 8 次（覆盖 `parallel_put.workers` ≤ 8 的全并发最坏情况）。
- **新增 `read_manifest_with_etag` 帮助函数**返回 `(Manifest, ETag)`，供 retry 路径使用。
- **新增并发测试**（落在 `crates/arkflow-plugin/src/wal/s3.rs` 内部 `#[cfg(test)] mod tests`，因为协调写入器是 `pub(crate)`、集成测试无法访问，而 Non-goal 禁止暴露新的公共 API）：用 `tokio::spawn` 并发触发 N 个 `write_manifest_with_etag`，断言最终 manifest 的 cursor 等于"所有调用者中最大的序列号"（早写入没丢）。
- **recover 路径沿用既有 D5 / D7 兜底**，不动。如果 race 写入导致 manifest 与 segments 名单短暂不一致（retry 期间），重启时 LIST 兜底仍能恢复。

非破坏性变更：仅在 `object_store::ObjectStore::put_opts` 已经支持的范围内使用 `If-Match` 语义；旧 manifest 形状、cursor 推进语义、recovery 行为均不变。

## Capabilities

### New Capabilities

- `wal-manifest-write-coordination`: 描述 S3 WAL manifest 的并发写入协调（If-Match + ETag + retry）能力。

### Modified Capabilities

- `s3-wal-pipeline`: 新增 Requirement "Manifest PUT is coördinated with an ETag precondition" 及其 Scenario。该 capability 现行的 `Failure handling and recovery` 章节只覆盖了 PUT 失败与 recovery 一致性，新增 Scenario 覆盖 "concurrent seal race" 这一被忽视的故障模式。

## Impact

**Affected code:**

- `crates/arkflow-plugin/src/wal/s3.rs` — `write_manifest_with_etag` 协调写入器、`read_manifest_with_etag`、`seal_active_segment` / `flush_manifest` 迁移到闭包形式、删除迁移产生的孤儿 `read_manifest_or_fresh`、并发回归测试（T1-T4，InMemory 后端，落在内部 `#[cfg(test)] mod tests`）。
- `openspec/specs/s3-wal-pipeline/spec.md` — 增加一条 Requirement 与两个 Scenario（delta 形式）。

**API changes:**

- 内部函数签名变化：`write_manifest(store: &S3Store, m: &Manifest)` 内部行为升级，签名保持兼容。
- 不暴露新的公共 API；WalConfig 字段无变化。

**Performance impact:**

- 单次写路径：读 manifest 时顺带拿到 ETag（`read_manifest_with_etag` 复用现有 GET，无额外 RTT）。失败 retry 时每次增加一次 GET + 一次 PUT，retry 期间临界区不持锁（task 间靠 object_store client 自身并发，无须新 mutex）。
- 跨实例部署：若原 race 触发频度高，retry 可显著降低 actual PUT 次数（每次 retry 是一次完整 round-trip，但避免了 silently-lost writes）。

**Non-goals:**

- 不引入进程级 mutex 或 manifest_lock（`If-Match` 已经足够，且不阻塞并行 PUT 的吞吐）。
- 不改 segment 对象 PUT 路径（segment 文件名不可变、并发 PUT 各写不同对象，本身无 race）。
- 不动 `read_after_cursor` 和 recovery 的 LIST 兜底（D5 / D7）。
- 不改 manifest 的数据布局 / JSON schema。
- 不引入 TTL、retention、tiered storage、cache（这些是后续 B1 / B2 / path A 的范畴，本 change 严格只修 race）。
- 不为并行 PUT 之外的并发源（如多 instance 共享 bucket）做额外协调——若未来要支持，需要更深的事务机制（manifest version vector），scope 大于本 fix。
