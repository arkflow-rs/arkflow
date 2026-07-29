## Context

### Current state

`S3Store::write_manifest`（`crates/arkflow-plugin/src/wal/s3.rs:876`）直接 `client.put(...)`，不携带任何条件。当前的并发环境是：

- `parallel_put.workers` 在 PR #1186 之后支持 1–8 个并发 PUT worker。每个 worker 完成上传后会回到 `on_complete` 回调并调用 `seal_active_segment → write_manifest`（`crates/arkflow-plugin/src/wal/s3.rs:790-808`）。
- `flusher` 后台 task 周期性 `flush_manifest`（`crates/arkflow-plugin/src/wal/s3.rs:891-910`）。
- `advance_cursor` 也会触发 `flush_manifest`（`crates/arkflow-plugin/src/wal/s3.rs:653`）。

这三条写路径**没有任何显式的协调**。当两个 task 同时进入临界区时，晚到者的 PUT 覆盖先到者的 cursor / sealed_segments 写入，**丢失的不是数据、是 cursor 推进的记录**——结果下游 ack 已经到达的本应不再 replay 的那条又被错误回放。

### Constraints

- 必须保持 at-least-once 交付语义（**不能因为 retry 反而变成 at-most-once**）。
- 不能引入新 mutex / channel —— 当前并行 PUT 写入吞吐不能回退。
- 必须兼容现有 manifest.json 数据布局（`Manifest` 结构和 JSON 字段都不变）。
- 必须沿用现有 read 路径的语义：所有读取 manifest 的路径都习惯于 `NotFound → fresh manifest`；retry 路径的 `read_manifest_with_etag` 使用相同的获取方式以避免双实现。
- 必须用 `object_store::ObjectStore` 已有的能力完成（`PutOptions::if_match`、错误 `PreconditionFailed`），不引入额外 SDK。

### Stakeholders

- 跑高吞吐 stream（`parallel_put.workers > 1`）的运维：当前 race 触发 = 重复消费。
- 多实例同 bucket 部署的尝试者：现状**不能**多实例；本 change 修好后**单 bucket 单写者**场景安全，但多写者仍是开放问题。
- B1 / B2 后续 change 依赖本 PR 先落地——否则 retention compaction 在会丢 cursor 的 manifest 上推进。

## Goals / Non-Goals

**Goals:**

1. 修复 `parallel_put.workers > 1` 时多 task 并发写 manifest 出现的 last-write-wins 丢 cursor 问题。
2. 沿用现有 `object_store` SDK，不引入新依赖。
3. 写失败路径可观测（tracing 日志 retry 次数与冲突原因）。
4. 加并发回归测试，覆盖 N 个并发写者最终一致性。
5. 不改变 manifest 数据布局 / JSON schema。

**Non-Goals:**

- 不解决多实例共享 bucket 的并发（manifest 协调是单实例唯一写者层面的，多写者需要 transaction / version vector，超出本 PR scope）。
- 不改 segment 对象的 PUT 路径（segment 文件名不可变 + 并发写不同对象，本身无 race）。
- 不引入进程级 / 跨实例 manifest_lock。
- 不动 read_after_cursor / recover 兜底逻辑（D5 / D7 保留）。
- 不引入 TTL、retention、tiered storage、cache。

## Decisions

### Decision 1: If-Match + ETag retry 作为协调机制

**Choice:** 改 `write_manifest` 为：从 ETag 化的 `read_manifest_with_etag` 拿到 base + ETag，构造本次 PUT 的 `Manifest`，再以 `PutOptions { if_match: Some(etag) }` 提交。条件失败 412 时进入 retry 循环，每次 retry 重新拉 base + ETag，再叠加本次 caller 想做的改动，再 PUT。

**Rationale:**

- `object_store` 已支持 `If-Match` 语义（HTTP `If-Match` 头），无须新依赖。
- ETag 是 S3 标准能力，所有兼容 S3 协议的对象存储（MinIO、阿里云 OSS、腾讯 COS 等）都支持。
- retry 循环天然吸收"晚到者"——晚到者总是把自己的改动叠加在最新 base 上，不会覆盖更新过的值。
- 等价于 S3 自身的 optimistic concurrency control；本机单进程内不需要额外的 mutex。

**Alternatives considered:**

- 引入 `tokio::Mutex` 串行化所有 manifest 写入：代价是并行 PUT worker 的并发优势被旁路——这是倒退。#1186 的全部收益建立在这之上。
- 用 Redis / 外部锁服务协调：引入新依赖 + 跨进程基础设施，超出 bug fix 的合理 scope。
- 用 WAL 自身的 `segments` 作为事件溯源日志而不是 manifest：完全重构，超出 fix scope。
- 改 `manifest.json` 形态为 append-only 日志 + snapshot：实质上是 B1 / B2 中的 manifest v2 设计，scope 太大。

**Implementation sketch:**

```rust
// crates/arkflow-plugin/src/wal/s3.rs

const MANIFEST_WRITE_MAX_RETRIES: usize = 8; // covers parallel_put.workers (≤ 8) worst case

/// Read the manifest object together with its current ETag.
/// `NotFound` returns a fresh in-memory manifest with `None` ETag.
async fn read_manifest_with_etag(
    store: &S3Store,
) -> Result<(Manifest, Option<String>), Error> {
    match store.client.get(&ObjectPath::from(store.manifest_key.as_str())).await {
        Ok(r) => {
            let etag = r.meta().get("ETag").cloned();
            let bytes = r.bytes().await
                .map_err(|e| Error::Process(format!("S3 GET manifest body: {}", e)))?;
            let m = Manifest::from_json(&bytes)
                .map_err(|e| Error::Process(format!("manifest JSON: {}", e)))?;
            Ok((m, etag))
        }
        Err(object_store::Error::NotFound { .. }) => {
            Ok((Manifest::fresh(store_ns_node_id(store), store_ns_stream_id(store)), None))
        }
        Err(e) => Err(Error::Process(format!("S3 GET manifest: {}", e))),
    }
}

/// Apply a mutator function to the manifest under ETag-coordinated PUT.
/// Retries on `PreconditionFailed` (HTTP 412) up to MANIFEST_WRITE_MAX_RETRIES times.
/// The mutator receives a freshly-read manifest each attempt and writes it back;
/// this mirrors how callers currently think: "advance to N", "add segment X", etc.
async fn write_manifest_with_etag<F>(
    store: &S3Store,
    mut mutate: F,
) -> Result<(), Error>
where
    F: FnMut(&mut Manifest),
{
    for attempt in 0..MANIFEST_WRITE_MAX_RETRIES {
        let (mut m, etag) = read_manifest_with_etag(store).await?;
        mutate(&mut m);
        let bytes = m.to_json()
            .map_err(|e| Error::Process(format!("manifest serialize: {}", e)))?;
        // Existing manifest: condition on its ETag (Update). Fresh manifest:
        // Create (if-none-exists) so concurrent first-writers race and the
        // losers get `AlreadyExists` → retry, instead of all silently
        // clobbering each other with Overwrite.
        let mode = match etag {
            Some(e) => PutMode::Update(UpdateVersion { e_tag: Some(e), version: None }),
            None => PutMode::Create,
        };
        let opts = PutOptions { mode, ..Default::default() };
        match store
            .client
            .put_opts(
                &ObjectPath::from(store.manifest_key.as_str()),
                PutPayload::from(Bytes::from(bytes)),
                opts,
            )
            .await
        {
            Ok(_) => return Ok(()),
            // Precondition = ETag mismatch on Update; AlreadyExists = lost the
            // first-write race on Create. Both → re-read and retry.
            Err(object_store::Error::Precondition { .. }
            | object_store::Error::AlreadyExists { .. }) => {
                tracing::debug!(
                    attempt = attempt + 1,
                    "manifest ETag mismatch, re-reading and retrying"
                );
                continue;
            }
            Err(e) => {
                return Err(Error::Process(format!("S3 PUT manifest: {}", e)));
            }
        }
    }
    Err(Error::Process(format!(
        "manifest write failed after {} retries (concurrent writers contending?)",
        MANIFEST_WRITE_MAX_RETRIES
    )))
}
```

**Caller refactor:** 把 `flush_manifest` / `seal_active_segment` 内对 `write_manifest(...)` 的直接调用改为闭包形式：

```rust
// advance path
write_manifest_with_etag(store, |m| {
    if new_cursor > m.cursor { m.cursor = new_cursor; }
}).await?;

// seal path
write_manifest_with_etag(store, |m| {
    if let Some(prev_active) = m.active_segment.take() {
        m.sealed_segments.push(prev_active);
    }
    m.active_segment = Some(name);
    if last_seq > m.max_sealed_seq { m.max_sealed_seq = last_seq; }
}).await?;
```

这样的好处是 mutator 拿到的是 **freshly-read base**——caller 不必自己先读 manifest 再写（这是个本来就潜在短窗口 race 的反模式）；每次 mutator 都从最新 base 出发，单一原子窗口。

### Decision 2: retry 次数固定 8

**Choice:** 上限 8 次硬截断（对齐 `parallel_put.workers` 的配置上限），retry 期间**不**加 backoff sleep。

**Rationale:**

- 最坏情况是 `parallel_put.workers` 个 worker 全并发 seal：对象存储的 PUT 是原子的，完全并发的 N 个写者每轮只有一个能赢，因此 N 个写者最坏需要 N 轮、每个写者最多 N 次 attempt 才能全部收敛。`workers` 配置上限是 8，所以 8 次刚好覆盖最坏情况（不会 flaky：第 N 轮成功的写者正好用 N 次 attempt ≤ 8）。
- 退避 sleep 会拉长临界区，反而**增加** seal 路径的尾延迟（无收益）。
- 超过 8 次 = 真异常（凭据失效、网络分区、S3 配额耗尽、或跨实例共享 bucket），应当快速失败而不消耗 RTT。
- 重试日志（`attempt=N`）足够运维定位"为什么这次写入花了 N 个 RTT"。

**Alternatives considered:**

- 固定 5（初版选择）：低估了"全并发 worker"的最坏情况——8 个 worker 全并发 seal 时后 3 个写者会耗尽 budget 并丢写。已被并发回归测试 T1（`manifest_race_concurrent_cursor_keeps_max`）证伪。
- exponential backoff：会放大 S3 客户端在被掐脖子场景下的重试放大效应。
- retry 3 次：太短，高 contention 多写者场景不够。
- 不限次数：可能挂在 S3 偶发性问题里很久，且消耗配额。

### Decision 3: 测试策略——本地 mock S3 + 真实并发

**Choice:** 在 `crates/arkflow-plugin/src/wal/s3.rs` 的内部 `#[cfg(test)] mod tests` 中新增并发测试，用 `object_store::memory::InMemory` 后端（`object_store` crate 自带）跑真并发。集成测试文件无法访问 `pub(crate)` 的 `write_manifest_with_etag` / `S3Store` / `Manifest`，而 proposal 的 Non-goal 又禁止暴露新的公共 API，因此测试落在内部模块（任务 4.1 的原定文件路径 `tests/wal_manifest_race.rs` 据此调整）。

**Rationale:**

- `InMemory` 后端支持 ETag，这是 `object_store` crate 的内建能力，无须 mock 自己实现。
- `cargo test` 在 CI 里能直接跑，比开 minio 容器轻得多。
- 用 `tokio::join!` 同时触发 N 个 `write_manifest_with_etag` 调用，断言**最终** cursor = max(所有 caller 的期望值)，即不丢写。

**Test cases:**

1. **T1 — concurrent cursor advance**  
   `tokio::join!` 8 个 task，每个调 `write_manifest_with_etag(|m| m.cursor = X_i)`，X_i 各不相同。  
   断言：最终 GET 的 manifest 的 cursor = max(X_i)。

2. **T2 — concurrent seal stress**  
   8 个 task 并发 seal 不同 segment，断言 `sealed_segments` 包含全部 8 个，无重复无丢失。

3. **T3 — single-writer baseline**  
   串行 8 次写入，断言最终结果与单线程版本一致（保证 retry 不引入新 bug）。

4. **T4 — retry budget exceeded**  
   用一个测试 fixture 在 6 次写入都触发 PreconditionFailed，断言第 6 次后返回 `Error::Process(...)`，不挂死。

**Non-goal of tests:** 不测多实例共享 bucket（仍是开放问题，超出 PR scope）。

### Decision 4: tracing 日志策略

**Choice:** `debug!` 记录 retry attempt 计数；正常一次成功路径不打印新日志。**首次冲突**和**重试次数 ≥ 3** 升级到 `warn!`，以便运维发现真实 contention 频度。

**Rationale:**

- 默认 `info` 日志级别下，retry 不污染输出（生产环境 INFO 级别下啥都不会刷）。
- 但运维调查 race 触发率时只要切到 `debug` 就能看到；如果问题严重切到 `warn` 就能直接抓到。

## Risks / Trade-offs

- [Race 真的发生了但 caller 的 mutate 是 "set X = V" 这种幂等] → 多次 retry 收敛到同一结果，无副作用。  
- [Race 真的发生了但 mutate 是 "append segment_name to sealed_segments"] → 第二次 retry 拿到的是**已包含本次 segment_name 的 base**，caller 的 mutate 会再 push 一次造成重复条目。  
  - **Mitigation:** mutator 在 retry 之前先做 idempotency guard：`if !m.sealed_segments.contains(&name) { m.sealed_segments.push(name); }`。seal 路径加这条 guard；cursor advance 路径天然幂等（`max(c, X)`）。
- [ETag 在对象被 multipart overwrite 时与单 PUT 不同] → 对象 store 的 ETag 行为本 PR 信任 SDK；这是上游契约问题。  
  - **Mitigation:** 测试用 `InMemory` 验证 ETag 行为真实。如果 `object_store` 抽象出问题，会在测试中暴露。
- [重试循环使单次 PUT 路径看起来变慢] → 平均成本：单实例低 contention 场景下走一次成功；只有出现真实 race 才会进入 retry。**P99 写入延迟**会因为 race 吸收而改善（不再 silently lost → 下游重复消费）。  
  - **Mitigation:** 8 次上限兜底（覆盖 `parallel_put.workers` ≤ 8 的全并发最坏情况）；超限快速失败。
- [如果 retry 次数用尽，caller 拿不到错误语义是否会丢 cursor] → 错误向上传播 `Error::Process("manifest write failed after N retries")`，由 `append_batch` 的 caller 处理——和现状一样的失败语义。  
  - **Mitigation:** 没有引入新的"静默丢失"路径，最坏情况和现状一样 panic；平均情况显著更好。
- [跨实例部署仍未解决] → 本 PR 修了"单实例多 worker 并发"，**不**覆盖"多实例共享 bucket"。后者在 PR 描述里显式标为 Non-goal，等真实用户需求再处理。
- [后端不支持 conditional PUT（`PutMode::Update`），如 `object_store::local::LocalFileSystem`] → `put_opts` 返回 `Error::NotImplemented`。
  - **Mitigation:** `write_manifest_with_etag` 捕获 `NotImplemented`，本次 attempt 回退 `PutMode::Overwrite`（无条件写）后返回。协调是 best-effort：在支持的后端（S3、InMemory）engaged，在不支持的后端退化为单写者 `Overwrite`。后者只用于本地测试（`LocalFileSystem` 的持久化便于测 restart 场景），单写者下 `Overwrite` 语义正确；生产 S3 支持 `Update`，不会触发回退。

## Migration Plan

- **Backward compatible:** 数据布局不动；manifest.json 历史版本照常读取（`Manifest` schema 不变）；FAIL 行为与失败语义不变。
- **Deploy order:** 与任何 stream / WAL 后端变更加入同样的发布流程。本地 dev、CI 全测试通过 → PR review → merge。
- **Rollback:** 回滚 commit 即可——write 路径回到裸 PUT。如果在生产已经触发过 race，回滚只意味着"再次回到潜在丢 cursor 状态"，**不会**比回滚之前更糟。无 schema migration，无 durable format migration。
- **Observability:** 上线后建议 dashboard 上加一条 `manifest_etag_retry_total` 指标（从 `tracing` span extract；先不加，等真有用户报 contention 再说）。

## Open Questions

1. **~~是否同时把 `read_manifest_or_fresh` 改造为 `read_manifest_with_etag`？~~**
   **RESOLVED:** 实施时发现迁移后 `read_manifest_or_fresh` 不再有任何调用点（所有写路径都直接用 `read_manifest_with_etag`），成为孤儿，已删除。recovery 路径读取 manifest 用的是独立的内联 GET（不经过这两个 helper），不受影响。
2. **是否需要 backpressure 给 retry 期间消耗的额外 RTT？**
   retry 8 次最坏多 8 × GET RTT（即 ~800ms）。单实例内正常 contention 几乎不会出现；如果出现，意味着已经撞 `workers` 上限——这是健康信号，应该尽快失败而非拖慢。状态：**不加退避**。
3. **是否公开 retry 行为给 WalConfig？**  
   现在 `MANIFEST_WRITE_MAX_RETRIES` 是常量。如果将来真的有用户想调，先判断是否必要——目前判断**不必要**，不暴露为字段。状态：**保持内部常量**。
