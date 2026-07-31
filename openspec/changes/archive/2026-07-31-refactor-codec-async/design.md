## Context

Codec trait 当前 sync（`core/codec/mod.rs:23-31`）：`Encoder::encode` / `Decoder::decode` 均为同步 fn，使 codec 内无法做异步 IO。首例需求是 `add-schema-registry` 的 schema_registry codec（按 id 异步查 Schema Registry），后续网络类 codec 同理。`async-trait` 已在 workspace deps；codec 注册机制（`register_codec_builder`）不变。

## Goals / Non-Goals

**Goals:**
- `Encoder`/`Decoder`/`Codec` trait 的 encode/decode 为 async。
- 现有 json/protobuf/debezium codec 行为完全不变（纯重构）。
- 现有测试全绿（回归保证）。

**Non-Goals:**
- 改 codec 编解码行为/语义。
- 新增 codec 或 IO 实现。
- 改 input/output/buffer trait。

## Decisions

### 决策 1：`#[async_trait]` + `async fn`
`Encoder`、`Decoder` 各加 `#[async_trait]`，`encode`/`decode` 改 `async fn ... -> Result<..., Error>`。`Codec` 是 `Encoder + Decoder` 的 super-trait 组合（`impl<T> Codec for T where T: Encoder + Decoder`），无需单独 async 化。

### 决策 2：分步重构（编译器驱动）
顺序：① trait（core）→ ② 3 个 codec impl → ③ codec_helper 包装 fn → ④ 调用点 `.await`。每步 `cargo build` 推进；漏 `.await` 由编译器逐个抓（机械错误，不会静默）。

### 决策 3：行为不变验证
现有 codec 测试覆盖回归：json round-trip（`codec/json.rs` tests）、protobuf round-trip（`codec/protobuf.rs` `test_codec_round_trip`）、debezium 10 测试。`cargo test --workspace --lib` 全绿即视为无行为变更。

## Risks / Trade-offs

- **[调用点多，漏 `.await`]** → 编译器逐个抓；分步 build 控制范围。
- **[`async_trait` 的 boxed future 开销]** → codec 调用多数按 batch 而非 per-row，频率非超高频，可接受；换 async IO 能力值得。
- **[重构面广]** → ~20 处，但逻辑零变更、纯机械。
- **[`memory.rs:62` 在 sync `new()` 内 decode initial messages]** → `new()` 是 sync（被 `InputBuilder::build` 调），无法 `.await` async codec。用 `block_in_place` + `Handle::block_on` 保持构建时 decode 行为（无变更）；现有 memory test 均 `codec=None` 不触发该路径，生产部署为 multi-thread tokio runtime（满足 `block_in_place` 前提）。

## Migration Plan

纯重构，**无配置/数据迁移**；codec 是内部 trait（非公开 API），无外部破坏。回滚：`git revert`。

## Open Questions

- 无（机械重构，路径确定）。
