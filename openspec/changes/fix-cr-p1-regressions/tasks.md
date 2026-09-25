## 1. 持久化与关停语义（arkflow-core）

- [x] 1.1 WAL drain window：在 `wal/mod.rs` 新增 `WAL_ACK_DRAIN_WINDOW = 30s` 常量与 `ack_drain_window_ms` 原子字段（含 `#[cfg(test)]` 覆写方法），parked-ack 分支恢复 `close.cancelled()` 竞速并在窗口耗尽时返回 `WAL closed while acknowledgement was pending`（合规 `input-durability` 既有 REQUIREMENT）
- [x] 1.2 回归测试：parked ack 在 close 后、窗口耗尽且前序不 settle 时返回错误（覆写窗口至 ~200ms）；修正 `stream_adapter.rs` 测试注释中的 "15s" 笔误为 30s；确认 `close_drains_parked_acknowledgement_after_in_flight_settles` 仍通过
- [x] 1.3 pump 退出路径：`remote.rs` 的 `pump_edge` 增加 `drained` 标志，仅 `result.is_err() || !drained` 时 `abort_all`；同步修正模块 doc 与 delta 规格一致
- [x] 1.4 回归测试：通道排空退出（drop sender）后已注册分支不被 abort，随后 `pending.apply(Acked)` 正常 ack；既有 `pump_cancel_tests` 两例保持通过

## 2. Hub 派发失败状态机（arkflow-server）

- [x] 2.1 `hub.rs` `reconcile_once` 预解析失败分支：`tracing::warn!` + `complete_attempt(attempt_id, "failed", Some("invalid_config"))` + `mark_outbox_processed` + 返回 `Ok(None)`（合规 `secret-references` 既有 target-failed 场景）
- [x] 2.2 回归测试：含未设置 `${secret:...}` 的 payload 经 `reconcile_once` 后 attempt 为 failed、intent 为 blocked、outbox 行已 processed，且不产生派发命令

## 3. 插件注册错误传播（arkflow-plugin）

- [x] 3.1 `input/output/processor/buffer/codec` 五个 kind 的 `mod.rs` 改为 `OnceLock<Result<(), String>>` 存储首次结果并克隆返回（新增 `component-registry-export` ADDED REQUIREMENT）
- [x] 3.2 `cargo test -p arkflow-plugin --lib` 与 `registry_consistency` 通过，确认成功路径幂等不变

## 4. 控制面安全边界（arkflow-server / arkflow-core）

- [x] 4.1 `server/lib.rs` `validate_configuration` 增加 `State(cp) + HeaderMap` 参数与 `authorized()` 守卫，未认证返回 401（新增 `configuration-management` MODIFIED REQUIREMENT）
- [x] 4.2 回归测试：无凭据 POST `/configuration/validate` 返回 401；带凭据返回验证报告
- [x] 4.3 `core/config.rs` `parse_engine_config` 的 `from_value` 失败改用值无关固定消息 `"Configuration error: validation failed after secret resolution"`（合规 `secret-references` 不泄漏保证）
- [x] 4.4 回归测试：含 `${env:...}` 解析成功但类型不符的文档，错误消息不含解析出的明文值

## 5. pgvector 并发 close 竞态（arkflow-plugin）

- [x] 5.1 `output/pgvector.rs` `write()` 单次获取 pool 锁，`expect` 改为 `let ... else` 返回 `Error::Connection`，锁跨查询持有
- [x] 5.2 既有 pgvector 测试通过；空 batch/未连接路径行为不变

## 6. 验证与收尾

- [x] 6.1 `cargo test --workspace --all-targets` 通过
- [x] 6.2 `cargo clippy --workspace --all-targets` 无新告警（改动前后告警数均为 95，逐一核对改动文件无新增）
- [x] 6.3 openspec 校验通过（`openspec validate --change fix-cr-p1-regressions`）
