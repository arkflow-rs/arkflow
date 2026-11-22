## 1. output 侧收编（机械替换保文案）

- [ ] 1.1 `output/influxdb.rs`：`flush()` 换 `retry_with_backoff_classified`（qdrant 式分类闭包：transport/429/5xx→retryable，其余 4xx→deterministic），错误文案逐字符不变，`max_attempts = retry_count.max(1)`（语义不变），消灭 `2_u64.pow` 溢出点；`test_flush_retries_then_succeeds`/`test_flush_server_error_retains_buffered_lines` 保绿
- [ ] 1.2 `output/http.rs`：`send()` 换 `retry_with_backoff_classified`（同上分类），错误文案与 64KiB 截断不变；`retry_count` 加 `#[serde(default)]` 默认 3（schema 同步）；`server_error_is_retried_then_reported` 保绿
- [ ] 1.3 `output/mqtt.rs`：`reconnect()` 换 `retry_with_backoff` + 常量 `RetryConfig{3, 1000, 4000, 2.0}`（调度逐字节复现 1s/2s），`RECONNECT_ATTEMPTS` 与最终错误文案保留，修正 188 行 "1s, 2s, 4s" 失真注释

## 2. input 侧收编（pulsar 式保循环）

- [ ] 2.1 `input/kafka.rs`：`settle_tombstone` 保循环形态与取消检查、自定义 `warn!` 不变，延迟换共享 `advance_backoff`/`sleep_duration` + 常量 `RetryConfig{4, 200, 5000, 2.0}`（曲线 200/400/800/1600）
- [ ] 2.2 `input/kafka.rs`：restore seek 的手写 `(200ms×attempt).min(1s)` 换共享 `sleep_duration`（本地常量 `RetryConfig{initial 200, cap 1000, ×2.0}`），deadline 循环形态不变
- [ ] 2.3 `input/nats.rs`：删除 296/308 两处死 sleep 与失真注释；批内逐条错误分支改「单发 `Delivery::Err(Disconnection)` 后 break」，对齐 spawned loop 形态；`jetstream_fetch_loop_pins_a_bounded_expires` 保绿

## 3. 骨架卫生

- [ ] 3.1 `retry.rs`：移除 `is_retryable_status` 的 `#[allow(dead_code)]` 与过时注释；22 行模块注释指向真实卫生测试名
- [ ] 3.2 `tests/connector_error_contract.rs`：pow 针规范化——数字字面量下划线归一 + 匹配 `\.(saturating_|checked_|wrapping_)?pow\(`，扫描范围扩到 `input/`（production section）
- [ ] 3.3 `tests/connector_error_contract.rs`：新增 `RETRY_ADOPTERS` 采用方针（qdrant/milvus/http/influxdb/mqtt output、kafka/pulsar input、AI 四处理器 production 段必须含 `crate::retry` 引用），并确认现有违例清单为零

## 4. 元数据与快照

- [ ] 4.1 `processor/vector_search.rs`、`processor/milvus_search.rs`：`register_processor_metadata` schema JSON 补 `retry_count` 声明（对齐 llm/embedding 措辞）
- [ ] 4.2 `ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot` 重生成 `docs/reference/component-inventory.json` 与 `docs/static/config-schema.json`

## 5. 文档与 CHANGELOG（EN+zh 锁步）

- [ ] 5.1 influxdb 组件页（EN+zh）：核对重试语义措辞（attempts 语义、4xx 立即失败、封顶退避）；http 组件页（EN+zh）：`retry_count` 默认 3 与 4xx 行为
- [ ] 5.2 CHANGELOG 记录行为变化：influxdb/http 4xx 分类、kafka 结算曲线、nats 错误路径收敛、http `retry_count` 变可选
- [ ] 5.3 相关 yaml 代码块分类标记核对（`validate=full`/`fragment`），`pnpm docs:check` 通过

## 6. 验证

- [ ] 6.1 `cargo test -p arkflow-plugin`（含 connector_error_contract、registry_consistency、docs_inventory_snapshot）全绿
- [ ] 6.2 `cargo clippy --workspace --all-targets` 零新增告警；`./target/release/arkflow --config <example> --validate` 抽验 influxdb/http/mqtt/kafka/nats 示例
- [ ] 6.3 对照主 spec 逐条核实 delta 场景（MODIFIED 两需求全场景落实），`openspec validate --specs` 全绿
