## Why

`consolidate-connector-skeleton`（#1386，已合入）落地了共享重试骨架（`retry.rs`/`http_util.rs`）与 7 个采用方，但 2026-11-22 的三路扫尾分诊（HEAD `ddf2fe9f` 逐一实测）发现收尾欠账：

- **最后一处真实溢出点**：`output/influxdb.rs:435` 的 `100 * 2_u64.pow(attempt)` 无饱和无封顶，`retry_count` 用户可配——attempt≥63 即 debug panic / release 回绕；且 `2_u64.pow`（下划线）与 `output/http.rs:248` 的 `2u64.saturating_pow` 两种写法变体**都绕过了卫生测试的字面子串针 `"2u64.pow"`**——测试绿但违规在。
- **http output 重试不分类**：`output/http.rs:198-251` 对 404/401 等确定性 4xx 一律重试到耗尽；`retry_count` 无 serde default（必填，与 AI 处理器的可选默认 3 不一致）。
- **三份手写退避未收编**：`output/mqtt.rs:187-209` 手写 `delay *= 2`（注释"1s, 2s, 4s"失真——4s 从不睡眠）；`input/kafka.rs:232-250` tombstone 结算手写线性 `200*(attempt+1)` ×4 次；`input/kafka.rs:1098-1109` restore seek 手写 `200ms*attempt` 封顶 1s。
- **nats 死代码**：`input/nats.rs:296/308` 两处 100ms sleep 注释写 "Short pause before retrying" 但其后没有任何 retry 发生（报 Disconnection 后直接 break/spawn 交引擎重连）；批内逐条错误还会重复发多条 Disconnection。
- **钉扎承诺未兑现**：`retry.rs:22` 宣称手写循环 "pinned by the connector skeleton hygiene test"，但卫生测试实际只钉了 pow 子串与通道容量，没有「采用方必须引用共享实现」的针；`retry.rs:63` 的 `#[allow(dead_code)]`（is_retryable_status）已过时——qdrant/milvus/http_util 三处均在用。
- **注册表元数据漂移**：`vector_search`/`milvus_search` 的 schema JSON 未声明 `retry_count`（llm/embedding 有），文档清单与配置面不一致。

## What Changes

- **influxdb output `flush()` 收编共享重试**：qdrant 式 `retry_with_backoff_classified`（自定义分类包装，既有错误文案逐字节不变）：429/5xx/传输错误重试，4xx 确定性立即失败（**行为变化**）；退避封顶 `max_delay_ms`（消灭最后一个 pow 溢出点）；`retry_count` 保持既有「总尝试数」语义与默认 3。
- **http output `send()` 收编共享重试**：同上 classified 收编（既有错误文案与 64KiB 截断保留，4xx 不再重试为**行为变化**）；`retry_count` 增 `#[serde(default = "default_retry_count")]` 默认 3（原必填变可选，非破坏）。
- **mqtt output `reconnect()` 收编**：`retry_with_backoff` + 常量 `RetryConfig{3, 1000, 4000, 2.0}`——调度逐字节复现（1s/2s），零行为变化；修正失真注释。
- **kafka input 两处收编（pulsar 式保循环）**：`settle_tombstone` 保留「sleep 前取消检查」循环形态，延迟调度换共享 `advance_backoff`/`sleep_duration`；线性 200ms×4 → 指数 200ms 起 ×2 封顶 5s（**行为变化**，常量保持硬编码、不加配置面）。restore seek 的手写公式换共享调度（deadline 循环形态不变）。
- **nats input 清理**：删除两处死 sleep；批内逐条错误重复发 Disconnection 收敛为单次。
- **卫生钉扎修复与扩展**：pow 针规范化——数字字面量下划线归一，匹配 `.pow(` / `.saturating_pow(` / `.checked_pow(` / `.wrapping_pow(`；扫描范围扩到 `input/`；新增「采用方文件必须引用 `crate::retry`」的针（兑现 retry.rs:22 承诺）；移除 `is_retryable_status` 过时 allow 与过时注释。
- **注册表元数据补齐**：vector_search/milvus_search schema JSON 补 `retry_count` 声明（对齐 llm/embedding），重生成 inventory/schema 快照。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `connector-resilience`: 共享重试采用义务从「qdrant/milvus/pulsar/AI 处理器」扩展到 http/influxdb/mqtt output 与 kafka input tombstone/restore 路径；「手写循环被钉扎禁止」场景更新为规范化 needle（pow 写法变体不可绕过）、覆盖 input/、采用方文件必须引用共享实现。
- `influxdb-output`: "InfluxDB v2 writes are batched and retried" 需求增补场景——4xx 确定性失败立即报错且缓冲保留（不再重试到耗尽）；429/5xx 按共享退避重试。

## Impact

- **代码**：`crates/arkflow-plugin` 的 `output/http.rs`、`output/influxdb.rs`、`output/mqtt.rs`、`input/kafka.rs`、`input/nats.rs`、`retry.rs`；`tests/connector_error_contract.rs`；两个处理器的注册表元数据。
- **配置面**：http output `retry_count` 必填→可选默认 3（非破坏）；其余组件配置面零变化。
- **用户可见行为变化**（进 CHANGELOG + 组件文档 EN+zh）：influxdb/http 的 4xx 不再重试（确定性失败更快暴露、退避封顶 5s）；kafka tombstone 结算延迟曲线线性→指数；nats 错误路径少一次 100ms 延迟、批内错误只报一次 Disconnection。
- **文档门禁**：influxdb 组件页（EN+zh）重试语义措辞核对；`ARKFLOW_REGENERATE_DOCS=1` 快照重生成；example YAML 均离线可验证，无需 `validate: false`。

## Non-goals

- 不统一 influxdb `retry_count` 的「总尝试数」语义到 qdrant/http 的「重试次数」语义——破坏已发布配置含义，需独立决策与弃用周期。
- 不给 sql/pgvector output 增加重试——现状零重试，接入是行为增强而非收编，另立 spec。
- 不收编 `wal/s3.rs` `write_manifest_with_etag` 的忙重试——mutator 形态与语义（ETag 协调）不同，归 `s3-wal-pipeline`；若要加退避间隔另行评估。
- 不给 output/kafka 的 QueueFull 50ms 自旋设上界——本地入队背压等待而非失败重试，上界化是行为变化，另议。
- 不做非 HTTP 错误分类器（kafka RDKafka 26 码 / redis 协议码 / SQLSTATE 三族）的共享化——独立 change。
- 不重写卫生测试的目录递归与 `production_section` 截断策略——测试基建重构，收益与本 change 不匹配。
