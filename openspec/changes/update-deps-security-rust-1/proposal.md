# Proposal: update-deps-security-rust-1

## Why

GitHub dependabot 在 main 上报出 Rust 侧 4 个带漏洞的依赖包（11 条告警），其中两个可立即收敛：`protobuf 2.28.0`（CVE-2025-53605，medium：反序列化不受控递归崩溃；由 `Cargo.toml:39` 的 `prometheus = "0.13"` 经 `prometheus 0.13.4 → protobuf ^2` 拉入，是 #1295 换 protox 后 rust-protobuf 2.x 在图中的最后一个用户）和 `rustls-webpki 0.102.8`（GHSA-82j2-j2ch-gfr8，high：畸形 CRL BIT STRING 触发 panic 拒绝服务；另伴随 GHSA-xgp8-3hg3-c2mh / GHSA-965h-392x-2mh5 两条 low，由 `crates/arkflow-plugin/Cargo.toml:85` 的 `async-nats = "0.46"` 与 `rumqttc 0.25.1` 分别拉入）。上游修复均只存在于新版线：prometheus 0.14 改用 `protobuf ^3.7.2`，async-nats 0.47+ 改用 `rustls-webpki ^0.103.10`，semver 内 `cargo update` 无法到达。

## What Changes

- 工作区 `prometheus` 0.13 → 0.14：`crates/arkflow-server/src/metrics.rs:11-12` 是唯一使用点（`prometheus::proto::{Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType}` + `TextEncoder`）；`protobuf` 在 0.14 为默认 feature，`proto` 模块照常可用，预计零到极小编译改动。
- 插件 `async-nats` 0.46 → 0.50（`crates/arkflow-plugin/src/input/nats.rs`、`crates/arkflow-plugin/src/output/nats.rs`）：0.47–0.50 无涉及所用 API（`PullConsumer` / `jetstream` / `ConnectOptions`）的破坏性变更；默认 TLS provider 保持 `ring`。
- 随带消灭：`protobuf` 2.x 及其传递依赖从 Cargo.lock 中消失；NATS 侧 `rustls-webpki` 0.102 拷贝消失，全图收敛到 0.103.15 单拷贝（rumqttc 的一份除外，见 Non-goals）。
- 运维可见的行为面变化（记录并验证，不视为回归）：async-nats 0.47 新增 subject 客户端校验（可 opt-out）、0.47 `connection_timeout` 覆盖完整 NATS 握手、0.49 新增客户端侧 max-payload 校验（新错误变体，超限 publish 提前失败）。

## Capabilities

### New Capabilities

- `nats-io`: NATS 输入/输出插件的运维可见契约——普通核心模式与 JetStream 模式的收发语义、鉴权配置，以及依赖升级后客户端侧校验（subject 合法性、max-payload）作为错误路径的边界。

### Modified Capabilities

- `data-plane-observability`: 新增一条 requirement——指标栈依赖升级（prometheus 0.14 / rust-protobuf 3.x）保持 Prometheus 文本导出契约（格式 0.0.4、HELP/TYPE、闭合标签词汇表、legacy `arkflow_stream_*` 序列名）逐字节不变。

## Impact

- 依赖：`Cargo.toml`（prometheus）、`crates/arkflow-plugin/Cargo.toml`（async-nats）、`Cargo.lock`（protobuf 2.x 移除、webpki 0.102 移除）。
- 代码：`crates/arkflow-server/src/metrics.rs`（如需）、两个 NATS 插件文件（如编译驱动需要）。
- 测试：`metrics.rs` 内 7 个 exposition 精确文本断言测试、`crates/arkflow-plugin/tests/nats_io.rs` 集成测试、arkflow-server 的 Hub 指标导出测试。
- 不涉及存储格式、wire 格式、配置 schema 变化；无需数据迁移。

## Non-goals

- 不升级 `pyo3`（0.28.3，high×2 + medium×2 告警）与 `thrift`（0.17.0，medium）：分别被 `arrow-pyarrow 58` 与 `parquet 58` 钉死，属于 datafusion 55 / arrow 59 大火车，不单独绕行。
- 不处理 `rumqttc 0.25.1` 残留的 `rustls-webpki ^0.102.8` 拷贝：0.25.1 已是最新版且仍钉旧版，上游无修复线；其暴露面为 CRL 解析路径，常规 MQTT TLS 证书校验不触发。记录为已知残留，revisit 触发条件为 rumqttc 发布升级 webpki 的版本。
- 不处理 Node 侧 35 条告警（docs/pnpm-lock.yaml 32 条含 2 critical、console/package-lock.json 3 条）：全部为构建期/开发期依赖，属另一批 lockfile 刷新变更。
- 不引入 `[patch]` 自用 fork、不为绕过 semver 约束做任何钉版操作。
- 不顺手做与本批安全收敛无关的依赖刷新。
