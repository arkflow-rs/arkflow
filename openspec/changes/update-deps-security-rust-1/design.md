# Design: update-deps-security-rust-1

## Context

dependabot Rust 侧分诊结论：11 条告警归为 4 个包。`prometheus 0.13.4`（拉入 protobuf 2.28.0，CVE-2025-53605）与 `async-nats 0.46.0`（拉入 rustls-webpki 0.102.8，1 high + 2 low）可独立升级收敛；`pyo3`/`thrift` 被 arrow 系钉死属大火车；`rumqttc` 为上游卡死残留。两个可动项共享同一动机（安全收敛）且互不阻塞，合并为一个变更、一个 PR。

上游事实（探索阶段查证）：
- prometheus 0.14.0（2025-03-27）：`protobuf` 为默认 feature（`^3.7.2`），`proto` 模块与 `TextEncoder` 保留；rust-protobuf 2→3 生成代码的 setter/mutator API 兼容。
- async-nats 0.47.0：`rustls-webpki` → `^0.103.10`、`rustls-native-certs` 0.7→0.8、新增 subject 校验（可 opt-out）、`connection_timeout` 覆盖完整握手。0.48.0：thiserror v2、修复非 UTF-8 读 panic。0.49.0：客户端侧 max-payload 校验（新错误变体）。0.49.1：连接性修复。0.50.0：chrono/time 后端可换（default 仍 time，需显式启用 `chrono` feature 才切换，我们不启用）。四版 release notes 均无涉及 `PullConsumer`/`jetstream::new`/`Context`/`publish`/`ConnectOptions` 的破坏性变更。
- async-nats 0.50 默认 feature 集含 `ring`（TLS provider），`aws-lc-rs`/`fips` 为 opt-in——不引入 C 构建链。
- 图中已有 `rustls-webpki 0.103.15`（经 `rustls 0.23.45`），满足 async-nats 0.50 的 `^0.103.10`，升级后 webpki 收敛为两份：0.103.15（干净）+ rumqttc 的 0.102.8（残留）。

## Goals / Non-Goals

**Goals:**
- 消灭 protobuf 2.28.0（medium CVE）与 NATS 侧 webpki 0.102.8（1 high + 2 low GHSA）告警。
- 保持 Prometheus 文本导出契约与 NATS IO 行为不变（以现有精确文本断言 + 集成测试验证）。
- 变更保持外科手术式：只动两处版本号 + 编译驱动的最小修补。

**Non-Goals:**
- 大火车项（pyo3 0.29、thrift 0.23、datafusion 55/arrow 59）。
- rumqttc webpki 残留（上游无修复线）与 Node 侧 lockfile（另一批）。
- 不新增功能、不改配置 schema。

## Decisions

1. **两项合并、锁步一个 PR**：动机同源（安全告警收敛）、互不阻塞、验证面各自独立（metrics.rs 单测 / nats_io.rs 集成测试），拆开只会增加流程开销。
2. **prometheus 走默认 feature**：`protobuf` 默认开启，`metrics.rs` 的 `use prometheus::proto::...` 无需 feature 声明变化；若编译出现 drift（如 `set_field_type` 之类 setter 签名变化），做机械修补，不改导出逻辑。
3. **async-nats 直升 0.50**（跳过 0.47）：中间版本无我们需要的渐进锚点，直升减少一次解析面；0.47 的 webpki 修复随 0.50 一并到位。
4. **行为面变化以"记录 + 测试覆盖"处理，不做代码消化**：
   - subject 校验：ArkFlow 的 subject 来自配置文件，合法 subject 属于既定契约；非法 subject 由客户端提前报错是收紧而非放松，与 fail-loud 原则一致。不启用 opt-out。
   - max-payload 客户端校验：超限 publish 提前失败，错误信息进入既有错误路径（`Error::Process`/IO 错误日志），不引入新错误类型。
   - connection_timeout 全握手覆盖：不改默认值；`nats_io.rs` 集成测试如出现时序敏感失败，按测试侧适配处理（先诊断再动，预判不需要）。
5. **告警验证以 dependabot 重扫描为准**：本地以 `cargo tree` 确认 protobuf 2.x 与 webpki 0.102 消失（rumqttc 份除外）；PR 描述给出告警号清单（#3、#75、#65、#66）与残留项说明。

## Risks / Trade-offs

- **rust-protobuf 3 生成代码与 `metrics.rs` 的类型面存在低概率不兼容**：缓解——使用面全部是 setter/encoder，`metrics.rs` 的 7 个测试断言精确 exposition 文本，编译期 + 测试期双重兜底。
- **async-nats 内部行为差异未被 changelog 覆盖**（如重连、flush 时序）：缓解——`nats_io.rs` 覆盖普通 + JetStream 两种模式的收发闭环；CI Docker 环境跑真实 nats-server。
- **subject 校验收紧可能暴露既有用户配置中的边缘 subject**：属运维可见变化，PR 描述明确列出，并提供 opt-out 的存在性说明（虽不默认启用）。
- **rumqttc 残留造成"告警未清零"的观感**：PR 描述预先说明残留原因与 revisit 触发条件，避免后续误判为遗漏。
