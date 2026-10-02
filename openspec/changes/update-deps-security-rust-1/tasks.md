## 1. Manifest and lock

- [x] 1.1 在分支 `deps/security-rust-1` 上：`Cargo.toml` 的 `prometheus` 0.13 → 0.14，`crates/arkflow-plugin/Cargo.toml` 的 `async-nats` 0.46 → 0.50，重新解析锁文件
- [x] 1.2 检查锁 diff 并记录：`protobuf` 2.x 及其独占传递依赖消失、NATS 路径的 `rustls-webpki` 0.102 消失（rumqttc 份保留）、无 `aws-lc-sys` 等新原生依赖、`cargo tree -i rustls-webpki` 仅剩 0.103.x + rumqttc 的 0.102.x；跑依赖锁卫生测试确认无新增重复栈告警

## 2. Compile-driven migration

- [x] 2.1 `cargo check --workspace --all-targets`：修补 `crates/arkflow-server/src/metrics.rs` 因 rust-protobuf 3 类型面产生的编译 drift（预期为零或纯机械），保持导出逻辑不动
- [x] 2.2 修补两个 NATS 插件文件因 async-nats 0.50 产生的编译 drift（changelog 预判为零），仅限遥测无关的调用签名调整；`cargo clippy --workspace --all-targets` 清洁

## 3. Verification against the spec contracts

- [x] 3.1 跑 `metrics.rs` 全部 exposition 精确文本断言测试与 arkflow-server 指标相关套件（含 Hub 导出 `node` 标签场景）——验证导出契约逐字节不变
- [x] 3.2 跑 `crates/arkflow-plugin/tests/nats_io.rs`（Docker 可用时覆盖普通 + JetStream 模式收发闭环与鉴权；无 Docker 环境记录 skip 并说明）
- [x] 3.3 跑工作区门禁：dependency-lock-hygiene、`examples_validate`、`docs_snippets_validate`、`two_node_job_smoke`，以及 `cargo test -p arkflow-server`、`cargo test -p arkflow-plugin`（聚焦受影响套件后全量确认）

## 4. Docs and ship

- [ ] 4.1 核对 NATS 组件文档页（`docs/docs/` 与 zh-Hans 对应页）：确认无与客户端校验行为矛盾的表述（如错误处理说明）；仅在存在矛盾时更新；PR 描述携带运维可见变化表（subject 校验、max-payload 预校验、connection_timeout 全握手）与告警收敛清单（#3/#75/#65/#66）+ rumqttc 残留说明
- [ ] 4.2 提交、开 PR、CI 绿后落地；归档本变更（delta 同步进 `data-plane-observability` 与新建 `nats-io`）
