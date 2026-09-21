# Design: close-hardening-loose-ends

## Context

`harden-remote-data-plane`（#1239）给跨节点数据面加了 HMAC 会话握手（绑定 job/generation/quad、防重放），但未认证路径是以"调用者自觉"的方式排除的：

- 生产侧事实上的防护链：Agent 无凭据不创建 `NetworkManager`（`agent.rs:1674-1695`）→ 不广播 `network_shuffle` 能力 → Hub 不做 split 放置；图构建只调用会话 API（`graph.rs:867/924`）。
- 内核侧无强制：`open_edge` / `open_edge_with_stream` / `open_edge_deferred`（`remote.rs:1530/1555/1601`）是公开的 None-auth 入口，`serve_stream_inner:1897` 的握手分支以 `credentials` 存在为前提——一个 `credentials: None` 的 manager 调 `bind_tcp` 就能得到明文监听器。
- 唯一消费者是测试：`remote.rs:3402`（内嵌）、`executor/tests.rs:4564-4567, 4674`。

另有同一批次遗留的两处非安全收尾：`barrier.rs:341-358` 的死代码、`hub.rs:2491-2527` 与 `2687-2723` 的 boot 失效逻辑双拷贝。

## Goals / Non-Goals

**Goals:**

- 未认证数据面从"约定不可达"变为"构建期/运行期不可达"（非测试构建）。
- 消除 barrier 协调器契约处的误导性死代码。
- Hub 的 boot 失效语义收敛到单一实现。

**Non-Goals:**

- 不改变任何认证协议本身（握手、nonce、generation 绑定均不动）。
- 不引入每节点密钥/mTLS（信任模型升级是独立变更）。
- 不拆分 hub.rs/lib.rs 文件结构（独立的机械重构变更）。
- 不动 `NetworkManager` 的通道容量、背压、receipt 语义。

## Decisions

### D1：三层收口，而非"凭据必填构造"

1. `open_edge` / `open_edge_with_stream` / `open_edge_deferred` 加 `#[cfg(test)]`。`executor/tests.rs` 与 `remote.rs` 内嵌测试同属 arkflow-core 的 cfg(test) 编译单元，可见性天然覆盖两个消费者；工作区内已核实无其他调用者（grep 全 workspace）。
2. `bind_tcp` 在 `credentials: None` 时返回显式配置错误——覆盖"`accept_stream`/`spawn` 仍需保持公开供测试注入 duplex 流"留下的最后一条生产监听路径。
3. `legacy_job` 登记路径与进程级 `failures` channel 保持原样：只被上述 cfg(test) API 触达，无需额外动作。

**备选（否决）**：把 `NetworkManagerConfig.credentials` 从 `Option` 改为必填。会迫使所有单元测试伪造密钥、并波及 colocated-only Agent 的构造路径，churn 大而新增安全性不超过上面两层。

### D2：测试迁移到会话 API，跟随既有示范

`remote.rs` 内嵌测试已有完整的认证会话用法（构造 `DataPlaneCredentials` → `with_config` → `open_edge_*_for_session` / `register_inbound_for_session`）。`tests.rs` 的三个远程集成测试按此迁移：

- `4564-4567`（barrier 对齐）→ `open_edge_deferred_for_job` + 会话注册；
- `4674`（下游处理失败）→ `open_edge_with_stream_for_session`；
- 4384（fail-closed 不可达）随 manager 构造方式联动调整。

测试语义不变（多出的握手帧对被测行为透明），这是迁移而非重写。

### D3：`BarrierCoordinator::complete` 只删不建

删除构造即弃的 `TaskAttemptSnapshot` 向量；在类型级 rustdoc 固化契约："协调器职责止于全部参与者上报；manifest 持久化由 Agent/Engine 接线拥有"。

**备选（否决）**：把 manifest 组装下沉进协调器。会移动持久化职责归属，属于行为变更，与本变更的零行为目标冲突。

### D4：Hub 去重只抽"同构核"

两处拷贝中，操作失效核（按 node_id 过滤 `job_start`、置 `NodeUnavailable` + `recovery_required`、收集 job 集合）完全同构，抽为私有方法；各入口差异化的后续动作（register 路径丢弃队列命令、report 路径重置观察游标）保留原位。抽取前先逐行 diff 两块，确认无隐藏差异——若有，以行为为准保留分支并只抽真正同构部分。

## Risks / Trade-offs

- [`#[cfg(test)]` 造成下游编译破坏] → 已 grep 核实 workspace 无非测试调用者；CI `cargo build --workspace --all-targets` 兜底。
- [测试迁移引入行为偏移（握手帧时序）] → 会话路径已有内嵌测试示范；迁移后逐测试跑通即为验收。
- [`bind_tcp` 运行期错误 vs 构造期拒绝] → 选择运行期错误保持 API 兼容；Agent 是唯一生产调用方且必带凭据，新错误路径只对误用生效。
- [与 `harden-remote-data-plane` 的归档顺序] → 双方对该能力都用 ADDED delta，任意归档顺序合并结果一致。

## Migration Plan

单 PR、零配置迁移、无部署影响；回滚即 revert。归档本变更前建议先归档 8 个已完成的存量变更（例行清理，不阻塞）。

## Open Questions

（无——范围在探索阶段已逐行核实。）
