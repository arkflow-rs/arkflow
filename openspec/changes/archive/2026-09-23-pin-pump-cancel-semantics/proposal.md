## Why

PLANNING 7.3 记录的「pump void-write 取消语义待办（remote.rs）」：outbound pump 在 wire 写失败、shutdown 取消、上游通道关闭等退出路径上，已注册回执（register）但回执永远不会到达的 batch（"void write"）必须被 abort——否则 source ack 永久泄漏或被虚假推进。当前实现依赖出口处的 `abort_all()`，但该语义从未被测试固定（pin），回归风险敞口存在。

## What Changes

- 为 `pump_edge` 的取消/失败语义添加确定性测试（Ack 间谍记录 ack/abort 调用）：
  1. wire 写失败：已注册分支被 abort，永不 ack，pump 返回 Err；
  2. shutdown 取消时已注册但回执未达的分支被 abort；
  3. 上游通道关闭：在退出 flush 之后才 abort，在途帧尽力送达（best-effort flush 先于 abort_all）；
  4. 正常路径：回执 Acked 到达后分支 ack、pending 移除。
- 在 remote.rs 模块文档固化不变量：不收到回执绝不 ack；任何退出路径 abort 未决回执；flush 先于 abort；at-least-once 允许下游重复。

## Capabilities

### New Capabilities

<!-- 无新能力：扩展 network-shuffle-data-plane。 -->

### Modified Capabilities

- `network-shuffle-data-plane`: 新增 pump 取消与 void-write 语义需求。

## Impact

- `crates/arkflow-core/src/executor/remote.rs`：测试模块扩展 + 模块文档补充；实现预期无需改动（若测试暴露真实缺陷则修复）。

## Non-goals

- 不改线协议帧格式。
- 不做 receipt 超时/重传机制（连接级失败由既有 fail-closed 路径处理）。
- 不处理 abort_all 的 spawn 饥饿残留（已记录为环境属性）。
