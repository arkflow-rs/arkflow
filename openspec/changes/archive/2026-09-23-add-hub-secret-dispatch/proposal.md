## Why

`add-secret-scheme` 提供了 `${secret:NAME}` → `ARKFLOW_SECRET_<NAME>` 的解析约定，但解析发生在**物化进程**——多节点部署下即 Agent 进程，密钥仍需铺到每个节点，「密钥只存 Hub」的中心化模型差最后一步：Hub 在向 Agent 分发配置（rollout 批次处理 `reconcile_rollouts` → intent `payload_json`）时，对 content 中的 `${secret:...}` 引用按 **Hub 进程环境**预解析，Agent 收到已解析配置、无需持有任何密钥环境。

## What Changes

- `crates/arkflow-core/src/secret.rs`：新增 `resolve_candidate_payload(payload) -> Result<Option<String>>`——输入序列化的 ConfigCandidate payload（`{format, content}`），仅解析 content 中的 `${secret:...}` 引用（`env:`/`file:` 引用与未知 scheme 原样保留，属节点本地语义），有解析时 content 以 JSON 文本重序列化且 `format` 置为 `json`；无 `secret:` 引用时返回 `None`（payload 原样）。
- `crates/arkflow-server/src/hub.rs` `reconcile_rollouts`：取得 config version content 后经 `resolve_candidate_payload` 处理——`Some(resolved)` 以解析后 payload 构建 intent；解析失败（如 `ARKFLOW_SECRET_*` 未设置）SHALL 将该 rollout target 置为 failed（错误指明引用），不派发半解析配置。
- Hub 存储的配置版本内容保持引用原文（不落明文）的既有保证不变。

## Capabilities

### New Capabilities

<!-- 无新能力：扩展 secret-references。 -->

### Modified Capabilities

- `secret-references`: 新增多节点分发语义需求——Hub 在 rollout 分发时按 Hub 进程环境预解析 `secret:` 引用；解析失败使目标分发失败。

## Impact

- `crates/arkflow-core/src/secret.rs`：新增 `resolve_candidate_payload` + 单测。
- `crates/arkflow-server/src/hub.rs`：rollout 分发钩子 + 集成测试。
- 文档：secret-references 文档节补充多节点分发语义（en/zh）。

## Non-goals

- 不改 env:/file: 的解析点（保持节点本地语义）。
- 不做持久化密钥库/KMS/轮换；不做回滚版本的预解析差异处理。
- 不触碰 agent 侧协议（payload 形状仍是 ConfigCandidate）。
