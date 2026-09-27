# Design: add-job-rescale-guard

## 现状事实

- `effective_state_namespace` 内嵌 task id；`JobPlan::compile` 按 `parallelism × operators` 展开任务。
- `restore_local_snapshot` 按 (namespace, key) 原样恢复——旧 task 命名空间的条目在新任务集下无人读取。
- `CheckpointManifest.task_attempts` 记录了封存时的任务集；封存侧已有 `attempt_tasks == participants` 校验，恢复侧缺失对应校验。

## 决策

1. **恢复侧全集比对**（而非仅并行度数值）：拓扑变化（增删算子）同样使旧命名空间失联，任务集比对覆盖两类变更且实现一处。
2. **fail-closed 而非告警**：恢复到空状态是正确性破坏，不是可降级场景。
3. **错误信息可操作**：明确指出两条出路，避免用户在报错前静默损失。

## 风险

- 用户流程变更：此前"能跑"（实则空状态）的变更高并行度恢复现在失败——这是把数据缺口暴露出来，属修复而非回归；发布说明明示。
