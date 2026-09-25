# Design: add-job-rescale

## 关键对齐：状态键 ↔ 路由哈希输入

重分布的正确性取决于"从状态键还原的 bytes"与"路由时哈希的 bytes"逐字节一致。两类编码均已核实：

| 算子 | 状态键 | 路由输入还原 |
| --- | --- | --- |
| Window | `window_start(BE 8B) + key(utf8)` | `key[8..]`（`hash_column` String 路径 = `value.as_bytes()`） |
| StatefulOperator | `"tag:" + value编码` | 剥前缀；整数 BE、binary 原样、`null:tag` 哨兵整条 |

`hash_column`/`task_for_key` 是归属的唯一权威；重分布复用同一 `key_group_for_key`。

## 决策

1. **显式 opt-in 而非自动**：拓扑变化也可能是误操作（错配置）；默认 fail-closed 保持守卫的保护，`rescale: true` 是操作员确认。
2. **重写 namespace 而非搬 value**：条目 (key, value, expires_at) 原样，只换归属——最小侵入，快照格式不变。
3. **编码白名单 + fail-closed**：未见前缀的键报错而非猜测——错误归属比失败更糟。
4. **max_parallelism 必须不变**：key-group 数 = max_parallelism；变化时旧 group 无法映射（v1 显式不支持，文档与校验兜底——plan 的 max_parallelism 与条目哈希用的相同值，天然一致；若用户改了 max_parallelism，group 数变了但哈希仍按新值计算，归属仍确定——**实际上算法对新 max_parallelism 也自洽**（按新值哈希分配），但语义是"重新分片"而非"保持归属"；文档建议保持不变以维持亲和性）。

## 风险

- 重分布后同 key 的窗口状态可能跨任务分裂再聚合（不同窗口 start 本就独立条目）✓ 无合并需求。
- 大状态量下重分布是恢复路径上的同步开销（条目重写）——与恢复本身同量级。
