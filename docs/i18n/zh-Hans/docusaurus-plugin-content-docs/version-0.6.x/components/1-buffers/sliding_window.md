---
components: [sliding_window]
description: ArkFlow 文档页。
---

# 滑动窗口(Sliding Window,已弃用)

:::warning 已弃用,且在 Stream 配置中不可用

流编译器会**拒绝**滑动窗口缓冲(Buffer),并返回一条可操作的错误。本页仅为迁移前的旧配置保留,用于说明这个旧版插件。对于基于时间的重叠窗口(Window),请在 Job DAG 中使用事件时间的滑动**窗口算子**,它会为每一行判定其所属的每个窗口的成员关系。

:::

## 旧行为

滑动窗口缓冲按时间推进,把消息(Message)分入相互重叠的窗口中。每次 `interval` 到期时,最多 `window_size` 条消息会作为一个批次(Batch)发出;每次发出后,窗口按 `slide_size` 条消息向前滑动。

## 旧配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `sliding_window` |
| window_size | integer | yes | — | 每个发出的窗口包含的消息数。 |
| interval | duration | yes | — | 两次窗口发出之间的间隔时间。示例:`1ms`、`1s`、`1m`、`1h`。 |
| slide_size | integer | yes | — | 每次发出后窗口前进的消息数。控制相邻窗口之间的重叠程度。 |

编译声明了该缓冲的 Stream 配置会失败并返回迁移提示;带滑动窗口算子的 Job DAG 是其替代方案。
