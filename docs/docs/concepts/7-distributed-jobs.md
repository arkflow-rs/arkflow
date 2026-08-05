---
sidebar_position: 7
---

# Distributed Jobs

ArkFlow 的 Job 是面向有状态流处理的新运行时契约，与现有 YAML Stream 并行存在。Job 由带稳定 ID 的算子和边组成，提交后生成不可变的 `JobVersion` 与物理任务计划。

## 时间语义

Job 可以声明事件时间字段、每分区 watermark、空闲分区超时和允许迟到时间。watermark 由活跃分区的最小进度聚合；超过窗口边界的事件按 `drop`、`route` 或 `update` 策略处理。

## 嵌入式状态与检查点

热路径状态保存在 Compute 本地的嵌入式 KV 中，按 Job、算子和 key namespace 隔离，并支持 TTL、大小计量和格式版本。检查点将状态快照、源位置和 watermark 以校验和保护的 manifest 写入共享对象存储；恢复顺序是先恢复状态和源位置，再开始读取输入。

## 控制面与兼容性

Hub 持久化 Job、版本、任务分配和恢复记录，使用 generation 防止旧任务报告覆盖新意图。Agent 通过能力声明确认 Job runtime、状态后端和 checkpoint 协议版本。旧的 `Stream` YAML API 不被转换或删除，可继续按原路径运行。

## API 示例

```http
POST /api/v1/jobs
PUT  /api/v1/jobs/{job_id}/desired-state
GET  /api/v1/jobs/{job_id}
```

建议先用 `stopped` 提交并检查编译后的 plan，再切换为 `running`。checkpoint/savepoint 的生命周期与 Job 版本、状态格式版本绑定。
