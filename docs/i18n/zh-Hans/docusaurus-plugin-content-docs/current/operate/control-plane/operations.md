---
sidebar_position: 4
description: ArkFlow 文档页面。
---

# 控制平面运维

Hub 在 `/liveness` 暴露进程存活,在 `/readiness` 暴露存储/恢复就绪状态,在
`/api/v1/operations/status` 提供有界诊断,在 `/api/v1/metrics` 提供 Prometheus 指标。
请将这些运维路由置于所配置的运维令牌边界或经过认证的监控代理之后;
绝不要公开 bearer 令牌、配置载荷、作为指标标签的节点 ID 或作为标签的错误文本。

`/api/v1/metrics` 支持内容协商:默认响应仍为上文的 Prometheus 文本;显式携带 `Accept: application/json`(或 `?format=json`)时返回 Web 控制台渲染的 JSON 聚合 `{"items": [{"node_id", "metrics"}], "aggregate"}`,两个分支都支持 `node_id` 过滤。Hub 还提供 `GET /api/v1/status`——机群聚合的引擎状态(流总量为各注册节点之和,以及 Hub 版本与运行时长)——使控制台客户端在本地与 Hub 模式下看到同一份概览契约。作业详情诊断(`GET /api/v1/jobs/{id}/detail`)只包含已度量的、作业级别的指标(`watermark_lag_ms`、`checkpoint_duration_ms`、`checkpoint_failures`),并将执行节点上报的观测任务状态合并到期望放置之上,显式标记尚未观测的任务,而不是把放置状态当作运行时状态。只读配置报告可通过 Hub 访问:`POST /api/v1/nodes/{node_id}/configuration/validate` 与 `GET /api/v1/nodes/{node_id}/configuration/diff?from&to`——两者都只派发只读节点命令(不产生版本或 rollout 副作用),报告随跟踪操作的 `result` 字段返回。

命令分发指标覆盖从入队到确认的延迟(`arkflow_command_duration_bucket`/`_count`/`_sum`)以及按结果分类的计数器
(`arkflow_command_total`),标签使用固定的 `command` 与 `outcome` 词表。这些计数器在 Hub
重启时归零,符合 Prometheus 计数器语义。作业生命周期变更(`job_start`、`job_stop`、
`job_checkpoint`、`job_savepoint`)还会以执行者、关联、结果与失败码元数据进行审计;
审计历史在有界窗口内保留(30 天、10 万条),可在 `/api/v1/audit` 查询。持久化调和历史同样有界:
终态操作记录、已处理的 outbox 行与终态 Attempt 记录在 24 小时后或超过 4096 行上限后被回收;
pending/failed 检查点记录在 24 小时后被回收;事件只保留最新的 2048 行。
未处理的 outbox 行与活跃的 Attempt 永远不会被回收;一个专用的 60 秒维护任务执行这些保留清理,
因此不会与每秒一次的调和节拍竞争。

就绪判定有意比存活判定更严格。启动恢复期间或存储故障时,存活进程可能从 `/liveness` 返回
`200`,而 `/readiness` 返回 `503`。应基于就绪状态、调和失败、过期节点与不断增长的 outbox
年龄告警,而不是仅因一次瞬时抓取失败就重启。

滚动部署时,经授权的运维人员应先 POST `/api/v1/nodes/{node_id}/drain`,等待活跃的 Attempt
落定,部署 Agent,再通过 DELETE `/api/v1/nodes/{node_id}/maintenance` 恢复。
若计划停机时间较长,可对维护路由使用 POST。这些状态转换会保留期望状态,并产生包含执行者与关联元数据的
`node_maintenance_changed` 审计事件。排空或维护期间,调和会抑制新的分发,但不会取消在途工作。

回滚是在代理或部署层禁用相应的运维变更或就绪策略。它不会删除期望状态、事件历史或审计记录,
Hub 也不会执行任何自动的破坏性修复。
