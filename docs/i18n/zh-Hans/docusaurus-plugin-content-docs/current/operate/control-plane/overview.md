---
sidebar_position: 1
---

# 控制平面概览

ArkFlow 提供一个可选的控制平面,用于把多个计算节点当作机群(fleet)来运维:
**Hub**(权威的注册表与命令中转)、各节点上的 **Agent**(上报运行时状态并执行命令),
以及 Web **控制台**。Agent 在注册时声明自己的能力——例如,配置了 shuffle 数据平面的节点会宣告
`network_shuffle` 能力以及一个可路由的数据地址,放置(placement)逻辑据此决定它可以接受哪些作业。

控制平面与健康检查共用同一个 HTTP 服务。通过 `health_check.address` 与
`health_check.api_prefix` 配置绑定地址和带版本前缀的路径;可选的 `health_check.api_token`
为生命周期命令以及配置读写启用 Bearer 认证。请将该监听保持在本地,或置于经过认证的反向代理之后。
当 `health_check.hub_url` 缺省时,ArkFlow 以独立(standalone)模式运行,只提供兼容性健康路由。

## API 端点

资源端点包括 `/api/v1/system`、`/nodes`、分页的 `/streams`、`/operations`、`/events`、
`/configuration`、`/components` 与 `/schema`。`/metrics` 暴露 Prometheus 文本格式指标。

生命周期命令返回 `202 Accepted` 和一个操作 ID;轮询 `/api/v1/operations/{id}`
直到它到达终态。在等价操作活跃期间,同一资源/动作的命令是幂等的。生命周期命令的目标是
`/api/v1/nodes/{node_id}/streams/{stream_id}/{action}`。

兼容性健康路由 `/health`、`/readiness`、`/liveness`、`/metrics`,
以及旧版 `/api/v1/config*` 别名仍然可用。

## 配置调和

配置变更在调和之前会先被解析和校验:

- **未变更**的流保留其正在运行的任务;
- **变更或移除**的流会被停止;新建/变更的流会先构建再启动;
- 版本以原子方式写入 `.arkflow/config-history`,回滚会创建一个新的子版本,而不是改写历史。

流 ID 只能包含 ASCII 字母、数字、`-` 或 `_`;旧配置会得到确定性的 `stream-0`、`stream-1`、…… ID。

节点配置从 `/api/v1/nodes/{node_id}/configuration` 读取;应用与回滚通过所选节点的
Agent 会话下发。配置快照在 Hub 保留之前会先做脱敏处理。

## 节点会话与租约

在节点租约之上,Hub 还为每个 Agent 会话设置了硬性过期时间(`session_ttl_ms`,默认一小时)。
过期会话不再通过认证,Agent 会像对待任何会话丢失一样处理拒绝:使用稳定的启动标识重新注册,
并保留排队的命令与已上报的资源。过期(stale)节点仍然可见,
但无法接收新命令;被标记为 stale 的节点会显示最后可见时间,需要 Agent
下发的变更类操作会被禁用或给出说明,而不是被静默排队。

如果计算节点无法连接 Hub,它会保持本地数据平面策略,以有界退避重试注册与心跳,并在本地呈现断连状态;
其正在运行的流继续对着已配置的输入与输出工作。WAL 仍保持至少一次(at-least-once)——重启流时会按照其配置的
WAL 游标语义重放未确认的条目(参见[投递语义](/zh-Hans/docs/build/delivery-semantics))。

## 调和模型

Hub 在两个层次上调和**期望与观测**状态:

- **节点**——已注册的能力与租约状态,对比最近一次上报的运行时快照;
- 每条**流**——已配置的规格,对比正在运行的任务。

调和由持久化的期望状态驱动,因此断连后重连的节点会继续朝同一份期望配置收敛,而不是停留在过期状态。
控制台首先呈现机群健康与各节点摘要;选中一个节点即可将运行时、配置、事件与操作视图限定到该节点。

## 后续步骤

- [部署](./deploy.md) —— 运行 Hub、计算节点与控制台。
- [运维](./operations.md) —— 上线后的运维手册。
- [HTTP API v1](/zh-Hans/docs/reference/api) —— 端点参考。
- [调和灰度发布与恢复](./reconciliation.md)。
