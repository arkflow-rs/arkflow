---
sidebar_position: 7
---

# Distributed Jobs

ArkFlow 的 Job 是面向有状态流处理的新运行时契约，与现有 YAML Stream 并行存在。Job 由带稳定 ID 的算子和边组成，提交后生成不可变的 `JobVersion` 与物理任务计划。

## 时间语义

Job 可以声明事件时间字段、每分区 watermark、空闲分区超时和允许迟到时间。watermark 由活跃分区的最小进度聚合；超过窗口边界的事件按 `drop`、`route` 或 `update` 策略处理。

事件时间字段接受 Int64(毫秒)以及 Arrow `Timestamp` 的秒、毫秒、微秒、纳秒列,统一换算为毫秒;负时间戳按与窗口边界一致的 `div_euclid` 规则取整,溢出返回可定位的字段错误而不是静默回绕。**空时间戳永远不会被无限期 Hold**:配置了 `route` 时它随迟到事件进入 side output,否则丢弃并完成确认。

同一批次先推进 watermark 再分类当前行:一个 `[2100, 100]` 的批次中,100 立即按迟到策略处理,只有真正的未来行保持 Held。窗口聚合的数值类型保持输入类型 — `Float64` 求和后以 `Float64` 输出,`Float32` 以 `Float32` 兼容 schema 输出,`sum`/`min`/`max` 不再退化为整数哨兵(该行为为 **BREAKING** 变更;整数输入保持 Int64)。非整除的滑动窗口(`size=5, slide=2`)枚举每一个包含事件时间戳的窗口起点,不做整数除法截断。

已触发的窗口在其允许迟到期限内保留:期限内的迟到 `Update` 修改同一个 `(operator, key, window)` 聚合,并带 `__arkflow_window_update` 标记列重发完整修正结果;超过期限后缓冲区才清理。

## 嵌入式状态与检查点

热路径状态保存在 Compute 本地的嵌入式 KV 中,按 Job、算子和 key namespace 隔离,并支持 TTL、大小计量和格式版本。检查点将状态快照、源位置和 watermark 以校验和保护的 manifest 写入共享对象存储;恢复顺序是先恢复状态和源位置,再开始读取输入。

### 已确认切点(acknowledged cut)

检查点的源位置、watermark、算子状态和 barrier 必须来自同一个**已确认边界**:源链在注入 barrier 前先排空在途(非 held)确认 — 每个确认都先落状态 journal、再推进 WAL 游标、最后提交源端 offset — 然后封存包含位置与 watermark 的不可变 cut。有状态算子的变更先进入执行本地 journal,只有输出成功确认后才提交到状态后端;输出失败或任务失败时回滚,重放不会重复提交。多输入链在 barrier 对齐后、释放 barrier 后数据前捕获已提交 epoch 的快照。窗口缓冲区中被 Hold 的确认不阻塞 barrier:其状态保持 staged,恢复后由源位置重放重建。

Kafka 的检查点位置是每个 topic-partition 的**最高连续已确认 offset**:乱序完成的 fan-out 分支不会跳过间隙中未确认的记录。单个源任务保持连接器的全分区订阅,只有多个物理任务才执行显式分区分配;恢复时 checkpoint 位置合并进完整配置 assignment,未记录的分区保留配置起点,并且恢复位置直接作为下一轮 checkpoint 的游标。

只有携带完整计划任务集合的 manifest 才会被封存为 Completed:缺失、重复或多余任务条目都会导致拒绝,节点离线时保留上一个有效恢复点。**状态格式相同即允许目标 Job 版本升级**(较新的版本恢复较旧的 savepoint);降级或格式变更没有迁移路径,双方一致拒绝。

## 控制面与兼容性

Hub 持久化 Job、版本、任务分配和恢复记录,使用 generation 防止旧任务报告覆盖新意图。Agent 通过能力声明确认 Job runtime、状态后端和 checkpoint 协议版本。旧的 `Stream` YAML API 不被转换或删除,可继续按原路径运行。

Job 的观察状态由同一 (generation, action) 下**全部预期 assignment 的聚合结果**推导:所有 assignment 成功才报告 running/stopped,任一仍在 pending 或可重试降级时保持 converging,单个节点的暂态失败不会覆盖健康节点。checkpoint 提交同样要求完整的预期 assignment 集合,离线节点的部分结果不会发布为可恢复 artifact。Agent 的报告身份是**注册 session 令牌**:每次重新注册从序列 0 开始,旧 session 的迟到报告被拒绝且不会回退新 session 的观察快照。

### 失败与就绪状态

所有校验入口(`--validate`、配置 API、YAML 声明的本地 Job、编译后的 Stream)执行与真实启动相同的免副作用深度构建:未知组件、不支持的状态后端、非法图边在校验期报错,而不是运行期。dry-run 打开的 WAL 在返回前关闭,同一 redb 路径可立即被真实运行时重开。进入 `Starting` 的运行时若在 dry-run、图构建或资源连接处失败,先转为 `Failed` 再返回错误;本地 Job 构建失败会让引擎启动失败而不是带病宣布就绪。临时资源(temporary)、源和 sink 在任何任务循环启动前按依赖顺序连接,部分启动按逆序关闭已连接资源。

## API 示例

```http
POST /api/v1/jobs/validate
POST /api/v1/jobs
PUT  /api/v1/jobs/{job_id}/desired-state
GET  /api/v1/jobs/{job_id}
GET  /api/v1/jobs/{job_id}/detail
GET  /api/v1/jobs/{job_id}/versions
POST /api/v1/jobs/{job_id}/checkpoints
POST /api/v1/jobs/{job_id}/savepoints
POST /api/v1/jobs/{job_id}/upgrades
POST /api/v1/jobs/{job_id}/upgrades/{upgrade_id}/rollback
```

工作台和 API 都应先调用 `validate` 检查编译计划与节点能力，再以 `stopped` 提交并检查
`detail`。确认后才切换为 `running`。版本升级要求 Job 已停止并收敛，且选择一个已完成、
状态格式兼容的 savepoint；升级失败时保持停止状态，由操作者明确恢复旧版本。checkpoint/savepoint
的生命周期与 Job 版本、状态格式版本绑定。
