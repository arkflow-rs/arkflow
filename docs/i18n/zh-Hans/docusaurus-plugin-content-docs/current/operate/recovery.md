---
sidebar_position: 7
title: 恢复
description: 崩溃恢复运行手册 —— WAL 重放、检查点恢复与回滚流程。
---

# 恢复

ArkFlow 有两套相互独立的恢复机制;弄清哪一套适用于你正在处理的故障,恢复工作就成功了一半:

| 机制 | 保护范围 | 触发方式 |
|-----------|----------|---------|
| **WAL 重放** | 进入启用了持久化的流的每一条消息 | 启动时自动执行 |
| **检查点/保存点恢复** | 有状态作业状态:源位置、水位线、键控状态、窗口状态 | 重启时自动执行(检查点)或显式触发(保存点) |

## 自动发生的恢复

### 流崩溃(WAL 重放)

当流启用了持久化时,每条消息都会在进入流水线**之前**被持久化并 `fsync` 到 WAL(预写日志)。WAL
游标只会推进到最高的**连续**已确认序列号,并且只有在输出确认写入之后,源提交才会发生。

重启时,引擎会:

1. 核对已被恢复的源位置覆盖的 WAL 条目,并使游标越过这段已覆盖的前缀。
2. 在流恢复运行之前,重放已提交游标之后的每一条 WAL 条目。

结果是至少一次(at-least-once)投递:故障窗口既不会被跳过,也不会被静默丢弃——它会被重放。
当你需要端到端精确一次(exactly-once)时,重复数据的处理见[精确一次投递](/zh-Hans/docs/build/exactly-once)。

乱序与扇出确认绝不会跳过空缺:如果序列 N+1 先于 N 确认,持久化游标会停在 N 之前,直到空缺补齐。

### 有状态作业崩溃(检查点恢复)

一个已完成的检查点标识了作业版本、任务分配、源位置、水位线状态、状态快照、格式版本与完整性校验和——全部来自**同一次已确认的切分(cut)**。
在屏障注入时下游确认尚未完成的在途记录会被排除在切分之外,并在恢复时从记录的源位置重放。

计算节点重启时,作业会从最后一个**有效**检查点恢复,重放所需的源区间,并在转为健康之前上报恢复进度。
不完整、损坏、校验和无效或任务集不完整的检查点绝不会被选中;前一个有效检查点会一直保留,直到其替代者被持久化提交(清单文件通过临时文件与原子替换写入)。

源重连时会从该源已确认的游标恢复——使用显式携带的偏移量,而非 `auto.offset.reset`——因此恢复是确定性的。

## 运维操作流程

### 触发检查点或保存点

```bash
# 屏障检查点(自动恢复点)
curl -X POST -H "Authorization: Bearer $TOKEN" \
  https://hub.example/api/v1/jobs/{id}/checkpoints

# 保存点(显式创建,供升级/回滚使用)
curl -X POST -H "Authorization: Bearer $TOKEN" \
  https://hub.example/api/v1/jobs/{id}/savepoints

# 列出恢复产物
curl -H "Authorization: Bearer $TOKEN" \
  https://hub.example/api/v1/jobs/{id}/checkpoints
```

### 将作业回滚到保存点

升级要求作业处于停止且已收敛的状态。如果新版本表现异常,可回滚到兼容的保存点:

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  https://hub.example/api/v1/jobs/{id}/upgrades/{upgrade_id}/rollback
```

恢复产物与作业版本及状态格式版本绑定;不兼容的产物会在恢复前被拒绝,而不会破坏状态。

### 确认恢复已完成

1. 在作业详情上观察收敛情况:作业会在转为健康之前上报恢复进度。
2. 通过 `GET /api/v1/jobs/{id}/detail` 检查已恢复的源位置。
3. 确认检查点健康状态——某一轮快照失败后,前一个有效检查点仍会被选用,作业会上报已降级的检查点健康状态;此时应着手排查,而不是等它自愈。

## 故障场景

| 症状 | 原因 | 处理措施 |
|---------|-------|--------|
| 重启后数据重复 | 来自 WAL/源游标的至少一次重放 | 符合预期;在下游去重或启用精确一次输出。 |
| 重启后数据丢失 | 流未启用持久化 | 启用持久化;没有 WAL 时,源提交是唯一的保障。 |
| 作业卡在“恢复中” | 检查点恢复或源重放正在进行 | 检查 `detail`;大状态或很深的重放窗口需要时间。 |
| 检查点轮次持续失败 | 某个任务无法快照或校验和不匹配 | 检查节点磁盘/对象存储健康状况;最后一个有效检查点仍然生效。 |
| 回滚因不兼容被拒绝 | 保存点属于其他版本或状态格式 | 使用兼容版本历史中的产物。 |

## 优雅关闭

优雅关闭时,启用了持久化的流的 WAL 关闭不再因为某个确认恰好排在更早的
in-flight 源提交之后而直接失败流:被阻塞的确认会获得一个有界的 drain
窗口(15 秒),一旦更早的交付完成即正常提交。若窗口耗尽仍未完成,流会
抛出 `WAL closed while acknowledgement was pending` 错误,重启后的常规
WAL 重放会覆盖未确认的交付——两种情况下的 at-least-once 语义都不变。

## TLS 支持矩阵

全部网络组件的加密传输启用方式一览:

| 组件 | TLS 启用方式 |
|-----------|----------------|
| kafka(input/output) | rdkafka `security.protocol` 配置 |
| mqtt(input/output) | `tls` 配置块(`enabled`、`ca`、`client_cert`、`client_key`) |
| nats(input/output) | `tls://` URL scheme(async-nats 原生协商) |
| pulsar(input/output) | `pulsar+ssl://` URL scheme |
| redis(input/output/temporary) | `rediss://` URL scheme |
| sql(output) | sqlx TLS(连接串 `sslmode` / `ssl-mode`) |
| mongodb(output) | `mongodb+srv` / `tls=true` 连接串 |
| http(input/output) | `https://` URL(TLS 由 reqwest/hyper 终结) |
| websocket(input) | `wss://` URL |
| qdrant/milvus/pgvector 输出与 embedding/llm/vector_search/pgvector_search/milvus_search 处理器 | `https://` 端点(reqwest / sqlx TLS) |
| secret 引用 | `${secret:NAME}` 在 Hub 分发时由 Hub 环境解析;`env:`/`file:` 在节点本地解析 |

无网络能力的组件(memory、generate、drop、stdout 及 batch/json/sql/vrl/python
处理器)不存在 TLS 面。

## WAL 后端

默认使用本地文件系统存储。对于共享或远程持久化,`object_store` WAL 后端会写入 S3 兼容存储——设计与实测权衡参见
[S3 WAL 后端性能](/zh-Hans/docs/develop/s3-wal-performance)。

## 相关页面

- [WAL 持久化与性能](/zh-Hans/docs/build/wal) —— WAL 的结构与调优方式。
- [投递语义](/zh-Hans/docs/build/delivery-semantics) —— 投递保证阶梯。
- [HTTP API 参考](/zh-Hans/docs/reference/api) —— 上面出现的每一个恢复相关路由。
