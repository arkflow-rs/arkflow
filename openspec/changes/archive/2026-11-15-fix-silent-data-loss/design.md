# Design: Fix Silent Data Loss

## Context

六处修复全部是**补齐守卫/重分类错误**,不引入新抽象。依据代码审查报告(CODE-REVIEW-2026-11-15.md §2 P1 #2/#3/#4/#5/#6/#7)。

## Goals / Non-Goals

- Goals: 六处静默点全部变为响亮失败或有界等待;每处有钉扎测试。
- Non-Goals: 共享 payload 对齐骨架;MQTT/NATS 重连语义变更;本地 WAL 改动。

## Decisions

### D1. MQTT output:目的地错位 → 命名错误(对齐 Kafka 兄弟)

`output/mqtt.rs:223` 的 `if let Some(topic_str) = topic.get(i)` 改为先做总长度校验:`payloads.len() != topics.len()`(或 topic 向量短于 payload)时在进入循环前返回
`Error::Process(format!("per-row topic count {} does not match payload count {}; a shorter destination vector would silently drop rows", …))`。
采用与 `output/kafka.rs:441-449` 相同的判定点(写循环之前),使失败发生在任何 publish 之前,无需补偿。

### D2. NATS output:同 D1

`output/nats.rs:166` 的 `let Some(subject_str) = subject.get(i) else { continue; }` 改为循环前长度校验 + 命名错误。` JetStream context 缺失时的静默成功形状(nats.rs:183-198)保持不变(超出本批 Non-goals)。

### D3. file/sql 输入:流错误重分类为 Disconnection

两个文件结构相同(`select!` 分支里 `result.map_err(|e| … Error::EOF)`):
- `Err(e)` → `Error::Disconnection`(保留 cause 日志;`error!` 降为 `warn!`,因为重连会重试)。
- `Ok(None)`(流耗尽)→ `Error::EOF` 不变。
- 取消分支(当前返回 `Err(Error::EOF)`)→ 保持 `EOF`:内核把 read() 的取消丢弃当作常规轮询,该路径本就不传播 EOS——见 task.rs:765 注释;实际广播 EOS 的是 channel 关闭路径。此处不动,仅在测试中钉住"取消不产生数据丢失"现状。

风险:Disconnection 会触发重连,重连=重建 SessionContext/连接池并从头执行查询(快照语义,一次性读取),重复输出整个前缀。file/sql 均为一次性快照源,当前错误即终止;改为重连后,前缀重复交付由 at-least-once 契约吸收。这个语义变化写入 spec 的"恢复场景"。

### D4. NATS input:订阅关闭 → Disconnection

`input/nats.rs:194-199` 的 `None => Delivery::Err(Error::EOF)` 改为 `Delivery::Err(Error::Disconnection)`。与 websocket/redis 兄弟一致。

### D5. NATS JetStream:空结果有界等待

`input/nats.rs:298-353` 的 `consumer.fetch().messages()`(async-nats 0.50 的 Batch 路径,no_wait=true、expires=None)改为显式构造:
`consumer.fetch().max_messages(…).expires(Some(Duration::from_millis(500))).messages().await`
若该 Builder 不支持 expires(以 vendored crate 为准,`pull.rs` 的 `Batch` 路径支持 `expires`),退回方案:空结果时 `tokio::time::sleep(500ms)`。实现时按编译结果二选一,测试钉"相邻 fetch 间存在等待"。

### D6. S3 WAL:截断/缺段告警

- `plugin/wal/segment.rs` 解码循环:检测到 CRC 不匹配而截断时,收集 `(segment_key, kept_count, dropped_count)`;由调用方(`s3.rs` 恢复读取处)发 `warn!`。为保持 segment.rs 纯解码,在返回值/结构上携带截断信息(新增字段或独立返回值),不在解码函数内打日志。
- `plugin/wal/s3.rs:916-919` 的 `NotFound => continue` 前加 `tracing::warn!(segment = %key, "manifest-referenced sealed segment is missing; unacked replay data in it is skipped")`。

## Risks / Trade-offs

- D3 的重连语义对一次性快照源意味着前缀重放(at-least-once 契约内;现状是"假完成",更糟)。
- D5 若驱动不支持 expires 需退避方案,空转消除效果等同(500ms 上界)。
- D6 的 warn 在损坏频繁时会刷日志——可接受(损坏本身就该引起注意),不做速率限制(与本地 WAL fail-closed 的响亮相比是降级,不是噪声)。

## Migration Plan

无配置变更,无数据迁移。S3 恢复告警是纯可观测性增量。MQTT/NATS 错误路径此前不可能成功送达(跳过即丢),现在失败——下游 ack 由内核 abort/重放。

## Open Questions

无——按"你决定=授权拍板"推进。
