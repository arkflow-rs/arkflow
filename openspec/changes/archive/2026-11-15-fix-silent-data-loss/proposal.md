# Fix Silent Data Loss

## Why

全量代码审查(2026-11-15,CODE-REVIEW-2026-11-15.md)发现 5 个会把数据语义问题**静默化**的 P1 缺陷:两个输出连接器会静默丢行(plugin/output/mqtt.rs:223、plugin/output/nats.rs:166),两个输入连接器把**中途故障**映射为干净的流结束(input/file.rs:486-489、input/sql.rs:196-199 把 stream error 映射为 `Error::EOF`;input/nats.rs:194-199 把订阅关闭映射为 EOF),NATS JetStream 空闲时以 no-wait fetch 打满一个核(input/nats.rs:298-353),S3 WAL 恢复对损坏与缺段零告警(plugin/wal/segment.rs:104-159、plugin/wal/s3.rs:916-919)。这些问题共同点:发生时无错误、无日志、无指标,用户事后才发现数据丢失,且与仓库自己的 fail-loud 标准(payload.rs:15-21 明文禁止静默丢行;本地 redb WAL 对等损坏场景是响亮失败)相矛盾。

## What Changes

- **MQTT output**:按行 topic 短于 payload 列表时,从静默跳过改为返回命名错误(与 output/kafka.rs:441-449 的行对齐守卫对齐)。
- **NATS output**:按行 subject 短于 payload 列表时,同样返回命名错误。
- **file/sql 输入**:DataFusion/object-store/DB 流中途错误映射为 `Error::Disconnection`(触发内核重连),仅 `None`(流耗尽)保留 `Error::EOF`;取消路径保留现有语义。
- **NATS input**:常规模式订阅关闭(`None`)从 `Delivery::Err(EOF)` 改为 `Error::Disconnection`;JetStream 拉取循环为空结果引入等待(带 expires 的 pull 或退避),消除空闲忙转。
- **S3 WAL**:段解码遇到 CRC 截断时,对被静默丢弃的条目计数并 `tracing::warn`(带 segment/被丢条数);恢复读遇 manifest 引用的段 `NotFound` 时 `warn` 并计入指标,不再完全无声。

## Capabilities

### New Capabilities

(无——全部行为落入既有能力)

### Modified Capabilities

- `connector-recovery-contract`: 新增"按行目的地错位 SHALL 失败而不静默丢行"与"无界源的异常终止 SHALL 归类为 Disconnection 而非 EOF"两条需求。
- `s3-wal-pipeline`: 修改"Failure handling and recovery"——静默截断与缺段 SHALL 产生可观测告警。

## Non-goals

- 不改 MQTT/NATS output 的重连与 TLS 语义(既有 "MQTT output 写路径 SHALL 有界重连且无泄漏" 需求不变)。
- 不引入跨输出共享的 payload 对齐骨架(那是后续 `connector-shared-skeleton` 治理项;本变更只在两处补齐守卫)。
- 不改本地 redb WAL 的校验行为(已经是 fail-closed)。
- 不处理 CODE-REVIEW 报告中的其他 P1(每批一个 openspec change)。
