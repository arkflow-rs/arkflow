# Tasks: Fix Silent Data Loss

## 1. 输出连接器行对齐守卫

- [x] 1.1 MQTT output:写循环前校验 per-row topic 数量与 payload 数量一致,错位返回命名 `Error::Process`(plugin/output/mqtt.rs)
- [x] 1.2 NATS output:同上校验 per-row subject(plugin/output/nats.rs,`check_subject_alignment`)
- [x] 1.3 两个 output 各加一个错位拒绝测试(构造 topic/subject 短于 payload 的批次,断言错误信息含两个数量、broker 无发布、ack 未结算)

## 2. 输入连接器错误重分类

- [x] 2.1 file 输入:流 `Err(e)` 分支映射 `Error::Disconnection`(保留 cause 日志);`Ok(None)` 保留 EOF(plugin/input/file.rs)
- [x] 2.2 sql 输入:同上(plugin/input/sql.rs)
- [x] 2.3 两个输入各加一个测试:流中途错误 → 断言返回 Disconnection 且非 EOF(MidStreamFailure 双测,注入 SendableRecordBatchStream)
- [x] 2.4 NATS input:常规订阅 `None` → `Error::Disconnection`(plugin/input/nats.rs)+ 结构性钉扎测试

## 3. NATS JetStream 空闲等待

- [x] 3.1 查证 async-nats 0.50 pull builder 支持 expires(FetchBuilder::expires,长轮询语义);JetStream 循环改为 `.expires(500ms)`(plugin/input/nats.rs:298-353)
- [x] 3.2 钉扎测试:`jetstream_fetch_loop_pins_a_bounded_expires` 结构性断言(include_str! 模式,沿用 input/kafka.rs 的 drain-engine 守卫先例)

## 4. S3 WAL 恢复可观测性

- [x] 4.1 segment.rs 解码携带截断信息(`DecodedSegment.truncation: Option<SegmentTruncation>`,offset/字节数/类别),不在解码函数内打日志(plugin/wal/segment.rs)
- [x] 4.2 s3.rs 恢复读取:截断发 warn(segment/丢弃字节数);NotFound 段发 warn(对象键);三个解码调用点全覆盖(plugin/wal/s3.rs)
- [x] 4.3 测试:撕裂尾/段中 CRC 损坏/完整段三例断言 truncation 字段;告警函数是截断信息的薄包装(结构可断言)

## 5. 验证与文档

- [x] 5.1 cargo test -p arkflow-plugin(全量 lib,881 通过)通过
- [x] 5.2 cargo clippy -p arkflow-plugin --all-targets 零警告;cargo fmt 通过
- [x] 5.3 文档:mqtt/nats 输出页(EN + zh-Hans)补按行目的地对齐错误行为;file/sql/nats-input 页未描述错误终止语义,无需改动
