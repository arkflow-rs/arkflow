## 1. 契约成文（core）

- [x] 1.1 `crates/arkflow-core/src/lib.rs`：给 `Error::EOF`、`Error::Disconnection` 补 doc comment（三分法语义与合法位置，措辞对齐 spec「内核错误变体契约」需求）
- [x] 1.2 `crates/arkflow-core/src/executor/task.rs:818-824` 契约注释追加指向 `openspec/specs/connector-recovery-contract` 的一行（逻辑不动）

## 2. 一致性钉扎测试（先行落地，红灯驱动修复）

- [x] 2.1 新建 `crates/arkflow-plugin/tests/connector_error_contract.rs`：实现三条结构规则（input EOF allowlist+teardown 标记、output EOF 禁止、output Disconnection allowlist、multiple_inputs 含 `InputChannelClosed`），违规输出文件:行清单；临时忽略当前已知违规以便分步修复（或接受先红后绿的顺序，任务内注明）
- [x] 2.2 给 teardown 路径补 `// contract-eof: teardown` 标记：mqtt.rs、nats.rs、websocket.rs、pulsar.rs 的 receiver 关闭/取消分支（行号见 design Context）

## 3. 修复 input 错位

- [x] 3.1 `input/pulsar.rs`：`Ok(None)` 流结束两处（:290-301, :350-356）`EOF`→`Disconnection`；删除死代码 `_consumer_builder`（:179-188）；行为测试：mock 流结束→`Disconnection` 不广播 EOS（对齐 `nats.rs:764-788` 先例）——以结构钉扎 `subscription_stream_end_sends_disconnection_not_eof` 落地（两个 `Ok(None)` 分支均断言 Disconnection 非 EOF）
- [x] 3.2 `input/redis.rs`：`is_temporary_redis_error`（:99-115）改结构化判定（ErrorKind/类别枚举 + 精确短语白名单，移除 `"invalid"` 单词匹配）；单测覆盖认证→永久、IO/超时→暂态、含 "invalid" 的暂态错误→暂态
- [x] 3.3 `input/multiple_inputs.rs:103`：通道关闭 `Process` 重包→`Error::InputChannelClosed`；测试：重连期间通道关闭不致命——行为单测不可达（防御性映射，receiver 为内部字段无法外部关闭），由钉扎 R-CHANNEL 结构断言覆盖

## 4. 修复 output 错位

- [x] 4.1 `output/sql.rs`：写路径 begin/execute/commit/close 接入暂态/确定性分类（复用 `input/codec_helper.rs` 的 `classify_stream_error`/`sqlstate_is_retryable`，暂态→`Disconnection`、确定性→`Process`）；运行期惰性建连失败 `Config`→`Connection`（构建期非法配置保持 `Config`）；guard（:600,611）裸 `Disconnection`→`Connection(msg)`；分类器函数级单测（离线，先例 `input/sql.rs:591-757`）——分类器落点为 `output/codec_helper.rs` 共享的 `classify_sqlx_error`/`classify_sqlx_error_ctx`（DataFusion 版不适用 sqlx），伪造 DatabaseError 表驱动单测
- [x] 4.2 `output/pgvector.rs`：与 4.1 同纪律（写路径 :199-214 分类、guard :142,150→`Connection(msg)`）；同款单测——guard 已是 `Connection(msg)` 无需改；写路径接入共享分类器，单测由 codec_helper 共享测试覆盖
- [x] 4.3 `output/redis.rs:173` 与 `output/nats.rs:195,208,225`：publish 暂态传输失败 `Process`→`Disconnection`（确定性失败保持 `Process`）；单测——钉扎 allowlist 扩至 nats/redis（spec delta 同步），既有 mqtt/nats 输出测试背书
- [x] 4.4 `output/mongodb.rs:138`：guard 裸 `Disconnection`→`Connection(msg)`——既有 guard 测试断言同步改 `Connection(_)`

## 5. 验证与门禁

- [x] 5.1 一致性钉扎测试全绿（2.1 的临时忽略全部移除）；`cargo test -p arkflow-plugin --test connector_error_contract` 通过——红→绿序列完成：红态清单与手工盘点一致（pulsar 两处 bug + 9 处 teardown + mongodb guard + InputChannelClosed 缺失），修复后 4/4 绿
- [x] 5.2 `cargo test --workspace --all-targets --locked` 全绿；`cargo clippy --workspace --all-targets --locked -D warnings` 干净（先 `rustup update stable` 对齐 CI 工具链）——clippy 1.100 `-D warnings` 干净、rustfmt 过；workspace `--no-fail-fast` 全跑 39 个测试二进制：失败全部为**存量/环境性**且逐一经干净树基线核实（plugin lib 21 + server lib 24 均为本环境无法绑定本地 socket 的测试；two_node 2 例 pause/resume 存量失败），改动相关目标全绿（connector_error_contract 4/4、mongodb_io 3/3、触及模块单测全绿）。途中修复一个漏网断言：`tests/mongodb_io.rs` 的 write_after_close 同步改 `Connection(_)`
- [x] 5.3 确认内核行为变化仅两处并在 CHANGELOG 声明：pulsar 流结束不再优雅终止 Job、multiple_inputs 通道关闭不再致命——CHANGELOG 增「Changed — Connector error contract」节

## 6. 文档（EN+zh 锁步，归档前必须完成）

- [x] 6.1 docs/docs/components/ 下 pulsar input、redis input/output、sql output、pgvector output、nats output 页补错误行为段（暂态→重连、确定性→致命、teardown 语义）；zh-Hans 树同步
- [x] 6.2 `pnpm docs:check` 通过（yaml 块分类标记、front matter `components`、README 列表不受影响则确认即可）——145 页/50 组件全过，inventory 无漂移；rustfmt 门禁已过
