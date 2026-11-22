## MODIFIED Requirements

### Requirement: 无界源的异常终止 SHALL 归类为 Disconnection 而非 EOF

订阅型(无界)输入连接器在底层流意外终止(订阅被关闭、流返回错误、流以 `None` 结束)时 SHALL 返回 `Error::Disconnection` 以触发内核重连;`Error::EOF` MUST 仅用于有界源的合法耗尽。把流错误映射为 `Error::EOF` 的连接器 MUST 将其改为 `Error::Disconnection` 或结构化错误。快照型流源(file/sql)的**暂态检索失败**(IO、对象存储故障、连接器传输中断)SHALL 归类为 `Error::Disconnection`;**确定性失败**(格式/解码损坏、schema 错误)SHALL 保持 `Error::Process` 致命上报——对确定性损坏重连只会重放同一前缀后再次撞上同一损坏,形成无限重启循环。无界源在**自身被关闭/取消**的 teardown 路径(receiver 关闭、任务取消)MAY 返回 `Error::EOF`,但该映射 MUST 以 `// contract-eof: teardown` 源码标记显式声明并受一致性钉扎测试约束。

#### Scenario: NATS 常规订阅被服务端关闭
- **WHEN** NATS 常规(非 JetStream)订阅的 message 流以 `None` 结束且源声明为无界
- **THEN** `read()` 返回 `Error::Disconnection`
- **THEN** 内核进入重连循环而不是向下游广播 EOS

#### Scenario: Pulsar 订阅流以 None 结束
- **WHEN** Pulsar consumer 的消息流返回 `Ok(None)`(流结束)且 Job 未处于停止流程
- **THEN** `read()` 返回 `Error::Disconnection`
- **THEN** 内核重连重建 consumer,而不是按优雅结束广播 EOS 终结整个 Job

#### Scenario: Pulsar receiver 在停止流程中被关闭
- **WHEN** Pulsar 源的 receiver 因任务取消或组件关闭而断开
- **THEN** 该路径返回 `Error::EOF` 且携带 `// contract-eof: teardown` 源码标记
- **THEN** 一致性钉扎测试接受该映射

#### Scenario: file 输入遭遇暂态检索失败
- **WHEN** file 输入的 record-batch 流在耗尽之前因暂态故障失败(对象存储中断、IO 错误)
- **THEN** `read()` 返回 `Error::Disconnection`
- **THEN** 该错误不携带 `Error::EOF` 语义,已读前缀不作为完整数据集被确认

#### Scenario: sql 输入遭遇暂态检索失败
- **WHEN** sql 输入的查询流在耗尽之前因暂态故障失败(连接断开、传输中断)
- **THEN** `read()` 返回 `Error::Disconnection`

#### Scenario: 确定性损坏走致命路径
- **WHEN** file/sql 输入的流因格式或解码损坏(Arrow/Parquet/schema 错误)失败
- **THEN** `read()` 返回 `Error::Process`(致命,不触发重连)
- **THEN** 已读前缀不作为完整数据集被确认;重连循环不会重放同一损坏

#### Scenario: 有界源正常耗尽
- **WHEN** 有界源(file/sql/generate)的流返回 `None`
- **THEN** `read()` 返回 `Error::EOF`,内核按正常 EOS 传播

## ADDED Requirements

### Requirement: 内核错误变体契约 SHALL 成文且连接器分类 SHALL 基于结构判定

`Error` 变体的内核语义 SHALL 在 spec 与 `arkflow-core` 的变体文档注释中成文:source 读路径 `EOF`=有界源自然耗尽(优雅结束)、`Disconnection`=已建立会话的接收失败且重连有帮助(内核唯一触发重连的变体)、`Connection`=建连期或状态永久错误(致命)、其余变体=致命;sink 写路径任何错误在内核均为致命,组件内有界自重连是组件行为而非内核语义;`InputChannelClosed` 专用于 interior chain 通道关闭,连接器 MUST NOT 将通道关闭重新包装为 `Process` 字符串。连接器内的暂态/永久错误分类器 MUST 基于结构化判定(错误类别、错误码、精确短语白名单)而非过宽子串;分类不确定时 MUST 倾向暂态归类(`Disconnection`,内核有退避重连可恢复)而非永久归类(致命,Job 死亡)。

#### Scenario: 子源重连期间通道关闭
- **WHEN** `multiple_inputs` 的某个子源进入重连流程且其内部通道在此期间关闭
- **THEN** 该条件映射为 `Error::InputChannelClosed`
- **THEN** 内核按既有 interior-chain 语义处理,而不是作为 `Process` 致命失败终止 Job

#### Scenario: Redis 认证错误判永久
- **WHEN** Redis input 遇到认证/权限/命令不存在的结构化错误类别
- **THEN** 分类为永久错误(致命)
- **THEN** 消息含 "invalid" 一词但错误类别为暂态(IO/超时/断连)时,MUST 分类为暂态而非永久

#### Scenario: Redis 暂态错误判可重连
- **WHEN** Redis input 遇到 IO/超时/连接中断类错误
- **THEN** 分类为暂态,`read()` 返回 `Error::Disconnection`,内核按退避重连

#### Scenario: 变体文档注释承载语义
- **WHEN** 开发者查看 `arkflow-core` 的 `Error::EOF`/`Error::Disconnection` 定义
- **THEN** doc comment 说明各自的三分法语义与合法使用位置,并与 spec 一致

### Requirement: output 连接器的错误变体 SHALL 按暂态/确定性/建连三分类纪律映射

output 写路径的变体选择是文档级语义(内核对 sink 错误一律致命),MUST 遵守:`Disconnection`=暂态传输失败(换新连接重试可能成功);`Process`=确定性失败(SQL 语法、约束违反、编码错误);`Connection`=运行期建连失败(服务不可达、认证拒绝);`EOF` MUST NOT 出现在 output;运行期建连失败 MUST NOT 映射为 `Config`(`Config` 保留给构建期的非法配置)。output 侧与 source 建连期的"未连接"guard MUST 映射为带消息的 `Connection`;source 接收路径的会话丢失 guard MUST 保持 `Disconnection`(内核重连是恢复路径)。具备组件内有界自重连的 output(mqtt)按其既有需求执行,不强制本分类。

#### Scenario: SQL output 事务中途断连
- **WHEN** SQL output 在 begin/insert/commit 期间遭遇数据库连接断开或服务不可用(暂态)
- **THEN** 错误映射为 `Error::Disconnection`,错误消息可区分于确定性失败

#### Scenario: SQL output 确定性失败保持 Process
- **WHEN** SQL output 遭遇 SQL 语法错误、约束违反或标识符错误
- **THEN** 错误映射为 `Error::Process`(重试无帮助)

#### Scenario: SQL output 运行期建连失败
- **WHEN** SQL output 在写路径惰性建连时目标数据库不可达
- **THEN** 错误映射为 `Error::Connection` 而非 `Config`
- **WHEN** 配置中的连接串本身非法(构建期)
- **THEN** 保持 `Error::Config`

#### Scenario: pgvector output 与 SQL output 同纪律
- **WHEN** pgvector output 遭遇暂态断连或确定性失败
- **THEN** 按 SQL output 同一分类纪律映射

#### Scenario: NATS/Redis output publish 暂态失败
- **WHEN** NATS/Redis output 的 publish 因传输层暂态失败
- **THEN** 错误映射为 `Error::Disconnection`,不再映射为 `Error::Process`

#### Scenario: 未连接 guard 按位置映射
- **WHEN** output 侧或 source 建连期的 guard 判定"未连接"状态
- **THEN** 映射为带消息的 `Error::Connection`
- **WHEN** source 接收路径的 guard 判定会话丢失
- **THEN** 保持 `Error::Disconnection`

### Requirement: 连接器错误变体一致性 SHALL 由 CI 钉扎测试守护

workspace 测试 SHALL 包含对插件源码的结构性钉扎:input 源码中的每个 `Error::EOF` 出现点要么位于有界源 allowlist(file/sql/generate/memory/multiple_inputs),要么所在行携带 `// contract-eof: teardown` 标记;output 源码 MUST NOT 出现 `Error::EOF`;output 源码中 `Error::Disconnection` 仅允许出现在写路径暂态分类的 allowlist(sql/pgvector/nats/redis,随采用契约的组件扩展);`multiple_inputs` 源码 MUST 使用 `Error::InputChannelClosed`。测试失败时 MUST 列出违规文件与行,使新的变体错位在合并前可见。

#### Scenario: 无界源新增 EOF 映射被拦截
- **WHEN** 某订阅型 input 的新代码把流结束映射为 `Error::EOF` 且无 teardown 标记
- **THEN** 一致性测试失败并列出文件与行号

#### Scenario: Pulsar 流结束修复后测试绿
- **WHEN** Pulsar 的 `Ok(None)` 流结束路径映射为 `Error::Disconnection` 且 teardown 路径补齐标记
- **THEN** 一致性测试通过

#### Scenario: output 误用 EOF 被拦截
- **WHEN** 任一 output 源码出现 `Error::EOF`
- **THEN** 一致性测试失败
