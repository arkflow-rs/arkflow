# documentation-accuracy Specification

## Purpose

Keep ArkFlow's user-facing landing documentation truthful with respect to the
shipped engine. The front-door pages (`README.md`, `README_zh.md`,
`docs/docs/0-intro.md`, `docs/docs/components/0-inputs/delivery-semantics.md`,
the control-plane pages, and the new exactly-once page) SHALL NOT contradict
the authoritative capability specs in `openspec/specs/**` or the registered
component inventory surfaced by `arkflow components list`. These requirements
exist because the docs drifted badly after the reliability and control-plane
work landed; they encode the invariants so future drift is detectable.

## Requirements

### Requirement: Engine delivery-semantics claim is truthful
The introduction page (`docs/docs/0-intro.md`) SHALL NOT describe ArkFlow as
"stateless", and SHALL NOT list write-ahead-log durability, message
acknowledgment, or exactly-once output as future/upcoming capabilities. It
SHALL state the shipped semantics: at-least-once delivery is the default
(available via WAL durability on any input, and automatically when the source
is replayable), and exactly-once delivery is available opt-in for
transactional sinks.

#### Scenario: No "stateless" description
- **WHEN** the introduction page is rendered
- **THEN** it does not contain the word "stateless" as a characterization of
  the engine, and does not promise transactional or state-management
  capabilities as upcoming work

#### Scenario: Layered semantics stated
- **WHEN** a reader reads the introduction's delivery-semantics note
- **THEN** the note states at-least-once as the default and exactly-once as
  opt-in, consistent with `openspec/specs/input-durability/spec.md` and
  `openspec/specs/exactly-once-output/spec.md`

### Requirement: Delivery-semantics page acknowledges exactly-once
`docs/docs/components/0-inputs/delivery-semantics.md` SHALL NOT state that
exactly-once delivery is not provided. It SHALL state that exactly-once is
available opt-in via transactional outputs and SHALL link to the
exactly-once page.

#### Scenario: Contradictory statement removed
- **WHEN** the delivery-semantics page is rendered
- **THEN** it does not contain the assertion "Exactly-once delivery is not
  provided" or any equivalent denial of the shipped `exactly-once-output`
  capability

#### Scenario: Link to exactly-once page
- **WHEN** the delivery-semantics page discusses delivery guarantees beyond
  at-least-once
- **THEN** it links to the exactly-once page for the opt-in transactional path

### Requirement: Exactly-once page documents the honest L2 boundary
The exactly-once page SHALL document the Kafka transactional (L2) contract:
opt-in via `exactly_once: true` plus a stable, required `transactional_id`;
one ack range equals one Kafka transaction; downstream `read_committed`
consumers observe each batch atomically. It SHALL prominently document the
honest effectively-once boundary — that a crash after the producer commits
and before the source offset commits can still produce duplicates, requiring
downstream idempotency — and SHALL state that L3 true end-to-end EOS
(`send_offsets_to_transaction`) is future work. It SHALL reference
`examples/eos-kafka.yaml`.

#### Scenario: Configuration contract documented
- **WHEN** a reader opens the exactly-once page
- **THEN** it shows `exactly_once: true` and a required `transactional_id` and
  references `examples/eos-kafka.yaml` for a working configuration

#### Scenario: Honest boundary stated
- **WHEN** the page describes what exactly-once guarantees
- **THEN** it states the post-commit / pre-source-offset-commit crash duplicate
  window and recommends downstream dedup, mirroring the
  "Effectively-once boundary is honestly scoped" requirement of
  `openspec/specs/exactly-once-output/spec.md`

### Requirement: Component inventory parity across landing docs
`README.md` and `README_zh.md` SHALL enumerate the same set of components for
the primary configurable categories (inputs, processors, outputs, and
buffers), and that set SHALL equal the components registered in the engine
(as surfaced by the generated component inventory) for those categories. The
SQL input SHALL be named consistently ("SQL") across both documents.
`docs/docs/intro.md` is a compatibility route: it SHALL link to the generated
component inventory as the authoritative component listing and SHALL NOT
mention component type-names that are absent from the registry. Any component
type-name a landing doc mentions SHALL be a registered component type-name;
"join" SHALL NOT be presented as a standalone buffer type (it is a
sub-configuration of the window buffers). Codecs and temporary storage are
covered by their own reference pages and the components listing; landing docs
MAY mention them but any type-name used SHALL be registered.

#### Scenario: Registered primary components are listed
- **WHEN** a reader surveys the input/processor/output/buffer list in either
  README
- **THEN** every registered input, processor, output, and buffer is mentioned
  under its registry type-name, including Memory and Multiple Inputs (inputs),
  Pulsar (inputs), the Python processor, and InfluxDB, Redis, and SQL outputs

#### Scenario: Cross-language parity
- **WHEN** the input/processor/output/buffer lists of `README.md` and
  `README_zh.md` are compared
- **THEN** they enumerate the same component set per category, with no
  component present in one and absent in another, and the SQL input uses one
  name

#### Scenario: Intro page defers to the generated inventory
- **WHEN** a reader opens `docs/docs/intro.md`
- **THEN** the page links to the generated component inventory as the
  authoritative listing, and every component type-name it does mention is
  registered

#### Scenario: Join is not a standalone buffer type
- **WHEN** a landing doc describes the available buffer types
- **THEN** it lists memory, tumbling window, sliding window, and session
  window only, and does not list "join" as a peer buffer type

#### Scenario: Landing docs use registry type-names
- **WHEN** a landing doc names any component
- **THEN** the name matches a registry entry exactly (e.g. the conversion
  processors are named `arrow_to_json`, `json_to_arrow`,
  `arrow_to_protobuf`, `protobuf_to_arrow`), and no unregistered name such
  as a bare `protobuf` processor appears

### Requirement: Feature coverage reflects shipped capabilities
The README (`README.md`, `README_zh.md`) and `docs/docs/0-intro.md` feature sections SHALL mention the shipped headline capabilities: CDC via Debezium,
Schema Registry (Confluent wire-format Protobuf), WAL input durability,
exactly-once output, and the control-plane Hub. Mentions need not be deep but
MUST surface each capability's existence to a first-time reader.

#### Scenario: Headline capabilities discoverable
- **WHEN** a first-time reader scans the Features section of any landing doc
- **THEN** CDC/Debezium, Schema Registry, WAL durability, exactly-once, and the
  control-plane Hub are each identifiable

### Requirement: Control-plane docs agree with the Hub specs
`docs/docs/control-plane.md` and `docs/docs/deploy/control-plane.md` SHALL be
consistent with the `control-plane-hub`, `compute-node-agent`, and
`fleet-control-console` specs. In particular they SHALL cover node leases and
stale-node behavior, desired-versus-observed state, reconnect/resume, and the
operator experience for targeting a specific node — adding only what the specs
require that the pages currently omit.

#### Scenario: Stale-node operator guidance present
- **WHEN** an operator reads the control-plane docs
- **THEN** the docs explain that a stale node remains visible but cannot receive
  new commands, consistent with the `control-plane-hub` node-lease requirement

#### Scenario: No contradiction with Hub specs
- **WHEN** any assertion in the control-plane docs is compared to the three Hub
  specs
- **THEN** the doc does not assert behavior the spec does not support

### Requirement: Component reference pages SHALL use registry type-names
Component reference pages SHALL document every registered component under its
exact registry type-name, and SHALL NOT present configuration examples using
type-names that are absent from the generated inventory. Every registered
component SHALL be documented by at least one reference page (via front-matter
ownership), including conversion processors such as `arrow_to_json`.

#### Scenario: A conversion processor is documented
- **WHEN** a reader looks for `arrow_to_json` in the processors section
- **THEN** a reference page documents it under that exact type-name with a
  valid configuration example

#### Scenario: A page uses a ghost type-name
- **WHEN** a component reference page presents a configuration example whose
  `type:` value is not in the generated inventory
- **THEN** the documentation check fails naming the page and the unknown
  type-name

### Requirement: deploy 页 SHALL 如实描述存储级写围栏的当前状态
`docs/docs/operate/control-plane/deploy.md`(及 zh-Hans 镜像)SHALL NOT 声称
存储级写围栏(storage-level write fencing)属于尚未实现的后续 HA 阶段——
存储写路径已强制 lease fencing epoch(shipped,见
`openspec/specs/hub-ha/spec.md`)。页面 SHALL 记录:leader 续租推进 epoch,
过期 leader 的持久化写入被显式拒绝,拒绝以可观察的错误/事件形式暴露。

#### Scenario: 无「后续阶段」矛盾断言
- **WHEN** 阅读者查看 deploy 页的 HA 小节(en 或 zh)
- **THEN** 页面不包含「full storage-level write fencing is a later HA
  stage」或同义的未来时描述

#### Scenario: stale-leader 拒写行为可发现
- **WHEN** 阅读者查看 deploy 页的 HA 小节
- **THEN** 页面说明:失去租约的 leader 的存储写入会因 epoch 过期被拒绝,
  且该拒绝有明确的可观察表现(错误响应或事件流条目)

### Requirement: API 参考 SHALL 覆盖 Hub 机群状态与配置校验端点
`docs/docs/reference/api.md`(及 zh-Hans 镜像)的 Hub API 小节 SHALL 记录:
`GET /api/v1/status`(机群聚合状态)、`GET /api/v1/metrics` 的内容协商
(默认 Prometheus text,`format=json` 返回 `{items, aggregate}`),以及
`POST /nodes/{id}/configuration/validate` 与
`GET /nodes/{id}/configuration/diff`。方法/路径/响应形态 SHALL 与
crates/arkflow-server 的 hub 路由实现一致。

#### Scenario: Hub 端点表完整
- **WHEN** 阅读者查看 api.md 的 Hub API 小节
- **THEN** 上述四个端点均已列出且参数/响应描述与实现路由一致,页面不再把
  Hub metrics 描述为仅有 Prometheus text exposition

### Requirement: console 文档 SHALL 覆盖节点维护操作与 Audit 视图
`docs/docs/operate/control-plane/console.md`(及 zh-Hans 镜像)SHALL 记录:
概览页的节点 Drain/Maintain/Resume 操作(含各操作触发什么、何时使用、
Drain/Maintain 属于影响调度的操作)、节点维护态徽标的含义,以及 Views 表
中的 Audit 视图行(审计页展示 fleet 级操作记录)。

#### Scenario: 破坏性节点操作有文档说明
- **WHEN** 阅读者查看 console 页的概览/节点操作小节
- **THEN** 页面解释 Drain、Maintain、Resume 三个操作的作用与后果,维护
  徽标的判定来源,以及 Audit 视图在 Views 表中有一行描述

### Requirement: 取消安全契约 SHALL 有用户文档
输入的取消安全契约 SHALL 出现在用户文档(en 与 zh-Hans 均须覆盖)。语义:read()
被取消时不丢已解码消息,解码在 producer 侧完成后才可被取消边界截断(权威
语义见 `openspec/specs/input-cancellation-safety/spec.md`)。载体:concepts
(或等价的语义说明页)有取消安全小节,取消敏感的 channel 型 input 页
(mqtt、nats、redis、websocket、pulsar、http)说明或链接到该契约。

#### Scenario: 取消敏感 input 页可发现取消语义
- **WHEN** 阅读者查看上述任一 channel 型 input 的组件页
- **THEN** 页面本身或其链接目标说明:取消 read 时,已解码/已接收的消息
  不会丢失,取消在解码边界生效

### Requirement: sql processor 页 SHALL 说明计划缓存与连接池语义
`docs/docs/components/2-processors/sql.md`(及 zh-Hans 镜像)SHALL 记录:
SQL 计划被缓存(同一 query 文本复用编译计划)、`now()`/`current_date`
等时间函数在每批上新鲜求值(缓存不冻结时间函数结果)、以及连接池/
context_pool 的并发语义(多 worker 并发查询共享池)。

#### Scenario: 性能与时间函数语义可发现
- **WHEN** 阅读者查看 sql processor 页
- **THEN** 页面说明计划缓存行为与时间函数每批求值的保证,且不与实现矛盾

### Requirement: deploy 页 SHALL 说明 console token 的构建注入方式
`docs/docs/operate/control-plane/deploy.md`(及 zh-Hans 镜像)SHALL 说明
console 生产镜像的 token 注入方式:`--build-arg VITE_API_TOKEN=...` 构建参数,
`.env*` 文件被排除在构建上下文之外,以及生产环境建议改用 OIDC 认证。

#### Scenario: token 构建流程可跟随
- **WHEN** 运维者按 deploy 页构建 console 生产镜像
- **THEN** 页面给出的注入方式与 console/Dockerfile、.dockerignore 的实际
  行为一致,且提示 token 会内联进前端 bundle、生产建议 OIDC

### Requirement: memory buffer 页 SHALL 说明容量与合并失败边界
`docs/docs/components/1-buffers/memory.md`(及 zh-Hans 镜像)SHALL 记录:
capacity 为 0 的构建会被拒绝,以及 merge 失败时队列保留原批、下次重试。

#### Scenario: 边界行为可发现
- **WHEN** 阅读者查看 memory buffer 页
- **THEN** 页面说明零容量拒绝与 merge 失败保留重试两种边界行为
