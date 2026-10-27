# Design: Kafka L3 split-placement affinity（共位钉扎）

## Context

L3 的进程内桥（`kafka_txn.rs`）以三个查表成立：输出写入期取组元数据与前沿、启动期配对校验、注册表弱引用生命周期。split 放置按物理 task 顺序轮转（`job.rs:1085` `node_ids[index % node_ids.len()]`），source（输入）排最前、sink（输出）排最后，≥2 节点必然拆开配对，输入节点启动即被 `validate_pairings` 拒绝——这就是"跨进程配对 fail-closed"边界的机制。

规划器已有同构先例：`JobSpec::side_edges()` 从算子配置推导旁路边约束（含读 `__arkflow_error_sink` 配置标记，`job.rs:429-437`），Hub 在 `assignments_for_nodes` 里校验（`job.rs:1097`），Agent 在 `build_subgraph` 里用同一 `validate_side_edge_nodes` 复校（`executor/graph.rs:438-441`）。Hub 进程与 Agent 同二进制、`main.rs` 先 `initialize()` 注册全部 builder（`main.rs:23`），因此核心 builder 注册表（`INPUT_BUILDERS`/`OUTPUT_BUILDERS`）在两侧都可用。

前置可行性核实（阻断了"必须跨节点传 CGM"的假设）：librdkafka（rdkafka-sys 4.10.0 / librdkafka 2.12.1）暴露 `rd_kafka_consumer_group_metadata_{group_id,generation_id,member_id,group_instance_id}` 访问器与 `rd_kafka_consumer_group_metadata_new_with_genid` 构造器，组元数据可明文跨进程重建。真正无法便宜跨节点的是**前沿**（contiguous acked frontier，`write_batch` 每次事务提交前都要钳制），它是热路径、随 ack 实时演进；经 Hub 中转违反"Hub 断连时数据面不受影响"的架构约束，经数据面边捎带则要穿透中间链的转发语义——两者都是新的协议面。

## Goals / Non-Goals

**Goals:**

- `placement: split` 的 Job 能使用 L3：配对输入/输出任务由规划器钉到同一节点，中间算子照常跨节点经数据面远程边传输；不新增任何跨节点协议。
- 钉扎确定性：assignment 仍是「任务顺序 + 节点列表」的纯函数，Hub 增量重放置（原位替换失败槽）无需感知亲和即可保持配对完整。
- 双侧 fail-closed：Hub 放置期与 Agent 建图期各校验一次；配对被拆开的 assignment 在任何任务启动前被拒。
- 亲和机制通用：核心只定义 key 语义，不认识 Kafka；任何组件对可用同一钩子声明共位需求。

**Non-Goals:**

- **不做跨节点 L3 元数据协议**（组元数据/前沿经 Hub 或数据面传递）。理由：前沿热路径 + 数据面独立性约束 + 协议面成本，远超"输入输出同节点"的放置代价；CGM 可跨进程重建的可行性结论留档于此，供未来真需要时立项。
- 不改变 `colocated` 放置的既有行为（接受的 assignment 逐位不变）。
- 不支持多读者输入（L3 输入仍是单 subtask；parallelism>1 时 assign 模式与 L3 的既有拒绝不变）。
- 不做资源感知的亲和调度（亲和组锚点由 plan 顺序决定，不参与 `resource-aware-placement` 的压力重平衡决策；重平衡以 Job 整体迁移，天然保持亲和）。

## Decisions

### D1：亲和经 builder 钩子声明，而非核心硬编码插件字段

`InputBuilder` / `OutputBuilder` 增加默认方法：

```rust
fn placement_affinity(&self, config: &Option<serde_json::Value>) -> Result<Option<String>, Error> {
    Ok(None) // 默认无亲和
}
```

Kafka 输入在 `transactional_offsets: true` 时返回 `Some("kafka-txn:{consumer_group}")`，输出在 `offset_commit_group` 设置时返回同 key。

- 备选（否决）：核心在 `JobSpec::affinity_groups()` 里读 `source.config["transactional_offsets"]` 等 Kafka 字段——把插件配置词法写死进核心，每新增一个带亲和需求的组件都要改核心；旁路边的 `__arkflow_error_sink` 先例是编译器注入的标记，而用户手写的 Kafka 配置没有注入点。
- 备选（否决）：在 `JobSpec` 上新增用户可见的 `affinity` 字段——把内部一致性约束暴露给用户手动维护，配对写错（组名拼写不一致）只能运行期发现；钩子从同一配置推导，错误在放置期即报。
- 钩子签名与 `build` 一致地拿 `config: &Option<Value>`；实现各自反序列化自己的配置结构。**错误向上传播**（配置畸形 → 放置失败，fail-closed）。

核心侧新增解析入口（与 `register_input_builder` 同居 `input/mod.rs` / `output/mod.rs`）：

```rust
pub fn placement_affinity(type_name: &str, config: &Option<Value>) -> Result<Option<String>, Error>
```

注册表中**不存在**的类型返回 `Ok(None)` 而非错误：该类型无法在任何节点构建，建图路径已 fail-closed；这也使核心单测里未注册的合成类型不受影响。

### D2：`JobSpec::affinity_groups()` 与旁路边同形

```rust
pub struct AffinityGroupSpec { pub key: String, pub operator_ids: Vec<String> }
```

对每个 source（`input_type` + `config`）与 sink（`output_type` + `config`）调用钩子，按 key 归并（BTreeMap，输出按 key 排序，确定性）。operator（中间算子）不参与——亲和只钉配对两端。与 `side_edges()` 一样是惰性纯函数、不加进 `JobSpec` 序列化面。

### D3：钉扎 = 轮转后的第二个 pass，锚点取组内首个成员的落位

`assignments_split` 在轮转生成 assignments 后：

1. 按 `self.tasks` 顺序找每个亲和组的第一个成员任务，取其轮转落位为锚节点；
2. 把该组全部成员算子的所有 subtask 任务改派到锚节点（TaskAttempt 的 `id` 含 node_id，直接重生成）；
3. 先 `validate_affinity_assignments`（防御性，钉扎后不应失败），再走既有 `validate_side_edge_assignments`。

- 为什么锚点取首个成员（即 source）的轮转节点：纯函数、零额外选择逻辑；与增量重放置兼容——Hub 替换失败槽只改节点列表，重算时锚点随新列表确定，配对整体跟随。
- 为什么钉扎放在旁路边校验**之前**：钉扎可能移动 sink，旁路边约束应对钉扎结果校验；两者冲突时（钉扎拆开 error 边）按旁路边规则拒绝整份 assignment（spec 场景已覆盖），不静默。
- `colocated` 分支：assignment 计算不变，但 `assignments_for_nodes` 对两种策略都追加 `validate_affinity_assignments`——唯一新拒绝的是"配对两端不连通且分落不同节点"，该组合此前在 Agent 启动期必失败，只是把失败提前到放置期。

### D4：双侧校验 API 镜像旁路边

```rust
pub fn validate_affinity_assignments(&self, assignments: &[TaskAttempt]) -> Result<(), Error>
pub fn validate_affinity_nodes(&self, node_by_task: &BTreeMap<String, String>) -> Result<(), Error>
```

语义：组内任一成员算子的任一 subtask 与组锚（或组内其他成员任务）不同节点即拒，错误指名 key、两端任务 id 与节点；缺 assignment 的成员任务 fail-closed（与旁路边一致）。Agent `build_subgraph` 在 `validate_side_edge_nodes` 旁并列调用 `validate_affinity_nodes`（`graph.rs:439` 处）。

### D5：运行期兜底与错误文案

`kafka_txn::validate_pairings` 与输出写入期 `no live input frontier in this process` 错误保持不变（最后一道防线），仅在文案中追加一句：分布式部署下该错误意味着 assignment 未经亲和钉扎/校验（正常路径不可能到达）。不新增监控面。

## Risks / Trade-offs

- [钉扎削弱 split 的负载分散] → 影响面仅限 L3 配对两端（source 链与 sink 链各自内部仍可并行/分布）；对无 L3 的 Job 零影响。文档明示"启用 L3 即接受输入输出共位"。
- [钉扎后旁路边冲突导致整份 assignment 被拒] → 该组合（error 路由目标同时是 L3 sink）本身罕见；错误信息同时指名亲和组与旁路边，操作者可调整。不做自动消解。
- [Hub 与 Agent 注册表漂移（自定义构建裁掉 Kafka 插件）导致 Hub 不派亲和、Agent 拒绝] → 行为仍是 fail-closed（Agent 放置校验/启动校验拒绝），且两侧同二进制的标准部署不漂移。
- [核心单测无法用真实 Kafka builder] → 核心测试注册一个仅实现 `placement_affinity` 的 stub builder；插件 crate 另用真实 Kafka builder（纯配置推导，无需 broker）补端到端放置断言。
- [`validate_pairings` 的进程全局状态在多 Job 共存时互相可见] → 既有语义不变；本变更不扩大其作用面。

## Migration Plan

无数据迁移、无配置变更：新行为由既有配置字段（`transactional_offsets` / `offset_commit_group` / `placement: split`）推导。升级后原先"必失败"的 split+L3 组合变为可运行；原先侥幸未触发校验路径的手工 assignment（若存在）会被更早拒绝。回滚即回退二进制，无持久化痕迹。

## Open Questions

（无——锚点选择、校验时机、非目标边界均已在上述决策中定死；实现期若发现 `tasks` 顺序与 sources→operators→sinks 的假设在某个 JobSpec 构造路径不成立，以 `self.tasks` 实际顺序为准并在钉扎注释中说明。）
