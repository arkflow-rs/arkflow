## MODIFIED Requirements

### Requirement: Effectively-once boundary is honestly scoped
The Kafka transactional output (L2) SHALL eliminate in-transaction partial writes and zombie duplicates. It SHALL NOT guarantee the absence of duplicates when a crash occurs after the producer transaction is committed and before the source offset is committed. Such residual duplicates MUST be absorbed by downstream idempotency (dedup key, business idempotency) or by a future L3 mechanism that commits the source offset inside the producer transaction. This limitation SHALL be documented. L3 (Kafka `send_offsets_to_transaction`) is delivered for Kafka→Kafka flows whose paired input and output run in one process: a process-internal group registry hands the input's consumer-group metadata to the transactional output, which folds the covered source offsets (derived from batch metadata columns) into its producer transaction; the paired input suppresses its own broker offset stores (see the "Kafka 输出 SHALL 支持事务内源位点提交（L3）" requirement). Cross-process pairings (distributed jobs splitting input and output across nodes) are covered by **placement affinity, not by a cross-node protocol**: the Kafka input and output declare a shared placement affinity key (see the "L3 事务配对 SHALL 声明放置亲和并拒绝跨进程拆分" requirement and `split-placement`), the placement planner pins the paired tasks onto one node while intermediate operators MAY span nodes via the network data plane, and a pairing actually split across nodes is rejected — at Hub placement validation and again at Agent graph build — with an explicit configuration error before any task runs. SQL outputs cover the aggregated-batch window transactionally instead (see `sql-output`).

#### Scenario: Post-commit pre-offset-commit crash produces duplicates
- **WHEN** the producer commits the transaction and the process crashes before the source offset is committed by auto-commit
- **THEN** on recovery the source redelivers the range, a new producer writes it again, and a `read_committed` downstream consumer observes duplicate rows — which downstream idempotency MUST absorb

#### Scenario: split placement keeps the L3 pair on one node
- **WHEN** a distributed job with a `transactional_offsets` Kafka input and a Kafka output whose `offset_commit_group` names that input's group runs under `placement: split` across two nodes
- **THEN** the paired input task and output task are placed on the same node (their in-process registry pairing works unchanged), while operators between them MAY be placed on the other node behind remote data edges

#### Scenario: a pairing split across nodes never runs
- **WHEN** an assignment places the L3 input task and its paired output task on different nodes (a stale or hand-crafted task map)
- **THEN** the assignment is rejected with a configuration error naming the affinity group and both tasks' nodes, before any task attempt starts

## ADDED Requirements

### Requirement: L3 事务配对 SHALL 声明放置亲和并拒绝跨进程拆分
声明 `transactional_offsets: true` 的 Kafka 输入 SHALL 通过其 builder 的放置亲和钩子返回以 `kafka-txn:<consumer_group>` 为 key 的亲和标识；配置了 `offset_commit_group` 的 Kafka 输出 SHALL 对指名组返回同一 key。亲和推导只读组件配置（不构建连接）。注册表中无该 `input_type` / `output_type` 的类型不贡献亲和——该类型在任何节点都无法构建，fail-closed 由既有构建路径承担。Agent 建图期 SHALL 校验亲和组共位：跨节点的亲和组以指名组 key 与两端任务/节点的配置错误拒绝，不进入运行。运行期兜底不变：写入期组无活输入（`no live input frontier in this process`）与启动期配对校验照旧 fail-closed，其错误信息 SHALL 提示分布式部署下该错误意味着 assignment 未经亲和校验。

#### Scenario: builder 亲和声明只依赖配置
- **WHEN** 依据配置（`transactional_offsets: true` 的输入、设置 `offset_commit_group` 的输出）查询放置亲和钩子
- **THEN** 两侧对同一 consumer group 返回相同的亲和 key，且不建立任何 broker 连接

#### Scenario: Agent 拒绝拆分的亲和组
- **WHEN** Agent 收到的 task map 把亲和组两端任务分到不同节点并尝试 `build_subgraph`
- **THEN** 图构建以配置错误失败，错误指名组 key 与两端任务所在节点，作业不在该 Agent 上启动
