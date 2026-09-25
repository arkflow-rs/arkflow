# checkpoint-recovery Specification

## Purpose
TBD - created by archiving change add-distributed-stateful-streaming-runtime. Update Purpose after archive.
## Requirements
### Requirement: Checkpoints SHALL capture a consistent Job position

Each completed checkpoint SHALL identify the Job version, task assignments, complete planned task membership, source positions, operator watermark state, in-flight barrier position, state snapshots, format versions, and integrity checksums. All source positions and state snapshots SHALL represent the same acknowledged cut. When execution chains fuse logical processors, the manifest SHALL retain a deterministic mapping from every planned task to its chain snapshot and SHALL NOT omit a planned task merely because it is not a chain entry.

#### Scenario: Complete a checkpoint

- **WHEN** all participating sources and stateful tasks acknowledge the checkpoint barrier and durable state files are verified
- **THEN** the checkpoint becomes the latest valid recovery point with a durable manifest whose task set exactly matches the complete planned assignment and whose source positions and state snapshots come from one acknowledged cut

#### Scenario: Data acknowledgement is pending at barrier injection

- **WHEN** a source has delivered a record but its downstream output acknowledgement has not completed when the barrier is sealed
- **THEN** the record and its uncommitted state mutation are excluded from the checkpoint cut, and recovery replays it from the recorded source position

#### Scenario: Local execution fuses stateless processors

- **WHEN** adjacent stateless processors are represented by one execution chain and local recovery persists a checkpoint
- **THEN** the manifest records every logical planned task through the chain mapping and the checkpoint passes exact task-set validation after restart

#### Scenario: A stateless Job uses a non-default state format

- **WHEN** local recovery is enabled for a Job configured with state format `N` greater than 1 but no task has state entries
- **THEN** the checkpoint manifest records format `N` rather than the empty-snapshot default format 1

### Requirement: Incomplete checkpoints SHALL NOT be recoverable

The runtime SHALL retain the last valid checkpoint and SHALL exclude incomplete, corrupt, checksum-invalid, or task-set-incomplete checkpoints from automatic recovery. A checkpoint SHALL NOT be completed from only the online subset of an expected assignment set. Local checkpoint persistence SHALL be crash-atomic: the manifest SHALL be written through a temporary file and an atomic replace with durability barriers, so a crash during the write leaves the previous valid checkpoint intact instead of an empty or torn file. Retention SHALL NOT delete the previous valid checkpoint before the replacement manifest is durably committed.

#### Scenario: A task fails during snapshot

- **WHEN** a task cannot finish its snapshot or its state checksum does not match
- **THEN** the checkpoint is marked failed, the previous valid checkpoint remains selected, and the Job reports degraded checkpoint health

#### Scenario: An expected task is absent

- **WHEN** a checkpoint round contains no manifest entry for one planned task or assignment
- **THEN** the round remains pending or fails and no artifact from that subset is eligible for recovery

#### Scenario: A crash mid-write cannot destroy the only recoverable point

- **WHEN** the local checkpoint store is replacing the manifest and the process crashes before the write completes
- **THEN** the previous valid checkpoint remains complete and selectable for recovery, and retention has not removed it in favor of the unfinished write

### Requirement: Recovery SHALL restore deterministic state

Recovery SHALL restore state, source positions, watermarks, and task assignments from one compatible checkpoint or savepoint before processing new input. Restored source cursors, task membership, state format, effective state namespaces, and execution-chain mappings SHALL be validated together. A recovery-required start with no compatible artifact SHALL fail explicitly and MUST NOT silently initialize an empty backend. An in-process source reconnect SHALL resume from the source's acknowledged cursor — explicit assignments SHALL carry those offsets — instead of relying on `auto.offset.reset`, so an outage window is neither skipped nor fully replayed.

#### Scenario: Compute node restarts

- **WHEN** a durable stateful Job restarts after a failure and the start is recovery-required
- **THEN** the Job restores from the selected checkpoint, replays the required source range, and reports recovery progress before becoming healthy

#### Scenario: Recovery artifact is absent

- **WHEN** a recovery-required start has no completed compatible checkpoint or savepoint
- **THEN** the Agent returns a missing-recovery error and does not connect the source or start the kernel with empty state

#### Scenario: Local checkpoint contains fused chain snapshots

- **WHEN** a local Job restarts with a checkpoint whose manifest maps all planned tasks to a fused chain snapshot
- **THEN** the runtime validates the complete mapping, restores the chain state and source position, and reads new input only after restore completes

#### Scenario: Reconnect resumes at the acknowledged frontier

- **WHEN** an explicit-partition Kafka source reconnects after a `Disconnection` with acknowledged offsets in its frontier
- **THEN** the rebuilt assignment carries the frontier offsets, records produced during the outage are consumed, and records acknowledged before the outage are not reprocessed

### Requirement: Savepoints SHALL support controlled upgrades

An authorized operator SHALL be able to create, inspect, restore, and delete savepoints subject to retention and state-compatibility checks.

#### Scenario: Upgrade from a savepoint

- **WHEN** a new compatible Job version is deployed from a savepoint
- **THEN** the runtime restores compatible state, preserves source progress, and refuses deployment if migration requirements are unmet

### Requirement: Checkpoint reports SHALL use the Job state format contract
Checkpoint aggregation SHALL validate state reports against the configured Job/backend state format. A stateless source or sink report with an empty compatibility snapshot SHALL NOT make a checkpoint invalid solely because its local report uses the default format.

#### Scenario: Stateful and stateless chains use one Job format
- **WHEN** a Job backend declares a non-default state format and the checkpoint includes stateless chains with empty snapshots
- **THEN** the stateful snapshots are checked against the Job format and the stateless reports are accepted without a false format-mismatch failure

### Requirement: Recovery compatibility SHALL be evaluated consistently

Hub authorization, Agent validation, repository validation, and runtime restore SHALL apply the same compatibility result for Job identity, generation, state namespaces, operator identities, format migration, task membership, and checksums. The compatibility result SHALL be applied identically for every path that selects a recovery artifact — savepoint upgrade, version rollback, and automatic latest-checkpoint selection — so no accepted deployment reaches the Agent with an artifact the Agent will reject, and a filtered-out artifact SHALL surface to the operator rather than degrading to a silent stateless start.

#### Scenario: Hub and Agent validate the same compatible artifact

- **WHEN** the Hub authorizes a savepoint upgrade whose format migration is registered
- **THEN** the Agent accepts the same artifact under the same compatibility result and does not reject it merely because the Job version changed

#### Scenario: A version rollback applies the same compatibility result

- **WHEN** an operator rolls a Job back to an earlier version whose state format the current artifact does not declare
- **THEN** the Hub rejects the rollback with the shared compatibility result instead of accepting it and leaving the Agent to discard the artifact at recovery time

#### Scenario: A rejected artifact does not silently degrade recovery

- **WHEN** the recovery selection filters out an incompatible artifact
- **THEN** the outcome is surfaced to the operator as a rejected request or an explicit recovery error rather than reported as a successful deployment that starts without state

### Requirement: 恢复工件的任务集兼容性 SHALL 在恢复前校验

带状态恢复在选择恢复工件后 SHALL 比对工件 `task_attempts` 记录的任务集与当前编译计划的任务集；不一致（并行度或算子拓扑变更导致）SHALL 以显式配置错误失败，错误信息 SHALL 说明 keyed 状态尚不能跨并行度重分布，并给出恢复原并行度或以新 checkpoint/savepoint 重置状态两条出路。任务集一致时行为与现状逐位一致；无状态作业无恢复工件可比对，不受影响。

#### Scenario: 变更并行度后恢复显式失败

- **WHEN** 一个有状态作业在并行度 1 下产出 checkpoint 后以并行度 2 重启并尝试恢复
- **THEN** 恢复在状态还原前以配置错误失败，错误指明任务集差异（移除/新增任务）与两条出路，不出现空状态静默继续

#### Scenario: 拓扑不变时零行为变化

- **WHEN** 一个作业以与封存时相同的任务集恢复
- **THEN** 校验通过，恢复流程与现状逐位一致

#### Scenario: 无状态作业变更并行度

- **WHEN** 一个无状态作业变更并行度重启
- **THEN** 不触发该校验，作业按新并行度正常编译运行
