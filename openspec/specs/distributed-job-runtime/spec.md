# distributed-job-runtime Specification

## Purpose
TBD - created by archiving change add-distributed-stateful-streaming-runtime. Update Purpose after archive.
## Requirements
### Requirement: Job plans SHALL have stable distributed identities

The runtime SHALL represent a Job as a versioned DAG with stable operator, task, subtask, partition, and key-group identities. Any fused execution-chain representation used for local recovery SHALL retain a deterministic mapping to every logical planned task so checkpoint membership remains complete.

#### Scenario: Compile a valid Job plan

- **WHEN** a valid SQL or Job specification is submitted
- **THEN** the system produces a versioned plan whose nodes, edges, partitions, and stateful operators have stable identities

#### Scenario: Persist a fused execution chain

- **WHEN** adjacent stateless processors are fused into one execution chain for a local Job checkpoint
- **THEN** the checkpoint records the chain identity and a mapping covering every logical task in the plan

### Requirement: Compute nodes SHALL execute fenced task attempts
The runtime SHALL assign task attempts to authenticated Compute nodes and SHALL fence stale assignments using Job generation and task attempt identity. A checkpoint SHALL be accepted only when every planned task in the execution model participates in the same valid cut.

#### Scenario: A stale task assignment arrives
- **WHEN** a Compute node receives an assignment for an older Job generation or superseded task attempt
- **THEN** it does not start the stale task and reports the assignment as superseded

#### Scenario: A checkpoint task is missing
- **WHEN** one planned task does not produce a checkpoint manifest for a round
- **THEN** the checkpoint is rejected or remains incomplete and cannot be selected for recovery

### Requirement: Job execution SHALL provide bounded backpressure

The runtime SHALL propagate input, operator, network, and output pressure through the Job DAG and SHALL expose a bounded state when a downstream task cannot make progress.

#### Scenario: A downstream task is unavailable

- **WHEN** a downstream task stops consuming data
- **THEN** upstream tasks stop or reduce dispatch within configured bounds and the Job observation reports the blocked edge and pressure reason

### Requirement: Job lifecycle SHALL support recovery operations
The control plane SHALL support submitting, starting, stopping, restarting, cancelling, and observing Jobs without changing the lifecycle semantics of existing YAML Streams. Recovery SHALL restore source positions and every physical event-time partition watermark from one acknowledged checkpoint cut.

#### Scenario: Restart a failed Job
- **WHEN** an authorized operator requests a restart for a failed Job
- **THEN** the Hub creates a new fenced task attempt and the Compute nodes restore or initialize the Job according to its recovery policy

### Requirement: Job resources SHALL be connected before task execution
The unified runtime SHALL connect all temporary resources, inputs, and outputs required by a Job before spawning its task event loops. Partial startup SHALL close every resource already opened and return the startup error.

#### Scenario: Temporary processor resource is used
- **WHEN** a processor resolves a Redis or other temporary resource during its first `get` call
- **THEN** the resource has already completed `connect()` and the processor does not receive a false disconnection error

#### Scenario: Resource connection fails
- **WHEN** one resource fails to connect after another resource has connected
- **THEN** the runtime closes the connected resources in reverse order and reports the failure before accepting input

### Requirement: Stream processor concurrency SHALL be preserved
The unified execution graph SHALL honor the configured Stream processor worker count (`pipeline.thread_num`) without changing a single source task into a single Kafka partition assignment. Processor worker pools SHALL remain bounded, cancellable, and ordered where the legacy Stream contract requires ordered output; stateful/window state commits SHALL remain serialized by their execution epoch.

#### Scenario: Stream requests multiple processor workers
- **WHEN** a Stream config sets `pipeline.thread_num` greater than 1
- **THEN** the compiled runtime creates the configured number of eligible processor workers while retaining the source and sink topology and bounded backpressure

### Requirement: Partitioned edges preserve complete downstream ownership

For a partitioned edge, graph construction SHALL connect each upstream task to the complete eligible set of downstream subtasks. Dispatch SHALL select the downstream task using the `JobPlan` key-group ownership mapping rather than a same-subtask shortcut.

#### Scenario: A key maps to another downstream subtask

- **WHEN** a source subtask emits a keyed record whose planned key-group owner is downstream subtask 1 while the source subtask is 0
- **THEN** the record is delivered to downstream subtask 1 and its keyed state is not split into the source-indexed task

### Requirement: Partitioned routing remains stable across source partitions

Partitioned routing SHALL produce the same downstream task for the same key regardless of which physical source partition or upstream subtask delivered it.

#### Scenario: Same key arrives from two source partitions

- **WHEN** identical keys arrive through two physical source partitions assigned to different upstream tasks
- **THEN** both records are routed to the one planned key-group owner and update one logical keyed state namespace

