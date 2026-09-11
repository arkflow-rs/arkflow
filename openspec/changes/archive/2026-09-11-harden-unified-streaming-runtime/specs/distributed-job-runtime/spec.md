## MODIFIED Requirements

### Requirement: Compute nodes SHALL execute fenced task attempts
The runtime SHALL assign task attempts to authenticated Compute nodes and SHALL fence stale assignments using Job generation and task attempt identity. A source task's physical partition assignment SHALL be derived from the actual number of source tasks: a single source task SHALL retain the connector's all-partition subscription, while multiple source tasks SHALL receive explicit, stable physical partitions.

#### Scenario: A stale task assignment arrives
- **WHEN** a Compute node receives an assignment for an older Job generation or superseded task attempt
- **THEN** it does not start the stale task and reports the assignment as superseded

#### Scenario: A single source task starts
- **WHEN** a Job has one task for a partition-capable source configured with multiple topic partitions
- **THEN** the source task does not get pinned to physical partition 0 and consumes all partitions covered by its subscription

#### Scenario: Multiple source tasks start
- **WHEN** a Job has more than one task for the same source operator
- **THEN** each task receives its assigned physical partition and the source rejects the plan if partitioned execution is unsupported

### Requirement: Job lifecycle SHALL support recovery operations
The control plane SHALL support submitting, starting, stopping, restarting, cancelling, and observing Jobs without changing the lifecycle semantics of existing YAML Streams. A Job SHALL be reported as running only after its resources and graph have started successfully; immediate construction or connection failures SHALL be observable as a failed Job attempt.

#### Scenario: Restart a failed Job
- **WHEN** an authorized operator requests a restart for a failed Job
- **THEN** the Hub creates a new fenced task attempt and the Compute nodes restore or initialize the Job according to its recovery policy

#### Scenario: Local Job construction fails
- **WHEN** a local Job has an invalid component, state backend, temporary resource, or graph during startup
- **THEN** the Engine reports the Job as failed and does not advertise readiness as if the Job were running

## ADDED Requirements

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
