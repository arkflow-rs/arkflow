# compute-node-agent Specification

## Purpose
TBD - created by archiving change make-control-plane-hub. Update Purpose after archive.
## Requirements
### Requirement: Compute node registration

The ArkFlow compute process SHALL support Agent mode with configured `hub_url`,
stable `node_id`, node credentials, and protocol version, and SHALL register
before declaring its control-plane session ready. The Hub SHALL issue the
per-session credential from a cryptographically secure random source so a
session token is not enumerable or predictable.

#### Scenario: Agent starts

- **WHEN** a compute node starts in Agent mode
- **THEN** it registers with the Hub, receives a session, and begins heartbeat
  and report loops without opening a Hub listener

#### Scenario: Session credentials are not enumerable

- **WHEN** two nodes register, and again after any node re-registers
- **THEN** each issued session token is an independent high-entropy random value with no observable sequence relationship

#### Scenario: Hub is temporarily unavailable

- **WHEN** registration or heartbeat cannot reach the Hub
- **THEN** the node keeps its local data-plane runtime policy, retries with
  bounded backoff, and exposes the disconnected state locally

### Requirement: Node heartbeat and report

The Agent SHALL periodically send authenticated heartbeat messages and full
reports containing node identity, capabilities, health, Stream snapshots,
operation snapshots, bounded events, and metrics.

#### Scenario: Healthy report

- **WHEN** the Agent sends a report within the lease interval
- **THEN** the Hub refreshes the node lease and the report's resources become
  queryable through the Hub

#### Scenario: Report does not leak secrets

- **WHEN** a node serializes configuration or capability data for a report
- **THEN** credentials and secret configuration values are redacted or omitted

### Requirement: Command polling and execution

The Agent SHALL poll for commands addressed to its node, validate expiry and idempotency, acknowledge receipt, execute supported local ControlPlane actions, and report terminal results with correlation metadata. The Agent SHALL present its session credential only in the `Authorization: Bearer` header and MUST NOT place the session credential in the request URL. The Hub SHALL continue to accept the credential from the legacy query parameter for Agents predating this change and SHALL prefer the header when both are present, recording a deprecation warning for query-parameter authentication.

#### Scenario: Execute a start command

- **WHEN** the Agent receives a valid start command for a local Stream
- **THEN** it acknowledges the command, invokes the local runtime manager, and reports observed state and terminal result to the Hub

#### Scenario: Reject an expired command

- **WHEN** a command's expiry time has passed before execution
- **THEN** the Agent rejects it without changing the Stream and reports an explicit expired outcome

#### Scenario: Upgraded Agent requires an upgraded Hub

- **WHEN** an Agent from this release polls a Hub from an earlier release that requires the query credential
- **THEN** the poll fails and the Agent retries through its normal reconnect loop, and mixed-fleet deployments upgrade the Hub before upgrading Agents

#### Scenario: Hub upgraded before the Agent

- **WHEN** an Agent that presents its credential only in the query parameter polls a Hub that prefers the header
- **THEN** the Hub authenticates the session from the query parameter and records a deprecation warning without failing the request

### Requirement: Reconnect and graceful shutdown

The Agent SHALL re-register after session loss and SHALL stop its polling loops gracefully without interrupting WAL shutdown semantics. Reconnection backoff SHALL incorporate random jitter so simultaneous session loss across the fleet does not produce synchronized re-registration bursts at the Hub.

#### Scenario: Reconnect after Hub restart

- **WHEN** the Hub restarts and the Agent reconnects
- **THEN** the Agent re-authenticates, sends a full report, and allows the Hub to reconstruct current node resources

#### Scenario: Process shutdown

- **WHEN** the compute process receives a termination signal
- **THEN** it stops accepting new commands, reports draining when possible, and shuts down local Streams using the existing WAL-safe lifecycle

#### Scenario: Session expiry behaves as session loss

- **WHEN** the Hub rejects an Agent request because the session credential has expired
- **THEN** the Agent treats the rejection as session loss, re-registers with its stable boot identity, resumes its loops, and redelivers cached terminal results through the existing command replay path

#### Scenario: Fleet-wide session loss does not stampede the Hub

- **WHEN** many Agents lose their sessions at the same moment, for example after a Hub restart
- **THEN** each Agent's retry delay is randomized within the bounded backoff window, desynchronizing re-registration attempts

### Requirement: Agent liveness is independent of command execution

The Agent SHALL continue polling heartbeat, cancellation, and required report branches while a long-running checkpoint or other command executes. Command work SHALL NOT block lease renewal for the duration of barrier or object-store I/O.

#### Scenario: Checkpoint exceeds the lease interval

- **WHEN** a checkpoint command runs longer than one Agent lease interval
- **THEN** heartbeat messages continue to refresh the lease and the Hub can still cancel or observe the Agent

### Requirement: Command failures produce terminal results

If checkpoint creation, aggregation, or any other command execution fails, the Agent SHALL send a terminal failed `CommandResult` containing the command and correlation metadata before reconnecting or ending the session. It SHALL NOT drop the dispatched command by propagating the error out of the session loop.

#### Scenario: Checkpoint aggregation fails

- **WHEN** a dispatched checkpoint commit cannot aggregate manifests
- **THEN** the Hub receives one terminal failed result for that command and can retry or mark the operation failed

### Requirement: Agent shutdown cancels command work

Agent shutdown SHALL stop accepting new commands, cancel in-flight command work when supported, and wait for or report bounded command termination while preserving local stream WAL-safe shutdown.

#### Scenario: Shutdown during checkpoint

- **WHEN** the Agent receives process cancellation while a checkpoint command is running
- **THEN** heartbeat/report draining and command cancellation proceed without leaving the local Job or command task orphaned

### Requirement: Redundant lifecycle starts settle within a bounded wait

When the Agent receives a lifecycle start for a Job whose previous kernel has not finished tearing down, it SHALL wait for the previous kernel's teardown for a bounded period. If the teardown exceeds the bound, the Agent SHALL record a warning and proceed with the new start; the redundant start SHALL then either complete or terminate with an explicit failure result, so the Hub operation record always settles instead of blocking indefinitely behind a wedged teardown.

#### Scenario: Previous kernel teardown completes in time

- **WHEN** a redundant start's previous kernel finishes its WAL-safe teardown within the bounded wait
- **THEN** the new start proceeds normally and reports its terminal result as usual

#### Scenario: Previous kernel teardown exceeds the bound

- **WHEN** the previous kernel's teardown does not finish within the bounded wait
- **THEN** the Agent records a warning, proceeds with the new start, and the Hub operation settles to a terminal outcome through completion or an explicit failure rather than remaining pending forever

### Requirement: Same-generation lifecycle starts are idempotent for live kernels and crash-visible for dead kernels

When the Agent receives a lifecycle start for a Job that already has a registered kernel at the same generation, it SHALL reply with success as a no-op if and only if that kernel is still running, without cancelling or restarting it. If the registered kernel at that generation has exited, or a higher-generation start supersedes a kernel that has exited, the Agent SHALL drain the exited kernel, release its state backend, and surface the exit outcome to the Hub through the job observation channel. The Agent SHALL NOT report success for an exited kernel as if the Job were running, and SHALL NOT silently discard the exit outcome of a replaced kernel. A superseded kernel that tears down cleanly SHALL NOT produce a crash observation.

#### Scenario: Healthy same-generation redelivery is a no-op success

- **WHEN** the Hub re-delivers a `job_start` for the generation whose kernel is registered and still running
- **THEN** the Agent reports success for the command without cancelling, restarting, or otherwise churning the running kernel

#### Scenario: Start arrives for a crashed kernel at the same generation

- **WHEN** a `job_start` arrives for a generation whose registered kernel has exited with an error before the polling drain observed the exit
- **THEN** the Agent removes the exited entry, reports the crash as a failed job observation for that generation, and starts a fresh kernel at that generation whose terminal result reflects the new start

#### Scenario: Generation bump replaces a crashed kernel

- **WHEN** a higher-generation `job_start` supersedes a registered kernel that has exited with an error
- **THEN** the crash outcome is reported through the job observation channel for the superseded generation, the superseded kernel's state backend is released, and the new generation proceeds

#### Scenario: Graceful replacement produces no crash observation

- **WHEN** a superseded kernel completes its teardown cleanly (success)
- **THEN** the Agent parks no failed observation for it and the new start proceeds normally

### Requirement: Agent SHALL sample and report node resource gauges

The Agent SHALL sample host resource gauges (CPU usage, memory used/total/available) on a fixed interval independent of heartbeat and report ticks, and SHALL merge the latest fresh snapshot into every report's existing metrics map under the fixed key vocabulary `node_cpu_usage_percent`, `node_memory_used_bytes`, `node_memory_total_bytes`, `node_memory_available_bytes`. Sampling SHALL be best-effort: any sampler failure SHALL NOT block, fail, or delay reporting, heartbeat, or command execution.

#### Scenario: Gauges ride the regular report

- **WHEN** the report tick fires after the sampler has published a fresh snapshot
- **THEN** the posted report's metrics map contains the four `node_*` keys carrying the latest sampled values, alongside the existing flow counters

#### Scenario: Sampler failure never blocks reporting

- **WHEN** the sampler cannot read host metrics (unsupported platform, read error, or task exit)
- **THEN** reports continue to be posted without the resource keys, the node's session remains online, and heartbeats and command polling continue unaffected

#### Scenario: Stale samples are omitted

- **WHEN** the latest published snapshot is older than twice the sampler interval at report time
- **THEN** the report omits all resource keys rather than sending stale values

#### Scenario: Vocabulary stays bounded

- **WHEN** resource gauges are merged into a report
- **THEN** only the four fixed keys are added, with scalar values and no per-Job, per-Stream, or per-core cardinality
