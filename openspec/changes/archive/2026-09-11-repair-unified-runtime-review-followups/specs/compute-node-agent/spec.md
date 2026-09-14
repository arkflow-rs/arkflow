## ADDED Requirements

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
