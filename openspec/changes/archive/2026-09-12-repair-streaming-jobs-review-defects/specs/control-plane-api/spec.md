## MODIFIED Requirements

### Requirement: Standard API errors

Control API failures SHALL use a consistent JSON error envelope containing an error code, human-readable message, and optional field or Stream ID context. A failed conditional write, such as a generation or version conflict, SHALL return a non-2xx conflict status carrying the expected and observed values, and SHALL NOT be reported as a generic invalid request.

#### Scenario: Invalid request

- **WHEN** a client submits an invalid Stream ID or malformed request body
- **THEN** the response contains a non-2xx status and the standard error envelope

#### Scenario: Concurrent mutation conflicts with a conditional write

- **WHEN** a request carrying an expected generation targets a resource whose generation has since advanced
- **THEN** the response is a precondition-failed envelope naming the expected and observed generation, and the stored resource keeps the newer state

## ADDED Requirements

### Requirement: Job upgrade and rollback SHALL fence the whole written record

A Job upgrade or rollback SHALL write the Job through a conditional update that fences the generation the handler read, and SHALL NOT write the recovery/checkpoint pointer, which is owned by the checkpoint path: a checkpoint moves it without bumping the generation, so a handler copying its earlier read back would regress recovery to a pointer retention may already have deleted. The version and spec a rollback restores SHALL be written, and artifact selection SHALL filter recovery candidates by job version and state format so a pointer the restored version cannot use is reported instead of silently ignored.

#### Scenario: A checkpoint lands during a rollback

- **WHEN** a checkpoint report moves the Job's recovery pointer after a rollback handler read the Job and before it writes
- **THEN** the conditional write leaves the newer pointer untouched, and the Job's recovery selection never regresses to the pointer the handler read

#### Scenario: Concurrent desired-state change

- **WHEN** a desired-state change bumps the Job's generation between a rollback handler's read and its write
- **THEN** the write fails with a conflict, the newer desired state is kept, and the operator can retry against the fresh generation

### Requirement: Upgrade and rollback SHALL validate state compatibility equally

An upgrade and a rollback SHALL both validate the selected artifact's state format against the Job's declared state format before accepting the request, and SHALL reject an incompatible target with an explicit error. A rollback SHALL NOT be accepted when the target version's state format the Job cannot restore.

#### Scenario: Rollback to an incompatible state format

- **WHEN** an operator requests a rollback to a Job version whose declared state format differs from the artifact the Job would restore
- **THEN** the API rejects the request with the state-format incompatibility error and leaves the Job unchanged, instead of accepting it and degrading to a stateless restart

#### Scenario: Rollback to a compatible state format

- **WHEN** the requested target version declares the same state format as the Job's current artifact
- **THEN** the rollback is accepted and the Job's recovery selection uses the restored version's spec
