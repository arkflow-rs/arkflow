## MODIFIED Requirements

### Requirement: Checkpoint completion semantics

A checkpoint SHALL complete only after every participating vertex whose event
loop is still running reports its `TaskCheckpointAck` (state snapshot reference,
source positions, watermark). Vertices whose event loop has already ended SHALL
be exempt from the required set and SHALL NOT receive further barrier
injections. Completion reuses the existing `CheckpointCoordinator` and manifest
contracts.

#### Scenario: All live vertices must ack

- **WHEN** one of three running vertices has not yet acknowledged
- **THEN** the checkpoint remains InProgress and no manifest is written

#### Scenario: Ended vertex is exempt

- **WHEN** a bounded source's chain ended at EOF while another source keeps flowing and a new barrier round starts
- **THEN** the round completes with reports from the live vertices and does not wait for the ended chain

#### Scenario: Stale barrier rejected

- **WHEN** an acknowledgement arrives with a different checkpoint id or generation than the in-flight barrier
- **THEN** the coordinator rejects it and the checkpoint is not completed by that ack
