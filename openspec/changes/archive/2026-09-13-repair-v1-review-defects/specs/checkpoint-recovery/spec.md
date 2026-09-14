## MODIFIED Requirements

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
