## ADDED Requirements

### Requirement: WAL recovery failure fails the stream
When a durability-enabled stream starts and WAL recovery cannot complete—either because `read_after_cursor` returns an error, or because forwarding a replayed entry into the stream's downstream channel/buffer fails—the `Stream::run` SHALL return `Err` and the stream SHALL NOT enter its normal running state. The Engine SHALL observe the error and prevent the stream (and, by existing behavior, the process) from continuing as if recovery had succeeded.

#### Scenario: WAL read failure surfaces to Stream::run
- **WHEN** a durability-enabled stream starts and `Wal::read_after_cursor()` returns `Err`
- **THEN** `Stream::run` returns `Err` without spawning the input/processor/output workers, and the WAL is closed via the existing close chain

#### Scenario: Replay forward failure surfaces to Stream::run
- **WHEN** a durability-enabled stream starts, `Wal::read_after_cursor()` returns entries to replay, and forwarding one of those entries (via `Stream::forward`) into the configured buffer or input channel returns `Err`
- **THEN** `Stream::run` returns `Err` without reading new input, without advancing the WAL cursor for the failed entry, and without spawning the input/processor/output workers past what was needed for replay

#### Scenario: Clean restart still replays nothing
- **WHEN** a durability-enabled stream starts and `Wal::read_after_cursor()` returns an empty vector (cursor at max written sequence)
- **THEN** `Stream::run` proceeds normally and reads new input

#### Scenario: Normal recovery still works
- **WHEN** a durability-enabled stream starts and `Wal::read_after_cursor()` returns entries that are all successfully forwarded
- **THEN** `Stream::run` proceeds normally, the replayed entries flow through the pipeline with `WalAck` decorators so the cursor advances on downstream confirmation, and new input is read only after replay completes
