# Capability: WAL Segment Tuning

## Purpose

Provide configurable segment batching strategies for the S3 WAL backend to optimize PUT frequency, crash window, and throughput based on workload requirements. Users can choose from preset strategies (aggressive, balanced, low-latency) or customize segment parameters directly.

## ADDED Requirements

### Requirement: Preset segment tuning strategies
The S3 WAL backend SHALL support preset segment tuning strategies that control when segments are sealed and uploaded to S3. The system SHALL provide three presets: `aggressive` (high throughput, large crash window), `balanced` (default, moderate trade-offs), and `low-latency` (small crash window, high PUT frequency).

#### Scenario: Aggressive strategy for high throughput
- **WHEN** a stream configures `segment_tuning.strategy: aggressive`
- **THEN** segments are flushed with `max_entries: 10000`, `max_bytes: 10MB`, `flush_interval: 10s`

#### Scenario: Balanced strategy for moderate workloads
- **WHEN** a stream configures `segment_tuning.strategy: balanced` (or default)
- **THEN** segments are flushed with `max_entries: 1000`, `max_bytes: 1MB`, `flush_interval: 1s`

#### Scenario: Low-latency strategy for minimal crash window
- **WHEN** a stream configures `segment_tuning.strategy: low-latency`
- **THEN** segments are flushed with `max_entries: 100`, `max_bytes: 100KB`, `flush_interval: 100ms`

### Requirement: Custom segment parameters
The system SHALL allow users to override individual segment parameters (`max_entries`, `max_bytes`, `flush_interval`) when using a preset strategy, or to specify all parameters without a preset.

#### Scenario: Override preset parameters
- **WHEN** a stream configures `segment_tuning.strategy: aggressive` and `segment_tuning.max_entries: 5000`
- **THEN** segments use `max_entries: 5000` with other `aggressive` defaults (`max_bytes: 10MB`, `flush_interval: 10s`)

#### Scenario: Custom parameters without preset
- **WHEN** a stream configures `segment_tuning.max_entries: 2000`, `segment_tuning.max_bytes: 2MB`, `segment_tuning.flush_interval: 5s` without a strategy
- **THEN** segments are flushed with exactly those parameters

### Requirement: Segment flush triggers
The S3 WAL backend SHALL seal and upload a segment when any of the configured triggers are met: entry count reaches `max_entries`, byte size reaches `max_bytes`, or time since first entry reaches `flush_interval`.

#### Scenario: Entry count trigger
- **WHEN** `max_entries: 1000` and the 1000th entry is appended
- **THEN** the segment is sealed and uploaded to S3

#### Scenario: Byte size trigger
- **WHEN** `max_bytes: 1MB` and appending an entry would exceed 1MB
- **THEN** the segment is sealed and uploaded to S3

#### Scenario: Time trigger
- **WHEN** `flush_interval: 1s` and 1 second has elapsed since the first entry in the active segment
- **THEN** the segment is sealed and uploaded to S3

### Requirement: Strategy validation
The system SHALL validate segment tuning parameters at configuration load time. Invalid values (non-positive integers, zero duration) SHALL cause the configuration to be rejected with a clear error message.

#### Scenario: Invalid max_entries
- **WHEN** a stream configures `segment_tuning.max_entries: 0`
- **THEN** configuration loading fails with an error indicating `max_entries` must be positive

#### Scenario: Invalid flush_interval
- **WHEN** a stream configures `segment_tuning.flush_interval: 0s`
- **THEN** configuration loading fails with an error indicating `flush_interval` must be greater than zero

### Requirement: Crash window predictability
The system SHALL document the crash window (maximum number of entries at risk on node loss) for each preset strategy, calculated as `min(max_entries, max_bytes / avg_msg_size, flush_interval * msg_rate)`.

#### Scenario: Crash window documentation
- **WHEN** users read the S3 WAL performance documentation
- **THEN** they see crash window estimates for each strategy at different message rates (1K, 10K, 100K msg/s)
