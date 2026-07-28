# Capability: S3 WAL Pipeline

## Purpose

Enable high-performance S3 WAL backend by allowing segment encoding and PUT operations to execute concurrently with ongoing WAL writes, eliminating blocking on the append_batch() path. This optimization reduces latency in high-QPS scenarios while maintaining data durability and recovery semantics.

## Requirements

### Requirement: Concurrent segment encoding and PUT
The system SHALL allow segment encoding and S3 PUT operations to execute concurrently with ongoing WAL writes, eliminating blocking on the append_batch() path.

#### Scenario: Seal does not block subsequent writes
- **WHEN** a segment reaches its seal threshold (max_entries or max_bytes)
- **THEN** subsequent append_batch() calls continue without waiting for the S3 PUT to complete
- **AND** the encoded segment is queued for asynchronous PUT

#### Scenario: Pipeline processes multiple segments in flight
- **WHEN** multiple segments are sealed in rapid succession
- **THEN** the system processes PUT operations concurrently
- **AND** up to N segments can be in-flight (where N is configurable or derived from channel capacity)

### Requirement: Ordered segment sealing
The system SHALL maintain segment ordering guarantees even with concurrent PUT operations.

#### Scenario: Segments are PUT in correct order
- **WHEN** segment A is sealed before segment B
- **THEN** segment A's PUT operation is initiated before segment B's PUT operation
- **AND** the manifest reflects the correct segment sequence

### Requirement: Failure handling and recovery
The system SHALL handle PUT failures without data loss and maintain compatibility with existing recovery semantics.

#### Scenario: PUT failure is surfaced to caller
- **WHEN** an asynchronous segment PUT fails
- **THEN** the error is propagated to the appropriate error handling path
- **AND** unacknowledged data remains in the WAL for retry

#### Scenario: Recovery preserves data integrity
- **WHEN** the system restarts after a mid-PUT failure
- **THEN** recovery reads both sealed segments and any in-flight segment data
- **AND** the union of manifest and LIST fallback behavior is preserved

### Requirement: Backpressure management
The system SHALL apply backpressure when the PUT pipeline is full to prevent unbounded memory growth.

#### Scenario: Channel full blocks append
- **WHEN** the PUT channel reaches capacity
- **THEN** append_batch() blocks until space is available
- **AND** the system does not accumulate unbounded in-flight segments

### Requirement: Backward compatibility
The system SHALL maintain all existing WAL configuration options and behavioral semantics.

#### Scenario: Existing configs work unchanged
- **WHEN** a user uses an existing S3 WAL configuration
- **THEN** the system functions with the new pipeline implementation
- **AND** no configuration changes are required

#### Scenario: Recovery semantics unchanged
- **WHEN** a WAL is opened after a crash
- **THEN** the recovery process (manifest GET + LIST union + segment decode) behaves identically to the current implementation
