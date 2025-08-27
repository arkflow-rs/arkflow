# State Management Guide

## Overview

ArkFlow provides a comprehensive state management system inspired by Apache Flink's design patterns. The system supports both in-memory and persistent (S3) state backends, exactly-once processing semantics, and transactional guarantees.

## Core Concepts

### 1. State Backend

State backends determine where and how state is stored:

- **Memory State**: Fast, in-memory storage for development and testing
- **S3 State**: Persistent, distributed storage for production workloads
- **Hybrid**: Combines memory for speed with S3 for durability

### 2. Checkpointing

Checkpoints are periodic snapshots of the entire state of the application:

- **Automatic**: Triggered at configurable intervals
- **Barrier-based**: Aligned across all operators using special markers
- **Incremental**: Only changes since the last checkpoint are saved

### 3. Exactly-Once Semantics

The system ensures that each message is processed exactly once, even in case of failures:

- **Transaction Logging**: All operations are logged before execution
- **Two-Phase Commit**: Ensures atomic updates across multiple outputs
- **Recovery**: Automatic restoration from the latest checkpoint

## Getting Started

### Basic State Operations

```rust
use arkflow_core::state::{SimpleMemoryState, StateHelper};

// Create a state store
let mut state = SimpleMemoryState::new();

// Store and retrieve typed values
state.put_typed("user_count", 42u64)?;
state.put_typed("session_data", SessionInfo { id: "123", active: true })?;

// Retrieve values
let count: Option<u64> = state.get_typed("user_count")?;
let session: Option<SessionInfo> = state.get_typed("session_data")?;
```

### Enhanced State Manager

For production use cases, use the `EnhancedStateManager`:

```rust
use arkflow_core::state::{EnhancedStateManager, EnhancedStateConfig, StateBackendType};

let config = EnhancedStateConfig {
    enabled: true,
    backend_type: StateBackendType::S3,
    s3_config: Some(S3StateBackendConfig {
        bucket: "my-app-state".to_string(),
        region: "us-east-1".to_string(),
        prefix: Some("production/checkpoints".to_string()),
        ..Default::default()
    }),
    checkpoint_interval_ms: 60000, // 1 minute
    exactly_once: true,
    ..Default::default()
};

let mut state_manager = EnhancedStateManager::new(config).await?;
```

## Advanced Features

### 1. Exactly-Once Processor

Wrap your existing processor to add exactly-once guarantees:

```rust
use arkflow_core::state::ExactlyOnceProcessor;

let processor = ExactlyOnceProcessor::new(
    my_processor,
    state_manager,
    "word_count_operator".to_string()
);

// Process messages with exactly-once guarantee
let results = processor.process(batch).await?;
```

### 2. Two-Phase Commit Output

Ensure atomic writes to external systems:

```rust
use arkflow_core::state::TwoPhaseCommitOutput;

let output = TwoPhaseCommitOutput::new(
    kafka_output,
    state_manager
);

// Write with transactional guarantees
output.write(transactional_batch).await?;
```

### 3. State Partitioning

For large-scale applications, partition state by key:

```rust
// Automatic key-based partitioning
let key = extract_key(&message);
let partition_id = hash_key(&key) % num_partitions;
let state = state_manager.get_partitioned_state(partition_id);
```

## Configuration Examples

### Development (Memory Backend)

```yaml
state:
  enabled: true
  backend_type: Memory
  checkpoint_interval_ms: 30000
  exactly_once: false
```

### Production (S3 Backend)

```yaml
state:
  enabled: true
  backend_type: S3
  checkpoint_interval_ms: 60000
  retained_checkpoints: 10
  exactly_once: true
  s3_config:
    bucket: "my-app-state"
    region: "us-east-1"
    prefix: "prod/checkpoints"
    use_ssl: true
```

## Best Practices

### 1. State Size Management

```rust
// Use TTL for temporary state
state.put_with_ttl("temp_data", value, Duration::from_hours(1))?;

// Regular cleanup
state_manager.cleanup_expired_state().await?;
```

### 2. Performance Optimization

```rust
// Batch state updates
for (key, value) in updates {
    state.batch_put(key, value)?;
}
state.commit_batch()?;

// Use async for large state operations
let future = state.async_load_large_dataset();
let dataset = future.await?;
```

### 3. Monitoring

```rust
// Track state metrics
let stats = state_manager.get_state_stats().await;
println!("Active transactions: {}", stats.active_transactions);
println!("State size: {} bytes", stats.total_bytes);
```

## Troubleshooting

### Common Issues

1. **Checkpoint Timeout**: Increase `checkpoint_timeout_ms` in configuration
2. **S3 Throttling**: Implement exponential backoff for retries
3. **Memory Pressure**: Use state partitioning or switch to S3 backend
4. **Recovery Failures**: Ensure checkpoint storage is accessible and consistent

### Debug Mode

Enable debug logging for detailed state operations:

```rust
env_logger::Builder::from_default_env()
    .filter_level(log::LevelFilter::Debug)
    .init();
```

## Migration Guide

### From Version 0.3

1. Replace direct state access with `StateHelper` trait methods
2. Add transaction context to your processors
3. Configure state backend in YAML instead of code
4. Update error handling to use `StateError`

## API Reference

### StateHelper Trait

Core methods for state operations:

- `get_typed<V>(&self, key: &str) -> Result<Option<V>, Error>`
- `put_typed<V>(&mut self, key: &str, value: V) -> Result<(), Error>`
- `remove(&mut self, key: &str) -> Result<(), Error>`
- `clear(&mut self) -> Result<(), Error>`

### EnhancedStateManager

Main state management interface:

- `new(config: EnhancedStateConfig) -> Result<Self, Error>`
- `process_batch(batch: MessageBatch) -> Result<Vec<MessageBatch>, Error>`
- `create_checkpoint() -> Result<u64, Error>`
- `recover_from_latest_checkpoint() -> Result<Option<u64>, Error>`

## Examples

See the `examples/` directory for complete working examples:

- `word_count.rs`: Basic stateful processing
- `session_window.rs`: Windowed aggregations
- `exactly_once_kafka.rs`: End-to-end exactly-once pipeline