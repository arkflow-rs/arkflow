# State Management in ArkFlow

This document provides an overview of the state management capabilities in ArkFlow, inspired by Apache Flink's design patterns.

## Table of Contents

1. [Overview](#overview)
2. [Core Concepts](#core-concepts)
3. [Features](#features)
4. [Getting Started](#getting-started)
5. [Examples](#examples)
6. [Performance](#performance)
7. [Monitoring](#monitoring)
8. [Configuration](#configuration)
9. [Best Practices](#best-practices)

## Overview

ArkFlow's state management system provides:

- **Stateful Stream Processing**: Maintain state across message processing
- **Exactly-Once Semantics**: Ensure each message is processed exactly once
- **Fault Tolerance**: Automatic recovery from failures using checkpoints
- **Multiple Backends**: Support for in-memory and S3-based state storage
- **Performance Optimizations**: Batching, compression, and caching
- **Comprehensive Monitoring**: Metrics and health checks

## Core Concepts

### State Backend

The state backend determines where state is stored:

- **Memory**: Fast, in-memory storage for development and testing
- **S3**: Persistent, distributed storage for production workloads
- **Hybrid**: Combines memory speed with S3 durability

### Checkpointing

Checkpoints are consistent snapshots of application state:

- **Automatic**: Periodic snapshots at configurable intervals
- **Aligned**: Using barrier mechanisms for consistency
- **Incremental**: Only changes are saved to reduce overhead

### Exactly-Once Processing

Guarantees that each message is processed exactly once:

- **Transaction Logging**: All operations are logged before execution
- **Two-Phase Commit**: Atomic updates across multiple outputs
- **Recovery**: Restore from latest checkpoint on failure

## Features

### 1. State Operations

```rust
use arkflow_core::state::{SimpleMemoryState, StateHelper};

// Basic state operations
let mut state = SimpleMemoryState::new();
state.put_typed("counter", 42u64)?;
let count: Option<u64> = state.get_typed("counter")?;
```

### 2. Enhanced State Manager

```rust
use arkflow_core::state::{EnhancedStateManager, EnhancedStateConfig};

let config = EnhancedStateConfig {
    enabled: true,
    backend_type: StateBackendType::S3,
    checkpoint_interval_ms: 60000,
    exactly_once: true,
    ..Default::default()
};

let manager = EnhancedStateManager::new(config).await?;
```

### 3. Exactly-Once Processor

```rust
use arkflow_core::state::ExactlyOnceProcessor;

let processor = ExactlyOnceProcessor::new(
    my_processor,
    state_manager,
    "operator_id".to_string()
);
```

### 4. Two-Phase Commit Output

```rust
use arkflow_core::state::TwoPhaseCommitOutput;

let output = TwoPhaseCommitOutput::new(my_output, state_manager);
```

### 5. Performance Optimizations

```rust
use arkflow_core::state::performance::{OptimizedS3Backend, PerformanceConfig};

let perf_config = PerformanceConfig {
    enable_batching: true,
    enable_compression: true,
    batch_size_bytes: 4 * 1024 * 1024, // 4MB
    ..Default::default()
};

let backend = OptimizedS3Backend::new(s3_config, perf_config).await?;
```

### 6. Monitoring

```rust
use arkflow_core::state::monitoring::{StateMonitor, MonitoredStateManager};

let monitor = Arc::new(StateMonitor::new()?);
let manager = MonitoredStateManager::new(config, monitor).await?;

// Export Prometheus metrics
let metrics = manager.export_metrics()?;
```

## Getting Started

### 1. Add Dependencies

```toml
[dependencies]
arkflow-core = "0.4"
tokio = { version = "1.0", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
```

### 2. Basic Usage

```rust
use arkflow_core::state::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Create state manager
    let config = EnhancedStateConfig {
        enabled: true,
        backend_type: StateBackendType::Memory,
        ..Default::default()
    };
    
    let mut manager = EnhancedStateManager::new(config).await?;
    
    // Process messages
    let batch = MessageBatch::from_string("hello world")?;
    let results = manager.process_batch(batch).await?;
    
    Ok(())
}
```

## Examples

### 1. Word Count

See `examples/word_count.rs` for a complete word counting example with state management.

### 2. Session Windows

See `examples/session_window.rs` for session-based aggregations with timeouts.

### 3. Stateful Pipeline

See `examples/stateful_pipeline.rs` for integration with ArkFlow components.

## Performance

### Optimizations

1. **Batch Operations**: Group multiple operations to reduce S3 calls
2. **Compression**: Zstd compression for state data
3. **Local Caching**: LRU cache for frequently accessed state
4. **Async Operations**: Concurrent execution for better throughput
5. **Connection Pooling**: Reuse S3 connections

### Benchmarks

Typical performance characteristics:

- **Memory Backend**: >100K operations/sec
- **S3 Backend**: 1K-10K operations/sec (depending on batching)
- **Checkpoint Overhead**: <100ms for 1MB state
- **Recovery Time**: Proportional to state size

## Monitoring

### Metrics

The system provides comprehensive metrics:

- Operation counts and latency
- State size and growth
- Checkpoint duration and success rate
- Cache hit/miss ratios
- Error rates

### Health Checks

```rust
let status = manager.health_status();
if !status.healthy {
    // Handle unhealthy state
}
```

### Prometheus Integration

```rust
// Start HTTP server for metrics
let registry = manager.monitor().registry();
HttpServer::new(move || {
    App::new().app_data(registry.clone()).route(
        "/metrics",
        web::get().to(|registry: web::Data<Registry>| {
            let encoder = TextEncoder::new();
            let metric_families = registry.gather();
            Ok(web::Bytes::from(encoder.encode_to_string(&metric_families).unwrap()))
        })
    )
})
.bind("0.0.0.0:9090")?
.run()
.await;
```

## Configuration

### YAML Configuration

```yaml
state:
  enabled: true
  backend_type: "s3"
  checkpoint_interval_ms: 60000
  retained_checkpoints: 5
  exactly_once: true
  s3_config:
    bucket: "my-app-state"
    region: "us-east-1"
    prefix: "production/checkpoints"
    use_ssl: true
```

### Environment Variables

```bash
ARKFLOW_STATE_ENABLED=true
ARKFLOW_STATE_BACKEND_TYPE=s3
ARKFLOW_STATE_S3_BUCKET=my-bucket
ARKFLOW_STATE_CHECKPOINT_INTERVAL_MS=60000
```

## Best Practices

### 1. State Size Management

- Keep state small and focused
- Use TTL for temporary state
- Regular cleanup of expired state
- Partition large state by key

### 2. Checkpoint Configuration

- Balance checkpoint frequency with overhead
- Use incremental checkpoints for large state
- Monitor checkpoint duration
- Set appropriate retention policies

### 3. Error Handling

- Implement retry logic for transient errors
- Use circuit breakers for backend failures
- Log errors with sufficient context
- Monitor error rates and alert

### 4. Performance

- Use batching for high-throughput scenarios
- Enable compression for large state
- Tune cache size based on access patterns
- Monitor memory usage

### 5. Production Deployment

- Use S3 backend for persistence
- Enable monitoring and alerting
- Set up proper IAM roles
- Configure appropriate timeouts

## API Reference

### Core Types

- `SimpleMemoryState`: Basic in-memory state store
- `EnhancedStateManager`: Advanced state management
- `ExactlyOnceProcessor`: Wrapper for exactly-once semantics
- `TwoPhaseCommitOutput`: Transactional output wrapper

### Configuration

- `EnhancedStateConfig`: State manager configuration
- `S3StateBackendConfig`: S3 backend configuration
- `PerformanceConfig`: Performance optimization settings

### Monitoring

- `StateMonitor`: Metrics collection
- `StateMetrics`: Prometheus metrics
- `HealthStatus`: System health information

## Troubleshooting

### Common Issues

1. **Checkpoint Failures**
   - Check S3 permissions
   - Verify network connectivity
   - Monitor available disk space

2. **High Latency**
   - Enable batching
   - Increase cache size
   - Check S3 performance

3. **Memory Pressure**
   - Use S3 backend
   - Reduce checkpoint frequency
   - Implement state partitioning

4. **Recovery Failures**
   - Verify checkpoint integrity
   - Check backend connectivity
   - Monitor error logs

### Debug Mode

Enable debug logging:

```rust
env_logger::Builder::from_default_env()
    .filter_level(log::LevelFilter::Debug)
    .init();
```

## Roadmap

### Planned Features

1. **State Partitioning**: Automatic sharding of large state
2. **Incremental Checkpoints**: Only save changes
3. **Async State Backends**: Non-blocking state operations
4. **State Schema Evolution**: Handle changing state schemas
5. **Distributed Checkpointing**: Multi-node coordination

### Performance Improvements

1. **Native Serialization**: Faster than JSON
2. **Compression Algorithms**: Choose based on data
3. **Caching Strategies**: Adaptive cache policies
4. **Batch Sizing**: Dynamic batch optimization

## Contributing

See the main repository for contribution guidelines.

## License

Apache License 2.0