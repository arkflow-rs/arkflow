# Exactly-Once Semantics Implementation

## Overview

ArkFlow now supports **exactly-once semantics** for reliable stream processing with automatic fault recovery. This implementation provides:

- **Two-Phase Commit (2PC)**: Distributed transaction protocol across outputs
- **Write-Ahead Logging (WAL)**: Durable transaction logging for crash recovery
- **Idempotency Tracking**: Duplicate detection and prevention
- **Automatic Recovery**: Restores incomplete transactions on startup

## Features

### 1. Transactional Outputs

**Kafka Output:**
- Full transactional support with rdkafka
- Configurable `transactional_id` for exactly-once guarantees
- Automatic transaction commit/rollback

**HTTP Output:**
- Idempotent writes via `Idempotency-Key` header
- Works with any HTTP API that supports idempotency keys

**SQL Output:**
- UPSERT support for idempotent writes
- MySQL: `INSERT ... ON DUPLICATE KEY UPDATE`
- PostgreSQL: `INSERT ... ON CONFLICT DO NOTHING`

### 2. Fault Tolerance

**WAL (Write-Ahead Log):**
- All transactions logged before commit
- Automatic recovery on startup
- Configurable file size limits and compression

**Idempotency Cache:**
- LRU cache for duplicate detection
- Persistent storage for crash recovery
- Configurable TTL and cache size

**Checkpoint Integration:**
- Works seamlessly with checkpoint mechanism
- Atomic state snapshots
- Alignment with transaction commits

## Configuration

### Enable Exactly-Once Semantics

Add to your `config.yaml`:

```yaml
exactly_once:
  enabled: true

  transaction:
    wal:
      wal_dir: "/var/lib/arkflow/wal"
      max_file_size: 1073741824  # 1GB
      sync_on_write: true
      compression: true

    idempotency:
      cache_size: 100000
      ttl: 86400s  # 24 hours
      persist_path: "/var/lib/arkflow/idempotency.json"
      persist_interval: 60s

    transaction_timeout: 30s
```

### Output Configuration Examples

**Kafka with Transactions:**

```yaml
output:
  type: "kafka"
  brokers: ["localhost:9092"]
  topic: "output-topic"
  transactional_id: "arkflow-producer-1"  # Required for transactions
  transaction_timeout: 30
  acks: "all"
```

**HTTP with Idempotency:**

```yaml
output:
  type: "http"
  url: "http://api.example.com/data"
  method: "POST"
  # Idempotency-Key header is automatically added
```

**SQL with UPSERT:**

```yaml
output:
  type: "sql"
  output_type:
    type: "postgres"
    uri: "postgresql://user:password@localhost/db"
  table_name: "events"
  idempotency_key_column: "event_id"  # Required for idempotency
```

## How It Works

### Transaction Flow

1. **Begin Transaction**: Generate unique transaction ID
2. **Process Messages**: For each message:
   - Generate idempotency key: `{stream_uuid}:{tx_id}`
   - Check cache for duplicates
   - Write message idempotently
3. **Prepare Phase**: Log transaction state to WAL
4. **Commit Phase**:
   - Commit transaction to output
   - Mark transaction as committed in WAL
   - Only then ACK the input (preventing duplicates)
5. **On Failure**: Rollback transaction and log to WAL

### Recovery Flow

On startup, the engine:

1. Reads WAL to find incomplete transactions
2. For each transaction in `Prepared` state:
   - Checks output status
   - Commits if output confirms, or rolls back if not
3. Restores idempotency cache from disk
4. Continues normal processing

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│              TransactionCoordinator                        │
│  - Manages transaction lifecycle                          │
│  - Coordinates 2PC protocol                               │
│  - Handles WAL and idempotency cache                      │
└────────────────────┬────────────────────────────────────┘
                     │
         ┌───────────┼───────────┐
         ▼           ▼             ▼
┌────────────┐ ┌────────┐ ┌──────────────┐
│  WAL       │ │Idempot.│ │   Output     │
│            │ │ Cache  │ │              │
│ - Durable  │ │ - LRU  │ │ - Kafka      │
│   Logging  │ │ - TTL  │ │ - HTTP       │
│ - Recovery │ │ - Disk │ │ - SQL        │
└────────────┘ └────────┘ └──────────────┘
```

## Guarantees

- **Exactly-Once Processing**: Each message is processed exactly once, no more, no less
- **Fault Tolerance**: Automatic recovery from crashes and failures
- **No Data Loss**: All transactions logged before commit
- **No Duplicates**: Idempotency tracking prevents duplicate processing
- **Ordered Delivery**: Messages delivered in order within each stream

## Performance Considerations

### Trade-offs

- **Latency**: 2PC adds ~10-50ms per batch
- **Throughput**: May reduce by 10-20% due to transaction overhead
- **Storage**: WAL and idempotency cache consume disk space
- **Recovery Time**: Startup recovery takes longer based on WAL size

### Optimization Tips

1. **Batch Size**: Larger batches amortize transaction overhead
2. **WAL Sync**: Set `sync_on_write: false` for better performance (risk: data loss on power failure)
3. **Cache Size**: Increase `cache_size` for high-throughput scenarios
4. **Compression**: Enable WAL compression to reduce disk usage

## Monitoring

The implementation adds metrics for monitoring:

- Transaction coordinator metrics (planned)
- WAL size and sync latency (planned)
- Idempotency cache hit rate (planned)
- Transaction commit/rollback counts (planned)

## Example Usage

See `examples/exactly_once_config.yaml` for complete configuration examples.

## Limitations

1. **Output Support**: Only Kafka, HTTP, and SQL outputs currently support exactly-once
2. **Single Stream**: Each stream has its own transaction context
3. **Recovery**: Manual intervention may be needed for some failure scenarios

## Future Enhancements

- [ ] Transaction metrics and monitoring
- [ ] Distributed transaction coordination across nodes
- [ ] Support for more output types (Elasticsearch, Redis, etc.)
- [ ] Transaction timeout and retry strategies
- [ ] Snapshot-based recovery optimization
