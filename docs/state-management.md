# State Management and Transaction Support

This document shows how to use the state management and transaction features in ArkFlow without modifying existing trait signatures.

## Overview

The implementation provides:
1. **Metadata support in MessageBatch** - Attach transaction context and custom metadata
2. **Transaction coordination** - Two-phase commit pattern
3. **Barrier injection** - Automatic checkpoint barriers
4. **Stateful processors** - Simple state management without trait changes

## Basic Usage

### 1. Using Stateful Processor

```rust
use arkflow_core::state::StatefulExampleProcessor;
use arkflow_core::MessageBatch;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a stateful processor
    let processor = StatefulExampleProcessor::new("my_processor".to_string());
    
    // Process messages
    let batch = MessageBatch::from_string("hello world")?;
    let results = processor.process(batch).await?;
    
    // Check state
    let count = processor.get_count("unknown").await?;
    println!("Processed {} messages", count);
    
    Ok(())
}
```

### 2. Transactional Output

```rust
use arkflow_core::state::TransactionalOutputWrapper;
use arkflow_core::output::Output;

// Wrap any existing output
let original_output = MyOutput::new();
let transactional_output = TransactionalOutputWrapper::new(original_output);

// Writing now handles transactions automatically
transactional_output.write(message_batch).await?;
```

### 3. Barrier Injection

```rust
use arkflow_core::state::SimpleBarrierInjector;

// Create barrier injector with 1 minute interval
let injector = SimpleBarrierInjector::new(60000);

// Inject barriers into stream
let processed_batch = injector.maybe_inject_barrier(batch).await?;
```

### 4. Working with Metadata

```rust
use arkflow_core::state::{Metadata, TransactionContext};

// Create metadata with transaction
let mut metadata = Metadata::new();
metadata.transaction = Some(TransactionContext::checkpoint(123));

// Attach to batch
let batch_with_meta = batch.with_metadata(metadata)?;

// Extract from batch
if let Some(tx_ctx) = batch.transaction_context() {
    println!("Checkpoint ID: {}", tx_ctx.checkpoint_id);
}
```

## State Management Patterns

### Counting Pattern

```rust
pub struct CountingProcessor {
    state: Arc<tokio::sync::RwLock<HashMap<String, u64>>>,
}

impl CountingProcessor {
    pub async fn process(&self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        if let Some(input_name) = batch.get_input_name() {
            let mut state = self.state.write().await;
            let key = format!("count_{}", input_name);
            let count = state.entry(key).or_insert(0);
            *count += batch.len() as u64;
        }
        Ok(vec![batch])
    }
}
```

### Aggregation Pattern

```rust
pub struct SummingProcessor {
    sums: Arc<tokio::sync::RwLock<HashMap<String, f64>>>,
}

impl SummingProcessor {
    pub async fn process(&self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        // Extract values from batch and sum them
        // Store sums by key in self.sums
        Ok(vec![batch])
    }
}
```

## Configuration

State management can be configured per stream:

```yaml
streams:
  - input:
      type: kafka
      topics: [input-topic]
    pipeline:
      processors:
        - type: my_stateful_processor
    output:
      type: kafka
      topic: output-topic
    
    # State management configuration
    state_management:
      enabled: true
      checkpoint_interval_ms: 60000
      backend: memory
```

## Integration with Existing Components

The state management features are designed to work with existing components without requiring modifications:

1. **No trait changes** - All existing traits remain unchanged
2. **Wrapper pattern** - Add state through composition
3. **Optional features** - Enable through configuration
4. **Backward compatible** - Existing code continues to work

## Next Steps

1. **S3 Backend Implementation** - Add persistent state storage
2. **Exactly-Once Guarantees** - Full two-phase commit implementation
3. **State Partitioning** - Scale state with key partitioning
4. **State TTL** - Automatic cleanup of old state
5. **Monitoring** - Metrics for state size and checkpointing

## Example: Word Count

Here's a complete example of a word count processor:

```rust
use arkflow_core::state::StatefulExampleProcessor;
use arkflow_core::{MessageBatch, Error};
use std::collections::HashMap;

pub struct WordCountProcessor {
    word_counts: Arc<tokio::sync::RwLock<HashMap<String, u64>>>,
}

impl WordCountProcessor {
    pub fn new() -> Self {
        Self {
            word_counts: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    pub async fn process(&self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        let mut counts = self.word_counts.write().await;
        
        // Extract text from batch
        if let Ok(texts) = batch.to_binary("__value__") {
            for text_bytes in texts {
                let text = String::from_utf8_lossy(&text_bytes);
                
                // Count words
                for word in text.split_whitespace() {
                    let count = counts.entry(word.to_string()).or_insert(0);
                    *count += 1;
                }
            }
        }
        
        Ok(vec![batch])
    }

    pub async fn get_word_count(&self, word: &str) -> u64 {
        let counts = self.word_counts.read().await;
        counts.get(word).copied().unwrap_or(0)
    }
}
```