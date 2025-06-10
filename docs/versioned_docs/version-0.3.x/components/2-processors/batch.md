# Batch

The Batch processor component allows you to accumulate messages into batches before they are sent to the next processor in the pipeline.

## Configuration

### **batch_size**

The number of messages to accumulate before creating a batch.

type: `integer`

default: `1`

## Examples

```yaml
- processor:
    type: "batch"
    batch_size: 1000
```