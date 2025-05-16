# Memory

The Memory input component reads data from an in-memory message queue.

## Configuration

### **messages**

The initial list of messages in the memory queue (optional).

type: `array` of `string`

## Examples

```yaml
- input:
    type: "memory"
    messages:
      - "Hello"
      - "World"
```