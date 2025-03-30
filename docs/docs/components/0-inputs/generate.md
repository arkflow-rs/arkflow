# Generate

## Configuration

### **context**

The context is a JSON object that will be used to generate the data. The JSON object will be serialized to bytes and sent as message content.

type: `string`

### **count**

The total number of data points to generate. If not specified, the generator will run indefinitely until manually stopped.

type: `integer`

optional: `true`

### **interval**

The interval is the time between each data point.

type: `string`

example: `1ms`, `1s`, `1m`, `1h`, `1d`

### **batch_size**

The batch size is the number of data points to generate at each interval. If the remaining count is less than batch_size, only the remaining messages will be sent.

type: `integer`

default: `1`

## Examples

```yaml
  - input:
      type: "generate"
      context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
      interval: 1ms
      batch_size: 1000
      count: 10000  # Optional: generate 10000 messages in total
```
