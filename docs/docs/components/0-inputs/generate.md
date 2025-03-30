# Generate

## Configuration

### **context**

The context is a JSON object that will be used to generate the data.

type: `string`

### **interval**

The interval is the time between each data point.

type: `string`

example: `1ms`, `1s`, `1m`, `1h`, `1d`

### **batch_size**

The batch size is the number of data points to generate at each interval.

type: `integer`

default: `1`

## Examples

```yaml
  - input:
      type: "generate"
      context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
      interval: 1ms
      batch_size: 1000
```
