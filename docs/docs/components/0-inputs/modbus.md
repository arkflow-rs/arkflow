# Modbus

The Modbus input component receives data from Modbus TCP devices, supporting various register types.

## Configuration

### **addr** (required)

Modbus TCP server address in the format `host:port`.

type: `integer`

### **slave_id** (required)

Modbus slave ID.

type: `integer`

### **points** (required)

List of Modbus points to read.

type: `array` of point objects

#### Point Object

##### **type** (required)

Type of Modbus register to read. Must be one of: `"coils"`, `"discrete_inputs"`, `"holding_registers"`, or `"input_registers"`.

type: `string`

##### **name** (required)

Name for the data point. This will be used as the field name in the output record.

type: `string`

##### **address** (required)

Modbus register address to read from.

type: `integer`

##### **quantity** (required)

Number of registers to read.

type: `integer`

### **read_interval** (required)

Interval between consecutive reads.

type: `string` (parsed as duration)

example: `"1s"`, `"500ms"`, `"1m"`

## Examples


```yaml
- input:
    type: "modbus"
    addr: "192.168.1.100:502"
    slave_id: 1
    read_interval: "1s"
    points:
      - type: "holding_registers"
        name: "temperature"
        address: 100
        quantity: 2
      - type: "coils"
        name: "status_flags"
        address: 200
        quantity: 2
```