---
sidebar_label: Modbus
---

# Modbus

The Modbus input polls a Modbus TCP device at a fixed interval and supports four register types: coils, discrete_inputs, holding_registers, and input_registers.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Constant value `"modbus"` |
| addr | string | yes | — | Modbus TCP server address in `host:port` format |
| slave_id | integer | yes | — | Modbus slave ID |
| points | array&lt;object&gt; | yes | — | List of points read each poll, see table below |
| interval | duration | yes | — | Polling interval, e.g. `1s`, `500ms` |

### points[]

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | Register type: `coils` / `discrete_inputs` / `holding_registers` / `input_registers` |
| name | string | yes | Data point name, used as the output field name |
| address | integer | yes | Starting register address |
| quantity | integer | yes | Number of registers to read |

## Examples

```yaml
input:
  type: "modbus"
  addr: "192.168.1.100:502"
  slave_id: 1
  interval: "1s"
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
