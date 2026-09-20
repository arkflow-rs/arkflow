---
components: [modbus]
sidebar_label: Modbus
---

# Modbus

Modbus 输入(Input)以固定的时间间隔轮询 Modbus TCP 设备,支持四种寄存器类型:coils(线圈)、discrete_inputs(离散输入)、holding_registers(保持寄存器)和 input_registers(输入寄存器)。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 常量值 `"modbus"` |
| addr | string | yes | — | Modbus TCP 服务器地址,格式为 `host:port` |
| slave_id | integer | yes | — | Modbus 从站 ID |
| points | array&lt;object&gt; | yes | — | 每次轮询读取的数据点列表,见下表 |
| interval | duration | yes | — | 轮询间隔,如 `1s`、`500ms` |

### points[]

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | 寄存器类型: `coils` / `discrete_inputs` / `holding_registers` / `input_registers` |
| name | string | yes | 数据点名称,用作输出字段名 |
| address | integer | yes | 起始寄存器地址 |
| quantity | integer | yes | 要读取的寄存器数量 |

## 示例

```yaml validate=fragment wrap=input
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
