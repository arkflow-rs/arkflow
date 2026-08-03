---
sidebar_position: 5
---

# IoT ingestion with MQTT or Modbus

Use [`examples/mqtt_example.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/mqtt_example.yaml)
for MQTT telemetry and the Modbus input reference for register polling. Add a
tumbling window before a database output when devices emit at high frequency.
