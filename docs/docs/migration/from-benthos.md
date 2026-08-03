---
sidebar_position: 2
---

# From Benthos or Bento

Map Benthos inputs, processors, and outputs to the corresponding ArkFlow
component `type` and place them under `input`, `pipeline.processors`, and
`output` in the stream configuration.

```yaml
input: {type: kafka, brokers: ["localhost:9092"], topics: [events], consumer_group: app}
pipeline: {processors: [{type: vrl, statement: ".level = \"info\"; ."}]}
output: {type: stdout}
```

ArkFlow's input WAL provides replay-oriented durability; it remains a
single-node engine, so do not assume Benthos or ArkFlow configs are cluster
failover configurations without testing the deployment boundary.
