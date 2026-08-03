---
sidebar_position: 7
---

# Schema Registry Protobuf

Use [`examples/schema_registry.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/schema_registry.yaml)
with the `schema_registry` codec when messages use Confluent's wire format.
Treat registry availability as a runtime dependency and test schema evolution
before changing producers.
