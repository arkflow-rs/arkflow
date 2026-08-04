---
description: Curated end-to-end ArkFlow examples.
---

# Example catalog

These examples are maintained with the repository and checked by `pnpm docs:check`. Service-backed examples require the corresponding local service; use the quickstart first when learning the configuration shape.

| Workflow | Example | Use it for |
| --- | --- | --- |
| Quickstart | [`generate_example.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/generate_example.yaml) | A local source-to-stdout pipeline |
| Kafka | [`kafka_example.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/kafka_example.yaml) | Kafka input configuration |
| SQL output | [`sql_output_example.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/sql_output_example.yaml) | Database sink configuration |
| Durability | [`durability_example.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/durability_example.yaml) | Per-stream WAL durability |
| S3 durability | [`durability_example_s3.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/durability_example_s3.yaml) | Object-storage-backed recovery |
| Control plane | [`control_plane_example.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/control_plane_example.yaml) | Hub and compute-node operation |
| Control plane hub | [`control_plane_hub.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/control_plane_hub.yaml) | Fleet-level configuration |
| Debezium CDC | [`cdc_debezium.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/cdc_debezium.yaml) | CDC envelope decoding |

For each service-backed example, validate credentials and endpoints before running it in a shared environment.
