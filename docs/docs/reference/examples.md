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
| Control plane node | [`control_plane_node.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/control_plane_node.yaml) | Compute-node (Agent) mode: registers with a Hub (`hub_urls`, `node_id`, `node_token`) |
| Local control API | [`control_plane_example.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/control_plane_example.yaml) | Standalone engine exposing the local control API |
| Health-check server | [`control_plane_hub.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/control_plane_hub.yaml) | Minimal health-check / control-plane endpoint config with no streams |
| Local streaming job | [`jobs_local.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/jobs_local.yaml) | Declarative `jobs` DAG with event time, state, and checkpoint |
| Debezium CDC | [`cdc_debezium.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/cdc_debezium.yaml) | CDC envelope decoding |

For each service-backed example, validate credentials and endpoints before running it in a shared environment.
