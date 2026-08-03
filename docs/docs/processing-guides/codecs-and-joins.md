---
sidebar_position: 5
---

# Codecs and joins

Inputs decode bytes into Arrow batches and outputs encode batches back to the
wire format. Select JSON or Protobuf for ordinary payloads; use Debezium or
Schema Registry codecs when the producer uses those envelopes.

For joins, configure a window buffer and give each input a stable source name.
The SQL processor can then select fields from the joined batch or a temporary
lookup table. Start from [`examples/join_buffer_example.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/join_buffer_example.yaml).
