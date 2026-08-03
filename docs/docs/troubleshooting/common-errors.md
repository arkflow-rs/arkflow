---
sidebar_position: 3
---

# Common errors

- **Parse error with a line and column**: inspect the reported span first;
  validate the same file with `arkflow --config file.yaml --validate`.
- **Unknown component type**: run `arkflow components list` and use the exact
  registered name, including underscores such as `schema_registry`.
- **Connection or authentication failure**: verify the endpoint from the
  ArkFlow process, credentials, and TLS settings before changing retry values.
- **Backpressure**: inspect output latency and error counters; increasing input
  concurrency does not fix a slow sink.
