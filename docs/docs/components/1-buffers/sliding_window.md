---
components: [sliding_window]
description: ArkFlow documentation page.
---

# Sliding Window (deprecated)

:::warning Deprecated and unreachable in Stream configs

The stream compiler **rejects** the sliding window buffer with an actionable error. This page documents
the legacy plugin for pre-migration configs only. For time-based overlapping windows, use an event-time
sliding **window operator** in a Job DAG, which classifies every containing window membership per row.

:::

## Legacy behavior

The Sliding Window buffer grouped messages into overlapping windows that advanced over time. Up to `window_size`
messages were emitted as a single batch on each `interval`; after each emission the window slid forward by
`slide_size` messages.

## Legacy configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `sliding_window` |
| window_size | integer | yes | — | Number of messages to include in each emitted window. |
| interval | duration | yes | — | Time between window emissions. Examples: `1ms`, `1s`, `1m`, `1h`. |
| slide_size | integer | yes | — | Number of messages to advance the window after each emission. Controls the overlap between consecutive windows. |

Compiling a Stream config that declares this buffer fails with a migration message; a Job DAG with a sliding
window operator is the replacement.
