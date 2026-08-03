---
sidebar_position: 2
---

# FAQ

## Why did a message appear twice?

The default delivery contract is at-least-once. A crash or restart can replay
unacknowledged WAL entries. Use a transactional or idempotent sink when the
application cannot tolerate duplicates.

## Why is a node not ready?

Check the configured health address, readiness route, and startup logs. In Hub
mode also check the node lease and the Hub URL.

## Why is a window empty?

Confirm the interval or inactivity gap, input traffic, and that the stream is
not blocked by downstream backpressure.
