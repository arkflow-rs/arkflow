---
sidebar_position: 4
---

# Debug guide

1. Validate the smallest complete configuration.
2. Replace the source with the `generate` input and the sink with `stdout`.
3. Add processors one at a time and compare `/metrics` before and after.
4. If the issue survives, capture logs, component versions, configuration
   format, and whether WAL replay was involved.

This workflow isolates connector, transformation, and sink failures without
discarding the original production configuration.
