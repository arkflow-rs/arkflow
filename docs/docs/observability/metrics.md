---
sidebar_position: 3
---

# Prometheus metrics

The shared HTTP server exposes `/metrics` in Prometheus text exposition when
the health server is enabled. In Hub mode, node reports also carry stream
metric snapshots for fleet views.

Scrape the endpoint from the configured health address and alert on sustained
input/output errors, reconnects, and stalled readiness. Keep labels bounded;
do not use untrusted message fields as metric labels.
