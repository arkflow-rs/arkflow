---
sidebar_position: 4
---

# Tracing

ArkFlow uses the Rust `tracing` facade for structured runtime events. Configure
the logging filter and format in the top-level `logging` section, then route
stdout or stderr to the collector used by the deployment. The current runtime
does not promise an OTLP exporter; use logs and metrics as the supported
observability surfaces.
