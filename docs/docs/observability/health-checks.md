---
sidebar_position: 2
---

# Health checks

When `health_check.enabled` is true, ArkFlow serves `/health`, `/readiness`,
and `/liveness` by default. The paths and bind address are configurable.

- **Health** reports the control-plane health document.
- **Readiness** becomes ready after the engine starts its configured streams.
- **Liveness** indicates that the process is running.

Use readiness for traffic admission and liveness for restart decisions. The
control-plane overview documents the compatibility routes and API prefix.
