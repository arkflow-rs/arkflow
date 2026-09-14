---
description: Documentation route migration map.
---

# Route migration map

The 2026 documentation rebuild moved narrative pages into five audience-facing
areas (Get Started, Build, SQL, Operate, Reference, Develop). **Every legacy
route still resolves** — each old path keeps a compatibility stub page that
points to the new location, and the route is only removed after the
replacement has been published for a release with an explicit release note.

| Legacy route | New route |
| --- | --- |
| `/docs/start-here` | `/docs/` |
| `/docs/intro` | `/docs/` |
| `/docs/getting-started/1-install` | `/docs/get-started/install` |
| `/docs/getting-started/2-quickstart` | `/docs/get-started/quickstart` |
| `/docs/tutorials/1-durable-pipeline` | `/docs/get-started/durable-pipeline` |
| `/docs/build-pipelines` | `/docs/build/streams` |
| `/docs/streaming-jobs` | `/docs/build/jobs` |
| `/docs/concepts/1-architecture` | `/docs/build/architecture` |
| `/docs/concepts/2-backpressure-ordering` | `/docs/build/backpressure` |
| `/docs/concepts/3-metadata` | `/docs/build/metadata` |
| `/docs/concepts/4-delivery-semantics` | `/docs/build/delivery-semantics` |
| `/docs/concepts/5-wal-optimization` | `/docs/build/wal` |
| `/docs/concepts/6-exactly-once` | `/docs/build/exactly-once` |
| `/docs/concepts/7-distributed-jobs` | `/docs/build/distributed-jobs` |
| `/docs/how-to/1-kafka-to-sql` | `/docs/build/recipes/kafka-to-sql` |
| `/docs/how-to/2-cdc-debezium` | `/docs/build/recipes/cdc-debezium` |
| `/docs/how-to/3-windowed-aggregation` | `/docs/build/recipes/windowed-aggregation` |
| `/docs/how-to/4-http-ingestion` | `/docs/build/recipes/http-ingestion` |
| `/docs/how-to/5-control-plane-rollout` | `/docs/operate/rollout` |
| `/docs/cases/1-webhook-durable` | `/docs/build/recipes/case-webhook-durable` |
| `/docs/cases/2-order-stream-sql` | `/docs/build/recipes/case-order-stream-sql` |
| `/docs/cases/3-telemetry-windows` | `/docs/build/recipes/case-telemetry-windows` |
| `/docs/operate` | `/docs/operate/overview` |
| `/docs/deploy/k8s-deployment` | `/docs/operate/kubernetes` |
| `/docs/control-plane/1-overview` | `/docs/operate/control-plane/overview` |
| `/docs/control-plane/2-deploy` | `/docs/operate/control-plane/deploy` |
| `/docs/control-plane/3-operations` | `/docs/operate/control-plane/operations` |
| `/docs/control-plane/reconciliation-rollout` | `/docs/operate/control-plane/reconciliation` |
| `/docs/control-plane/http-api-v1` | `/docs/reference/api` |
| `/docs/configuration/1-top-level` | `/docs/reference/configuration` |
| `/docs/configuration/2-ide-schema` | `/docs/reference/ide-schema` |

Component pages (`/docs/components/...`) and SQL language pages (`/docs/sql/...`)
kept their URLs and gained proper index pages (`/docs/components`,
`/docs/sql`).

With `lastVersion: 'current'`, the maintained tree is served at `/docs/` and
released snapshots live at `/docs/0.5.x`, `/docs/0.3.x`, and `/docs/0.2.x`;
the version dropdown switches between them.
