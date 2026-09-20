---
description: 文档路由迁移映射。
---

# 路由迁移映射

2026 年的文档重建将叙述性页面归入了六个面向读者的领域(Get Started、Build、SQL、Operate、Reference、Develop)。**所有旧路由仍然有效** —— 每个旧路径都保留一个兼容性占位页并指向新位置;只有在替代页面发布满一个版本、并附有明确的版本说明之后,旧路由才会被移除。

| 旧路由 | 新路由 |
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

组件页面(`/docs/components/...`)与 SQL 语言页面(`/docs/sql/...`)保留了原有 URL,并新增了正式的索引页(`/docs/components`、`/docs/sql`)。

在 `lastVersion: 'current'` 设置下,持续维护的文档树由 `/docs/` 提供,已发布的快照则位于 `/docs/0.5.x`、`/docs/0.3.x` 和 `/docs/0.2.x`;版本下拉菜单可在它们之间切换。
