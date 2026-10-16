---
sidebar_position: 12
title: TLS support matrix
description: Encrypted-transport enablement for every ArkFlow network component.
---

# TLS support matrix

Every ArkFlow component that opens a network connection, and how to enable
encrypted transport for it. Components without network capability (memory,
generate, drop, stdout, and pure-compute processors) have no TLS surface.

| Component | TLS mechanism |
|-----------|--------------|
| kafka (input/output) | rdkafka `security.protocol` + broker/CA config |
| mqtt (input/output) | `tls` block (`enabled`, `ca`, `client_cert`, `client_key`) |
| nats (input/output) | `tls://` URL scheme (native async-nats negotiation) |
| pulsar (input/output) | `pulsar+ssl://` URL scheme (native) |
| redis (input/output/temporary) | `rediss://` URL scheme |
| sql (output) | sqlx TLS (Postgres `sslmode`, MySQL `ssl-mode`) |
| mongodb (output) | `mongodb+srv` / `tls=true` connection string |
| http (input/output) | `https://` URL (TLS terminated by reqwest/hyper) |
| websocket (input) | `wss://` URL |
| embedding / llm / vector_search / milvus_search (processors) | `https://` endpoint via reqwest |
| pgvector_search (processor) / pgvector (output) | PostgreSQL connection URL via sqlx (`sslmode`, see the `sql` row) |
| qdrant / milvus (outputs) | `https://` endpoint via reqwest |
| secret references | `${secret:NAME}` resolved from the Hub/node environment at materialization |

## Loopback proxy bypass

All HTTP-based components (embedding, llm, vector_search, milvus_search,
qdrant, milvus) bypass the system proxy when connecting to loopback
endpoints. SQLx-backed components (sql, pgvector) are not HTTP clients and
are unaffected by proxy environment variables. This is never
what a user means when connecting to a local broker or database.

## Non-goals

- Client-certificate rotation, OCSP stapling, or online revocation checking.
- mTLS for NATS (server-side TLS only; client-certificate authentication
  is not supported by the connection configuration).
