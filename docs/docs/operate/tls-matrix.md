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
| embedding / llm / vector_search / milvus_search / pgvector_search (processors) | `https://` endpoint via reqwest |
| qdrant / pgvector / milvus (outputs) | `https://` / `pulsar+ssl://` endpoint |
| secret references | `${secret:NAME}` resolved from the Hub/node environment at materialization |

## Loopback proxy bypass

All HTTP-based components (embedding, llm, qdrant, milvus, pgvector, sql)
bypass the system proxy when connecting to loopback endpoints. This is never
what a user means when connecting to a local broker or database.

## Non-goals

- Client-certificate rotation, OCSP stapling, or online revocation checking.
- mTLS for NATS (server-side TLS only; use the connection string for
  client-certificate authentication if needed).
