---
sidebar_position: 12
title: TLS 支持矩阵
description: 每个 ArkFlow 网络组件的加密传输启用方式。
---

# TLS 支持矩阵

每个会建立网络连接的 ArkFlow 组件,以及如何为其启用加密传输。不具备网络能力的组件(memory、generate、drop、stdout 以及纯计算处理器)没有 TLS 面。

| Component | TLS 机制 |
|-----------|--------------|
| 控制面 Hub (`arkflow-server`) | `ARKFLOW_HUB_TLS_CERT` + `ARKFLOW_HUB_TLS_KEY`;附加 `ARKFLOW_HUB_TLS_CA` + `ARKFLOW_HUB_TLS_CLIENT_AUTH`(`agent_required` \| `required` \| `none`)启用 mTLS 及可选的 `urn:arkflow:node:<id>` SAN 绑定 |
| 控制面 Agent (`arkflow`) | `health_check.hub_urls` 使用 `https://`,`ARKFLOW_AGENT_TLS_CA`(`tls_ca`),并成对设置 `ARKFLOW_AGENT_TLS_CERT` / `ARKFLOW_AGENT_TLS_KEY`(`tls_cert` / `tls_key`) |
| 数据面 shuffle(Agent ↔ Agent) | `ARKFLOW_DATA_PLANE_TLS_CA` + `ARKFLOW_DATA_PLANE_TLS_CERT` + `ARKFLOW_DATA_PLANE_TLS_KEY`(`DNS:arkflow-data-plane` + 可选 `URI:urn:arkflow:node:<id>`);自动进入 `hub_provisioned` 逐边 HMAC 密钥模式,无需 `ARKFLOW_DATA_PLANE_SECRET` |
| kafka (input/output) | rdkafka `security.protocol` + broker/CA 配置 |
| mqtt (input/output) | `tls` 块(`enabled`、`ca`、`client_cert`、`client_key`) |
| nats (input/output) | `tls://` URL 方案(由 async-nats 原生协商) |
| amqp09 (input/output) | `amqps://` URL 方案(由 lapin 通过 rustls 与系统根证书原生协商) |
| pulsar (input/output) | `pulsar+ssl://` URL 方案(原生) |
| redis (input/output/temporary) | `rediss://` URL 方案 |
| sql (output) | sqlx TLS(Postgres `sslmode`、MySQL `ssl-mode`) |
| mongodb (output) | `mongodb+srv` / `tls=true` 连接字符串 |
| http (input/output) | `https://` URL(TLS 由 reqwest/hyper 终结) |
| websocket (input) | `wss://` URL |
| embedding / llm / vector_search / milvus_search (processors) | 经 reqwest 的 `https://` 端点 |
| pgvector_search (processor) / pgvector (output) | 经 sqlx 的 PostgreSQL 连接串(`sslmode`,见 `sql` 行) |
| qdrant / milvus (outputs) | 经 reqwest 的 `https://` 端点 |
| secret 引用 | `${secret:NAME}` 在配置物化时从 Hub/节点环境解析 |

## 环回代理绕过

所有基于 HTTP 的组件(embedding、llm、vector_search、milvus_search、qdrant、milvus)在连接环回端点时会绕过系统代理。基于 sqlx 的组件(sql、pgvector)不是 HTTP 客户端,不受代理环境变量影响。连接本地 broker 或数据库时,走系统代理从来都不是用户本意。

## 非目标

- 客户端证书轮换、OCSP stapling 或在线吊销检查。
- NATS 的 mTLS(仅服务端 TLS;连接配置不支持客户端证书认证)。
