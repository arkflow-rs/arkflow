---
sidebar_position: 12
title: TLS 支持矩阵
description: 每个 ArkFlow 网络组件的加密传输启用方式。
---

# TLS 支持矩阵

每个会建立网络连接的 ArkFlow 组件,以及如何为其启用加密传输。不具备网络能力的组件(memory、generate、drop、stdout 以及纯计算处理器)没有 TLS 面。

| Component | TLS 机制 |
|-----------|--------------|
| kafka (input/output) | rdkafka `security.protocol` + broker/CA 配置 |
| mqtt (input/output) | `tls` 块(`enabled`、`ca`、`client_cert`、`client_key`) |
| nats (input/output) | `tls://` URL 方案(由 async-nats 原生协商) |
| pulsar (input/output) | `pulsar+ssl://` URL 方案(原生) |
| redis (input/output/temporary) | `rediss://` URL 方案 |
| sql (output) | sqlx TLS(Postgres `sslmode`、MySQL `ssl-mode`) |
| mongodb (output) | `mongodb+srv` / `tls=true` 连接字符串 |
| http (input/output) | `https://` URL(TLS 由 reqwest/hyper 终结) |
| websocket (input) | `wss://` URL |
| embedding / llm / vector_search / milvus_search / pgvector_search (processors) | 经 reqwest 的 `https://` 端点 |
| qdrant / pgvector / milvus (outputs) | `https://` / `pulsar+ssl://` 端点 |
| secret 引用 | `${secret:NAME}` 在配置物化时从 Hub/节点环境解析 |

## 环回代理绕过

所有基于 HTTP 的组件(embedding、llm、qdrant、milvus、pgvector、sql)在连接环回端点时会绕过系统代理。连接本地 broker 或数据库时,走系统代理从来都不是用户本意。

## 非目标

- 客户端证书轮换、OCSP stapling 或在线吊销检查。
- NATS 的 mTLS(仅服务端 TLS;如需客户端证书认证,请使用连接字符串)。
