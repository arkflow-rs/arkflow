---
components: [input/pulsar]
sidebar_label: Pulsar
---

# Pulsar

Pulsar 输入(Input)订阅 Apache Pulsar 主题(Topic),支持四种订阅(Subscription)类型——exclusive / shared / failover / key_shared——并可选支持 Token 或 OAuth2 认证。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 常量值 `"pulsar"` |
| service_url | string | yes | — | Pulsar 服务 URL,如 `pulsar://host:6650` 或 `pulsar+ssl://host:6651`;多个集群 URL 以逗号分隔 |
| topic | string | yes | — | 主题,如 `persistent://tenant/namespace/topic` 或短名称 |
| subscription_name | string | yes | — | 订阅名称 |
| subscription_type | string | no | `"exclusive"` | 订阅类型: `exclusive` / `shared` / `failover` / `key_shared` |
| auth | object | no | — | 认证配置,见下表(带标签的枚举) |
| retry_config | object | no | — | 重试配置,见下表 |

### auth

`auth` 是一个带标签的枚举(tagged enum,通过 `type` 字段区分),有两种互斥的形式:

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `"token"` 或 `"o_auth2"` |
| token | string | yes (token) | Token 字符串 |
| issuer_url | string | yes (o_auth2) | OAuth2 issuer URL |
| credentials_url | string | yes (o_auth2) | OAuth2 凭证 URL |
| audience | string | yes (o_auth2) | OAuth2 audience |

### retry_config

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| max_attempts | integer | yes | — | 最大重试次数 |
| initial_delay_ms | integer | yes | — | 初始退避延迟(毫秒) |
| max_delay_ms | integer | yes | — | 最大退避延迟(毫秒) |
| backoff_multiplier | number | yes | — | 指数退避乘数 |

## 示例

```yaml validate=fragment wrap=input
input:
  type: "pulsar"
  service_url: "pulsar://localhost:6650"
  topic: "my-namespace/my-topic"
  subscription_name: "my-subscription"
```

```yaml validate=fragment wrap=input
input:
  type: "pulsar"
  service_url: "pulsar://pulsar-cluster:6650"
  topic: "persistent://my-tenant/my-ns/events"
  subscription_name: "consumer-group-1"
  subscription_type: "shared"
```

```yaml validate=fragment wrap=input
input:
  type: "pulsar"
  service_url: "pulsar+ssl://secure-pulsar:6651"
  topic: "secure-topic"
  subscription_name: "secure-subscription"
  auth:
    type: "token"
    token: "${PULSAR_TOKEN}"
```

```yaml validate=fragment wrap=input
input:
  type: "pulsar"
  service_url: "pulsar+ssl://pulsar.cloud:6651"
  topic: "cloud-topic"
  subscription_name: "oauth-subscription"
  auth:
    type: "o_auth2"
    issuer_url: "https://auth.example.com"
    credentials_url: "https://auth.example.com/credentials.json"
    audience: "pulsar-cluster"
```

## 说明

- 元数据:`__meta_topic`、`__meta_message_id`、`__meta_publish_time`、`__meta_ingest_time`。
- 订阅类型:`exclusive`(单消费者,有序)、`shared`(轮询分发,无序)、`failover`(主备,有序)、`key_shared`(按键路由,同一键内有序)。
