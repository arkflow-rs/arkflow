---
components: [output/mqtt]
description: ArkFlow 文档页面。
---

# MQTT

MQTT 输出(Output)将每条消息(Message)发布到 MQTT 代理(Broker)的主题(Topic)。它支持 QoS 0/1/2、干净会话、保活(keep-alive)、保留消息,以及可选的用户名/密码认证。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | 固定值 `"mqtt"` |
| host | string | yes | — | MQTT 代理主机名。 |
| port | integer | yes | — | MQTT 代理端口。 |
| client_id | string | yes | — | 客户端标识符。 |
| username | string | no | — | 用于认证的用户名。 |
| password | string | no | — | 用于认证的密码。 |
| topic | object | yes | — | 目标主题(表达式;见下文)。 |
| qos | integer | no | — | 服务质量:`0`、`1` 或 `2`。 |
| clean_session | boolean | no | — | 是否使用干净会话。 |
| keep_alive | integer | no | — | 保活间隔(秒)。 |
| retain | boolean | no | — | 是否在代理上保留该消息。 |
| value_field | string | no | — | 用作消息载荷的记录字段。 |

### topic

`topic` 是一个 `Expr<String>` 对象,具有以下形态之一:

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `value`(静态)或 `expr`(SQL 表达式)。 |
| value | string | yes (`value`) | 静态主题名称。 |
| expr | string | yes (`expr`) | 对每条消息求值的 SQL 表达式。 |


## TLS

设置 `tls` 块以通过 TLS 连接(MQTT 通常为端口 8883):

```yaml validate=fragment wrap=output
output:
  type: "mqtt"
  host: "localhost"
  port: 8883
  client_id: "tls-publisher"
  topic:
    type: "value"
    value: "demo"
  tls:
    enabled: true
    ca: "/etc/arkflow/certs/ca.pem"
```

`enabled` 在块存在时默认为 true(可设 `enabled: false` 仅作文档说明);`ca`
用于校验 broker 证书;`client_cert`/`client_key` 启用 mTLS。省略 `tls` 块
则保持明文 TCP。`client_id` 对每个连接必须唯一。

## 示例

### 静态主题

```yaml validate=fragment wrap=output
output:
  type: "mqtt"
  host: "localhost"
  port: 1883
  client_id: "my-client"
  username: "user"
  password: "pass"
  topic:
    type: "value"
    value: "my-topic"
  qos: 2
  clean_session: true
  keep_alive: 60
  retain: true
  value_field: "message"
```

### 通过 SQL 表达式动态指定主题

```yaml validate=fragment wrap=output
output:
  type: "mqtt"
  host: "localhost"
  port: 1883
  client_id: "sensor-client"
  topic:
    type: "expr"
    expr: "concat('sensor/', id)"
  qos: 1
```
