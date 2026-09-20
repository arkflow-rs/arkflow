---
components: [input/mqtt]
sidebar_label: MQTT
---

# MQTT

MQTT 输入(Input)连接到 MQTT broker,订阅一个或多个主题(Topic),并接收实时消息。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 常量值 `"mqtt"` |
| host | string | yes | — | MQTT broker 地址 |
| port | integer | yes | — | MQTT broker 端口 |
| client_id | string | yes | — | 唯一的客户端标识符 |
| topics | array&lt;string&gt; | yes | — | 要订阅的主题列表(支持通配符) |
| username | string | no | — | 认证用户名 |
| password | string | no | — | 认证密码 |
| qos | integer | no | — | QoS 等级(0、1、2) |
| clean_session | boolean | no | — | 是否使用干净会话(clean session) |
| keep_alive | integer | no | — | 保活间隔(秒) |

## 示例

```yaml validate=fragment wrap=input
input:
  type: "mqtt"
  host: "localhost"
  port: 1883
  client_id: "my_client"
  username: "user"
  password: "pass"
  topics:
    - "sensors/temperature"
    - "sensors/humidity"
  qos: 1
  clean_session: true
  keep_alive: 60
```
