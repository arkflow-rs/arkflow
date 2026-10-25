---
sidebar_position: 7
title: Webhook 告警
---

# Webhook 告警

Hub 可以把控制面事件转发到外部 HTTP 端点。一个 webhook 订阅按
`event_type` 前缀过滤事件，每个匹配事件收到一次 `POST`，载荷与 SSE 流
携带的有界载荷相同——绝不包含凭证、原始配置内容或无界错误文本。

本页覆盖订阅管理、投递语义、签名验证与失败模型。所有事件的持久记录
始终在 Hub 事件日志中（`GET /events`、`GET /events/stream`）；webhook
是通知通道，不是记录系统。

## 管理订阅

订阅通过全量替换语义管理：`PUT /webhooks` 提交完整集合（至多 16 条），
提交的列表即完整集合。读取用 `GET /webhooks`；两者都要求
`ManageWebhooks` 动作（admin 与 operator 角色），每次接受的替换或拒绝都
入审计（`webhook.replace`）。

```bash
# 提交完整订阅集（替换全部）。
curl -X PUT "$HUB/api/v1/webhooks" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "subscriptions": [
          {
            "id": "oncall",
            "url": "https://alerts.example.com/hook",
            "event_types": ["rollout.", "node.", "webhook."],
            "enabled": true,
            "secret": "signing-secret"
          }
        ]
      }'

# 读取已存集合（secret 永不回显；以 has_secret 标识其存在）。
curl "$HUB/api/v1/webhooks" -H "Authorization: Bearer $TOKEN"
```

校验边界：仅允许 `http://`/`https://` URL，至多 2048 字符；每个订阅至少
一个非空事件类型前缀；id 唯一；secret 至多 256 字符。违规以
`invalid_webhooks` 拒绝，不部分生效。

签名密钥是只写的。由于响应永不包含它，替换条目中省略 `secret` 字段时
**保留**该 id 已存的密钥——暂停或编辑某一行不会静默去掉它的签名。要清
除密钥，显式提交空字符串。

Console 的 Settings 页提供同一能力：列表、新增、编辑、暂停/启用、以及
逐订阅的测试投递。

## 投递语义

对每个启用的订阅，Hub 以 JSON POST 每个匹配事件（与 SSE 相同的 wire
表示），携带以下头：

| 头 | 值 |
| --- | --- |
| `X-Arkflow-Event-Id` | 持久事件 id（未持久化时用时间戳）。 |
| `X-Arkflow-Event-Type` | 事件的 `event_type`。 |
| `X-Arkflow-Timestamp` | `occurred_at_ms`（epoch 毫秒）。 |
| `X-Arkflow-Signature` | 请求体精确字节的十六进制 HMAC-SHA256，以订阅密钥为键——仅在配置了密钥时存在。 |

任何 HTTP 2xx 响应计为投递成功。连接错误、超时（10 秒）与非 2xx 响应
按指数退避（1 秒、4 秒）重试，每个事件至多 3 次尝试。

**投递端到端为 best-effort。** 四个独立丢失源叠加——应把它们当作一份
契约来理解：

1. **leader 交接** — 仅租约持有者转发；接管窗口内发布的事件不会被新
   leader 重放。
2. **broadcast 滞后** — 转发器落后实时流超过 256 个事件时，滞后事件被
   跳过而非重放。
3. **队列溢出** — 每个订阅至多持有 64 个待投递事件；溢出丢弃**最旧**
   的排队事件（新鲜度优先于完备性）并计数。投递与丢弃序列都保持发布
   顺序。
4. **暂停窗口** — 被暂停的订阅在运维重新启用前不接收任何事件。

接收方不得依赖 webhook 投递作为任何事件的唯一记录；事件日志才是权威。

## 自动暂停与恢复

连续 5 个事件耗尽重试后，Hub 自动暂停该订阅：`enabled` 持久化为
`false`，发布一条 `webhook.paused` 控制面事件（同时持久化进事件日志，
并以 `system` actor 记入 `webhook.pause` 审计），Console 显示该订阅为
已暂停。被暂停的订阅在运维重新启用前不接收事件——重新启用会发布
`webhook.resumed` 事件。

### 死信监控（meta 订阅）

被暂停的 webhook 无法向它自己的接收方宣布这一点。要观察 webhook *A*
的失败，再注册一个指向**不同**端点、过滤 `webhook.` 的订阅 *B*：生命
周期事件（`webhook.paused`、`webhook.resumed`）是普通控制面事件，与其
他事件一样被转发。只要两个端点不同时故障，A 的暂停总能在 B 观察到。

## 验证签名

接收方在信任一次投递前，应对请求体的精确原始字节验证签名：

```python
import hmac, hashlib

def verify(secret: str, body: bytes, signature: str) -> bool:
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)

# FastAPI/Flask 风格处理示意：保留 RAW 请求体——
# 重新序列化的 JSON 无法通过验证。
```

`POST /webhooks/{id}/test` 同步投递一条合成的 `webhook.test` 事件，并
返回端点的 HTTP 状态或传输错误。测试投递不持久化、不进入重试队列、不
影响失败计数与启用状态。

## 安全边界

- **授权**：所有路由要求 `ManageWebhooks` 动作；测试投递与替换共用它。
  拒绝入审计。
- **SSRF**：订阅与测试目标是 operator 级受信输入。仅校验 URL scheme
  （`http`/`https`）；Hub 不过滤私网段、回环或 DNS 重绑定目标。把
  webhook 管理视同配置任何外发集成，并据此收敛 operator 凭据集。
- **静态密钥**：签名密钥以明文存储在控制面数据库（与 Hub 环境变量
  secret 同一信任级别），API 永不回显，审计记录永不包含其值。外部
  Secret Manager 集成在路线图中单独立项。
