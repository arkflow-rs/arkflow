---
sidebar_position: 7
title: Webhook alerting
---

# Webhook alerting

The Hub can forward control-plane events to external HTTP endpoints. A
webhook subscription filters events by `event_type` prefix and receives a
`POST` per matching event with the same bounded payload the SSE stream
carries — never credentials, raw configuration content, or unbounded error
text.

This page covers subscription management, delivery semantics, signature
verification, and the failure model. The durable record of every event
stays in the Hub event log (`GET /events`, `GET /events/stream`); webhooks
are a notification channel, not a record system.

## Managing subscriptions

Subscriptions are managed through full-replace semantics: `PUT /webhooks`
submits the complete set (at most 16 entries), and the submitted list
becomes the whole set. Reading is `GET /webhooks`; both require the
`ManageWebhooks` action (admin and operator roles) and every accepted
replacement or denial is audited (`webhook.replace`).

```bash
# Submit the complete subscription set (this replaces everything).
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

# List the stored set (secrets are never echoed; `has_secret` reports one).
curl "$HUB/api/v1/webhooks" -H "Authorization: Bearer $TOKEN"
```

Validation bounds: `http://`/`https://` URLs only, at most 2048 characters;
at least one non-empty event-type prefix per subscription; unique ids;
secrets at most 256 characters. Violations are rejected with
`invalid_webhooks` without partially applying.

The signing secret is write-only. Because responses never include it, a
replacement entry that omits the `secret` field **retains** the stored
secret for the same id — pausing or editing one row never silently strips
signing from it. To clear a secret, submit an explicitly empty string.

The console exposes the same surface on the Settings page: list, add, edit,
pause/enable, and per-subscription test delivery.

## Delivery semantics

For each enabled subscription the Hub POSTs every matching event as JSON
(the same wire representation as SSE) with these headers:

| Header | Value |
| --- | --- |
| `X-Arkflow-Event-Id` | Durable event id (timestamp when not yet durable). |
| `X-Arkflow-Event-Type` | The event's `event_type`. |
| `X-Arkflow-Timestamp` | `occurred_at_ms` (epoch milliseconds). |
| `X-Arkflow-Signature` | Hex HMAC-SHA256 of the exact body, keyed by the subscription secret — only when a secret is configured. |

Any HTTP 2xx response counts as delivered. Connection errors, timeouts
(10 seconds), and non-2xx responses are retried with exponential backoff
(1s, then 4s) for at most 3 total attempts per event.

**Delivery is best-effort end to end.** Four independent loss sources stack
— treat them as one contract:

1. **Leader handover** — only the lease holder forwards; events published
   in a takeover window are not replayed by the new leader.
2. **Broadcast lag** — if the forwarder falls more than 256 events behind
   the live stream, lagged events are skipped, not replayed.
3. **Queue overflow** — each subscription holds at most 64 pending events;
   overflow drops the **oldest** queued event (freshness over completeness)
   and counts it. Delivered and dropped sequences both stay in publication
   order.
4. **Pause windows** — a paused subscription receives nothing until an
   operator re-enables it.

Receivers must not rely on webhook delivery as the sole record of any
event; the event log remains authoritative.

## Auto-pause and recovery

After 5 consecutive events exhaust their retries, the Hub automatically
pauses the subscription: `enabled` is persisted as `false`, a
`webhook.paused` control-plane event is published (also durable in the
event log and audited as `webhook.pause` with a `system` actor), and the
console shows the subscription as paused. A paused subscription receives
nothing until an operator re-enables it — which publishes a
`webhook.resumed` event.

### Dead-letter monitoring (meta subscription)

A paused webhook cannot announce itself to its own receiver. To observe
failures of webhook *A*, register a second subscription *B* pointing at a
**different** endpoint with the filter `webhook.`: lifecycle events
(`webhook.paused`, `webhook.resumed`) are ordinary control-plane events and
forward like any others. As long as the two endpoints do not fail together,
the pause of A is always observable at B.

## Verifying signatures

Receivers should verify the signature over the exact raw body bytes before
trusting a delivery:

```python
import hmac, hashlib

def verify(secret: str, body: bytes, signature: str) -> bool:
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)

# FastAPI/Flask-style handler sketch: keep the RAW request body —
# re-serialized JSON will not verify.
```

`POST /webhooks/{id}/test` performs one synchronous delivery of a synthetic
`webhook.test` event and reports the endpoint's HTTP status or transport
error. Test deliveries are not persisted, do not enter the retry queue, and
do not affect the failure counter or the enabled state.

## Security boundaries

- **Authorization**: all routes require the `ManageWebhooks` action; test
  delivery and replacement share it. Denials are audited.
- **SSRF**: subscription and test targets are operator-trustworthy inputs.
  Only the URL scheme is validated (`http`/`https`); the Hub does not
  filter private ranges, loopback, or DNS-rebinding targets. Treat webhook
  management as equivalent to configuring any outbound integration, and
  restrict the operator credential set accordingly.
- **Secrets at rest**: signing secrets are stored in the control-plane
  database in plain text (the same trust level as Hub environment secrets),
  never echoed by the API, and never written to audit records. External
  secret-manager integration is tracked separately in the roadmap.
