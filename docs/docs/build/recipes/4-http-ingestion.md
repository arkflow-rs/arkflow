---
sidebar_position: 4
description: Receive HTTP webhooks and events with the HTTP input server.
---

# How to: ingest over HTTP

Run ArkFlow as an HTTP endpoint that turns POST requests into pipeline
messages.

**Prerequisites**

- ArkFlow [installed](../../get-started/1-install.md)
- No other services required for the basic path

## Configuration

```yaml validate=fragment wrap=engine
logging:
  level: info

streams:
  - input:
      type: http
      address: "0.0.0.0:8080"
      path: "/events"

    pipeline:
      thread_num: 4
      processors:
        - type: json_to_arrow
        - type: sql
          query: "SELECT * FROM flow"
        - type: arrow_to_json

    output:
      type: stdout

    error_output:
      type: stdout
```

The input listens on `address` and accepts POST requests on `path`; each
request body (JSON) becomes one message. Basic and Bearer authentication and
CORS are available — see the
[HTTP input reference](../../components/0-inputs/http.md).

## Run and verify

```bash
./target/release/arkflow --config http.yaml
```

Send a request:

```bash
curl -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{"user": "u1", "action": "click"}'
```

Expected result: the payload echoes on stdout through the pipeline, and the
endpoint answers with an acceptance response.

## Making it durable

HTTP is **not replayable**: once the endpoint answers, a crash would lose an
unacknowledged message. Add a WAL to the stream so every accepted request is
persisted before processing — the validated end-to-end version is
[Case: durable webhook collection](./10-case-webhook-durable.md).

## Troubleshooting

- **404 on POST** — the path must match exactly (`path: "/events"` ≠ `/`).
- **Messages lost on restart** — that is the non-replayable input without
  `durability`; add the WAL block as in the case study.
