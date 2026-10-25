## ADDED Requirements

### Requirement: Webhook subscriptions are durable, bounded, and operator-managed

The Hub SHALL store webhook subscriptions as a durable, operator-managed set: each subscription SHALL carry a unique identifier, target URL (`http://` or `https://` only), event-type filter list (prefix matching), enabled flag, optional signing secret, creator identity, and creation timestamp. The set SHALL contain at most 16 subscriptions and SHALL persist across Hub restarts. Subscriptions SHALL be managed through full-replace semantics (the submitted list becomes the complete set). The signing secret is write-only: because API representations never include it, a replacement entry for an existing id that omits the secret SHALL retain the stored secret (an omitted secret must not silently strip signing from unchanged rows), and an explicitly empty secret SHALL clear it. A submission exceeding the bound, carrying an invalid URL scheme, or carrying an invalid event-type filter SHALL be rejected with an explicit validation error and SHALL NOT partially apply. API representations of a subscription SHALL NOT include the signing secret.

#### Scenario: Subscriptions survive Hub restart

- **WHEN** the Hub restarts after subscriptions were created
- **THEN** the subscription set reported by the API is identical to the pre-restart set, including enabled flags

#### Scenario: Reject an over-bound or malformed subscription set

- **WHEN** an operator submits a set of 17 subscriptions, or a subscription with a `ftp://` URL, or with an empty event-type filter entry
- **THEN** the Hub rejects the request with an explicit validation error and the stored set is unchanged

#### Scenario: Secrets are never echoed

- **WHEN** an operator lists subscriptions or replaces the set
- **THEN** no response body contains any signing secret value

#### Scenario: An omitted secret is retained on replacement

- **WHEN** an operator replaces the set re-submitting an existing subscription with the secret field omitted
- **THEN** the stored signing secret for that id is unchanged and deliveries keep being signed

### Requirement: Webhook management is authorized and audited

Managing webhook subscriptions (replace, and test delivery) SHALL require the `ManageWebhooks` action. Every accepted subscription replacement and every authorization denial SHALL be recorded in the audit log with the actor and outcome; audit records MUST NOT contain signing secret values.

#### Scenario: Operator replaces the subscription set

- **WHEN** an operator holding `ManageWebhooks` submits a valid subscription set to the replacement route
- **THEN** the stored set becomes exactly the submitted set and an audit event records the actor and outcome

#### Scenario: Unauthorized management is denied and audited

- **WHEN** a principal without `ManageWebhooks` (for example a viewer) submits a subscription set replacement or a test delivery
- **THEN** the Hub rejects the request as forbidden, the stored set is unchanged, and the denial is audited

### Requirement: Matching events are forwarded with signed, bounded payloads

For each enabled subscription, the Hub SHALL forward every published control-plane event whose `event_type` starts with at least one of the subscription's filter entries. Forwarding SHALL be an HTTP POST whose body is the event's standard wire representation (event ID, node ID, event type, outcome, correlation ID, actor, bounded message — never credentials, raw configuration content, or secret values) and SHALL carry `X-Arkflow-Event-Id`, `X-Arkflow-Event-Type`, and `X-Arkflow-Timestamp` headers. When the subscription defines a signing secret, the Hub SHALL also carry an `X-Arkflow-Signature` header containing the hex-encoded HMAC-SHA256 of the exact request body keyed by that secret. Events not matching any filter entry SHALL NOT be forwarded.

#### Scenario: A matching event is forwarded with signature

- **WHEN** an event with `event_type` `rollout.failed` is published and an enabled subscription filters on `rollout.` with a signing secret
- **THEN** the receiver gets a POST whose body is the event wire representation, the three identification headers, and a signature header that verifies as HMAC-SHA256 of the body under the secret

#### Scenario: A non-matching event is not forwarded

- **WHEN** an event with `event_type` `node.heartbeat` is published and the only subscription filters on `rollout.`
- **THEN** no HTTP request is made to the subscription URL

### Requirement: Delivery is bounded, retried, and auto-paused on sustained failure

Delivery SHALL be bounded and best-effort end to end: the durable record of control-plane events remains the event log, and receivers MUST NOT rely on webhook delivery as the sole record of any event. Per subscription the Hub SHALL queue at most 64 pending events and deliver them serially (a slow receiver SHALL NOT block other subscriptions or the event pipeline). When a subscription's queue is full, the Hub SHALL drop the oldest queued event and count it rather than buffer unboundedly, retaining the freshest events; both delivered and dropped sequences remain order-preserving subsets of publication order. A delivery attempt SHALL succeed on any HTTP 2xx response; connection errors, timeouts (10 seconds), and non-2xx responses SHALL be retried with exponential backoff for at most 3 total attempts. After 5 consecutive failed deliveries the Hub SHALL automatically pause the subscription (persist `enabled = false`) and publish a `webhook.paused` control-plane event identifying the subscription; when a paused subscription is subsequently re-enabled, the Hub SHALL publish a `webhook.resumed` control-plane event identifying it. A paused subscription SHALL NOT receive events until re-enabled by an operator.

#### Scenario: A transient failure is retried and succeeds

- **WHEN** a subscription endpoint returns 500 for the first attempt and 200 for the second attempt of the same event
- **THEN** the event is delivered, no other event is sent out of order for that subscription, and the failure counter resets

#### Scenario: Sustained failure auto-pauses the subscription

- **WHEN** a subscription endpoint fails all retry attempts for 5 consecutive events
- **THEN** the subscription is persisted as disabled, a `webhook.paused` event is published, and subsequent matching events are not forwarded

#### Scenario: Queue overflow drops the oldest queued event

- **WHEN** a subscription's pending queue holds 64 events and a new matching event is published
- **THEN** the oldest queued event is dropped with a counted metric, the new event is enqueued, the Hub's event pipeline is not blocked, and deliveries remain in publication order

#### Scenario: Re-enabling publishes a resumed event

- **WHEN** an operator re-enables a paused subscription through subscription replacement
- **THEN** the Hub publishes a `webhook.resumed` event identifying the subscription and subsequent matching events are forwarded

### Requirement: Forwarding runs only on the leading Hub

In a multi-Hub (HA) deployment, only the lease holder SHALL forward webhook deliveries. A Hub that loses or does not hold leadership SHALL NOT deliver webhooks; on acquiring leadership the Hub SHALL resume delivery for enabled subscriptions with subsequently published events (no replay of events published while another Hub held or lost the lease).

#### Scenario: Standby Hub does not forward

- **WHEN** an event is published while a standby Hub (not holding the lease) observes it
- **THEN** the standby Hub makes no webhook request, and the lease holder forwards the event

### Requirement: Test delivery sends a synthetic event

The Hub SHALL provide a test-delivery action per subscription that synchronously performs one delivery attempt of a synthetic event (distinct event type identifying it as a test), reports the HTTP status or transport error to the caller, and neither persists the synthetic event nor affects the subscription's failure counter or enabled state.

#### Scenario: A test delivery reports the endpoint outcome

- **WHEN** an operator with `ManageWebhooks` invokes the test action for a subscription whose endpoint returns 200
- **THEN** the response reports success with the endpoint's status, no synthetic event is persisted, and the subscription's delivery state is unchanged
