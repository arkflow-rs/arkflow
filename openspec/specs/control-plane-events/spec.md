# Purpose

Define authenticated, durable, bounded control-plane event delivery and replay.

# Requirements

### Requirement: Authenticated SSE event stream

The service SHALL expose an authenticated SSE stream for operation, rollout, node, Stream, and audit changes, with event ID, event type, resource identity, correlation ID, and bounded payload.

#### Scenario: Subscribe to control-plane events
- **WHEN** an authorized client connects to the event stream with valid filters
- **THEN** the service sends only permitted events in a stable SSE representation

### Requirement: Event replay and snapshot fallback

The event stream SHALL support `Last-Event-ID` replay for events within the configured retention window and SHALL return a recoverable snapshot/resync indication when the requested event is no longer available.

#### Scenario: Reconnect within the event window
- **WHEN** a client reconnects with a retained Last-Event-ID
- **THEN** the service replays subsequent events in order without requiring a full resubscription

#### Scenario: Reconnect after pruning
- **WHEN** a client reconnects with an event ID outside the retention window
- **THEN** the service instructs the client to reload the relevant REST snapshot before consuming new events

### Requirement: Bounded event payloads

SSE and REST event payloads SHALL exclude credentials, secret configuration content, arbitrary metric labels, and unbounded error text.

#### Scenario: Publish a configuration event
- **WHEN** a configuration rollout changes state
- **THEN** the event contains version identity and outcome metadata but not the raw configuration or secrets
