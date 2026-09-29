# control-plane-service Delta — console-hub-alignment

## ADDED Requirements

### Requirement: Hub fleet status resource

The Hub SHALL serve `GET /api/v1/status` returning the EngineStatus contract as a fleet aggregate: `streams_total`, `streams_running`, and `streams_failed` summed from the registered nodes' latest gauges, `state` reflecting the serving Hub (`running` when it holds the lease), `version` the Hub build version, and `uptime_seconds` the serving Hub's process uptime. A standby Hub SHALL apply its existing standby rejection (503 `hub_standby`) rather than serving a partial aggregate.

#### Scenario: Console opens the overview against a healthy Hub

- **WHEN** a client requests `GET /api/v1/status` from a leader Hub with three registered online nodes reporting stream gauges
- **THEN** the response is a single EngineStatus object whose stream counters equal the sum over registered nodes and whose `version` and `uptime_seconds` describe the Hub process

#### Scenario: Standby Hub does not serve fleet status

- **WHEN** a client requests `GET /api/v1/status` from a Hub that does not hold the control-plane lease
- **THEN** the Hub responds 503 with the `hub_standby` problem code and no partial aggregate
