# control-plane-observability — Delta

## ADDED Requirements

### Requirement: Hub process logging and OTel trace export

The `arkflow-server` Hub binary SHALL initialize a tracing subscriber at startup, before any startup validation logging: log output goes to the console by default at level `info`, with level, file output, and format taken from the `logging` configuration (file or `ARKFLOW_LOG_LEVEL`/`RUST_LOG`). The effective log level SHALL follow the precedence chain `RUST_LOG` > `ARKFLOW_LOG_LEVEL` > `logging.level` (file) > default; an invalid `RUST_LOG` value SHALL warn on stderr and fall back to the static level. When `observability.tracing` is enabled (default off), the Hub SHALL export spans via OTLP/HTTP-JSON to the configured endpoint with the configured `service.name`; exporter build failure SHALL warn and disable export without preventing startup. On graceful shutdown (Ctrl-C or serve termination) the tracer provider SHALL be flushed before exit so buffered spans are not lost. When tracing is disabled the process MUST NOT build any OTel objects or issue OTLP network requests. Existing metrics, readiness, and liveness endpoint behavior SHALL be unchanged by this requirement.

#### Scenario: Default startup logs to the console
- **WHEN** the Hub starts with no configuration file and no logging-related environment variables
- **THEN** `tracing` events from Hub startup, election, and session handling appear on the console at `info` level, including the SQLite HA development warning path

#### Scenario: Tracing disabled stays inert
- **WHEN** `observability.tracing` is absent or `enabled: false`
- **THEN** no OTel exporter or provider is built, no OTLP requests are made, and logging behavior is identical to tracing being unconfigured

#### Scenario: Spans flushed on graceful shutdown
- **WHEN** tracing is enabled and the operator stops the Hub with Ctrl-C after it produced spans
- **THEN** the buffered spans are flushed to the OTLP endpoint before process exit

#### Scenario: Bad OTel endpoint never blocks serve
- **WHEN** `observability.tracing.endpoint` is invalid
- **THEN** startup logs a warning, tracing export is disabled, and the Hub serves normally

#### Scenario: Invalid RUST_LOG falls back
- **WHEN** `RUST_LOG` is set to a value EnvFilter cannot parse
- **THEN** the process warns on stderr and applies the static level from `ARKFLOW_LOG_LEVEL`/`logging.level` instead of failing startup
