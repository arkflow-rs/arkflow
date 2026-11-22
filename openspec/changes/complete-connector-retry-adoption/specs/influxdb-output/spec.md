## MODIFIED Requirements

### Requirement: InfluxDB v2 writes are batched and retried
The output SHALL send buffered lines to `/api/v2/write` with token authentication, encoded `org`/`bucket` query parameters, and retry failed requests according to configuration; a failed flush SHALL retain the buffered lines. Retrying SHALL distinguish transient from deterministic failures via the shared retry implementation: transport errors and 429/5xx responses are retried with capped exponential backoff (`retry_count` keeps its existing "number of HTTP attempts per flush" semantics); other 4xx responses fail immediately without retrying, with the buffered lines retained and the error message unchanged.

#### Scenario: Successful batch flush
- **WHEN** the configured batch threshold is reached or the output closes with pending lines
- **THEN** one authenticated text/plain request is sent and the buffer is cleared only after a successful HTTP status

#### Scenario: Failed request is retryable
- **WHEN** all configured attempts fail or return a non-success status
- **THEN** `write`/`close` returns a connection error and the pending lines remain buffered

#### Scenario: Deterministic 4xx fails immediately
- **WHEN** the write endpoint responds 401 or 404 on the first attempt
- **THEN** the flush fails immediately without further attempts, the pending lines remain buffered, and the error message matches the single-shot failure text

#### Scenario: Transient 5xx is retried with capped backoff
- **WHEN** the write endpoint responds 503 and any `retry_count` (including oversized values) is configured
- **THEN** attempts are retried with exponential backoff capped at the shared `max_delay_ms`, with no overflow, panic, or wrap for any configured value
