## ADDED Requirements

### Requirement: The lookup processor enriches rows with dimension-table columns

The system SHALL provide a processor registered under the type name `lookup` that, for each input row, resolves the row's lookup key against a configured dimension-table backend and appends the configured result columns to the output batch. Output column values MUST be aligned with input rows by position, and the output batch MUST contain all input columns (including `__meta_` metadata columns) unchanged, plus the configured lookup columns.

#### Scenario: Enrichment appends mapped columns row-aligned

- **WHEN** a batch of N rows is processed with key column `user_id` and columns configured as `user_name` and `tier`, and the backend returns a record for every distinct key
- **THEN** the output batch has N rows and contains the input columns plus `user_name` and `tier`, where each row's appended values come from the backend record for that row's key

#### Scenario: Metadata columns are preserved

- **WHEN** the input batch carries `__meta_source` / `__meta_partition` / `__meta_offset` columns
- **THEN** the output batch carries the same metadata columns with unchanged values

### Requirement: Key extraction supports a closed set of column types

The processor SHALL accept key columns of type Utf8, Int64, UInt64, or Boolean and SHALL normalize each key value to its canonical string form (integers in decimal notation, booleans as `true`/`false`). When multiple key columns are configured, the processor SHALL form the composite key by joining the normalized values in configured order with the unit-separator control character (U+001F). Because the input schema is only known when a batch arrives, configuration naming a key column of any other data type (including floating-point types) MUST be rejected when the first batch is processed, with an error reporting the column name and offending type. A key cell that is null SHALL be treated as a miss for that row (following the missing-key semantics) without issuing a backend request.

#### Scenario: Composite integer key

- **WHEN** key columns are configured as [`tenant_id` (Int64), `user_id` (Int64)] and a row holds `tenant_id = 42` and `user_id = 7`
- **THEN** the lookup key resolved for that row is the composite string `42\u{1f}7`

#### Scenario: Unsupported key type fails on first batch

- **WHEN** a configuration names a Float64 column as a key column and the first batch arrives
- **THEN** processing fails with an error naming the column and its data type

#### Scenario: Null key cell follows missing semantics

- **WHEN** a row's key column value is null
- **THEN** no backend request is issued for that row and the missing-key semantics apply to it

### Requirement: Cache hits avoid backend requests within the TTL

The processor SHALL maintain a per-instance in-process cache keyed by the normalized lookup key, storing both hits and misses (negative caching) for a configurable TTL (`ttl_ms`, default 60000; `negative_ttl_ms` overrides the TTL for misses). Within the TTL window of a key's resolution, the processor MUST NOT issue a backend request for that key. Cached entries MUST expire after their TTL elapses, after which the next occurrence triggers a fresh backend resolution.

#### Scenario: Repeated key within TTL resolves from cache

- **WHEN** two batches within the TTL window each contain rows whose key is `u1`
- **THEN** the backend receives a request for `u1` only once, and the second batch's rows are enriched from the cached record

#### Scenario: Miss is negatively cached

- **WHEN** a key `u404` resolves to no record and further rows with `u404` arrive within `negative_ttl_ms`
- **THEN** no additional backend request is issued for `u404` while the negative entry is valid, and those rows follow the missing-key semantics

#### Scenario: Expired entry re-resolves

- **WHEN** a key's cached entry has exceeded its TTL and a new row with that key arrives
- **THEN** the processor issues a fresh backend request for the key

### Requirement: Cache is bounded

The cache SHALL evict entries so that the number of stored entries never exceeds `max_entries` (default 100000). Upon reaching the bound, the processor MUST first discard expired entries and, if still over the bound, evict the least-recently-used entries. The bound MUST apply to hits and negative entries alike.

#### Scenario: Capacity bound holds under distinct-key flood

- **WHEN** more than `max_entries` distinct keys are resolved over successive batches
- **THEN** the cache never holds more than `max_entries` entries and processing continues without error

### Requirement: Batch resolution deduplicates keys and bounds concurrency

For each input batch, the processor SHALL resolve only the set of distinct keys not served by cache, issuing at most one backend request per distinct key. Concurrent in-flight backend requests MUST NOT exceed `concurrency` (default 8), and results MUST be applied to rows in input order regardless of completion order.

#### Scenario: Duplicate keys collapse to one request

- **WHEN** a batch of 1000 rows contains only 3 distinct keys, none cached
- **THEN** exactly 3 backend requests are issued and all 1000 rows receive their enrichment

#### Scenario: Concurrency cap is respected

- **WHEN** a batch contains 50 distinct uncached keys and `concurrency` is 8
- **THEN** no more than 8 backend requests are in flight at any moment

### Requirement: HTTP backend resolves keys through a URL template

The `http` backend SHALL resolve each key by substituting it into the configured URL template at the `{{key}}` placeholder, with the key percent-encoded unless `encode_key` is false, and applying the configured method (default GET) and headers. An HTTP 404 response SHALL resolve the key to no record (a miss), which participates in negative caching; any other non-2xx status, a transport error, or a timeout (`timeout_ms`, default 30000) MUST be treated as a backend error per the failure semantics requirement. A 2xx response body MUST be parsed as JSON, and each configured column is extracted from the record by its dot-separated `path` and coerced to the column's configured type (`utf8`, `int64`, `float64`, or `bool`; default `utf8`); an absent path or a value not coercible to the configured type yields a null cell. The cached record SHALL be the extracted column values, not the raw response.

#### Scenario: URL template substitution

- **WHEN** `url` is `http://dimsvc/users/{{key}}` and the key is `u1`
- **THEN** the backend issues `GET http://dimsvc/users/u1`

#### Scenario: Key is percent-encoded by default

- **WHEN** a key value contains `/` or `?` and `encode_key` is not disabled
- **THEN** the substituted URL percent-encodes those characters so the key cannot alter the URL structure

#### Scenario: Typed column extraction with absent path

- **WHEN** the configured column `age` has type `int64` and path `profile.age`, and the response record for some key lacks `profile.age`
- **THEN** the `age` cell for rows with that key is null, and rows with other keys are unaffected

#### Scenario: Type-coercion failure yields null, not error

- **WHEN** the response value at a column's path is the string `"abc"` while the column type is `int64`
- **THEN** that cell is null and the batch is not failed

#### Scenario: 404 resolves to a miss

- **WHEN** the dimension service responds with HTTP 404 for a key
- **THEN** that key resolves to no record, following the missing-key semantics and negative caching

#### Scenario: Non-2xx other than 404 is a backend error

- **WHEN** the dimension service responds with HTTP 500
- **THEN** the backend error semantics apply (see "Backend errors fail closed by default")

### Requirement: Missing keys follow configured missing semantics

When a key resolves to no record (a miss), the processor SHALL, by default (`on_missing: null`), emit the row with all lookup columns null. When `on_missing: discard` is configured, the processor MUST drop the row from the output; the output batch MUST then have fewer rows than the input batch, which is a processing decision rather than a failure and MUST NOT affect acknowledgement of the input.

#### Scenario: Default missing behavior keeps the row with null columns

- **WHEN** a key resolves to no record and `on_missing` is unset
- **THEN** the row appears in the output with every lookup column null

#### Scenario: Discard drops the row without failing

- **WHEN** `on_missing: discard` and a batch of 10 rows contains 2 rows whose keys resolve to no record
- **THEN** the output batch has 8 rows and processing succeeds

### Requirement: Backend errors fail closed by default

When any backend request fails (transport error, timeout, or non-2xx status), the processor SHALL fail the batch with an error by default. When `on_backend_error: null` is configured, the processor SHALL instead treat the affected keys as misses for the current batch (following the missing-key semantics) and MUST NOT cache the error as a negative entry.

#### Scenario: Default failure propagates

- **WHEN** the dimension service is unreachable and `on_backend_error` is unset
- **THEN** the processor returns an error and the chain's error path handles the batch per the at-least-once contract

#### Scenario: Degrade-to-null mode continues without caching the error

- **WHEN** `on_backend_error: null` and the backend request for key `u9` times out
- **THEN** rows with key `u9` are emitted with null lookup columns, no negative entry is stored for `u9`, and the batch succeeds

### Requirement: Configuration is validated at build time

The processor SHALL reject at build time any configuration that: omits required fields (`key_columns`, `backend`, or the backend's required fields such as `url` and at least one `columns` entry); names a key column with an unsupported data type; or names an unknown backend kind. Because the input schema is only known when a batch arrives, a lookup column whose name collides with an existing input column (including `__meta_`-prefixed columns) SHALL be rejected at process time on the first batch, with an error naming the duplicated column. The registered component metadata schema SHALL declare these fields so `--validate` and IDE completion reflect the contract.

#### Scenario: Missing url fails validation

- **WHEN** an `http` backend configuration omits `url`
- **THEN** component build fails with an error identifying the missing field, and `--validate` reports it

#### Scenario: Unknown backend kind fails validation

- **WHEN** `backend` names an unregistered backend kind
- **THEN** component build fails with an error naming the unknown kind

#### Scenario: Column name collision is rejected at process time

- **WHEN** a configured lookup column name equals an existing column of the arriving batch
- **THEN** the processor returns an error naming the duplicated column, and the batch follows the chain's error path
