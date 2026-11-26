# Changelog

All notable changes to ArkFlow are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the
project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added — Engine & Durability

- Object-store (`s3`) WAL segment retention and crash-safe reclamation
  (`durability.backend.object_store.retention`: `enabled`, `max_segments`,
  `max_age`, `max_bytes`). By default (`enabled: true` with no explicit caps),
  sealed segments wholly behind the rewind-safe committed floor
  (`min(manifest.cursor, reclaimable_hwm)` updated via `mark_committed`) are
  reclaimed asynchronously on a decoupled, cancellable background loop using a
  manifest-first `pending_deletes` protocol. `manifest.json` now persists
  backward-compatible v2 per-segment metadata (`segment_meta`), allowing
  startup recovery and replay reads to skip `GET` requests for covered segments
  and `pending_deletes`.

### Added — Engine build

- Connector families are gated behind cargo features on `arkflow-plugin`
  (mirrored by the `arkflow` binary and `arkflow-server`): `connectors-sql`,
  `connectors-kafka`, `connectors-python`, `connectors-mongodb`,
  `connectors-pulsar`, `connectors-hdfs`, `connectors-mqtt`,
  `connectors-nats`, `connectors-redis`, `connectors-websocket`,
  `connectors-modbus`, `ai-vector`, `processors-vrl`, and
  `codecs-protobuf`. The default feature set enables every family, so
  default builds register exactly the same components as before — no
  behavior change. `--no-default-features --features engine-minimal` builds
  a minimal binary with the core engine plus the light component set (see
  "Build profiles" in the install guide). In a build without a family,
  configs referencing its types fail validation naming the feature, e.g.
  `Unknown input type: kafka (requires the 'connectors-kafka' feature)`.
  CI compiles the reduced feature sets and structurally asserts the minimal
  dependency tree excludes every gated family.

### Added — Control plane

- `arkflow-server` accepts `--config <FILE>`: a full startup configuration
  file (YAML/JSON/TOML by extension) covering every `ServerConfig` field
  (including fields no environment variable can set — `api_prefix`, health
  paths, `cors_origins`, `session_ttl_ms`), a `hub.operator_token` section,
  the `ha` block, and `logging`/`observability`. Secret references
  (`${env:}`/`${file:}`/`${secret:}`) resolve at load; unknown top-level
  keys fail startup naming the key; explicitly set environment variables
  override the file, so env-only deployments (including the Helm chart)
  behave exactly as before.
- The Hub binary now initializes a tracing subscriber at startup: by
  default it logs to the console at `info` (election, lease, session,
  webhook, and HTTP `TraceLayer` output were previously invisible). When
  `observability.tracing` is enabled, Hub spans export over OTLP/HTTP-JSON
  (off by default; exporter build failures never block startup) and
  buffered spans are flushed on graceful shutdown. New environment
  variables: `ARKFLOW_LOG_LEVEL`, `ARKFLOW_OTEL_TRACING_ENABLED`,
  `ARKFLOW_OTEL_ENDPOINT`, `ARKFLOW_OTEL_SERVICE_NAME`.
- `RUST_LOG` (EnvFilter directives) is now honored by both the engine and
  the Hub as an override on top of the configured log level, with the
  precedence chain `RUST_LOG` > `ARKFLOW_LOG_LEVEL` > `logging.level` >
  default. An unparsable value warns and falls back to the static level.
  This makes the `RUST_LOG=info` usage documented in the Kubernetes guide
  actually effective.

### Changed — Control plane

- **BREAKING (behavior fix)**: the `arkflow-server` serve path now rejects
  unknown flags, stray positional arguments, and a missing `--config`
  value with a usage message and exit code `2`; previously any unknown
  first argument silently started the Hub. Unreadable/unparseable config
  files also exit `2`. Invalid environment-variable spellings (booleans
  outside `1/true/yes|0/false/no`, non-numeric limits) now warn on stderr
  instead of being silently dropped; sub-floor TTL values (below 1000 ms)
  do not apply and the underlying value stands.

- Audit history now enforces the existing `ReadAudit` permission and `audit`
  resource scope (admin/viewer allowed; operator returns `403`). Queries
  support exact `actor`/`action` filters and inclusive `from_ms`/`to_ms`
  bounds before pagination; inverted ranges return `400 invalid_audit_query`.
- Job operation audits carry the authenticated principal for direct operator
  dispatches, including dispatches deferred until a target reconnects;
  operations with no originating request use `system`. Optional operation actor
  metadata survives persistence and retry; historical operation JSON remains
  readable.
- Job-level pause/resume (`POST /api/v1/jobs/{id}/actions/pause` and
  `/resume`, plus `PUT /jobs/{id}/desired-state {"state":"paused"}`):
  pausing is a durable quiesce, not a bare stop. The Hub captures a recovery
  artifact at the pause generation — a checkpoint for
  `recovery: latest_checkpoint` (retention-pinned until the pause clears), a
  savepoint for `latest_savepoint`; `recovery: fail` Jobs are rejected with
  `pause_requires_recovery_policy` — and only then stops the Job. A Job whose
  spec declares no `checkpoint` configuration skips the capture and stops
  directly, so its resume is a cold start. `desired_state` gains the third
  value `paused` (Job creation still accepts only `running`/`stopped`), and Job
  responses gain additive `pause_phase`
  (`capturing`/`stopping`/`paused`/`resuming`), `pause_artifact_id`,
  `pause_prev_checkpoint_id`, `pause_retry_count`, `paused_at_ms`, and
  `paused_by` fields. A paused Job converges only when the phase is `paused`
  and every target node observed `stopped` — an offline node keeps it
  reconciling — and a paused Job is fenced from start/re-placement. Resume
  bumps the generation and restores from the pause artifact; events processed
  between the artifact barrier and teardown completion replay (transactional
  sinks exactly-once, non-transactional at-least-once). Pause and upgrade
  orchestration are mutually exclusive (`job_paused` /
  `orchestration_in_progress`), and the mutations require a durable-storage
  Hub (`503 pause_requires_durable_storage` in volatile mode). Audited as
  `job.pause` / `job.resume`, counted in
  `arkflow_job_pause_total{outcome=requested|paused|resumed|failed|rejected}`,
  and broadcast as `job.pause_requested` / `job.paused` /
  `job.resume_requested` / `job.resumed` / `job.pause_failed` events. Storage
  schema v4 (SQLite/PostgreSQL) adds the pause columns and migrates
  automatically on open.
- Per-credential request rate limiting on both control planes
  (`health_check.rate_limit` / `ARKFLOW_RATE_LIMIT_*`): token buckets keyed on
  a hashed credential (per operator credential or browser session, per agent
  node; Bearer and legacy query-token channels share one node bucket),
  capacity-bounded bucket table with a catch-all lane, `429` + `Retry-After`
  rejections outside the audit stream, and the
  `arkflow_control_plane_rate_limit_rejections_total{plane,key_kind}` metric.
  Enabled by default with limits far above legitimate traffic;
  `enabled: false` restores unlimited handling.
- Configuration approval gate: `control_api.approval_required` (default
  off) gates every configuration rollout creation path — batch creation,
  node-level apply, and rollback — behind approval requests decided by a
  second principal (`/api/v1/config-approvals`). Approvals create the
  rollout as the approver, survive Hub restarts, adopt (never duplicate) a
  rollout a crashed approval created, expire pending requests after
  `approval_ttl_ms`, and audit submit/approve/reject with scalar metadata.
  Rollback is exempt only for versions previously applied to the affected
  nodes. Console gains an approval queue page and a policy-aware publish
  flow.
- Control-plane store backup and restore (`arkflow-server backup --from
  <store> --to <file>` / `restore --from <file> --to <store>`): consistent
  single-file SQLite-format snapshots from either backend (SQLite via
  `VACUUM INTO` on the storage actor, PostgreSQL via one repeatable-read
  transaction), fail-closed restore into an empty target only (every
  non-empty table named), schema-version compatibility checks, per-table
  row-count verification, identity-sequence reset, and cross-backend
  restores. The restore is atomic and blocks writers for its duration: the
  SQLite target is restored in one immediate transaction and the PostgreSQL
  target locks every business table `ACCESS EXCLUSIVE NOWAIT` for a single
  all-or-nothing transaction, so an active Hub transaction fails the
  restore with a busy error and a failure leaves nothing behind. An idle
  Hub is not detected — stop every Hub instance against the target first.
  The lease table is never
  copied — a restored Hub rebuilds its lease through the existing CAS path,
  and restoring never mutates the archived snapshot (it opens a working
  copy). Snapshots contain configuration payloads verbatim; handle them
  with database-level secrecy.
- Fixed the `migrate` tool silently dropping webhook subscriptions:
  `cp_webhooks` was missing from the shared table copy order, so a
  SQLite-to-PostgreSQL migration lost every subscription. The table now
  participates in the order shared by `migrate`, `backup`, and `restore`,
  and table copies coerce values to the target column types (SQLite
  integer booleans into PostgreSQL `BOOLEAN`, typed NULLs into
  `BIGINT`/`BOOLEAN`/`float8`/`bytea`).
- Managed user directory and per-user credentials on the Hub
  (`GET/POST /api/v1/users`, `GET/PATCH/DELETE /api/v1/users/{id}`,
  `GET/POST /api/v1/users/{id}/credentials`,
  `POST /api/v1/credentials/{id}/rotate`, `DELETE /api/v1/credentials/{id}`,
  `POST /api/v1/users/{id}/revoke-tokens`). Users carry exactly one role
  (`admin`/`operator`/`viewer`) plus resource scopes and an optional OIDC
  subject binding; every route requires the new `manage_users` action (admin
  only). Issued credentials are opaque `ark_<credential_id>_<secret>` bearer
  tokens whose plaintext appears exactly once — the store keeps only
  `SHA-256(credential_id ‖ 0x00 ‖ secret)` with constant-time comparison.
  Authentication resolves static operator credential → managed credential →
  browser session → OIDC JWT; a failed managed token is a hard 401 and never
  falls back to OIDC, and identity storage errors deny the request
  (fail-closed). Invariants: at most 1024 users, 16 active credentials per
  user, one user per OIDC subject, and the last enabled admin cannot be
  disabled/deleted/demoted (`last_admin_required`) unless a static operator
  credential exists as break-glass. Rotation revokes the old credential in
  one mutation; revocation is idempotent; identity mutations are audited as
  `user.create`/`user.update`/`user.delete`/`user.revoke_tokens`/
  `credential.create`/`credential.rotate`/`credential.revoke` with scalar
  metadata only. Storage schema v5 (SQLite/PostgreSQL) adds `cp_users`,
  `cp_credentials`, and `cp_sessions` and migrates automatically on open;
  `cp_users`/`cp_credentials` join the backup/migrate/restore table order.
- Durable browser sessions and OIDC token revocation: with durable storage,
  OIDC login sessions persist in `cp_sessions` (8-hour TTL) instead of living
  in memory, so they survive a Hub restart and can be revoked centrally —
  logout deletes the row, and disabling/deleting the user or
  `POST /users/{id}/revoke-tokens` invalidates it on the next request. A
  directory-bound `oidc_subject` makes the user record authoritative for role
  and scopes; a disabled user is 401, a token whose `iat` predates the user's
  `tokens_valid_after_ms` (60-second skew tolerance) is rejected, and a bound
  token without `iat` is rejected. Unbound subjects keep claim-mapped
  behavior unless `ARKFLOW_OIDC_MANAGED_USERS_ONLY=true` requires binding.
  `revoke-tokens` does not revoke managed credentials (use rotate/revoke).
  Session rows are pruned by the periodic maintenance task and are
  deliberately excluded from backup/restore so recovery never resurrects a
  login.

### Added — Engine

- `state.max_staged_bytes` caps the state journal's uncommitted staged
  mutations per Job (default 64MB). Raise it when rarely-firing
  high-cardinality windows stage more uncommitted state than the default
  allows; the benchmark window tiers do exactly that alongside their
  `max_pending_transactions` raise.

### Changed — Connector error contract

The kernel's error-variant semantics (EOF = graceful end of a bounded
source, Disconnection = reconnect, everything else = fatal) are now a
normative spec (`connector-recovery-contract`) pinned by a
cross-connector consistency test; the following connector mappings were
aligned to it, two of which change runtime behavior:

- **Pulsar input**: a subscription stream ending (`Ok(None)`) no longer
  maps to `EOF`, which silently ended the whole Job as if the data had
  legitimately ended; it now maps to `Disconnection` and the engine
  reconnects. Teardown paths (receiver closed / cancelled) keep `EOF`.
- **Multiple inputs**: an interior channel closing while a child source is
  reconnecting no longer re-wraps into a fatal `Process` string; it now
  surfaces as `InputChannelClosed`, which the kernel handles structurally
  (the surviving chain drains instead of the Job dying).
- SQL/pgvector outputs: write-path failures are now classified — transient
  transport failures (connection loss, SQLSTATE `08xxx`/`40xxx`/`57xxx`,
  MySQL 2006/2013) report `Disconnection`; deterministic failures (syntax,
  constraints, type errors) stay `Process`. Runtime connection failures
  report `Connection` instead of `Config` (an invalid connection string
  still reports `Config`).
- NATS/Redis outputs: a publish transport failure reports `Disconnection`
  instead of `Process` (aligned with the MQTT/Kafka siblings; deterministic
  failures are unchanged).
- Redis input: the temporary/permanent error classifier is now structural
  (driver error kinds and exact protocol codes). A message merely
  containing "invalid" no longer flips an otherwise transient error to
  permanent (which killed the Job instead of reconnecting).
- MongoDB output: the not-connected guard reports `Connection(msg)` instead
  of a bare `Disconnection`, consistent with the other outputs.

### Changed — Connector resilience (shared retry skeleton)

- The four AI processors (`embedding`, `llm`, `vector_search`,
  `milvus_search`) now retry transient failures (429/5xx/transport) with
  capped exponential backoff by default. New optional `retry_count`
  config field (default 3, 0 = single attempt, matching the qdrant/milvus
  outputs' field semantics). Previously a single 429/5xx failed the whole
  batch immediately. Error messages and deterministic-failure behavior
  (4xx, validation errors) are unchanged.
- Qdrant/Milvus outputs and the Pulsar input's receive backoff now use one
  shared retry implementation (`retry` module) instead of hand-rolled
  loops. The qdrant/milvus hand-rolled delay `100 * 2^attempt` had no cap:
  any `retry_count >= 64` overflowed (debug panic). Delays are now capped
  at 5s for any configured count, and `RetryConfig` gains optional
  `jitter`. Default retry counts are unchanged (qdrant 3, milvus 0).
- Input components' internal delivery channels align on a shared capacity
  of 1024 (kernel edge-channel capacity) instead of per-file `1000`
  magic numbers.

### Changed — Connector resilience (retry adoption sweep)

- The InfluxDB and HTTP outputs now classify failures through the shared
  retry implementation: transport errors, `429`, and `5xx` are retried
  with capped exponential backoff (delays cap at 5 s), while other `4xx`
  responses fail on the **first attempt** with the error message
  unchanged. Previously every non-success status (a bad token, a wrong
  path) was retried until the budget ran out. The InfluxDB backoff also
  loses its last-surviving overflow hazard (`100 * 2^attempt` panicked
  for `retry_count >= 63`; every configured value is now safe).
- The HTTP output's `retry_count` is now optional (default `3`,
  matching the qdrant output and the AI processors); it was previously a
  required field. The InfluxDB `retry_count` keeps its existing
  "total HTTP attempts per flush" meaning (documented in its schema).
- The MQTT output's lazy reconnect uses the shared backoff schedule with
  byte-identical delays (1 s, 2 s) — no behavior change, one less
  hand-rolled loop.
- The Kafka input's tombstone-settlement retry now uses the shared
  exponential schedule (200 ms, 400 ms, 800 ms, 1.6 s — capped at 5 s)
  instead of a linear 200/400/600/800 ms; the restore-seek retry shares
  the same scheduling. Cancellation and at-least-once semantics are
  unchanged.
- NATS input: the JetStream pre-warm path no longer sleeps 100 ms after
  reporting a `Disconnection` (no retry followed the sleep — the engine
  owns reconnection), and a JetStream **receive error** now stops the
  pre-warm batch after surfacing one `Disconnection` instead of one per
  failed message. Codec decode failures keep their original behavior: the
  codec error is queued per message and the batch continues, so operators
  can tell a bad payload from a connection failure.

### Changed — Documentation, agent guidance, and chart

- `CLAUDE.md` no longer restates workspace facts (it claimed Rust 1.88 vs
  the required 1.97, three crates vs four, 12 inputs vs 13, and window
  `buffer:` YAML the compiler rejects); it now defers to `AGENTS.md` as the
  single checked-in agent authority, and the `documentation-accuracy` spec
  forbids the agent instruction entry from duplicating facts that drift.
- The plugin development guide's Rust snippets compile in CI: a
  `#[cfg(doctest)]` marker in `arkflow-plugin` embeds
  `docs/docs/develop/plugins.md` via `#[doc = include_str!]`, so
  `cargo test --doc -p arkflow-plugin` compiles every Rust fence against
  the real API (the drifted `Option<&String>` builder signature now fails
  with E0053). The `src/mod.rs` wiring path is corrected to the real
  `register_components()` / `lib.rs::initialize()` layout, and the zh-Hans
  mirror is fixed in lockstep.
- The Helm chart's `appVersion` (was `0.5.0` against workspace `0.6.0`) is
  bumped to `0.6.0`, and the `helm.yml` publish job now fails the release,
  naming all three values, when `appVersion`, the workspace version, and
  the release tag diverge.

### Added — Component metadata

- A conformance gate (`crates/arkflow-plugin/tests/metadata_conformance.rs`)
  checks every registered component schema against the config struct its
  builder deserializes: field coverage in both directions, `required`,
  `default`, and string-enum values, following `#[serde(flatten)]`, nested
  objects and arrays, and serde's externally, internally, and untagged enum
  layouts. The struct is read from source, so the field list cannot go
  stale; every component with a schema must be mapped, and the two exemption
  lists (`HIDDEN_FIELDS`, `BUILDER_REQUIRED`) fail the gate once an entry is
  no longer needed, and a `BUILDER_REQUIRED` entry is probed through the
  real builder (the example itself must build, and the builder must refuse a
  config without the key, naming it). Generic field
  types such as `Expr<String>` are resolved by name, so tagged-expression
  properties are checked too. An advertised default is compared with the
  value serde produces (the type's zero, or the literal the default
  function returns — a default of the field's own before the container's,
  and the function is found through its path); a key demanded by only some
  `anyOf`/`oneOf` branches does not count as required, a key the enclosing
  node demands does; every branch that selects a variant is checked — a
  tagged branch without a selector included — each has to demand the tag,
  a default the enclosing node states counts for every branch, and a
  variant may not be selected twice under `oneOf`; a tag value that is not a
  decodable variant,
  an untagged branch that demands less than any variant, a flattened type
  the gate cannot read, and a mapped component with an opaque root schema
  are all reported. serde's `rename_all` conversions — different for fields
  and variants, and ASCII-only — are cross-checked against the real derive.
- Example configs cover the newly documented fields: `pulsar_tls_example.yaml`
  (the Pulsar `tls` block on an input and an output) and `concurrency` in
  the `milvus_search` example.

### Changed — Component metadata

The gate found drift in thirteen components; the IDE-facing schema and the
committed inventory now match what serde accepts:

- `kafka` input: `transactional_offsets` is in the schema (the IDE no longer
  rejects it), and omitting `start_from_latest` now means `false` — the
  default the schema always advertised — instead of failing with a
  missing-field error. Its description no longer says the flag makes
  committed offsets ignored: it only sets the offset reset policy — with a
  single-consumer subscription a committed group offset still in range wins
  on restart; under an explicit partition assignment committed group offsets
  are not used. A config that relies on the
  default fails to build again on the previous version: add
  `start_from_latest: false` before rolling back.
- `pulsar` input and output: the `tls` block (`certificate_chain_file`,
  `hostname_verification`) is in the schema, and `certificate_chain_file` is
  described as what the driver does with it — the chain the broker
  certificate is verified against, not a client certificate for mTLS. The
  component pages said the same wrong thing and are corrected.
  The input's
  `subscription_type` lists the values serde accepts (`exclusive`, `shared`,
  `failover`, `key_shared`; the capitalized spellings it listed were
  rejected at runtime).
- `milvus_search`: `concurrency` is in the schema.
- `pulsar` output: the `topic` schema requires `value` with `type: value`
  and `expr` with `type: expr`; the flat schema let the IDE pass an
  expression object carrying no expression.
- `sql` processor: the `temporary_list[].key` schema is the tagged
  expression the docs and examples already use (`type: value` or
  `type: expr`); it required `value` and omitted the tag, so the IDE
  rejected the documented form.
- `http` input: the schema no longer documents `method` and `interval` —
  the polling client mode does not exist — the description no longer
  promises `PUT` (the server routes `POST` only), and a config carrying
  either key
  now fails at build with an explanatory error instead of being silently
  ignored while the input runs as a server.
- `http` output: omitting `method` means `POST` (the advertised default);
  `timeout_ms` is marked required, as serde requires it. A config that
  relies on this fails to build again on the previous version: add
  `method: POST` before rolling back.
- `kafka` output: `compression` no longer offers `zstd`, which the enum
  never accepted. `nats` output: the `jet_stream` mode no longer documents
  a required `stream` key that nothing reads.
- The `session_window` and `tumbling_window` buffers mark the `join`
  block's `query` and `codec` as required; the `schema_registry` codec marks
  `auth.type` as required. The `redis` input description no longer claims
  stream consumer groups, and the `http` input description no longer
  advertises polling.

### Fixed — Connectors

- The `http` output builds its client through the shared
  `http_util::build_http_client`, so — like every other HTTP component — a
  loopback URL (`localhost`, `127.0.0.1`, `::1`) bypasses the system proxy
  instead of being forwarded to it. On a machine with a system proxy this
  also made the refused-connection test fail intermittently with the
  proxy's `502`. The shared client now recognizes every loopback address
  (`127.0.0.0/8`, `::1`), not only `127.0.0.1`.
- The `sql` output rejects an empty quoted part in `table_name`
  (`analytics.""`) when the output is built, instead of letting the
  server reject the zero-length identifier on the first write.

### Security — SQL output

- Every identifier the `sql` output writes into SQL text is quoted for the
  target dialect with embedded quotes doubled: column names, `upsert_keys`,
  conflict assignments, and the table. Column names come from the batch
  schema and may be inferred from data; a name containing quotes, backticks,
  or `); DROP TABLE …; --` is written as that single identifier instead of
  being able to change the statement.
- `table_name` is quoted one dot-separated part at a time, so
  `analytics.events` keeps working: a bare part resolves exactly like the
  unquoted identifier it replaces (folded to lower case on PostgreSQL), and
  a part already written in the dialect's quotes is kept verbatim. A
  malformed name (empty part, unclosed quote, text after a closing quote,
  NUL byte) fails when the output is built, so `--validate` reports it. A
  column name containing a NUL byte fails the write.
- The pgvector output and `pgvector_search` share the same implementation;
  their generated SQL is unchanged.

### Fixed — Engine and control plane

- The in-memory Hub no longer lets a settled Job observation roll a
  concurrent desired-state mutation back: `observe_job` and `update_job`
  re-inserted a stale Job-record clone outside the write lock after their
  compare-and-set, which could resurrect an older generation and misattribute
  the generation's lifecycle operations to the previous mutation's actor (the
  `volatile_reconcile_pairs_each_generation_with_its_actor` flake on main).

## [0.6.0] — 2026-10-16

Everything below landed between v0.5.0 (2025-10-19) and this release. Entries
are grouped by user-perceivable area; internal refactoring is summarized
rather than listed commit-by-commit.

### Added — Engine and data plane

- Columnar `MessageBatch` over Apache Arrow with typed source metadata columns
  (`__meta_source`, `__meta_partition`, `__meta_offset`, ...) and full Kafka
  metadata support. (#1094, #1095, #1096)
- Unified execution kernel: all streams and jobs compile through one executor
  with bounded inter-chain edges and Notify-based backpressure signaling
  (replaces the per-stream ad-hoc runtimes). (#1196, #1222)
- Event-time processing: watermarks, tumbling/sliding/session windows, late
  event handling, and columnar window aggregation operators. (#1222)
- Stream join operator with keyed inner and outer joins. (#1256, #1266)
- Durable input WAL with crash recovery — local backend plus S3 object-store
  backend with concurrent writes and manifest write coordination; at-least-once
  replay and close-drain semantics. (#1178, #1183, #1186, #1191, #1192, #1193)
- End-to-end exactly-once: Kafka transactional producer (L2), transactional
  offset commit for Kafka→Kafka pipelines (L3), and single-transaction batch
  writes for the SQL output. (#1195, #1256)
- CDC ingestion via Debezium (MySQL/PostgreSQL/MongoDB) with tombstone and
  schema-evolution handling. (#1195)
- Schema Registry integration (Avro/Protobuf/JSON) with per-subject
  compatibility gates and Avro logical-type decoding. (#1195, #1227)
- Cross-node network shuffle data plane with split placement, transparent
  reconnect, failed-receipt aborts, and pending-replay byte budgets. (#1226,
  #1239, #1256, #1301)
- Distributed jobs: resource-aware placement and rebalancing, node resource
  reporting, key-group resharding on rescale, per-job resource quotas, atomic
  job upgrade orchestration. (#1244, #1256, #1264, #1265)
- Bounded buffering and timeouts across the reliability path: window buffer
  entry caps, checkpoint round deadlines, sink write and snapshot timeouts,
  watermark stall unfreezing on idle inputs. (#1275, #1278, #1301)
- Data-plane frame authentication (wire protocol v2): the session handshake
  exchanges fresh nonces in both directions and derives per-connection,
  per-direction session keys via HKDF-SHA256; every post-handshake frame
  (data, signal, receipt) carries a truncated HMAC tag over its header,
  payload, and a connection-level monotonic sequence number, verified
  before any payload parsing. Forged, modified, or replayed frames fail
  closed as deterministic protocol errors.
- Data-plane determinism hardening: authentication/protocol errors
  (forged or malformed frames, credential mismatch, nonce replay, quad
  mismatch) fail the attempt immediately instead of consuming the reconnect
  budget, and a batch whose Arrow memory alone exceeds the pending-replay
  byte budget fails the edge instead of livelocking abort-and-redeliver on a
  rewinding source.
- Data-plane resource/SLO tuning for agents: `ARKFLOW_DATA_PLANE_MAX_CONNECTIONS`,
  `ARKFLOW_DATA_PLANE_RECONNECT_ATTEMPTS`, `ARKFLOW_DATA_PLANE_RECONNECT_GRACE_MS`,
  `ARKFLOW_DATA_PLANE_REGISTRATION_GRACE_MS`, and
  `ARKFLOW_DATA_PLANE_MAX_FRAME_LEN_BYTES` (all optional, defaults unchanged);
  connection rejections are surfaced through a bounded counter with a
  throttled warning instead of an unconsumed channel.
- Data-frame IPC bodies decode without the intermediate full-body copy
  (shared-ownership `Bytes` buffers carry the IPC body into Arrow), removing
  the per-frame whole-body second copy; Arrow may still copy individual
  array buffers when alignment requires it.

### Added — Components

- Codec support (JSON/Protobuf/Avro/custom) across all input and output
  components. (#1098, #1099, #1104)
- MongoDB output (#1214) and a completed InfluxDB output (#1114, #1213).
- Security transports: Kafka SASL/SSL (#1246), TLS for MQTT, NATS, and Pulsar,
  authenticated network shuffle with mTLS (#1264).
- AI pipeline components: `embedding` and `llm` processors (OpenAI-compatible,
  streaming and tool use), vector store outputs for Qdrant, pgvector, and
  Milvus, plus `vector_search` / `pgvector_search` / `milvus_search`
  processors covering ingestion and retrieval end to end. (#1247)
- `${secret:}` references resolved from the environment with Hub-side dispatch
  to worker nodes. (#1247)

### Added — Control plane (Hub/Agent) and console

- Hub–Agent control plane: distributed job runtime, durable reconciliation,
  fleet rollout operations, job audit and command metrics. (#1200, #1203,
  #1204, #1216)
- Hub high availability: PostgreSQL storage backend, lease-based leader
  election, incremental failover, and lease-epoch write fencing. (#1256, #1264,
  #1272) Agent multi-Hub discovery and failover. (#1289)
- Authentication: operator tokens with three-role RBAC, OIDC as an OAuth2
  resource server (JWT bearer), browser authorization-code login, and console
  SSO integration. (#1247)
- Web console: redesigned UI with per-page modules, node drain/maintenance
  actions, fleet-wide audit view, zh-Hans internationalization, and honest
  operation progress/error reporting. (#1260, #1261, #1262, #1263, #1283)

### Added — Observability, deployment, and docs

- Data-plane Prometheus metrics and health probes for jobs and agents. (#1224)
- OpenTelemetry distributed tracing with OTLP export, batch-level spans, and
  cross-node trace propagation over checkpoint barriers. (#1247)
- Public benchmark suite reproducible with one cargo command. (#1247)
- Helm charts for engine and control-plane deployments with Hub/Console
  images. (#1300)
- Documentation site rebuilt around product-area navigation with a zh-Hans
  tree, generated JSON Schema and component inventory, and CI-enforced docs
  accuracy gates. (#1181, #1215, #1223, #1229)

### Changed

- **BREAKING**: cross-node shuffle frames are authenticated (wire protocol
  v2, not interoperable with v1). A mixed-version fleet fails the data-plane
  handshake fail-closed — stop distributed jobs, upgrade every Agent in the
  same window, then restart.
- **BREAKING**: the data-plane shared secret no longer falls back to the
  fleet-shared `node_token`. Network shuffle now requires an explicit
  `ARKFLOW_DATA_PLANE_SECRET` on every participating node; a node without
  one runs colocated-only (with a startup warning). Deployments relying on
  the fallback must set the variable before upgrading.
- Pending-receipt budget overflows fail the single delivery (branch aborted,
  at-least-once redelivery) instead of tearing the edge down, so transient
  congestion no longer escalates into reconnect or attempt failures.
- DataFusion upgraded from 47 to 54 (Arrow/API surface refresh). (#1187)
- **BREAKING**: Pulsar `tls.ca_file` is rejected at build time and removed
  from the configuration surface. The field was parsed and documented but
  never applied — the driver has no separate `ca_file` setting; a private-CA
  trust anchor is provided via `tls.certificate_chain_file`, which the
  driver installs to authenticate the server. Configs still carrying the
  key now fail startup with an error naming it and pointing at the
  supported option.
- Stream-level `codec:` is now rejected at build time for file, SQL, and
  Modbus inputs (the setting never worked); use component-level codecs. (#1284)
- `hub_url` became a list `hub_urls` for agent multi-Hub discovery. (#1289)
- Internal Rust API surface reduced ahead of the v1.0 semver freeze
  (builder `name` parameters now `Option<&str>`, `Temporary::get` takes
  string keys, `HealthCheckConfig` renamed to `NodeConfig` with grouped
  sub-configs, Hub-only secret dispatch helper moved to `arkflow-server`).
  Configuration files are unaffected: the YAML/JSON `health_check` section
  keeps its exact historical shape. (#1304)
- Error classification tightened: input channel closure has a dedicated
  variant, config parse errors carry line/column locations. (#1207, #1301)
- Schema-registry Avro decoding is ~20% faster: parsed schemas are cached
  behind an `Arc` instead of being deep-cloned per message. The public
  benchmark suite gained three `avro-decode-w5/w25/w100` scenarios that
  exercise the real codec decode path offline. (#1309)
- Kafka input aggregates buffered messages into multi-row read batches: the
  first message is awaited (blocking, cancellation-safe) and already-buffered
  messages drain without waiting, bounded by new `batch_max_rows` (default
  1024) and `batch_max_bytes` (default 8 MiB) fields — `batch_max_rows: 1`
  restores per-message batches. Metadata columns are attached in a single
  RecordBatch rebuild with per-row `__meta_partition/__meta_offset/__meta_key/
  __meta_timestamp/__meta_ext` values (key/timestamp appear as nullable
  columns only when some row carries one; `__meta_ingest_time` is
  batch-granular), codec decode runs once per batch with a per-payload
  fallback that preserves exact row↔metadata alignment, and acknowledgements
  settle per contiguous `(topic, partition)` segment whose compensation
  replays the whole segment — at-least-once semantics unchanged. (#1314)
- Avro decoding is substantially faster again on wide schemas: messages
  sharing a schema id now accumulate into one set of Arrow column builders
  instead of one single-row batch per message plus a concat copy.
  Multi-version mixed batches still merge through the union schema (rows
  grouped by schema id in first-appearance order); column nullability now
  consistently reflects the writer schema. (#1310)
- Protobuf schema-registry decoding gets the same columnar treatment:
  messages sharing a schema id accumulate into one multi-row batch with
  per-batch column plans and field-number value access, instead of one
  single-row batch per message plus a concat copy. Output and error
  behavior are unchanged; ~18x faster on a 26-field message timing run.
  (#1312)
- Hot-path mechanical batch: the standalone protobuf codec and the
  `protobuf_to_arrow` processor decode through the same columnar converter
  (one multi-row batch per decode call, no per-message schema construction);
  window aggregation normalizes narrow integer value columns (Int8/16/32,
  UInt*) once per batch instead of re-casting the whole column per row —
  wide batches were quadratic; and the kernel resolves per-chain
  metrics once at chain startup instead of twice per batch (lock + string
  alloc + map lookup). No observable behavior changes. (A fourth candidate,
  pinning streaming SQL sessions to one DataFusion target partition, was
  measured to regress GROUP BY throughput ~11% — multi-partition execution
  gives the aggregate intra-query parallelism that outweighs its overhead —
  and was dropped.)
- JSON decoding is ~1.5x faster on typical records (and ~1.3x faster on
  200-column records): schema inference no longer materializes a
  `serde_json::Value` tree per record (a borrowed-tree streaming inferrer
  mirrors arrow-json's merge state machine — same IndexMap containers with a
  fast hasher — and produces the byte-identical schema, pinned by differential
  tests against arrow-json's inference), and decode flushes one RecordBatch
  sized to the input instead of internal 1024-row chunks re-joined with a
  concat copy. Output schema, values, row order, and error behavior are
  unchanged for the `json` codec, `debezium_json` codec, and `json_to_arrow`
  processor. The public benchmark suite gained a `json-decode` scenario
  isolating the decode step.

### Fixed

- v1.0-readiness review closed 12 P1 defects: input read cancellation safety
  across MQTT/Pulsar/NATS/Redis/WebSocket (silent message loss on the regular
  path), S3 WAL async-path runtime panic, network shuffle dedup race, Kafka L3
  row-level offset attribution, SQL processor pool leak and hang, Pulsar
  output/input functional breakage and ack livelock, multiple-inputs lifecycle
  (task duplication, hot loop, unbounded channel), batch/memory buffer data
  loss on failure and close, HTTP input bind failure swallowed. (#1269, #1270,
  #1271, #1272, #1273, #1274)
- P2 robustness batches: console Error Boundary and API timeouts with SSE
  reconnect backoff, runtime manager stop/restart races, `input_name`
  invariant loss through derived batches, operator auth as route-level
  middleware with a 401 matrix, pagination overflow, storage actor isolation,
  placement plan caching, JWKS periodic refresh. (#1275, #1278, #1279, #1280,
  #1281, #1282, #1283, #1284, #1301, #1302)
- Kafka timestamp overflow/negative values (#1097); input reconnection logic
  (#1118); VRL/protobuf silent data loss (#1182); expression row-routing null
  handling fail-closed (#1284).
- Deep-audit correctness batch: object-store WAL acknowledgements now gate on
  segment sealing (the configured loss window becomes a replay window — no
  acknowledged entry is lost on node loss; flusher failures are counted and
  logged); JSON decoding infers its schema from the whole batch (no silent
  int truncation or dropped late fields); the `batch` processor defers
  acknowledgements until its merged emission is confirmed downstream and
  merges by schema normalization (`count` now counts message rows); the
  Kafka L3 offset bridge checks the synchronous `send_offsets` result,
  clamps committed offsets to the paired input's contiguous frontier,
  validates input/output pairing at startup, and no longer touches
  `store_offset` on undo; `TrackingAck` undo-after-abort is a terminal no-op
  and WAL parked acknowledgements carry a bounded 60s lease (silent-stall
  modes become explicit errors). (#1308)

### Security

- Lease fencing epochs enforced on the storage write path (stale leader
  writes rejected). (#1272)
- Standalone engine control API hardened to Hub parity: a non-loopback bind
  without a token refuses startup unless `insecure_local` is set explicitly
  (a serve failure now fails the process loudly instead of running the
  engine without its control API), and all local-plane endpoints — reads
  included — sit behind the default-deny Bearer middleware. (#1308)
- Console operator token excluded from image build context and repository;
  static-token mode documented as trusted-network-only with OIDC for
  production. (#1273)
- `${file:}` secret loading sandboxed. (#1282)
- Dependency security refreshes (Rust and Node trees, including Prometheus
  0.13→0.14 and async-nats 0.46→0.50 bumps). (#1298, #1299)

### Removed

- Legacy per-stream executors (single compute job runner, linear stream
  executor) — all execution goes through the unified kernel. (#1222)
