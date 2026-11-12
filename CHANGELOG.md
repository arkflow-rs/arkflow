# Changelog

All notable changes to ArkFlow are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the
project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added — Control plane

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

- DataFusion upgraded from 47 to 54 (Arrow/API surface refresh). (#1187)
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
