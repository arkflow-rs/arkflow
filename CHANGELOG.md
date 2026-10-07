# Changelog

All notable changes to ArkFlow are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the
project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] — accumulating toward v1.0

Everything below has landed since v0.5.0 (2025-10-19) and will ship together in
the next tagged release. Entries are grouped by user-perceivable area; internal
refactoring is summarized rather than listed commit-by-commit.

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
