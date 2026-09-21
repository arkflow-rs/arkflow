## 1. Dependency and constants

- [x] 1.1 Add `sysinfo` to `[workspace.dependencies]` (default features off, only needed components) and reference it from `crates/arkflow-server/Cargo.toml`
- [x] 1.2 Extend `ALLOWED_NODE_METRICS` in `crates/arkflow-server/src/hub.rs` with the four resource keys; hub unit test asserts they survive `sanitize_metrics`

## 2. Agent sampler

- [x] 2.1 Implement the resource sampler module in `crates/arkflow-server/src/agent.rs`: fixed-interval task publishing a fresh snapshot (values + `sampled_at_ms`) into a shared slot; CPU gauge only after the first full interval; memory gauges from first sample
- [x] 2.2 Spawn the sampler alongside the session loop and abort it on shutdown; a dead sampler must not affect the session `select` loop
- [x] 2.3 Merge fresh gauges into the report metrics map in `report()`; omit keys when snapshot missing or older than 2× sampler interval
- [x] 2.4 Unit tests: sampler publishes bounded vocabulary; freshness window omission; sampler failure leaves report intact

## 3. Hub-side exposure and integration tests

- [x] 3.1 Hub unit test: a report carrying resource keys populates the node metrics view
- [x] 3.2 Hub unit test: restart-with-storage clears gauges until the next report
- [x] 3.3 Extend the Hub metrics export test to cover a resource series carrying the `node` label and absent-node case
- [x] 3.4 Integration test in `crates/arkflow-server/tests/`: a real Agent session surfaces resource gauges at the Hub metrics endpoint

## 4. Validation

- [x] 4.1 `cargo test -p arkflow-server` passes
- [x] 4.2 `cargo clippy --workspace --all-targets` clean
- [x] 4.3 `cargo test -p arkflow-plugin --test docs_inventory_snapshot` unaffected (no registry changes) and `cargo test --workspace --all-targets` green
