## 1. Core: resource declaration

- [x] 1.1 `JobResourceSpec { cpu_millicores: Option<u32>, memory_bytes: Option<u64> }` on JobSpec (`resources`, default None) with serde + validation (non-zero, at least one field)
- [x] 1.2 Parsing round-trip test (declared and absent); absent keeps all existing behavior

## 2. Hub: accounting, feasibility, ranking

- [x] 2.1 Agent: `cpu_cores` in ResourceSnapshot + `node_cpu_cores` gauge; Hub allowlist entry
- [x] 2.2 Allocation recompute helper: for desired-running jobs with declared resources, per-node (cpu_millicores, memory_bytes) from placement order/previous nodes × per-node assignment counts (stateless recompute)
- [x] 2.3 Feasibility gate in reconcile_job for declared jobs: effective fit vs node_cpu_cores×1000 and 90% memory total; explicit insufficient-capacity error when no candidate fits; gauge-less nodes exempt
- [x] 2.4 Ranking: headroom key subtracts allocations (effective headroom); existing rank tests updated/extended
- [x] 2.5 Tests: multi-job accounting shares, loaded-node skip, explicit error, effective-headroom ordering, undeclared zero-regression

## 3. Agent: dedicated bounded runtime

- [x] 3.1 Declared-cpu jobs spawn their kernel on a dedicated multi-thread runtime with max(1, ceil(millicores/1000)) workers, owned by JobTask and shut down on stop; undeclared jobs keep the shared runtime
- [x] 3.2 Test: worker count derived from declaration; runtime torn down on stop; undeclared path unchanged

## 4. Gates + docs

- [x] 4.1 `cargo test --workspace --all-targets` green; clippy no new warnings
- [x] 4.2 Docs (en + zh-Hans): `resources` fields, per-task semantics, scheduling behavior, isolation boundary (worker-level, not cgroup)
- [x] 4.3 `pnpm docs:check` passes
