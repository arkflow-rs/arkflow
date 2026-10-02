## 1. Manifest and lock

- [x] 1.1 On branch `deps/stage6-redb-4`: bump workspace `redb` from `"2"` to `"4"`, run `cargo update -p redb`, confirm the lock lands on 4.3.x as the single redb copy
- [x] 1.2 Inspect the lock diff for new duplicate stacks (extend the dependency-lock-hygiene guard only if a meaningful new dupe appears)

## 2. Compile-driven migration

- [x] 2.1 Fix all compile breakage (`ReadableDatabase` import for `begin_read`/`begin_write` in `wal/store.rs`, `state.rs`, and any other importer) until `cargo check --workspace` is clean
- [x] 2.2 Wrap the `Database::create` failure paths at `wal/store.rs` and `state.rs` with file-path + storage-format-boundary context so legacy v2 files (and any IO failure) fail loudly and diagnosably

## 3. Verification against the spec contract

- [x] 3.1 Run the WAL and state test suites unmodified (append/replay/trim, watermark advancement, close/reopen lock lifecycle) plus one new unit test pinning the create-failure error wrapper (path-naming, fail-loud)
- [x] 3.2 Run the dependency-lock-hygiene guard, `two_node_job_smoke`, `examples_validate`, `docs_snippets_validate`; `cargo clippy --workspace --all-targets` clean

## 4. Ship

- [ ] 4.1 Commit, open PR with the storage-format boundary called out as BREAKING in the description, land on CI green
- [ ] 4.2 Archive this change (`openspec archive`)
