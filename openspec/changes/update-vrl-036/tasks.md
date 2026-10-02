## 1. Manifest bump and resolution

- [x] 1.1 Bump `vrl` to `0.36` in `crates/arkflow-plugin/Cargo.toml`, resolve any feature renames, run `cargo update -p vrl`
- [x] 1.2 Inspect the lock diff for new duplicate major stacks (extend the dependency-lock-hygiene guard only if a meaningful new dupe appears)

## 2. Compile-driven migration

- [x] 2.1 Fix all compile breakage in `crates/arkflow-plugin/src/processor/vrl.rs` (`vrl::prelude`, `Program`, `VrlValue`, stdlib wiring) until `cargo clippy --workspace --all-targets` is clean
- [x] 2.2 Where a test expression uses a stdlib function removed upstream, rewrite the expression to an equivalent surviving function without changing the asserted behavior

## 3. Verification against the spec contract

- [x] 3.1 Run the full processor test suite; every existing `vrl-processor` requirement scenario passes unmodified (string round-trip, error observability, timestamp units, unsupported shapes)
- [x] 3.2 Run the dependency-lock-hygiene guard and the arkflow integration tests (`examples_validate`, `docs_snippets_validate`)
- [x] 3.3 Confirm the delta-spec scenarios: valid 0.30-era program still compiles; invalid source surfaces the VRL diagnostic on the existing error path

## 4. Ship

- [x] 4.1 Commit on a `deps/stage4-vrl-036` branch, open PR, land on CI green
- [ ] 4.2 Archive this change (`openspec archive`)
