## 1. Vocabulary and marker contract

- [x] 1.1 Verify the fence metastring approach against Docusaurus 3.9: add a
      marker to one block in a scratch page, run the docs build, confirm the
      metastring is not rendered or copied (fallback if broken: first-line
      comment markers; record the choice)
- [x] 1.2 Finalize the marker vocabulary (3 classifications, wrap kinds) and
      document it in `docs/DOCUMENTATION.md` with one example per kind
- [x] 1.3 Confirm the stub components accept minimal configs (inspect
      `input/generate`, `input/memory`, `output/drop`, `output/stdout`
      builders in the registry); pick the complements for wrap templates
      (chosen: `input/memory` + `output/drop`; `wrap` kinds finalized as
      input / output / processors / durability / engine)

## 2. Fast gate (Node, docs-check)

- [x] 2.1 Extend `docs/scripts/docs-check.mjs` with a fence scanner (reuse
      the existing inFence logic) that collects ```yaml blocks with their
      metastring and fails on missing or unrecognized classification; the
      error message states the exact marker to add
- [x] 2.2 Run `pnpm docs:check` and confirm it reports the current untagged
      corpus (expect ~129 blocks across 58 files)

## 3. Deep gate (Rust workspace test)

- [x] 3.1 Add `crates/arkflow/tests/docs_snippets_validate.rs`: walk
      `docs/docs`, extract classified yaml blocks (fence-state scan), parse
      the marker metastring, skip-until-tagged not permitted — untagged or
      unknown markers fail
- [x] 3.2 Implement the four wrap templates (input / output /
      processor-list / top-level-section) on the shared minimal skeleton,
      using the complements chosen in 1.3; unit-check each template against
      one known-good snippet
- [x] 3.3 Validate `full` and wrapped `fragment` blocks through
      `arkflow_core::config::EngineConfig` + `validate_config` (mirror
      `examples_validate.rs` error reporting: file, block index, error);
      validate `foreign` blocks for YAML well-formedness only
- [x] 3.4 Run `cargo test -p arkflow --test docs_snippets_validate` and
      confirm both gates agree on the same vocabulary (each fails on the
      other's unknown markers is not possible — assert identical accepted set)

## 4. Corpus pass

- [x] 4.1 Tag all `docs/docs` yaml blocks with the classification marker,
      area by area (components → build → develop → operate → reference →
      get-started), keeping `docs:check` and the Rust test green per commit
- [x] 4.2 Repair every block the first validation pass surfaces as rotted
      (field renames, removed options); anything intentionally illustrative
      gets `foreign` with a stated reason, counted and listed in the change
      notes
- [x] 4.3 Update `AGENTS.md` documentation-workflow section with a one-line
      rule: every docs yaml block carries a classification marker

## 5. External-link scheduled job (low-priority tail)

- [x] 5.1 Choose lychee (preferred) or a minimal cached Node script; add the
      checked-in config with domain exclusions (flaky/auth-walled domains)
- [x] 5.2 Add the weekly scheduled GitHub Actions workflow: report-only,
      files/updates one issue listing dead links with page locations; no PR
      gating
- [x] 5.3 Run the job once manually (workflow_dispatch) and triage the
      current 62 links; fix any dead ones found

## 6. Final verification

- [x] 6.1 `pnpm docs:check`, `cargo test --workspace --all-targets`, and
      `cargo clippy --workspace --all-targets` all pass
- [x] 6.2 Docs build (`pnpm build`) passes with the markers in place

## Notes: foreign-classified blocks (task 4.2)

17 of the 128 classified yaml blocks carry `validate=foreign` with a stated
reason — counted and listed here per task 4.2:

| Count | Reason | Files |
| --- | --- | --- |
| 8 | WAL tuning option fragment (`segment_tuning` ×4, `segment`, `cursor`, `parallel_put`, `compression`) | `docs/develop/s3-wal-performance.md` |
| 5 | Kubernetes manifest (×4) + manifest fragment (volumes section, ×1) | `docs/operate/kubernetes.md` |
| 2 | legacy join configuration — join buffers cannot compile to the unified kernel; use a Job DAG join operator | `docs/components/1-buffers/session_window.md`, `docs/components/1-buffers/tumbling_window.md` |
| 1 | Prometheus scrape config | `docs/operate/observability.md` |
| 1 | SQL processor `temporary_list` option fragment | `docs/components/4-temporary/redis.md` |

In addition, one former yaml block (the front-matter example in
`docs/develop/plugins.md`) is not single-document YAML by nature and moved to
a ` ```text ` fence, leaving 128 yaml blocks under the contract.
