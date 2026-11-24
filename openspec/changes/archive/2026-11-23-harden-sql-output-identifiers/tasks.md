## 1. Shared identifier quoting

- [x] 1.1 Add `crates/arkflow-plugin/src/sql_ident.rs`: `QuoteStyle`, `quote_identifier` (doubles the dialect quote character), `quote_qualified_name` (dot-split, quoted-part pass-through with validation, PostgreSQL lower-casing of bare parts, NUL rejection), with unit tests covering `a"b`, ``a`b``, semicolons, qualified names, pre-quoted parts, and every rejection
- [x] 1.2 Make `vector_util::escape_identifier` delegate to `quote_identifier(.., QuoteStyle::DoubleQuote)`; confirm the pgvector output and search processor SQL-text tests are unchanged and green
- [x] 1.3 Add a shared test-case table used for both quote styles so every hostile identifier is exercised on PostgreSQL and MySQL quoting

## 2. SQL output adoption

- [x] 2.1 `SqlOutputConfig`: add `#[serde(skip)] table_name_sql`; resolve it in the builder with `quote_qualified_name` for the configured dialect; update existing test config literals
- [x] 2.2 Route columns, upsert keys, and conflict assignments through `quote_identifier`; use `table_name_sql` in both INSERT builders
- [x] 2.3 Reject NUL-bearing column names in `batch_rows` before the transaction starts
- [x] 2.4 Unit tests: hostile column names and table names for both dialects (exact statement text), qualified and pre-quoted table names, malformed `table_name` fails the builder, NUL column fails the write
- [x] 2.5 Update the metadata `table_name` description to state how dotted and quoted names are handled

## 3. Real-database verification

- [x] 3.1 Add a PostgreSQL identifier round-trip case to `crates/arkflow-plugin/tests/postgres_io.rs` (table and columns with `"`, backtick, and `;` in their names; upsert replay stays idempotent; no other table is affected)
- [x] 3.2 Add `crates/arkflow-plugin/tests/mysql_io.rs` with the probe-and-skip pattern (`ARKFLOW_TEST_MYSQL_URI`) and the same round trip for MySQL
- [x] 3.3 Run both against local Docker servers with `ARKFLOW_REQUIRE_POSTGRES=1` / `ARKFLOW_REQUIRE_MYSQL=1` so a missing server fails instead of skipping; record the transcript — PostgreSQL (3 sql-output tests) and MySQL (1) green against Docker with the require flags set; transcript in the PR description

## 4. Docs, changelog, registry

- [x] 4.1 Update the `sql` output page under `docs/docs/components/3-outputs/` and its zh-Hans mirror: identifier quoting, dotted and pre-quoted table names, malformed-name errors
- [x] 4.2 Add a CHANGELOG `[Unreleased]` entry (security/changed: identifier quoting)
- [x] 4.3 Strike the PA-P1-04 row in `PRODUCT-ITERATION-2026-11-22.md` with the change name

## 5. Gates

- [x] 5.1 `cargo test -p arkflow-plugin --lib --locked` and `--test postgres_io --test mysql_io --test metadata_examples_validate --locked` green (database tests against Docker) — green; the gated database tests ran for real because the servers were up
- [x] 5.2 `cargo fmt --all -- --check` and `cargo clippy -p arkflow-plugin --all-targets -D warnings` green — green workspace-wide
- [x] 5.3 `cd docs && pnpm docs:check` green — green
