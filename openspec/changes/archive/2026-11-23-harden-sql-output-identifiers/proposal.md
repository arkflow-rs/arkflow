## Why

The `sql` output wraps column names in the dialect quote character but never escapes an embedded quote, and splices the configured table name into the statement raw. `crates/arkflow-plugin/src/output/sql.rs:218` (`` format!("`{}` = VALUES(`{}`)") ``), `sql.rs:224`, `sql.rs:227`, `sql.rs:246` and `sql.rs:286` build identifiers with bare `format!`, and `sql.rs:242-243` / `sql.rs:282-283` emit `INSERT INTO {}` with `output_config.table_name` verbatim. Column names are taken straight from the Arrow schema (`sql.rs:352`) and can originate from JSON key inference — i.e. from data — so a field named ``a`) VALUES ('x'); --`` changes the structure of the statement instead of naming a column. A safe implementation already exists for the pgvector components (`vector_util.rs:197-199`, double-quote doubling) but the SQL output does not share it, and has no backtick variant for MySQL.

## What Changes

- New shared module `crates/arkflow-plugin/src/sql_ident.rs`: dialect-aware identifier quoting. `quote_identifier` wraps a name in the dialect's quote character and doubles embedded ones; `quote_qualified_name` splits a dotted name into parts, keeps already-quoted parts, and quotes bare parts. `vector_util::escape_identifier` becomes a thin wrapper, so the pgvector output, the pgvector search processor, and the SQL output share one implementation and one test case set (pgvector's generated SQL is byte-identical).
- `sql` output routes every identifier through it: columns, upsert keys, conflict assignments, and the table name. The table name is quoted per part at build time (`analytics.events` → `"analytics"."events"` on PostgreSQL, `` `analytics`.`events` `` on MySQL). Bare parts keep today's semantics on PostgreSQL (unquoted identifiers fold to lower case); parts already wrapped in the dialect's quote character pass through unchanged. Malformed names — empty part, unterminated quote, text after a closing quote, NUL byte — fail at build time with a configuration error naming the table.
- Column names containing a NUL byte are rejected before the transaction starts.
- Tests: unit tests for both dialects on `a"b`, ``a`b``, semicolons, qualified and pre-quoted names; real-database round trips on PostgreSQL and MySQL through the gated integration tests.
- Docs (EN + zh-Hans), CHANGELOG, and the PA-P1-04 row in `PRODUCT-ITERATION-2026-11-22.md`.

## Capabilities

### New Capabilities

<!-- none — all work lands under existing capabilities -->

### Modified Capabilities

- `sql-output`: add a requirement that identifiers spliced into SQL text are quoted and escaped per dialect, including qualified table names, with build-time rejection of malformed names.

## Impact

- **Files**: new `crates/arkflow-plugin/src/sql_ident.rs` (module + unit tests), `crates/arkflow-plugin/src/output/sql.rs`, `crates/arkflow-plugin/src/vector_util.rs` (wrapper), `crates/arkflow-plugin/tests/postgres_io.rs` (identifier round trip), new gated `crates/arkflow-plugin/tests/mysql_io.rs`; docs pages for the `sql` output (EN + zh-Hans); `CHANGELOG.md`; `PRODUCT-ITERATION-2026-11-22.md`.
- **Behavior**: generated SQL text now quotes the table name; semantics for every previously valid `table_name` are preserved. A malformed table name fails at build (also under `--validate`) instead of producing a server error or a statement with a different meaning. A batch whose column name contains NUL fails the write with an explicit error.
- **Dependencies**: none.
- **CI**: unit tests join `cargo test --workspace --all-targets`; the real-database tests stay opt-in (`ARKFLOW_TEST_PG_URI` / `ARKFLOW_TEST_MYSQL_URI`), as the existing Postgres tests are.
