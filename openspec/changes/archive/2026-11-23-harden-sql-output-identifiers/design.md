## Context

`output/sql.rs` assembles `INSERT INTO <table> (<cols>) VALUES (...)` text and hands it to sqlx `QueryBuilder` (values are bound parameters; identifiers are the only interpolated part). Columns and upsert keys are wrapped in the dialect quote character without escaping (`format!("`{}`", c)` / `format!("\"{}\"", c)`), and `table_name` is interpolated verbatim. Column names come from the Arrow schema and may be inferred from JSON keys, so they are data-controlled. The pgvector components already quote safely through `vector_util::escape_identifier` (double-quote doubling) but there is no MySQL (backtick) variant and the SQL output does not share the code.

Constraints: no new dependency; existing valid configurations (`events`, `analytics.events`, pre-quoted names, mixed-case names on PostgreSQL) must keep resolving to the same tables; quoting must be decided once at build time so `--validate` reports malformed names and the hot path does no extra parsing.

## Goals / Non-Goals

**Goals:**

- Every identifier the `sql` output interpolates (columns, upsert keys, conflict assignments, table name) is quoted and escaped for the target dialect.
- Qualified table names keep working; bare parts keep their current resolution semantics.
- One implementation shared with the pgvector components, with one test case set.
- Malformed table names and NUL-bearing column names fail with explicit errors.

**Non-Goals:**

- User-authored SQL text (`sql` input queries, the `sql` processor's `query`): that is a statement, not an identifier spliced by the engine.
- InfluxDB line-protocol escaping (`output/influxdb.rs` has its own, different rules).
- Changing the pgvector `table` contract: it stays a single quoted identifier (a dotted pgvector table name must already be written quoted); only the implementation is shared.
- SQLite: the SQL output supports MySQL and PostgreSQL only.

## Decisions

**D1 — A neutral `sql_ident` module; `vector_util::escape_identifier` becomes a wrapper.**
`QuoteStyle::{DoubleQuote, Backtick}` selects the quote character. `quote_identifier(name, style)` wraps and doubles embedded quote characters. `quote_qualified_name(name, style)` handles dotted object names. The pgvector call sites are untouched and their generated SQL is byte-identical; both dialects and both consumers share the unit tests in the module.

**D2 — Bare parts are always quoted; on PostgreSQL they are lower-cased first.**
Alternatives: (a) validate against a strict identifier grammar and reject everything else — rejects legitimate unusual names and does not satisfy "injection attempts are treated as plain identifiers"; (b) pass "safe" names through raw and quote only the rest — two code paths for one meaning, and the raw path keeps the reserved-word failure. Always quoting is one rule. Lower-casing bare PostgreSQL parts reproduces what the server does with an unquoted identifier, so `Events` still resolves to `events` while `"Events"` stays case-sensitive — today's semantics exactly. MySQL bare parts are quoted as written (backticks are case-preserving, like unquoted names on case-sensitive installations).

**D3 — Parts already wrapped in the dialect quote character pass through unchanged, after validation.**
A part is "quoted" when it starts with the style's quote character; it must be closed by a non-doubled quote with nothing but a `.` or end-of-name after it. This keeps `"Events"` and `` `my table` `` working without double-quoting them, and makes malformed quoting a build-time error instead of a server-side surprise. A quote character appearing inside a bare part does not start a quoted part; the part is quoted as a whole, so `ev"ents` names a table literally called `ev"ents`.

**D4 — The table reference is resolved at build time and stored on the parsed config (`#[serde(skip)] table_name_sql`).**
The builder computes it right after parsing, so a malformed `table_name` fails under `--validate`, and the per-batch builders only read a `&str`. The field is not a configuration key (`serde(skip)`), which the metadata conformance gate recognizes.

**D5 — NUL bytes are rejected.**
Neither server's lexer represents a NUL inside a quoted identifier faithfully (it can truncate the statement), so `quote_qualified_name` rejects it and `batch_rows` rejects a column name containing it before the transaction opens.

**D6 — Verified against real servers.**
Unit tests pin the generated text for both dialects; a PostgreSQL round trip joins `tests/postgres_io.rs` and a new `tests/mysql_io.rs` does the same for MySQL, both using the existing probe-and-skip pattern (`ARKFLOW_TEST_PG_URI`, `ARKFLOW_TEST_MYSQL_URI`). CI has no database services, so these stay opt-in; they are run locally against Docker containers for this change and the transcript goes into the PR description.

## Risks / Trade-offs

- [The emitted SQL text changes (table name is now quoted)] → no semantic change for any previously valid name (D2/D3 preserve resolution); documented in the CHANGELOG under "Changed".
- [A previously "working" malformed name now fails at build] → only names that already produced a server error or a different statement; the error names `table_name`.
- [`to_lowercase()` is not exactly PostgreSQL's downcasing for every Unicode edge case] → PostgreSQL folds using its lower() collation rules; for ASCII and common names they agree, and a user who needs exact casing quotes the part.
- [The real-database tests are not in CI] → the text-level unit tests are the CI gate; the database tests protect against dialect regressions when run locally.
- [Pre-quoted parts written for the other dialect (backticks on PostgreSQL)] → treated as a bare part and quoted, producing a "relation does not exist" error naming the literal table; no injection, and the configuration was already invalid.

## Migration Plan

No migration. Configurations that relied on raw table-name interpolation to do something other than name a table (none is supported) would start failing at build with an explanatory error.

## Open Questions

None.
