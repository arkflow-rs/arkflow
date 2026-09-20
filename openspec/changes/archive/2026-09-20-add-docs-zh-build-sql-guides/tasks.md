## 1. Sidebar labels

- [x] 1.1 Localize `sidebar.tutorialSidebar.category.Recipes` (实战配方) and `sidebar.tutorialSidebar.category.SQL Reference` (SQL 参考) in `current.json`

## 2. Build core (6 pages)

- [x] 2.1 Translate `build/backpressure.md`, `build/delivery-semantics.md`, `build/exactly-once.md`, `build/metadata.md`, `build/wal.md`, `build/distributed-jobs.md`; `pnpm docs:check` passes

## 3. Build recipes (8 pages)

- [x] 3.1 Translate `build/recipes.md` and `build/recipes/1-kafka-to-sql.md`, `2-cdc-debezium.md`, `3-windowed-aggregation.md`, `4-http-ingestion.md`; `pnpm docs:check` passes
- [x] 3.2 Translate `build/recipes/10-case-webhook-durable.md`, `11-case-order-stream-sql.md`, `12-case-telemetry-windows.md`; `pnpm docs:check` passes

## 4. SQL reference small pages (6)

- [x] 4.1 Translate `sql.md`, `sql/0-data-types.md`, `sql/1-operators.md`, `sql/2-select.md`, `sql/4-subqueries.md`, `sql/6-window_functions.md`; `pnpm docs:check` passes

## 5. SQL scalar-function reference (1 page, ~4.8k lines)

- [x] 5.1 Translate `sql/7-scalar_functions.md` (function tables row-by-row, identifiers verbatim); fidelity check reports 0 fence/identity mismatches

## 6. SQL remaining + configuration (5 pages)

- [x] 6.1 Translate `sql/5-aggregate_functions.md`, `sql/8-special_functions.md`, `sql/9-udf.md`, `configuration/1-top-level.md`, `configuration/2-ide-schema.md`; `pnpm docs:check` passes

## 7. How-to, tutorials, cases, root pages (10 pages)

- [x] 7.1 Translate `how-to/` (5 pages) reusing recipe terminology; `pnpm docs:check` passes
- [x] 7.2 Translate `tutorials/1-durable-pipeline.md`, `cases/` (3 pages), `streaming-jobs.md`, `build-pipelines.md`; `pnpm docs:check` passes

## 8. Reverse-link conversions and final validation

- [x] 8.1 Convert flagged English file-relative links to absolute `/docs/...` routes until `pnpm docs:check` passes
- [x] 8.2 Coverage check: the 37 target pages exist under the zh tree; `pnpm docs:check` and `npm run build` both pass
