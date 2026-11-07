//! Shared table-copy engine for the offline store maintenance commands:
//! `migrate` (SQLite → PostgreSQL), `backup` (either backend → a
//! SQLite-format snapshot file), and `restore` (a snapshot → either backend).
//!
//! Every copy path walks the same `TABLE_ORDER` and shares one value
//! currency ([`PgVal`]): the SQLite side decodes into it, the PostgreSQL
//! side binds it, and the SQLite insert path converts it back. Values are
//! coerced per target column kind before a PostgreSQL insert — SQLite is
//! dynamically typed, so a source `INTEGER 1` landing in a `BOOLEAN` column
//! or a plain NULL landing in a `BIGINT` column would otherwise fail
//! PostgreSQL's strict prepared-statement type check (SQLSTATE 42804).
//!
//! All flows assume an offline store (documented for `migrate`, `restore`,
//! and `backup`): row counts are verified per table against the count taken
//! inside the read that produced the rows.
use super::postgres::PgVal;
use super::sqlite::SqliteBackend;
use super::StorageError;
use rusqlite::types::ValueRef;
use sqlx::{Postgres, Row};

/// Foreign-key-safe copy order (referenced tables first). Shared by
/// `migrate`, `backup`, and `restore`: a table missing from this list is
/// silently dropped by every copy command, so webhook subscriptions
/// (`cp_webhooks`) belong here exactly like every other business table.
pub const TABLE_ORDER: [&str; 21] = [
    "cp_nodes",
    "cp_jobs",
    "cp_job_versions",
    "cp_job_tasks",
    "cp_job_checkpoints",
    "cp_config_versions",
    "cp_intents",
    "cp_stream_desired",
    "cp_stream_observed",
    "cp_attempts",
    "cp_events",
    "cp_webhooks",
    // Managed identity: the user directory and credential rows are business
    // state and must survive backup/migrate/restore. Browser sessions
    // (`cp_sessions`) are deliberately NOT in this list: a session id is a
    // live bearer secret, and restoring old login state after a disaster
    // recovery is a security anti-pattern (same rationale as the lease row).
    "cp_users",
    "cp_credentials",
    "cp_audit_events",
    "cp_rollouts",
    "cp_rollout_targets",
    "cp_config_approval_requests",
    "cp_job_upgrades",
    "cp_operations",
    "cp_outbox",
];

/// Identity (autoincrement) columns that need sequence resets after copying.
const IDENTITY_COLUMNS: [(&str, &str); 3] = [
    ("cp_events", "event_id"),
    ("cp_audit_events", "event_id"),
    ("cp_outbox", "outbox_id"),
];

/// Self-referencing foreign keys inside a single table: a child row
/// referencing a parent row in the SAME table violates the constraint when
/// it inserts first, so the copy must order rows parents-first.
const SELF_REFERENCING: [(&str, &str); 2] = [
    ("cp_config_versions", "parent_version_id"),
    ("cp_intents", "superseded_by_intent_id"),
];

const CHUNK: usize = 1000;

/// Outcome of a successful copy: per-table row counts.
#[derive(Debug, Clone, Default)]
pub struct CopyReport {
    pub rows_per_table: Vec<(String, usize)>,
}

impl CopyReport {
    pub fn total_rows(&self) -> usize {
        self.rows_per_table.iter().map(|(_, count)| *count).sum()
    }
}

pub(crate) fn value_to_pg(value: ValueRef<'_>) -> PgVal {
    match value {
        ValueRef::Null => PgVal::Null,
        ValueRef::Integer(v) => PgVal::Int(v),
        ValueRef::Real(v) => PgVal::Real(v),
        ValueRef::Text(v) => PgVal::Text(String::from_utf8_lossy(v).into_owned()),
        ValueRef::Blob(v) => PgVal::Bytes(v.to_vec()),
    }
}

/// A PostgreSQL column kind, discovered from `information_schema` for both
/// the source read (typed row decode) and the target write (typed binds).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PgColumnType {
    Text,
    Integer,
    Real,
    Boolean,
    Bytes,
}

impl PgColumnType {
    fn parse(data_type: &str) -> Result<Self, StorageError> {
        Ok(match data_type {
            "text" | "character varying" | "varchar" | "character" | "bpchar" | "citext" => {
                Self::Text
            }
            "bigint" | "integer" | "smallint" | "int2" | "int4" | "int8" => Self::Integer,
            "boolean" | "bool" => Self::Boolean,
            "double precision" | "real" | "numeric" | "float4" | "float8" => Self::Real,
            "bytea" => Self::Bytes,
            other => return Err(StorageError::ColumnType(other.to_string())),
        })
    }

    fn name(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Integer => "bigint",
            Self::Real => "float8",
            Self::Boolean => "boolean",
            Self::Bytes => "bytea",
        }
    }
}

/// Reorder rows so that a row never precedes the row its `parent_column`
/// references. Rows referencing missing parents (dangling foreign keys are
/// possible in SQLite when constraints were disabled) keep their relative
/// order — the PostgreSQL copy surfaces the violation as a copy error
/// instead of silently reordering past it.
pub(crate) fn order_parents_first(
    columns: &[String],
    rows: Vec<Vec<PgVal>>,
    parent_column: &str,
) -> Vec<Vec<PgVal>> {
    let Some(key_index) = columns
        .iter()
        .position(|column| column == "config_version_id" || column == "intent_id")
    else {
        return rows;
    };
    let Some(parent_index) = columns.iter().position(|column| column == parent_column) else {
        return rows;
    };
    let key_of = |row: &Vec<PgVal>| -> Option<String> {
        match row.get(key_index) {
            Some(PgVal::Text(key)) => Some(key.clone()),
            _ => None,
        }
    };
    let parent_of = |row: &Vec<PgVal>| -> Option<String> {
        match row.get(parent_index) {
            Some(PgVal::Text(key)) => Some(key.clone()),
            _ => None, // NULL parent: root row
        }
    };
    // All present keys: a parent reference that no row satisfies is
    // dangling and never blocks emission.
    let present: std::collections::BTreeSet<String> = rows.iter().filter_map(key_of).collect();
    let mut emitted: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut pending: std::collections::VecDeque<Vec<PgVal>> = rows.into();
    let mut ordered: Vec<Vec<PgVal>> = Vec::with_capacity(pending.len());
    let mut progress = true;
    while progress {
        progress = false;
        let scan: Vec<Vec<PgVal>> = pending.drain(..).collect();
        let mut remaining: Vec<Vec<PgVal>> = Vec::with_capacity(scan.len());
        for row in scan {
            let blocked = parent_of(&row)
                .filter(|parent| present.contains(parent))
                .is_some_and(|parent| !emitted.contains(&parent));
            if blocked {
                remaining.push(row);
            } else {
                if let Some(key) = key_of(&row) {
                    emitted.insert(key);
                }
                ordered.push(row);
                progress = true;
            }
        }
        pending = remaining.into();
    }
    // Cycles or deep chains that made no progress in the last pass keep
    // their order at the tail; the copy surfaces any real violation.
    ordered.extend(pending);
    ordered
}

// ---------- SQLite source ----------

fn sqlite_table_columns(source: &SqliteBackend, table: &str) -> Result<Vec<String>, StorageError> {
    source.with_connection(|connection| {
        let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(columns)
    })
}

fn sqlite_read_table(
    source: &SqliteBackend,
    table: &str,
    columns: &[String],
) -> Result<Vec<Vec<PgVal>>, StorageError> {
    let column_list = columns.join(", ");
    let sql = format!("SELECT {column_list} FROM {table}");
    source.with_connection(|connection| {
        let mut statement = connection.prepare(&sql)?;
        let mut rows = statement.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let mut values = Vec::with_capacity(columns.len());
            for index in 0..columns.len() {
                values.push(value_to_pg(row.get_ref(index)?));
            }
            out.push(values);
        }
        Ok(out)
    })
}

/// Read one table from a SQLite source with parents-first ordering for the
/// self-referencing tables. A missing `cp_*` table fails loudly: silently
/// copying an absent table would drop its data from every downstream copy.
fn read_sqlite_table_ordered(
    source: &SqliteBackend,
    table: &str,
) -> Result<(Vec<String>, Vec<Vec<PgVal>>), StorageError> {
    let columns = sqlite_table_columns(source, table)?;
    if columns.is_empty() {
        return Err(StorageError::Unsupported(
            "source database is missing a cp_* table",
        ));
    }
    let mut rows = sqlite_read_table(source, table, &columns)?;
    if let Some((_, parent_column)) = SELF_REFERENCING.iter().find(|(name, _)| *name == table) {
        rows = order_parents_first(&columns, rows, parent_column);
    }
    Ok((columns, rows))
}

fn sqlite_count_rows_conn(
    connection: &rusqlite::Connection,
    table: &str,
) -> Result<usize, rusqlite::Error> {
    let count: i64 = connection.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get(0)
    })?;
    Ok(count as usize)
}

// ---------- SQLite target ----------

fn sqlite_value(value: &PgVal) -> rusqlite::types::Value {
    match value {
        PgVal::Null | PgVal::NullInt | PgVal::NullReal | PgVal::NullBool | PgVal::NullBytes => {
            rusqlite::types::Value::Null
        }
        PgVal::Text(v) => rusqlite::types::Value::Text(v.clone()),
        PgVal::Int(v) => rusqlite::types::Value::Integer(*v),
        PgVal::Real(v) => rusqlite::types::Value::Real(*v),
        PgVal::Bool(v) => rusqlite::types::Value::Integer(i64::from(*v)),
        PgVal::Bytes(v) => rusqlite::types::Value::Blob(v.clone()),
    }
}

/// Insert rows into a SQLite database. `INSERT OR IGNORE` mirrors the
/// PostgreSQL copy's `ON CONFLICT DO NOTHING`: an empty target never
/// conflicts, and a defensive re-run stays idempotent instead of aborting
/// on the first duplicate primary key.
pub(crate) fn sqlite_insert_chunk(
    connection: &rusqlite::Connection,
    table: &str,
    columns: &[String],
    rows: &[Vec<PgVal>],
) -> Result<(), rusqlite::Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let column_list = columns.join(", ");
    let placeholders = vec!["?"; columns.len()].join(", ");
    let sql = format!("INSERT OR IGNORE INTO {table} ({column_list}) VALUES ({placeholders})");
    let mut statement = connection.prepare(&sql)?;
    for values in rows {
        statement.execute(rusqlite::params_from_iter(values.iter().map(sqlite_value)))?;
    }
    Ok(())
}

// ---------- PostgreSQL source / target helpers ----------

/// Column names and kinds of a `cp_*` table, in ordinal order.
async fn pg_table_columns<'e, E>(
    executor: E,
    table: &str,
) -> Result<Vec<(String, PgColumnType)>, StorageError>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT column_name, data_type FROM information_schema.columns \
         WHERE table_name = $1 AND table_schema = current_schema() \
         ORDER BY ordinal_position",
    )
    .bind(table)
    .fetch_all(executor)
    .await?;
    rows.into_iter()
        .map(|(name, data_type)| Ok((name, PgColumnType::parse(&data_type)?)))
        .collect()
}

/// Decode one cell by its column kind. Type-aware decode is what makes a
/// PostgreSQL source copyable at all: the row comes back dynamically.
fn decode_pg_cell(
    row: &sqlx::postgres::PgRow,
    idx: usize,
    kind: PgColumnType,
) -> Result<PgVal, StorageError> {
    Ok(match kind {
        PgColumnType::Text => row
            .try_get::<Option<String>, _>(idx)?
            .map(PgVal::Text)
            .unwrap_or(PgVal::Null),
        PgColumnType::Integer => row
            .try_get::<Option<i64>, _>(idx)?
            .map(PgVal::Int)
            .unwrap_or(PgVal::Null),
        PgColumnType::Real => row
            .try_get::<Option<f64>, _>(idx)?
            .map(PgVal::Real)
            .unwrap_or(PgVal::Null),
        PgColumnType::Boolean => row
            .try_get::<Option<bool>, _>(idx)?
            .map(PgVal::Bool)
            .unwrap_or(PgVal::Null),
        PgColumnType::Bytes => row
            .try_get::<Option<Vec<u8>>, _>(idx)?
            .map(PgVal::Bytes)
            .unwrap_or(PgVal::Null),
    })
}

/// Coerce one source value into the bind shape a PostgreSQL column of
/// `kind` accepts. NULL binds must carry the target column's type (a
/// text-typed NULL for a bigint column fails with SQLSTATE 42804), and the
/// SQLite booleans-as-integers / reals-as-integers crossings convert.
fn coerce_for_postgres_target(
    value: PgVal,
    kind: PgColumnType,
    table: &str,
) -> Result<PgVal, StorageError> {
    let value_kind = match &value {
        PgVal::Null | PgVal::NullInt | PgVal::NullReal | PgVal::NullBool | PgVal::NullBytes => {
            "NULL"
        }
        PgVal::Text(_) => "text",
        PgVal::Int(_) => "integer",
        PgVal::Real(_) => "real",
        PgVal::Bool(_) => "boolean",
        PgVal::Bytes(_) => "bytes",
    };
    let coerced = match (&value, kind) {
        (PgVal::Null, PgColumnType::Text) => PgVal::Null,
        (PgVal::Null, PgColumnType::Integer) => PgVal::NullInt,
        (PgVal::Null, PgColumnType::Real) => PgVal::NullReal,
        (PgVal::Null, PgColumnType::Boolean) => PgVal::NullBool,
        (PgVal::Null, PgColumnType::Bytes) => PgVal::NullBytes,
        (PgVal::Text(_), PgColumnType::Text)
        | (PgVal::Int(_), PgColumnType::Integer)
        | (PgVal::Real(_), PgColumnType::Real)
        | (PgVal::Bool(_), PgColumnType::Boolean)
        | (PgVal::Bytes(_), PgColumnType::Bytes) => value,
        (PgVal::Int(v), PgColumnType::Boolean) => PgVal::Bool(*v != 0),
        (PgVal::Int(v), PgColumnType::Real) => PgVal::Real(*v as f64),
        _ => {
            return Err(StorageError::Encoding(format!(
                "table {table}: cannot store a {value_kind} value in a {} column",
                kind.name()
            )));
        }
    };
    Ok(coerced)
}

/// Align source rows to the target schema: every source column must exist
/// in the target with a known kind, and every value must be coercible.
fn coerce_rows_for_postgres(
    rows: Vec<Vec<PgVal>>,
    source_columns: &[String],
    target_columns: &[(String, PgColumnType)],
    table: &str,
) -> Result<Vec<Vec<PgVal>>, StorageError> {
    let mut kinds: Vec<PgColumnType> = Vec::with_capacity(source_columns.len());
    for name in source_columns {
        let kind = target_columns
            .iter()
            .find(|(target_name, _)| target_name == name)
            .map(|(_, kind)| *kind)
            .ok_or_else(|| {
                StorageError::Encoding(format!(
                    "table {table}: source column {name} does not exist in the target schema"
                ))
            })?;
        kinds.push(kind);
    }
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .zip(&kinds)
                .map(|(value, kind)| coerce_for_postgres_target(value, *kind, table))
                .collect()
        })
        .collect()
}

/// Insert already-coerced rows into a PostgreSQL table on the given
/// connection, with no transaction of its own: the caller decides the commit
/// granularity (`migrate` commits per chunk; `restore` inserts everything on
/// one transaction so a failure rolls the whole restore back).
async fn pg_insert_rows(
    connection: &mut sqlx::PgConnection,
    table: &str,
    columns: &[String],
    rows: &[Vec<PgVal>],
) -> Result<(), StorageError> {
    let column_list = columns.join(", ");
    for values in rows {
        let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${i}")).collect();
        let sql = format!(
            "INSERT INTO {table} ({column_list}) VALUES ({}) ON CONFLICT DO NOTHING",
            placeholders.join(", ")
        );
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
        for value in values {
            query = match value {
                PgVal::Null => query.bind(Option::<String>::None),
                PgVal::NullInt => query.bind(Option::<i64>::None),
                PgVal::NullReal => query.bind(Option::<f64>::None),
                PgVal::NullBool => query.bind(Option::<bool>::None),
                PgVal::NullBytes => query.bind(Option::<Vec<u8>>::None),
                PgVal::Text(v) => query.bind(v.clone()),
                PgVal::Int(v) => query.bind(*v),
                PgVal::Real(v) => query.bind(*v),
                PgVal::Bool(v) => query.bind(*v),
                PgVal::Bytes(v) => query.bind(v.clone()),
            };
        }
        query.execute(&mut *connection).await?;
    }
    Ok(())
}

/// `migrate`'s copy granularity: per-chunk transactions, matching the
/// historical behavior of committing bounded batches as it goes.
async fn pg_insert_chunk(
    connection: &mut sqlx::PgConnection,
    table: &str,
    columns: &[String],
    rows: &[Vec<PgVal>],
) -> Result<(), StorageError> {
    pg_insert_rows(connection, table, columns, rows).await
}

async fn pg_count(
    executor: impl sqlx::Executor<'_, Database = Postgres>,
    table: &str,
) -> Result<usize, StorageError> {
    let count: i64 =
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
            .fetch_one(executor)
            .await?;
    Ok(count as usize)
}

/// List every `TABLE_ORDER` business table that already holds rows, in copy
/// order. Restore surfaces ALL of them before writing anything, so an
/// operator can see the full picture instead of fixing one table at a time.
async fn postgres_non_empty_tables(
    connection: &mut sqlx::PgConnection,
) -> Result<Vec<String>, StorageError> {
    let mut non_empty = Vec::new();
    for table in TABLE_ORDER {
        let exists: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT EXISTS(SELECT 1 FROM {table})"
        )))
        .fetch_one(&mut *connection)
        .await?;
        if exists {
            non_empty.push(table.to_string());
        }
    }
    Ok(non_empty)
}

fn sqlite_non_empty_tables(
    connection: &rusqlite::Connection,
) -> Result<Vec<String>, rusqlite::Error> {
    let mut non_empty = Vec::new();
    for table in TABLE_ORDER {
        if sqlite_count_rows_conn(connection, table)? > 0 {
            non_empty.push(table.to_string());
        }
    }
    Ok(non_empty)
}

/// Reset PostgreSQL IDENTITY sequences above the copied max ids, so newly
/// generated ids continue after the restored rows instead of colliding.
async fn reset_postgres_sequences(connection: &mut sqlx::PgConnection) -> Result<(), StorageError> {
    for (table, column) in IDENTITY_COLUMNS {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT setval(pg_get_serial_sequence('{table}', '{column}'), COALESCE((SELECT MAX({column}) FROM {table}), 0) + 1, false)"
        )))
        .execute(&mut *connection)
        .await?;
    }
    Ok(())
}

// ---------- copy flows ----------

/// Copy a SQLite source into a PostgreSQL target: read per table in
/// `TABLE_ORDER`, order self-referencing rows parents-first, coerce values
/// to the target column kinds, insert in bounded per-chunk transactions, and
/// verify the per-table row counts. Used by `migrate`; `restore` uses
/// [`restore_postgres_target`] for atomic all-or-nothing semantics.
pub async fn copy_sqlite_to_postgres(
    source: &SqliteBackend,
    pool: &sqlx::PgPool,
) -> Result<CopyReport, StorageError> {
    let mut report = CopyReport::default();
    for table in TABLE_ORDER {
        let (columns, rows) = read_sqlite_table_ordered(source, table)?;
        let target_columns = pg_table_columns(pool, table).await?;
        if target_columns.is_empty() {
            return Err(StorageError::Encoding(format!(
                "target database is missing table {table}"
            )));
        }
        let rows = coerce_rows_for_postgres(rows, &columns, &target_columns, table)?;
        for chunk in rows.chunks(CHUNK) {
            let mut transaction = pool.begin().await?;
            pg_insert_chunk(&mut transaction, table, &columns, chunk).await?;
            transaction.commit().await?;
        }
        verify_count(&mut report, table, rows.len(), pg_count(pool, table).await?)?;
    }
    let mut connection = pool.acquire().await?;
    reset_postgres_sequences(&mut connection).await?;
    Ok(report)
}

/// Copy a PostgreSQL source into a fresh SQLite-format snapshot file: one
/// repeatable-read transaction gives cross-table consistency (every table
/// is read at the same MVCC point), and the snapshot is created through the
/// normal SQLite backend path so it carries the schema and version stamp.
/// The caller must guarantee the snapshot file does not exist.
pub async fn backup_postgres_to_sqlite(
    pool: &sqlx::PgPool,
    snapshot_path: &str,
) -> Result<CopyReport, StorageError> {
    let snapshot = SqliteBackend::open(snapshot_path)?;
    let mut tx = pool.begin().await?;
    // Must precede the first read in the transaction.
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await?;
    let mut report = CopyReport::default();
    for table in TABLE_ORDER {
        let target_columns = pg_table_columns(&mut *tx, table).await?;
        if target_columns.is_empty() {
            return Err(StorageError::Unsupported(
                "source database is missing a cp_* table",
            ));
        }
        let column_names: Vec<String> = target_columns
            .iter()
            .map(|(name, _)| name.clone())
            .collect();
        let column_list = column_names.join(", ");
        let pg_rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {column_list} FROM {table}"
        )))
        .fetch_all(&mut *tx)
        .await?;
        let rows: Vec<Vec<PgVal>> = pg_rows
            .iter()
            .map(|row| {
                target_columns
                    .iter()
                    .enumerate()
                    .map(|(idx, (_, kind))| decode_pg_cell(row, idx, *kind))
                    .collect()
            })
            .collect::<Result<Vec<_>, _>>()?;
        snapshot.with_connection(|connection| {
            for chunk in rows.chunks(CHUNK) {
                sqlite_insert_chunk(connection, table, &column_names, chunk)?;
            }
            Ok(())
        })?;
        verify_count(
            &mut report,
            table,
            rows.len(),
            pg_count(&mut *tx, table).await?,
        )?;
    }
    drop(tx);
    // `SqliteBackend::open` enables WAL; fold the WAL back into a single
    // self-contained file so the snapshot travels without sidecar files
    // (the SQLite-source path's `VACUUM INTO` output is already non-WAL).
    snapshot.with_connection(|connection| {
        connection.pragma_update(None, "journal_mode", "DELETE")?;
        Ok(())
    })?;
    Ok(report)
}

/// Restore a snapshot into an empty SQLite target. The whole restore runs
/// in one immediate transaction: the empty check sees the pre-copy state,
/// any failure rolls back every insert, and the write lock the transaction
/// holds makes a Hub that is writing at that moment fail loudly instead of
/// racing. An idle Hub holds no locks and is NOT detected — operators must
/// still stop every Hub instance against the target first.
pub async fn restore_sqlite_target(
    source: &SqliteBackend,
    target_path: &str,
) -> Result<CopyReport, StorageError> {
    let target = SqliteBackend::open(target_path)?;
    // Pre-read the snapshot before opening the write transaction so the
    // exclusive phase is short.
    let mut tables = Vec::new();
    for table in TABLE_ORDER {
        let (columns, rows) = read_sqlite_table_ordered(source, table)?;
        tables.push((table, columns, rows));
    }
    target.immediate_transaction(|connection| {
        let non_empty = sqlite_non_empty_tables(connection)?;
        if !non_empty.is_empty() {
            return Err(StorageError::NonEmptyTarget { tables: non_empty });
        }
        let mut report = CopyReport::default();
        for (table, columns, rows) in &tables {
            for chunk in rows.chunks(CHUNK) {
                sqlite_insert_chunk(connection, table, columns, chunk)?;
            }
            verify_count(
                &mut report,
                table,
                rows.len(),
                sqlite_count_rows_conn(connection, table)?,
            )?;
        }
        Ok(report)
    })
}

/// Restore a snapshot into an empty PostgreSQL target. The whole restore —
/// exclusive locks, empty check, copy, count verification, and sequence
/// resets — runs in ONE transaction: any failure rolls back every insert, so
/// a retry never trips over a partially restored database. Before the empty
/// check every `TABLE_ORDER` table is locked `ACCESS EXCLUSIVE ... NOWAIT`:
/// a session with an open transaction on any table makes the restore fail
/// immediately with `TargetBusy`, and no writer can interleave for the
/// restore's duration. An idle Hub holds no locks and is NOT detected;
/// operators must still stop every Hub instance against the target first.
pub async fn restore_postgres_target(
    source: &SqliteBackend,
    pool: &sqlx::PgPool,
) -> Result<CopyReport, StorageError> {
    let mut tx = pool.begin().await?;
    for table in TABLE_ORDER {
        let lock = format!("LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE NOWAIT");
        if let Err(error) = sqlx::query(sqlx::AssertSqlSafe(lock))
            .execute(&mut *tx)
            .await
        {
            return Err(StorageError::TargetBusy(format!(
                "could not lock table {table} exclusively: {error}"
            )));
        }
    }
    let non_empty = postgres_non_empty_tables(&mut tx).await?;
    if !non_empty.is_empty() {
        return Err(StorageError::NonEmptyTarget { tables: non_empty });
    }
    let mut report = CopyReport::default();
    for table in TABLE_ORDER {
        let (columns, rows) = read_sqlite_table_ordered(source, table)?;
        let target_columns = pg_table_columns(&mut *tx, table).await?;
        if target_columns.is_empty() {
            return Err(StorageError::Encoding(format!(
                "target database is missing table {table}"
            )));
        }
        let rows = coerce_rows_for_postgres(rows, &columns, &target_columns, table)?;
        pg_insert_rows(&mut tx, table, &columns, &rows).await?;
        verify_count(
            &mut report,
            table,
            rows.len(),
            pg_count(&mut *tx, table).await?,
        )?;
    }
    reset_postgres_sequences(&mut tx).await?;
    tx.commit().await?;
    Ok(report)
}

/// Post-process a SQLite-source snapshot produced by `VACUUM INTO` on the
/// storage actor's connection: strip the lease row (a snapshot never
/// carries a lease — the restored Hub rebuilds it through the existing CAS
/// path) and verify the per-table counts captured in the same
/// actor-serialized window that produced the snapshot.
pub fn finalize_sqlite_snapshot(
    snapshot_path: &str,
    expected: Vec<(String, usize)>,
) -> Result<CopyReport, StorageError> {
    let snapshot = rusqlite::Connection::open(snapshot_path)?;
    snapshot.execute("DELETE FROM cp_hub_lease", [])?;
    let mut report = CopyReport::default();
    for (table, expected_rows) in expected {
        let actual = sqlite_count_rows_conn(&snapshot, &table)?;
        verify_count(&mut report, &table, expected_rows, actual)?;
    }
    Ok(report)
}

fn verify_count(
    report: &mut CopyReport,
    table: &str,
    expected: usize,
    actual: usize,
) -> Result<(), StorageError> {
    if expected != actual {
        return Err(StorageError::RowCountMismatch {
            table: table.to_string(),
            expected,
            actual,
        });
    }
    report.rows_per_table.push((table.to_string(), expected));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn self_referencing_rows_copy_parents_first() {
        let columns = vec![
            "intent_id".to_string(),
            "superseded_by_intent_id".to_string(),
        ];
        // Child before parent in scan order; a NULL-parent root and a
        // dangling reference must not block emission.
        let rows = vec![
            vec![PgVal::Text("i3".into()), PgVal::Text("i2".into())],
            vec![PgVal::Text("i1".into()), PgVal::Null],
            vec![PgVal::Text("i2".into()), PgVal::Text("i1".into())],
            vec![PgVal::Text("i4".into()), PgVal::Text("missing".into())],
        ];
        let ordered = order_parents_first(&columns, rows, "superseded_by_intent_id");
        let position = |key: &str| {
            ordered
                .iter()
                .position(|row| matches!(row.first(), Some(PgVal::Text(k)) if k == key))
                .unwrap()
        };
        assert!(position("i1") < position("i2"));
        assert!(position("i2") < position("i3"));
        // The dangling reference emits without blocking.
        assert!(position("i4") < ordered.len());
    }

    #[test]
    fn table_order_puts_referenced_tables_first() {
        let nodes = TABLE_ORDER.iter().position(|t| *t == "cp_nodes").unwrap();
        let jobs = TABLE_ORDER.iter().position(|t| *t == "cp_jobs").unwrap();
        let config = TABLE_ORDER
            .iter()
            .position(|t| *t == "cp_config_versions")
            .unwrap();
        let rollouts = TABLE_ORDER
            .iter()
            .position(|t| *t == "cp_rollouts")
            .unwrap();
        let targets = TABLE_ORDER
            .iter()
            .position(|t| *t == "cp_rollout_targets")
            .unwrap();
        let webhooks = TABLE_ORDER
            .iter()
            .position(|t| *t == "cp_webhooks")
            .unwrap();
        assert!(nodes < jobs);
        assert!(config < rollouts);
        assert!(rollouts < targets);
        // The webhook table must be part of the shared copy order: a table
        // missing here is silently dropped by every copy command.
        assert!(webhooks > 0 && webhooks < TABLE_ORDER.len());
    }

    /// Every SQLite value kind maps onto the typed bind the PostgreSQL side
    /// expects; a mismatch here surfaces only mid-copy as an opaque
    /// SQLSTATE error, so the mapping is pinned by unit test instead.
    #[test]
    fn value_to_pg_maps_every_sqlite_value_kind() {
        assert!(matches!(value_to_pg(ValueRef::Null), PgVal::Null));
        assert!(matches!(value_to_pg(ValueRef::Integer(-7)), PgVal::Int(-7)));
        assert!(matches!(
            value_to_pg(ValueRef::Real(2.5)),
            PgVal::Real(v) if v == 2.5
        ));
        assert!(matches!(
            value_to_pg(ValueRef::Text(b"txt")),
            PgVal::Text(v) if v == "txt"
        ));
        assert!(matches!(
            value_to_pg(ValueRef::Blob(b"bytes")),
            PgVal::Bytes(v) if v == b"bytes"
        ));
    }

    #[test]
    fn copy_report_sums_per_table_rows() {
        let report = CopyReport {
            rows_per_table: vec![
                ("cp_nodes".into(), 2),
                ("cp_jobs".into(), 3),
                ("cp_outbox".into(), 0),
            ],
        };
        assert_eq!(report.total_rows(), 5);
        assert_eq!(CopyReport::default().total_rows(), 0);
    }

    /// Without the columns the ordering needs, rows pass through in scan
    /// order instead of panicking or dropping data.
    #[test]
    fn parents_first_ordering_degrades_to_scan_order_without_its_columns() {
        let rows = vec![vec![PgVal::Int(1), PgVal::Int(2)]];
        // Neither a key column nor the parent column exists.
        let ordered = order_parents_first(&["other".to_string()], rows.clone(), "parent");
        assert_eq!(ordered.len(), 1);
        // Key column present but the parent column is absent.
        let ordered =
            order_parents_first(&["intent_id".to_string()], rows, "superseded_by_intent_id");
        assert_eq!(ordered.len(), 1);
    }

    /// SQLite is dynamically typed: a non-TEXT key or parent reference is
    /// treated as absent (root row) rather than blocking emission.
    #[test]
    fn non_text_keys_and_parents_never_block_emission() {
        let columns = vec![
            "intent_id".to_string(),
            "superseded_by_intent_id".to_string(),
        ];
        let rows = vec![
            vec![PgVal::Int(1), PgVal::Null],
            vec![PgVal::Null, PgVal::Text("dangling".into())],
            vec![PgVal::Text("i1".into()), PgVal::Int(2)],
        ];
        let ordered = order_parents_first(&columns, rows, "superseded_by_intent_id");
        assert_eq!(ordered.len(), 3, "every row emits as a root");
        assert!(matches!(ordered[2].first(), Some(PgVal::Text(key)) if key == "i1"));
    }

    /// A dependency cycle can make no progress in any pass: those rows keep
    /// their scan order at the tail so the copy surfaces the violation.
    #[test]
    fn cyclic_parent_chains_keep_their_order_at_the_tail() {
        let columns = vec![
            "intent_id".to_string(),
            "superseded_by_intent_id".to_string(),
        ];
        let rows = vec![
            vec![PgVal::Text("a".into()), PgVal::Text("b".into())],
            vec![PgVal::Text("b".into()), PgVal::Text("a".into())],
        ];
        let ordered = order_parents_first(&columns, rows, "superseded_by_intent_id");
        assert_eq!(ordered.len(), 2);
        assert!(matches!(ordered[0].first(), Some(PgVal::Text(key)) if key == "a"));
        assert!(matches!(ordered[1].first(), Some(PgVal::Text(key)) if key == "b"));
    }

    /// The coercions that keep a SQLite-sourced copy inside PostgreSQL's
    /// strict prepared-statement typing: NULLs become typed NULLs for
    /// non-text columns, SQLite's integer booleans become booleans, and
    /// integer-stored reals become reals.
    #[test]
    fn coercion_maps_nulls_and_sqlite_type_crossings() {
        use PgColumnType as K;
        let table = "t";
        // NULL takes the target column's type.
        assert!(matches!(
            coerce_for_postgres_target(PgVal::Null, K::Integer, table).unwrap(),
            PgVal::NullInt
        ));
        assert!(matches!(
            coerce_for_postgres_target(PgVal::Null, K::Boolean, table).unwrap(),
            PgVal::NullBool
        ));
        assert!(matches!(
            coerce_for_postgres_target(PgVal::Null, K::Real, table).unwrap(),
            PgVal::NullReal
        ));
        assert!(matches!(
            coerce_for_postgres_target(PgVal::Null, K::Bytes, table).unwrap(),
            PgVal::NullBytes
        ));
        assert!(matches!(
            coerce_for_postgres_target(PgVal::Null, K::Text, table).unwrap(),
            PgVal::Null
        ));
        // SQLite boolean (INTEGER 0/1) -> BOOLEAN; integer-stored real -> REAL.
        assert!(matches!(
            coerce_for_postgres_target(PgVal::Int(1), K::Boolean, table).unwrap(),
            PgVal::Bool(true)
        ));
        assert!(matches!(
            coerce_for_postgres_target(PgVal::Int(0), K::Boolean, table).unwrap(),
            PgVal::Bool(false)
        ));
        assert!(matches!(
            coerce_for_postgres_target(PgVal::Int(7), K::Real, table).unwrap(),
            PgVal::Real(v) if v == 7.0
        ));
        // Matching kinds pass through untouched.
        assert!(matches!(
            coerce_for_postgres_target(PgVal::Text("x".into()), K::Text, table).unwrap(),
            PgVal::Text(_)
        ));
        // Everything else is rejected loudly instead of misbinding.
        assert!(coerce_for_postgres_target(PgVal::Text("x".into()), K::Integer, table).is_err());
        assert!(coerce_for_postgres_target(PgVal::Bytes(vec![0]), K::Text, table).is_err());
    }

    #[test]
    fn pg_column_type_parse_covers_the_store_ddl_kinds() {
        use PgColumnType as K;
        assert_eq!(K::parse("text").unwrap(), K::Text);
        assert_eq!(K::parse("bigint").unwrap(), K::Integer);
        assert_eq!(K::parse("boolean").unwrap(), K::Boolean);
        assert_eq!(K::parse("double precision").unwrap(), K::Real);
        assert_eq!(K::parse("bytea").unwrap(), K::Bytes);
        assert!(K::parse("jsonb").is_err());
    }

    /// Shared fixture: one job, one node with NULL bigint columns, two
    /// webhook subscriptions (one disabled — the boolean round trip), and
    /// one intent with its event + outbox rows.
    async fn seed_fixture(store: &crate::storage::ControlPlaneStore) {
        use crate::storage::{
            DesiredMutation, JobRecord, NodeMutation, StorageBackend, WebhookSetMutation,
            WebhookSubscription,
        };
        StorageBackend::upsert_job(
            store,
            JobRecord {
                job_id: "copier-job".into(),
                version: 1,
                spec_json: "{}".into(),
                desired_state: "running".into(),
                observed_state: "running".into(),
                convergence: "converged".into(),
                generation: 3,
                node_ids: vec!["node-1".into()],
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 7,

                pause_phase: None,
                pause_artifact_id: None,
                pause_prev_checkpoint_id: None,
                pause_retry_count: 0,
                paused_at_ms: None,
                paused_by: None,
            },
        )
        .await
        .unwrap();
        StorageBackend::upsert_node(
            store,
            NodeMutation {
                node_id: "node-1".into(),
                version: "test".into(),
                state: "online".into(),
                capabilities_json: "[]".into(),
                boot_id: None,
                report_seq: None,
                last_seen_at_ms: 5,
                lease_expires_at_ms: 0,
                maintenance_state: None,
                maintenance_updated_at_ms: None,
            },
        )
        .await
        .unwrap();
        StorageBackend::replace_webhooks(
            store,
            WebhookSetMutation {
                subscriptions: vec![
                    WebhookSubscription {
                        id: "hook-on".into(),
                        url: "https://example.test/on".into(),
                        event_types: vec!["job.".into()],
                        enabled: true,
                        secret: Some("hmac".into()),
                        created_by: Some("operator".into()),
                        created_at_ms: 9,
                    },
                    WebhookSubscription {
                        id: "hook-off".into(),
                        url: "https://example.test/off".into(),
                        event_types: vec![],
                        enabled: false,
                        secret: None,
                        created_by: None,
                        created_at_ms: 10,
                    },
                ],
                actor: None,
                correlation_id: None,
            },
            9,
        )
        .await
        .unwrap();
        StorageBackend::set_desired(
            store,
            DesiredMutation {
                node_id: "node-1".into(),
                stream_id: "stream-1".into(),
                desired_state: "running".into(),
                config_version_id: None,
                action_id: None,
                expected_generation: None,
                actor: None,
                correlation_id: None,
                idempotency_key: None,
                intent_type: None,
                payload_json: None,
            },
        )
        .await
        .unwrap();
    }

    fn count_of(report: &CopyReport, table: &str) -> usize {
        report
            .rows_per_table
            .iter()
            .find(|(name, _)| name == table)
            .map(|(_, count)| *count)
            .unwrap_or_else(|| panic!("{table} missing from report"))
    }

    /// (sqlite → sqlite): restore a seeded store's snapshot into an empty
    /// SQLite target and verify the contract-level round trip.
    #[tokio::test]
    async fn restores_a_sqlite_store_into_an_empty_sqlite_target() {
        use crate::storage::{ControlPlaneStore, StorageBackend};
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.sqlite");
        let target_path = dir.path().join("target.sqlite");
        let store = ControlPlaneStore::open(source_path.to_str().unwrap())
            .await
            .unwrap();
        seed_fixture(&store).await;
        drop(store);

        let source = SqliteBackend::open(&source_path).unwrap();
        let report = restore_sqlite_target(&source, target_path.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(count_of(&report, "cp_jobs"), 1);
        assert_eq!(count_of(&report, "cp_nodes"), 1);
        assert_eq!(count_of(&report, "cp_webhooks"), 2);
        assert!(count_of(&report, "cp_intents") >= 1);
        assert!(count_of(&report, "cp_events") >= 1);

        let target = ControlPlaneStore::open(target_path.to_str().unwrap())
            .await
            .unwrap();
        let job = StorageBackend::get_job(&target, "copier-job")
            .await
            .unwrap()
            .expect("restored job");
        assert_eq!(job.generation, 3);
        let hooks = StorageBackend::list_webhooks(&target).await.unwrap();
        // list_webhooks orders by id.
        assert_eq!(
            hooks
                .iter()
                .map(|h| (h.id.as_str(), h.enabled))
                .collect::<Vec<_>>(),
            vec![("hook-off", false), ("hook-on", true)]
        );

        // A second restore into the now-populated target is refused, naming
        // every non-empty business table.
        let error = restore_sqlite_target(&source, target_path.to_str().unwrap())
            .await
            .unwrap_err();
        let StorageError::NonEmptyTarget { tables } = error else {
            panic!("expected NonEmptyTarget, got {error}");
        };
        assert!(tables.contains(&"cp_jobs".to_string()), "{tables:?}");
        assert!(tables.contains(&"cp_webhooks".to_string()), "{tables:?}");
        assert!(tables.len() > 2, "must list every non-empty table");
    }

    /// (pg → sqlite → pg, gated): backup a PostgreSQL store into a snapshot
    /// file, restore it into an empty PostgreSQL database, and verify
    /// full-table count equivalence, NULL bigint preservation, boolean
    /// round trips, and identity-sequence continuation.
    #[tokio::test]
    async fn backup_and_restore_round_trip_postgres_through_a_snapshot() {
        if std::env::var("ARKFLOW_TEST_POSTGRES_URL").is_err() {
            eprintln!("skipping: ARKFLOW_TEST_POSTGRES_URL not set");
            return;
        }
        let source_url = super::super::contract_database_url("copier_backup_src").await;
        let target_url = super::super::contract_database_url("copier_restore_dst").await;
        use crate::storage::{ControlPlaneStore, DesiredMutation, StorageBackend};

        let store = ControlPlaneStore::open(&source_url).await.unwrap();
        seed_fixture(&store).await;
        let ControlPlaneStore::Postgres(backend) = &store else {
            panic!("postgres URL must open PostgreSQL");
        };
        let source_pool = backend.pool().clone();

        let dir = tempfile::tempdir().unwrap();
        let snapshot_path = dir.path().join("snapshot.sqlite");
        let backup_report =
            backup_postgres_to_sqlite(&source_pool, snapshot_path.to_str().unwrap())
                .await
                .unwrap();
        assert_eq!(count_of(&backup_report, "cp_jobs"), 1);
        assert_eq!(count_of(&backup_report, "cp_webhooks"), 2);
        assert!(count_of(&backup_report, "cp_outbox") >= 1);

        // Snapshot hygiene: current schema version stamped, no lease row.
        let snapshot = rusqlite::Connection::open(&snapshot_path).unwrap();
        let version: i64 = snapshot
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, super::super::SCHEMA_VERSION as i64);
        let lease_rows: i64 = snapshot
            .query_row("SELECT COUNT(*) FROM cp_hub_lease", [], |row| row.get(0))
            .unwrap();
        assert_eq!(lease_rows, 0, "snapshots never carry a lease row");
        drop(snapshot);

        let target_store = ControlPlaneStore::open(&target_url).await.unwrap();
        let ControlPlaneStore::Postgres(target_backend) = &target_store else {
            panic!("postgres URL must open PostgreSQL");
        };
        let target_pool = target_backend.pool().clone();
        let snapshot_backend = SqliteBackend::open(&snapshot_path).unwrap();
        let restore_report = restore_postgres_target(&snapshot_backend, &target_pool)
            .await
            .unwrap();
        assert_eq!(
            restore_report.rows_per_table, backup_report.rows_per_table,
            "every table must round-trip with identical counts"
        );

        // Contract equivalence: job, webhook booleans.
        let job = StorageBackend::get_job(&target_store, "copier-job")
            .await
            .unwrap()
            .expect("restored job");
        assert_eq!(job.generation, 3);
        let hooks = StorageBackend::list_webhooks(&target_store).await.unwrap();
        // list_webhooks orders by id.
        assert_eq!(
            hooks
                .iter()
                .map(|h| (h.id.as_str(), h.enabled))
                .collect::<Vec<_>>(),
            vec![("hook-off", false), ("hook-on", true)]
        );

        // NULL bigints survive the pg → sqlite → pg round trip.
        let null_nodes: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM cp_nodes WHERE last_report_seq IS NULL")
                .fetch_one(&target_pool)
                .await
                .unwrap();
        assert_eq!(null_nodes, 1);

        // Identity sequences continue after the restored rows.
        let before: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(outbox_id), 0) FROM cp_outbox")
            .fetch_one(&target_pool)
            .await
            .unwrap();
        StorageBackend::set_desired(
            &target_store,
            DesiredMutation {
                node_id: "node-2".into(),
                stream_id: "stream-after-restore".into(),
                desired_state: "running".into(),
                config_version_id: None,
                action_id: None,
                expected_generation: None,
                actor: None,
                correlation_id: None,
                idempotency_key: None,
                intent_type: None,
                payload_json: None,
            },
        )
        .await
        .unwrap();
        let after: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(outbox_id), 0) FROM cp_outbox")
            .fetch_one(&target_pool)
            .await
            .unwrap();
        assert!(after > before, "sequences must continue, not restart");

        // The populated target now refuses a second restore.
        let error = restore_postgres_target(&snapshot_backend, &target_pool)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, StorageError::NonEmptyTarget { tables } if tables.contains(&"cp_jobs".to_string())),
            "{error}"
        );
    }
}
