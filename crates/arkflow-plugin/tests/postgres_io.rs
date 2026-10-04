/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! Integration tests for the SQL output (`sql`) and the pgvector components
//! (`pgvector` output, `pgvector_search` processor) against real PostgreSQL
//! servers, following the probe-and-skip discipline of `pulsar_io.rs` /
//! `redis_input.rs`: every test first connects with a bounded timeout and
//! returns Ok (printing a skip note) when the server is unavailable.
//!
//! Default targets are the local Docker test containers; override with
//! `ARKFLOW_TEST_PG_URI` / `ARKFLOW_TEST_PGVECTOR_URI`:
//!
//! - plain Postgres 16:    postgres://arkflow:arkflow@localhost:5433/arkflow_test
//!   (container `arkflow-test-pg`)
//! - pgvector Postgres 16: postgres://arkflow:arkflow@localhost:5434/arkflow_test
//!   (container `arkflow-test-pgvector`; the `vector` extension is created
//!   by the test itself with `CREATE EXTENSION IF NOT EXISTS`)

use arkflow_core::output::{Output, OutputConfig};
use arkflow_core::processor::ProcessorConfig;
use arkflow_core::{MessageBatch, MessageBatchRef, ProcessResult, Resource};
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int64Array, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use sqlx::{Connection, PgConnection};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Once};
use std::time::Duration;

fn pg_uri() -> String {
    std::env::var("ARKFLOW_TEST_PG_URI")
        .unwrap_or_else(|_| "postgres://arkflow:arkflow@localhost:5433/arkflow_test".to_string())
}

fn pgvector_uri() -> String {
    std::env::var("ARKFLOW_TEST_PGVECTOR_URI")
        .unwrap_or_else(|_| "postgres://arkflow:arkflow@localhost:5434/arkflow_test".to_string())
}

static INIT: Once = Once::new();
static UNIQUE: AtomicUsize = AtomicUsize::new(0);

fn ensure_init() {
    INIT.call_once(|| {
        arkflow_plugin::output::init().expect("plugin output init");
        arkflow_plugin::processor::init().expect("plugin processor init");
    });
}

fn resource() -> Resource {
    Resource {
        temporary: HashMap::new(),
        input_names: RefCell::new(vec![]),
    }
}

/// Fresh table name per run and per use, so repeated runs and parallel
/// tests never collide; `DROP TABLE IF EXISTS` keeps leftovers harmless.
fn unique_table(prefix: &str) -> String {
    format!(
        "{}_{}_{}",
        prefix,
        std::process::id(),
        UNIQUE.fetch_add(1, Ordering::SeqCst)
    )
}

/// Connect with a bounded timeout; `None` means "skip this test".
async fn try_connect(uri: &str, case: &str) -> Option<PgConnection> {
    match tokio::time::timeout(Duration::from_secs(5), PgConnection::connect(uri)).await {
        Ok(Ok(conn)) => Some(conn),
        Ok(Err(e)) => {
            eprintln!("--- SKIPPED {case}: Postgres at {uri} refused a connection: {e} ---");
            None
        }
        Err(_) => {
            eprintln!("--- SKIPPED {case}: Postgres at {uri} did not answer within 5s ---");
            None
        }
    }
}

async fn exec(admin: &mut PgConnection, sql: &str) {
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .execute(admin)
        .await
        .unwrap_or_else(|e| panic!("admin statement failed ({sql}): {e}"));
}

fn build_output(output_type: &str, config: serde_json::Value) -> Arc<dyn Output> {
    OutputConfig {
        output_type: output_type.to_string(),
        name: None,
        codec: None,
        config: Some(config),
    }
    .build(&resource())
    .unwrap_or_else(|e| panic!("build {output_type} output: {e}"))
}

/// (id, name, score, active, note) rows; `note` carries NULLs.
fn events_batch(rows: Vec<(i64, &str, f64, bool, Option<&str>)>) -> MessageBatchRef {
    let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let names: Vec<Option<&str>> = rows.iter().map(|r| Some(r.1)).collect();
    let scores: Vec<Option<f64>> = rows.iter().map(|r| Some(r.2)).collect();
    let actives: Vec<Option<bool>> = rows.iter().map(|r| Some(r.3)).collect();
    let notes: Vec<Option<&str>> = rows.iter().map(|r| r.4).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
        Field::new("note", DataType::Utf8, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(names)),
        Arc::new(Float64Array::from(scores)),
        Arc::new(BooleanArray::from(actives)),
        Arc::new(StringArray::from(notes)),
    ];
    Arc::new(MessageBatch::new_arrow(
        RecordBatch::try_new(schema, columns).unwrap(),
    ))
}

/// A two-column batch whose `extra` column does not exist in the target
/// table — used to force a mid-transaction failure.
fn bad_batch(id: i64, name: &str) -> MessageBatchRef {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("extra", DataType::Utf8, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![id])),
        Arc::new(StringArray::from(vec![Some(name)])),
        Arc::new(StringArray::from(vec![Some("not a table column")])),
    ];
    Arc::new(MessageBatch::new_arrow(
        RecordBatch::try_new(schema, columns).unwrap(),
    ))
}

/// (doc_id, [f32; 2], text) rows in the shape the pgvector output expects.
fn vector_doc_batch(rows: Vec<(i64, [f32; 2], &str)>) -> MessageBatchRef {
    let dim = 2i32;
    let flat: Vec<f32> = rows.iter().flat_map(|r| r.1).collect();
    let vectors = Arc::new(FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        dim,
        Arc::new(Float32Array::from(flat)),
        None,
    ));
    let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let texts: Vec<Option<&str>> = rows.iter().map(|r| Some(r.2)).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("doc_id", DataType::Int64, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
            true,
        ),
        Field::new("text", DataType::Utf8, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids)),
        vectors,
        Arc::new(StringArray::from(texts)),
    ];
    Arc::new(MessageBatch::new_arrow(
        RecordBatch::try_new(schema, columns).unwrap(),
    ))
}

/// Query batch for the search processor: only the `embedding` column.
fn query_vector_batch(vectors: Vec<Vec<f32>>) -> MessageBatchRef {
    let dim = vectors[0].len() as i32;
    let flat: Vec<f32> = vectors.iter().flatten().copied().collect();
    let list = Arc::new(FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        dim,
        Arc::new(Float32Array::from(flat)),
        None,
    ));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "embedding",
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
        true,
    )]));
    Arc::new(MessageBatch::new_arrow(
        RecordBatch::try_new(schema, vec![list]).unwrap(),
    ))
}

async fn count(admin: &mut PgConnection, table: &str) -> i64 {
    let (count,): (i64,) =
        sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
            .fetch_one(admin)
            .await
            .unwrap();
    count
}

// ---------------------------------------------------------------------
// sql output against plain Postgres
// ---------------------------------------------------------------------

#[tokio::test]
async fn sql_output_pg_write_upsert_and_readback() {
    ensure_init();
    let case = "sql_output_pg_write_upsert_and_readback";
    let uri = pg_uri();
    let Some(mut admin) = try_connect(&uri, case).await else {
        return;
    };
    let table = unique_table("arkflow_sql_out");
    exec(&mut admin, &format!("DROP TABLE IF EXISTS {table}")).await;
    exec(
        &mut admin,
        &format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT, score DOUBLE PRECISION, active BOOLEAN, note TEXT)"
        ),
    )
    .await;

    let output = build_output(
        "sql",
        serde_json::json!({
            "output_type": {"type": "postgres", "uri": uri},
            "table_name": table,
            "upsert": true,
            "upsert_keys": ["id"]
        }),
    );
    output.connect().await.expect("connect sql output");

    // Plain write of two rows, one with a NULL note.
    output
        .write(events_batch(vec![
            (1, "alpha", 1.5, true, None),
            (2, "beta", -2.25, false, Some("hi")),
        ]))
        .await
        .expect("write");
    let rows: Vec<(i64, String, f64, bool, Option<String>)> = sqlx::query_as(sqlx::AssertSqlSafe(
        format!("SELECT id, name, score, active, note FROM {table} ORDER BY id"),
    ))
    .fetch_all(&mut admin)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2, "both rows must be inserted");
    assert_eq!(rows[0], (1, "alpha".to_string(), 1.5, true, None));
    assert_eq!(
        rows[1],
        (2, "beta".to_string(), -2.25, false, Some("hi".to_string()))
    );

    // Upsert: rewriting id=1 updates in place instead of duplicating.
    output
        .write(events_batch(vec![(1, "alpha2", 9.5, false, Some("upd"))]))
        .await
        .expect("upsert write");
    assert_eq!(
        count(&mut admin, &table).await,
        2,
        "upsert must not duplicate"
    );
    let (name, note): (String, Option<String>) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT name, note FROM {table} WHERE id = 1"
    )))
    .fetch_one(&mut admin)
    .await
    .unwrap();
    assert_eq!(name, "alpha2");
    assert_eq!(note.as_deref(), Some("upd"));

    // write_batch commits the whole ack range atomically.
    output
        .write_batch(&[
            events_batch(vec![(3, "gamma", 0.0, true, None)]),
            events_batch(vec![(4, "delta", 3.75, false, Some("batch"))]),
        ])
        .await
        .expect("write_batch");
    assert_eq!(count(&mut admin, &table).await, 4);

    // Close is idempotent-ish: closing twice stays Ok, writing after close
    // reports the disconnection instead of silently dropping data.
    output.close().await.expect("close");
    assert!(output.close().await.is_ok(), "second close must succeed");
    let err = output
        .write(events_batch(vec![(5, "epsilon", 1.0, true, None)]))
        .await
        .expect_err("write after close must fail");
    assert!(matches!(err, arkflow_core::Error::Disconnection), "{err:?}");

    exec(&mut admin, &format!("DROP TABLE IF EXISTS {table}")).await;
}

#[tokio::test]
async fn sql_output_pg_batch_rolls_back_on_failure() {
    ensure_init();
    let case = "sql_output_pg_batch_rolls_back_on_failure";
    let uri = pg_uri();
    let Some(mut admin) = try_connect(&uri, case).await else {
        return;
    };
    let table = unique_table("arkflow_sql_tx");
    exec(&mut admin, &format!("DROP TABLE IF EXISTS {table}")).await;
    exec(
        &mut admin,
        &format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT)"),
    )
    .await;

    let output = build_output(
        "sql",
        serde_json::json!({
            "output_type": {"type": "postgres", "uri": uri},
            "table_name": table
        }),
    );
    output.connect().await.expect("connect sql output");

    // First message fits the table, second carries an unknown column: the
    // whole transaction must roll back, leaving the table empty.
    let err = output
        .write_batch(&[
            events_batch(vec![(1, "alpha", 1.5, true, None)]),
            bad_batch(2, "beta"),
        ])
        .await
        .expect_err("write_batch with an invalid column must fail");
    assert!(
        format!("{err}").contains("Failed to execute"),
        "expected the SQL execution error, got: {err}"
    );
    assert_eq!(
        count(&mut admin, &table).await,
        0,
        "the earlier insert must be rolled back with its batch"
    );

    // A single write against a table without the batch's columns errors too.
    let err = output
        .write(bad_batch(3, "gamma"))
        .await
        .expect_err("write with an invalid column must fail");
    assert!(
        format!("{err}").contains("Failed to execute"),
        "expected the SQL execution error, got: {err}"
    );

    // Upsert keys missing from the schema are rejected before any SQL runs.
    let upsert_output = build_output(
        "sql",
        serde_json::json!({
            "output_type": {"type": "postgres", "uri": uri},
            "table_name": table,
            "upsert": true,
            "upsert_keys": ["absent_key"]
        }),
    );
    upsert_output
        .connect()
        .await
        .expect("connect upsert output");
    let err = upsert_output
        .write(events_batch(vec![(4, "delta", 1.0, true, None)]))
        .await
        .expect_err("missing upsert key must be rejected");
    assert!(
        format!("{err}").contains("absent_key"),
        "error must name the missing upsert key: {err}"
    );
    assert_eq!(count(&mut admin, &table).await, 0);

    output.close().await.expect("close");
    upsert_output.close().await.expect("close upsert output");
    exec(&mut admin, &format!("DROP TABLE IF EXISTS {table}")).await;
}

// ---------------------------------------------------------------------
// pgvector output and search processor against pgvector Postgres
// ---------------------------------------------------------------------

#[tokio::test]
async fn pgvector_output_roundtrip_and_upsert() {
    ensure_init();
    let case = "pgvector_output_roundtrip_and_upsert";
    let uri = pgvector_uri();
    let Some(mut admin) = try_connect(&uri, case).await else {
        return;
    };
    if sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
        .execute(&mut admin)
        .await
        .is_err()
    {
        eprintln!("--- SKIPPED {case}: cannot CREATE EXTENSION vector on {uri} ---");
        return;
    }
    let table = unique_table("arkflow_pgvec_out");
    exec(&mut admin, &format!("DROP TABLE IF EXISTS {table}")).await;
    exec(
        &mut admin,
        &format!(
            "CREATE TABLE {table} (doc_id BIGINT PRIMARY KEY, embedding vector(2), payload jsonb)"
        ),
    )
    .await;

    let output = build_output(
        "pgvector",
        serde_json::json!({
            "url": uri,
            "table": table,
            "id_field": "doc_id",
        }),
    );
    output.connect().await.expect("connect pgvector output");

    let batch = vector_doc_batch(vec![(10, [1.5, 2.0], "hello"), (20, [-3.0, 4.25], "world")]);
    output.write(batch.clone()).await.expect("write vectors");

    let rows: Vec<(i64, String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT doc_id, embedding::text, payload::text FROM {table} ORDER BY doc_id"
    )))
    .fetch_all(&mut admin)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 10);
    assert_eq!(rows[0].1, "[1.5,2]", "vector text must round-trip");
    assert_eq!(rows[1].1, "[-3,4.25]");
    let payload: serde_json::Value = serde_json::from_str(&rows[0].2).unwrap();
    assert_eq!(
        payload["text"], "hello",
        "payload must pack the text column"
    );

    // Upsert: writing the same ids again overwrites, never duplicates.
    output.write(batch).await.expect("rewrite same ids");
    assert_eq!(count(&mut admin, &table).await, 2);

    // write_batch (trait default: per-message writes) adds the new ids.
    output
        .write_batch(&[
            vector_doc_batch(vec![(30, [0.0, 1.0], "third")]),
            vector_doc_batch(vec![(40, [1.0, 0.0], "fourth")]),
        ])
        .await
        .expect("write_batch vectors");
    assert_eq!(count(&mut admin, &table).await, 4);

    output.close().await.expect("close pgvector output");
    let err = output
        .write(vector_doc_batch(vec![(50, [0.0, 0.0], "closed")]))
        .await
        .expect_err("write after close must fail");
    assert!(
        format!("{err}").to_lowercase().contains("not connected"),
        "{err}"
    );

    exec(&mut admin, &format!("DROP TABLE IF EXISTS {table}")).await;
}

#[tokio::test]
async fn pgvector_search_processor_returns_nearest_neighbors() {
    ensure_init();
    let case = "pgvector_search_processor_returns_nearest_neighbors";
    let uri = pgvector_uri();
    let Some(mut admin) = try_connect(&uri, case).await else {
        return;
    };
    if sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
        .execute(&mut admin)
        .await
        .is_err()
    {
        eprintln!("--- SKIPPED {case}: cannot CREATE EXTENSION vector on {uri} ---");
        return;
    }
    let table = unique_table("arkflow_pgvec_search");
    exec(&mut admin, &format!("DROP TABLE IF EXISTS {table}")).await;
    exec(
        &mut admin,
        &format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, embedding vector(2), payload jsonb)"
        ),
    )
    .await;
    for (id, vector, text) in [
        (1i64, "[1.0,0.0]", "near"),
        (2, "[0.9,0.1]", "also near"),
        (3, "[0.0,1.0]", "far"),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} (id, embedding, payload) VALUES ($1, $2::vector, $3::jsonb)"
        )))
        .bind(id)
        .bind(vector)
        .bind(serde_json::json!({"text": text}).to_string())
        .execute(&mut admin)
        .await
        .unwrap();
    }

    let processor = ProcessorConfig {
        processor_type: "pgvector_search".to_string(),
        name: None,
        config: Some(serde_json::json!({
            "url": uri,
            "table": table,
            "top_k": 2,
        })),
    }
    .build(&resource())
    .expect("build pgvector_search processor");

    let result = processor
        .process(query_vector_batch(vec![vec![1.0, 0.0], vec![0.0, 1.0]]))
        .await
        .expect("search must succeed");
    let ProcessResult::Single(output) = result else {
        panic!("expected a single output batch");
    };
    let matches = output
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("matches column is Utf8");

    let row0: serde_json::Value = serde_json::from_str(matches.value(0)).unwrap();
    assert_eq!(row0.as_array().unwrap().len(), 2, "top_k = 2");
    assert_eq!(row0[0]["id"], "1", "nearest neighbor comes first");
    assert_eq!(row0[0]["payload"]["text"], "near");
    assert!(
        row0[0]["distance"].as_f64().unwrap() < row0[1]["distance"].as_f64().unwrap(),
        "distances must be ascending: {row0}"
    );

    let row1: serde_json::Value = serde_json::from_str(matches.value(1)).unwrap();
    assert_eq!(row1[0]["id"], "3", "order follows the input rows");
    assert_eq!(row1[0]["payload"]["text"], "far");

    // Payload disabled: the matches carry no payload key at all.
    let no_payload = ProcessorConfig {
        processor_type: "pgvector_search".to_string(),
        name: None,
        config: Some(serde_json::json!({
            "url": uri,
            "table": table,
            "top_k": 1,
            "payload_column": "",
        })),
    }
    .build(&resource())
    .expect("build payload-less processor");
    let result = no_payload
        .process(query_vector_batch(vec![vec![1.0, 0.0]]))
        .await
        .expect("payload-less search must succeed");
    let ProcessResult::Single(output) = result else {
        panic!("expected a single output batch");
    };
    let matches = output
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("matches column is Utf8");
    let parsed: serde_json::Value = serde_json::from_str(matches.value(0)).unwrap();
    assert!(parsed[0].get("payload").is_none(), "{parsed}");
    assert_eq!(parsed[0]["id"], "1");

    // A missing table surfaces the Postgres error, not a panic.
    let failing = ProcessorConfig {
        processor_type: "pgvector_search".to_string(),
        name: None,
        config: Some(serde_json::json!({
            "url": uri,
            "table": "arkflow_pgvector_missing_table",
            "timeout_ms": 5000,
        })),
    }
    .build(&resource())
    .expect("build failing processor");
    let err = failing
        .process(query_vector_batch(vec![vec![1.0, 0.0]]))
        .await
        .expect_err("missing table must fail");
    let msg = format!("{err}");
    assert!(msg.contains("query failed"), "{msg}");

    processor.close().await.expect("close processor");
    no_payload
        .close()
        .await
        .expect("close payload-less processor");
    failing.close().await.expect("close failing processor");
    exec(&mut admin, &format!("DROP TABLE IF EXISTS {table}")).await;
}
