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

//! pgvector output component
//!
//! Upserts each batch's rows into a PostgreSQL table with the pgvector
//! extension: a Float32 list column is written as the vector (text bind
//! with an explicit `::vector` cast), every remaining column is packed
//! per-row into a jsonb payload column, and an optional id column keys
//! `ON CONFLICT ... DO UPDATE`. No new dependencies: values travel as
//! bound text parameters. `url` supports secret references.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arkflow_core::component::{register_output_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatchRef, Resource};
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, FixedSizeListArray, Float32Array, Int32Array, Int64Array, LargeStringArray, ListArray,
    StringArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::json::LineDelimitedWriter;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::QueryBuilder;
use tokio::sync::Mutex;

pub fn init() -> Result<(), Error> {
    register_output_builder("pgvector", Arc::new(PgVectorOutputBuilder))?;
    register_output_metadata(ComponentMetadata::with_schema(
        "pgvector",
        "Upserts batch rows into a PostgreSQL table with the pgvector extension: a Float32 list column becomes the vector, other columns are packed into a jsonb payload, and an optional id column keys ON CONFLICT upserts.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "url": {"type": "string", "description": "Postgres connection string, e.g. postgres://user:pass@host:5432/db; supports secret references."},
                "table": {"type": "string", "description": "Target table (must already exist with a matching vector column)."},
                "vector_field": {"type": "string", "description": "Name of the vector column (FixedSizeList/List of Float32). Defaults to 'embedding'."},
                "id_field": {"type": "string", "description": "Column used as the upsert conflict key (integer or string). When omitted, plain INSERTs are written."},
                "payload_field": {"type": "string", "description": "jsonb column receiving every remaining column as a per-row JSON object. Defaults to 'payload'; set to an empty string to disable."},
                "max_connections": {"type": "integer", "description": "Connection pool size. Defaults to 4."},
                "timeout_ms": {"type": "integer", "description": "Pool acquire/connect timeout in milliseconds. Defaults to 30000."}
            },
            "required": ["url", "table"]
        }),
    )
    .with_example(serde_json::json!({
        "url": "postgres://postgres:postgres@localhost:5432/vectors",
        "table": "documents",
        "vector_field": "embedding",
        "id_field": "doc_id"
    })))?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PgVectorOutputConfig {
    url: String,
    table: String,
    #[serde(default = "default_vector_field")]
    vector_field: String,
    #[serde(default)]
    id_field: Option<String>,
    #[serde(default = "default_payload_field")]
    payload_field: String,
    #[serde(default = "default_max_connections")]
    max_connections: u32,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
}

fn default_vector_field() -> String {
    "embedding".to_string()
}
fn default_payload_field() -> String {
    "payload".to_string()
}
fn default_max_connections() -> u32 {
    4
}
fn default_timeout_ms() -> u64 {
    30000
}

#[derive(Clone)]
enum IdValue {
    Int(i64),
    Text(String),
}

struct PointRow {
    id: Option<IdValue>,
    vector: String,
    payload: Option<String>,
}

struct PgVectorOutput {
    config: PgVectorOutputConfig,
    pool: Mutex<Option<sqlx::PgPool>>,
    connected: AtomicBool,
}

#[async_trait]
impl Output for PgVectorOutput {
    async fn connect(&self) -> Result<(), Error> {
        let pool = PgPoolOptions::new()
            .max_connections(self.config.max_connections)
            .acquire_timeout(Duration::from_millis(self.config.timeout_ms))
            .connect(&self.config.url)
            .await
            .map_err(|e| Error::Connection(format!("Unable to connect to Postgres: {}", e)))?;
        *self.pool.lock().await = Some(pool);
        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
        let rows = msg.num_rows();
        if rows == 0 {
            return Ok(());
        }
        if !self.connected.load(Ordering::SeqCst) {
            return Err(Error::Connection("The output is not connected".to_string()));
        }
        // Hold the pool lock from the connectivity check through the insert:
        // releasing between the two would let a concurrent `close()` swap the
        // pool to `None` and turn the lookup below into a panic instead of a
        // retryable connection error.
        let pool = self.pool.lock().await;
        let Some(pool) = pool.as_ref() else {
            return Err(Error::Connection("The output is not connected".to_string()));
        };

        let vectors = extract_vectors(&msg, &self.config.vector_field)?;
        let ids = extract_ids(&msg, &self.config.id_field)?;
        let payloads = extract_payloads(&msg, &self.config)?;

        let point_rows: Vec<PointRow> = (0..rows)
            .map(|row| PointRow {
                id: ids.as_ref().map(|ids| ids[row].clone()),
                vector: vectors[row].clone(),
                payload: payloads.as_ref().map(|payloads| payloads[row].clone()),
            })
            .collect();

        build_insert(&self.config, &point_rows)
            .build()
            .execute(pool)
            .await
            .map_err(|e| Error::Process(format!("pgvector output: insert failed: {}", e)))?;
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        self.connected.store(false, Ordering::SeqCst);
        *self.pool.lock().await = None;
        Ok(())
    }
}

/// Builds the parameterized INSERT (with the optional upsert clause).
/// Returned as a `QueryBuilder` so tests can assert the generated SQL
/// text via `.build().sql()`.
fn build_insert(config: &PgVectorOutputConfig, rows: &[PointRow]) -> QueryBuilder<'static, sqlx::Postgres> {
    let mut columns: Vec<String> = Vec::with_capacity(3);
    if config.id_field.is_some() {
        columns.push(config.id_field.clone().unwrap());
    }
    columns.push(config.vector_field.clone());
    let payload_enabled = !config.payload_field.is_empty();
    if payload_enabled {
        columns.push(config.payload_field.clone());
    }
    let column_list: Vec<String> = columns
        .iter()
        .map(|column| format!("\"{column}\""))
        .collect();

    let mut query_builder = QueryBuilder::<sqlx::Postgres>::new(format!(
        "INSERT INTO \"{}\" ({})",
        config.table,
        column_list.join(", ")
    ));
    query_builder.push(" VALUES ");
    let mut first_row = true;
    for row in rows {
        if first_row {
            query_builder.push("(");
            first_row = false;
        } else {
            query_builder.push(", (");
        }
        {
            // Fresh per-row separator: first value unseparated, the rest after ", ".
            let mut values = query_builder.separated(", ");
            if let Some(id) = &row.id {
                match id {
                    IdValue::Int(value) => values.push_bind(*value),
                    IdValue::Text(value) => values.push_bind(value.clone()),
                };
            }
            values.push_bind(row.vector.clone());
            values.push_unseparated("::vector");
            if let Some(payload) = &row.payload {
                values.push_bind(payload.clone());
                values.push_unseparated("::jsonb");
            }
        }
        query_builder.push(")");
    }

    if let Some(id_field) = &config.id_field {
        let mut assignments = vec![format!(
            "\"{}\" = EXCLUDED.\"{}\"",
            config.vector_field, config.vector_field
        )];
        if payload_enabled {
            assignments.push(format!(
                "\"{}\" = EXCLUDED.\"{}\"",
                config.payload_field, config.payload_field
            ));
        }
        query_builder.push(format!(
            " ON CONFLICT (\"{id_field}\") DO UPDATE SET {}",
            assignments.join(", ")
        ));
    }
    query_builder
}

fn extract_vectors(batch: &MessageBatchRef, field: &str) -> Result<Vec<String>, Error> {
    let column = find_column(batch, field)?;
    let rows = column.len();
    let mut floats: Vec<Vec<f32>> = Vec::with_capacity(rows);
    match column.data_type() {
        DataType::FixedSizeList(_, dim) => {
            let list = column
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| not_a_vector_error(field))?;
            let values = list
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| not_a_vector_error(field))?;
            for row in 0..rows {
                if list.is_null(row) {
                    return Err(null_vector_error(field, row));
                }
                let start = row as i64 * *dim as i64;
                floats.push(
                    (start..start + *dim as i64)
                        .map(|i| values.value(i as usize))
                        .collect(),
                );
            }
        }
        DataType::List(_) => {
            let list = column
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| not_a_vector_error(field))?;
            let values = list
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| not_a_vector_error(field))?;
            for row in 0..rows {
                if list.is_null(row) {
                    return Err(null_vector_error(field, row));
                }
                let offsets = list.value_offsets();
                floats.push(
                    (offsets[row]..offsets[row + 1])
                        .map(|i| values.value(i as usize))
                        .collect(),
                );
            }
        }
        other => {
            return Err(Error::Process(format!(
                "pgvector output: column '{}' must be FixedSizeList(Float32) or List(Float32), got {:?}",
                field, other
            )));
        }
    }

    let mut texts = Vec::with_capacity(floats.len());
    for (row, vector) in floats.iter().enumerate() {
        if vector.is_empty() {
            return Err(Error::Process(format!(
                "pgvector output: column '{}' has an empty vector at row {row}",
                field
            )));
        }
        // pgvector text format: [1.0,2.0]
        let parts: Vec<String> = vector
            .iter()
            .map(|value| {
                let mut formatted = format!("{value}");
                if !formatted.contains('.') && !formatted.contains('e') && !formatted.contains('E')
                {
                    formatted.push_str(".0");
                }
                formatted
            })
            .collect();
        texts.push(format!("[{}]", parts.join(",")));
    }
    Ok(texts)
}

fn not_a_vector_error(field: &str) -> Error {
    Error::Process(format!(
        "pgvector output: column '{}' is not a Float32 vector list",
        field
    ))
}

fn null_vector_error(field: &str, row: usize) -> Error {
    Error::Process(format!(
        "pgvector output: column '{}' has a null vector at row {row}",
        field
    ))
}

fn extract_ids(
    batch: &MessageBatchRef,
    field: &Option<String>,
) -> Result<Option<Vec<IdValue>>, Error> {
    let field = match field {
        Some(field) if !field.trim().is_empty() => field.clone(),
        _ => return Ok(None),
    };
    let column = find_column(batch, &field)?;
    let mut ids = Vec::with_capacity(column.len());
    match column.data_type() {
        DataType::Int64 => {
            let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
            for row in 0..array.len() {
                if array.is_null(row) {
                    return Err(null_id_error(&field, row));
                }
                let value = array.value(row);
                let id = u64::try_from(value).map_err(|_| {
                    Error::Process(format!(
                        "pgvector output: id column '{field}' has a negative value {value} at row {row}"
                    ))
                })?;
                ids.push(IdValue::Int(id as i64));
            }
        }
        DataType::Int32 => {
            let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
            for row in 0..array.len() {
                if array.is_null(row) {
                    return Err(null_id_error(&field, row));
                }
                if array.value(row) < 0 {
                    return Err(Error::Process(format!(
                        "pgvector output: id column '{field}' has a negative value at row {row}"
                    )));
                }
                ids.push(IdValue::Int(array.value(row) as i64));
            }
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            for row in 0..column.len() {
                if column.is_null(row) {
                    return Err(null_id_error(&field, row));
                }
                ids.push(IdValue::Text(downcast_string_value(column, row)?.to_string()));
            }
        }
        other => {
            return Err(Error::Process(format!(
                "pgvector output: id column '{}' must be Int64/Int32 or Utf8, got {:?}",
                field, other
            )));
        }
    }
    Ok(Some(ids))
}

fn null_id_error(field: &str, row: usize) -> Error {
    Error::Process(format!(
        "pgvector output: id column '{field}' has a null value at row {row}"
    ))
}

fn downcast_string_value(column: &Arc<dyn Array>, row: usize) -> Result<&str, Error> {
    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        Ok(array.value(row))
    } else if let Some(array) = column.as_any().downcast_ref::<LargeStringArray>() {
        Ok(array.value(row))
    } else {
        Err(Error::Process(
            "pgvector output: unexpected string array type".to_string(),
        ))
    }
}

fn extract_payloads(
    batch: &MessageBatchRef,
    config: &PgVectorOutputConfig,
) -> Result<Option<Vec<String>>, Error> {
    if config.payload_field.is_empty() {
        return Ok(None);
    }
    let excluded: Vec<String> = vec![
        Some(config.vector_field.clone()),
        config.id_field.clone(),
    ]
    .into_iter()
    .flatten()
    .collect();
    let payload_columns: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .filter(|name| !excluded.contains(name))
        .collect();

    if payload_columns.is_empty() {
        let empty = json!({}).to_string();
        return Ok(Some(vec![empty; batch.num_rows()]));
    }

    let filtered = batch
        .filter_columns(&payload_columns.iter().cloned().collect::<std::collections::HashSet<_>>())?;
    let mut buffer = Vec::new();
    let mut writer = LineDelimitedWriter::new(&mut buffer);
    writer
        .write(&filtered)
        .map_err(|e| Error::Process(format!("pgvector output: payload serialization failed: {}", e)))?;
    writer
        .finish()
        .map_err(|e| Error::Process(format!("pgvector output: payload serialization failed: {}", e)))?;
    let text = String::from_utf8(buffer)
        .map_err(|e| Error::Process(format!("pgvector output: payload is not UTF-8: {}", e)))?;
    text.lines()
        .map(|line| {
            // Re-serialize compactly so each payload is one JSON object text.
            let value: Value = serde_json::from_str(line)
                .map_err(|e| Error::Process(format!("pgvector output: payload parse failed: {}", e)))?;
            Ok(value.to_string())
        })
        .collect::<Result<Vec<String>, Error>>()
        .map(Some)
}

fn find_column<'a>(
    batch: &'a MessageBatchRef,
    field: &str,
) -> Result<&'a Arc<dyn Array>, Error> {
    batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == field)
        .map(|index| batch.column(index))
        .ok_or_else(|| Error::Process(format!("pgvector output: column '{}' not found", field)))
}

struct PgVectorOutputBuilder;
impl OutputBuilder for PgVectorOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<Value>,
        _codec: Option<Arc<dyn arkflow_core::codec::Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        let mut config: PgVectorOutputConfig = parse_config(config, "pgvector output")?;
        config.id_field = config
            .id_field
            .filter(|field| !field.trim().is_empty());
        if config.url.trim().is_empty() {
            return Err(Error::Config(
                "pgvector output: 'url' must not be empty".to_string(),
            ));
        }
        if config.table.trim().is_empty() {
            return Err(Error::Config(
                "pgvector output: 'table' must not be empty".to_string(),
            ));
        }
        if config.max_connections == 0 {
            return Err(Error::Config(
                "pgvector output: 'max_connections' must be at least 1".to_string(),
            ));
        }
        // Touch the URL so a malformed connection string fails at build time.
        Url::parse(&config.url).map_err(|e| {
            Error::Config(format!("pgvector output: invalid 'url': {e}"))
        })?;
        Ok(Arc::new(PgVectorOutput {
            config,
            pool: Mutex::new(None),
            connected: AtomicBool::new(false),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::MessageBatch;
    use datafusion::arrow::array::{ArrayRef, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::cell::RefCell;

    fn test_resource() -> Resource {
        Resource {
            temporary: Default::default(),
            input_names: RefCell::new(Default::default()),
        }
    }

    fn build_output(config: Value) -> Arc<dyn Output> {
        PgVectorOutputBuilder
            .build(None, &Some(config), None, &test_resource())
            .unwrap()
    }

    fn config_with(extra: Value) -> PgVectorOutputConfig {
        let mut config = serde_json::json!({
            "url": "postgres://postgres:postgres@localhost:5432/vectors",
            "table": "documents",
            "id_field": "doc_id",
        });
        let obj = config.as_object_mut().unwrap();
        for (key, value) in extra.as_object().unwrap() {
            obj.insert(key.clone(), value.clone());
        }
        let mut parsed: PgVectorOutputConfig =
            parse_config(&Some(config), "pgvector output").unwrap();
        parsed.id_field = parsed.id_field.filter(|field| !field.trim().is_empty());
        parsed
    }

    fn sample_batch() -> MessageBatchRef {
        let dim = 2i32;
        let item_field = Arc::new(Field::new("item", DataType::Float32, true));
        let flat = Float32Array::from(vec![1.5f32, 2.0, -3.0, 4.25]);
        let vectors = Arc::new(FixedSizeListArray::new(item_field, dim, Arc::new(flat), None));
        let schema = Arc::new(Schema::new(vec![
            Field::new("doc_id", DataType::Int64, false),
            Field::new("embedding", DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim), true),
            Field::new("text", DataType::Utf8, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![10i64, 20])),
            vectors,
            Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
        ];
        Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(schema, columns).unwrap(),
        ))
    }

    fn row_of(id: i64, vector: &str, payload: &str) -> PointRow {
        PointRow {
            id: Some(IdValue::Int(id)),
            vector: vector.to_string(),
            payload: Some(payload.to_string()),
        }
    }

    #[test]
    fn insert_sql_shape_with_all_columns() {
        let config = config_with(serde_json::json!({}));
        let sql = build_insert(
            &config,
            &[row_of(1, "[1.0,2.0]", r#"{"text":"a"}"#), row_of(2, "[3.0,4.0]", r#"{"text":"b"}"#)],
        )
        .sql()
        .to_string();
        assert_eq!(
            sql,
            "INSERT INTO \"documents\" (\"doc_id\", \"embedding\", \"payload\") VALUES ($1, $2::vector, $3::jsonb), ($4, $5::vector, $6::jsonb) ON CONFLICT (\"doc_id\") DO UPDATE SET \"embedding\" = EXCLUDED.\"embedding\", \"payload\" = EXCLUDED.\"payload\""
        );
    }

    #[test]
    fn insert_sql_without_id_is_plain_insert() {
        let config = config_with(serde_json::json!({"id_field": ""}));
        let rows = vec![PointRow {
            id: None,
            vector: "[1.0,2.0]".to_string(),
            payload: Some(r#"{"text":"a"}"#.to_string()),
        }];
        let sql = build_insert(&config, &rows).sql().to_string();
        assert_eq!(
            sql,
            "INSERT INTO \"documents\" (\"embedding\", \"payload\") VALUES ($1::vector, $2::jsonb)"
        );
    }

    #[test]
    fn insert_sql_with_payload_disabled() {
        let config = config_with(serde_json::json!({"payload_field": ""}));
        let rows = vec![PointRow {
            id: Some(IdValue::Text("doc-1".to_string())),
            vector: "[1.0,2.0]".to_string(),
            payload: None,
        }];
        let sql = build_insert(&config, &rows).sql().to_string();
        assert_eq!(
            sql,
            "INSERT INTO \"documents\" (\"doc_id\", \"embedding\") VALUES ($1, $2::vector) ON CONFLICT (\"doc_id\") DO UPDATE SET \"embedding\" = EXCLUDED.\"embedding\""
        );
    }

    #[test]
    fn vector_text_format_roundtrip() {
        let batch = sample_batch();
        let vectors = extract_vectors(&batch, "embedding").unwrap();
        assert_eq!(vectors, vec!["[1.5,2.0]", "[-3.0,4.25]"]);
    }

    #[tokio::test]
    async fn payload_packs_remaining_columns() {
        let config = config_with(serde_json::json!({}));
        let payloads = extract_payloads(&sample_batch(), &config).unwrap().unwrap();
        let parsed: Vec<Value> = payloads
            .iter()
            .map(|payload| serde_json::from_str(payload).unwrap())
            .collect();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["text"], "hello");
        assert_eq!(parsed[1]["text"], "world");
        assert!(parsed[0].get("doc_id").is_none(), "id must not leak into payload");
        assert!(parsed[0].get("embedding").is_none(), "vector must not leak into payload");
    }

    #[tokio::test]
    async fn payload_disabled_yields_none() {
        let config = config_with(serde_json::json!({"payload_field": ""}));
        assert!(extract_payloads(&sample_batch(), &config).unwrap().is_none());
    }

    #[tokio::test]
    async fn null_vector_row_errors() {
        let dim = 2i32;
        let with_null = FixedSizeListArray::from_iter_primitive::<
            datafusion::arrow::datatypes::Float32Type,
            _,
            _,
        >(vec![Some(vec![Some(1.0), Some(2.0)]), None], dim);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
            true,
        )]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(schema, vec![Arc::new(with_null)]).unwrap(),
        ));
        let err = extract_vectors(&batch, "embedding").unwrap_err().to_string();
        assert!(err.contains("null vector at row 1"), "{err}");
    }

    #[tokio::test]
    async fn connect_to_unreachable_address_yields_connection_error() {
        // Bind then drop a listener to get a guaranteed-closed port.
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let closed_port = probe.local_addr().unwrap().port();
        drop(probe);
        let output = build_output(serde_json::json!({
            "url": format!("postgres://postgres:postgres@127.0.0.1:{closed_port}/vectors"),
            "table": "documents",
            "timeout_ms": 500,
        }));
        let err = output.connect().await.unwrap_err().to_string();
        assert!(err.to_lowercase().contains("unable to connect"), "{err}");
    }

    #[tokio::test]
    async fn missing_vector_column_errors() {
        let config = config_with(serde_json::json!({"vector_field": "nope"}));
        let err = extract_vectors(&sample_batch(), &config.vector_field)
            .unwrap_err()
            .to_string();
        assert!(err.contains("'nope'"), "{err}");
    }

    #[tokio::test]
    async fn empty_batch_succeeds_without_sql() {
        let output = build_output(serde_json::json!({
            "url": "postgres://postgres:postgres@localhost:5432/vectors",
            "table": "documents",
        }));
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(Vec::<Option<&str>>::new()))])
                .unwrap(),
        ));
        // Not connected: an empty batch must short-circuit before the
        // connection check produces an error.
        let result = output.write(batch).await;
        assert!(result.is_ok(), "empty batch must short-circuit: {result:?}");
    }

    #[tokio::test]
    async fn write_before_connect_errors() {
        let output = build_output(serde_json::json!({
            "url": "postgres://postgres:postgres@localhost:5432/vectors",
            "table": "documents",
        }));
        let err = output.write(sample_batch()).await.unwrap_err().to_string();
        assert!(err.to_lowercase().contains("not connected"), "{err}");
    }

    #[test]
    fn invalid_configs_rejected() {
        for bad in [
            serde_json::json!({"table": "t"}),
            serde_json::json!({"url": "postgres://localhost/db"}),
            serde_json::json!({"url": "not a url at all", "table": "t"}),
            serde_json::json!({
                "url": "postgres://localhost/db",
                "table": "t",
                "max_connections": 0
            }),
        ] {
            assert!(
                PgVectorOutputBuilder
                    .build(None, &Some(bad), None, &test_resource())
                    .is_err(),
                "config must be rejected"
            );
        }
    }

    /// Live round-trip against a real Postgres with pgvector. Run with:
    /// `docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres pgvector/pgvector:pg16`
    /// then `cargo test -p arkflow-plugin --lib output::pgvector -- --ignored`
    #[tokio::test]
    #[ignore = "requires a live Postgres with the pgvector extension"]
    async fn live_pgvector_insert_then_upsert() {
        let output = build_output(serde_json::json!({
            "url": "postgres://postgres:postgres@localhost:5432/postgres",
            "table": "arkflow_pgvector_test",
            "id_field": "doc_id",
        }));
        output.connect().await.unwrap();
        let pool = sqlx::PgPool::connect("postgres://postgres:postgres@localhost:5432/postgres")
            .await
            .unwrap();
        sqlx::query("DROP TABLE IF EXISTS arkflow_pgvector_test")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE arkflow_pgvector_test (doc_id BIGINT PRIMARY KEY, embedding vector(2), payload jsonb)",
        )
        .execute(&pool)
        .await
        .unwrap();

        output.write(sample_batch()).await.unwrap();
        let first = sqlx::query_as::<_, (String, String)>(
            "SELECT embedding::text, payload::text FROM arkflow_pgvector_test ORDER BY doc_id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(first, vec![("[1.5,2]".to_string(), r#"{"text": "hello"}"#.to_string()), ("[-3,4.25]".to_string(), r#"{"text": "world"}"#.to_string())]);

        // Same ids again: the upsert must overwrite, not duplicate.
        output.write(sample_batch()).await.unwrap();
        let count = sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM arkflow_pgvector_test")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count.0, 2);
        sqlx::query("DROP TABLE arkflow_pgvector_test")
            .execute(&pool)
            .await
            .unwrap();
    }
}
