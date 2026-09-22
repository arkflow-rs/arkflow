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

//! pgvector similarity search processor
//!
//! Reads a Float32 list column from the batch (typically produced by the
//! `embedding` processor), runs one nearest-neighbor SELECT per row
//! against a PostgreSQL table with the pgvector extension, and appends
//! the matches as a JSON array text column. The table shape mirrors what
//! the `pgvector` output writes: an id column, a vector column, and an
//! optional jsonb payload column. Requests run with bounded,
//! order-preserving concurrency; the pool is created lazily so building
//! never touches the network.

use std::sync::Arc;
use std::time::Duration;

use arkflow_core::component::{register_processor_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, ProcessResult, Resource};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, FixedSizeListArray, Float32Array, ListArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

pub fn init() -> Result<(), Error> {
    register_processor_builder("pgvector_search", Arc::new(PgVectorSearchProcessorBuilder))?;
    register_processor_metadata(ComponentMetadata::with_schema(
        "pgvector_search",
        "Searches a PostgreSQL table with the pgvector extension for the top-k nearest neighbors of each row's vector and appends the matches as a JSON array text column.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "url": {"type": "string", "description": "Postgres connection string; supports secret references."},
                "table": {"type": "string", "description": "Table to search (must already exist with a pgvector column)."},
                "vector_field": {"type": "string", "description": "Name of the query vector column (FixedSizeList/List of Float32). Defaults to 'embedding'."},
                "target_field": {"type": "string", "description": "Name of the appended matches column (JSON array text). Defaults to 'matches'."},
                "id_column": {"type": "string", "description": "Table column returned as the match id (as text). Defaults to 'id'."},
                "vector_column": {"type": "string", "description": "Table column holding the stored vectors. Defaults to 'embedding'."},
                "payload_column": {"type": "string", "description": "jsonb column included in each match as a payload object. Defaults to 'payload'; set to an empty string to disable."},
                "metric": {"type": "string", "enum": ["cosine", "l2", "inner_product"], "description": "Distance operator: <=> (cosine, default), <-> (l2), <#> (inner product, negative)."},
                "top_k": {"type": "integer", "description": "Number of neighbors per row. Defaults to 5."},
                "concurrency": {"type": "integer", "description": "Maximum in-flight queries. Defaults to 4."},
                "max_connections": {"type": "integer", "description": "Connection pool size. Defaults to 4."},
                "timeout_ms": {"type": "integer", "description": "Pool acquire timeout in milliseconds. Defaults to 30000."}
            },
            "required": ["url", "table"]
        }),
    )
    .with_example(serde_json::json!({
        "url": "postgres://postgres:postgres@localhost:5432/vectors",
        "table": "documents",
        "metric": "cosine",
        "top_k": 5
    })))?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PgVectorSearchProcessorConfig {
    url: String,
    table: String,
    #[serde(default = "default_vector_field")]
    vector_field: String,
    #[serde(default = "default_target_field")]
    target_field: String,
    #[serde(default = "default_id_column")]
    id_column: String,
    #[serde(default = "default_vector_column")]
    vector_column: String,
    #[serde(default = "default_payload_column")]
    payload_column: String,
    #[serde(default)]
    metric: Metric,
    #[serde(default = "default_top_k")]
    top_k: usize,
    #[serde(default = "default_concurrency")]
    concurrency: usize,
    #[serde(default = "default_max_connections")]
    max_connections: u32,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
}

fn default_vector_field() -> String {
    "embedding".to_string()
}
fn default_target_field() -> String {
    "matches".to_string()
}
fn default_id_column() -> String {
    "id".to_string()
}
fn default_vector_column() -> String {
    "embedding".to_string()
}
fn default_payload_column() -> String {
    "payload".to_string()
}
fn default_top_k() -> usize {
    5
}
fn default_concurrency() -> usize {
    4
}
fn default_max_connections() -> u32 {
    4
}
fn default_timeout_ms() -> u64 {
    30000
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
enum Metric {
    #[default]
    #[serde(rename = "cosine")]
    Cosine,
    #[serde(rename = "l2")]
    L2,
    #[serde(rename = "inner_product")]
    InnerProduct,
}

impl Metric {
    fn operator(self) -> &'static str {
        match self {
            Metric::Cosine => "<=>",
            Metric::L2 => "<->",
            Metric::InnerProduct => "<#>",
        }
    }
}

struct PgVectorSearchProcessor {
    config: PgVectorSearchProcessorConfig,
    pool: PgPool,
}

#[async_trait]
impl Processor for PgVectorSearchProcessor {
    async fn process(&self, msg_batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        let rows = msg_batch.num_rows();
        if rows == 0 {
            return Ok(ProcessResult::None);
        }

        let vectors = extract_vectors(&msg_batch, &self.config.vector_field)?;
        let matches = self.search_all(&vectors).await?;
        let batch = append_column(&msg_batch, &self.config.target_field, &matches)?;
        Ok(ProcessResult::Single(Arc::new(batch)))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl PgVectorSearchProcessor {
    /// One query per row; results are collected in row order while at
    /// most `concurrency` queries are in flight.
    async fn search_all(&self, vectors: &[Vec<f32>]) -> Result<Vec<String>, Error> {
        let sql = build_search_sql(&self.config);
        let owned: Vec<String> = vectors.iter().map(|v| vector_to_pgvector_text(v)).collect();
        futures_util::stream::iter(owned.into_iter().map(|text| self.search_row(&sql, text)))
            .buffered(self.config.concurrency)
            .collect::<Vec<Result<String, Error>>>()
            .await
            .into_iter()
            .collect()
    }

    async fn search_row(&self, sql: &str, vector_text: String) -> Result<String, Error> {
        let rows: Vec<(String, Option<String>, f64)> = sqlx::query_as(sql)
            .bind(vector_text)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                Error::Process(format!("pgvector_search processor: query failed: {}", e))
            })?;
        rows_to_matches(rows, !self.config.payload_column.is_empty())
    }
}

/// Builds the nearest-neighbor SELECT. The id is returned as text and the
/// payload (when enabled) as text for parsing, keeping sqlx free of the
/// json feature while staying parameterized.
fn build_search_sql(config: &PgVectorSearchProcessorConfig) -> String {
    let mut columns = vec![format!("\"{}\"::text AS \"id\"", config.id_column)];
    if !config.payload_column.is_empty() {
        columns.push(format!(
            "\"{}\"::text AS \"payload\"",
            config.payload_column
        ));
    }
    columns.push(format!(
        "\"{}\" {} $1::vector AS \"distance\"",
        config.vector_column,
        config.metric.operator()
    ));
    format!(
        "SELECT {} FROM \"{}\" ORDER BY \"{}\" {} $1::vector LIMIT {}",
        columns.join(", "),
        config.table,
        config.vector_column,
        config.metric.operator(),
        config.top_k
    )
}

/// Maps fetched rows (id, payload text, distance) into the compact JSON
/// matches array for one input row.
fn rows_to_matches(rows: Vec<(String, Option<String>, f64)>, include_payload: bool) -> Result<String, Error> {
    let items: Vec<Value> = rows
        .into_iter()
        .map(|(id, payload, distance)| {
            let mut object = Map::new();
            object.insert("id".to_string(), json!(id));
            object.insert("distance".to_string(), json!(distance));
            if include_payload {
                let payload: Value = match payload {
                    Some(text) => serde_json::from_str(&text).map_err(|e| {
                        Error::Process(format!(
                            "pgvector_search processor: payload parse failed: {}",
                            e
                        ))
                    })?,
                    None => Value::Null,
                };
                object.insert("payload".to_string(), payload);
            }
            Ok(Value::Object(object))
        })
        .collect::<Result<Vec<Value>, Error>>()?;
    serde_json::to_string(&items)
        .map_err(|e| Error::Process(format!("pgvector_search processor: serialization failed: {}", e)))
}

struct PgVectorSearchProcessorBuilder;
impl ProcessorBuilder for PgVectorSearchProcessorBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        let config: PgVectorSearchProcessorConfig =
            parse_config(config, "pgvector_search processor")?;
        if config.url.trim().is_empty() {
            return Err(Error::Config(
                "pgvector_search processor: 'url' must not be empty".to_string(),
            ));
        }
        if config.table.trim().is_empty() {
            return Err(Error::Config(
                "pgvector_search processor: 'table' must not be empty".to_string(),
            ));
        }
        if config.vector_field.trim().is_empty() {
            return Err(Error::Config(
                "pgvector_search processor: 'vector_field' must not be empty".to_string(),
            ));
        }
        if config.top_k == 0 {
            return Err(Error::Config(
                "pgvector_search processor: 'top_k' must be at least 1".to_string(),
            ));
        }
        if config.concurrency == 0 {
            return Err(Error::Config(
                "pgvector_search processor: 'concurrency' must be at least 1".to_string(),
            ));
        }
        // Lazy pool: no network I/O until the first query, so building a
        // processor stays offline-safe (config validation, tests).
        let options: sqlx::postgres::PgConnectOptions = config
            .url
            .parse()
            .map_err(|e| Error::Config(format!("pgvector_search processor: invalid 'url': {e}")))?;
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(Duration::from_millis(config.timeout_ms))
            .connect_lazy_with(options);
        Ok(Arc::new(PgVectorSearchProcessor { config, pool }))
    }
}

fn extract_vectors(batch: &MessageBatchRef, field: &str) -> Result<Vec<Vec<f32>>, Error> {
    let column = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == field)
        .map(|index| batch.column(index))
        .ok_or_else(|| {
            Error::Process(format!(
                "pgvector_search processor: input column '{}' not found",
                field
            ))
        })?;
    let rows = column.len();
    let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(rows);
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
                vectors.push(
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
                vectors.push(
                    (offsets[row]..offsets[row + 1])
                        .map(|i| values.value(i as usize))
                        .collect(),
                );
            }
        }
        other => {
            return Err(Error::Process(format!(
                "pgvector_search processor: column '{}' must be FixedSizeList(Float32) or List(Float32), got {:?}",
                field, other
            )));
        }
    }
    if let Some((row, _)) = vectors.iter().enumerate().find(|(_, v)| v.is_empty()) {
        return Err(Error::Process(format!(
            "pgvector_search processor: column '{}' has an empty vector at row {row}",
            field
        )));
    }
    Ok(vectors)
}

fn vector_to_pgvector_text(vector: &[f32]) -> String {
    let parts: Vec<String> = vector
        .iter()
        .map(|value| {
            let mut formatted = format!("{value}");
            if !formatted.contains('.') && !formatted.contains('e') && !formatted.contains('E') {
                formatted.push_str(".0");
            }
            formatted
        })
        .collect();
    format!("[{}]", parts.join(","))
}

fn not_a_vector_error(field: &str) -> Error {
    Error::Process(format!(
        "pgvector_search processor: column '{}' is not a Float32 vector list",
        field
    ))
}

fn null_vector_error(field: &str, row: usize) -> Error {
    Error::Process(format!(
        "pgvector_search processor: column '{}' has a null vector at row {row}",
        field
    ))
}

fn append_column(
    batch: &MessageBatch,
    target_field: &str,
    matches: &[String],
) -> Result<MessageBatch, Error> {
    let schema = batch.schema();
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(target_field, DataType::Utf8, true)));
    let mut columns: Vec<ArrayRef> = (0..batch.num_columns())
        .map(|index| batch.column(index).clone())
        .collect();
    columns.push(Arc::new(StringArray::from(matches.to_vec())));

    let record_batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(
        |e| Error::Process(format!("pgvector_search processor: batch rebuild failed: {e}")),
    )?;
    Ok(MessageBatch::new_arrow(record_batch))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::LargeStringArray;
    use std::cell::RefCell;

    fn test_resource() -> Resource {
        Resource {
            temporary: Default::default(),
            input_names: RefCell::new(Default::default()),
        }
    }

    fn config_with(extra: Value) -> PgVectorSearchProcessorConfig {
        let mut config = serde_json::json!({
            "url": "postgres://postgres:postgres@localhost:5432/vectors",
            "table": "documents",
        });
        let obj = config.as_object_mut().unwrap();
        for (key, value) in extra.as_object().unwrap() {
            obj.insert(key.clone(), value.clone());
        }
        parse_config(&Some(config), "pgvector_search processor").unwrap()
    }

    fn build_processor(config: Value) -> Arc<dyn Processor> {
        PgVectorSearchProcessorBuilder
            .build(None, &Some(config), &test_resource())
            .unwrap()
    }

    fn vector_batch(vectors: Vec<Vec<f32>>) -> MessageBatchRef {
        let dim = vectors[0].len() as i32;
        let item_field = Arc::new(Field::new("item", DataType::Float32, true));
        let flat: Vec<f32> = vectors.iter().flatten().copied().collect();
        let list = Arc::new(FixedSizeListArray::new(
            item_field,
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

    #[test]
    fn search_sql_shape_with_payload_and_metrics() {
        let cosine = build_search_sql(&config_with(serde_json::json!({"metric": "cosine"})));
        assert_eq!(
            cosine,
            "SELECT \"id\"::text AS \"id\", \"payload\"::text AS \"payload\", \"embedding\" <=> $1::vector AS \"distance\" FROM \"documents\" ORDER BY \"embedding\" <=> $1::vector LIMIT 5"
        );
        let l2 = build_search_sql(&config_with(serde_json::json!({"metric": "l2", "top_k": 3})));
        assert!(l2.contains("\"embedding\" <-> $1::vector"), "{l2}");
        assert!(l2.ends_with("LIMIT 3"), "{l2}");
        let inner = build_search_sql(&config_with(serde_json::json!({"metric": "inner_product"})));
        assert!(inner.contains("\"embedding\" <#> $1::vector"), "{inner}");
    }

    #[test]
    fn search_sql_without_payload_column() {
        let config = config_with(serde_json::json!({"payload_column": ""}));
        let sql = build_search_sql(&config);
        assert_eq!(
            sql,
            "SELECT \"id\"::text AS \"id\", \"embedding\" <=> $1::vector AS \"distance\" FROM \"documents\" ORDER BY \"embedding\" <=> $1::vector LIMIT 5"
        );
        assert!(!sql.contains("\"payload\""), "{sql}");
    }

    #[test]
    fn fetched_rows_map_to_match_json() {
        let matches = rows_to_matches(
            vec![
                ("42".to_string(), Some(r#"{"text":"a"}"#.to_string()), 0.1),
                ("7".to_string(), None, 0.4),
            ],
            true,
        )
        .unwrap();
        let parsed: Vec<Value> =
            serde_json::from_str(&matches).expect("valid JSON array");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["id"], "42");
        assert_eq!(parsed[0]["distance"], 0.1);
        assert_eq!(parsed[0]["payload"]["text"], "a");
        assert_eq!(parsed[1]["payload"], Value::Null, "null jsonb becomes null payload");
    }

    #[test]
    fn payload_disabled_matches_carry_no_payload_key() {
        let matches = rows_to_matches(
            vec![("42".to_string(), Some(r#"{"text":"a"}"#.to_string()), 0.1)],
            false,
        )
        .unwrap();
        let parsed: Value = serde_json::from_str(&matches).unwrap();
        assert!(parsed[0].get("payload").is_none());
        assert_eq!(parsed[0]["id"], "42");
    }

    #[test]
    fn vector_text_format_roundtrip() {
        assert_eq!(vector_to_pgvector_text(&[1.5, 2.0, -3.0]), "[1.5,2.0,-3.0]");
        assert_eq!(vector_to_pgvector_text(&[0.0]), "[0.0]");
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
    async fn non_list_column_errors() {
        let processor = build_processor(serde_json::json!({
            "url": "postgres://postgres:postgres@localhost:5432/vectors",
            "table": "documents",
        }));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::LargeUtf8,
            true,
        )]));
        let array = Arc::new(LargeStringArray::from(vec!["nope"]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(schema, vec![array]).unwrap(),
        ));
        let err = processor.process(batch).await.unwrap_err().to_string();
        assert!(err.contains("must be FixedSizeList(Float32) or List(Float32)"), "{err}");
    }

    #[tokio::test]
    async fn empty_batch_short_circuits_without_query() {
        let processor = build_processor(serde_json::json!({
            "url": "postgres://postgres:postgres@localhost:5432/vectors",
            "table": "documents",
        }));
        let schema = Arc::new(Schema::new(vec![Field::new("embedding", DataType::Utf8, true)]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(
                schema,
                vec![Arc::new(StringArray::from(Vec::<Option<&str>>::new()))],
            )
            .unwrap(),
        ));
        let result = processor.process(batch).await.unwrap();
        assert!(matches!(result, ProcessResult::None));
    }

    #[test]
    fn invalid_configs_rejected() {
        for bad in [
            serde_json::json!({"table": "t"}),
            serde_json::json!({"url": "postgres://localhost/db"}),
            serde_json::json!({"url": "postgres://localhost/db", "table": "t", "vector_field": ""}),
            serde_json::json!({"url": "postgres://localhost/db", "table": "t", "top_k": 0}),
            serde_json::json!({"url": "postgres://localhost/db", "table": "t", "concurrency": 0}),
        ] {
            assert!(
                PgVectorSearchProcessorBuilder
                    .build(None, &Some(bad), &test_resource())
                    .is_err(),
                "config must be rejected"
            );
        }
    }

    /// Live round-trip against a real Postgres with pgvector. Run with:
    /// `docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres pgvector/pgvector:pg16`
    /// then `cargo test -p arkflow-plugin --lib processor::pgvector_search -- --ignored`
    #[tokio::test]
    #[ignore = "requires a live Postgres with the pgvector extension"]
    async fn live_pgvector_search_returns_nearest_first() {
        let url = "postgres://postgres:postgres@localhost:5432/postgres";
        let pool = sqlx::PgPool::connect(url).await.unwrap();
        sqlx::query("DROP TABLE IF EXISTS arkflow_search_test")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE arkflow_search_test (id BIGINT PRIMARY KEY, embedding vector(2), payload jsonb)",
        )
        .execute(&pool)
        .await
        .unwrap();
        for (id, vector, text) in [
            (1i64, "[1.0,0.0]", "near"),
            (2, "[0.9,0.1]", "also near"),
            (3, "[0.0,1.0]", "far"),
        ] {
            sqlx::query("INSERT INTO arkflow_search_test (id, embedding, payload) VALUES ($1, $2::vector, $3::jsonb)")
                .bind(id)
                .bind(vector)
                .bind(text)
                .execute(&pool)
                .await
                .unwrap();
        }

        let processor = build_processor(serde_json::json!({
            "url": url,
            "table": "arkflow_search_test",
            "top_k": 2,
        }));
        let batch = vector_batch(vec![vec![1.0, 0.0], vec![0.0, 1.0]]);
        let result = processor.process(batch).await.unwrap();
        let ProcessResult::Single(output) = result else {
            panic!("expected single result")
        };
        let matches = output
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let row0: Vec<Value> = serde_json::from_str(matches.value(0)).unwrap();
        assert_eq!(row0.len(), 2);
        assert_eq!(row0[0]["id"], "1", "nearest neighbor first");
        assert_eq!(row0[0]["payload"]["text"], "near");

        let row1: Vec<Value> = serde_json::from_str(matches.value(1)).unwrap();
        assert_eq!(row1[0]["id"], "3", "order follows the input rows");
        assert_eq!(row1[0]["payload"]["text"], "far");

        sqlx::query("DROP TABLE arkflow_search_test")
            .execute(&pool)
            .await
            .unwrap();

        // Failure path: a missing table surfaces the Postgres error.
        let failing = build_processor(serde_json::json!({
            "url": url,
            "table": "arkflow_search_missing_table",
        }));
        let batch = vector_batch(vec![vec![1.0, 0.0]]);
        let err = failing
            .process(batch)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("query failed"), "{err}");
        assert!(
            err.to_lowercase().contains("does not exist"),
            "expected the postgres cause: {err}"
        );
    }
}
