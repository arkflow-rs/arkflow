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
//! Debezium CDC codec.
//!
//! Decodes Debezium Envelope JSON (e.g. consumed from Kafka via the Kafka input's
//! codec hook) into a columnar Arrow `MessageBatch`. Each event is flattened:
//! the `after` (or `before` for deletes) fields become top-level columns, and the
//! envelope metadata (`op`, `ts_ms`, `source_db`, `source_table`) plus the full
//! `before`/`source` objects are attached as JSON text columns, so downstream SQL processors
//! can route by `op` and inspect change values directly.
//!
//! CDC offset is NOT managed here: it is the Kafka input's ack-gated offset (see
//! the `input-durability` capability). This codec only parses payloads.

use crate::component;
use arkflow_core::codec::{Codec, CodecBuilder, Decoder, Encoder};
use arkflow_core::component::{register_codec_metadata, ComponentMetadata};
use arkflow_core::{Bytes, Error, MessageBatch, Resource};
use async_trait::async_trait;
use datafusion::arrow;
use serde_json::{Map, Value};
use std::sync::Arc;
use tracing::warn;

/// Debezium Envelope JSON codec.
pub struct DebeziumJsonCodec;

#[async_trait]
impl Encoder for DebeziumJsonCodec {
    async fn encode(&self, batch: MessageBatch) -> Result<Vec<Bytes>, Error> {
        // Encoding is not Debezium-specific; emit the Arrow batch as line-delimited
        // JSON so the codec satisfies the `Codec` contract and stays round-trippable.
        let mut buf = Vec::new();
        let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
        writer
            .write(&batch)
            .map_err(|e| Error::Process(format!("Debezium codec encode error: {}", e)))?;
        writer
            .finish()
            .map_err(|e| Error::Process(format!("Debezium codec encode finish error: {}", e)))?;
        let json_str = String::from_utf8(buf)
            .map_err(|e| Error::Process(format!("Conversion to UTF-8 failed: {}", e)))?;
        Ok(json_str.lines().map(|s| s.as_bytes().to_vec()).collect())
    }
}

#[async_trait]
impl Decoder for DebeziumJsonCodec {
    async fn decode(&self, b: Vec<Bytes>) -> Result<MessageBatch, Error> {
        let mut json_data: Vec<u8> = Vec::new();
        let mut collisions: Vec<String> = Vec::new();
        let mut rows = 0usize;
        for bytes in &b {
            // Zero-length payloads are Debezium tombstones (Kafka null-value
            // messages are usually intercepted by the Kafka input before the
            // codec; this covers every other transport). Skip instead of
            // failing the whole batch.
            if bytes.is_empty() {
                warn!("debezium_json codec: skipping tombstone (empty payload)");
                continue;
            }
            let envelope: Value = serde_json::from_slice(bytes)
                .map_err(|e| Error::Process(format!("Invalid Debezium envelope JSON: {}", e)))?;
            let row = flatten_envelope(envelope, &mut collisions);
            let line = serde_json::to_vec(&row)
                .map_err(|e| Error::Process(format!("Failed to serialize flattened row: {}", e)))?;
            json_data.extend_from_slice(&line);
            json_data.push(b'\n');
            rows += 1;
        }
        if !collisions.is_empty() {
            warn!(
                "debezium_json codec: business fields {:?} collided with envelope metadata \
                 columns; envelope values were moved to `__debezium_<name>` columns",
                collisions
            );
        }
        if rows == 0 {
            // Everything was a tombstone: an empty batch is the honest result.
            return Ok(MessageBatch::new_arrow(
                datafusion::arrow::array::RecordBatch::new_empty(Arc::new(
                    datafusion::arrow::datatypes::Schema::empty(),
                )),
            ));
        }
        let record_batch = component::json::try_to_arrow(&json_data, None)?;
        Ok(MessageBatch::new_arrow(record_batch))
    }
}

/// Insert an envelope metadata column. When the business payload already uses
/// the column name, the business value keeps the column and the envelope value
/// moves to the reserved `__debezium_<name>` name (recorded in `collisions`).
fn insert_meta(
    row: &mut Map<String, Value>,
    name: &str,
    value: Value,
    collisions: &mut Vec<String>,
) {
    if row.contains_key(name) {
        if !collisions.iter().any(|c| c == name) {
            collisions.push(name.to_string());
        }
        row.insert(format!("__debezium_{name}"), value);
    } else {
        row.insert(name.to_string(), value);
    }
}

/// Flatten a Debezium Envelope into a single row object:
/// - business fields from `after` (or `before` when `after` is null, e.g. deletes)
///   are promoted to the top level;
/// - `op`, `ts_ms`, `source_db`, `source_table` are added as top-level columns
///   (on a name collision with a business field the envelope value moves to
///   `__debezium_<name>` and the business value keeps the column);
/// - the full `before` and `source` objects are preserved as JSON text columns.
fn flatten_envelope(envelope: Value, collisions: &mut Vec<String>) -> Value {
    let op = envelope.get("op").cloned().unwrap_or(Value::Null);
    let ts_ms = envelope.get("ts_ms").cloned().unwrap_or(Value::Null);
    let source = envelope.get("source").cloned().unwrap_or(Value::Null);
    let before = envelope.get("before").cloned().unwrap_or(Value::Null);
    let after = envelope.get("after").cloned().unwrap_or(Value::Null);

    // Business payload: prefer `after`; fall back to `before` for deletes / when
    // `after` is null. Clone `before` here so the original is still available below
    // for the preserved `before` column.
    let business = if after.is_object() {
        after
    } else if before.is_object() {
        before.clone()
    } else {
        Value::Null
    };

    let mut row = match business {
        Value::Object(m) => m,
        _ => Map::new(),
    };

    let (source_db, source_table) = match &source {
        Value::Object(s) => (
            s.get("db").cloned().unwrap_or(Value::Null),
            s.get("table").cloned().unwrap_or(Value::Null),
        ),
        _ => (Value::Null, Value::Null),
    };

    // Preserve the full `before`/`source` as JSON text columns. The Arrow JSON
    // reader's single-pass schema inference cannot reconcile a null-vs-object mix
    // within a batch (e.g. `before` is null on inserts but an object on updates),
    // so we serialize them to stable UTF-8 columns; downstream SQL can use JSON
    // functions to inspect them. The most-used source fields (`source_db`,
    // `source_table`) are already promoted to top-level scalar columns above.
    let before_json = serde_json::to_string(&before).unwrap_or_else(|_| "null".to_string());
    let source_json = serde_json::to_string(&source).unwrap_or_else(|_| "null".to_string());
    insert_meta(&mut row, "op", op, collisions);
    insert_meta(&mut row, "ts_ms", ts_ms, collisions);
    insert_meta(&mut row, "source_db", source_db, collisions);
    insert_meta(&mut row, "source_table", source_table, collisions);
    insert_meta(&mut row, "before", Value::String(before_json), collisions);
    insert_meta(&mut row, "source", Value::String(source_json), collisions);

    Value::Object(row)
}

struct DebeziumJsonCodecBuilder;

impl CodecBuilder for DebeziumJsonCodecBuilder {
    fn build(
        &self,
        _name: Option<&str>,
        _config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Codec>, Error> {
        Ok(Arc::new(DebeziumJsonCodec))
    }
}

pub(crate) fn init() -> Result<(), Error> {
    arkflow_core::codec::register_codec_builder(
        "debezium_json",
        Arc::new(DebeziumJsonCodecBuilder),
    )?;
    register_codec_metadata(
        ComponentMetadata::with_schema(
            "debezium_json",
            "Decodes Debezium CDC Envelope JSON (before/after/op/source/ts_ms) into a columnar \
             Arrow batch; attach to a Kafka input consuming a Debezium topic. CDC offset is the \
             Kafka input's ack-gated offset.",
            serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {}
            }),
        )
        .with_optional(),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::AsArray;

    async fn decode_one(json: &str) -> MessageBatch {
        let codec = DebeziumJsonCodec;
        codec.decode(vec![json.as_bytes().to_vec()]).await.unwrap()
    }

    fn str_col<'a>(batch: &'a MessageBatch, name: &str, row: usize) -> &'a str {
        let col = batch
            .record_batch()
            .column_by_name(name)
            .unwrap_or_else(|| panic!("column `{}` must exist", name));
        col.as_string::<i32>().value(row)
    }

    #[tokio::test]
    async fn test_create_event() {
        let batch = decode_one(
            r#"{"before":null,"after":{"id":1,"name":"alice"},"op":"c","ts_ms":1700000000000,"source":{"db":"shop","table":"users"}}"#,
        )
        .await;
        assert_eq!(batch.len(), 1);
        assert_eq!(str_col(&batch, "op", 0), "c");
        assert_eq!(str_col(&batch, "name", 0), "alice");
        assert_eq!(str_col(&batch, "source_db", 0), "shop");
        assert_eq!(str_col(&batch, "source_table", 0), "users");
    }

    #[tokio::test]
    async fn test_update_event_after_wins() {
        let batch = decode_one(
            r#"{"before":{"id":1,"name":"alice"},"after":{"id":1,"name":"ALICE"},"op":"u","ts_ms":1700000000001,"source":{"db":"shop","table":"users"}}"#,
        )
        .await;
        assert_eq!(batch.len(), 1);
        assert_eq!(str_col(&batch, "op", 0), "u");
        assert_eq!(str_col(&batch, "name", 0), "ALICE"); // after takes precedence
    }

    #[tokio::test]
    async fn test_delete_event_uses_before() {
        // after is null -> business fields come from before; must not error.
        let batch = decode_one(
            r#"{"before":{"id":1,"name":"alice"},"after":null,"op":"d","ts_ms":1700000000002,"source":{"db":"shop","table":"users"}}"#,
        )
        .await;
        assert_eq!(batch.len(), 1);
        assert_eq!(str_col(&batch, "op", 0), "d");
        assert_eq!(str_col(&batch, "name", 0), "alice"); // from before
    }

    #[tokio::test]
    async fn test_snapshot_read_event() {
        let batch = decode_one(
            r#"{"before":null,"after":{"id":2,"name":"bob"},"op":"r","ts_ms":1700000000003,"source":{"db":"shop","table":"users"}}"#,
        )
        .await;
        assert_eq!(batch.len(), 1);
        assert_eq!(str_col(&batch, "op", 0), "r");
        assert_eq!(str_col(&batch, "name", 0), "bob");
    }

    #[tokio::test]
    async fn test_missing_source_is_tolerated() {
        // No `source` field -> should not error; op still decoded.
        let batch =
            decode_one(r#"{"before":null,"after":{"id":3,"name":"c"},"op":"c","ts_ms":1}"#).await;
        assert_eq!(batch.len(), 1);
        assert_eq!(str_col(&batch, "op", 0), "c");
    }

    #[tokio::test]
    async fn test_invalid_json_errors() {
        let codec = DebeziumJsonCodec;
        let result = codec.decode(vec![b"not json".to_vec()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_tombstone_in_batch_is_skipped() {
        // One empty payload (tombstone) plus two valid envelopes: only the
        // valid rows are produced and the batch must not fail.
        let codec = DebeziumJsonCodec;
        let data = vec![
            Vec::new(),
            br#"{"before":null,"after":{"id":1,"name":"a"},"op":"c","ts_ms":1,"source":{"db":"s","table":"t"}}"#.to_vec(),
            br#"{"before":null,"after":{"id":2,"name":"b"},"op":"c","ts_ms":2,"source":{"db":"s","table":"t"}}"#.to_vec(),
        ];
        let batch = codec.decode(data).await.unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(str_col(&batch, "op", 0), "c");
    }

    #[tokio::test]
    async fn test_all_tombstones_yield_empty_batch() {
        let codec = DebeziumJsonCodec;
        let batch = codec.decode(vec![Vec::new(), Vec::new()]).await.unwrap();
        assert_eq!(batch.len(), 0);
    }

    #[tokio::test]
    async fn test_json_null_literal_is_not_a_tombstone() {
        // A JSON `null` payload parses to an all-null metadata row (existing
        // behavior); only a zero-length payload is a tombstone.
        let codec = DebeziumJsonCodec;
        let batch = codec.decode(vec![b"null".to_vec()]).await.unwrap();
        assert_eq!(batch.len(), 1);
    }

    #[tokio::test]
    async fn test_business_field_named_op_wins_column() {
        // A business column named `op` keeps its value; the envelope operation
        // moves to the reserved `__debezium_op` column instead of silently
        // overwriting business data.
        let batch = decode_one(
            r#"{"before":null,"after":{"id":1,"op":"custom"},"op":"c","ts_ms":1,"source":{"db":"s","table":"t"}}"#,
        )
        .await;
        assert_eq!(batch.len(), 1);
        assert_eq!(str_col(&batch, "op", 0), "custom"); // business value survives
        assert_eq!(str_col(&batch, "__debezium_op", 0), "c"); // envelope value relocated
    }

    #[tokio::test]
    async fn test_no_collision_keeps_canonical_names() {
        // Without a collision the metadata columns keep their canonical names.
        let batch = decode_one(
            r#"{"before":null,"after":{"id":1,"name":"a"},"op":"c","ts_ms":1,"source":{"db":"s","table":"t"}}"#,
        )
        .await;
        assert!(batch
            .record_batch()
            .column_by_name("__debezium_op")
            .is_none());
        assert_eq!(str_col(&batch, "op", 0), "c");
    }

    #[tokio::test]
    async fn test_multiple_events_in_one_batch() {
        let codec = DebeziumJsonCodec;
        let data = vec![
            br#"{"before":null,"after":{"id":1,"name":"a"},"op":"c","ts_ms":1,"source":{"db":"s","table":"t"}}"#.to_vec(),
            br#"{"before":{"id":1,"name":"a"},"after":{"id":2,"name":"b"},"op":"u","ts_ms":2,"source":{"db":"s","table":"t"}}"#.to_vec(),
        ];
        let batch = codec.decode(data).await.unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(str_col(&batch, "op", 0), "c");
        assert_eq!(str_col(&batch, "op", 1), "u");
        assert_eq!(str_col(&batch, "name", 1), "b"); // after wins on update
    }

    #[tokio::test]
    async fn test_before_and_source_are_json_text() {
        let batch = decode_one(
            r#"{"before":{"id":1,"name":"alice"},"after":{"id":1,"name":"ALICE"},"op":"u","ts_ms":1,"source":{"db":"shop","table":"users"}}"#,
        )
        .await;
        let before = str_col(&batch, "before", 0);
        assert!(before.contains("\"name\":\"alice\"")); // full before preserved as JSON text
        let source = str_col(&batch, "source", 0);
        assert!(source.contains("\"db\":\"shop\""));
    }

    #[tokio::test]
    async fn test_ts_ms_extracted() {
        use datafusion::arrow::datatypes::Int64Type;
        let batch = decode_one(
            r#"{"before":null,"after":{"id":1},"op":"c","ts_ms":1700000000000,"source":{"db":"s","table":"t"}}"#,
        )
        .await;
        let ts = batch
            .record_batch()
            .column_by_name("ts_ms")
            .expect("ts_ms column");
        assert_eq!(ts.as_primitive::<Int64Type>().value(0), 1700000000000);
    }

    #[tokio::test]
    async fn test_builder() {
        let builder = DebeziumJsonCodecBuilder;
        let resource = Resource {
            temporary: std::collections::HashMap::new(),
            input_names: std::cell::RefCell::new(Vec::new()),
        };
        let result = builder.build(None, &None, &resource);
        assert!(result.is_ok());
    }
}
