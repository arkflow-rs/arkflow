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
//! Each payload is parsed exactly once into a borrowed [`component::json::CowValue`]
//! tree; flattening, schema inference and columnar construction all operate on
//! that tree in place (no intermediate NDJSON round-trip). CDC offset is NOT
//! managed here: it is the Kafka input's ack-gated offset (see
//! the `input-durability` capability). This codec only parses payloads.

use crate::component;
use arkflow_core::codec::{Codec, CodecBuilder, Decoder, Encoder};
use arkflow_core::component::{register_codec_metadata, ComponentMetadata};
use arkflow_core::{Bytes, Error, MessageBatch, Resource};
use async_trait::async_trait;
use component::json::{CowValue, RecordObject};
use datafusion::arrow;
use serde_json::to_string;
use std::borrow::Cow;
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
        decode_payloads(&b)
    }
}

/// Synchronous decode core (the trait method only adapts it to `async`):
/// parse each payload once, flatten in place, infer + build columnar.
fn decode_payloads(b: &[Bytes]) -> Result<MessageBatch, Error> {
    let mut rows: Vec<RecordObject<'_>> = Vec::new();
    let mut collisions: Vec<String> = Vec::new();
    for bytes in b {
        // Zero-length payloads are Debezium tombstones (Kafka null-value
        // messages are usually intercepted by the Kafka input before the
        // codec; this covers every other transport). Skip instead of
        // failing the whole batch.
        if bytes.is_empty() {
            warn!("debezium_json codec: skipping tombstone (empty payload)");
            continue;
        }
        // Single parse: the borrowed tree feeds flattening, inference and
        // decoding directly, so the batch is never re-serialized to JSON
        // text (the text path would parse every payload a second time).
        let envelope: CowValue = serde_json::from_slice(bytes)
            .map_err(|e| Error::Process(format!("Invalid Debezium envelope JSON: {}", e)))?;
        rows.push(flatten_envelope(envelope, &mut collisions));
    }
    if !collisions.is_empty() {
        warn!(
            "debezium_json codec: business fields {:?} collided with envelope metadata \
             columns; envelope values were moved to `__debezium_<name>` columns",
            collisions
        );
    }
    if rows.is_empty() {
        // Everything was a tombstone: an empty batch is the honest result.
        return Ok(MessageBatch::new_arrow(
            datafusion::arrow::array::RecordBatch::new_empty(Arc::new(
                datafusion::arrow::datatypes::Schema::empty(),
            )),
        ));
    }
    let record_batch = component::json::values_to_arrow(&rows)?;
    Ok(MessageBatch::new_arrow(record_batch))
}

/// Insert an envelope metadata column. When the business payload already uses
/// the column name, the business value keeps the column and the envelope value
/// moves to the reserved `__debezium_<name>` name (recorded in `collisions`).
///
/// `name` is always one of six literals, so the canonical key borrows from the
/// binary (`'static`) instead of allocating per row; only the rare collision
/// key (`__debezium_<name>`) allocates.
fn insert_meta<'de>(
    row: &mut RecordObject<'de>,
    name: &'static str,
    value: CowValue<'de>,
    collisions: &mut Vec<String>,
) {
    if row.contains_key(name) {
        if !collisions.iter().any(|c| c == name) {
            collisions.push(name.to_string());
        }
        row.insert(Cow::Owned(format!("__debezium_{name}")), value);
    } else {
        row.insert(Cow::Borrowed(name), value);
    }
}

/// Flatten a Debezium Envelope into a single row object:
/// - business fields from `after` (or `before` when `after` is null, e.g. deletes)
///   are promoted to the top level;
/// - `op`, `ts_ms`, `source_db`, `source_table` are added as top-level columns
///   (on a name collision with a business field the envelope value moves to
///   `__debezium_<name>` and the business value keeps the column);
/// - the full `before` and `source` objects are preserved as JSON text columns.
///
/// The envelope is consumed: `before` is serialized to its text column first
/// and then *moved* into the business row on deletes, so a delete event never
/// deep-copies its payload. Non-object envelopes (JSON `null` literals,
/// arrays, scalars) yield an all-null metadata row, as before.
fn flatten_envelope<'de>(
    envelope: CowValue<'de>,
    collisions: &mut Vec<String>,
) -> RecordObject<'de> {
    let mut env = match envelope {
        CowValue::Object(map) => map,
        // Non-object envelope: every envelope field reads as absent, which is
        // the all-null metadata row below.
        _ => return empty_meta_row(collisions),
    };

    // Remove (move) the envelope fields; removal order does not matter because
    // the row is rebuilt from the business object, not from `env`.
    let op = env.shift_remove("op").unwrap_or(CowValue::Null);
    let ts_ms = env.shift_remove("ts_ms").unwrap_or(CowValue::Null);
    let source = env.shift_remove("source").unwrap_or(CowValue::Null);
    let before = env.shift_remove("before").unwrap_or(CowValue::Null);
    let after = env.shift_remove("after").unwrap_or(CowValue::Null);

    // Preserve the full `before`/`source` as JSON text columns. The single-pass
    // schema inference cannot reconcile a null-vs-object mix within a batch
    // (e.g. `before` is null on inserts but an object on updates), so we keep
    // them as stable UTF-8 columns; downstream SQL can use JSON functions to
    // inspect them. The most-used source fields (`source_db`, `source_table`)
    // are promoted to top-level scalar columns below.
    // Serialize BEFORE moving: on deletes `before` becomes the business row,
    // and this way one parse serves both consumers without a deep copy.
    let before_json = to_string(&before).unwrap_or_else(|_| "null".to_string());
    let source_json = to_string(&source).unwrap_or_else(|_| "null".to_string());
    // `source` is already serialized above and never lands in the row itself,
    // so consume it: the promoted scalars keep their borrowed strings instead
    // of being deep-copied per envelope.
    let (source_db, source_table) = match source {
        CowValue::Object(mut s) => (
            s.shift_remove("db").unwrap_or(CowValue::Null),
            s.shift_remove("table").unwrap_or(CowValue::Null),
        ),
        _ => (CowValue::Null, CowValue::Null),
    };

    // Business payload: prefer `after`; fall back to `before` for deletes /
    // when `after` is null. Either object is moved into the row (after its
    // text-column serialization above, so a delete's `before` is consumed,
    // not copied).
    let mut row = match after {
        CowValue::Object(map) => map,
        _ => match before {
            CowValue::Object(map) => map,
            _ => RecordObject::default(),
        },
    };

    insert_meta(&mut row, "op", op, collisions);
    insert_meta(&mut row, "ts_ms", ts_ms, collisions);
    insert_meta(&mut row, "source_db", source_db, collisions);
    insert_meta(&mut row, "source_table", source_table, collisions);
    insert_meta(
        &mut row,
        "before",
        CowValue::Str(Cow::Owned(before_json)),
        collisions,
    );
    insert_meta(
        &mut row,
        "source",
        CowValue::Str(Cow::Owned(source_json)),
        collisions,
    );

    row
}

/// The row produced by a non-object envelope (or one with no fields): only
/// the six metadata columns, all null, with `"null"` JSON text columns.
fn empty_meta_row<'de>(collisions: &mut Vec<String>) -> RecordObject<'de> {
    let mut row = RecordObject::default();
    insert_meta(&mut row, "op", CowValue::Null, collisions);
    insert_meta(&mut row, "ts_ms", CowValue::Null, collisions);
    insert_meta(&mut row, "source_db", CowValue::Null, collisions);
    insert_meta(&mut row, "source_table", CowValue::Null, collisions);
    insert_meta(
        &mut row,
        "before",
        CowValue::Str(Cow::Owned("null".to_string())),
        collisions,
    );
    insert_meta(
        &mut row,
        "source",
        CowValue::Str(Cow::Owned("null".to_string())),
        collisions,
    );
    row
}

struct DebeziumJsonCodecBuilder;

impl CodecBuilder for DebeziumJsonCodecBuilder {
    fn build(
        &self,
        _name: Option<&str>,
        _config: &Option<serde_json::Value>,
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

    // ---- Differential equivalence: value-domain path vs. the historical
    // text path (flatten to serde_json::Value → serialize to NDJSON →
    // `try_to_arrow`). The oracle below is the previous implementation,
    // kept verbatim. Every corpus case must produce byte-identical schema
    // (fields, types, order) and rendered values — or fail on BOTH paths.

    mod differential {
        use super::*;
        use arkflow_core::MessageBatch;

        // ORACLE: the historical text-domain decode path.
        fn flatten_envelope_oracle(
            envelope: serde_json::Value,
            collisions: &mut Vec<String>,
        ) -> serde_json::Value {
            fn insert_meta_oracle(
                row: &mut serde_json::Map<String, serde_json::Value>,
                name: &str,
                value: serde_json::Value,
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

            let op = envelope
                .get("op")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let ts_ms = envelope
                .get("ts_ms")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let source = envelope
                .get("source")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let before = envelope
                .get("before")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let after = envelope
                .get("after")
                .cloned()
                .unwrap_or(serde_json::Value::Null);

            let business = if after.is_object() {
                after
            } else if before.is_object() {
                before.clone()
            } else {
                serde_json::Value::Null
            };

            let mut row = match business {
                serde_json::Value::Object(m) => m,
                _ => serde_json::Map::new(),
            };

            let (source_db, source_table) = match &source {
                serde_json::Value::Object(s) => (
                    s.get("db").cloned().unwrap_or(serde_json::Value::Null),
                    s.get("table").cloned().unwrap_or(serde_json::Value::Null),
                ),
                _ => (serde_json::Value::Null, serde_json::Value::Null),
            };

            let before_json = serde_json::to_string(&before).unwrap_or_else(|_| "null".to_string());
            let source_json = serde_json::to_string(&source).unwrap_or_else(|_| "null".to_string());
            insert_meta_oracle(&mut row, "op", op, collisions);
            insert_meta_oracle(&mut row, "ts_ms", ts_ms, collisions);
            insert_meta_oracle(&mut row, "source_db", source_db, collisions);
            insert_meta_oracle(&mut row, "source_table", source_table, collisions);
            insert_meta_oracle(
                &mut row,
                "before",
                serde_json::Value::String(before_json),
                collisions,
            );
            insert_meta_oracle(
                &mut row,
                "source",
                serde_json::Value::String(source_json),
                collisions,
            );

            serde_json::Value::Object(row)
        }

        pub(super) fn decode_oracle(b: &[Bytes]) -> Result<MessageBatch, Error> {
            let mut json_data: Vec<u8> = Vec::new();
            let mut collisions: Vec<String> = Vec::new();
            let mut rows = 0usize;
            for bytes in b {
                if bytes.is_empty() {
                    continue;
                }
                let envelope: serde_json::Value = serde_json::from_slice(bytes).map_err(|e| {
                    Error::Process(format!("Invalid Debezium envelope JSON: {}", e))
                })?;
                let row = flatten_envelope_oracle(envelope, &mut collisions);
                let line = serde_json::to_vec(&row)
                    .map_err(|e| Error::Process(format!("serialize: {e}")))?;
                json_data.extend_from_slice(&line);
                json_data.push(b'\n');
                rows += 1;
            }
            if rows == 0 {
                return Ok(MessageBatch::new_arrow(
                    datafusion::arrow::array::RecordBatch::new_empty(Arc::new(
                        datafusion::arrow::datatypes::Schema::empty(),
                    )),
                ));
            }
            component::json::try_to_arrow(&json_data, None).map(MessageBatch::new_arrow)
        }

        fn render(batch: &MessageBatch) -> String {
            let mut buf = Vec::new();
            let mut writer = datafusion::arrow::json::LineDelimitedWriter::new(&mut buf);
            writer.write(batch).expect("render");
            writer.finish().expect("render finish");
            String::from_utf8(buf).expect("utf8")
        }

        async fn assert_differential(label: &str, payloads: Vec<Bytes>) {
            let new = DebeziumJsonCodec.decode(payloads.clone()).await;
            let oracle = decode_oracle(&payloads);
            match (new, oracle) {
                (Ok(a), Ok(b)) => {
                    assert_eq!(a.len(), b.len(), "{label}: row count");
                    assert_eq!(
                        a.record_batch().schema(),
                        b.record_batch().schema(),
                        "{label}: schema (fields, types, order) mismatch\nnew:\n{}\noracle:\n{}",
                        render(&a),
                        render(&b)
                    );
                    assert_eq!(render(&a), render(&b), "{label}: values mismatch");
                }
                (Err(_), Err(_)) => {
                    // Failure-set parity: both paths reject the batch.
                }
                (new, oracle) => {
                    panic!("{label}: failure-set mismatch — new={new:?} oracle={oracle:?}")
                }
            }
        }

        fn envelope(body: &str) -> Bytes {
            body.as_bytes().to_vec()
        }

        #[tokio::test]
        async fn corpus_scalar_widen_and_order() {
            assert_differential(
                "scalar widen + first-seen order",
                vec![
                    envelope(
                        r#"{"before":null,"after":{"b":1,"a":"x"},"op":"c","ts_ms":1,"source":{"db":"d","table":"t"}}"#,
                    ),
                    envelope(
                        r#"{"before":null,"after":{"c":2.5,"a":"y"},"op":"u","ts_ms":2,"source":{"db":"d","table":"t"}}"#,
                    ),
                    envelope(
                        r#"{"before":null,"after":{"b":3,"a":"z"},"op":"d","ts_ms":3,"source":{"db":"d","table":"t"}}"#,
                    ),
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn corpus_null_only_column_and_missing_fields() {
            assert_differential(
                "null-only column + missing fields",
                vec![
                    envelope(r#"{"after":{"v":null,"n":1},"op":"c","ts_ms":1}"#),
                    envelope(r#"{"after":{"v":null},"op":"c","ts_ms":2}"#),
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn corpus_nested_objects_and_arrays() {
            assert_differential(
                "nested struct + list corners",
                vec![
                    envelope(r#"{"after":{"o":{"a":1,"m":{"z":true}},"l":[1,2],"s":[{"x":"p"}],"nl":[[1,2],[3]]},"op":"r","ts_ms":1}"#),
                    envelope(r#"{"after":{"o":{"b":2},"l":[3],"s":[{"y":1,"x":"q"}],"nl":[]},"op":"r","ts_ms":2}"#),
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn corpus_scalar_into_list_and_mixed_list_failing() {
            // Scalar where another row had a list → coerced List column; the
            // scalar row must fail on BOTH paths.
            assert_differential(
                "scalar into List column fails",
                vec![
                    envelope(r#"{"after":{"v":[1]},"op":"c","ts_ms":1}"#),
                    envelope(r#"{"after":{"v":5},"op":"c","ts_ms":2}"#),
                ],
            )
            .await;
            // Mixed element types inside a list → List(Utf8); the int element
            // must fail on BOTH paths.
            assert_differential(
                "mixed list element types fail",
                vec![
                    envelope(r#"{"after":{"v":[1]},"op":"c","ts_ms":1}"#),
                    envelope(r#"{"after":{"v":["a"]},"op":"c","ts_ms":2}"#),
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn corpus_bool_and_string_mix_failing() {
            // Bool ⊕ Utf8 coerces to Utf8; the bool row must fail on BOTH paths.
            assert_differential(
                "bool into Utf8 column fails",
                vec![
                    envelope(r#"{"after":{"v":"text"},"op":"c","ts_ms":1}"#),
                    envelope(r#"{"after":{"v":true},"op":"c","ts_ms":2}"#),
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn corpus_u64_widens_to_float() {
            assert_differential(
                "u64 beyond i64::MAX widens to Float64",
                vec![
                    envelope(r#"{"after":{"v":1},"op":"c","ts_ms":1}"#),
                    envelope(r#"{"after":{"v":18446744073709551615},"op":"c","ts_ms":2}"#),
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn corpus_float_int_widen_values_exact() {
            assert_differential(
                "int+float widen exact values",
                vec![
                    envelope(r#"{"after":{"v":1},"op":"c","ts_ms":1}"#),
                    envelope(r#"{"after":{"v":2.5},"op":"c","ts_ms":2}"#),
                    envelope(r#"{"after":{"v":-7},"op":"c","ts_ms":3}"#),
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn corpus_collision_names() {
            assert_differential(
                "all six meta collisions",
                vec![envelope(
                    r#"{"before":{"before":"b1","source":"s1","op":"o1","ts_ms":99,"source_db":"db1","source_table":"t1","id":1},"after":{"before":"b2","source":"s2","op":"o2","ts_ms":98,"source_db":"db2","source_table":"t2","id":2},"op":"u","ts_ms":7,"source":{"db":"d","table":"t"}}"#,
                )],
            )
            .await;
        }

        #[tokio::test]
        async fn corpus_non_object_envelopes() {
            for payload in [
                r#"null"#, r#"[1,2]"#, r#""str""#, r#"123"#, r#"true"#, r#"{}"#,
            ] {
                assert_differential(
                    &format!("non-object envelope {payload}"),
                    vec![envelope(payload)],
                )
                .await;
            }
        }

        #[tokio::test]
        async fn corpus_tombstones_and_empty() {
            assert_differential(
                "tombstone mixed",
                vec![
                    Bytes::new(),
                    envelope(r#"{"after":{"v":1},"op":"c","ts_ms":1}"#),
                ],
            )
            .await;
            assert_differential("all tombstones", vec![Bytes::new(), Bytes::new()]).await;
            assert_differential("empty batch", vec![]).await;
        }

        #[tokio::test]
        async fn corpus_json_text_columns_byte_parity() {
            // Escapes, unicode, key ordering inside before/source text columns.
            assert_differential(
                "text column parity",
                vec![envelope(
                    r#"{"before":{"name":"al\"ice","日":"本","z":1,"a":null},"after":{"name":"ALICE"},"op":"u","ts_ms":1,"source":{"db":"d","table":"t","snapshot":"true","lsn":{"num":42}}}"#,
                )],
            )
            .await;
        }

        #[tokio::test]
        async fn corpus_source_db_non_string() {
            assert_differential(
                "numeric source.db keeps its type",
                vec![envelope(
                    r#"{"after":{"v":1},"op":"c","ts_ms":1,"source":{"db":42,"table":null}}"#,
                )],
            )
            .await;
        }

        #[tokio::test]
        async fn corpus_deletes_business_from_before() {
            assert_differential(
                "delete uses before as business row",
                vec![envelope(
                    r#"{"before":{"id":1,"name":"alice"},"after":null,"op":"d","ts_ms":1,"source":{"db":"d","table":"t"}}"#,
                )],
            )
            .await;
        }

        #[tokio::test]
        async fn corpus_invalid_json_fails_both() {
            let new = DebeziumJsonCodec.decode(vec![b"not json".to_vec()]).await;
            let oracle = decode_oracle(&[b"not json".to_vec()]);
            assert!(new.is_err() && oracle.is_err());
        }
    }

    /// Ad-hoc release timing (NOT run by CI): value-domain single-parse path
    /// vs the historical text path (re-serialize flattened rows to NDJSON →
    /// `try_to_arrow`), kept inline as the oracle. Run with:
    /// `cargo test --release -p arkflow-plugin --lib -- --ignored debezium_decode_timing --nocapture`
    #[test]
    #[ignore]
    fn debezium_decode_timing() {
        use differential::decode_oracle;

        let batch_rows = 1_000usize;
        let rounds = 200usize; // 200k envelopes per measurement

        let payloads: Vec<Vec<Bytes>> = {
            let mut batches = Vec::new();
            for b in 0..rounds {
                let mut batch = Vec::with_capacity(batch_rows);
                for i in 0..batch_rows {
                    let id = (b * batch_rows + i) as i64;
                    batch.push(Bytes::from(format!(
                        r#"{{"before":null,"after":{{"id":{id},"name":"user_{id}","amount":{id}.25,"active":true,"tags":["a","b"]}},"op":"c","ts_ms":17000000000,"source":{{"db":"shop","table":"users","lsn":{{"num":{id}}},"snapshot":"false"}}}}"#,
                    )));
                }
                batches.push(batch);
            }
            batches
        };

        let time = |name: &str, f: &mut dyn FnMut() -> usize| {
            let mut best = std::time::Duration::MAX;
            let mut rows = 0;
            for _ in 0..3 {
                let start = std::time::Instant::now();
                rows = f();
                best = best.min(start.elapsed());
            }
            println!(
                "{name}: {rows} rows in {best:?} ({:.0} rows/s)",
                rows as f64 / best.as_secs_f64()
            );
        };

        let mut new_path = || {
            let mut rows = 0usize;
            for batch in &payloads {
                rows += decode_payloads(batch).expect("new").len();
            }
            rows
        };
        time("value-domain single parse", &mut new_path);

        let mut oracle_path = || {
            let mut rows = 0usize;
            for batch in &payloads {
                rows += decode_oracle(batch).expect("oracle").len();
            }
            rows
        };
        time("text path (serialize + re-parse)", &mut oracle_path);
    }
}
