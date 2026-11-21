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
use arkflow_core::Error;
use arrow_json::ReaderBuilder;
use datafusion::arrow;
use datafusion::arrow::record_batch::RecordBatch;
use once_cell::sync::Lazy;
use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

mod infer;
mod values;

pub(crate) use infer::CowValue;

/// A parsed record object in the value domain (see [`infer`]'s `CowValue`).
pub(crate) type RecordObject<'de> = infer::ObjMap<Cow<'de, str>, CowValue<'de>>;

/// Process-level cache of inferred schemas keyed by the EXACT input bytes
/// plus the projection set. Schema inference is a deterministic function of
/// the content, so an exact-byte hit cannot produce a different schema — the
/// hit path skips the whole inference pass (the extra whole-buffer parse the
/// decode pass doesn't need). Variable payloads miss and pay the same
/// inference as before plus one linear hash of the content (cheap against
/// the parse — see `schema_cache_key`), so this is a pure win for
/// fixed-template workloads (templated IoT/heartbeat payloads, replayed
/// fixtures) and near-neutral elsewhere. Bounded FIFO of small entries; `DefaultHasher::new()` is
/// deterministic within a process, which is all a process-local cache needs.
/// A hash collision would reuse a wrong schema for one batch — astronomically
/// unlikely at 64 bits and bounded cache size, and it degrades to a bad
/// decode, never to unsound memory.
static SCHEMA_CACHE: Lazy<Mutex<SchemaCache>> = Lazy::new(|| Mutex::new(SchemaCache::default()));

const SCHEMA_CACHE_CAPACITY: usize = 128;

#[derive(Default)]
struct SchemaCache {
    entries: HashMap<u64, Arc<arrow::datatypes::Schema>>,
    order: VecDeque<u64>,
}

fn schema_cache_key(content: &[u8], fields_to_include: Option<&HashSet<String>>) -> u64 {
    let mut hasher = DefaultHasher::new();
    content.hash(&mut hasher);
    match fields_to_include {
        None => 0u8.hash(&mut hasher),
        Some(set) => {
            1u8.hash(&mut hasher);
            let mut names: Vec<&String> = set.iter().collect();
            names.sort();
            for name in names {
                name.hash(&mut hasher);
            }
        }
    }
    hasher.finish()
}

pub(crate) fn try_to_arrow(
    content: &[u8],
    fields_to_include: Option<&HashSet<String>>,
) -> Result<RecordBatch, Error> {
    let cache_key = schema_cache_key(content, fields_to_include);
    // Clone the schema Arc out of the lock scope: decode_with_schema is the
    // expensive part, and this is a process-global mutex — holding it across
    // a hit's parse/allocation would serialize concurrent decodes (json and
    // Debezium alike) even when they use different cache keys.
    let cached_schema = {
        let cache = SCHEMA_CACHE.lock().unwrap();
        cache.entries.get(&cache_key).cloned()
    };
    if let Some(schema) = cached_schema {
        return decode_with_schema(content, schema);
    }

    // Infer the schema from ALL records in the batch. The streaming inferrer
    // produces the byte-equal result of arrow-json's full-record
    // `infer_json_schema` (union across records: Int64+Float64 widen to
    // Float64, later-seen fields become columns, nulls relax nullability)
    // without materializing a per-record serde_json Value tree — see
    // `infer`'s differential tests.
    let mut inferred_schema = infer::infer_json_schema_streaming(content)?;
    if let Some(set) = fields_to_include {
        inferred_schema = inferred_schema
            .project(
                &set.iter()
                    .filter_map(|name| inferred_schema.index_of(name).ok())
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| Error::Process(format!("Arrow JSON Projection Error: {}", e)))?;
    }

    let inferred_schema = Arc::new(inferred_schema);
    {
        let mut cache = SCHEMA_CACHE.lock().unwrap();
        if !cache.entries.contains_key(&cache_key) {
            if cache.entries.len() >= SCHEMA_CACHE_CAPACITY {
                if let Some(oldest) = cache.order.pop_front() {
                    cache.entries.remove(&oldest);
                }
            }
            cache.order.push_back(cache_key);
            cache.entries.insert(cache_key, inferred_schema.clone());
        }
    }
    decode_with_schema(content, inferred_schema)
}

/// Infer a schema across already-parsed record objects and decode them into a
/// single `RecordBatch` — the value-domain counterpart of `try_to_arrow` for
/// callers that parsed their payloads exactly once (debezium envelopes).
/// Schema rules are shared with the byte-domain path (same merge machinery);
/// value conversion mirrors arrow-json's default tape decoder (see `values`).
pub(crate) fn values_to_arrow(rows: &[RecordObject<'_>]) -> Result<RecordBatch, Error> {
    let schema = infer::infer_schema_from_objects(rows)?;
    values::decode_object_rows(rows, &schema)
}

/// Decode newline-delimited JSON records into a single `RecordBatch` under a
/// pre-computed schema.
///
/// The decoder's internal row threshold is sized to the input (one record per
/// line for NDJSON, so newline count is an exact upper bound — the inference
/// pass rejects anything packing several records per line), letting one
/// `Decoder::flush` at EOF yield the entire batch — the chunked `Reader` +
/// `concat_batches` path (internal 1024-row flushes, one full column copy) is
/// gone. EOF handling matches `arrow_json::Reader::read()` exactly, so a final
/// record without a trailing newline still settles. Inputs with more rows than
/// the capacity cap below (the decoder pre-allocates tape proportional to
/// rows × schema fields) — or records packed denser than one per line when
/// this helper is fed a pre-built schema directly — trip the threshold; those
/// fall back to flushing the blocked chunks and concatenating (output is still
/// one batch).
fn decode_with_schema(
    content: &[u8],
    schema: Arc<arrow::datatypes::Schema>,
) -> Result<RecordBatch, Error> {
    // Cap the row estimate: the decoder pre-allocates tape capacity
    // proportional to rows × schema fields, so an uncapped newline-count
    // estimate on a huge body (e.g. a large HTTP input POST) would commit
    // gigabytes upfront. Estimates beyond the cap simply make the decoder
    // block mid-buffer and fall back to chunked flush + concat below.
    const MAX_ESTIMATED_ROWS: usize = 65_536;
    let estimated_rows =
        (content.iter().filter(|&&b| b == b'\n').count() + 1).min(MAX_ESTIMATED_ROWS);
    let mut decoder = ReaderBuilder::new(schema.clone())
        .with_batch_size(estimated_rows)
        .build_decoder()
        .map_err(|e| Error::Process(format!("Arrow JSON Reader Builder Error: {}", e)))?;

    let mut rest = content;
    let mut chunks: Vec<RecordBatch> = Vec::new();
    while !rest.is_empty() {
        match decoder
            .decode(rest)
            .map_err(|e| Error::Process(format!("Arrow JSON Reader Error: {}", e)))?
        {
            0 => {
                // Row threshold hit with bytes remaining: flush to unblock.
                match decoder
                    .flush()
                    .map_err(|e| Error::Process(format!("Arrow JSON Reader Error: {}", e)))?
                {
                    Some(batch) => chunks.push(batch),
                    None => {
                        return Err(Error::Process(
                            "Arrow JSON decode stalled: consumed 0 bytes with an empty decoder"
                                .to_string(),
                        ))
                    }
                }
            }
            consumed => rest = &rest[consumed..],
        }
    }

    let final_chunk = decoder
        .flush()
        .map_err(|e| Error::Process(format!("Arrow JSON Reader Error: {}", e)))?;
    match (chunks.is_empty(), final_chunk) {
        (true, None) => Ok(RecordBatch::new_empty(schema)),
        (true, Some(batch)) => Ok(batch),
        (false, trailing) => {
            if let Some(batch) = trailing {
                chunks.push(batch);
            }
            arrow::compute::concat_batches(&schema, &chunks)
                .map_err(|e| Error::Process(format!("Merge batches failed: {}", e)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::AsArray;

    fn ndjson(rows: usize) -> Vec<u8> {
        let mut content = Vec::new();
        for i in 0..rows {
            content.extend_from_slice(format!(r#"{{"v":{i},"tag":"t{i}"}}"#).as_bytes());
            content.push(b'\n');
        }
        content
    }

    /// The whole input must come out as ONE batch even when it spans several
    /// internal decoder chunks (default batch size is 1024).
    #[test]
    fn decodes_multi_chunk_input_as_a_single_batch() {
        for rows in [1usize, 1023, 1024, 1025, 5000] {
            let batch = try_to_arrow(&ndjson(rows), None).expect("decode");
            assert_eq!(batch.num_rows(), rows, "row count at {rows}");
            let v = batch
                .column_by_name("v")
                .unwrap()
                .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
            assert_eq!(v.value(rows - 1), (rows - 1) as i64);
            let tag = batch.column_by_name("tag").unwrap().as_string::<i32>();
            assert_eq!(tag.value(rows - 1), format!("t{}", rows - 1));
        }
    }

    /// Codec paths join payloads with `\n` — no trailing newline. The final
    /// record must settle, not surface as a truncated-record error.
    #[test]
    fn decodes_input_without_trailing_newline() {
        let content = br#"{"v":1}
{"v":2}"#;
        let batch = try_to_arrow(content, None).expect("decode");
        assert_eq!(batch.num_rows(), 2);
    }

    /// Empty (or whitespace-only) input yields an empty batch with the
    /// inferred (empty) schema — unchanged from the concat-based path.
    #[test]
    fn empty_input_yields_empty_batch() {
        for content in [&b""[..], &b" \n \n"[..]] {
            let batch = try_to_arrow(content, None).expect("decode");
            assert_eq!(batch.num_rows(), 0);
            assert_eq!(batch.schema().fields().len(), 0);
        }
    }

    /// The chunked fallback: records packed denser than the newline-count
    /// row estimate (here: no newlines at all, estimate = 1) trip the decoder
    /// threshold mid-buffer; the flushed chunks concatenate in order into one
    /// batch. Fed directly through `decode_with_schema` — inference rejects
    /// such lines, so `try_to_arrow` can only reach this path for inputs
    /// exceeding the row-estimate cap.
    #[test]
    fn packed_records_fall_back_to_chunked_concat() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, true),
        ]));
        let content = br#"{"v":1}{"v":2}{"v":3}"#;
        let batch = decode_with_schema(content, schema).expect("decode");
        assert_eq!(batch.num_rows(), 3);
        let v = batch
            .column_by_name("v")
            .unwrap()
            .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
        assert_eq!(v.value(0), 1);
        assert_eq!(v.value(1), 2);
        assert_eq!(v.value(2), 3);
    }

    /// Ad-hoc release timing (NOT run by CI): the new pipeline (streaming
    /// inference + sized-decoder single flush) vs the historical path
    /// (`infer_json_schema` Value-tree inference + chunked Reader +
    /// `concat_batches`), kept inline as the oracle. Runs a narrow 4-column
    /// workload and a wide 200-column workload (wide records stress the
    /// per-field keyed lookups). Run with:
    /// `cargo test --release -p arkflow-plugin --lib -- --ignored json_decode_timing --nocapture`
    #[test]
    #[ignore]
    fn json_decode_timing() {
        let total = 200_000usize;
        let batch_rows = 1_000usize;

        let narrow = {
            let mut content = Vec::new();
            for i in 0..batch_rows {
                content.extend_from_slice(
                    format!(
                        r#"{{"value":{i},"sensor":"sensor_{:03}","active":true,"ratio":0.5}}"#,
                        i % 100
                    )
                    .as_bytes(),
                );
                content.push(b'\n');
            }
            content
        };
        let wide = {
            let fields: Vec<String> = (0..200).map(|f| format!(r#""f{f:03}":{f}"#)).collect();
            let row = format!("{{{}}}", fields.join(","));
            let mut content = Vec::new();
            for _ in 0..batch_rows {
                content.extend_from_slice(row.as_bytes());
                content.push(b'\n');
            }
            content
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
                "{name}: {} rows in {:?} ({:.0} rows/s)",
                rows,
                best,
                rows as f64 / best.as_secs_f64()
            );
        };

        for (label, content) in [("4-column", &narrow), ("200-column wide", &wide)] {
            let mut new_path = || {
                let mut rows = 0;
                for _ in 0..total / batch_rows {
                    rows += try_to_arrow(content, None).expect("new path").num_rows();
                }
                rows
            };
            time(&format!("new pipeline ({label})"), &mut new_path);

            let mut oracle_path = || {
                let mut rows = 0;
                for _ in 0..total / batch_rows {
                    let mut cursor = std::io::Cursor::new(content);
                    let (schema, _) = arrow_json::reader::infer_json_schema(&mut cursor, None)
                        .expect("oracle inference");
                    let schema = Arc::new(schema);
                    let reader = arrow_json::ReaderBuilder::new(schema.clone())
                        .build(std::io::Cursor::new(content))
                        .expect("oracle reader");
                    let chunks = reader.map(|b| b.expect("oracle chunk")).collect::<Vec<_>>();
                    let batch = if chunks.is_empty() {
                        RecordBatch::new_empty(schema)
                    } else {
                        arrow::compute::concat_batches(&schema, &chunks).expect("oracle concat")
                    };
                    rows += batch.num_rows();
                }
                rows
            };
            time(&format!("old path ({label})"), &mut oracle_path);
        }
    }
}

#[cfg(test)]
mod schema_cache_tests {
    use super::*;
    use std::collections::HashSet;

    fn sample_ndjson(rows: usize) -> Vec<u8> {
        let mut content = Vec::new();
        for i in 0..rows {
            content.extend_from_slice(format!(r#"{{"v":{i},"tag":"t{i}"}}"#).as_bytes());
            content.push(b'\n');
        }
        content
    }

    /// Same bytes decode identically on the cache-hit path and the inference
    /// path: the hit cannot produce a different schema (determinism of
    /// inference over identical input).
    #[test]
    fn cache_hit_path_is_byte_identical_to_inference_path() {
        let content = sample_ndjson(50);
        let first = try_to_arrow(&content, None).expect("cold decode");
        let second = try_to_arrow(&content, None).expect("warm decode");
        assert!(
            first.schema() == second.schema(),
            "schemas match through the cache"
        );
        assert_eq!(first.num_columns(), second.num_columns());
        for (a, b) in first.columns().iter().zip(second.columns()) {
            assert_eq!(a, b, "column data must be identical through the cache");
        }
    }

    /// Different content infers its own schema even under the same cache: an
    /// added field appears (exact-byte keys cannot cross-contaminate).
    #[test]
    fn different_content_infers_its_own_schema() {
        let base = sample_ndjson(10);
        // Same first 9 records; the 10th carries an extra field.
        let mut extended = sample_ndjson(9);
        extended.extend_from_slice(br#"{"v":9,"tag":"t9","extra":1}"#);
        extended.push(b'\n');

        let first = try_to_arrow(&base, None).expect("base decode");
        let second = try_to_arrow(&extended, None).expect("extended decode");
        assert!(first.column_by_name("extra").is_none());
        assert!(
            second.column_by_name("extra").is_some(),
            "new field inferred"
        );

        // Re-decoding the base must still yield the base schema (no bleed).
        let again = try_to_arrow(&base, None).expect("base decode again");
        assert!(again.column_by_name("extra").is_none());
    }

    /// The projection set participates in the cache key: the same content
    /// with and without `fields_to_include` must not share an entry.
    #[test]
    fn projection_set_is_part_of_the_cache_key() {
        let content = sample_ndjson(5);
        let full = try_to_arrow(&content, None).expect("full decode");
        let mut set = HashSet::new();
        set.insert("v".to_string());
        let projected = try_to_arrow(&content, Some(&set)).expect("projected decode");
        assert_eq!(full.num_columns(), 2, "v + tag");
        assert_eq!(projected.num_columns(), 1, "projection took effect");
        assert!(projected.column_by_name("v").is_some());
        assert!(projected.column_by_name("tag").is_none());

        // And the full-shape entry survived alongside the projected one.
        let full_again = try_to_arrow(&content, None).expect("full decode again");
        assert_eq!(full_again.num_columns(), 2);
    }
}
