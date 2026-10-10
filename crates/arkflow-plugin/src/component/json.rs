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
use std::collections::HashSet;
use std::sync::Arc;

mod infer;

pub(crate) fn try_to_arrow(
    content: &[u8],
    fields_to_include: Option<&HashSet<String>>,
) -> Result<RecordBatch, Error> {
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
    decode_with_schema(content, inferred_schema)
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
