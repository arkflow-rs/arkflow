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
use std::io::Cursor;
use std::sync::Arc;

pub(crate) fn try_to_arrow(
    content: &[u8],
    fields_to_include: Option<&HashSet<String>>,
) -> Result<RecordBatch, Error> {
    let mut cursor_for_inference = Cursor::new(content);
    let (mut inferred_schema, _) =
        arrow_json::reader::infer_json_schema(&mut cursor_for_inference, Some(1))
            .map_err(|e| Error::Process(format!("Schema inference error: {}", e)))?;
    if let Some(ref set) = fields_to_include {
        inferred_schema = inferred_schema
            .project(
                &set.iter()
                    .filter_map(|name| inferred_schema.index_of(name).ok())
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| Error::Process(format!("Arrow JSON Projection Error: {}", e)))?;
    }

    let inferred_schema = Arc::new(inferred_schema);
    let reader = ReaderBuilder::new(inferred_schema.clone())
        .build(Cursor::new(content))
        .map_err(|e| Error::Process(format!("Arrow JSON Reader Builder Error: {}", e)))?;

    let result = reader
        .map(|batch| {
            Ok(batch.map_err(|e| Error::Process(format!("Arrow JSON Reader Error: {}", e)))?)
        })
        .collect::<Result<Vec<RecordBatch>, Error>>()?;
    if result.is_empty() {
        return Ok(RecordBatch::new_empty(inferred_schema));
    }

    let new_batch = arrow::compute::concat_batches(&inferred_schema, &result)
        .map_err(|e| Error::Process(format!("Merge batches failed: {}", e)))?;

    Ok(new_batch)
}
