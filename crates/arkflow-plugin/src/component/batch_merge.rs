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

//! Schema-union merge for record batches coming from different sources
//! (schema-registry multi-version decoding, window buffers fanning in
//! heterogeneous inputs, isolated codec decoding). Batches are projected
//! onto the field-name union schema — missing columns are null-filled —
//! and then concatenated. A column name carrying two different types is
//! an error rather than a guessed cast.

use arkflow_core::Error;
use datafusion::arrow::array::{new_null_array, ArrayRef, RecordBatch};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{Field, Schema};
use std::collections::HashMap;
use std::sync::Arc;

/// Project each batch onto the union schema (first-seen column order,
/// missing columns null-filled) and concatenate.
pub(crate) fn normalize_and_concat(batches: &[RecordBatch]) -> Result<RecordBatch, Error> {
    if batches.is_empty() {
        return Err(Error::Process(
            "batch merge: no batches to merge".to_string(),
        ));
    }
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }

    let mut fields: Vec<Field> = Vec::new();
    let mut positions: HashMap<String, usize> = HashMap::new();
    for batch in batches {
        for field in batch.schema().fields() {
            match positions.get(field.name()) {
                None => {
                    positions.insert(field.name().to_string(), fields.len());
                    fields.push(field.as_ref().clone().with_nullable(true));
                }
                Some(&i) => {
                    let existing = &fields[i];
                    if existing.data_type() != field.data_type() {
                        return Err(Error::Process(format!(
                            "batch merge: column `{}` has conflicting types: {} vs {}",
                            field.name(),
                            existing.data_type(),
                            field.data_type()
                        )));
                    }
                    // A column nullable in some batches must stay nullable in the union.
                    if !existing.is_nullable() {
                        fields[i] = existing.clone().with_nullable(true);
                    }
                }
            }
        }
    }

    let schema = Arc::new(Schema::new(fields));
    let mut projected: Vec<RecordBatch> = Vec::with_capacity(batches.len());
    for batch in batches {
        projected.push(project_to_schema(batch, &schema)?);
    }
    concat_batches(&schema, &projected).map_err(|e| Error::Process(format!("batch merge: {}", e)))
}

/// Rebuild `batch` with exactly `schema`'s columns: existing columns pass
/// through (type equality was already checked), absent columns become
/// null arrays.
fn project_to_schema(batch: &RecordBatch, schema: &Schema) -> Result<RecordBatch, Error> {
    if batch.schema().as_ref() == schema {
        return Ok(batch.clone());
    }
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        match batch.schema().index_of(field.name()) {
            Ok(i) => columns.push(batch.column(i).clone()),
            Err(_) => columns.push(new_null_array(field.data_type(), batch.num_rows())),
        }
    }
    RecordBatch::try_new(Arc::new(schema.clone()), columns)
        .map_err(|e| Error::Process(format!("batch merge: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::DataType;

    fn batch(schema_fields: Vec<Field>, columns: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(schema_fields)), columns).unwrap()
    }

    fn utf8(name: &str, vals: Vec<Option<&str>>) -> (Field, ArrayRef) {
        (
            Field::new(name, DataType::Utf8, true),
            Arc::new(StringArray::from(vals)),
        )
    }

    fn i64(name: &str, vals: Vec<Option<i64>>) -> (Field, ArrayRef) {
        (
            Field::new(name, DataType::Int64, true),
            Arc::new(Int64Array::from(vals)),
        )
    }

    #[test]
    fn same_schema_passes_through_concat() {
        let (fa, ca) = utf8("id", vec![Some("a")]);
        let (fb, cb) = utf8("v", vec![Some("1")]);
        let b1 = batch(vec![fa, fb], vec![ca, cb]);
        let (fa2, ca2) = utf8("id", vec![Some("b")]);
        let (fb2, cb2) = utf8("v", vec![None]);
        let b2 = batch(vec![fa2, fb2], vec![ca2, cb2]);

        let merged = normalize_and_concat(&[b1, b2]).unwrap();
        assert_eq!(merged.num_rows(), 2);
        assert_eq!(merged.num_columns(), 2);
    }

    #[test]
    fn missing_column_is_null_filled() {
        let (fa, ca) = utf8("id", vec![Some("a")]);
        let (fb, cb) = i64("extra", vec![Some(1)]);
        let b1 = batch(vec![fa, fb], vec![ca, cb]);
        let (fa2, ca2) = utf8("id", vec![Some("b")]);
        let b2 = batch(vec![fa2], vec![ca2]);

        let merged = normalize_and_concat(&[b1, b2]).unwrap();
        assert_eq!(merged.num_columns(), 2);
        let extra = merged
            .column(merged.schema().index_of("extra").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(extra.value(0), 1);
        assert!(extra.is_null(1));
        // the union column must be nullable even though batch 1 declared it non-null
        assert!(merged
            .schema()
            .field(merged.schema().index_of("extra").unwrap())
            .is_nullable());
    }

    #[test]
    fn conflicting_types_error_names_column_and_types() {
        let (fa, ca) = utf8("id", vec![Some("a")]);
        let b1 = batch(vec![fa], vec![ca]);
        let (fb, cb) = i64("id", vec![Some(1)]);
        let b2 = batch(vec![fb], vec![cb]);

        let err = normalize_and_concat(&[b1, b2]).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("`id`"),
            "message should name the column: {msg}"
        );
        assert!(
            msg.contains("Utf8") && msg.contains("Int64"),
            "message should name both types: {msg}"
        );
    }

    #[test]
    fn empty_input_errors() {
        assert!(normalize_and_concat(&[]).is_err());
    }

    #[test]
    fn single_batch_returns_clone() {
        let (fa, ca) = utf8("id", vec![Some("a")]);
        let b1 = batch(vec![fa], vec![ca]);
        let merged = normalize_and_concat(std::slice::from_ref(&b1)).unwrap();
        assert_eq!(merged, b1);
    }
}
