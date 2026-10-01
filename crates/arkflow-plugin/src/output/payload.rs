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

//! Per-row payload selection for `value_field`-configured outputs: when a
//! component names a column, each row's value (binary or string) becomes
//! that row's message body; absent the field the component falls back to
//! its codec encoding path. A null cell is an error, never a skipped row:
//! payloads stay index-aligned with rows because callers resolve per-row
//! topics/keys by the same index — silently dropping a row would shift
//! every later payload onto the previous row's destination.

use arkflow_core::{Error, MessageBatchRef};

fn null_value_field(component: &str, field: &str) -> Error {
    Error::Config(format!(
        "{component} value_field '{field}' contains a null value"
    ))
}

/// Select each row's payload from the named column of `msg`.
pub(crate) fn field_payloads(
    component: &str,
    msg: &MessageBatchRef,
    field: &str,
) -> Result<Vec<Vec<u8>>, Error> {
    use datafusion::arrow::array::{
        Array, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray,
    };
    use datafusion::arrow::datatypes::DataType;

    let column = msg.column_by_name(field).ok_or_else(|| {
        Error::Config(format!(
            "{component} value_field '{field}' not found in the batch"
        ))
    })?;
    match column.data_type() {
        DataType::Binary => Ok(column
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("checked binary column")
            .iter()
            .map(|v| v.map(<[u8]>::to_vec).ok_or_else(|| null_value_field(component, field)))
            .collect::<Result<Vec<_>, _>>()?),
        DataType::LargeBinary => Ok(column
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("checked large binary column")
            .iter()
            .map(|v| v.map(<[u8]>::to_vec).ok_or_else(|| null_value_field(component, field)))
            .collect::<Result<Vec<_>, _>>()?),
        DataType::Utf8 => Ok(column
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("checked utf8 column")
            .iter()
            .map(|v| v.map(|s| s.as_bytes().to_vec()).ok_or_else(|| null_value_field(component, field)))
            .collect::<Result<Vec<_>, _>>()?),
        DataType::LargeUtf8 => Ok(column
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .expect("checked large utf8 column")
            .iter()
            .map(|v| v.map(|s| s.as_bytes().to_vec()).ok_or_else(|| null_value_field(component, field)))
            .collect::<Result<Vec<_>, _>>()?),
        other => Err(Error::Config(format!(
            "{component} value_field '{field}' has unsupported type {other} (use a binary or string column)"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        ArrayRef, BinaryArray, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
    };
    use datafusion::arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn batch_of(field_name: &str, array: ArrayRef) -> MessageBatchRef {
        let field = Field::new(field_name, array.data_type().clone(), true);
        let rb = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap();
        Arc::new(arkflow_core::MessageBatch::new_arrow(rb))
    }

    #[test]
    fn payloads_stay_row_aligned_for_every_supported_type() {
        let cases: Vec<(&str, ArrayRef)> = vec![
            (
                "utf8",
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])) as ArrayRef,
            ),
            (
                "large_utf8",
                Arc::new(LargeStringArray::from(vec![Some("a"), Some("b")])) as ArrayRef,
            ),
            (
                "binary",
                Arc::new(BinaryArray::from_opt_vec(vec![
                    Some(b"a" as &[u8]),
                    Some(b"b"),
                ])) as ArrayRef,
            ),
            (
                "large_binary",
                Arc::new(LargeBinaryArray::from_opt_vec(vec![
                    Some(b"a" as &[u8]),
                    Some(b"b"),
                ])) as ArrayRef,
            ),
        ];
        for (name, array) in cases {
            let payloads = field_payloads("test", &batch_of("col", array.clone()), "col").unwrap();
            assert_eq!(payloads, vec![b"a".to_vec(), b"b".to_vec()], "{name}");
        }
    }

    #[test]
    fn null_cells_error_for_every_supported_type() {
        // A null cell must never shrink the payload list: callers resolve
        // per-row topics/keys by index, so a dropped row would shift every
        // later payload onto the previous row's destination.
        let cases: Vec<(&str, ArrayRef)> = vec![
            (
                "utf8",
                Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef,
            ),
            (
                "large_utf8",
                Arc::new(LargeStringArray::from(vec![Some("a"), None])) as ArrayRef,
            ),
            (
                "binary",
                Arc::new(BinaryArray::from_opt_vec(vec![Some(b"a" as &[u8]), None])) as ArrayRef,
            ),
            (
                "large_binary",
                Arc::new(LargeBinaryArray::from_opt_vec(vec![
                    Some(b"a" as &[u8]),
                    None,
                ])) as ArrayRef,
            ),
        ];
        for (name, array) in cases {
            let err = field_payloads("test", &batch_of("col", array), "col").unwrap_err();
            let msg = format!("{err}");
            assert!(
                msg.contains("null") && msg.contains("col"),
                "{name} must name the null and the column, got: {msg}"
            );
        }
    }

    #[test]
    fn missing_column_and_unsupported_type_error() {
        let batch = batch_of(
            "other",
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![1i64])),
        );
        let err = field_payloads("test", &batch, "col").unwrap_err();
        assert!(format!("{err}").contains("not found"), "{err}");

        let batch = batch_of(
            "n",
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![1i64])),
        );
        let err = field_payloads("test", &batch, "n").unwrap_err();
        assert!(format!("{err}").contains("unsupported type"), "{err}");
    }
}
