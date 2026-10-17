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

//! Decode already-parsed [`CowValue`] rows into a columnar `RecordBatch`
//! under a pre-computed schema — the value-domain counterpart of feeding
//! re-serialized JSON text through `arrow_json`'s tape decoder.
//!
//! The per-value rules mirror arrow-json 58's tape decoders under the
//! default reader options (`coerce_primitive=false`, `strict_mode=false`,
//! `StructMode::ObjectOnly`, `ignore_type_conflicts=false`):
//! - primitive columns parse numbers (integer first, float fallback with
//!   checked widening — `1.5` truncates to `1`, out-of-range fails), parse
//!   strings that hold numbers, and **error** on booleans/containers;
//! - Utf8 columns accept strings and nulls, and **error** on anything else;
//! - boolean columns error on non-booleans;
//! - Null columns error on any non-null value;
//! - list rows accept arrays (null row → null list) and error on scalars;
//! - struct rows accept objects (missing field → null child, extra field
//!   ignored, null row → null struct) and error on scalars.
//!
//! A missing field is treated exactly like an explicit JSON `null`, matching
//! the tape decoder's null-placeholder fill for absent struct fields.
//!
//! The differential tests in `codec/debezium.rs` pin this decoder (plus the
//! shared inference) to byte-equal output against the text path
//! (`try_to_arrow`) on a corpus covering every rule above.

use std::borrow::Cow;
use std::sync::Arc;

use arkflow_core::Error;
use datafusion::arrow::array::builder::{
    BooleanBuilder, Float64Builder, GenericStringBuilder, Int64Builder, NullBufferBuilder,
};
use datafusion::arrow::array::{ArrayRef, ListArray, NullArray, StructArray};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::arrow::record_batch::RecordBatch;

use super::infer::{CowValue, ObjMap};

/// Decode whole-row objects under `schema`. A field absent from a row
/// decodes as null (tape parity for missing struct fields).
pub(super) fn decode_object_rows(
    rows: &[ObjMap<Cow<'_, str>, CowValue<'_>>],
    schema: &Schema,
) -> Result<RecordBatch, Error> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let values: Vec<Option<&CowValue<'_>>> = rows
                .iter()
                .map(|row| row.get(field.name().as_str()))
                .collect();
            build_array(field.data_type(), &values, field.name())
        })
        .collect::<Result<Vec<ArrayRef>, Error>>()?;

    RecordBatch::try_new(Arc::new(schema.clone()), columns)
        .map_err(|e| Error::Process(format!("Arrow JSON decode failed: {}", e)))
}

/// A slot is either the field's value, an explicit null, or a missing field —
/// the latter two decode identically (tape parity).
type Slot<'a, 'de> = Option<&'a CowValue<'de>>;

fn build_array<'a, 'de>(
    data_type: &DataType,
    values: &[Slot<'a, 'de>],
    path: &str,
) -> Result<ArrayRef, Error> {
    // Errors name the actual column (and nested path), so operators can find
    // the offending field instead of reading a bare type label.
    match data_type {
        DataType::Null => {
            for (row, v) in values.iter().enumerate() {
                if !matches!(v, None | Some(CowValue::Null)) {
                    return Err(Error::Process(format!(
                        "JSON decode failed for column `{}`: expected null, found {:?} at row {row}",
                        path, v
                    )));
                }
            }
            Ok(Arc::new(NullArray::new(values.len())))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for (row, v) in values.iter().enumerate() {
                match v {
                    None | Some(CowValue::Null) => builder.append_null(),
                    Some(CowValue::Bool(b)) => builder.append_value(*b),
                    Some(other) => {
                        return Err(Error::Process(format!(
                            "JSON decode failed for column `{}`: expected boolean, found {:?} at row {row}",
                            path, other
                        )))
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(values.len());
            for (row, v) in values.iter().enumerate() {
                match v {
                    None | Some(CowValue::Null) => builder.append_null(),
                    Some(CowValue::Int(i)) => builder.append_value(*i),
                    Some(CowValue::UInt(u)) => match i64::try_from(*u) {
                        Ok(i) => builder.append_value(i),
                        // Tape parity: the digits parse as f64 and the checked
                        // widening then fails for anything beyond i64's range.
                        Err(_) => {
                            return Err(Error::Process(format!(
                                "JSON decode failed for column `{}`: failed to parse {u} as Int64 at row {row}",
                                path
                            )))
                        }
                    },
                    Some(CowValue::Float(f)) => match f64_to_i64(*f) {
                        Some(i) => builder.append_value(i),
                        None => {
                            return Err(Error::Process(format!(
                                "JSON decode failed for column `{}`: failed to parse {f} as Int64 at row {row}",
                                path
                            )))
                        }
                    },
                    Some(CowValue::Str(s)) => match parse_json_int(s) {
                        Some(i) => builder.append_value(i),
                        None => {
                            return Err(Error::Process(format!(
                                "JSON decode failed for column `{}`: failed to parse \"{s}\" as Int64 at row {row}",
                                path
                            )))
                        }
                    },
                    Some(other) => {
                        return Err(Error::Process(format!(
                            "JSON decode failed for column `{}`: expected primitive, found {:?} at row {row}",
                            path, other
                        )))
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(values.len());
            for (row, v) in values.iter().enumerate() {
                match v {
                    None | Some(CowValue::Null) => builder.append_null(),
                    Some(CowValue::Int(i)) => builder.append_value(*i as f64),
                    Some(CowValue::UInt(u)) => builder.append_value(*u as f64),
                    Some(CowValue::Float(f)) => builder.append_value(*f),
                    Some(CowValue::Str(s)) => match parse_json_float(s) {
                        Some(f) => builder.append_value(f),
                        None => {
                            return Err(Error::Process(format!(
                                "JSON decode failed for column `{}`: failed to parse \"{s}\" as Float64 at row {row}",
                                path
                            )))
                        }
                    },
                    Some(other) => {
                        return Err(Error::Process(format!(
                            "JSON decode failed for column `{}`: expected primitive, found {:?} at row {row}",
                            path, other
                        )))
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = GenericStringBuilder::<i32>::with_capacity(values.len(), 0);
            for (row, v) in values.iter().enumerate() {
                match v {
                    None | Some(CowValue::Null) => builder.append_null(),
                    Some(CowValue::Str(s)) => builder.append_value(s.as_ref()),
                    Some(other) => {
                        return Err(Error::Process(format!(
                            "JSON decode failed for column `{}`: expected string, found {:?} at row {row}",
                            path, other
                        )))
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::List(field) => build_list_array(field, values, path),
        DataType::Struct(fields) => build_struct_array(fields, values, path),
        // JSON inference never produces other types (see `infer.rs`); a
        // hand-built schema reaching here is a programming error upstream.
        other => Err(Error::Process(format!(
            "JSON decode failed for column `{}`: unsupported inferred type {other:?}",
            path
        ))),
    }
}

fn build_list_array<'a, 'de>(
    field: &Field,
    values: &[Slot<'a, 'de>],
    path: &str,
) -> Result<ArrayRef, Error> {
    let mut child_slots: Vec<Slot<'a, 'de>> = Vec::with_capacity(values.len());
    let mut offsets: Vec<i32> = Vec::with_capacity(values.len() + 1);
    offsets.push(0);
    let mut nulls = NullBufferBuilder::new(values.len());

    for (row, v) in values.iter().enumerate() {
        match v {
            None | Some(CowValue::Null) => nulls.append_null(),
            Some(CowValue::Array(items)) => {
                nulls.append_non_null();
                child_slots.extend(items.iter().map(Some));
            }
            Some(other) => {
                return Err(Error::Process(format!(
                    "JSON decode failed for column `{}`: expected list, found {:?} at row {row}",
                    path, other
                )))
            }
        }
        let end = i32::try_from(child_slots.len()).map_err(|_| {
            Error::Process(format!(
                "JSON decode failed for column `{}`: offset overflow decoding ListArray",
                path
            ))
        })?;
        offsets.push(end);
    }

    let child = build_array(field.data_type(), &child_slots, &format!("{path}[]"))?;
    let array = ListArray::new(
        Arc::new(field.clone()),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        child,
        nulls.finish(),
    );
    Ok(Arc::new(array))
}

fn build_struct_array<'a, 'de>(
    fields: &Fields,
    values: &[Slot<'a, 'de>],
    path: &str,
) -> Result<ArrayRef, Error> {
    let mut nulls = NullBufferBuilder::new(values.len());
    let mut row_kinds: Vec<Option<&ObjMap<Cow<'de, str>, CowValue<'de>>>> =
        Vec::with_capacity(values.len());
    for (row, v) in values.iter().enumerate() {
        match v {
            None | Some(CowValue::Null) => {
                nulls.append_null();
                row_kinds.push(None);
            }
            Some(CowValue::Object(map)) => {
                nulls.append_non_null();
                row_kinds.push(Some(map));
            }
            Some(other) => {
                return Err(Error::Process(format!(
                    "JSON decode failed for column `{}`: expected object, found {:?} at row {row}",
                    path, other
                )))
            }
        }
    }

    let children = fields
        .iter()
        .map(|field| {
            let child_slots: Vec<Slot<'a, 'de>> = row_kinds
                .iter()
                .map(|row| row.and_then(|m| m.get(field.name().as_str())))
                .collect();
            build_array(
                field.data_type(),
                &child_slots,
                &format!("{path}.{}", field.name()),
            )
        })
        .collect::<Result<Vec<ArrayRef>, Error>>()?;

    let array = StructArray::try_new(fields.clone(), children, nulls.finish())
        .map_err(|e| Error::Process(format!("JSON decode failed for column `{}`: {}", path, e)))?;
    Ok(Arc::new(array))
}

/// `num-traits`-parity checked f64 → i64 widening (the tape decoder's
/// `NumCast::from` on the float fallback path): truncation toward zero with
/// `None` outside i64's representable range.
fn f64_to_i64(f: f64) -> Option<i64> {
    if f >= i64::MIN as f64 && f < 9_223_372_036_854_775_808.0 {
        Some(f as i64)
    } else {
        None
    }
}

fn parse_json_int(s: &str) -> Option<i64> {
    s.parse::<i64>()
        .ok()
        .or_else(|| parse_json_float(s).and_then(f64_to_i64))
}

fn parse_json_float(s: &str) -> Option<f64> {
    s.parse::<f64>().ok().filter(|f| f.is_finite())
}
