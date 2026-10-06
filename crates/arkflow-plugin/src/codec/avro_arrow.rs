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
//! Avro value → Arrow conversion for the `schema_registry` codec.
//!
//! Mirrors `protobuf_to_arrow`'s flat convention: the top-level record fields
//! become the columns of a single-row batch. Nested records, arrays, maps and
//! unions other than `[null, T]` are rejected rather than silently flattened.

use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::types::Value as AvroValue;
use apache_avro::Schema as AvroSchema;
use arkflow_core::Error;
use datafusion::arrow::array::{
    Array, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Date32Array, Decimal128Array,
    Decimal128Builder, Float32Array, Float64Array, Int32Array, Int64Array, PrimitiveBuilder,
    StringArray, StringBuilder, Time32MillisecondArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Field, Float32Type, Float64Type, Int32Type, Int64Type, Schema,
    Time32MillisecondType, Time64MicrosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType,
};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use std::sync::Arc;

const UTC: &str = "UTC";

/// Decodes one Avro binary datum against the writer `schema` and converts it
/// to a single-row Arrow batch.
pub fn avro_to_arrow(schema: &AvroSchema, payload: &[u8]) -> Result<RecordBatch, Error> {
    let reader = GenericDatumReader::builder(schema)
        .build()
        .map_err(|e| Error::Process(format!("Avro reader build failed: {}", e)))?;
    let value = reader
        .read_value(&mut std::io::Cursor::new(payload))
        .map_err(|e| Error::Process(format!("Avro decode failed: {}", e)))?;
    avro_value_to_arrow(schema, &value)
}

/// Converts a decoded Avro record value into a single-row Arrow batch.
pub fn avro_value_to_arrow(schema: &AvroSchema, value: &AvroValue) -> Result<RecordBatch, Error> {
    let (record_schema, values) = match (schema, value) {
        (AvroSchema::Record(r), AvroValue::Record(values)) => (r, values),
        _ => {
            return Err(Error::Process(
                "Avro payload must decode to a record to be mapped to Arrow columns".to_string(),
            ));
        }
    };
    if record_schema.fields.len() != values.len() {
        return Err(Error::Process(format!(
            "Avro record field count mismatch: schema has {} fields, value has {}",
            record_schema.fields.len(),
            values.len()
        )));
    }
    let mut arrow_fields = Vec::with_capacity(values.len());
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(values.len());
    for (field, (value_name, value)) in record_schema.fields.iter().zip(values.iter()) {
        if field.name != *value_name {
            return Err(Error::Process(format!(
                "Avro record field order mismatch: schema field '{}' vs value field '{}'",
                field.name, value_name
            )));
        }
        let (f, arr) = field_to_arrow(&field.name, &field.schema, value)?;
        arrow_fields.push(f);
        columns.push(arr);
    }
    let arrow_schema = Arc::new(Schema::new(arrow_fields));
    RecordBatch::try_new(arrow_schema, columns)
        .map_err(|e| Error::Process(format!("Creating an Arrow record batch failed: {}", e)))
}

/// Converts one record field to an Arrow column. `[null, T]` unions unwrap
/// into a nullable column of `T`.
fn field_to_arrow(
    name: &str,
    schema: &AvroSchema,
    value: &AvroValue,
) -> Result<(Arc<Field>, Arc<dyn Array>), Error> {
    let (schema, value, nullable) = match (schema, value) {
        (AvroSchema::Union(u), AvroValue::Union(idx, inner)) => {
            let variants = u.variants();
            if variants.len() != 2 || !variants.iter().any(|v| matches!(v, AvroSchema::Null)) {
                return Err(Error::Process(format!(
                    "Unsupported Avro union for field '{}': only [null, T] unions are supported (found {} branches)",
                    name,
                    variants.len()
                )));
            }
            if matches!(inner.as_ref(), AvroValue::Null) {
                // A null entry still belongs to a nullable column of T.
                let typed = variants
                    .iter()
                    .find(|v| !matches!(v, AvroSchema::Null))
                    .expect("checked above: union contains a non-null branch");
                let (dt, arr) = null_column(typed, name)?;
                return Ok((Arc::new(Field::new(name, dt, true)), arr));
            }
            (&variants[*idx as usize], inner.as_ref(), true)
        }
        (AvroSchema::Union(_), _) => {
            return Err(Error::Process(format!(
                "Avro field '{}' does not hold a union value but its schema is a union",
                name
            )));
        }
        (s, v) => (s, v, false),
    };

    if matches!(value, AvroValue::Null) {
        return Err(Error::Process(format!(
            "Avro field '{}' is null but its schema is not nullable",
            name
        )));
    }

    leaf_to_arrow(name, schema, value, nullable)
}

/// A single null entry typed by the resolved schema (for nullable unions).
fn null_column(schema: &AvroSchema, name: &str) -> Result<(DataType, Arc<dyn Array>), Error> {
    let (dt, arr): (DataType, Arc<dyn Array>) = match schema {
        AvroSchema::Boolean => (DataType::Boolean, Arc::new(BooleanArray::from(vec![None]))),
        AvroSchema::Int => (DataType::Int32, Arc::new(Int32Array::from(vec![None]))),
        AvroSchema::Long => (DataType::Int64, Arc::new(Int64Array::from(vec![None]))),
        AvroSchema::Float => (DataType::Float32, Arc::new(Float32Array::from(vec![None]))),
        AvroSchema::Double => (DataType::Float64, Arc::new(Float64Array::from(vec![None]))),
        AvroSchema::Bytes | AvroSchema::Fixed(_) => (
            DataType::Binary,
            Arc::new(BinaryArray::from(vec![None::<&[u8]>])),
        ),
        AvroSchema::String | AvroSchema::Enum(_) | AvroSchema::Uuid(_) => (
            DataType::Utf8,
            Arc::new(StringArray::from(vec![None::<String>])),
        ),
        AvroSchema::Date => (DataType::Date32, Arc::new(Date32Array::from(vec![None]))),
        AvroSchema::TimeMillis => (
            DataType::Time32(TimeUnit::Millisecond),
            Arc::new(Time32MillisecondArray::from(vec![None])),
        ),
        AvroSchema::TimeMicros => (
            DataType::Time64(TimeUnit::Microsecond),
            Arc::new(Time64MicrosecondArray::from(vec![None])),
        ),
        AvroSchema::TimestampMillis => (
            DataType::Timestamp(TimeUnit::Millisecond, Some(UTC.into())),
            Arc::new(TimestampMillisecondArray::from(vec![None]).with_timezone(Arc::from(UTC))),
        ),
        AvroSchema::TimestampMicros => (
            DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
            Arc::new(TimestampMicrosecondArray::from(vec![None]).with_timezone(Arc::from(UTC))),
        ),
        AvroSchema::LocalTimestampMillis => (
            DataType::Timestamp(TimeUnit::Millisecond, None),
            // Local timestamps carry no timezone; stamping one would make the
            // array type diverge from the declared column type.
            Arc::new(TimestampMillisecondArray::from(vec![None])),
        ),
        AvroSchema::LocalTimestampMicros => (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            Arc::new(TimestampMicrosecondArray::from(vec![None])),
        ),
        AvroSchema::Decimal(d) => {
            let (precision, scale) = decimal_metadata(d, name)?;
            let arr = Decimal128Array::from(vec![None::<i128>])
                .with_precision_and_scale(precision, scale)
                .map_err(|e| {
                    Error::Process(format!(
                        "Avro decimal for field '{}' cannot set precision/scale: {}",
                        name, e
                    ))
                })?;
            (DataType::Decimal128(precision, scale), Arc::new(arr))
        }
        other => {
            return Err(Error::Process(format!(
                "Unsupported Avro type for field '{}': {:?}",
                name, other
            )));
        }
    };
    Ok((dt, arr))
}

/// Maps a non-null leaf value to a typed single-entry Arrow array.
fn leaf_to_arrow(
    name: &str,
    schema: &AvroSchema,
    value: &AvroValue,
    nullable: bool,
) -> Result<(Arc<Field>, Arc<dyn Array>), Error> {
    let (dt, arr): (DataType, Arc<dyn Array>) = match (schema, value) {
        (AvroSchema::Boolean, AvroValue::Boolean(v)) => (
            DataType::Boolean,
            Arc::new(BooleanArray::from(vec![Some(*v)])),
        ),
        (AvroSchema::Int, AvroValue::Int(v)) => {
            (DataType::Int32, Arc::new(Int32Array::from(vec![Some(*v)])))
        }
        (AvroSchema::Long, AvroValue::Long(v)) => {
            (DataType::Int64, Arc::new(Int64Array::from(vec![Some(*v)])))
        }
        (AvroSchema::Float, AvroValue::Float(v)) => (
            DataType::Float32,
            Arc::new(Float32Array::from(vec![Some(*v)])),
        ),
        (AvroSchema::Double, AvroValue::Double(v)) => (
            DataType::Float64,
            Arc::new(Float64Array::from(vec![Some(*v)])),
        ),
        (AvroSchema::Bytes, AvroValue::Bytes(v)) => (
            DataType::Binary,
            Arc::new(BinaryArray::from(vec![Some(v.as_slice())])),
        ),
        (AvroSchema::Fixed(_), AvroValue::Fixed(_, v)) => (
            DataType::Binary,
            Arc::new(BinaryArray::from(vec![Some(v.as_slice())])),
        ),
        (AvroSchema::String, AvroValue::String(v)) => (
            DataType::Utf8,
            Arc::new(StringArray::from(vec![Some(v.as_str())])),
        ),
        (AvroSchema::Enum(_), AvroValue::Enum(_, symbol)) => (
            DataType::Utf8,
            Arc::new(StringArray::from(vec![Some(symbol.as_str())])),
        ),
        (AvroSchema::Uuid(_), AvroValue::Uuid(v)) => (
            DataType::Utf8,
            Arc::new(StringArray::from(vec![Some(v.to_string())])),
        ),
        (AvroSchema::Uuid(_), AvroValue::String(v)) => (
            DataType::Utf8,
            Arc::new(StringArray::from(vec![Some(v.as_str())])),
        ),
        (AvroSchema::Date, AvroValue::Date(v)) => (
            DataType::Date32,
            Arc::new(Date32Array::from(vec![Some(*v)])),
        ),
        (AvroSchema::TimeMillis, AvroValue::TimeMillis(v)) => (
            DataType::Time32(TimeUnit::Millisecond),
            Arc::new(Time32MillisecondArray::from(vec![Some(*v)])),
        ),
        (AvroSchema::TimeMicros, AvroValue::TimeMicros(v)) => (
            DataType::Time64(TimeUnit::Microsecond),
            Arc::new(Time64MicrosecondArray::from(vec![Some(*v)])),
        ),
        (AvroSchema::TimestampMillis, AvroValue::TimestampMillis(v)) => (
            DataType::Timestamp(TimeUnit::Millisecond, Some(UTC.into())),
            Arc::new(TimestampMillisecondArray::from(vec![Some(*v)]).with_timezone(Arc::from(UTC))),
        ),
        (AvroSchema::TimestampMicros, AvroValue::TimestampMicros(v)) => (
            DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(*v)]).with_timezone(Arc::from(UTC))),
        ),
        (AvroSchema::LocalTimestampMillis, AvroValue::LocalTimestampMillis(v)) => (
            DataType::Timestamp(TimeUnit::Millisecond, None),
            // Local timestamps carry no timezone (mirrors null_column).
            Arc::new(TimestampMillisecondArray::from(vec![Some(*v)])),
        ),
        (AvroSchema::LocalTimestampMicros, AvroValue::LocalTimestampMicros(v)) => (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(*v)])),
        ),
        (AvroSchema::Decimal(d), AvroValue::Decimal(dec)) => {
            let (precision, scale) = decimal_metadata(d, name)?;
            let bytes = <Vec<u8>>::try_from(dec).map_err(|e| {
                Error::Process(format!(
                    "Avro decimal conversion failed for field '{}': {}",
                    name, e
                ))
            })?;
            let unscaled = decode_decimal_i128(&bytes).ok_or_else(|| {
                Error::Process(format!(
                    "Avro decimal for field '{}' exceeds decimal128 range",
                    name
                ))
            })?;
            let arr = Decimal128Array::from(vec![Some(unscaled)])
                .with_precision_and_scale(precision, scale)
                .map_err(|e| {
                    Error::Process(format!(
                        "Avro decimal for field '{}' cannot set precision/scale: {}",
                        name, e
                    ))
                })?;
            (DataType::Decimal128(precision, scale), Arc::new(arr))
        }
        (AvroSchema::Record(_) | AvroSchema::Array(_) | AvroSchema::Map(_), _) => {
            return Err(Error::Process(format!(
                "Unsupported nested Avro type for field '{}': nested records/arrays/maps are not supported by the flat Arrow mapping",
                name
            )));
        }
        (s, v) => {
            return Err(Error::Process(format!(
                "Unsupported Avro type for field '{}': schema {:?} with value {:?}",
                name, s, v
            )));
        }
    };
    Ok((Arc::new(Field::new(name, dt, nullable)), arr))
}

fn decimal_metadata(d: &apache_avro::schema::DecimalSchema, name: &str) -> Result<(u8, i8), Error> {
    let precision = d.precision;
    let scale = d.scale;
    if precision == 0 || precision > 38 {
        return Err(Error::Process(format!(
            "Avro decimal for field '{}' has precision {} out of decimal128 range (1..=38)",
            name, precision
        )));
    }
    if scale > precision {
        return Err(Error::Process(format!(
            "Avro decimal for field '{}' has scale {} greater than precision {}",
            name, scale, precision
        )));
    }
    Ok((precision as u8, scale as i8))
}

/// Interprets minimal two's-complement big-endian bytes as i128.
fn decode_decimal_i128(bytes: &[u8]) -> Option<i128> {
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }
    let sign = if bytes[0] & 0x80 != 0 { 0xFF } else { 0x00 };
    let mut be = [sign; 16];
    be[16 - bytes.len()..].copy_from_slice(bytes);
    Some(i128::from_be_bytes(be))
}

// ===== Columnar batch conversion (one shared writer schema) =====

/// The Arrow leaf type of a column, derived once per batch from the writer
/// schema (never from message values).
enum LeafKind {
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
    Binary,
    Utf8,
    Date32,
    Time32Millisecond,
    Time64Microsecond,
    TimestampMillisecondUtc,
    TimestampMillisecondLocal,
    TimestampMicrosecondUtc,
    TimestampMicrosecondLocal,
    Decimal128 { precision: u8, scale: i8 },
}

/// One column's schema-derived mapping, teed out of the first message's
/// field walk so later messages skip schema-level checks entirely.
struct ColumnPlan<'a> {
    name: &'a str,
    nullable: bool,
    kind: LeafKind,
}

/// Typed Arrow builder per column; grows by append and `finish`es into the
/// final array, replacing the per-message single-element array allocation.
enum ColumnBuilder {
    Boolean(BooleanBuilder),
    Int32(PrimitiveBuilder<Int32Type>),
    Int64(PrimitiveBuilder<Int64Type>),
    Float32(PrimitiveBuilder<Float32Type>),
    Float64(PrimitiveBuilder<Float64Type>),
    Binary(BinaryBuilder),
    Utf8(StringBuilder),
    Date32(PrimitiveBuilder<Date32Type>),
    Time32Millisecond(PrimitiveBuilder<Time32MillisecondType>),
    Time64Microsecond(PrimitiveBuilder<Time64MicrosecondType>),
    TimestampMillisecond(PrimitiveBuilder<TimestampMillisecondType>),
    TimestampMicrosecond(PrimitiveBuilder<TimestampMicrosecondType>),
    Decimal128(Decimal128Builder),
}

impl ColumnBuilder {
    fn append_null(&mut self) {
        match self {
            ColumnBuilder::Boolean(b) => b.append_null(),
            ColumnBuilder::Int32(b) => b.append_null(),
            ColumnBuilder::Int64(b) => b.append_null(),
            ColumnBuilder::Float32(b) => b.append_null(),
            ColumnBuilder::Float64(b) => b.append_null(),
            ColumnBuilder::Binary(b) => b.append_null(),
            ColumnBuilder::Utf8(b) => b.append_null(),
            ColumnBuilder::Date32(b) => b.append_null(),
            ColumnBuilder::Time32Millisecond(b) => b.append_null(),
            ColumnBuilder::Time64Microsecond(b) => b.append_null(),
            ColumnBuilder::TimestampMillisecond(b) => b.append_null(),
            ColumnBuilder::TimestampMicrosecond(b) => b.append_null(),
            ColumnBuilder::Decimal128(b) => b.append_null(),
        }
    }
}

fn kind_data_type(kind: &LeafKind) -> DataType {
    match kind {
        LeafKind::Boolean => DataType::Boolean,
        LeafKind::Int32 => DataType::Int32,
        LeafKind::Int64 => DataType::Int64,
        LeafKind::Float32 => DataType::Float32,
        LeafKind::Float64 => DataType::Float64,
        LeafKind::Binary => DataType::Binary,
        LeafKind::Utf8 => DataType::Utf8,
        LeafKind::Date32 => DataType::Date32,
        LeafKind::Time32Millisecond => DataType::Time32(TimeUnit::Millisecond),
        LeafKind::Time64Microsecond => DataType::Time64(TimeUnit::Microsecond),
        LeafKind::TimestampMillisecondUtc => {
            DataType::Timestamp(TimeUnit::Millisecond, Some(UTC.into()))
        }
        LeafKind::TimestampMillisecondLocal => DataType::Timestamp(TimeUnit::Millisecond, None),
        LeafKind::TimestampMicrosecondUtc => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into()))
        }
        LeafKind::TimestampMicrosecondLocal => DataType::Timestamp(TimeUnit::Microsecond, None),
        LeafKind::Decimal128 { precision, scale } => DataType::Decimal128(*precision, *scale),
    }
}

fn builder_kind_mismatch(name: &str) -> Error {
    Error::Process(format!(
        "internal error: column builder kind mismatch for field '{}'",
        name
    ))
}

/// Mirrors `leaf_to_arrow`'s per-value decimal validation (arrow rejects
/// values that do not fit the declared precision) so the batch path fails on
/// the same message a single-row conversion would.
fn validate_decimal_value(
    name: &str,
    unscaled: i128,
    precision: u8,
    scale: i8,
) -> Result<(), Error> {
    Decimal128Array::from(vec![Some(unscaled)])
        .with_precision_and_scale(precision, scale)
        .map(|_| ())
        .map_err(|e| {
            Error::Process(format!(
                "Avro decimal for field '{}' cannot set precision/scale: {}",
                name, e
            ))
        })
}

/// First-message path for one field: mirrors `leaf_to_arrow` arm by arm —
/// same checks, same order, same error texts — while teeing the mapping into
/// a `ColumnPlan` and appending into a fresh builder instead of allocating a
/// single-element array.
fn leaf_plan_and_append<'a>(
    name: &'a str,
    schema: &AvroSchema,
    value: &AvroValue,
    nullable: bool,
    plans: &mut Vec<ColumnPlan<'a>>,
    builders: &mut Vec<ColumnBuilder>,
) -> Result<(), Error> {
    match (schema, value) {
        (AvroSchema::Boolean, AvroValue::Boolean(v)) => {
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::Boolean,
            });
            let mut b = BooleanBuilder::new();
            b.append_value(*v);
            builders.push(ColumnBuilder::Boolean(b));
        }
        (AvroSchema::Int, AvroValue::Int(v)) => {
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::Int32,
            });
            let mut b = PrimitiveBuilder::<Int32Type>::new();
            b.append_value(*v);
            builders.push(ColumnBuilder::Int32(b));
        }
        (AvroSchema::Long, AvroValue::Long(v)) => {
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::Int64,
            });
            let mut b = PrimitiveBuilder::<Int64Type>::new();
            b.append_value(*v);
            builders.push(ColumnBuilder::Int64(b));
        }
        (AvroSchema::Float, AvroValue::Float(v)) => {
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::Float32,
            });
            let mut b = PrimitiveBuilder::<Float32Type>::new();
            b.append_value(*v);
            builders.push(ColumnBuilder::Float32(b));
        }
        (AvroSchema::Double, AvroValue::Double(v)) => {
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::Float64,
            });
            let mut b = PrimitiveBuilder::<Float64Type>::new();
            b.append_value(*v);
            builders.push(ColumnBuilder::Float64(b));
        }
        (AvroSchema::Bytes, AvroValue::Bytes(v)) => {
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::Binary,
            });
            let mut b = BinaryBuilder::new();
            b.append_value(v.as_slice());
            builders.push(ColumnBuilder::Binary(b));
        }
        (AvroSchema::Fixed(_), AvroValue::Fixed(_, v)) => {
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::Binary,
            });
            let mut b = BinaryBuilder::new();
            b.append_value(v.as_slice());
            builders.push(ColumnBuilder::Binary(b));
        }
        (AvroSchema::String, AvroValue::String(v)) => {
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::Utf8,
            });
            let mut b = StringBuilder::new();
            b.append_value(v.as_str());
            builders.push(ColumnBuilder::Utf8(b));
        }
        (AvroSchema::Enum(_), AvroValue::Enum(_, symbol)) => {
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::Utf8,
            });
            let mut b = StringBuilder::new();
            b.append_value(symbol.as_str());
            builders.push(ColumnBuilder::Utf8(b));
        }
        (AvroSchema::Uuid(_), AvroValue::Uuid(v)) => {
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::Utf8,
            });
            let mut b = StringBuilder::new();
            b.append_value(v.to_string());
            builders.push(ColumnBuilder::Utf8(b));
        }
        (AvroSchema::Uuid(_), AvroValue::String(v)) => {
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::Utf8,
            });
            let mut b = StringBuilder::new();
            b.append_value(v.as_str());
            builders.push(ColumnBuilder::Utf8(b));
        }
        (AvroSchema::Date, AvroValue::Date(v)) => {
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::Date32,
            });
            let mut b = PrimitiveBuilder::<Date32Type>::new();
            b.append_value(*v);
            builders.push(ColumnBuilder::Date32(b));
        }
        (AvroSchema::TimeMillis, AvroValue::TimeMillis(v)) => {
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::Time32Millisecond,
            });
            let mut b = PrimitiveBuilder::<Time32MillisecondType>::new();
            b.append_value(*v);
            builders.push(ColumnBuilder::Time32Millisecond(b));
        }
        (AvroSchema::TimeMicros, AvroValue::TimeMicros(v)) => {
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::Time64Microsecond,
            });
            let mut b = PrimitiveBuilder::<Time64MicrosecondType>::new();
            b.append_value(*v);
            builders.push(ColumnBuilder::Time64Microsecond(b));
        }
        (AvroSchema::TimestampMillis, AvroValue::TimestampMillis(v)) => {
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::TimestampMillisecondUtc,
            });
            let mut b = PrimitiveBuilder::<TimestampMillisecondType>::new();
            b.append_value(*v);
            builders.push(ColumnBuilder::TimestampMillisecond(b));
        }
        (AvroSchema::TimestampMicros, AvroValue::TimestampMicros(v)) => {
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::TimestampMicrosecondUtc,
            });
            let mut b = PrimitiveBuilder::<TimestampMicrosecondType>::new();
            b.append_value(*v);
            builders.push(ColumnBuilder::TimestampMicrosecond(b));
        }
        (AvroSchema::LocalTimestampMillis, AvroValue::LocalTimestampMillis(v)) => {
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::TimestampMillisecondLocal,
            });
            let mut b = PrimitiveBuilder::<TimestampMillisecondType>::new();
            b.append_value(*v);
            builders.push(ColumnBuilder::TimestampMillisecond(b));
        }
        (AvroSchema::LocalTimestampMicros, AvroValue::LocalTimestampMicros(v)) => {
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::TimestampMicrosecondLocal,
            });
            let mut b = PrimitiveBuilder::<TimestampMicrosecondType>::new();
            b.append_value(*v);
            builders.push(ColumnBuilder::TimestampMicrosecond(b));
        }
        (AvroSchema::Decimal(d), AvroValue::Decimal(dec)) => {
            let (precision, scale) = decimal_metadata(d, name)?;
            let bytes = <Vec<u8>>::try_from(dec).map_err(|e| {
                Error::Process(format!(
                    "Avro decimal conversion failed for field '{}': {}",
                    name, e
                ))
            })?;
            let unscaled = decode_decimal_i128(&bytes).ok_or_else(|| {
                Error::Process(format!(
                    "Avro decimal for field '{}' exceeds decimal128 range",
                    name
                ))
            })?;
            validate_decimal_value(name, unscaled, precision, scale)?;
            plans.push(ColumnPlan {
                name,
                nullable,
                kind: LeafKind::Decimal128 { precision, scale },
            });
            let mut b = Decimal128Builder::new();
            b.append_value(unscaled);
            builders.push(ColumnBuilder::Decimal128(b));
        }
        (AvroSchema::Record(_) | AvroSchema::Array(_) | AvroSchema::Map(_), _) => {
            return Err(Error::Process(format!(
                "Unsupported nested Avro type for field '{}': nested records/arrays/maps are not supported by the flat Arrow mapping",
                name
            )));
        }
        (s, v) => {
            return Err(Error::Process(format!(
                "Unsupported Avro type for field '{}': schema {:?} with value {:?}",
                name, s, v
            )));
        }
    }
    Ok(())
}

/// First-message path for a nullable-union field whose value is null: mirrors
/// `null_column` arm by arm (its rejection text differs from the value path's).
fn null_union_plan_and_append<'a>(
    name: &'a str,
    typed: &AvroSchema,
    plans: &mut Vec<ColumnPlan<'a>>,
    builders: &mut Vec<ColumnBuilder>,
) -> Result<(), Error> {
    let kind = match typed {
        AvroSchema::Boolean => LeafKind::Boolean,
        AvroSchema::Int => LeafKind::Int32,
        AvroSchema::Long => LeafKind::Int64,
        AvroSchema::Float => LeafKind::Float32,
        AvroSchema::Double => LeafKind::Float64,
        AvroSchema::Bytes | AvroSchema::Fixed(_) => LeafKind::Binary,
        AvroSchema::String | AvroSchema::Enum(_) | AvroSchema::Uuid(_) => LeafKind::Utf8,
        AvroSchema::Date => LeafKind::Date32,
        AvroSchema::TimeMillis => LeafKind::Time32Millisecond,
        AvroSchema::TimeMicros => LeafKind::Time64Microsecond,
        AvroSchema::TimestampMillis => LeafKind::TimestampMillisecondUtc,
        AvroSchema::TimestampMicros => LeafKind::TimestampMicrosecondUtc,
        AvroSchema::LocalTimestampMillis => LeafKind::TimestampMillisecondLocal,
        AvroSchema::LocalTimestampMicros => LeafKind::TimestampMicrosecondLocal,
        AvroSchema::Decimal(d) => {
            let (precision, scale) = decimal_metadata(d, name)?;
            LeafKind::Decimal128 { precision, scale }
        }
        other => {
            return Err(Error::Process(format!(
                "Unsupported Avro type for field '{}': {:?}",
                name, other
            )));
        }
    };
    let mut builder = ColumnBuilder::from_kind(&kind);
    builder.append_null();
    builders.push(builder);
    plans.push(ColumnPlan {
        name,
        nullable: true,
        kind,
    });
    Ok(())
}

impl ColumnBuilder {
    fn from_kind(kind: &LeafKind) -> Self {
        match kind {
            LeafKind::Boolean => ColumnBuilder::Boolean(BooleanBuilder::new()),
            LeafKind::Int32 => ColumnBuilder::Int32(PrimitiveBuilder::<Int32Type>::new()),
            LeafKind::Int64 => ColumnBuilder::Int64(PrimitiveBuilder::<Int64Type>::new()),
            LeafKind::Float32 => ColumnBuilder::Float32(PrimitiveBuilder::<Float32Type>::new()),
            LeafKind::Float64 => ColumnBuilder::Float64(PrimitiveBuilder::<Float64Type>::new()),
            LeafKind::Binary => ColumnBuilder::Binary(BinaryBuilder::new()),
            LeafKind::Utf8 => ColumnBuilder::Utf8(StringBuilder::new()),
            LeafKind::Date32 => ColumnBuilder::Date32(PrimitiveBuilder::<Date32Type>::new()),
            LeafKind::Time32Millisecond => {
                ColumnBuilder::Time32Millisecond(PrimitiveBuilder::<Time32MillisecondType>::new())
            }
            LeafKind::Time64Microsecond => {
                ColumnBuilder::Time64Microsecond(PrimitiveBuilder::<Time64MicrosecondType>::new())
            }
            LeafKind::TimestampMillisecondUtc | LeafKind::TimestampMillisecondLocal => {
                ColumnBuilder::TimestampMillisecond(
                    PrimitiveBuilder::<TimestampMillisecondType>::new(),
                )
            }
            LeafKind::TimestampMicrosecondUtc | LeafKind::TimestampMicrosecondLocal => {
                ColumnBuilder::TimestampMicrosecond(
                    PrimitiveBuilder::<TimestampMicrosecondType>::new(),
                )
            }
            LeafKind::Decimal128 { .. } => ColumnBuilder::Decimal128(Decimal128Builder::new()),
        }
    }
}

/// Later-message path for one field: mirrors `leaf_to_arrow`'s match (same
/// error texts) but appends into the existing builder.
fn append_decoded(
    builder: &mut ColumnBuilder,
    name: &str,
    schema: &AvroSchema,
    value: &AvroValue,
) -> Result<(), Error> {
    match (schema, value) {
        (AvroSchema::Boolean, AvroValue::Boolean(v)) => {
            let ColumnBuilder::Boolean(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(*v);
        }
        (AvroSchema::Int, AvroValue::Int(v)) => {
            let ColumnBuilder::Int32(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(*v);
        }
        (AvroSchema::Long, AvroValue::Long(v)) => {
            let ColumnBuilder::Int64(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(*v);
        }
        (AvroSchema::Float, AvroValue::Float(v)) => {
            let ColumnBuilder::Float32(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(*v);
        }
        (AvroSchema::Double, AvroValue::Double(v)) => {
            let ColumnBuilder::Float64(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(*v);
        }
        (AvroSchema::Bytes, AvroValue::Bytes(v)) => {
            let ColumnBuilder::Binary(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(v.as_slice());
        }
        (AvroSchema::Fixed(_), AvroValue::Fixed(_, v)) => {
            let ColumnBuilder::Binary(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(v.as_slice());
        }
        (AvroSchema::String, AvroValue::String(v)) => {
            let ColumnBuilder::Utf8(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(v.as_str());
        }
        (AvroSchema::Enum(_), AvroValue::Enum(_, symbol)) => {
            let ColumnBuilder::Utf8(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(symbol.as_str());
        }
        (AvroSchema::Uuid(_), AvroValue::Uuid(v)) => {
            let ColumnBuilder::Utf8(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(v.to_string());
        }
        (AvroSchema::Uuid(_), AvroValue::String(v)) => {
            let ColumnBuilder::Utf8(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(v.as_str());
        }
        (AvroSchema::Date, AvroValue::Date(v)) => {
            let ColumnBuilder::Date32(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(*v);
        }
        (AvroSchema::TimeMillis, AvroValue::TimeMillis(v)) => {
            let ColumnBuilder::Time32Millisecond(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(*v);
        }
        (AvroSchema::TimeMicros, AvroValue::TimeMicros(v)) => {
            let ColumnBuilder::Time64Microsecond(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(*v);
        }
        (AvroSchema::TimestampMillis, AvroValue::TimestampMillis(v)) => {
            let ColumnBuilder::TimestampMillisecond(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(*v);
        }
        (AvroSchema::TimestampMicros, AvroValue::TimestampMicros(v)) => {
            let ColumnBuilder::TimestampMicrosecond(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(*v);
        }
        (AvroSchema::LocalTimestampMillis, AvroValue::LocalTimestampMillis(v)) => {
            let ColumnBuilder::TimestampMillisecond(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(*v);
        }
        (AvroSchema::LocalTimestampMicros, AvroValue::LocalTimestampMicros(v)) => {
            let ColumnBuilder::TimestampMicrosecond(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(*v);
        }
        (AvroSchema::Decimal(d), AvroValue::Decimal(dec)) => {
            let (precision, scale) = decimal_metadata(d, name)?;
            let bytes = <Vec<u8>>::try_from(dec).map_err(|e| {
                Error::Process(format!(
                    "Avro decimal conversion failed for field '{}': {}",
                    name, e
                ))
            })?;
            let unscaled = decode_decimal_i128(&bytes).ok_or_else(|| {
                Error::Process(format!(
                    "Avro decimal for field '{}' exceeds decimal128 range",
                    name
                ))
            })?;
            validate_decimal_value(name, unscaled, precision, scale)?;
            let ColumnBuilder::Decimal128(b) = builder else {
                return Err(builder_kind_mismatch(name));
            };
            b.append_value(unscaled);
        }
        (AvroSchema::Record(_) | AvroSchema::Array(_) | AvroSchema::Map(_), _) => {
            return Err(Error::Process(format!(
                "Unsupported nested Avro type for field '{}': nested records/arrays/maps are not supported by the flat Arrow mapping",
                name
            )));
        }
        (s, v) => {
            return Err(Error::Process(format!(
                "Unsupported Avro type for field '{}': schema {:?} with value {:?}",
                name, s, v
            )));
        }
    }
    Ok(())
}

fn finish_column(plan: &ColumnPlan<'_>, builder: ColumnBuilder) -> Result<Arc<dyn Array>, Error> {
    let array: Arc<dyn Array> = match (&plan.kind, builder) {
        (LeafKind::Boolean, ColumnBuilder::Boolean(mut b)) => Arc::new(b.finish()),
        (LeafKind::Int32, ColumnBuilder::Int32(mut b)) => Arc::new(b.finish()),
        (LeafKind::Int64, ColumnBuilder::Int64(mut b)) => Arc::new(b.finish()),
        (LeafKind::Float32, ColumnBuilder::Float32(mut b)) => Arc::new(b.finish()),
        (LeafKind::Float64, ColumnBuilder::Float64(mut b)) => Arc::new(b.finish()),
        (LeafKind::Binary, ColumnBuilder::Binary(mut b)) => Arc::new(b.finish()),
        (LeafKind::Utf8, ColumnBuilder::Utf8(mut b)) => Arc::new(b.finish()),
        (LeafKind::Date32, ColumnBuilder::Date32(mut b)) => Arc::new(b.finish()),
        (LeafKind::Time32Millisecond, ColumnBuilder::Time32Millisecond(mut b)) => {
            Arc::new(b.finish())
        }
        (LeafKind::Time64Microsecond, ColumnBuilder::Time64Microsecond(mut b)) => {
            Arc::new(b.finish())
        }
        (LeafKind::TimestampMillisecondUtc, ColumnBuilder::TimestampMillisecond(mut b)) => {
            Arc::new(b.finish().with_timezone(Arc::from(UTC)))
        }
        (LeafKind::TimestampMillisecondLocal, ColumnBuilder::TimestampMillisecond(mut b)) => {
            Arc::new(b.finish())
        }
        (LeafKind::TimestampMicrosecondUtc, ColumnBuilder::TimestampMicrosecond(mut b)) => {
            Arc::new(b.finish().with_timezone(Arc::from(UTC)))
        }
        (LeafKind::TimestampMicrosecondLocal, ColumnBuilder::TimestampMicrosecond(mut b)) => {
            Arc::new(b.finish())
        }
        (LeafKind::Decimal128 { precision, scale }, ColumnBuilder::Decimal128(mut b)) => {
            let arr = b
                .finish()
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| {
                    Error::Process(format!(
                        "Avro decimal for field '{}' cannot set precision/scale: {}",
                        plan.name, e
                    ))
                })?;
            Arc::new(arr)
        }
        (_, _) => return Err(builder_kind_mismatch(plan.name)),
    };
    Ok(array)
}

/// Columnar batch converter for Avro payloads sharing one writer schema.
///
/// The first `push` walks the record exactly like `avro_value_to_arrow`
/// (same checks in the same per-field order, same error texts) and tees the
/// schema→Arrow mapping into per-column plans; every later `push` skips the
/// schema-level checks and appends through the builders. `finish` produces
/// the single multi-row batch directly — no per-message single-row batches,
/// no concat pass.
pub struct AvroBatchConverter<'a> {
    schema: &'a AvroSchema,
    plans: Vec<ColumnPlan<'a>>,
    builders: Vec<ColumnBuilder>,
    started: bool,
    rows: usize,
}

impl<'a> AvroBatchConverter<'a> {
    pub fn new(schema: &'a AvroSchema) -> Self {
        Self {
            schema,
            plans: Vec::new(),
            builders: Vec::new(),
            started: false,
            rows: 0,
        }
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn push(&mut self, payload: &[u8]) -> Result<(), Error> {
        let reader = GenericDatumReader::builder(self.schema)
            .build()
            .map_err(|e| Error::Process(format!("Avro reader build failed: {}", e)))?;
        let value = reader
            .read_value(&mut std::io::Cursor::new(payload))
            .map_err(|e| Error::Process(format!("Avro decode failed: {}", e)))?;
        self.push_decoded(&value)
    }

    fn push_decoded(&mut self, value: &AvroValue) -> Result<(), Error> {
        let (record_schema, values) = match (self.schema, value) {
            (AvroSchema::Record(r), AvroValue::Record(values)) => (r, values),
            _ => {
                return Err(Error::Process(
                    "Avro payload must decode to a record to be mapped to Arrow columns"
                        .to_string(),
                ));
            }
        };
        if record_schema.fields.len() != values.len() {
            return Err(Error::Process(format!(
                "Avro record field count mismatch: schema has {} fields, value has {}",
                record_schema.fields.len(),
                values.len()
            )));
        }
        for (i, (field, (value_name, value))) in
            record_schema.fields.iter().zip(values.iter()).enumerate()
        {
            if field.name != *value_name {
                return Err(Error::Process(format!(
                    "Avro record field order mismatch: schema field '{}' vs value field '{}'",
                    field.name, value_name
                )));
            }
            let (field_schema, val, nullable) = match (&field.schema, value) {
                (AvroSchema::Union(u), AvroValue::Union(idx, inner)) => {
                    let variants = u.variants();
                    if variants.len() != 2
                        || !variants.iter().any(|v| matches!(v, AvroSchema::Null))
                    {
                        return Err(Error::Process(format!(
                            "Unsupported Avro union for field '{}': only [null, T] unions are supported (found {} branches)",
                            field.name,
                            variants.len()
                        )));
                    }
                    if matches!(inner.as_ref(), AvroValue::Null) {
                        if self.started {
                            self.builders[i].append_null();
                        } else {
                            let typed = variants
                                .iter()
                                .find(|v| !matches!(v, AvroSchema::Null))
                                .expect("checked above: union contains a non-null branch");
                            null_union_plan_and_append(
                                &field.name,
                                typed,
                                &mut self.plans,
                                &mut self.builders,
                            )?;
                        }
                        continue;
                    }
                    (&variants[*idx as usize], inner.as_ref(), true)
                }
                (AvroSchema::Union(_), _) => {
                    return Err(Error::Process(format!(
                        "Avro field '{}' does not hold a union value but its schema is a union",
                        field.name
                    )));
                }
                (s, v) => (s, v, false),
            };
            if matches!(val, AvroValue::Null) {
                return Err(Error::Process(format!(
                    "Avro field '{}' is null but its schema is not nullable",
                    field.name
                )));
            }
            if self.started {
                append_decoded(&mut self.builders[i], &field.name, field_schema, val)?;
            } else {
                leaf_plan_and_append(
                    &field.name,
                    field_schema,
                    val,
                    nullable,
                    &mut self.plans,
                    &mut self.builders,
                )?;
            }
        }
        self.started = true;
        self.rows += 1;
        Ok(())
    }

    /// Builds the accumulated rows into one batch. `force_nullable` marks
    /// every column nullable, replicating what the schema-union merge does
    /// to multi-message batches (callers pass `rows >= 2`).
    pub fn finish(self, force_nullable: bool) -> Result<RecordBatch, Error> {
        if !self.started {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        let Self {
            plans,
            builders,
            rows,
            ..
        } = self;
        let fields: Vec<Field> = plans
            .iter()
            .map(|p| {
                Field::new(
                    p.name,
                    kind_data_type(&p.kind),
                    force_nullable || p.nullable,
                )
            })
            .collect();
        let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(builders.len());
        for (plan, builder) in plans.iter().zip(builders) {
            columns.push(finish_column(plan, builder)?);
        }
        RecordBatch::try_new_with_options(
            Arc::new(Schema::new(fields)),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|e| Error::Process(format!("Creating an Arrow record batch failed: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::Value;
    use apache_avro::writer::datum::GenericDatumWriter;
    use apache_avro::Decimal;

    fn encode(schema: &AvroSchema, value: Value) -> Vec<u8> {
        GenericDatumWriter::builder(schema)
            .build()
            .expect("writer build")
            .write_value_to_vec(value)
            .expect("encode")
    }

    fn parse(schema_json: &str) -> AvroSchema {
        AvroSchema::parse_str(schema_json).expect("schema parse")
    }

    #[test]
    fn basic_and_logical_types_map_to_arrow() {
        let schema = parse(
            r#"{
                "type": "record", "name": "R", "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"},
                    {"name": "score", "type": "double"},
                    {"name": "active", "type": "boolean"},
                    {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                    {"name": "day", "type": {"type": "int", "logicalType": "date"}},
                    {"name": "level", "type": {"type": "enum", "name": "Level", "symbols": ["LOW", "HIGH"]}},
                    {"name": "payload", "type": "bytes"}
                ]
            }"#,
        );
        let payload = encode(
            &schema,
            Value::Record(vec![
                ("id".into(), Value::Long(42)),
                ("name".into(), Value::String("sensor".into())),
                ("score".into(), Value::Double(1.5)),
                ("active".into(), Value::Boolean(true)),
                ("ts".into(), Value::TimestampMillis(1_700_000_000_000)),
                ("day".into(), Value::Date(19_000)),
                ("level".into(), Value::Enum(1, "HIGH".into())),
                ("payload".into(), Value::Bytes(vec![0xAB, 0xCD])),
            ]),
        );
        let batch = avro_to_arrow(&schema, &payload).unwrap();
        assert_eq!(batch.num_rows(), 1);

        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::{
            Date32Type, Float64Type, Int64Type, TimestampMillisecondType,
        };
        assert_eq!(
            batch
                .column_by_name("id")
                .unwrap()
                .as_primitive::<Int64Type>()
                .value(0),
            42
        );
        assert_eq!(
            batch
                .column_by_name("name")
                .unwrap()
                .as_string::<i32>()
                .value(0),
            "sensor"
        );
        assert_eq!(
            batch
                .column_by_name("score")
                .unwrap()
                .as_primitive::<Float64Type>()
                .value(0),
            1.5
        );
        assert!(batch
            .column_by_name("active")
            .unwrap()
            .as_boolean()
            .value(0));
        assert_eq!(
            batch
                .column_by_name("ts")
                .unwrap()
                .as_primitive::<TimestampMillisecondType>()
                .value(0),
            1_700_000_000_000
        );
        assert_eq!(
            batch.column_by_name("ts").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from(UTC)))
        );
        assert_eq!(
            batch
                .column_by_name("day")
                .unwrap()
                .as_primitive::<Date32Type>()
                .value(0),
            19_000
        );
        assert_eq!(
            batch
                .column_by_name("level")
                .unwrap()
                .as_string::<i32>()
                .value(0),
            "HIGH"
        );
        assert_eq!(
            batch
                .column_by_name("payload")
                .unwrap()
                .as_binary::<i32>()
                .value(0),
            &[0xAB, 0xCD]
        );
    }

    #[test]
    fn nullable_union_maps_to_nullable_column() {
        use datafusion::arrow::array::AsArray;
        let schema = parse(
            r#"{
                "type": "record", "name": "R", "fields": [
                    {"name": "note", "type": ["null", "string"]}
                ]
            }"#,
        );
        let with_value = encode(
            &schema,
            Value::Record(vec![(
                "note".into(),
                Value::Union(1, Box::new(Value::String("hi".into()))),
            )]),
        );
        let with_null = encode(
            &schema,
            Value::Record(vec![(
                "note".into(),
                Value::Union(0, Box::new(Value::Null)),
            )]),
        );
        let b1 = avro_to_arrow(&schema, &with_value).unwrap();
        let b2 = avro_to_arrow(&schema, &with_null).unwrap();
        let col1 = b1.column_by_name("note").unwrap();
        let col2 = b2.column_by_name("note").unwrap();
        assert_eq!(col1.data_type(), &DataType::Utf8);
        assert_eq!(col1.null_count(), 0);
        assert_eq!(col2.null_count(), 1);
        assert_eq!(col1.as_string::<i32>().value(0), "hi");
    }

    #[test]
    fn decimal_maps_to_decimal128() {
        let schema = parse(
            r#"{"type": "record", "name": "R", "fields": [
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}}
            ]}"#,
        );
        // 123.45 with scale 2 => unscaled 12345 => big-endian [0x30, 0x39]
        let payload = encode(
            &schema,
            Value::Record(vec![(
                "amount".into(),
                Value::Decimal(Decimal::from(vec![0x30, 0x39])),
            )]),
        );
        let batch = avro_to_arrow(&schema, &payload).unwrap();
        let col = batch.column_by_name("amount").unwrap();
        assert_eq!(col.data_type().to_string(), "Decimal128(10, 2)");
        use datafusion::arrow::array::AsArray;
        assert_eq!(
            col.as_primitive::<datafusion::arrow::datatypes::Decimal128Type>()
                .value(0),
            12345
        );
    }

    #[test]
    fn nullable_decimal_null_row_keeps_precision() {
        let schema = parse(
            r#"{"type": "record", "name": "R", "fields": [
                {"name": "amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}]}
            ]}"#,
        );
        let payload = encode(
            &schema,
            Value::Record(vec![(
                "amount".into(),
                Value::Union(0, Box::new(Value::Null)),
            )]),
        );
        let batch = avro_to_arrow(&schema, &payload).unwrap();
        let col = batch.column_by_name("amount").unwrap();
        assert_eq!(col.data_type().to_string(), "Decimal128(10, 2)");
        assert_eq!(col.null_count(), 1);
    }

    #[test]
    fn uuid_maps_to_utf8() {
        let schema = parse(
            r#"{"type": "record", "name": "R", "fields": [
                {"name": "uid", "type": {"type": "string", "logicalType": "uuid"}}
            ]}"#,
        );
        let payload = encode(
            &schema,
            Value::Record(vec![(
                "uid".into(),
                Value::Uuid("00000000-0000-0000-0000-000000000001".parse().unwrap()),
            )]),
        );
        let batch = avro_to_arrow(&schema, &payload).unwrap();
        use datafusion::arrow::array::AsArray;
        assert_eq!(
            batch
                .column_by_name("uid")
                .unwrap()
                .as_string::<i32>()
                .value(0),
            "00000000-0000-0000-0000-000000000001"
        );
    }

    #[test]
    fn null_union_rows_cover_every_supported_leaf_type() {
        let cases: Vec<(&str, &str)> = vec![
            ("b", "\"boolean\""),
            ("i", "\"int\""),
            ("l", "\"long\""),
            ("f", "\"float\""),
            ("d", "\"double\""),
            ("by", "\"bytes\""),
            ("s", "\"string\""),
            ("date", "{\"type\": \"int\", \"logicalType\": \"date\"}"),
            (
                "tms",
                "{\"type\": \"int\", \"logicalType\": \"time-millis\"}",
            ),
            (
                "tmcs",
                "{\"type\": \"long\", \"logicalType\": \"time-micros\"}",
            ),
            (
                "tsms",
                "{\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}",
            ),
            (
                "tslocal",
                "{\"type\": \"long\", \"logicalType\": \"local-timestamp-millis\"}",
            ),
        ];
        for (name, type_json) in cases {
            let schema = parse(&format!(
                "{{\"type\": \"record\", \"name\": \"R\", \"fields\": [{{\"name\": \"{name}\", \"type\": [\"null\", {type_json}]}}]}}"
            ));
            let payload = encode(
                &schema,
                Value::Record(vec![(name.into(), Value::Union(0, Box::new(Value::Null)))]),
            );
            let batch = avro_to_arrow(&schema, &payload)
                .unwrap_or_else(|e| panic!("field {name} with null union: {e}"));
            assert_eq!(batch.num_rows(), 1, "field {name}");
            assert!(batch.column(0).is_null(0), "field {name}");
        }
    }

    #[test]
    fn union_with_three_branches_is_rejected() {
        let schema = parse(
            r#"{"type": "record", "name": "R", "fields": [
                {"name": "x", "type": ["null", "int", "string"]}
            ]}"#,
        );
        let payload = encode(
            &schema,
            Value::Record(vec![("x".into(), Value::Union(1, Box::new(Value::Int(5))))]),
        );
        let err = avro_to_arrow(&schema, &payload).unwrap_err();
        assert!(err.to_string().contains("only [null, T] unions"), "{err}");
    }

    #[test]
    fn null_value_under_a_non_nullable_schema_is_rejected() {
        let schema =
            parse(r#"{"type": "record", "name": "R", "fields": [{"name": "x", "type": "int"}]}"#);
        // Build the mismatch directly through avro_value_to_arrow so the
        // null-under-non-nullable branch is exercised without the encoder
        // refusing first.
        let value = Value::Record(vec![("x".into(), Value::Null)]);
        let err = avro_value_to_arrow(&schema, &value).unwrap_err();
        assert!(err.to_string().contains("not nullable"), "{err}");
    }

    #[test]
    fn unsupported_leaf_kinds_are_rejected() {
        // An array leaf reaches leaf_to_arrow's unsupported arm.
        let schema = parse(
            r#"{"type": "record", "name": "R", "fields": [{"name": "x", "type": {"type": "array", "items": "int"}}]}"#,
        );
        let value = Value::Record(vec![("x".into(), Value::Array(vec![Value::Int(1)]))]);
        let err = avro_value_to_arrow(&schema, &value).unwrap_err();
        assert!(
            err.to_string().contains("Unsupported nested Avro type"),
            "{err}"
        );

        // A map leaf also lands on the same rejection.
        let schema = parse(
            r#"{"type": "record", "name": "R", "fields": [{"name": "x", "type": {"type": "map", "values": "int"}}]}"#,
        );
        let mut map = std::collections::HashMap::new();
        map.insert("k".to_string(), Value::Int(1));
        let value = Value::Record(vec![("x".into(), Value::Map(map))]);
        let err = avro_value_to_arrow(&schema, &value).unwrap_err();
        assert!(
            err.to_string().contains("Unsupported nested Avro type"),
            "{err}"
        );
    }

    #[test]
    fn every_leaf_type_maps_to_its_arrow_column() {
        // Direct value conversion (no encoder round trip) so every logical
        // type and value form can be exercised.
        let cases: Vec<(&str, Value)> = vec![
            ("f32", Value::Float(1.5)),
            ("by", Value::Bytes(vec![0xAB])),
            ("s", Value::String("text".into())),
            ("tmcs", Value::TimeMicros(1_500)),
            ("tslocal", Value::LocalTimestampMillis(1_700_000_000_000)),
            (
                "tslocal_us",
                Value::LocalTimestampMicros(1_700_000_000_000_000),
            ),
            ("uuid_str", Value::String(uuid_like())),
        ];
        let schema = parse(
            r#"{"type": "record", "name": "R", "fields": [
                {"name": "f32", "type": "float"},
                {"name": "by", "type": "bytes"},
                {"name": "s", "type": "string"},
                {"name": "tmcs", "type": {"type": "long", "logicalType": "time-micros"}},
                {"name": "tslocal", "type": {"type": "long", "logicalType": "local-timestamp-millis"}},
                {"name": "tslocal_us", "type": {"type": "long", "logicalType": "local-timestamp-micros"}},
                {"name": "uuid_str", "type": {"type": "string", "logicalType": "uuid"}}
            ]}"#,
        );
        let value = Value::Record(
            cases
                .iter()
                .map(|(name, value)| (name.to_string(), value.clone()))
                .collect(),
        );
        let batch = avro_value_to_arrow(&schema, &value)
            .unwrap_or_else(|e| panic!("leaf mapping failed: {e}"));
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), cases.len());
        // The uuid-as-string branch keeps text form.
        let uuid_col = batch.column_by_name("uuid_str").unwrap();
        assert_eq!(uuid_col.data_type(), &DataType::Utf8);
    }

    // A Uuid-shaped string for the schema-vs-value widening branch.
    fn uuid_like() -> String {
        "550e8400-e29b-41d4-a716-446655440000".to_string()
    }

    #[test]
    fn schema_value_mismatch_is_rejected_in_leaf_mapping() {
        // Schema says long but the value is a string: the catch-all arm.
        let schema =
            parse(r#"{"type": "record", "name": "R", "fields": [{"name": "x", "type": "long"}]}"#);
        let value = Value::Record(vec![("x".into(), Value::String("nope".into()))]);
        let err = avro_value_to_arrow(&schema, &value).unwrap_err();
        assert!(err.to_string().contains("Unsupported Avro type"), "{err}");
    }

    #[test]
    fn union_schema_with_non_union_value_is_rejected() {
        let schema = parse(
            r#"{"type": "record", "name": "R", "fields": [{"name": "x", "type": ["null", "int"]}]}"#,
        );
        let value = Value::Record(vec![("x".into(), Value::Int(3))]);
        let err = avro_value_to_arrow(&schema, &value).unwrap_err();
        assert!(
            err.to_string().contains("does not hold a union value"),
            "{err}"
        );
    }

    #[test]
    fn nested_structures_are_rejected() {
        let schema = parse(
            r#"{"type": "record", "name": "R", "fields": [
                {"name": "tags", "type": {"type": "array", "items": "string"}}
            ]}"#,
        );
        let payload = encode(
            &schema,
            Value::Record(vec![(
                "tags".into(),
                Value::Array(vec![Value::String("a".into())]),
            )]),
        );
        let err = avro_to_arrow(&schema, &payload).unwrap_err();
        assert!(format!("{err}").contains("nested"), "got: {err}");
    }

    #[test]
    fn multi_branch_union_is_rejected() {
        let schema = parse(
            r#"{"type": "record", "name": "R", "fields": [
                {"name": "v", "type": ["null", "string", "int"]}
            ]}"#,
        );
        let payload = encode(
            &schema,
            Value::Record(vec![(
                "v".into(),
                Value::Union(1, Box::new(Value::String("x".into()))),
            )]),
        );
        let err = avro_to_arrow(&schema, &payload).unwrap_err();
        assert!(format!("{err}").contains("union"), "got: {err}");
    }

    #[test]
    fn non_record_payload_is_rejected() {
        let schema = parse(r#""string""#);
        let payload = encode(&schema, Value::String("x".into()));
        let err = avro_to_arrow(&schema, &payload).unwrap_err();
        assert!(format!("{err}").contains("record"), "got: {err}");
    }

    // ===== AvroBatchConverter =====

    /// Wide schema covering every supported leaf type (nullable unions mixed
    /// with required fields, logical types, decimal, enum).
    fn wide_schema() -> AvroSchema {
        parse(
            r#"{
                "type": "record", "name": "R", "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": ["null", "string"]},
                    {"name": "score", "type": "double"},
                    {"name": "active", "type": ["null", "boolean"]},
                    {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                    {"name": "tsl", "type": {"type": "long", "logicalType": "local-timestamp-micros"}},
                    {"name": "day", "type": {"type": "int", "logicalType": "date"}},
                    {"name": "tm", "type": {"type": "int", "logicalType": "time-millis"}},
                    {"name": "tmc", "type": {"type": "long", "logicalType": "time-micros"}},
                    {"name": "level", "type": {"type": "enum", "name": "Level", "symbols": ["LOW", "HIGH"]}},
                    {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
                    {"name": "payload", "type": "bytes"},
                    {"name": "ratio", "type": "float"},
                    {"name": "tsu", "type": {"type": "long", "logicalType": "timestamp-micros"}}
                ]
            }"#,
        )
    }

    fn wide_value(seq: i64, name: Option<&str>) -> Value {
        let union_name = match name {
            Some(s) => Value::Union(1, Box::new(Value::String(s.to_string()))),
            None => Value::Union(0, Box::new(Value::Null)),
        };
        Value::Record(vec![
            ("id".into(), Value::Long(seq)),
            ("name".into(), union_name),
            ("score".into(), Value::Double(seq as f64 * 0.5)),
            (
                "active".into(),
                Value::Union(
                    if seq % 2 == 0 { 1 } else { 0 },
                    Box::new(if seq % 2 == 0 {
                        Value::Boolean(true)
                    } else {
                        Value::Null
                    }),
                ),
            ),
            ("ts".into(), Value::TimestampMillis(1_700_000_000_000 + seq)),
            (
                "tsl".into(),
                Value::LocalTimestampMicros(1_700_000_000_000_000 + seq),
            ),
            ("day".into(), Value::Date(19_000 + seq as i32)),
            ("tm".into(), Value::TimeMillis(500 + seq as i32)),
            ("tmc".into(), Value::TimeMicros(1_500 + seq)),
            (
                "level".into(),
                Value::Enum(
                    (seq % 2) as u32,
                    if seq % 2 == 0 {
                        "LOW".into()
                    } else {
                        "HIGH".into()
                    },
                ),
            ),
            (
                "amount".into(),
                Value::Decimal(apache_avro::Decimal::from(vec![0x30, 0x39])),
            ),
            (
                "payload".into(),
                Value::Bytes(vec![0xAB, (seq % 256) as u8]),
            ),
            ("ratio".into(), Value::Float(1.5)),
            (
                "tsu".into(),
                Value::TimestampMicros(1_700_000_000_000_000 + seq),
            ),
        ])
    }

    /// The legacy semantics: per-message single-row batches merged the way
    /// `SchemaRegistryCodec::decode` used to do it.
    fn per_message_concat(schema: &AvroSchema, payloads: &[Vec<u8>]) -> RecordBatch {
        let batches: Vec<RecordBatch> = payloads
            .iter()
            .map(|p| avro_to_arrow(schema, p).unwrap())
            .collect();
        crate::component::batch_merge::normalize_and_concat(&batches).unwrap()
    }

    #[test]
    fn batch_converter_multi_row_all_types() {
        let schema = wide_schema();
        let payloads: Vec<Vec<u8>> = (0..3)
            .map(|i| {
                let name = if i == 1 { None } else { Some(format!("s{i}")) };
                encode(&schema, wide_value(i, name.as_deref()))
            })
            .collect();
        let mut conv = AvroBatchConverter::new(&schema);
        for p in &payloads {
            conv.push(p).unwrap();
        }
        let batch = conv.finish(true).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 14);

        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::{
            Date32Type, TimestampMicrosecondType, TimestampMillisecondType,
        };
        // Row order follows message order; nulls interleave per row.
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int64Type>();
        assert_eq!(
            (0..3).map(|i| ids.value(i)).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        let names = batch.column_by_name("name").unwrap().as_string::<i32>();
        assert_eq!(names.value(0), "s0");
        assert!(names.is_null(1));
        assert_eq!(names.value(2), "s2");
        let active = batch.column_by_name("active").unwrap().as_boolean();
        assert!(active.value(0));
        assert!(active.is_null(1));
        assert!(active.value(2));
        let ts = batch
            .column_by_name("ts")
            .unwrap()
            .as_primitive::<TimestampMillisecondType>();
        assert_eq!(ts.value(1), 1_700_000_000_001);
        assert_eq!(
            batch.column_by_name("ts").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from(UTC)))
        );
        assert_eq!(
            batch.column_by_name("tsl").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        let day = batch
            .column_by_name("day")
            .unwrap()
            .as_primitive::<Date32Type>();
        assert_eq!(day.value(2), 19_002);
        let level = batch.column_by_name("level").unwrap().as_string::<i32>();
        assert_eq!(level.value(0), "LOW");
        assert_eq!(level.value(1), "HIGH");
        let amount = batch.column_by_name("amount").unwrap();
        assert_eq!(amount.data_type().to_string(), "Decimal128(10, 2)");
        let tsu = batch
            .column_by_name("tsu")
            .unwrap()
            .as_primitive::<TimestampMicrosecondType>();
        assert_eq!(tsu.value(0), 1_700_000_000_000_000);
        // N>=2 replicates the union merge's all-nullable promotion.
        assert!(batch.schema().fields().iter().all(|f| f.is_nullable()));
    }

    #[test]
    fn batch_converter_output_matches_per_message_concat() {
        let schema = wide_schema();
        let payloads: Vec<Vec<u8>> = (0..4)
            .map(|i| {
                let name = if i % 2 == 0 {
                    Some(format!("n{i}"))
                } else {
                    None
                };
                encode(&schema, wide_value(i, name.as_deref()))
            })
            .collect();
        let mut conv = AvroBatchConverter::new(&schema);
        for p in &payloads {
            conv.push(p).unwrap();
        }
        let fast = conv.finish(payloads.len() >= 2).unwrap();
        let legacy = per_message_concat(&schema, &payloads);
        assert_eq!(fast.schema().as_ref(), legacy.schema().as_ref());
        assert_eq!(fast.num_rows(), legacy.num_rows());
        for (i, name) in fast.schema().fields().iter().enumerate() {
            assert!(
                fast.column(i) == legacy.column(i),
                "column {} ({}) differs",
                i,
                name.name()
            );
        }

        // N = 1 keeps the plan (schema-faithful) nullability.
        let one = encode(&schema, wide_value(9, Some("x")));
        let mut conv1 = AvroBatchConverter::new(&schema);
        conv1.push(&one).unwrap();
        let fast1 = conv1.finish(false).unwrap();
        let legacy1 = per_message_concat(&schema, &[one]);
        assert_eq!(fast1.schema().as_ref(), legacy1.schema().as_ref());
        assert!(fast1.column(0) == legacy1.column(0));
        assert!(!fast1.schema().field(0).is_nullable());
    }

    #[test]
    fn batch_converter_keeps_error_texts_and_order() {
        // Field-level interleaving on the first message: an earlier field's
        // value-level error wins over a later field's schema-level error.
        let schema = parse(
            r#"{"type": "record", "name": "R", "fields": [
                {"name": "a", "type": "long"},
                {"name": "b", "type": {"type": "array", "items": "int"}}
            ]}"#,
        );
        let mut conv = AvroBatchConverter::new(&schema);
        let err = conv
            .push_decoded(&Value::Record(vec![
                ("a".into(), Value::Null),
                ("b".into(), Value::Array(vec![])),
            ]))
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("null but its schema is not nullable"),
            "{err}"
        );

        let mut conv = AvroBatchConverter::new(&schema);
        let err = conv
            .push_decoded(&Value::Record(vec![
                ("a".into(), Value::Long(1)),
                ("b".into(), Value::Array(vec![Value::Int(1)])),
            ]))
            .unwrap_err();
        assert!(
            err.to_string().contains("Unsupported nested Avro type"),
            "{err}"
        );

        // Later messages re-check values (schema-level rejection is decided
        // once and cannot resurface, mirroring reachability in the old path).
        let ok_schema = parse(
            r#"{"type": "record", "name": "R", "fields": [
                {"name": "u", "type": ["null", "int"]}
            ]}"#,
        );
        let mut conv = AvroBatchConverter::new(&ok_schema);
        conv.push_decoded(&Value::Record(vec![(
            "u".into(),
            Value::Union(1, Box::new(Value::Int(5))),
        )]))
        .unwrap();
        let err = conv
            .push_decoded(&Value::Record(vec![("u".into(), Value::Int(6))]))
            .unwrap_err();
        assert!(
            err.to_string().contains("does not hold a union value"),
            "{err}"
        );
        // Null rows on later messages land in the nullable column.
        conv.push_decoded(&Value::Record(vec![(
            "u".into(),
            Value::Union(0, Box::new(Value::Null)),
        )]))
        .unwrap();
        let batch = conv.finish(true).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.column(0).null_count(), 1);
    }

    #[test]
    fn batch_converter_decimal_behaves_like_the_single_value_path() {
        // Whatever arrow accepts or rejects for a value/precision pair, the
        // batch path must agree with the per-message path on the same value.
        let schema = parse(
            r#"{"type": "record", "name": "R", "fields": [
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 1}}
            ]}"#,
        );
        let value = Value::Record(vec![(
            "amount".into(),
            Value::Decimal(apache_avro::Decimal::from(vec![0x30, 0x39])),
        )]);
        let mut conv = AvroBatchConverter::new(&schema);
        let batch_result = conv.push_decoded(&value).and_then(|_| conv.finish(true));
        let single_result = avro_value_to_arrow(&schema, &value);
        match (batch_result, single_result) {
            (Ok(a), Ok(b)) => {
                assert_eq!(a.num_rows(), 1);
                assert!(a.column(0) == b.column(0), "decimal column differs");
            }
            (Err(a), Err(b)) => assert_eq!(a.to_string(), b.to_string()),
            (a, b) => panic!("batch vs single-value divergence: {a:?} vs {b:?}"),
        }

        // Out-of-range decimal *metadata* is a schema-level rejection whose
        // text the batch path keeps verbatim.
        let bad = parse(
            r#"{"type": "record", "name": "R", "fields": [
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 40, "scale": 1}}
            ]}"#,
        );
        let value = Value::Record(vec![(
            "amount".into(),
            Value::Decimal(apache_avro::Decimal::from(vec![0x30])),
        )]);
        let mut conv = AvroBatchConverter::new(&bad);
        let err = conv.push_decoded(&value).unwrap_err();
        assert!(err.to_string().contains("out of decimal128 range"), "{err}");
    }
}
