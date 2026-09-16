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

use apache_avro::Schema as AvroSchema;
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::types::Value as AvroValue;
use arkflow_core::Error;
use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, StringArray, Time32MillisecondArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
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
            Arc::new(TimestampMillisecondArray::from(vec![None]).with_timezone(Arc::from(UTC))),
        ),
        AvroSchema::LocalTimestampMicros => (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            Arc::new(TimestampMicrosecondArray::from(vec![None]).with_timezone(Arc::from(UTC))),
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
        (AvroSchema::Boolean, AvroValue::Boolean(v)) => {
            (DataType::Boolean, Arc::new(BooleanArray::from(vec![Some(*v)])))
        }
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
        (AvroSchema::Date, AvroValue::Date(v)) => {
            (DataType::Date32, Arc::new(Date32Array::from(vec![Some(*v)])))
        }
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
            Arc::new(TimestampMillisecondArray::from(vec![Some(*v)]).with_timezone(Arc::from(UTC))),
        ),
        (AvroSchema::LocalTimestampMicros, AvroValue::LocalTimestampMicros(v)) => (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(*v)]).with_timezone(Arc::from(UTC))),
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
        (
            AvroSchema::Record(_) | AvroSchema::Array(_) | AvroSchema::Map(_),
            _,
        ) => {
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

fn decimal_metadata(
    d: &apache_avro::schema::DecimalSchema,
    name: &str,
) -> Result<(u8, i8), Error> {
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
                (
                    "ts".into(),
                    Value::TimestampMillis(1_700_000_000_000),
                ),
                ("day".into(), Value::Date(19_000)),
                ("level".into(), Value::Enum(1, "HIGH".into())),
                ("payload".into(), Value::Bytes(vec![0xAB, 0xCD])),
            ]),
        );
        let batch = avro_to_arrow(&schema, &payload).unwrap();
        assert_eq!(batch.num_rows(), 1);

        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::{Date32Type, Float64Type, Int64Type, TimestampMillisecondType};
        assert_eq!(batch.column_by_name("id").unwrap().as_primitive::<Int64Type>().value(0), 42);
        assert_eq!(
            batch.column_by_name("name").unwrap().as_string::<i32>().value(0),
            "sensor"
        );
        assert_eq!(
            batch.column_by_name("score").unwrap().as_primitive::<Float64Type>().value(0),
            1.5
        );
        assert!(batch.column_by_name("active").unwrap().as_boolean().value(0));
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
            batch.column_by_name("level").unwrap().as_string::<i32>().value(0),
            "HIGH"
        );
        assert_eq!(
            batch.column_by_name("payload").unwrap().as_binary::<i32>().value(0),
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
            Value::Record(vec![("note".into(), Value::Union(1, Box::new(Value::String("hi".into()))))]),
        );
        let with_null = encode(
            &schema,
            Value::Record(vec![("note".into(), Value::Union(0, Box::new(Value::Null)))]),
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
        assert_eq!(col.as_primitive::<datafusion::arrow::datatypes::Decimal128Type>().value(0), 12345);
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
            batch.column_by_name("uid").unwrap().as_string::<i32>().value(0),
            "00000000-0000-0000-0000-000000000001"
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
            Value::Record(vec![("tags".into(), Value::Array(vec![Value::String("a".into())]))]),
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
            Value::Record(vec![("v".into(), Value::Union(1, Box::new(Value::String("x".into()))))]),
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
}
