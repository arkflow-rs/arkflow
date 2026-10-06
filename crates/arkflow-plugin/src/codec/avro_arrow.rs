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
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int32Builder, Int64Builder, StringBuilder, Time32MillisecondBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

const UTC: &str = "UTC";

/// Decodes one Avro binary datum against the writer `schema` and converts it
/// to a single-row Arrow batch.
pub fn avro_to_arrow(schema: &AvroSchema, payload: &[u8]) -> Result<RecordBatch, Error> {
    let value = avro_read_value(schema, payload)?;
    let mut accumulator = AvroArrowAccumulator::new(schema)?;
    accumulator.push(&value)?;
    accumulator.finish()
}

/// Decodes one Avro binary datum against the writer `schema` into its
/// `Avro::Value` form (the per-message reader step of the codec hot path).
pub fn avro_read_value(schema: &AvroSchema, payload: &[u8]) -> Result<AvroValue, Error> {
    let reader = GenericDatumReader::builder(schema)
        .build()
        .map_err(|e| Error::Process(format!("Avro reader build failed: {}", e)))?;
    reader
        .read_value(&mut std::io::Cursor::new(payload))
        .map_err(|e| Error::Process(format!("Avro decode failed: {}", e)))
}

/// One column builder, mirroring the flat leaf mapping of the single-row
/// conversion path: same Arrow types, same timezone and decimal constants.
enum Column {
    Boolean(BooleanBuilder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Date32(Date32Builder),
    Time32Milli(Time32MillisecondBuilder),
    Time64Micro(Time64MicrosecondBuilder),
    TimestampMilliUtc(TimestampMillisecondBuilder),
    TimestampMicroUtc(TimestampMicrosecondBuilder),
    TimestampMilliLocal(TimestampMillisecondBuilder),
    TimestampMicroLocal(TimestampMicrosecondBuilder),
    Decimal128(Decimal128Builder, u8, i8),
}

impl Column {
    /// Builder + Arrow type for one leaf schema. Nested records, arrays and
    /// maps are rejected here (construction time) exactly like the flat
    /// mapping rejects them at conversion time.
    fn new(name: &str, schema: &AvroSchema) -> Result<(Column, DataType), Error> {
        Ok(match schema {
            AvroSchema::Boolean => (Column::Boolean(BooleanBuilder::new()), DataType::Boolean),
            AvroSchema::Int => (Column::Int32(Int32Builder::new()), DataType::Int32),
            AvroSchema::Long => (Column::Int64(Int64Builder::new()), DataType::Int64),
            AvroSchema::Float => (Column::Float32(Float32Builder::new()), DataType::Float32),
            AvroSchema::Double => (Column::Float64(Float64Builder::new()), DataType::Float64),
            AvroSchema::String | AvroSchema::Enum(_) | AvroSchema::Uuid(_) => {
                (Column::Utf8(StringBuilder::new()), DataType::Utf8)
            }
            AvroSchema::Bytes | AvroSchema::Fixed(_) => {
                (Column::Binary(BinaryBuilder::new()), DataType::Binary)
            }
            AvroSchema::Date => (Column::Date32(Date32Builder::new()), DataType::Date32),
            AvroSchema::TimeMillis => (
                Column::Time32Milli(Time32MillisecondBuilder::new()),
                DataType::Time32(TimeUnit::Millisecond),
            ),
            AvroSchema::TimeMicros => (
                Column::Time64Micro(Time64MicrosecondBuilder::new()),
                DataType::Time64(TimeUnit::Microsecond),
            ),
            AvroSchema::TimestampMillis => (
                Column::TimestampMilliUtc(TimestampMillisecondBuilder::new()),
                DataType::Timestamp(TimeUnit::Millisecond, Some(UTC.into())),
            ),
            AvroSchema::TimestampMicros => (
                Column::TimestampMicroUtc(TimestampMicrosecondBuilder::new()),
                DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
            ),
            AvroSchema::LocalTimestampMillis => (
                Column::TimestampMilliLocal(TimestampMillisecondBuilder::new()),
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ),
            AvroSchema::LocalTimestampMicros => (
                Column::TimestampMicroLocal(TimestampMicrosecondBuilder::new()),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            AvroSchema::Decimal(d) => {
                let (precision, scale) = decimal_metadata(d, name)?;
                (
                    Column::Decimal128(Decimal128Builder::new(), precision, scale),
                    DataType::Decimal128(precision, scale),
                )
            }
            AvroSchema::Record(_) | AvroSchema::Array(_) | AvroSchema::Map(_) => {
                return Err(Error::Process(format!(
                    "Unsupported nested Avro type for field '{}': nested records/arrays/maps are not supported by the flat Arrow mapping",
                    name
                )));
            }
            other => {
                return Err(Error::Process(format!(
                    "Unsupported Avro type for field '{}': {:?}",
                    name, other
                )));
            }
        })
    }

    /// Appends one non-null leaf value. The catch-all arm keeps the
    /// schema-vs-value mismatch rejection of the single-row path.
    fn push_leaf(
        &mut self,
        name: &str,
        schema: &AvroSchema,
        value: &AvroValue,
    ) -> Result<(), Error> {
        match (schema, value) {
            (AvroSchema::Boolean, AvroValue::Boolean(v)) => {
                self.boolean().append_value(*v);
            }
            (AvroSchema::Int, AvroValue::Int(v)) => self.int32().append_value(*v),
            (AvroSchema::Long, AvroValue::Long(v)) => self.int64().append_value(*v),
            (AvroSchema::Float, AvroValue::Float(v)) => self.float32().append_value(*v),
            (AvroSchema::Double, AvroValue::Double(v)) => self.float64().append_value(*v),
            (AvroSchema::String, AvroValue::String(v)) => self.utf8().append_value(v),
            (AvroSchema::Enum(_), AvroValue::Enum(_, symbol)) => {
                self.utf8().append_value(symbol);
            }
            (AvroSchema::Uuid(_), AvroValue::Uuid(v)) => {
                self.utf8().append_value(v.to_string());
            }
            // A Uuid-shaped plain string keeps the text form (widening branch).
            (AvroSchema::Uuid(_), AvroValue::String(v)) => self.utf8().append_value(v),
            (AvroSchema::Bytes, AvroValue::Bytes(v)) => self.binary().append_value(v),
            (AvroSchema::Fixed(_), AvroValue::Fixed(_, v)) => self.binary().append_value(v),
            (AvroSchema::Date, AvroValue::Date(v)) => self.date32().append_value(*v),
            (AvroSchema::TimeMillis, AvroValue::TimeMillis(v)) => {
                self.time32_milli().append_value(*v);
            }
            (AvroSchema::TimeMicros, AvroValue::TimeMicros(v)) => {
                self.time64_micro().append_value(*v);
            }
            (AvroSchema::TimestampMillis, AvroValue::TimestampMillis(v)) => {
                self.timestamp_milli_utc().append_value(*v);
            }
            (AvroSchema::TimestampMicros, AvroValue::TimestampMicros(v)) => {
                self.timestamp_micro_utc().append_value(*v);
            }
            (AvroSchema::LocalTimestampMillis, AvroValue::LocalTimestampMillis(v)) => {
                self.timestamp_milli_local().append_value(*v);
            }
            (AvroSchema::LocalTimestampMicros, AvroValue::LocalTimestampMicros(v)) => {
                self.timestamp_micro_local().append_value(*v)
            }
            (AvroSchema::Decimal(_), AvroValue::Decimal(dec)) => {
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
                match self {
                    Column::Decimal128(builder, _, _) => builder.append_value(unscaled),
                    _ => unreachable!("decimal metadata construction guarantees the builder kind"),
                }
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

    fn append_null(&mut self) {
        match self {
            Column::Boolean(b) => b.append_null(),
            Column::Int32(b) => b.append_null(),
            Column::Int64(b) => b.append_null(),
            Column::Float32(b) => b.append_null(),
            Column::Float64(b) => b.append_null(),
            Column::Utf8(b) => b.append_null(),
            Column::Binary(b) => b.append_null(),
            Column::Date32(b) => b.append_null(),
            Column::Time32Milli(b) => b.append_null(),
            Column::Time64Micro(b) => b.append_null(),
            Column::TimestampMilliUtc(b) => b.append_null(),
            Column::TimestampMicroUtc(b) => b.append_null(),
            Column::TimestampMilliLocal(b) => b.append_null(),
            Column::TimestampMicroLocal(b) => b.append_null(),
            Column::Decimal128(b, _, _) => b.append_null(),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Column::Boolean(b) => Arc::new(b.finish()),
            Column::Int32(b) => Arc::new(b.finish()),
            Column::Int64(b) => Arc::new(b.finish()),
            Column::Float32(b) => Arc::new(b.finish()),
            Column::Float64(b) => Arc::new(b.finish()),
            Column::Utf8(b) => Arc::new(b.finish()),
            Column::Binary(b) => Arc::new(b.finish()),
            Column::Date32(b) => Arc::new(b.finish()),
            Column::Time32Milli(b) => Arc::new(b.finish()),
            Column::Time64Micro(b) => Arc::new(b.finish()),
            Column::TimestampMilliUtc(b) => Arc::new(b.finish().with_timezone(Arc::from(UTC))),
            Column::TimestampMicroUtc(b) => Arc::new(b.finish().with_timezone(Arc::from(UTC))),
            // Local timestamps carry no timezone; stamping one would make the
            // array type diverge from the declared column type.
            Column::TimestampMilliLocal(b) => Arc::new(b.finish()),
            Column::TimestampMicroLocal(b) => Arc::new(b.finish()),
            Column::Decimal128(b, precision, scale) => Arc::new(
                b.finish()
                    .with_precision_and_scale(*precision, *scale)
                    .expect("decimal metadata construction guarantees valid precision/scale"),
            ),
        }
    }

    // Typed accessors used by `push_leaf`; construction guarantees the kind.
    fn boolean(&mut self) -> &mut BooleanBuilder {
        match self {
            Column::Boolean(b) => b,
            _ => unreachable!("schema/builder kind mismatch"),
        }
    }
    fn int32(&mut self) -> &mut Int32Builder {
        match self {
            Column::Int32(b) => b,
            _ => unreachable!("schema/builder kind mismatch"),
        }
    }
    fn int64(&mut self) -> &mut Int64Builder {
        match self {
            Column::Int64(b) => b,
            _ => unreachable!("schema/builder kind mismatch"),
        }
    }
    fn float32(&mut self) -> &mut Float32Builder {
        match self {
            Column::Float32(b) => b,
            _ => unreachable!("schema/builder kind mismatch"),
        }
    }
    fn float64(&mut self) -> &mut Float64Builder {
        match self {
            Column::Float64(b) => b,
            _ => unreachable!("schema/builder kind mismatch"),
        }
    }
    fn utf8(&mut self) -> &mut StringBuilder {
        match self {
            Column::Utf8(b) => b,
            _ => unreachable!("schema/builder kind mismatch"),
        }
    }
    fn binary(&mut self) -> &mut BinaryBuilder {
        match self {
            Column::Binary(b) => b,
            _ => unreachable!("schema/builder kind mismatch"),
        }
    }
    fn date32(&mut self) -> &mut Date32Builder {
        match self {
            Column::Date32(b) => b,
            _ => unreachable!("schema/builder kind mismatch"),
        }
    }
    fn time32_milli(&mut self) -> &mut Time32MillisecondBuilder {
        match self {
            Column::Time32Milli(b) => b,
            _ => unreachable!("schema/builder kind mismatch"),
        }
    }
    fn time64_micro(&mut self) -> &mut Time64MicrosecondBuilder {
        match self {
            Column::Time64Micro(b) => b,
            _ => unreachable!("schema/builder kind mismatch"),
        }
    }
    fn timestamp_milli_utc(&mut self) -> &mut TimestampMillisecondBuilder {
        match self {
            Column::TimestampMilliUtc(b) => b,
            _ => unreachable!("schema/builder kind mismatch"),
        }
    }
    fn timestamp_micro_utc(&mut self) -> &mut TimestampMicrosecondBuilder {
        match self {
            Column::TimestampMicroUtc(b) => b,
            _ => unreachable!("schema/builder kind mismatch"),
        }
    }
    fn timestamp_milli_local(&mut self) -> &mut TimestampMillisecondBuilder {
        match self {
            Column::TimestampMilliLocal(b) => b,
            _ => unreachable!("schema/builder kind mismatch"),
        }
    }
    fn timestamp_micro_local(&mut self) -> &mut TimestampMicrosecondBuilder {
        match self {
            Column::TimestampMicroLocal(b) => b,
            _ => unreachable!("schema/builder kind mismatch"),
        }
    }
}

/// Columnar accumulator over one writer schema: values are appended straight
/// into per-field builders, so a batch of N messages costs one builder per
/// field instead of N×fields single-element arrays plus a concat copy.
pub struct AvroArrowAccumulator {
    fields: Vec<Field>,
    /// Resolved (union-unwrapped) leaf schema per column, kept for the
    /// schema-vs-value pairing checks of `push`.
    leaf_schemas: Vec<AvroSchema>,
    columns: Vec<Column>,
    rows: usize,
}

impl AvroArrowAccumulator {
    pub fn new(schema: &AvroSchema) -> Result<Self, Error> {
        let AvroSchema::Record(record) = schema else {
            return Err(Error::Process(
                "Avro payload must decode to a record to be mapped to Arrow columns".to_string(),
            ));
        };
        let mut fields = Vec::with_capacity(record.fields.len());
        let mut leaf_schemas = Vec::with_capacity(record.fields.len());
        let mut columns = Vec::with_capacity(record.fields.len());
        for field in &record.fields {
            let (schema, nullable) = match &field.schema {
                AvroSchema::Union(u) => {
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
                    let typed = variants
                        .iter()
                        .find(|v| !matches!(v, AvroSchema::Null))
                        .expect("checked above: union contains a non-null branch");
                    (typed.clone(), true)
                }
                other => (other.clone(), false),
            };
            let (column, data_type) = Column::new(&field.name, &schema)?;
            fields.push(Field::new(&field.name, data_type, nullable));
            leaf_schemas.push(schema);
            columns.push(column);
        }
        Ok(Self {
            fields,
            leaf_schemas,
            columns,
            rows: 0,
        })
    }

    /// Appends one decoded record value. Field count, order and union
    /// nullability keep the single-row path's error messages.
    pub fn push(&mut self, value: &AvroValue) -> Result<(), Error> {
        let AvroValue::Record(values) = value else {
            return Err(Error::Process(
                "Avro payload must decode to a record to be mapped to Arrow columns".to_string(),
            ));
        };
        if self.fields.len() != values.len() {
            return Err(Error::Process(format!(
                "Avro record field count mismatch: schema has {} fields, value has {}",
                self.fields.len(),
                values.len()
            )));
        }
        for (index, (value_name, value)) in values.iter().enumerate() {
            let name = self.fields[index].name();
            if name != value_name {
                return Err(Error::Process(format!(
                    "Avro record field order mismatch: schema field '{}' vs value field '{}'",
                    name, value_name
                )));
            }
            let leaf_schema = &self.leaf_schemas[index];
            if self.fields[index].is_nullable() {
                // Nullable ⇔ the writer field was a [null, T] union: the
                // decoded value must be a union value (bare values keep the
                // single-row path's fail-loud error).
                match value {
                    AvroValue::Union(_, inner) => {
                        if matches!(inner.as_ref(), AvroValue::Null) {
                            self.columns[index].append_null();
                        } else {
                            self.columns[index].push_leaf(name, leaf_schema, inner)?;
                        }
                    }
                    _ => {
                        return Err(Error::Process(format!(
                            "Avro field '{}' does not hold a union value but its schema is a union",
                            name
                        )));
                    }
                }
            } else {
                match value {
                    AvroValue::Null => {
                        return Err(Error::Process(format!(
                            "Avro field '{}' is null but its schema is not nullable",
                            name
                        )));
                    }
                    other => self.columns[index].push_leaf(name, leaf_schema, other)?,
                }
            }
        }
        self.rows += 1;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<RecordBatch, Error> {
        let schema = Arc::new(Schema::new(self.fields.clone()));
        let columns: Vec<ArrayRef> = self.columns.iter_mut().map(Column::finish).collect();
        RecordBatch::try_new(schema, columns)
            .map_err(|e| Error::Process(format!("Creating an Arrow record batch failed: {}", e)))
    }

    pub fn len(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::Value;
    use apache_avro::writer::datum::GenericDatumWriter;
    use apache_avro::Decimal;

    /// Push one raw value through the accumulator (no encoder round trip) —
    /// the replacement for the removed single-row `avro_value_to_arrow`.
    fn push_value(schema: &AvroSchema, value: &Value) -> Result<RecordBatch, Error> {
        let mut accumulator = AvroArrowAccumulator::new(schema)?;
        accumulator.push(value)?;
        accumulator.finish()
    }

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
        let err = push_value(&schema, &value).unwrap_err();
        assert!(err.to_string().contains("not nullable"), "{err}");
    }

    #[test]
    fn unsupported_leaf_kinds_are_rejected() {
        // An array leaf reaches leaf_to_arrow's unsupported arm.
        let schema = parse(
            r#"{"type": "record", "name": "R", "fields": [{"name": "x", "type": {"type": "array", "items": "int"}}]}"#,
        );
        let value = Value::Record(vec![("x".into(), Value::Array(vec![Value::Int(1)]))]);
        let err = push_value(&schema, &value).unwrap_err();
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
        let err = push_value(&schema, &value).unwrap_err();
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
        let batch =
            push_value(&schema, &value).unwrap_or_else(|e| panic!("leaf mapping failed: {e}"));
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
        let err = push_value(&schema, &value).unwrap_err();
        assert!(err.to_string().contains("Unsupported Avro type"), "{err}");
    }

    #[test]
    fn union_schema_with_non_union_value_is_rejected() {
        let schema = parse(
            r#"{"type": "record", "name": "R", "fields": [{"name": "x", "type": ["null", "int"]}]}"#,
        );
        let value = Value::Record(vec![("x".into(), Value::Int(3))]);
        let err = push_value(&schema, &value).unwrap_err();
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

    #[test]
    fn multi_row_accumulation_matches_per_message_batches() {
        // Same schema, heterogeneous rows (incl. nulls): the accumulated
        // multi-row batch must equal the concat of per-message decodes.
        let schema = parse(
            r#"{
                "type": "record", "name": "R", "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "note", "type": ["null", "string"]},
                    {"name": "score", "type": "double"}
                ]
            }"#,
        );
        let rows = [
            (1i64, Some("a"), 1.5f64),
            (2, None, 2.5),
            (3, Some("c"), 3.5),
        ];
        let payloads: Vec<Vec<u8>> = rows
            .iter()
            .map(|(id, note, score)| {
                encode(
                    &schema,
                    Value::Record(vec![
                        ("id".into(), Value::Long(*id)),
                        (
                            "note".into(),
                            match note {
                                Some(n) => Value::Union(1, Box::new(Value::String(n.to_string()))),
                                None => Value::Union(0, Box::new(Value::Null)),
                            },
                        ),
                        ("score".into(), Value::Double(*score)),
                    ]),
                )
            })
            .collect();

        let mut accumulator = AvroArrowAccumulator::new(&schema).unwrap();
        for payload in &payloads {
            accumulator
                .push(&avro_read_value(&schema, payload).unwrap())
                .unwrap();
        }
        assert_eq!(accumulator.len(), 3);
        let merged = accumulator.finish().unwrap();
        assert_eq!(merged.num_rows(), 3);
        assert_eq!(merged.num_columns(), 3);

        // Columnar equivalence with the per-message path: row content and
        // column order/types match; nullability now consistently reflects
        // the writer schema (the union merge used to lose non-nullable flags
        // on multi-message batches while single-message decodes kept them).
        let per_message: Vec<RecordBatch> = payloads
            .iter()
            .map(|p| avro_to_arrow(&schema, p).unwrap())
            .collect();
        let expected = crate::component::batch_merge::normalize_and_concat(&per_message).unwrap();
        assert_eq!(merged.num_rows(), expected.num_rows());
        for index in 0..merged.num_columns() {
            assert_eq!(
                merged.schema().field(index).data_type(),
                expected.schema().field(index).data_type(),
                "column {index} type differs"
            );
            assert_eq!(
                merged.column(index).to_data(),
                expected.column(index).to_data(),
                "column {index} differs"
            );
        }
        assert!(
            !merged.schema().field(0).is_nullable(),
            "id is non-nullable"
        );
        assert!(merged.schema().field(1).is_nullable(), "note is nullable");

        use datafusion::arrow::array::{Array, AsArray};
        assert!(merged.column(1).as_string::<i32>().is_null(1));
        assert_eq!(merged.column(1).as_string::<i32>().value(0), "a");
        assert_eq!(merged.column(1).as_string::<i32>().value(2), "c");
    }
}
