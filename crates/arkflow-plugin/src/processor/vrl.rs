use std::collections::BTreeMap;
use std::sync::Arc;

use arkflow_core::{
    component::{register_processor_metadata, ComponentMetadata},
    processor::{register_processor_builder, Processor, ProcessorBuilder},
    Error, MessageBatch, MessageBatchRef, ProcessResult, Resource,
};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema, TimeUnit};
use datafusion::arrow::{array::*, datatypes::DataType};
use datafusion::parquet::data_type::AsBytes;
use duckdb::arrow::datatypes::FieldRef;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{error, warn};
use vrl::prelude::ObjectMap;
use vrl::{
    compiler::{self, Program, TargetValue, TimeZone},
    prelude::{state::RuntimeState, Context},
    stdlib,
    value::{Secrets, Value as VrlValue},
};

pub fn init() -> Result<(), Error> {
    register_processor_builder("vrl", Arc::new(VrlProcessorBuilder))?;
    register_processor_metadata(ComponentMetadata::with_schema(
        "vrl",
        "Runs a Vector Remap Language (VRL) program against each batch for safe transformation and enrichment.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "statement": {"type": "string", "description": "VRL program source."},
                "timezone": {"type": "string", "description": "Optional timezone for VRL timestamp operations (e.g. 'Asia/Shanghai', 'UTC', 'local'). Defaults to the platform local timezone; invalid values fall back to the default with a warning."}
            },
            "required": ["statement"]
        }),
    )
    .with_example(serde_json::json!({
        "statement": ".message = parse_json!(.message)\n.timestamp = now()"
    })))?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VrlProcessorConfig {
    statement: String,
    #[serde(default)]
    timezone: Option<String>,
}

struct VrlProcessor {
    program: Program,
    timezone: TimeZone,
}

#[async_trait]
impl Processor for VrlProcessor {
    async fn process(&self, msg_batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        let result = message_batch_to_vrl_values((*msg_batch).clone())?;

        let mut state = RuntimeState::default();
        let timezone = self.timezone;
        let mut output: Vec<Vec<VrlValue>> = Vec::with_capacity(result.len());

        for x in result {
            let mut target = TargetValue {
                value: x,
                // the metadata is empty
                metadata: VrlValue::Object(BTreeMap::new()),
                // and there are no secrets associated with the target
                secrets: Secrets::default(),
            };

            let mut ctx = Context::new(&mut target, &mut state, &timezone);
            // D2: surface runtime errors instead of silently dropping the batch.
            let v = self.program.resolve(&mut ctx).map_err(|e| {
                error!("VRL statement evaluation failed: {:?}", e);
                Error::Process(format!("VRL statement evaluation failed: {:?}", e))
            })?;
            match v {
                VrlValue::Array(vv) => output.push(vv),
                _ => output.push(vec![v]),
            }
        }

        let batches = output
            .into_iter()
            .map(vrl_values_to_message_batch)
            .collect::<Result<Vec<MessageBatch>, Error>>()?;

        // Convert to ProcessResult
        if batches.is_empty() {
            Ok(ProcessResult::None)
        } else if batches.len() == 1 {
            Ok(ProcessResult::Single(Arc::new(
                batches.into_iter().next().unwrap(),
            )))
        } else {
            Ok(ProcessResult::Multiple(
                batches.into_iter().map(Arc::new).collect(),
            ))
        }
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

struct VrlProcessorBuilder;
impl ProcessorBuilder for VrlProcessorBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "VRL processor configuration is missing".to_string(),
            ));
        }
        let vrl_config: VrlProcessorConfig = serde_json::from_value(config.clone().unwrap())?;

        // D5: optional timezone; fall back to the default on invalid input.
        let timezone = match vrl_config.timezone.as_deref() {
            Some(tz) => match TimeZone::parse(tz) {
                Some(t) => t,
                None => {
                    warn!(
                        "Invalid VRL timezone '{}'; falling back to the default timezone.",
                        tz
                    );
                    TimeZone::default()
                }
            },
            None => TimeZone::default(),
        };

        let fns = stdlib::all();
        let result = compiler::compile(&vrl_config.statement, &fns)
            .map_err(|e| Error::Config(format!("Failed to compile VRL statement: {:?}", e)))?;

        Ok(Arc::new(VrlProcessor {
            program: result.program,
            timezone,
        }))
    }
}

fn downcast_failure(
    name: &str,
    data_type: &datafusion::arrow::datatypes::DataType,
    ty: &str,
) -> Error {
    Error::Process(format!(
        "VRL processor: column `{}` declared as {} but failed to downcast to {}",
        name, data_type, ty
    ))
}

fn message_batch_to_vrl_values(message_batch: MessageBatch) -> Result<Vec<VrlValue>, Error> {
    let rows = message_batch.num_rows();
    let num_columns = message_batch.num_columns();
    let schema = message_batch.schema();

    // Pre-allocate with capacity to reduce reallocations
    let mut vrl_values: Vec<ObjectMap> = Vec::with_capacity(rows);
    for _ in 0..rows {
        // Pre-allocate BTreeMap with expected column count
        // BTreeMap starts with a small capacity, so we specify capacity upfront
        vrl_values.push(BTreeMap::new());
    }

    for i in 0..num_columns {
        let column = message_batch.column(i);
        let name = schema.field(i).name();
        match column.data_type() {
            DataType::Utf8 => {
                let col = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        downcast_failure(name, column.data_type(), stringify!(StringArray))
                    })?;
                for i in 0..rows {
                    let value = col.value(i);
                    let vrl_value = VrlValue::from(value.to_owned());
                    insert(i, &mut vrl_values, name, vrl_value)
                }
            }
            DataType::Binary => {
                let col = column
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        downcast_failure(name, column.data_type(), stringify!(BinaryArray))
                    })?;
                for i in 0..rows {
                    let value = col.value(i);
                    let vrl_value = VrlValue::Bytes(value.to_vec().into());
                    insert(i, &mut vrl_values, name, vrl_value)
                }
            }
            DataType::Boolean => {
                let col = column
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        downcast_failure(name, column.data_type(), stringify!(BooleanArray))
                    })?;
                for i in 0..rows {
                    let value = col.value(i);
                    let vrl_value = VrlValue::Boolean(value);
                    insert(i, &mut vrl_values, name, vrl_value)
                }
            }
            DataType::Float64 => {
                let col = column
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        downcast_failure(name, column.data_type(), stringify!(Float64Array))
                    })?;
                for i in 0..rows {
                    let value = col.value(i);
                    let vrl_value = VrlValue::Float(value.try_into().unwrap_or_default());
                    insert(i, &mut vrl_values, name, vrl_value)
                }
            }
            DataType::Float32 => {
                let col = column
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| {
                        downcast_failure(name, column.data_type(), stringify!(Float32Array))
                    })?;
                for i in 0..rows {
                    let value = col.value(i);
                    let vrl_value = VrlValue::Float((value as f64).try_into().unwrap_or_default());
                    insert(i, &mut vrl_values, name, vrl_value)
                }
            }
            DataType::Int64 => {
                let col = column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        downcast_failure(name, column.data_type(), stringify!(Int64Array))
                    })?;
                for i in 0..rows {
                    let value = col.value(i);
                    let vrl_value = VrlValue::Integer(value);
                    insert(i, &mut vrl_values, name, vrl_value)
                }
            }
            DataType::Int32 => {
                let col = column
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| {
                        downcast_failure(name, column.data_type(), stringify!(Int32Array))
                    })?;
                for i in 0..rows {
                    let value = col.value(i);
                    let vrl_value = VrlValue::Integer(value as i64);
                    insert(i, &mut vrl_values, name, vrl_value)
                }
            }
            DataType::Int16 => {
                let col = column
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| {
                        downcast_failure(name, column.data_type(), stringify!(Int16Array))
                    })?;
                for i in 0..rows {
                    let value = col.value(i);
                    let vrl_value = VrlValue::Integer(value as i64);
                    insert(i, &mut vrl_values, name, vrl_value)
                }
            }
            DataType::Int8 => {
                let col = column.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                    downcast_failure(name, column.data_type(), stringify!(Int8Array))
                })?;
                for i in 0..rows {
                    let value = col.value(i);
                    let vrl_value = VrlValue::Integer(value as i64);
                    insert(i, &mut vrl_values, name, vrl_value)
                }
            }
            DataType::UInt64 => {
                let col = column
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| {
                        downcast_failure(name, column.data_type(), stringify!(UInt64Array))
                    })?;
                for i in 0..rows {
                    let value = col.value(i);
                    // VRL integers are i64: values beyond i64::MAX must be a
                    // loud error, never a silent negative wraparound.
                    let vrl_value = VrlValue::Integer(i64::try_from(value).map_err(|_| {
                        Error::Process(format!(
                            "VRL processor: column `{}` has value {} which exceeds i64::MAX (VRL integers are i64); remove or cast the column first, e.g. with filter_columns or a SQL processor",
                            name, value
                        ))
                    })?);
                    insert(i, &mut vrl_values, name, vrl_value)
                }
            }
            DataType::UInt32 => {
                let col = column
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| {
                        downcast_failure(name, column.data_type(), stringify!(UInt32Array))
                    })?;
                for i in 0..rows {
                    let value = col.value(i);
                    let vrl_value = VrlValue::Integer(value as i64);
                    insert(i, &mut vrl_values, name, vrl_value)
                }
            }
            DataType::UInt16 => {
                let col = column
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .ok_or_else(|| {
                        downcast_failure(name, column.data_type(), stringify!(UInt16Array))
                    })?;
                for i in 0..rows {
                    let value = col.value(i);
                    let vrl_value = VrlValue::Integer(value as i64);
                    insert(i, &mut vrl_values, name, vrl_value)
                }
            }
            DataType::UInt8 => {
                let col = column
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .ok_or_else(|| {
                        downcast_failure(name, column.data_type(), stringify!(UInt8Array))
                    })?;
                for i in 0..rows {
                    let value = col.value(i);
                    let vrl_value = VrlValue::Integer(value as i64);
                    insert(i, &mut vrl_values, name, vrl_value)
                }
            }
            DataType::Date32 => {
                let col = column
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| {
                        downcast_failure(name, column.data_type(), stringify!(Date32Array))
                    })?;
                for i in 0..rows {
                    let value = col.value(i);
                    let vrl_value = VrlValue::Integer(value as i64);
                    insert(i, &mut vrl_values, name, vrl_value)
                }
            }
            DataType::Date64 => {
                let col = column
                    .as_any()
                    .downcast_ref::<Date64Array>()
                    .ok_or_else(|| {
                        downcast_failure(name, column.data_type(), stringify!(Date64Array))
                    })?;
                for i in 0..rows {
                    let value = col.value(i);
                    let vrl_value = VrlValue::Integer(value);
                    insert(i, &mut vrl_values, name, vrl_value)
                }
            }
            DataType::Null => {
                // Handle null values
                for i in 0..rows {
                    let vrl_value = VrlValue::Null;
                    insert(i, &mut vrl_values, name, vrl_value)
                }
            }
            DataType::Timestamp(unit, _tz) => {
                // D3: honor the Arrow time unit instead of only Nanosecond.
                for i in 0..rows {
                    let dt = match unit {
                        TimeUnit::Second => column
                            .as_any()
                            .downcast_ref::<TimestampSecondArray>()
                            .and_then(|c| c.value_as_datetime(i)),
                        TimeUnit::Millisecond => column
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .and_then(|c| c.value_as_datetime(i)),
                        TimeUnit::Microsecond => column
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .and_then(|c| c.value_as_datetime(i)),
                        TimeUnit::Nanosecond => column
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .and_then(|c| c.value_as_datetime(i)),
                    };
                    match dt {
                        // D3: a missing/unconvertible timestamp is Null, not epoch.
                        Some(value) => {
                            let vrl_value = VrlValue::Timestamp(value.and_utc());
                            insert(i, &mut vrl_values, name, vrl_value);
                        }
                        None => insert(i, &mut vrl_values, name, VrlValue::Null),
                    }
                }
            }

            other => {
                // Silently nulling an unsupported column is data loss nobody
                // sees: fail loudly and tell the user how to project it away.
                return Err(Error::Process(format!(
                    "VRL processor: column `{}` has unsupported type {} (supported: booleans, integers, floats, utf8/binary, dates, timestamps, null); remove it first, e.g. with filter_columns or a SQL processor",
                    name, other
                )));
            }
        };
    }
    Ok(vrl_values.into_iter().map(|v| v.into()).collect())
}

fn vrl_values_to_message_batch(mut vrl_values: Vec<VrlValue>) -> Result<MessageBatch, Error> {
    let first_value = match vrl_values.first() {
        Some(v) => v,
        None => {
            return Ok(MessageBatch::from(RecordBatch::new_empty(Arc::new(
                Schema::empty(),
            ))));
        }
    };

    // D4: a non-object result (scalar, etc.) is an error, not a silent empty batch.
    let first_obj = match first_value {
        VrlValue::Object(obj) => obj,
        other => {
            return Err(Error::Process(format!(
                "VRL statement must return a row object (or an array of row objects) per row; got {}",
                other.kind_str()
            )));
        }
    };

    let fields = first_obj
        .iter()
        .map(|(k, v)| {
            let field_name = k.to_string();
            match get_arrow_data_type(v, field_name.as_str(), &vrl_values) {
                Ok(data_type) => Ok(FieldRef::new(Field::new(field_name, data_type, true))),
                Err(e) => Err(e),
            }
        })
        .collect::<Result<Vec<FieldRef>, Error>>()?;

    let mut cols: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    for field in fields.iter() {
        let field_name = field.name();
        let data_type = field.data_type();
        let array: ArrayRef = match data_type {
            DataType::Null => Arc::new(NullArray::new(vrl_values.len())),
            DataType::Boolean => {
                let mut cols = Vec::with_capacity(vrl_values.len());
                for vrl_value in vrl_values.iter_mut() {
                    match vrl_value {
                        VrlValue::Object(obj) => {
                            if let Some(VrlValue::Boolean(v)) = obj.remove(field_name.as_str()) {
                                cols.push(Some(v));
                            } else {
                                cols.push(None)
                            }
                        }
                        _ => cols.push(None),
                    }
                }
                Arc::new(BooleanArray::from(cols))
            }

            DataType::Int64 => {
                let mut cols = Vec::with_capacity(vrl_values.len());
                for vrl_value in vrl_values.iter_mut() {
                    match vrl_value {
                        VrlValue::Object(obj) => {
                            if let Some(VrlValue::Integer(v)) = obj.remove(field_name.as_str()) {
                                cols.push(Some(v));
                            } else {
                                cols.push(None)
                            }
                        }
                        _ => cols.push(None),
                    }
                }
                Arc::new(Int64Array::from(cols))
            }

            DataType::Float64 => {
                let mut cols = Vec::with_capacity(vrl_values.len());
                for vrl_value in vrl_values.iter_mut() {
                    match vrl_value {
                        VrlValue::Object(obj) => {
                            if let Some(VrlValue::Float(v)) = obj.remove(field_name.as_str()) {
                                cols.push(Some(v.into_inner()));
                            } else {
                                cols.push(None)
                            }
                        }
                        _ => cols.push(None),
                    }
                }
                Arc::new(Float64Array::from(cols))
            }
            DataType::Timestamp(_, _) => {
                let mut cols = Vec::with_capacity(vrl_values.len());
                for vrl_value in vrl_values.iter_mut() {
                    match vrl_value {
                        VrlValue::Object(obj) => {
                            if let Some(VrlValue::Timestamp(v)) = obj.remove(field_name.as_str()) {
                                cols.push(v.timestamp_nanos_opt());
                            } else {
                                cols.push(None)
                            }
                        }
                        _ => cols.push(None),
                    }
                }
                Arc::new(TimestampNanosecondArray::from(cols))
            }

            // D1: a string column stays a string column.
            DataType::Utf8 => {
                let mut col_vals: Vec<Option<String>> = Vec::with_capacity(vrl_values.len());
                for vrl_value in vrl_values.iter_mut() {
                    match vrl_value {
                        VrlValue::Object(obj) => {
                            if let Some(VrlValue::Bytes(v)) = obj.remove(field_name.as_str()) {
                                match std::str::from_utf8(v.as_bytes()) {
                                    Ok(s) => col_vals.push(Some(s.to_owned())),
                                    Err(_) => col_vals.push(None),
                                }
                            } else {
                                col_vals.push(None);
                            }
                        }
                        _ => col_vals.push(None),
                    }
                }
                Arc::new(StringArray::from(col_vals))
            }

            DataType::Binary => {
                let mut cols = Vec::with_capacity(vrl_values.len());
                for vrl_value in vrl_values.iter_mut() {
                    match vrl_value {
                        VrlValue::Object(obj) => {
                            if let Some(VrlValue::Bytes(v)) = obj.get(field_name.as_str()) {
                                cols.push(Some(v.as_bytes()));
                            } else {
                                cols.push(None)
                            }
                        }
                        _ => cols.push(None),
                    }
                }
                Arc::new(BinaryArray::from(cols))
            }

            _ => {
                return Err(Error::Config(format!(
                    "Unsupported data type: {:?}",
                    data_type
                )));
            }
        };
        cols.push(array);
    }
    let result = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)
        .map_err(|e| Error::Process(format!("Creating an Arrow record batch failed: {}", e)))?;

    Ok(MessageBatch::new_arrow(result))
}

fn insert(i: usize, vrl_values: &mut [ObjectMap], name: &str, val: VrlValue) {
    if let Some(obj) = vrl_values.get_mut(i) {
        obj.insert(name.to_string().into(), val);
    }
}

fn get_arrow_data_type(val: &VrlValue, field: &str, all: &[VrlValue]) -> Result<DataType, Error> {
    match val {
        // D1: emit Utf8 when every value in this column is valid UTF-8, else Binary.
        VrlValue::Bytes(_) => {
            let mut all_utf8 = true;
            for v in all {
                let fv = match v {
                    VrlValue::Object(o) => o.get(field),
                    _ => None,
                };
                if let Some(VrlValue::Bytes(b)) = fv {
                    if std::str::from_utf8(b.as_bytes()).is_err() {
                        all_utf8 = false;
                        break;
                    }
                }
            }
            if all_utf8 {
                Ok(DataType::Utf8)
            } else {
                Ok(DataType::Binary)
            }
        }
        VrlValue::Integer(_) => Ok(DataType::Int64),
        VrlValue::Float(_) => Ok(DataType::Float64),
        VrlValue::Boolean(_) => Ok(DataType::Boolean),
        VrlValue::Timestamp(_) => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        VrlValue::Null => Ok(DataType::Null),
        VrlValue::Object(_) | VrlValue::Array(_) => Err(Error::Process(format!(
            "VRL returned a nested {} for field '{}'; nested objects/arrays are not supported as column values",
            val.kind_str(),
            field
        ))),
        _ => Err(Error::Process(format!(
            "VRL returned an unsupported value type ({}) for field '{}'",
            val.kind_str(),
            field
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::record_batch::RecordBatch;
    use serde_json::json;
    use std::cell::RefCell;

    fn test_resource() -> Resource {
        Resource {
            temporary: Default::default(),
            input_names: RefCell::new(Default::default()),
        }
    }

    fn build_processor(statement: &str) -> Result<Arc<dyn Processor>, Error> {
        let config = Some(json!({ "statement": statement }));
        VrlProcessorBuilder.build(None, &config, &test_resource())
    }

    fn build_processor_with_timezone(
        statement: &str,
        timezone: &str,
    ) -> Result<Arc<dyn Processor>, Error> {
        let config = Some(json!({ "statement": statement, "timezone": timezone }));
        VrlProcessorBuilder.build(None, &config, &test_resource())
    }

    #[tokio::test]
    async fn test_string_roundtrip_stays_utf8() -> Result<(), Error> {
        let processor = build_processor(".")?;
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let arr = Arc::new(StringArray::from(vec![Some("alice")]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?;
        match result {
            ProcessResult::Single(b) => {
                assert_eq!(
                    b.column(0).data_type(),
                    &DataType::Utf8,
                    "string column must stay Utf8 after VRL"
                );
                let col = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8 column");
                assert_eq!(col.value(0), "alice");
            }
            _ => panic!("expected single result"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_binary_stays_binary() -> Result<(), Error> {
        let processor = build_processor(".")?;
        let bad: Vec<u8> = vec![0xFF, 0xFE, 0xFD];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::Binary,
            true,
        )]));
        let arr = Arc::new(BinaryArray::from(vec![Some(bad.as_slice())]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?;
        match result {
            ProcessResult::Single(b) => {
                assert_eq!(
                    b.column(0).data_type(),
                    &DataType::Binary,
                    "non-UTF-8 bytes must stay Binary"
                );
            }
            _ => panic!("expected single result"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_integer_roundtrip() -> Result<(), Error> {
        let processor = build_processor(".")?;
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let arr = Arc::new(Int64Array::from(vec![Some(1)]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?;
        match result {
            ProcessResult::Single(b) => {
                assert_eq!(b.column(0).data_type(), &DataType::Int64);
            }
            _ => panic!("expected single result"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_runtime_error_returns_err() -> Result<(), Error> {
        // parse_json! is fallible; on bad input the processor must surface an error,
        // not silently drop the batch.
        let processor = build_processor("parse_json!(.message)")?;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "message",
            DataType::Utf8,
            true,
        )]));
        let arr = Arc::new(StringArray::from(vec![Some("not json")]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await;
        assert!(
            result.is_err(),
            "a failing VRL statement must return Err, not silently drop the batch"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_scalar_result_returns_err() -> Result<(), Error> {
        // A scalar result cannot form a row; it must error rather than produce an empty batch.
        let processor = build_processor("1 + 1")?;
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let arr = Arc::new(StringArray::from(vec![Some("alice")]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await;
        assert!(
            result.is_err(),
            "a scalar VRL result must error, not produce an empty batch"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_empty_batch_returns_none() -> Result<(), Error> {
        let processor = build_processor(".")?;
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let arr = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?;
        assert!(matches!(result, ProcessResult::None));
        Ok(())
    }

    #[tokio::test]
    async fn test_timestamp_second_not_dropped() -> Result<(), Error> {
        // D3: a Second-precision timestamp column must reach the VRL program (not be dropped).
        let processor = build_processor(".")?;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        )]));
        let arr = Arc::new(TimestampSecondArray::from(vec![Some(1_000_000i64)]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?;
        match result {
            ProcessResult::Single(b) => {
                assert!(
                    b.schema().field_with_name("ts").is_ok(),
                    "ts field must survive a Second-precision timestamp round-trip"
                );
            }
            _ => panic!("expected single result"),
        }
        Ok(())
    }

    #[test]
    fn test_compile_error_rejected() {
        let config = Some(json!({ "statement": "this is not valid vrl !!!" }));
        let result = VrlProcessorBuilder.build(None, &config, &test_resource());
        assert!(
            result.is_err(),
            "an invalid VRL statement must be rejected at build time"
        );
    }

    #[test]
    fn test_removed_function_surfaces_compile_diagnostic() {
        // A function that no longer exists in the linked vrl stdlib (e.g.
        // removed upstream by an upgrade — truncate_timestamp was removed
        // by 0.36) must fail through the same Config path as any invalid
        // source, with the VRL diagnostic surfacing to the operator. This
        // pins the delta-spec scenario for upgrade-drift: the program does
        // not build, it does not fail at runtime.
        let err = build_processor("no_such_function_xyz(.message)")
            .err()
            .expect("an undefined function must fail at build time");
        let message = format!("{err}");
        assert!(
            message.contains("Failed to compile VRL statement")
                && message.contains("undefined function"),
            "must surface the VRL compiler diagnostic naming the function, got: {message}"
        );
    }

    #[test]
    fn test_timezone_config_accepted() {
        let config = Some(json!({ "statement": ".x = 1", "timezone": "Asia/Shanghai" }));
        let result = VrlProcessorBuilder.build(None, &config, &test_resource());
        assert!(
            result.is_ok(),
            "a valid timezone config should build successfully"
        );
    }

    #[test]
    fn test_invalid_timezone_falls_back() {
        let config = Some(json!({ "statement": ".x = 1", "timezone": "Not/A_Real_Zone" }));
        let result = VrlProcessorBuilder.build(None, &config, &test_resource());
        assert!(
            result.is_ok(),
            "an invalid timezone should fall back to the default, not fail configuration"
        );
    }

    #[tokio::test]
    async fn test_uint64_overflow_errors_loudly() -> Result<(), Error> {
        let processor = build_processor(".")?;
        let schema = Arc::new(Schema::new(vec![Field::new("big", DataType::UInt64, true)]));
        let arr = Arc::new(UInt64Array::from(vec![Some(u64::MAX)]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        let err = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("`big`") && msg.contains("i64::MAX"),
            "error must name the column and the limit, got: {msg}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_uint64_in_range_processes_normally() -> Result<(), Error> {
        let processor = build_processor(".")?;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::UInt64, true)]));
        let arr = Arc::new(UInt64Array::from(vec![Some(i64::MAX as u64)]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?;
        match result {
            ProcessResult::Single(b) => assert_eq!(b.len(), 1),
            _ => panic!("expected single result"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_unsupported_list_column_errors_instead_of_null() -> Result<(), Error> {
        let processor = build_processor(".")?;
        let list_field = Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        );
        let schema = Arc::new(Schema::new(vec![list_field]));
        let values = StringArray::from(vec![Some("a"), None, Some("b")]);
        let list_arr = Arc::new(
            ListArray::try_new(
                Arc::new(Field::new("item", DataType::Utf8, true)),
                datafusion::arrow::buffer::OffsetBuffer::from_lengths([1, 1, 1]),
                Arc::new(values),
                None,
            )
            .map_err(|e| Error::Process(e.to_string()))?,
        );
        let rb = RecordBatch::try_new(schema, vec![list_arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        let err = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("`tags`") && msg.contains("filter_columns"),
            "error must name the column and the workaround, got: {msg}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_numeric_boolean_date_and_timestamp_columns_roundtrip() -> Result<(), Error> {
        let processor = build_processor(".")?;
        use datafusion::arrow::array::{
            BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int16Array,
            Int8Array, Int32Array, TimestampMillisecondArray, UInt16Array, UInt32Array,
            UInt64Array, UInt8Array,
        };
        let fields = vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("i32", DataType::Int32, true),
            Field::new("i16", DataType::Int16, true),
            Field::new("i8", DataType::Int8, true),
            Field::new("u64", DataType::UInt64, true),
            Field::new("u32", DataType::UInt32, true),
            Field::new("u16", DataType::UInt16, true),
            Field::new("u8", DataType::UInt8, true),
            Field::new("d32", DataType::Date32, true),
            Field::new("d64", DataType::Date64, true),
            Field::new("null", DataType::Null, true),
            Field::new(
                "ts",
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ),
        ];
        let schema = Arc::new(Schema::new(fields));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(BooleanArray::from(vec![Some(true)])),
            Arc::new(Float64Array::from(vec![Some(1.5)])),
            Arc::new(Float32Array::from(vec![Some(2.5f32)])),
            Arc::new(Int32Array::from(vec![Some(-7)])),
            Arc::new(Int16Array::from(vec![Some(-8)])),
            Arc::new(Int8Array::from(vec![Some(-9)])),
            Arc::new(UInt64Array::from(vec![Some(u64::MAX / 2)])),
            Arc::new(UInt32Array::from(vec![Some(7u32)])),
            Arc::new(UInt16Array::from(vec![Some(8u16)])),
            Arc::new(UInt8Array::from(vec![Some(9u8)])),
            Arc::new(Date32Array::from(vec![Some(19_000)])),
            Arc::new(Date64Array::from(vec![Some(1_700_000_000_000)])),
            Arc::new(datafusion::arrow::array::NullArray::new(1)),
            Arc::new(TimestampMillisecondArray::from(vec![Some(1_700_000_000_000)])),
        ];
        let rb = RecordBatch::try_new(schema, columns)
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?;
        match result {
            ProcessResult::Single(b) => {
                assert_eq!(b.num_rows(), 1);
                // Booleans stay booleans through the VRL value model.
                assert_eq!(b.column(0).data_type(), &DataType::Boolean);
                // Timestamps travel through the VRL datetime model; the
                // column survives (as text) rather than being dropped.
                assert!(b.num_columns() >= 14);
            }
            _ => panic!("expected single result"),
        }
        Ok(())
    }

    #[test]
    fn builder_rejects_missing_config_and_bad_statement() {
        let Err(missing) = VrlProcessorBuilder.build(None, &None, &test_resource()) else {
            panic!("missing config must be rejected");
        };
        assert!(missing.to_string().contains("configuration is missing"));

        let Err(bad) = VrlProcessorBuilder.build(
            None,
            &Some(json!({ "statement": "this is ((( not vrl" })),
            &test_resource(),
        ) else {
            panic!("an uncompilable statement must be rejected");
        };
        assert!(bad.to_string().contains("compile"), "{bad}");
    }

    #[test]
    fn timezone_configuration_accepts_valid_and_falls_back_on_invalid() {
        let valid = VrlProcessorBuilder
            .build(
                None,
                &Some(json!({ "statement": ".", "timezone": "Asia/Shanghai" })),
                &test_resource(),
            )
            .unwrap();
        drop(valid);
        // An invalid timezone logs a warning and falls back to the default.
        let fallback = VrlProcessorBuilder
            .build(
                None,
                &Some(json!({ "statement": ".", "timezone": "Not/A_Zone" })),
                &test_resource(),
            )
            .unwrap();
        drop(fallback);
    }

    #[tokio::test]
    async fn empty_input_batch_produces_process_result_none() -> Result<(), Error> {
        let processor = build_processor(".")?;
        use datafusion::arrow::array::StringArray;
        use datafusion::arrow::datatypes::Schema;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
        let arr = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        match processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?
        {
            ProcessResult::None => Ok(()),
            _ => panic!("an empty batch must produce ProcessResult::None"),
        }
    }

    #[tokio::test]
    async fn array_output_splits_into_rows_and_multiple_batches() -> Result<(), Error> {
        // One input row whose statement yields an array of two objects:
        // the output becomes two rows (single batch).
        let processor = build_processor("[{ \"v\": .v }, { \"v\": .v }]")?;
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::Schema;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let arr = Arc::new(Int64Array::from(vec![Some(21)]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        match processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?
        {
            ProcessResult::Single(b) => assert_eq!(b.num_rows(), 2),
            other => panic!("expected single split batch, got {other:?}"),
        }

        // Two input rows each keeping their own object: Multiple batches.
        let processor = build_processor(".")?;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let arr = Arc::new(Int64Array::from(vec![Some(1), Some(2)]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        match processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?
        {
            ProcessResult::Multiple(batches) => assert_eq!(batches.len(), 2),
            other => panic!("expected multiple batches, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_uint64_beyond_i64_max_fails_loud() -> Result<(), Error> {
        let processor = build_processor(".")?;
        use datafusion::arrow::array::UInt64Array;
        let schema = Arc::new(Schema::new(vec![Field::new("big", DataType::UInt64, true)]));
        let arr = Arc::new(UInt64Array::from(vec![Some(u64::MAX)]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        let err = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await
            .unwrap_err();
        let message = format!("{err}");
        assert!(
            message.contains("exceeds i64::MAX"),
            "overflow must fail loudly, got: {message}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_timezone_changes_evaluation_result() -> Result<(), Error> {
        // The configured timezone must flow into the evaluation context.
        // get_timezone_name() reads it directly, so a broken TimeZone
        // plumbing (e.g. a vrl upgrade silently freezing every program on
        // the builder default) fails here while all other tests stay
        // green.
        async fn evaluate_tz_name(tz: &str) -> Result<String, Error> {
            let processor =
                build_processor_with_timezone(".tz, err = get_timezone_name()\n.", tz)?;
            let schema = Arc::new(Schema::new(vec![Field::new("message", DataType::Utf8, true)]));
            let arr = Arc::new(StringArray::from(vec![Some("hello")]));
            let rb = RecordBatch::try_new(schema, vec![arr])
                .map_err(|e| Error::Process(format!("arrow: {e}")))?;
            match processor
                .process(Arc::new(MessageBatch::new_arrow(rb)))
                .await?
            {
                ProcessResult::Single(b) => {
                    let col = b
                        .column_by_name("tz")
                        .expect("timezone name column")
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("Utf8 timezone name");
                    Ok(col.value(0).to_string())
                }
                _ => panic!("expected single result"),
            }
        }

        assert_eq!(evaluate_tz_name("UTC").await?, "UTC");
        assert_eq!(evaluate_tz_name("Asia/Shanghai").await?, "Asia/Shanghai");
        Ok(())
    }

    #[tokio::test]
    async fn test_source_meta_columns_survive_passthrough() -> Result<(), Error> {
        // Real batches carry __meta_ columns; the VRL processor must not
        // drop or rename them. VRL has no unsigned integer concept, so the
        // pinned contract is: names and values survive; integer metas come
        // back as Int64 (downstream consumers already cast
        // __meta_partition back to UInt32 where needed).
        let processor = build_processor(".")?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("__meta_source", DataType::Utf8, true),
            Field::new("__meta_partition", DataType::UInt32, true),
            Field::new("__meta_offset", DataType::Int64, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let rb = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("kafka-a")])),
                Arc::new(UInt32Array::from(vec![Some(3u32)])),
                Arc::new(Int64Array::from(vec![Some(42i64)])),
                Arc::new(StringArray::from(vec![Some("hello")])),
            ],
        )
        .map_err(|e| Error::Process(format!("arrow: {e}")))?;

        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?;
        let ProcessResult::Single(b) = result else {
            panic!("expected single result")
        };
        let source = b
            .column_by_name("__meta_source")
            .expect("__meta_source survives VRL")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 __meta_source");
        assert_eq!(source.value(0), "kafka-a");
        let partition = b
            .column_by_name("__meta_partition")
            .expect("__meta_partition survives VRL")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("VRL integers are Int64 on output");
        assert_eq!(partition.value(0), 3);
        let offset = b
            .column_by_name("__meta_offset")
            .expect("__meta_offset survives VRL")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 __meta_offset");
        assert_eq!(offset.value(0), 42);
        Ok(())
    }

    #[tokio::test]
    async fn test_nested_object_field_errors_loudly() -> Result<(), Error> {
        // The spec pins loud failure for non-row top-level results; the
        // same guarantee must hold for a nested object produced as a FIELD
        // value — it must not silently coerce to a string or drop rows.
        let processor = build_processor(".geo = {\"city\": \"shanghai\"}\n.")?;
        let schema = Arc::new(Schema::new(vec![Field::new("message", DataType::Utf8, true)]));
        let arr = Arc::new(StringArray::from(vec![Some("hello")]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        let err = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await
            .expect_err("nested object field must fail loudly");
        let message = format!("{err}");
        assert!(
            message.contains("nested") && message.contains("geo"),
            "error must name the nested field, got: {message}"
        );
        Ok(())
    }
}
