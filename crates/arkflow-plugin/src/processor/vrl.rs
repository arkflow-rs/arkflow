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
        let result = message_batch_to_vrl_values(&msg_batch)?;

        let mut state = RuntimeState::default();
        let timezone = self.timezone;
        // Every program result row across the whole input batch lands in ONE
        // output vector: one input batch → one output batch (see the
        // `vrl-processor` spec's batch-granularity requirement). A row whose
        // program returns an array contributes its elements as consecutive
        // rows, in input order.
        let mut output: Vec<VrlValue> = Vec::with_capacity(result.len());

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
                VrlValue::Array(vv) => {
                    for item in vv {
                        output.push(require_row_object(item)?);
                    }
                }
                _ => output.push(require_row_object(v)?),
            }
        }

        if output.is_empty() {
            return Ok(ProcessResult::None);
        }
        let batch = vrl_values_to_message_batch(output)?;
        Ok(ProcessResult::Single(Arc::new(batch)))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

struct VrlProcessorBuilder;
impl ProcessorBuilder for VrlProcessorBuilder {
    fn build(
        &self,
        _name: Option<&str>,
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

/// Every program result row must be an object. Rows are merged into one
/// output batch, so this is checked per row up front: a non-object result in
/// the middle of a batch must fail loudly (D4) rather than decay into an
/// all-null row after the first row has fixed the field set.
fn require_row_object(v: VrlValue) -> Result<VrlValue, Error> {
    match v {
        VrlValue::Object(_) => Ok(v),
        other => Err(Error::Process(format!(
            "VRL statement must return a row object (or an array of row objects) per row; got {}",
            other.kind_str()
        ))),
    }
}

fn message_batch_to_vrl_values(message_batch: &MessageBatch) -> Result<Vec<VrlValue>, Error> {
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
    if !matches!(first_value, VrlValue::Object(_)) {
        return Err(Error::Process(format!(
            "VRL statement must return a row object (or an array of row objects) per row; got {}",
            first_value.kind_str()
        )));
    }

    // Field set = union across every row in first-seen order; a field's
    // column type comes from its first non-null value (nulls carry no type),
    // so heterogeneous rows merge into one schema with nulls for the gaps
    // instead of dropping fields that the first row happened not to have.
    //
    // A field whose non-null values differ in kind across rows cannot share a
    // column. Before batches were merged such a field simply produced one
    // batch per row (each with its own type); merging would silently null the
    // minority values, so it fails loudly instead (D1/D4: no silent data
    // loss). The fix is in the program: make the field's type consistent.
    let mut field_order: Vec<String> = Vec::new();
    let mut typing_value: BTreeMap<String, Option<(&VrlValue, usize)>> = BTreeMap::new();
    for (row_index, row) in vrl_values.iter().enumerate() {
        let VrlValue::Object(obj) = row else {
            continue;
        };
        for (k, v) in obj {
            let name = k.as_str();
            let incoming = (!matches!(v, VrlValue::Null)).then_some((v, row_index));
            match typing_value.get_mut(name) {
                None => {
                    field_order.push(name.to_string());
                    typing_value.insert(name.to_string(), incoming);
                }
                Some(slot) => {
                    // `Option<(&VrlValue, usize)>` is Copy: read the current
                    // typing value without holding a borrow on the slot.
                    if let Some((prev, prev_row)) = *slot {
                        if let Some((v, row_index)) = incoming {
                            if prev.kind_str() != v.kind_str() {
                                return Err(Error::Process(format!(
                                    "VRL processor: field `{}` has conflicting types across result rows ({} at result row {}, {} at result row {}; rows are 0-based over the merged output, so array fan-out shifts them off the input rows); a merged batch needs one column type per field — make the field consistent, e.g. with to_float()/to_string()",
                                    name,
                                    prev.kind_str(),
                                    prev_row,
                                    v.kind_str(),
                                    row_index,
                                )));
                            }
                        }
                    } else if incoming.is_some() {
                        *slot = incoming;
                    }
                }
            }
        }
    }

    let fields = field_order
        .iter()
        .map(|field_name| {
            let data_type = match typing_value.get(field_name).copied().flatten() {
                Some((v, _)) => get_arrow_data_type(v, field_name.as_str(), &vrl_values)?,
                None => DataType::Null,
            };
            Ok(FieldRef::new(Field::new(
                field_name.clone(),
                data_type,
                true,
            )))
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
            Int32Array, Int8Array, TimestampMillisecondArray, UInt16Array, UInt32Array,
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
            Arc::new(TimestampMillisecondArray::from(vec![Some(
                1_700_000_000_000,
            )])),
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
    async fn array_output_and_multi_row_input_merge_into_one_batch() -> Result<(), Error> {
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

        // Two input rows each keeping their own object: ONE batch of two
        // rows in input order (batch granularity is preserved end to end;
        // the historical per-row `Multiple` result is gone).
        let processor = build_processor(".")?;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let arr = Arc::new(Int64Array::from(vec![Some(1), Some(2)]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        match processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?
        {
            ProcessResult::Single(b) => {
                assert_eq!(b.num_rows(), 2);
                let v = b
                    .column_by_name("v")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!((v.value(0), v.value(1)), (1, 2), "input order preserved");
            }
            other => panic!("expected one merged batch, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn array_rows_and_object_rows_interleave_into_one_batch() -> Result<(), Error> {
        // Row 1 (v=1) expands to two rows, row 2 (v=2) stays one row: a
        // single 3-row batch, array elements first and in order.
        let processor = build_processor(
            "if .v == 1 { [{ \"v\": .v, \"k\": \"a\" }, { \"v\": .v, \"k\": \"b\" }] } else { { \"v\": .v, \"k\": \"c\" } }",
        )?;
        use datafusion::arrow::array::Int64Array;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let arr = Arc::new(Int64Array::from(vec![Some(1), Some(2)]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        let ProcessResult::Single(b) = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?
        else {
            panic!("expected one merged batch");
        };
        assert_eq!(b.num_rows(), 3);
        let k = b
            .column_by_name("k")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!((k.value(0), k.value(1), k.value(2)), ("a", "b", "c"));
        Ok(())
    }

    #[tokio::test]
    async fn heterogeneous_rows_merge_as_field_union_with_null_gaps() -> Result<(), Error> {
        // Row 1 emits {a}, row 2 emits {b}: the batch has both columns (first
        // seen order a, b) with nulls where a row lacks the field.
        let processor = build_processor("if .v == 1 { { \"a\": \"x\" } } else { { \"b\": 7 } }")?;
        use datafusion::arrow::array::Int64Array;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let arr = Arc::new(Int64Array::from(vec![Some(1), Some(2)]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        let ProcessResult::Single(b) = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?
        else {
            panic!("expected one merged batch");
        };
        assert_eq!(b.num_rows(), 2);
        let schema = b.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["a", "b"], "union in first-seen order");
        let a = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let bcol = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a.value(0), "x");
        assert!(a.is_null(1), "row 2 lacks `a`");
        assert!(bcol.is_null(0), "row 1 lacks `b`");
        assert_eq!(bcol.value(1), 7);
        Ok(())
    }

    #[tokio::test]
    async fn null_first_occurrence_does_not_fix_a_null_column_type() -> Result<(), Error> {
        // Row 1 sets .x = null, row 2 sets .x = 5: the column is typed from
        // the first NON-null value (Int64), not frozen as a Null column
        // that would drop row 2's value.
        let processor = build_processor("if .v == 1 { { \"x\": null } } else { { \"x\": 5 } }")?;
        use datafusion::arrow::array::Int64Array;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let arr = Arc::new(Int64Array::from(vec![Some(1), Some(2)]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        let ProcessResult::Single(b) = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?
        else {
            panic!("expected one merged batch");
        };
        assert_eq!(b.column(0).data_type(), &DataType::Int64);
        let x = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(x.is_null(0));
        assert_eq!(x.value(1), 5);
        Ok(())
    }

    #[tokio::test]
    async fn non_object_result_mid_batch_fails_loudly() -> Result<(), Error> {
        // Row 1 returns an object, row 2 returns a scalar: merging must not
        // turn row 2 into a silent all-null row — the D4 loud failure holds
        // for every row, not only the first.
        let processor = build_processor("if .v == 1 { { \"a\": 1 } } else { 42 }")?;
        use datafusion::arrow::array::Int64Array;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let arr = Arc::new(Int64Array::from(vec![Some(1), Some(2)]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        let err = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await
            .expect_err("scalar row must fail loudly");
        assert!(
            format!("{err}").contains("row object"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn conflicting_field_types_across_rows_fail_loudly() -> Result<(), Error> {
        // Merging batches means one column type per field. Before the merge
        // each row was its own batch, so `x` could be an integer in row 1 and
        // a string in row 2 without loss; after merging, silently nulling one
        // side would be invisible data loss — it must error instead.
        for statement in [
            "if .v == 1 { { \"x\": 1 } } else { { \"x\": \"s\" } }",
            "if .v == 1 { { \"x\": 1 } } else { { \"x\": 1.5 } }",
            "if .v == 1 { { \"x\": true } } else { { \"x\": 1 } }",
        ] {
            let processor = build_processor(statement)?;
            use datafusion::arrow::array::Int64Array;
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
            let arr = Arc::new(Int64Array::from(vec![Some(1), Some(2)]));
            let rb = RecordBatch::try_new(schema, vec![arr])
                .map_err(|e| Error::Process(format!("arrow: {e}")))?;
            let err = processor
                .process(Arc::new(MessageBatch::new_arrow(rb)))
                .await
                .expect_err("conflicting field types must fail loudly");
            let message = format!("{err}");
            assert!(
                message.contains("`x`") && message.contains("conflicting types"),
                "error must name the field and the conflict, got: {message}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn null_on_one_row_does_not_conflict_with_a_typed_field() -> Result<(), Error> {
        // nulls carry no type, so they never conflict: [null, 5] is an Int64
        // column with a null first row, not an error.
        let processor = build_processor("if .v == 1 { { \"x\": null } } else { { \"x\": 5 } }")?;
        use datafusion::arrow::array::Int64Array;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let arr = Arc::new(Int64Array::from(vec![Some(1), Some(2)]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        let ProcessResult::Single(batch) = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?
        else {
            panic!("expected a single merged batch");
        };
        assert_eq!(batch.column(0).data_type(), &DataType::Int64);
        Ok(())
    }

    #[tokio::test]
    async fn statement_yielding_only_empty_arrays_produces_none() -> Result<(), Error> {
        // A program that fans every row out to zero rows emits nothing: with
        // merged batches that is `None` (previously: one empty batch per row).
        let processor = build_processor("[]")?;
        use datafusion::arrow::array::Int64Array;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let arr = Arc::new(Int64Array::from(vec![Some(1), Some(2)]));
        let rb = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| Error::Process(format!("arrow: {e}")))?;
        match processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?
        {
            ProcessResult::None => Ok(()),
            other => panic!("expected None, got {other:?}"),
        }
    }

    /// Ad-hoc release timing (NOT run by CI): merged single-batch output vs
    /// the historical per-input-row batch construction, kept inline as the
    /// oracle (both paths share the row conversion; the oracle builds one
    /// `MessageBatch` per input row). Run with:
    /// `cargo test --release -p arkflow-plugin --lib -- --ignored vrl_merge_timing --nocapture`
    #[test]
    #[ignore]
    fn vrl_merge_timing() {
        use datafusion::arrow::array::{Float64Array, Int64Array};
        let rows = 1_000usize;
        let batches = 200usize;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));
        let rb = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())),
                Arc::new(StringArray::from(
                    (0..rows).map(|i| format!("user_{i}")).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    (0..rows).map(|i| i as f64 * 0.5).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let batch = MessageBatch::new_arrow(rb);

        let time = |name: &str, f: &mut dyn FnMut() -> usize| {
            let mut best = std::time::Duration::MAX;
            let mut out_rows = 0;
            for _ in 0..3 {
                let start = std::time::Instant::now();
                out_rows = f();
                best = best.min(start.elapsed());
            }
            println!(
                "{name}: {out_rows} rows in {best:?} ({:.0} rows/s)",
                out_rows as f64 / best.as_secs_f64()
            );
        };

        // New path: convert once, build ONE batch.
        let mut merged = || {
            let mut n = 0usize;
            for _ in 0..batches {
                let values = message_batch_to_vrl_values(&batch).unwrap();
                n += vrl_values_to_message_batch(values).unwrap().num_rows();
            }
            n
        };
        time("merged single batch", &mut merged);

        // Oracle: the historical shape — clone the input batch, convert, then
        // one MessageBatch per input row.
        let mut per_row = || {
            let mut n = 0usize;
            for _ in 0..batches {
                let values = message_batch_to_vrl_values(&batch.clone()).unwrap();
                for v in values {
                    n += vrl_values_to_message_batch(vec![v]).unwrap().num_rows();
                }
            }
            n
        };
        time("per-row batches (historical)", &mut per_row);
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
            let processor = build_processor_with_timezone(".tz, err = get_timezone_name()\n.", tz)?;
            let schema = Arc::new(Schema::new(vec![Field::new(
                "message",
                DataType::Utf8,
                true,
            )]));
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
        let schema = Arc::new(Schema::new(vec![Field::new(
            "message",
            DataType::Utf8,
            true,
        )]));
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
