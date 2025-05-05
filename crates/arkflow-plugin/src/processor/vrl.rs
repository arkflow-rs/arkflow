use std::collections::BTreeMap;
use std::sync::Arc;

use arkflow_core::{
    processor::{register_processor_builder, Processor, ProcessorBuilder},
    Error, MessageBatch,
};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema, TimeUnit};
use datafusion::arrow::{array::*, datatypes::DataType};
use datafusion::parquet::data_type::AsBytes;
use duckdb::arrow::datatypes::FieldRef;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::error;
use vrl::prelude::ObjectMap;
use vrl::{
    compiler::{self, Program, TargetValue, TimeZone},
    prelude::{state::RuntimeState, Context},
    stdlib,
    value::{Secrets, Value as VrlValue},
};

pub fn init() -> Result<(), Error> {
    register_processor_builder("vrl", Arc::new(VrlProcessorBuilder))?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VrlProcessorConfig {
    pub statement: String,
}

pub struct VrlProcessor {
    #[allow(unused)]
    config: VrlProcessorConfig,

    program: Program,
}

#[async_trait]
impl Processor for VrlProcessor {
    async fn process(&self, msg_batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        let result = message_batch_to_vrl_values(msg_batch);

        let mut state = RuntimeState::default();
        let timezone = TimeZone::default();
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
            if let Ok(v) = self.program.resolve(&mut ctx) {
                match v {
                    VrlValue::Array(vv) => {
                        output.push(vv);
                    }
                    _ => output.push(vec![v]),
                }
            }
        }
        // for x in output {
        //     let batch = vrl_values_to_message_batch(x)?;
        //     batch
        // }
        output
            .into_iter()
            .map(|x| vrl_values_to_message_batch(x))
            .collect::<Result<Vec<MessageBatch>, Error>>()
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

pub(crate) struct VrlProcessorBuilder;
impl ProcessorBuilder for VrlProcessorBuilder {
    fn build(&self, config: &Option<Value>) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "JsonToArrow processor configuration is missing".to_string(),
            ));
        }
        let vrl_config: VrlProcessorConfig = serde_json::from_value(config.clone().unwrap())?;

        let fns = stdlib::all();
        let result = compiler::compile(&vrl_config.statement, &fns)
            .map_err(|e| Error::Config(format!("Failed to compile VRL statement: {:?}", e)))?;

        Ok(Arc::new(VrlProcessor {
            config: vrl_config,
            program: result.program,
        }))
    }
}

pub(crate) fn message_batch_to_vrl_values(message_batch: MessageBatch) -> Vec<VrlValue> {
    let rows = message_batch.num_rows();
    let mut vrl_values: Vec<ObjectMap> = Vec::with_capacity(rows);
    for _ in 0..rows {
        vrl_values.push(BTreeMap::new());
    }

    let schema = message_batch.schema();
    let num_columns = message_batch.num_columns();
    for i in 0..num_columns {
        let column = message_batch.column(i);
        let name = schema.field(i).name();
        match column.data_type() {
            DataType::Utf8 => {
                if let Some(col) = column.as_any().downcast_ref::<StringArray>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::from(value.to_owned());
                        insert(i, &mut vrl_values, name, vrl_value)
                    }
                }
            }
            DataType::Binary => {
                if let Some(col) = column.as_any().downcast_ref::<BinaryArray>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Bytes(value.to_vec().into());
                        insert(i, &mut vrl_values, name, vrl_value)
                    }
                }
            }
            DataType::Boolean => {
                if let Some(col) = column.as_any().downcast_ref::<BooleanArray>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Boolean(value);
                        insert(i, &mut vrl_values, name, vrl_value)
                    }
                }
            }
            DataType::Float64 => {
                if let Some(col) = column.as_any().downcast_ref::<Float64Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Float(value.try_into().unwrap_or_default());
                        insert(i, &mut vrl_values, name, vrl_value)
                    }
                }
            }
            DataType::Float32 => {
                if let Some(col) = column.as_any().downcast_ref::<Float32Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value =
                            VrlValue::Float((value as f64).try_into().unwrap_or_default());
                        insert(i, &mut vrl_values, name, vrl_value)
                    }
                }
            }
            DataType::Int64 => {
                if let Some(col) = column.as_any().downcast_ref::<Int64Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value);
                        insert(i, &mut vrl_values, name, vrl_value)
                    }
                }
            }
            DataType::Int32 => {
                if let Some(col) = column.as_any().downcast_ref::<Int32Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value as i64);
                        insert(i, &mut vrl_values, name, vrl_value)
                    }
                }
            }
            DataType::Int16 => {
                if let Some(col) = column.as_any().downcast_ref::<Int16Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value as i64);
                        insert(i, &mut vrl_values, name, vrl_value)
                    }
                }
            }
            DataType::Int8 => {
                if let Some(col) = column.as_any().downcast_ref::<Int8Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value as i64);
                        insert(i, &mut vrl_values, name, vrl_value)
                    }
                }
            }
            DataType::UInt64 => {
                if let Some(col) = column.as_any().downcast_ref::<UInt64Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value as i64);
                        insert(i, &mut vrl_values, name, vrl_value)
                    }
                }
            }
            DataType::UInt32 => {
                if let Some(col) = column.as_any().downcast_ref::<UInt32Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value as i64);
                        insert(i, &mut vrl_values, name, vrl_value)
                    }
                }
            }
            DataType::UInt16 => {
                if let Some(col) = column.as_any().downcast_ref::<UInt16Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value as i64);
                        insert(i, &mut vrl_values, name, vrl_value)
                    }
                }
            }
            DataType::UInt8 => {
                if let Some(col) = column.as_any().downcast_ref::<UInt8Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value as i64);
                        insert(i, &mut vrl_values, name, vrl_value)
                    }
                }
            }
            DataType::Date32 => {
                if let Some(col) = column.as_any().downcast_ref::<Date32Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value as i64);
                        insert(i, &mut vrl_values, name, vrl_value)
                    }
                }
            }
            DataType::Date64 => {
                if let Some(col) = column.as_any().downcast_ref::<Date64Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value);
                        insert(i, &mut vrl_values, name, vrl_value)
                    }
                }
            }
            DataType::Null => {
                // Handle null values
                for i in 0..rows {
                    let vrl_value = VrlValue::Null;
                    insert(i, &mut vrl_values, name, vrl_value)
                }
            }
            DataType::Timestamp(_unit, _tz) => {
                if let Some(col) = column.as_any().downcast_ref::<TimestampNanosecondArray>() {
                    for i in 0..rows {
                        let value = col.value_as_datetime(i).unwrap_or_default();

                        let vrl_value = VrlValue::Timestamp(value.and_utc());
                        insert(i, &mut vrl_values, name, vrl_value);
                    }
                }
            }

            _ => {
                // Handle unsupported data types
                for i in 0..rows {
                    let vrl_value = VrlValue::Null;
                    insert(i, &mut vrl_values, name, vrl_value)
                }
                error!("Unsupported data type: {:?}", column.data_type());
            }
        };
    }
    vrl_values.into_iter().map(|v| v.into()).collect()
}

pub(crate) fn vrl_values_to_message_batch(
    mut vrl_values: Vec<VrlValue>,
) -> Result<MessageBatch, Error> {
    if vrl_values.is_empty() {}
    let Some(first_value) = vrl_values.get(0) else {
        return Ok(MessageBatch::from(RecordBatch::new_empty(Arc::new(
            Schema::empty(),
        ))));
    };

    let fields = match first_value {
        VrlValue::Object(obj) => obj
            .iter()
            .map(|(k, v)| match get_arrow_data_type(v) {
                Ok(data_type) => Ok(FieldRef::new(Field::new(k.to_string(), data_type, true))),
                Err(e) => Err(e),
            })
            .collect::<Result<Vec<FieldRef>, Error>>()?,
        _ => {
            return Ok(MessageBatch::from(RecordBatch::new_empty(Arc::new(
                Schema::empty(),
            ))));
        }
    };

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
                                cols.push(
                                    v.timestamp_nanos_opt().map_or_else(|| None, |v| Some(v)),
                                );
                            } else {
                                cols.push(None)
                            }
                        }
                        _ => cols.push(None),
                    }
                }
                Arc::new(TimestampNanosecondArray::from(cols))
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

pub(crate) fn insert(i: usize, vrl_values: &mut Vec<ObjectMap>, name: &str, val: VrlValue) {
    if let Some(obj) = vrl_values.get_mut(i) {
        obj.insert(name.to_string().into(), val);
    }
}

pub(crate) fn get_arrow_data_type(val: &VrlValue) -> Result<DataType, Error> {
    match val {
        VrlValue::Bytes(_) => Ok(DataType::Binary),
        VrlValue::Integer(_) => Ok(DataType::Int64),
        VrlValue::Float(_) => Ok(DataType::Float64),
        VrlValue::Boolean(_) => Ok(DataType::Boolean),
        VrlValue::Timestamp(_) => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        VrlValue::Null => Ok(DataType::Null),
        _ => Err(Error::Config("Unsupported data type".to_string())),
    }
}
