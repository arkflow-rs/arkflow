use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    arrow::{array::*, datatypes::DataType, record_batch::RecordBatch},
    error,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{error, warn};
use vrl::{
    compiler::{self, Program, TargetValue, TimeZone},
    prelude::{state::RuntimeState, Context},
    stdlib,
    value::{Secrets, Value as VrlValue},
};

use arkflow_core::{
    processor::{register_processor_builder, Processor, ProcessorBuilder},
    Error, MessageBatch,
};

use super::json::json_to_arrow_inner;

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
        let mut batches = vec![];
        let result = recordbatch_to_vrl_value(msg_batch.0);

        let mut state = RuntimeState::default();
        let timezone = TimeZone::default();
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
                let v = vrl_value_to_json_value(v);
                match json_to_arrow_inner(v, None) {
                    Ok(rb) => {
                        batches.push(MessageBatch(rb));
                    }
                    Err(e) => warn!("Failed to convert JSON to Arrow: {:?}", e),
                }
            }
        }

        Ok(batches)
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

pub(crate) fn vrl_value_to_json_value(vrl_value: VrlValue) -> Value {
    match vrl_value {
        VrlValue::Null => Value::Null,
        VrlValue::Boolean(b) => Value::Bool(b),
        VrlValue::Float(n) => Value::Number(serde_json::Number::from_f64(*n).unwrap()),
        VrlValue::Integer(n) => Value::Number(serde_json::Number::from(n)),
        VrlValue::Bytes(s) => Value::String(String::from_utf8_lossy(&s).to_string()),
        VrlValue::Regex(s) => Value::String(s.to_string()),
        VrlValue::Timestamp(s) => Value::String(s.to_string()),
        VrlValue::Array(arr) => {
            let arr: Vec<Value> = arr.into_iter().map(vrl_value_to_json_value).collect();
            Value::Array(arr)
        }
        VrlValue::Object(obj) => {
            let obj: BTreeMap<String, Value> = obj
                .into_iter()
                .map(|(k, v)| (k.to_string(), vrl_value_to_json_value(v)))
                .collect();
            Value::Object(serde_json::Map::from_iter(obj))
        }
    }
}

pub(crate) fn recordbatch_to_vrl_value(record_batch: RecordBatch) -> Vec<VrlValue> {
    let rows = record_batch.num_rows();
    let mut vrl_values = Vec::with_capacity(rows);
    for _ in 0..rows {
        let map = BTreeMap::new();
        vrl_values.push(VrlValue::Object(map));
    }
    for i in 0..record_batch.num_columns() {
        let column = record_batch.column(i);
        let schema = record_batch.schema();
        let name = schema.field(i).name();
        match column.data_type() {
            DataType::Utf8 => {
                if let Some(col) = column.as_any().downcast_ref::<StringArray>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Bytes(value.to_string().into());
                        vrl_values[i]
                            .as_object_mut()
                            .unwrap()
                            .insert(name.to_string().into(), vrl_value);
                    }
                }
            }
            DataType::Binary => {
                if let Some(col) = column.as_any().downcast_ref::<BinaryArray>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Bytes(value.to_vec().into());
                        vrl_values[i]
                            .as_object_mut()
                            .unwrap()
                            .insert(name.to_string().into(), vrl_value);
                    }
                }
            }
            DataType::Boolean => {
                if let Some(col) = column.as_any().downcast_ref::<BooleanArray>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Boolean(value);
                        vrl_values[i]
                            .as_object_mut()
                            .unwrap()
                            .insert(name.to_string().into(), vrl_value);
                    }
                }
            }
            DataType::Float64 => {
                if let Some(col) = column.as_any().downcast_ref::<Float64Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Float(value.try_into().unwrap_or_default());
                        vrl_values[i]
                            .as_object_mut()
                            .unwrap()
                            .insert(name.to_string().into(), vrl_value);
                    }
                }
            }
            DataType::Float32 => {
                if let Some(col) = column.as_any().downcast_ref::<Float32Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value =
                            VrlValue::Float((value as f64).try_into().unwrap_or_default());
                        vrl_values[i]
                            .as_object_mut()
                            .unwrap()
                            .insert(name.to_string().into(), vrl_value);
                    }
                }
            }
            DataType::Int64 => {
                if let Some(col) = column.as_any().downcast_ref::<Int64Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value);
                        vrl_values[i]
                            .as_object_mut()
                            .unwrap()
                            .insert(name.to_string().into(), vrl_value);
                    }
                }
            }
            DataType::Int32 => {
                if let Some(col) = column.as_any().downcast_ref::<Int32Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value as i64);
                        vrl_values[i]
                            .as_object_mut()
                            .unwrap()
                            .insert(name.to_string().into(), vrl_value);
                    }
                }
            }
            DataType::Int16 => {
                if let Some(col) = column.as_any().downcast_ref::<Int16Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value as i64);
                        vrl_values[i]
                            .as_object_mut()
                            .unwrap()
                            .insert(name.to_string().into(), vrl_value);
                    }
                }
            }
            DataType::Int8 => {
                if let Some(col) = column.as_any().downcast_ref::<Int8Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value as i64);
                        vrl_values[i]
                            .as_object_mut()
                            .unwrap()
                            .insert(name.to_string().into(), vrl_value);
                    }
                }
            }
            DataType::UInt64 => {
                if let Some(col) = column.as_any().downcast_ref::<UInt64Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value as i64);
                        vrl_values[i]
                            .as_object_mut()
                            .unwrap()
                            .insert(name.to_string().into(), vrl_value);
                    }
                }
            }
            DataType::UInt32 => {
                if let Some(col) = column.as_any().downcast_ref::<UInt32Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value as i64);
                        vrl_values[i]
                            .as_object_mut()
                            .unwrap()
                            .insert(name.to_string().into(), vrl_value);
                    }
                }
            }
            DataType::UInt16 => {
                if let Some(col) = column.as_any().downcast_ref::<UInt16Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value as i64);
                        vrl_values[i]
                            .as_object_mut()
                            .unwrap()
                            .insert(name.to_string().into(), vrl_value);
                    }
                }
            }
            DataType::UInt8 => {
                if let Some(col) = column.as_any().downcast_ref::<UInt8Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value as i64);
                        vrl_values[i]
                            .as_object_mut()
                            .unwrap()
                            .insert(name.to_string().into(), vrl_value);
                    }
                }
            }
            DataType::Date32 => {
                if let Some(col) = column.as_any().downcast_ref::<Date32Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value as i64);
                        vrl_values[i]
                            .as_object_mut()
                            .unwrap()
                            .insert(name.to_string().into(), vrl_value);
                    }
                }
            }
            DataType::Date64 => {
                if let Some(col) = column.as_any().downcast_ref::<Date64Array>() {
                    for i in 0..rows {
                        let value = col.value(i);
                        let vrl_value = VrlValue::Integer(value);
                        vrl_values[i]
                            .as_object_mut()
                            .unwrap()
                            .insert(name.to_string().into(), vrl_value);
                    }
                }
            }
            DataType::Null => {
                // Handle null values
                for i in 0..rows {
                    let vrl_value = VrlValue::Null;
                    vrl_values[i]
                        .as_object_mut()
                        .unwrap()
                        .insert(name.to_string().into(), vrl_value);
                }
            }
            _ => {
                // Handle unsupported data types
                for i in 0..rows {
                    let vrl_value = VrlValue::Null;
                    vrl_values[i]
                        .as_object_mut()
                        .unwrap()
                        .insert(name.to_string().into(), vrl_value);
                }
                error!("Unsupported data type: {:?}", column.data_type());
            }
        };
    }
    vrl_values
}
