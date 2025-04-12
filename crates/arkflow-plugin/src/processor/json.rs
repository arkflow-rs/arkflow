//! Arrow Processor Components
//!
//! A processor for converting between binary data and the Arrow format

use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Bytes, Error, MessageBatch, DEFAULT_BINARY_VALUE_FIELD};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, NullArray, StringArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;

/// Arrow format conversion processor configuration

/// Arrow Format Conversion Processor
#[derive(Debug, Clone, Serialize, Deserialize)]
struct JsonProcessorConfig {
    pub value_field: Option<String>,
    pub need_convert_field: Option<HashSet<String>>,
}

pub struct JsonToArrowProcessor {
    config: JsonProcessorConfig,
}

#[async_trait]
impl Processor for JsonToArrowProcessor {
    async fn process(&self, msg_batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        let mut batches = Vec::with_capacity(msg_batch.len());
        let result = msg_batch.to_binary(
            self.config
                .value_field
                .as_deref()
                .unwrap_or(DEFAULT_BINARY_VALUE_FIELD),
        )?;
        for x in result {
            let record_batch = self.json_to_arrow(x)?;
            batches.push(record_batch)
        }

        if batches.is_empty() {
            return Ok(vec![]);
        }

        let schema = batches[0].schema();
        let batch = arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| Error::Process(format!("Merge batches failed: {}", e)))?;
        Ok(vec![MessageBatch::new_arrow(batch)])
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl JsonToArrowProcessor {
    fn json_to_arrow(&self, content: &[u8]) -> Result<RecordBatch, Error> {
        let json_value: Value = serde_json::from_slice(content)
            .map_err(|e| Error::Process(format!("JSON parsing error: {}", e)))?;

        match json_value {
            Value::Object(obj) => {
                let mut fields = Vec::new();
                let mut columns: Vec<ArrayRef> = Vec::new();

                for (key, value) in obj {
                    if let Some(ref set) = self.config.need_convert_field {
                        if !set.contains(&key) {
                            continue;
                        }
                    }
                    match value {
                        Value::Null => {
                            fields.push(Field::new(&key, DataType::Null, true));
                            // 空值列处理
                            columns.push(Arc::new(NullArray::new(1)));
                        }
                        Value::Bool(v) => {
                            fields.push(Field::new(&key, DataType::Boolean, false));
                            columns.push(Arc::new(BooleanArray::from(vec![v])));
                        }
                        Value::Number(v) => {
                            if v.is_i64() {
                                fields.push(Field::new(&key, DataType::Int64, false));
                                columns.push(Arc::new(Int64Array::from(vec![v.as_i64().unwrap()])));
                            } else if v.is_u64() {
                                fields.push(Field::new(&key, DataType::UInt64, false));
                                columns
                                    .push(Arc::new(UInt64Array::from(vec![v.as_u64().unwrap()])));
                            } else {
                                fields.push(Field::new(&key, DataType::Float64, false));
                                columns.push(Arc::new(Float64Array::from(vec![v
                                    .as_f64()
                                    .unwrap_or(0.0)])));
                            }
                        }
                        Value::String(v) => {
                            fields.push(Field::new(&key, DataType::Utf8, false));
                            columns.push(Arc::new(StringArray::from(vec![v])));
                        }
                        Value::Array(v) => {
                            fields.push(Field::new(&key, DataType::Utf8, false));
                            if let Ok(x) = serde_json::to_string(&v) {
                                columns.push(Arc::new(StringArray::from(vec![x])));
                            } else {
                                columns.push(Arc::new(StringArray::from(vec!["[]".to_string()])));
                            }
                        }
                        Value::Object(v) => {
                            fields.push(Field::new(&key, DataType::Utf8, false));
                            if let Ok(x) = serde_json::to_string(&v) {
                                columns.push(Arc::new(StringArray::from(vec![x])));
                            } else {
                                columns.push(Arc::new(StringArray::from(vec!["{}".to_string()])));
                            }
                        }
                    };
                }

                let schema = Arc::new(Schema::new(fields));
                RecordBatch::try_new(schema, columns).map_err(|e| {
                    Error::Process(format!("Creating an Arrow record batch failed: {}", e))
                })
            }
            _ => Err(Error::Process(
                "The input must be a JSON object".to_string(),
            )),
        }
    }
}

pub struct ArrowToJsonProcessor {
    config: JsonProcessorConfig,
}

#[async_trait]
impl Processor for ArrowToJsonProcessor {
    async fn process(&self, msg_batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        let json_data = self.arrow_to_json(msg_batch.clone())?;

        Ok(vec![msg_batch.new_binary_with_origin(json_data)?])
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl ArrowToJsonProcessor {
    /// Convert Arrow format to JSON
    fn arrow_to_json(&self, mut batch: MessageBatch) -> Result<Vec<Bytes>, Error> {
        if let Some(ref set) = self.config.need_convert_field {
            batch.remove_columns(set)
        }

        let mut buf = Vec::new();
        let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
        writer
            .write(&batch)
            .map_err(|e| Error::Process(format!("Arrow JSON Serialization error: {}", e)))?;
        writer.finish().map_err(|e| {
            Error::Process(format!("Arrow JSON Serialization Complete Error: {}", e))
        })?;
        let json_str = String::from_utf8(buf)
            .map_err(|e| Error::Process(format!("Conversion to UTF-8 string failed:{}", e)))?;

        Ok(json_str.lines().map(|s| s.as_bytes().to_vec()).collect())
    }
}

pub(crate) struct JsonToArrowProcessorBuilder;
impl ProcessorBuilder for JsonToArrowProcessorBuilder {
    fn build(&self, config: &Option<Value>) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "JsonToArrow processor configuration is missing".to_string(),
            ));
        }
        let config: JsonProcessorConfig = serde_json::from_value(config.clone().unwrap())?;

        Ok(Arc::new(JsonToArrowProcessor { config }))
    }
}
pub(crate) struct ArrowToJsonProcessorBuilder;
impl ProcessorBuilder for ArrowToJsonProcessorBuilder {
    fn build(&self, config: &Option<Value>) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "JsonToArrow processor configuration is missing".to_string(),
            ));
        }
        let config: JsonProcessorConfig = serde_json::from_value(config.clone().unwrap())?;

        Ok(Arc::new(ArrowToJsonProcessor { config }))
    }
}

pub fn init() {
    register_processor_builder("arrow_to_json", Arc::new(ArrowToJsonProcessorBuilder));
    register_processor_builder("json_to_arrow", Arc::new(JsonToArrowProcessorBuilder));
}
