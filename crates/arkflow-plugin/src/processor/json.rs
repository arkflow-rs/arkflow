//! Arrow Processor Components
//!
//! A processor for converting between binary data and the Arrow format

use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Error, MessageBatch};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, NullArray, StringArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// Arrow format conversion processor configuration

/// Arrow Format Conversion Processor
#[derive(Debug, Clone, Serialize, Deserialize)]
struct JsonProcessorConfig {
    pub value_field: Option<String>,
}

pub struct JsonToArrowProcessor {
    config: JsonProcessorConfig,
}

#[async_trait]
impl Processor for JsonToArrowProcessor {
    async fn process(&self, msg_batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        let mut batches = Vec::with_capacity(msg_batch.len());
        let result = msg_batch.to_binary(self.config.value_field.as_deref().unwrap_or("value"))?;
        for x in result {
            let record_batch = json_to_arrow(x)?;
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

pub struct ArrowToJsonProcessor;

#[async_trait]
impl Processor for ArrowToJsonProcessor {
    async fn process(&self, msg_batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        let json_data = arrow_to_json(msg_batch)?;
        Ok(vec![MessageBatch::new_binary(vec![json_data])?])
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

fn json_to_arrow(content: &[u8]) -> Result<RecordBatch, Error> {
    // 解析JSON内容
    let json_value: Value = serde_json::from_slice(content)
        .map_err(|e| Error::Process(format!("JSON解析错误: {}", e)))?;

    match json_value {
        Value::Object(obj) => {
            // 单个对象转换为单行表
            let mut fields = Vec::new();
            let mut columns: Vec<ArrayRef> = Vec::new();

            // 提取所有字段和值
            for (key, value) in obj {
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
                            columns.push(Arc::new(UInt64Array::from(vec![v.as_u64().unwrap()])));
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
        _ => Err(Error::Process("输入必须是JSON对象".to_string())),
    }
}

/// Convert Arrow format to JSON
fn arrow_to_json(batch: MessageBatch) -> Result<Vec<u8>, Error> {
    // 使用Arrow的JSON序列化功能
    let mut buf = Vec::new();
    let mut writer = arrow::json::ArrayWriter::new(&mut buf);
    writer
        .write(&batch)
        .map_err(|e| Error::Process(format!("Arrow JSON序列化错误: {}", e)))?;
    writer
        .finish()
        .map_err(|e| Error::Process(format!("Arrow JSON序列化完成错误: {}", e)))?;

    Ok(buf)
}

pub(crate) struct JsonToArrowProcessorBuilder;
impl ProcessorBuilder for JsonToArrowProcessorBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Processor>, Error> {
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
    fn build(&self, _: &Option<serde_json::Value>) -> Result<Arc<dyn Processor>, Error> {
        Ok(Arc::new(ArrowToJsonProcessor))
    }
}

pub fn init() {
    register_processor_builder("arrow_to_json", Arc::new(ArrowToJsonProcessorBuilder));
    register_processor_builder("json_to_arrow", Arc::new(JsonToArrowProcessorBuilder));
}
