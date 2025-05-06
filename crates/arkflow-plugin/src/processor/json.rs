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

//! Arrow Processor Components
//!
//! A processor for converting between binary data and the Arrow format

use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Bytes, Error, MessageBatch, DEFAULT_BINARY_VALUE_FIELD};
use arrow_json::ReaderBuilder;
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;

/// Arrow format conversion processor configuration

/// Arrow Format Conversion Processor
#[derive(Debug, Clone, Serialize, Deserialize)]
struct JsonProcessorConfig {
    pub value_field: Option<String>,
    pub fields_to_include: Option<HashSet<String>>,
}

pub struct JsonToArrowProcessor {
    config: JsonProcessorConfig,
}

#[async_trait]
impl Processor for JsonToArrowProcessor {
    async fn process(&self, msg_batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        let result = msg_batch.to_binary(
            self.config
                .value_field
                .as_deref()
                .unwrap_or(DEFAULT_BINARY_VALUE_FIELD),
        )?;

        let json_data: Vec<u8> = result.join(b"\n" as &[u8]);
        let record_batch = self.json_to_arrow(&json_data)?;
        Ok(vec![MessageBatch::new_arrow(record_batch)])
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl JsonToArrowProcessor {
    fn json_to_arrow(&self, content: &[u8]) -> Result<RecordBatch, Error> {
        let mut cursor_for_inference = Cursor::new(content);
        let (mut inferred_schema, _) =
            arrow_json::reader::infer_json_schema(&mut cursor_for_inference, Some(1))
                .map_err(|e| Error::Process(format!("Schema inference error: {}", e)))?;
        if let Some(ref set) = self.config.fields_to_include {
            inferred_schema = inferred_schema
                .project(
                    &set.iter()
                        .filter_map(|name| inferred_schema.index_of(name).ok())
                        .collect::<Vec<_>>(),
                )
                .map_err(|e| Error::Process(format!("Arrow JSON Projection Error: {}", e)))?;
        }

        let inferred_schema = Arc::new(inferred_schema);
        let mut decoder = ReaderBuilder::new(inferred_schema.clone())
            .build_decoder()
            .map_err(|e| Error::Process(format!("Arrow JSON Reader Builder Error: {}", e)))?;
        decoder
            .decode(content)
            .map_err(|e| Error::Process(format!("Arrow JSON Reader Error: {}", e)))?;
        let batch = decoder
            .flush()
            .map_err(|e| Error::Process(format!("Arrow JSON Reader Flush Error: {}", e)))?
            .unwrap_or(RecordBatch::new_empty(inferred_schema));

        Ok(batch)
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
        if let Some(ref set) = self.config.fields_to_include {
            batch = batch.filter_columns(set)?
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

pub fn init() -> Result<(), Error> {
    register_processor_builder("arrow_to_json", Arc::new(ArrowToJsonProcessorBuilder))?;
    register_processor_builder("json_to_arrow", Arc::new(JsonToArrowProcessorBuilder))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::processor::json::{ArrowToJsonProcessorBuilder, JsonToArrowProcessorBuilder};
    use arkflow_core::processor::ProcessorBuilder;
    use arkflow_core::{Error, MessageBatch, DEFAULT_BINARY_VALUE_FIELD};
    use serde_json::json;
    use std::collections::HashSet;

    #[tokio::test]
    async fn test_json_to_arrow_basic_types() -> Result<(), Error> {
        let config = Some(json!({
            "value_field": DEFAULT_BINARY_VALUE_FIELD,
            "fields_to_include": null
        }));
        let processor = JsonToArrowProcessorBuilder.build(&config)?;

        let json_data = json!({
            "null_field": null,
            "bool_field": true,
            "int_field": 42,
            "uint_field": 18446744073709551615u64,
            "float_field": 3.14,
            "string_field": "hello",
            "array_field": [1, 2, 3],
            "object_field": {"key": "value"}
        });

        let msg_batch = MessageBatch::new_binary(vec![json_data.to_string().into_bytes()]).unwrap();

        let result = processor.process(msg_batch).await.unwrap();
        assert_eq!(result.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_json_to_arrow_field_filtering() -> Result<(), Error> {
        let mut fields = HashSet::new();
        fields.insert("int_field".to_string());
        fields.insert("string_field".to_string());

        let config = Some(json!({
            "value_field": DEFAULT_BINARY_VALUE_FIELD,
            "fields_to_include": fields
        }));
        let processor = JsonToArrowProcessorBuilder.build(&config)?;

        let json_data = json!({
            "int_field": 42,
            "string_field": "hello",
            "ignored_field": true
        });

        let msg_batch = MessageBatch::new_binary(vec![json_data.to_string().into_bytes()])?;

        let result = processor.process(msg_batch).await?;
        assert_eq!(result.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_json_to_arrow_invalid_input() -> Result<(), Error> {
        let config = Some(json!({
            "value_field": "data",
            "fields_to_include": null
        }));
        let processor = JsonToArrowProcessorBuilder.build(&config)?;

        let invalid_json = b"not a json object";
        let msg_batch = MessageBatch::new_binary(vec![invalid_json.to_vec()])?;

        let result = processor.process(msg_batch).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_arrow_to_json_basic() {
        let config = Some(json!({
            "value_field": DEFAULT_BINARY_VALUE_FIELD,
            "fields_to_include": null
        }));
        let json_to_arrow = JsonToArrowProcessorBuilder.build(&config).unwrap();
        let arrow_to_json = ArrowToJsonProcessorBuilder.build(&config).unwrap();

        let json_data = json!({
            "int_field": 42,
            "string_field": "hello"
        });

        let msg_batch = MessageBatch::new_binary(vec![json_data.to_string().into_bytes()]).unwrap();

        // Convert JSON to Arrow
        let arrow_batch = json_to_arrow.process(msg_batch).await.unwrap();
        assert_eq!(arrow_batch.len(), 1);

        // Convert Arrow back to JSON
        let json_result = arrow_to_json.process(arrow_batch[0].clone()).await.unwrap();
        assert_eq!(json_result.len(), 1);
    }

    #[tokio::test]
    async fn test_processor_missing_config() {
        let result = JsonToArrowProcessorBuilder.build(&None);
        assert!(result.is_err());

        let result = ArrowToJsonProcessorBuilder.build(&None);
        assert!(result.is_err());
    }
}
