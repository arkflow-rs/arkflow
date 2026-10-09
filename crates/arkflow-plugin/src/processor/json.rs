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

use crate::component;
use arkflow_core::component::{
    register_processor_metadata, ComponentMetadata as CoreComponentMetadata,
};
use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{
    Bytes, Error, MessageBatch, MessageBatchRef, ProcessResult, Resource,
    DEFAULT_BINARY_VALUE_FIELD,
};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;

/// Arrow Format Conversion Processor
#[derive(Debug, Clone, Serialize, Deserialize)]
struct JsonProcessorConfig {
    value_field: Option<String>,
    fields_to_include: Option<HashSet<String>>,
}

struct JsonToArrowProcessor {
    config: JsonProcessorConfig,
}

#[async_trait]
impl Processor for JsonToArrowProcessor {
    async fn process(&self, msg_batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        let result = msg_batch.to_binary(
            self.config
                .value_field
                .as_deref()
                .unwrap_or(DEFAULT_BINARY_VALUE_FIELD),
        )?;

        let json_data: Vec<u8> = result.join(b"\n" as &[u8]);
        let record_batch = self.json_to_arrow(&json_data)?;
        Ok(ProcessResult::Single(Arc::new(MessageBatch::new_arrow(
            record_batch,
        ))))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl JsonToArrowProcessor {
    fn json_to_arrow(&self, content: &[u8]) -> Result<RecordBatch, Error> {
        component::json::try_to_arrow(content, self.config.fields_to_include.as_ref())
    }
}

pub struct ArrowToJsonProcessor {
    config: JsonProcessorConfig,
}

#[async_trait]
impl Processor for ArrowToJsonProcessor {
    async fn process(&self, msg_batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        let json_data = self.arrow_to_json((*msg_batch).clone())?;

        Ok(ProcessResult::Single(Arc::new(
            msg_batch.new_binary_with_origin(json_data)?,
        )))
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

struct JsonToArrowProcessorBuilder;

impl ProcessorBuilder for JsonToArrowProcessorBuilder {
    fn build(
        &self,
        _name: Option<&str>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "JsonToArrow processor configuration is missing".to_string(),
            ));
        }
        let config: JsonProcessorConfig = serde_json::from_value(config.clone().unwrap())?;

        Ok(Arc::new(JsonToArrowProcessor { config }))
    }
}
struct ArrowToJsonProcessorBuilder;

impl ProcessorBuilder for ArrowToJsonProcessorBuilder {
    fn build(
        &self,
        _name: Option<&str>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
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
    register_processor_metadata(CoreComponentMetadata::with_schema(
        "arrow_to_json",
        "Converts an Arrow RecordBatch into JSON byte payloads (one per row).",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "value_field": {"type": "string", "description": "Binary column carrying one JSON payload per row (defaults to the standard binary value column)."},
                "fields_to_include": {"type": "array", "items": {"type": "string"}, "description": "Only emit these top-level fields."}
            }
        }),
    ).with_optional())?;
    register_processor_metadata(CoreComponentMetadata::with_schema(
        "json_to_arrow",
        "Parses JSON byte payloads into an Arrow RecordBatch with inferred schema.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "value_field": {"type": "string", "description": "Binary column carrying one JSON payload per row (defaults to the standard binary value column)."},
                "fields_to_include": {"type": "array", "items": {"type": "string"}, "description": "Only parse these top-level fields into columns."}
            }
        }),
    ).with_optional())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::processor::json::{ArrowToJsonProcessorBuilder, JsonToArrowProcessorBuilder};
    use arkflow_core::processor::{Processor, ProcessorBuilder};
    use arkflow_core::{
        Error, MessageBatch, MessageBatchRef, ProcessResult, Resource, DEFAULT_BINARY_VALUE_FIELD,
    };
    use serde_json::json;
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::sync::Arc;

    fn resource() -> Resource {
        Resource {
            temporary: Default::default(),
            input_names: RefCell::new(Default::default()),
        }
    }

    fn to_arrow_processor(include: Option<&[&str]>) -> Arc<dyn Processor> {
        let config = Some(json!({
            "value_field": DEFAULT_BINARY_VALUE_FIELD,
            "fields_to_include": include.map(|f| f.iter().map(|s| s.to_string()).collect::<Vec<_>>()),
        }));
        JsonToArrowProcessorBuilder
            .build(None, &config, &resource())
            .expect("processor")
    }

    async fn process_json(processor: &Arc<dyn Processor>, line: &str) -> MessageBatchRef {
        let msg_batch = MessageBatch::new_binary(vec![line.as_bytes().to_vec()]).unwrap();
        let result: MessageBatchRef = Arc::new(msg_batch);
        match processor.process(result).await.unwrap() {
            ProcessResult::Single(batch) => batch,
            _ => panic!("Expected single result"),
        }
    }

    /// The unprojected path must keep picking up fields that first appear in
    /// later batches — every batch is independently inferred.
    #[tokio::test]
    async fn unprojected_path_keeps_late_fields() -> Result<(), Error> {
        let processor = to_arrow_processor(None);

        let first = process_json(&processor, r#"{"a":1}"#).await;
        assert!(first.schema().field_with_name("a").is_ok());
        assert!(first.schema().field_with_name("late").is_err());

        let second = process_json(&processor, r#"{"a":2,"late":"x"}"#).await;
        assert!(second.schema().field_with_name("late").is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_json_to_arrow_basic_types() -> Result<(), Error> {
        let config = Some(json!({
            "value_field": DEFAULT_BINARY_VALUE_FIELD,
            "fields_to_include": null
        }));
        let processor = JsonToArrowProcessorBuilder.build(
            None,
            &config,
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        )?;

        let json_data = json!({
            "null_field": null,
            "bool_field": true,
            "int_field": 42,
            "uint_field": 18446744073709551615u64,
            "float_field": std::f64::consts::PI,
            "string_field": "hello",
            "array_field": [1, 2, 3],
            "object_field": {"key": "value"}
        });

        let msg_batch = MessageBatch::new_binary(vec![json_data.to_string().into_bytes()]).unwrap();

        let result = processor.process(Arc::new(msg_batch)).await.unwrap();
        match result {
            ProcessResult::Single(batch) => {
                assert_eq!(batch.len(), 1);
            }
            _ => panic!("Expected single result"),
        }
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
        let processor = JsonToArrowProcessorBuilder.build(
            None,
            &config,
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        )?;

        let json_data = json!({
            "int_field": 42,
            "string_field": "hello",
            "ignored_field": true
        });

        let msg_batch = MessageBatch::new_binary(vec![json_data.to_string().into_bytes()])?;

        let result = processor.process(Arc::new(msg_batch)).await?;
        match result {
            ProcessResult::Single(batch) => {
                assert_eq!(batch.len(), 1);
            }
            _ => panic!("Expected single result"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_json_to_arrow_whole_batch_widens_types_and_keeps_late_fields() -> Result<(), Error>
    {
        // Regression: `json_to_arrow` joins the whole batch with '\n' and
        // decodes it in one pass, so schema inference must cover ALL
        // messages. First-record-only inference inferred `v` as Int64 and
        // never saw `tag`, silently truncating `1.5`/`2.9` to integers and
        // dropping the `tag` column.
        let config = Some(json!({
            "value_field": DEFAULT_BINARY_VALUE_FIELD,
            "fields_to_include": null
        }));
        let processor = JsonToArrowProcessorBuilder.build(
            None,
            &config,
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        )?;

        let msg_batch = MessageBatch::new_binary(vec![
            br#"{"v":1}"#.to_vec(),
            br#"{"v":1.5}"#.to_vec(),
            br#"{"v":2.9,"tag":"x"}"#.to_vec(),
        ])?;

        let result = processor.process(Arc::new(msg_batch)).await?;
        let batch = match result {
            ProcessResult::Single(batch) => batch,
            _ => panic!("Expected single result"),
        };

        assert_eq!(batch.len(), 3);
        let schema = batch.record_batch().schema();
        assert_eq!(
            schema.field_with_name("v").unwrap().data_type(),
            &datafusion::arrow::datatypes::DataType::Float64,
            "Int64 + Float64 across messages must widen to Float64, not truncate"
        );
        let tag_field = schema.field_with_name("tag").unwrap();
        assert_eq!(
            tag_field.data_type(),
            &datafusion::arrow::datatypes::DataType::Utf8,
            "fields first seen in later messages must still become columns"
        );
        assert!(tag_field.is_nullable());

        use datafusion::arrow::array::AsArray;
        let v = batch
            .record_batch()
            .column_by_name("v")
            .unwrap()
            .as_primitive::<datafusion::arrow::datatypes::Float64Type>();
        assert_eq!(v.value(0), 1.0);
        assert_eq!(v.value(1), 1.5);
        assert_eq!(v.value(2), 2.9);

        let tag_col = batch.record_batch().column_by_name("tag").unwrap();
        assert!(tag_col.is_null(0));
        assert!(tag_col.is_null(1));
        assert!(!tag_col.is_null(2));
        Ok(())
    }

    #[tokio::test]
    async fn test_json_to_arrow_whole_batch_all_int_stays_int64() -> Result<(), Error> {
        // Full-batch inference widens only when needed: an all-integer
        // column must stay Int64.
        let config = Some(json!({
            "value_field": DEFAULT_BINARY_VALUE_FIELD,
            "fields_to_include": null
        }));
        let processor = JsonToArrowProcessorBuilder.build(
            None,
            &config,
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        )?;

        let msg_batch = MessageBatch::new_binary(vec![
            br#"{"v":1}"#.to_vec(),
            br#"{"v":2}"#.to_vec(),
            br#"{"v":3}"#.to_vec(),
        ])?;

        let result = processor.process(Arc::new(msg_batch)).await?;
        let batch = match result {
            ProcessResult::Single(batch) => batch,
            _ => panic!("Expected single result"),
        };

        assert_eq!(batch.len(), 3);
        assert_eq!(
            batch
                .record_batch()
                .schema()
                .field_with_name("v")
                .unwrap()
                .data_type(),
            &datafusion::arrow::datatypes::DataType::Int64
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_json_to_arrow_invalid_input() -> Result<(), Error> {
        let config = Some(json!({
            "value_field": "data",
            "fields_to_include": null
        }));
        let processor = JsonToArrowProcessorBuilder.build(
            None,
            &config,
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        )?;

        let invalid_json = b"not a json object";
        let msg_batch = MessageBatch::new_binary(vec![invalid_json.to_vec()])?;

        let result = processor.process(Arc::new(msg_batch)).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_arrow_to_json_basic() {
        let config = Some(json!({
            "value_field": DEFAULT_BINARY_VALUE_FIELD,
            "fields_to_include": null
        }));
        let json_to_arrow = JsonToArrowProcessorBuilder
            .build(
                None,
                &config,
                &Resource {
                    temporary: Default::default(),
                    input_names: RefCell::new(Default::default()),
                },
            )
            .unwrap();
        let arrow_to_json = ArrowToJsonProcessorBuilder
            .build(
                None,
                &config,
                &Resource {
                    temporary: Default::default(),
                    input_names: RefCell::new(Default::default()),
                },
            )
            .unwrap();

        let json_data = json!({
            "int_field": 42,
            "string_field": "hello"
        });

        let msg_batch = MessageBatch::new_binary(vec![json_data.to_string().into_bytes()]).unwrap();

        // Convert JSON to Arrow
        let arrow_result = json_to_arrow.process(Arc::new(msg_batch)).await.unwrap();

        let arrow_batch = match arrow_result {
            ProcessResult::Single(batch) => batch,
            _ => panic!("Expected single result"),
        };

        // Convert Arrow back to JSON
        let json_result = arrow_to_json.process(arrow_batch).await.unwrap();

        match json_result {
            ProcessResult::Single(batch) => {
                assert_eq!(batch.len(), 1);
            }
            _ => panic!("Expected single result"),
        }
    }

    #[tokio::test]
    async fn test_processor_missing_config() {
        let result = JsonToArrowProcessorBuilder.build(
            None,
            &None,
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        );
        assert!(result.is_err());

        let result = ArrowToJsonProcessorBuilder.build(
            None,
            &None,
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        );
        assert!(result.is_err());
    }
}
