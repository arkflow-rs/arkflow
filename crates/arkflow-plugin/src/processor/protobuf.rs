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

//! Protobuf Processor Components
//!
//! The processor used to convert between Protobuf data and the Arrow format

use crate::component::protobuf::{
    arrow_to_protobuf, parse_proto_file, protobuf_to_arrow, ProtobufConfig,
};
use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{
    Error, MessageBatch, MessageBatchRef, ProcessResult, Resource, DEFAULT_BINARY_VALUE_FIELD,
};
use async_trait::async_trait;
use datafusion::arrow;
use prost_reflect::MessageDescriptor;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

/// Protobuf format conversion processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProtobufProcessorConfig {
    /// Protobuf message type descriptor file path
    proto_inputs: Vec<String>,
    proto_includes: Option<Vec<String>>,
    /// Protobuf message type name
    message_type: String,
    mode: ToType,
    fields_to_include: Option<HashSet<String>>,
}

impl ProtobufConfig for ProtobufProcessorConfig {
    fn proto_inputs(&self) -> &Vec<String> {
        &self.proto_inputs
    }

    fn proto_includes(&self) -> &Option<Vec<String>> {
        &self.proto_includes
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ToType {
    ArrowToProtobuf,
    ProtobufToArrow(ArrowConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArrowConfig {
    value_field: Option<String>,
}
/// Protobuf Format Conversion Processor
struct ProtobufProcessor {
    _config: ProtobufProcessorConfig,
    descriptor: MessageDescriptor,
}

impl ProtobufProcessor {
    /// Create a new Protobuf format conversion processor
    fn new(config: ProtobufProcessorConfig) -> Result<Self, Error> {
        // Check the file extension to see if it's a proto file or a binary descriptor file
        let file_descriptor_set = parse_proto_file(&config)?;

        let descriptor_pool = prost_reflect::DescriptorPool::from_file_descriptor_set(
            file_descriptor_set,
        )
        .map_err(|e| Error::Config(format!("Unable to create Protobuf descriptor pool: {}", e)))?;

        let message_descriptor = descriptor_pool
            .get_message_by_name(&config.message_type)
            .ok_or_else(|| {
                Error::Config(format!(
                    "The message type could not be found: {}",
                    config.message_type
                ))
            })?;

        Ok(Self {
            _config: config.clone(),
            descriptor: message_descriptor,
        })
    }
}

#[async_trait]
impl Processor for ProtobufProcessor {
    async fn process(&self, msg: MessageBatchRef) -> Result<ProcessResult, Error> {
        if msg.is_empty() {
            return Ok(ProcessResult::None);
        }

        let result = match self._config.mode {
            ToType::ArrowToProtobuf => {
                // Convert Arrow format to Protobuf.
                let proto_data = if let Some(ref fields_to_include) = self._config.fields_to_include
                {
                    let filter_msg = (*msg).filter_columns(fields_to_include)?;
                    arrow_to_protobuf(&self.descriptor, &filter_msg)?
                } else {
                    arrow_to_protobuf(&self.descriptor, &msg)?
                };

                Arc::new((*msg).new_binary_with_origin(proto_data)?)
            }
            ToType::ProtobufToArrow(ref c) => {
                if msg.is_empty() {
                    return Ok(ProcessResult::None);
                }

                let mut batches = Vec::with_capacity(msg.len());
                let result = (*msg).to_binary(
                    c.value_field
                        .as_deref()
                        .unwrap_or(DEFAULT_BINARY_VALUE_FIELD),
                )?;
                for x in result {
                    // Convert Protobuf messages to Arrow format.
                    let batch = protobuf_to_arrow(&self.descriptor, x)?;
                    batches.push(batch)
                }

                let schema = batches[0].schema();
                let batch = arrow::compute::concat_batches(&schema, &batches)
                    .map_err(|e| Error::Process(format!("Batch merge failed: {}", e)))?;
                Arc::new(MessageBatch::new_arrow(batch))
            }
        };

        Ok(ProcessResult::Single(result))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProtobufToArrowProcessorConfig {
    #[serde(flatten)]
    c: CommonProtobufProcessorConfig,
    value_field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommonProtobufProcessorConfig {
    proto_inputs: Vec<String>,
    proto_includes: Option<Vec<String>>,
    /// Protobuf message type name
    message_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArrowToProtobufProcessorConfig {
    c: CommonProtobufProcessorConfig,
    fields_to_include: Option<HashSet<String>>,
}

impl From<ArrowToProtobufProcessorConfig> for ProtobufProcessorConfig {
    fn from(config: ArrowToProtobufProcessorConfig) -> Self {
        Self {
            proto_inputs: config.c.proto_inputs,
            proto_includes: config.c.proto_includes,
            message_type: config.c.message_type,
            mode: ToType::ArrowToProtobuf,
            fields_to_include: config.fields_to_include,
        }
    }
}

impl From<ProtobufToArrowProcessorConfig> for ProtobufProcessorConfig {
    fn from(config: ProtobufToArrowProcessorConfig) -> Self {
        Self {
            proto_inputs: config.c.proto_inputs,
            proto_includes: config.c.proto_includes,
            message_type: config.c.message_type,
            mode: ToType::ProtobufToArrow(ArrowConfig {
                value_field: config.value_field,
            }),
            fields_to_include: None,
        }
    }
}

struct ProtobufToArrowProcessorBuilder;
impl ProcessorBuilder for ProtobufToArrowProcessorBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "ProtobufToArrow processor configuration is missing".to_string(),
            ));
        }
        let config: ProtobufToArrowProcessorConfig =
            serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(ProtobufProcessor::new(config.into())?))
    }
}
struct ArrowToProtobufProcessorBuilder;
impl ProcessorBuilder for ArrowToProtobufProcessorBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "ArrowToProtobuf processor configuration is missing".to_string(),
            ));
        }
        let config: ArrowToProtobufProcessorConfig =
            serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(ProtobufProcessor::new(config.into())?))
    }
}

pub fn init() -> Result<(), Error> {
    register_processor_builder(
        "arrow_to_protobuf",
        Arc::new(ArrowToProtobufProcessorBuilder),
    )?;
    register_processor_builder(
        "protobuf_to_arrow",
        Arc::new(ProtobufToArrowProcessorBuilder),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::processor::ProcessorBuilder;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use prost_reflect::prost::Message;
    use prost_reflect::{DynamicMessage, Value};
    use std::cell::RefCell;
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::{tempdir, TempDir};

    fn create_test_proto_file() -> Result<(TempDir, PathBuf), Error> {
        let dir =
            tempdir().map_err(|e| Error::Process(format!("Failed to create temp dir: {}", e)))?;
        let proto_dir = dir.path().join("proto");
        std::fs::create_dir_all(&proto_dir)
            .map_err(|e| Error::Process(format!("Failed to create proto dir: {}", e)))?;

        let proto_file_path = proto_dir.join("test_message.proto");
        let mut file = File::create(&proto_file_path)
            .map_err(|e| Error::Process(format!("Failed to create proto file: {}", e)))?;

        let proto_content = r#"syntax = "proto3";

package test;

message TestMessage {
  int64 timestamp = 1;
  double value = 2;
  string sensor = 3;
}
"#;

        file.write_all(proto_content.as_bytes())
            .map_err(|e| Error::Process(format!("Failed to write proto file: {}", e)))?;

        file.flush()
            .map_err(|e| Error::Process(format!("Failed to flush proto file: {}", e)))?;

        Ok((dir, proto_dir))
    }

    #[tokio::test]
    async fn test_protobuf_to_arrow_conversion() -> Result<(), Error> {
        let (_x, proto_dir) = create_test_proto_file()?;

        let config = ProtobufToArrowProcessorConfig {
            c: CommonProtobufProcessorConfig {
                proto_inputs: vec![proto_dir.to_string_lossy().to_string()],
                proto_includes: None,
                message_type: "test.TestMessage".to_string(),
            },
            value_field: Some(DEFAULT_BINARY_VALUE_FIELD.to_string()),
        };

        let processor = ProtobufProcessor::new(config.into())?;

        let descriptor = processor.descriptor.clone();
        let mut test_message = DynamicMessage::new(descriptor);

        test_message.set_field_by_name("timestamp", Value::I64(1634567890));
        test_message.set_field_by_name("value", Value::F64(42.5));
        test_message.set_field_by_name("sensor", Value::String("temperature".to_string()));

        let mut encoded = Vec::new();
        test_message.encode(&mut encoded).unwrap();
        let msg_batch = MessageBatch::new_binary(vec![encoded])?;

        let result = processor.process(Arc::new(msg_batch)).await?;
        assert_eq!(result.len(), 1);

        let batch = match &result {
            ProcessResult::Single(b) => b,
            _ => panic!("Expected single result"),
        };

        assert_eq!(batch.schema().fields().len(), 3);

        let schema = batch.schema();
        let field_names: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        assert!(field_names.contains(&String::from("timestamp")));
        assert!(field_names.contains(&String::from("value")));
        assert!(field_names.contains(&String::from("sensor")));

        Ok(())
    }

    #[tokio::test]
    async fn test_arrow_to_protobuf_conversion() -> Result<(), Error> {
        let (_x, proto_dir) = create_test_proto_file()?;

        let config = ArrowToProtobufProcessorConfig {
            c: CommonProtobufProcessorConfig {
                proto_inputs: vec![proto_dir.to_string_lossy().to_string()],
                proto_includes: None,
                message_type: "test.TestMessage".to_string(),
            },
            fields_to_include: None,
        };

        let processor = ProtobufProcessor::new(config.into())?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("sensor", DataType::Utf8, false),
        ]));

        let timestamp_array = Int64Array::from(vec![1634567890]);
        let value_array = Float64Array::from(vec![42.5]);
        let sensor_array = StringArray::from(vec!["temperature"]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(timestamp_array),
                Arc::new(value_array),
                Arc::new(sensor_array),
            ],
        )
        .map_err(|e| Error::Process(format!("Failed to create record batch: {}", e)))?;

        let msg_batch = MessageBatch::new_arrow(batch);

        let result = processor.process(Arc::new(msg_batch)).await?;
        assert_eq!(result.len(), 1);

        let batch = match &result {
            ProcessResult::Single(b) => b,
            _ => panic!("Expected single result"),
        };
        let binary_data = batch.to_binary(DEFAULT_BINARY_VALUE_FIELD)?;
        assert_eq!(binary_data.len(), 1);

        let decoded_msg =
            DynamicMessage::decode(processor.descriptor.clone(), binary_data[0].as_ref())
                .map_err(|e| Error::Process(format!("Failed to decode protobuf: {}", e)))?;

        let timestamp = decoded_msg.get_field_by_name("timestamp").unwrap();
        let value = decoded_msg.get_field_by_name("value").unwrap();
        let sensor = decoded_msg.get_field_by_name("sensor").unwrap();

        assert_eq!(timestamp.as_ref(), &Value::I64(1634567890));
        assert_eq!(value.as_ref(), &Value::F64(42.5));
        assert_eq!(sensor.as_ref(), &Value::String("temperature".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn test_protobuf_processor_empty_batch() -> Result<(), Error> {
        let (_x, proto_dir) = create_test_proto_file()?;

        let config = ProtobufToArrowProcessorConfig {
            c: CommonProtobufProcessorConfig {
                proto_inputs: vec![proto_dir.to_string_lossy().to_string()],
                proto_includes: None,
                message_type: "test.TestMessage".to_string(),
            },
            value_field: None,
        };

        let processor = ProtobufProcessor::new(config.into())?;

        let empty_batch = MessageBatch::new_binary(vec![])?;

        let result = processor.process(Arc::new(empty_batch)).await?;
        assert_eq!(result.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_processor_builder() {
        let result = ProtobufToArrowProcessorBuilder.build(
            None,
            &None,
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        );
        assert!(result.is_err());

        let result = ArrowToProtobufProcessorBuilder.build(
            None,
            &None,
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        );
        assert!(result.is_err());

        let (_x, proto_dir) = create_test_proto_file().unwrap();
        let config = serde_json::to_value(ProtobufToArrowProcessorConfig {
            c: CommonProtobufProcessorConfig {
                proto_inputs: vec![proto_dir.to_string_lossy().to_string()],
                proto_includes: None,
                message_type: "test.TestMessage".to_string(),
            },
            value_field: None,
        })
        .unwrap();

        let result = ProtobufToArrowProcessorBuilder.build(
            None,
            &Some(config),
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        );
        assert!(result.is_ok());
    }
}
