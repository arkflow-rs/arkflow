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
//! The processor used to convert between Protobuf data and the Arrow format.
//!
//! # Supported field types
//!
//! Scalar proto3 fields are supported: `bool`, `int32`/`sint32`/`sfixed32`,
//! `int64`/`sint64`/`sfixed64`, `uint32`/`fixed32`, `uint64`/`fixed64`,
//! `float`, `double`, `string`, `bytes`, and `enum` (mapped to Arrow `Int32`).
//! Nested message / repeated / map / oneof / proto3 optional fields are NOT
//! supported and produce an error.

use crate::component::protobuf::{
    arrow_to_protobuf, parse_proto_file, protobuf_to_arrow, ProtobufConfig,
};
use arkflow_core::component::{register_processor_metadata, ComponentMetadata};
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

pub(crate) fn init() -> Result<(), Error> {
    register_processor_builder(
        "arrow_to_protobuf",
        Arc::new(ArrowToProtobufProcessorBuilder),
    )?;
    register_processor_builder(
        "protobuf_to_arrow",
        Arc::new(ProtobufToArrowProcessorBuilder),
    )?;
    register_processor_metadata(ComponentMetadata::with_schema(
        "arrow_to_protobuf",
        "Serializes Arrow RecordBatches into Protobuf wire-format bytes.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "message_type": {"type": "string", "description": "Fully-qualified Protobuf message type name."},
                "proto_inputs": {"type": "array", "items": {"type": "string"}, "description": "Paths to .proto files."},
                "proto_includes": {"type": "array", "items": {"type": "string"}, "description": "Include paths for proto resolution."},
                "fields_to_include": {"type": "array", "items": {"type": "string"}, "description": "Optional allow-list of field names to include when serializing to Protobuf."}
            },
            "required": ["message_type", "proto_inputs"]
        }),
    ))?;
    register_processor_metadata(ComponentMetadata::with_schema(
        "protobuf_to_arrow",
        "Decodes Protobuf wire-format bytes into Arrow RecordBatches.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "message_type": {"type": "string", "description": "Fully-qualified Protobuf message type name."},
                "proto_inputs": {"type": "array", "items": {"type": "string"}, "description": "Paths to .proto files."},
                "proto_includes": {"type": "array", "items": {"type": "string"}, "description": "Include paths for proto resolution."},
                "value_field": {"type": "string", "description": "Name of the binary column holding the Protobuf wire-format bytes (defaults to '__value')."}
            },
            "required": ["message_type", "proto_inputs"]
        }),
    ))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::processor::ProcessorBuilder;
    use datafusion::arrow::array::{
        BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, UInt32Array,
    };
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

    fn create_test_proto_file_full() -> Result<(TempDir, PathBuf), Error> {
        let dir =
            tempdir().map_err(|e| Error::Process(format!("Failed to create temp dir: {}", e)))?;
        let proto_dir = dir.path().join("proto");
        std::fs::create_dir_all(&proto_dir)
            .map_err(|e| Error::Process(format!("Failed to create proto dir: {}", e)))?;
        let proto_file_path = proto_dir.join("full_message.proto");
        std::fs::write(&proto_file_path, r#"syntax = "proto3";

package test;

message FullMessage {
  int64 timestamp = 1;
  double value = 2;
  string sensor = 3;
  bool active = 4;
  uint32 count = 5;
  bytes payload = 6;
}
"#)
        .map_err(|e| Error::Process(format!("Failed to write proto file: {}", e)))?;
        Ok((dir, proto_dir))
    }

    fn create_test_proto_file_nested() -> Result<(TempDir, PathBuf), Error> {
        let dir =
            tempdir().map_err(|e| Error::Process(format!("Failed to create temp dir: {}", e)))?;
        let proto_dir = dir.path().join("proto");
        std::fs::create_dir_all(&proto_dir)
            .map_err(|e| Error::Process(format!("Failed to create proto dir: {}", e)))?;
        let proto_file_path = proto_dir.join("nested_message.proto");
        std::fs::write(&proto_file_path, r#"syntax = "proto3";

package test;

message WithNested {
  Sub sub = 1;
}

message Sub {
  int32 x = 1;
}
"#)
        .map_err(|e| Error::Process(format!("Failed to write proto file: {}", e)))?;
        Ok((dir, proto_dir))
    }

    #[tokio::test]
    async fn test_arrow_to_protobuf_full_scalar_round_trip() -> Result<(), Error> {
        let (_x, proto_dir) = create_test_proto_file_full()?;
        let config = ArrowToProtobufProcessorConfig {
            c: CommonProtobufProcessorConfig {
                proto_inputs: vec![proto_dir.to_string_lossy().to_string()],
                proto_includes: None,
                message_type: "test.FullMessage".to_string(),
            },
            fields_to_include: None,
        };
        let processor = ProtobufProcessor::new(config.into())?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("sensor", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
            Field::new("count", DataType::UInt32, false),
            Field::new("payload", DataType::Binary, false),
        ]));
        let rb = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1634567890])),
                Arc::new(Float64Array::from(vec![42.5])),
                Arc::new(StringArray::from(vec!["temperature"])),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(UInt32Array::from(vec![7u32])),
                Arc::new(BinaryArray::from(vec![b"\x01\x02".as_ref()])),
            ],
        )
        .map_err(|e| Error::Process(format!("Failed to create record batch: {}", e)))?;
        let msg_batch = MessageBatch::new_arrow(rb);

        let result = processor.process(Arc::new(msg_batch)).await?;
        let batch = match result {
            ProcessResult::Single(b) => b,
            _ => panic!("Expected single result"),
        };
        let data = batch.to_binary(DEFAULT_BINARY_VALUE_FIELD)?;
        let decoded =
            DynamicMessage::decode(processor.descriptor.clone(), data[0].as_ref())
                .map_err(|e| Error::Process(format!("Failed to decode: {}", e)))?;
        assert_eq!(decoded.get_field_by_name("timestamp").unwrap().as_ref(), &Value::I64(1634567890));
        assert_eq!(decoded.get_field_by_name("value").unwrap().as_ref(), &Value::F64(42.5));
        assert_eq!(decoded.get_field_by_name("active").unwrap().as_ref(), &Value::Bool(true));
        assert_eq!(decoded.get_field_by_name("count").unwrap().as_ref(), &Value::U32(7));
        Ok(())
    }

    #[tokio::test]
    async fn test_type_mismatch_errors() {
        // An Arrow Int32 column for a proto int64 field must error, not silently drop the field.
        let (_x, proto_dir) = create_test_proto_file_full().unwrap();
        let config = ArrowToProtobufProcessorConfig {
            c: CommonProtobufProcessorConfig {
                proto_inputs: vec![proto_dir.to_string_lossy().to_string()],
                proto_includes: None,
                message_type: "test.FullMessage".to_string(),
            },
            fields_to_include: None,
        };
        let processor = ProtobufProcessor::new(config.into()).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("timestamp", DataType::Int32, false)]));
        let rb = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await;
        assert!(
            result.is_err(),
            "Int32 column for proto int64 field must error, not silently drop"
        );
    }

    #[tokio::test]
    async fn test_absent_field_concat_succeeds() -> Result<(), Error> {
        // Two messages, A with all fields set, B with some unset — concat must not fail
        // because the schema is descriptor-driven (all fields nullable).
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
        let d = processor.descriptor.clone();

        let mut a = DynamicMessage::new(d.clone());
        a.set_field_by_name("timestamp", Value::I64(1));
        a.set_field_by_name("value", Value::F64(1.0));
        a.set_field_by_name("sensor", Value::String("s".to_string()));

        let mut b = DynamicMessage::new(d.clone());
        b.set_field_by_name("timestamp", Value::I64(2));
        // b.value and b.sensor intentionally unset

        let mut ea = Vec::new();
        a.encode(&mut ea).map_err(|e| Error::Process(format!("encode: {}", e)))?;
        let mut eb = Vec::new();
        b.encode(&mut eb).map_err(|e| Error::Process(format!("encode: {}", e)))?;
        let msg_batch = MessageBatch::new_binary(vec![ea, eb])?;

        let result = processor.process(Arc::new(msg_batch)).await?;
        let batch = match result {
            ProcessResult::Single(b) => b,
            _ => panic!("Expected single result"),
        };
        assert_eq!(batch.len(), 2);
        // sensor column must exist (descriptor-driven) even though message B lacks it.
        assert!(batch.schema().field_with_name("sensor").is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_null_arrow_value_left_unset() -> Result<(), Error> {
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
            Field::new("timestamp", DataType::Int64, true),
            Field::new("value", DataType::Float64, true),
            Field::new("sensor", DataType::Utf8, true),
        ]));
        let rb = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None])),
                Arc::new(Float64Array::from(vec![Some(1.0), None])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .map_err(|e| Error::Process(format!("Failed to create record batch: {}", e)))?;
        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?;
        let batch = match result {
            ProcessResult::Single(b) => b,
            _ => panic!("Expected single result"),
        };
        let data = batch.to_binary(DEFAULT_BINARY_VALUE_FIELD)?;
        // Row 1 (all null) → fields unset → decoded message has no timestamp field.
        let decoded =
            DynamicMessage::decode(processor.descriptor.clone(), data[1].as_ref())
                .map_err(|e| Error::Process(format!("Failed to decode: {}", e)))?;
        // proto3 does not distinguish unset from default on the wire; assert the null row
        // decodes to the default (0) and never carries another row's value.
        let ts = decoded.get_field_by_name("timestamp");
        let is_default = match &ts {
            None => true,
            Some(c) => matches!(c.as_ref(), Value::I64(0)),
        };
        assert!(
            is_default,
            "null Arrow value must decode to the proto default, got {:?}",
            ts
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_fields_to_include_filters() -> Result<(), Error> {
        let (_x, proto_dir) = create_test_proto_file()?;
        let mut include = HashSet::new();
        include.insert("timestamp".to_string());
        let config = ArrowToProtobufProcessorConfig {
            c: CommonProtobufProcessorConfig {
                proto_inputs: vec![proto_dir.to_string_lossy().to_string()],
                proto_includes: None,
                message_type: "test.TestMessage".to_string(),
            },
            fields_to_include: Some(include),
        };
        let processor = ProtobufProcessor::new(config.into())?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("sensor", DataType::Utf8, false),
        ]));
        let rb = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(Float64Array::from(vec![1.0])),
                Arc::new(StringArray::from(vec!["s"])),
            ],
        )
        .map_err(|e| Error::Process(format!("Failed to create record batch: {}", e)))?;
        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(rb)))
            .await?;
        let batch = match result {
            ProcessResult::Single(b) => b,
            _ => panic!("Expected single result"),
        };
        let data = batch.to_binary(DEFAULT_BINARY_VALUE_FIELD)?;
        let decoded =
            DynamicMessage::decode(processor.descriptor.clone(), data[0].as_ref())
                .map_err(|e| Error::Process(format!("Failed to decode: {}", e)))?;
        assert_eq!(decoded.get_field_by_name("timestamp").unwrap().as_ref(), &Value::I64(42));
        // value/sensor were filtered out of the Arrow batch, so they must not carry
        // the originally-passed values.
        let value = decoded.get_field_by_name("value");
        let encoded_value = match &value {
            None => false,
            Some(c) => matches!(c.as_ref(), Value::F64(1.0)),
        };
        assert!(
            !encoded_value,
            "filtered-out field 'value' must not be encoded, got {:?}",
            value
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_unsupported_nested_field_errors() -> Result<(), Error> {
        // A nested-message field is unsupported and must error, naming the kind.
        let (_x, proto_dir) = create_test_proto_file_nested()?;
        let config = ProtobufToArrowProcessorConfig {
            c: CommonProtobufProcessorConfig {
                proto_inputs: vec![proto_dir.to_string_lossy().to_string()],
                proto_includes: None,
                message_type: "test.WithNested".to_string(),
            },
            value_field: None,
        };
        let processor = ProtobufProcessor::new(config.into())?;
        let msg = DynamicMessage::new(processor.descriptor.clone());
        let mut buf = Vec::new();
        msg.encode(&mut buf)
            .map_err(|e| Error::Process(format!("encode: {}", e)))?;
        let msg_batch = MessageBatch::new_binary(vec![buf])?;
        let result = processor.process(Arc::new(msg_batch)).await;
        let err = result.err().expect("nested field must error");
        assert!(
            format!("{:?}", err).to_lowercase().contains("kind"),
            "error should mention field kind, got: {:?}",
            err
        );
        Ok(())
    }
}
