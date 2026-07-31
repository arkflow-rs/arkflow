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

//! Protobuf Codec Components
//!
//! The codec used to convert between Protobuf data and the Arrow format.
//!
//! # Supported field types
//!
//! Scalar proto3 fields are supported: `bool`, `int32`/`sint32`/`sfixed32`,
//! `int64`/`sint64`/`sfixed64`, `uint32`/`fixed32`, `uint64`/`fixed64`,
//! `float`, `double`, `string`, `bytes`, and `enum` (mapped to Arrow `Int32`).
//! Nested message / repeated / map / oneof / proto3 optional fields are NOT
//! supported and produce an error.

use async_trait::async_trait;
use crate::component::protobuf::{
    arrow_to_protobuf, parse_proto_file, protobuf_to_arrow, ProtobufConfig,
};
use arkflow_core::codec::{Codec, CodecBuilder, Decoder, Encoder};
use arkflow_core::component::{register_codec_metadata, ComponentMetadata};
use arkflow_core::{codec, Bytes, Error, MessageBatch, Resource};
use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use prost_reflect::MessageDescriptor;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Protobuf codec configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProtobufCodecConfig {
    /// Protobuf message type descriptor file paths
    proto_inputs: Vec<String>,
    /// Include paths for proto files
    proto_includes: Option<Vec<String>>,
    /// Protobuf message type name
    message_type: String,
}

impl ProtobufConfig for ProtobufCodecConfig {
    fn proto_inputs(&self) -> &Vec<String> {
        &self.proto_inputs
    }

    fn proto_includes(&self) -> &Option<Vec<String>> {
        &self.proto_includes
    }
}

/// Protobuf Codec
struct ProtobufCodec {
    descriptor: MessageDescriptor,
}

impl ProtobufCodec {
    /// Create a new Protobuf codec
    fn new(config: ProtobufCodecConfig) -> Result<Self, Error> {
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
            descriptor: message_descriptor,
        })
    }
}

#[async_trait]
impl Encoder for ProtobufCodec {
    async fn encode(&self, b: MessageBatch) -> Result<Vec<Bytes>, Error> {
        arrow_to_protobuf(&self.descriptor, &b)
    }
}

#[async_trait]
impl Decoder for ProtobufCodec {
    async fn decode(&self, b: Vec<Bytes>) -> Result<MessageBatch, Error> {
        let mut batches = Vec::with_capacity(b.len());

        for data in b {
            let record_batch = protobuf_to_arrow(&self.descriptor, &data)?;
            batches.push(record_batch);
        }

        if batches.is_empty() {
            return Ok(MessageBatch::new_arrow(RecordBatch::new_empty(Arc::new(
                Schema::empty(),
            ))));
        }

        let schema = batches[0].schema();
        let merged_batch = arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| Error::Process(format!("Batch merge failed: {}", e)))?;

        Ok(MessageBatch::new_arrow(merged_batch))
    }
}

struct ProtobufCodecBuilder;

impl CodecBuilder for ProtobufCodecBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Codec>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Protobuf codec configuration is missing".to_string(),
            ));
        }

        let config: ProtobufCodecConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(ProtobufCodec::new(config)?))
    }
}

pub(crate) fn init() -> Result<(), Error> {
    codec::register_codec_builder("protobuf", Arc::new(ProtobufCodecBuilder))?;
    register_codec_metadata(ComponentMetadata::with_schema(
        "protobuf",
        "Encodes/decodes Arrow RecordBatches using a Protobuf descriptor.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "message_type": {"type": "string", "description": "Fully-qualified Protobuf message type name."},
                "proto_inputs": {"type": "array", "items": {"type": "string"}, "description": "Paths to .proto files."},
                "proto_includes": {"type": "array", "items": {"type": "string"}, "description": "Include paths for proto resolution."}
            },
            "required": ["message_type", "proto_inputs"]
        }),
    ))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field};
    use std::cell::RefCell;
    use tempfile::TempDir;

    fn create_test_resource() -> Resource {
        Resource {
            temporary: Default::default(),
            input_names: RefCell::new(Default::default()),
        }
    }

    #[tokio::test]
    async fn test_protobuf_codec_config_deserialization() {
        let config_json = serde_json::json!({
            "proto_inputs": ["/path/to/file.proto"],
            "message_type": "MyMessage"
        });

        let config: ProtobufCodecConfig = serde_json::from_value(config_json).unwrap();
        assert_eq!(config.proto_inputs, vec!["/path/to/file.proto"]);
        assert_eq!(config.message_type, "MyMessage");
        assert!(config.proto_includes.is_none());
    }

    #[tokio::test]
    async fn test_protobuf_codec_config_with_includes() {
        let config_json = serde_json::json!({
            "proto_inputs": ["/path/to/file.proto"],
            "proto_includes": ["/include/path"],
            "message_type": "MyMessage"
        });

        let config: ProtobufCodecConfig = serde_json::from_value(config_json).unwrap();
        assert_eq!(config.proto_inputs, vec!["/path/to/file.proto"]);
        assert_eq!(config.message_type, "MyMessage");
        assert!(config.proto_includes.is_some());
        assert_eq!(config.proto_includes.unwrap(), vec!["/include/path"]);
    }

    #[tokio::test]
    async fn test_protobuf_codec_builder_with_valid_config() {
        let config_json = serde_json::json!({
            "proto_inputs": ["/path/to/file.proto"],
            "message_type": "MyMessage"
        });

        // This will fail at runtime because the file doesn't exist,
        // but we can at least test that config parsing works
        let config: ProtobufCodecConfig = serde_json::from_value(config_json).unwrap();
        assert_eq!(config.message_type, "MyMessage");
    }

    #[tokio::test]
    async fn test_protobuf_codec_builder_without_config() {
        let builder = ProtobufCodecBuilder;
        let result = builder.build(
            Some(&"test-codec".to_string()),
            &None,
            &create_test_resource(),
        );

        assert!(result.is_err());
        assert!(matches!(result, Err(Error::Config(_))));
    }

    #[tokio::test]
    async fn test_protobuf_codec_config_impl() {
        let config = ProtobufCodecConfig {
            proto_inputs: vec!["test.proto".to_string()],
            proto_includes: Some(vec!["/include".to_string()]),
            message_type: "TestMessage".to_string(),
        };

        assert_eq!(config.proto_inputs(), &vec!["test.proto".to_string()]);
        assert_eq!(config.proto_includes(), &Some(vec!["/include".to_string()]));
    }

    #[tokio::test]
    async fn test_protobuf_codec_builder_invalid_json() {
        let builder = ProtobufCodecBuilder;
        let invalid_json = serde_json::json!({
            "proto_inputs": "should_be_array"
        });

        let result = builder.build(
            Some(&"test-codec".to_string()),
            &Some(invalid_json),
            &create_test_resource(),
        );

        // Should fail due to invalid JSON structure
        assert!(result.is_err());
    }

    fn create_test_proto_file() -> Result<(TempDir, std::path::PathBuf), Error> {
        let dir = tempfile::tempdir()
            .map_err(|e| Error::Process(format!("Failed to create temp dir: {}", e)))?;
        let proto_dir = dir.path().join("proto");
        std::fs::create_dir_all(&proto_dir)
            .map_err(|e| Error::Process(format!("Failed to create proto dir: {}", e)))?;
        let proto_file_path = proto_dir.join("test_message.proto");
        std::fs::write(
            &proto_file_path,
            r#"syntax = "proto3";

package test;

message TestMessage {
  int64 timestamp = 1;
  double value = 2;
  string sensor = 3;
}
"#,
        )
        .map_err(|e| Error::Process(format!("Failed to write proto file: {}", e)))?;
        Ok((dir, proto_dir))
    }

    #[tokio::test]
    async fn test_codec_round_trip() -> Result<(), Error> {
        let (_x, proto_dir) = create_test_proto_file()?;
        let config = serde_json::json!({
            "proto_inputs": [proto_dir.to_string_lossy()],
            "message_type": "test.TestMessage",
        });
        let codec = ProtobufCodecBuilder.build(None, &Some(config), &create_test_resource())?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("sensor", DataType::Utf8, false),
        ]));
        let rb = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1634567890])),
                Arc::new(Float64Array::from(vec![42.5])),
                Arc::new(StringArray::from(vec!["temperature"])),
            ],
        )
        .map_err(|e| Error::Process(format!("Failed to create record batch: {}", e)))?;
        let original = MessageBatch::new_arrow(rb);

        let encoded = codec.encode(original).await?;
        assert_eq!(encoded.len(), 1, "one row → one encoded message");
        let decoded = codec.decode(encoded).await?;
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded.column(0).data_type(), &DataType::Int64);

        Ok(())
    }
}
