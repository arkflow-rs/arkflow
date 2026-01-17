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
//! The codec used to convert between Protobuf data and the Arrow format

use crate::component::protobuf::{
    arrow_to_protobuf, parse_proto_file, protobuf_to_arrow, ProtobufConfig,
};
use arkflow_core::codec::{Codec, CodecBuilder, Decoder, Encoder};
use arkflow_core::{codec, Bytes, Error, MessageBatch, Resource};
use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use prost_reflect::MessageDescriptor;
use serde::{Deserialize, Serialize};
use serde_json;
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

impl Encoder for ProtobufCodec {
    fn encode(&self, b: MessageBatch) -> Result<Vec<Bytes>, Error> {
        arrow_to_protobuf(&self.descriptor, &b)
    }
}

impl Decoder for ProtobufCodec {
    fn decode(&self, b: Vec<Bytes>) -> Result<MessageBatch, Error> {
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    fn create_test_resource() -> Resource {
        Resource {
            temporary: Default::default(),
            input_names: RefCell::new(Default::default()),
        }
    }

    #[test]
    fn test_protobuf_codec_config_deserialization() {
        let config_json = serde_json::json!({
            "proto_inputs": ["/path/to/file.proto"],
            "message_type": "MyMessage"
        });

        let config: ProtobufCodecConfig = serde_json::from_value(config_json).unwrap();
        assert_eq!(config.proto_inputs, vec!["/path/to/file.proto"]);
        assert_eq!(config.message_type, "MyMessage");
        assert!(config.proto_includes.is_none());
    }

    #[test]
    fn test_protobuf_codec_config_with_includes() {
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

    #[test]
    fn test_protobuf_codec_builder_with_valid_config() {
        let config_json = serde_json::json!({
            "proto_inputs": ["/path/to/file.proto"],
            "message_type": "MyMessage"
        });

        // This will fail at runtime because the file doesn't exist,
        // but we can at least test that config parsing works
        let config: ProtobufCodecConfig = serde_json::from_value(config_json).unwrap();
        assert_eq!(config.message_type, "MyMessage");
    }

    #[test]
    fn test_protobuf_codec_builder_without_config() {
        let builder = ProtobufCodecBuilder;
        let result = builder.build(
            Some(&"test-codec".to_string()),
            &None,
            &create_test_resource(),
        );

        assert!(result.is_err());
        assert!(matches!(result, Err(Error::Config(_))));
    }

    #[test]
    fn test_protobuf_codec_config_impl() {
        let config = ProtobufCodecConfig {
            proto_inputs: vec!["test.proto".to_string()],
            proto_includes: Some(vec!["/include".to_string()]),
            message_type: "TestMessage".to_string(),
        };

        assert_eq!(config.proto_inputs(), &vec!["test.proto".to_string()]);
        assert_eq!(config.proto_includes(), &Some(vec!["/include".to_string()]));
    }

    #[test]
    fn test_protobuf_codec_builder_invalid_json() {
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
}
