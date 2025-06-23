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

use arkflow_core::codec::{Codec, CodecBuilder, Decoder, Encoder};
use arkflow_core::{codec, Bytes, Error, MessageBatch, Resource};
use crate::component::protobuf::{arrow_to_protobuf, parse_proto_file, protobuf_to_arrow, ProtobufConfig};
use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use prost_reflect::{MessageDescriptor};
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

    fn message_type(&self) -> &String {
        &self.message_type
    }
}

/// Protobuf Codec
struct ProtobufCodec {
    config: ProtobufCodecConfig,
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
            config,
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
