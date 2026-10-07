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

use crate::component::protobuf::{
    arrow_to_protobuf, parse_proto_file, ProtobufBatchConverter, ProtobufConfig,
};
use arkflow_core::codec::{Codec, CodecBuilder, Decoder, Encoder};
use arkflow_core::component::{register_codec_metadata, ComponentMetadata};
use arkflow_core::{codec, Bytes, Error, MessageBatch, Resource};
use async_trait::async_trait;
use prost_reflect::MessageDescriptor;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::warn;

/// Per-message decode-error policy: `fail` (default) fails the whole batch,
/// `skip` isolates bad messages and decodes the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OnError {
    #[default]
    Fail,
    Skip,
}

/// Protobuf codec configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProtobufCodecConfig {
    /// Protobuf message type descriptor file paths
    proto_inputs: Vec<String>,
    /// Include paths for proto files
    proto_includes: Option<Vec<String>>,
    /// Protobuf message type name
    message_type: String,
    /// Decode-error policy (see [`OnError`]).
    #[serde(default)]
    on_error: OnError,
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
    on_error: OnError,
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
            on_error: config.on_error,
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
        // Every message shares this codec's single descriptor, so one
        // columnar converter accumulates the whole batch: the first message
        // tees the per-column plans, later messages append by field number,
        // and finish() emits one multi-row batch — no per-message schema
        // construction and no union-merge pass.
        let mut converter = ProtobufBatchConverter::new(self.descriptor.clone());
        let mut good = 0usize;
        let mut skipped = 0usize;

        for (idx, data) in b.iter().enumerate() {
            match converter.push(data) {
                Ok(()) => good += 1,
                Err(e) if self.on_error == OnError::Skip => {
                    warn!(
                        "protobuf codec: skipping message #{} ({} bytes): {}",
                        idx,
                        data.len(),
                        e
                    );
                    skipped += 1;
                }
                Err(e) => return Err(e),
            }
        }

        if skipped > 0 {
            warn!(
                "protobuf codec: skipped {} of {} messages in the batch",
                skipped,
                good + skipped
            );
        }
        if good == 0 {
            if skipped > 0 {
                return Err(Error::Process(format!(
                    "protobuf codec: all {} messages in the batch failed to decode",
                    skipped
                )));
            }
            return Ok(MessageBatch::new_arrow(converter.finish()?));
        }

        Ok(MessageBatch::new_arrow(converter.finish()?))
    }
}

struct ProtobufCodecBuilder;

impl CodecBuilder for ProtobufCodecBuilder {
    fn build(
        &self,
        _name: Option<&str>,
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
                "proto_includes": {"type": "array", "items": {"type": "string"}, "description": "Include paths for proto resolution."},
                "on_error": {"type": "string", "enum": ["fail", "skip"], "default": "fail", "description": "Decode-error policy: `fail` fails the batch (default), `skip` isolates bad messages with a warning and decodes the rest."}
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
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
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
        let result = builder.build(Some("test-codec"), &None, &create_test_resource());

        assert!(result.is_err());
        assert!(matches!(result, Err(Error::Config(_))));
    }

    #[tokio::test]
    async fn test_protobuf_codec_config_impl() {
        let config = ProtobufCodecConfig {
            proto_inputs: vec!["test.proto".to_string()],
            proto_includes: Some(vec!["/include".to_string()]),
            message_type: "TestMessage".to_string(),
            on_error: OnError::default(),
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
            Some("test-codec"),
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

    #[tokio::test]
    async fn test_codec_skip_isolates_bad_messages() -> Result<(), Error> {
        let (_x, proto_dir) = create_test_proto_file()?;
        let config = serde_json::json!({
            "proto_inputs": [proto_dir.to_string_lossy()],
            "message_type": "test.TestMessage",
            "on_error": "skip",
        });
        let codec = ProtobufCodecBuilder.build(None, &Some(config), &create_test_resource())?;

        let make_row = |t: i64| -> Result<MessageBatch, Error> {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "timestamp",
                DataType::Int64,
                false,
            )]));
            let rb = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![t]))])
                .map_err(|e| Error::Process(e.to_string()))?;
            Ok(MessageBatch::new_arrow(rb))
        };
        let good1 = codec
            .encode(make_row(1)?)
            .await?
            .into_iter()
            .next()
            .unwrap();
        let good2 = codec
            .encode(make_row(2)?)
            .await?
            .into_iter()
            .next()
            .unwrap();

        let decoded = codec
            .decode(vec![good1, b"garbage".to_vec(), good2])
            .await?;
        assert_eq!(decoded.len(), 2, "bad middle message must be skipped");
        // The surviving rows must be exactly the good messages, in order:
        // the converter accumulates pushes in input order, so the dropped
        // message leaves no hole and no reordering.
        let timestamps = decoded
            .record_batch()
            .column_by_name("timestamp")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>());
        assert_eq!(
            timestamps.map(|values| values.values().to_vec()),
            Some(vec![1i64, 2]),
            "surviving rows must be the good messages in order"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_codec_skip_all_bad_errors() -> Result<(), Error> {
        let (_x, proto_dir) = create_test_proto_file()?;
        let config = serde_json::json!({
            "proto_inputs": [proto_dir.to_string_lossy()],
            "message_type": "test.TestMessage",
            "on_error": "skip",
        });
        let codec = ProtobufCodecBuilder.build(None, &Some(config), &create_test_resource())?;
        let result = codec
            .decode(vec![b"garbage".to_vec(), b"more garbage".to_vec()])
            .await;
        let error = result.expect_err("all-bad batch must error").to_string();
        // The skip count is part of the observable outcome: the all-bad
        // error names how many messages were skipped.
        assert!(
            error.contains("all 2 messages"),
            "skip count must surface in the error: {error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_codec_fail_mode_keeps_whole_batch_failure() -> Result<(), Error> {
        let (_x, proto_dir) = create_test_proto_file()?;
        let config = serde_json::json!({
            "proto_inputs": [proto_dir.to_string_lossy()],
            "message_type": "test.TestMessage",
        });
        let codec = ProtobufCodecBuilder.build(None, &Some(config), &create_test_resource())?;
        let result = codec.decode(vec![b"garbage".to_vec()]).await;
        assert!(result.is_err(), "default policy must fail the batch");
        Ok(())
    }

    #[tokio::test]
    async fn test_codec_columnar_batch_equals_per_message_merge() -> Result<(), Error> {
        let (_x, proto_dir) = create_test_proto_file()?;
        let config = serde_json::json!({
            "proto_inputs": [proto_dir.to_string_lossy()],
            "message_type": "test.TestMessage",
        });
        let config: ProtobufCodecConfig = serde_json::from_value(config)?;
        let codec = ProtobufCodec::new(config)?;

        // Three rows with heterogeneous field presence, encoded one-by-one.
        let make_row = |t: i64, v: Option<f64>, s: Option<&str>| -> Result<MessageBatch, Error> {
            let schema = Arc::new(Schema::new(vec![
                Field::new("timestamp", DataType::Int64, true),
                Field::new("value", DataType::Float64, true),
                Field::new("sensor", DataType::Utf8, true),
            ]));
            let rb = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![Some(t)])),
                    Arc::new(Float64Array::from(vec![v])),
                    Arc::new(StringArray::from(vec![s])),
                ],
            )
            .map_err(|e| Error::Process(e.to_string()))?;
            Ok(MessageBatch::new_arrow(rb))
        };
        let mut encoded = Vec::with_capacity(3);
        for row in [
            make_row(1, Some(1.5), Some("a"))?,
            make_row(2, None, None)?,
            make_row(3, Some(3.5), Some("c"))?,
        ] {
            encoded.push(codec.encode(row).await.unwrap().into_iter().next().unwrap());
        }

        // New path: one converter, one multi-row batch.
        let columnar = codec.decode(encoded.clone()).await?;
        assert_eq!(columnar.len(), 3);

        // Legacy path: per-message decode then union-merge, as a reference.
        let per_message: Vec<RecordBatch> = encoded
            .iter()
            .map(|data| crate::component::protobuf::protobuf_to_arrow(&codec.descriptor, data))
            .collect::<Result<_, _>>()?;
        let merged = crate::component::batch_merge::normalize_and_concat(&per_message)?;

        assert_eq!(columnar.schema().fields(), merged.schema().fields());
        for i in 0..merged.num_columns() {
            assert_eq!(
                format!("{:?}", columnar.column(i)),
                format!("{:?}", merged.column(i)),
                "column {} must match the per-message-merge result",
                merged.schema().field(i).name()
            );
        }
        Ok(())
    }
}
