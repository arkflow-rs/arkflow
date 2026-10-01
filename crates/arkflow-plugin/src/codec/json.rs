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
use crate::component;
use arkflow_core::codec::{Codec, CodecBuilder, Decoder, Encoder};
use arkflow_core::component::{register_codec_metadata, ComponentMetadata};
use arkflow_core::{codec, Bytes, Error, MessageBatch, Resource};
use async_trait::async_trait;
use datafusion::arrow;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tracing::warn;

/// Per-message decode-error policy: `fail` (default) fails the whole batch,
/// `skip` isolates bad messages and decodes the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OnError {
    #[default]
    Fail,
    Skip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
struct JsonCodecConfig {
    #[serde(default)]
    on_error: OnError,
}

struct JsonCodec {
    on_error: OnError,
}

#[async_trait]
impl Encoder for JsonCodec {
    async fn encode(&self, batch: MessageBatch) -> Result<Vec<Bytes>, Error> {
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
        let new_batch: Vec<Bytes> = json_str.lines().map(|s| s.as_bytes().to_vec()).collect();
        Ok(new_batch)
    }
}

#[async_trait]
impl Decoder for JsonCodec {
    async fn decode(&self, b: Vec<Bytes>) -> Result<MessageBatch, Error> {
        if self.on_error == OnError::Skip {
            return self.decode_isolating(b).await;
        }
        // Default: one bad message fails the whole batch (unchanged path).
        let json_data: Vec<u8> = b.join(b"\n" as &[u8]);

        let arrow = component::json::try_to_arrow(&json_data, None)?;
        Ok(MessageBatch::new_arrow(arrow))
    }
}

impl JsonCodec {
    /// `on_error: skip` path: decode message by message, warn-and-drop the bad
    /// ones, and merge the good ones on the union schema.
    async fn decode_isolating(&self, b: Vec<Bytes>) -> Result<MessageBatch, Error> {
        let mut batches = Vec::with_capacity(b.len());
        let mut skipped = 0usize;
        for (idx, bytes) in b.iter().enumerate() {
            match component::json::try_to_arrow(bytes, None) {
                Ok(rb) => batches.push(rb),
                Err(e) => {
                    warn!(
                        "json codec: skipping message #{} ({} bytes): {}",
                        idx,
                        bytes.len(),
                        e
                    );
                    skipped += 1;
                }
            }
        }
        if batches.is_empty() {
            return Err(Error::Process(format!(
                "json codec: all {} messages in the batch failed to decode",
                skipped
            )));
        }
        if skipped > 0 {
            warn!(
                "json codec: skipped {} of {} messages in the batch",
                skipped,
                b.len()
            );
        }
        let merged = crate::component::batch_merge::normalize_and_concat(&batches)?;
        Ok(MessageBatch::new_arrow(merged))
    }
}

struct JsonCodecBuilder;
impl CodecBuilder for JsonCodecBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Codec>, Error> {
        let config: JsonCodecConfig = match config {
            Some(v) => serde_json::from_value(v.clone())?,
            None => JsonCodecConfig::default(),
        };
        Ok(Arc::new(JsonCodec {
            on_error: config.on_error,
        }))
    }
}

pub(crate) fn init() -> Result<(), Error> {
    codec::register_codec_builder("json", Arc::new(JsonCodecBuilder))?;
    register_codec_metadata(ComponentMetadata::with_schema(
        "json",
        "Encodes/decodes Arrow RecordBatches as JSON byte payloads.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "on_error": {"type": "string", "enum": ["fail", "skip"], "default": "fail", "description": "Decode-error policy: `fail` fails the batch (default), `skip` isolates bad messages with a warning and decodes the rest."}
            }
        }),
    ).with_optional())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_resource() -> Resource {
        Resource {
            temporary: std::collections::HashMap::new(),
            input_names: std::cell::RefCell::new(Vec::new()),
        }
    }

    #[tokio::test]
    async fn test_json_codec_encode() {
        let codec = JsonCodec {
            on_error: OnError::Fail,
        };

        // Create a simple message batch
        let batch = MessageBatch::new_binary(vec![
            br#"{"name":"Alice","age":30}"#.to_vec(),
            br#"{"name":"Bob","age":25}"#.to_vec(),
        ])
        .unwrap();

        let result = codec.encode(batch).await;
        assert!(result.is_ok());
        let encoded = result.unwrap();

        // Should have encoded the data
        assert!(!encoded.is_empty());
    }

    #[tokio::test]
    async fn test_json_codec_decode() {
        let codec = JsonCodec {
            on_error: OnError::Fail,
        };

        let json_data = vec![
            br#"{"name":"Alice","age":30}"#.to_vec(),
            br#"{"name":"Bob","age":25}"#.to_vec(),
        ];

        let result = codec.decode(json_data).await;
        assert!(result.is_ok());
        let batch = result.unwrap();

        // Should have decoded to a message batch
        assert!(!batch.is_empty());
    }

    #[tokio::test]
    async fn test_json_codec_encode_decode_roundtrip() {
        let codec = JsonCodec {
            on_error: OnError::Fail,
        };

        let original_data = vec![
            br#"{"id":1,"value":"test1"}"#.to_vec(),
            br#"{"id":2,"value":"test2"}"#.to_vec(),
        ];

        // Decode
        let batch = codec.decode(original_data.clone()).await.unwrap();

        // Encode
        let encoded = codec.encode(batch.clone()).await.unwrap();

        // Decode again
        let final_batch = codec.decode(encoded).await.unwrap();

        // Both batches should have the same number of rows
        assert_eq!(batch.len(), final_batch.len());
    }

    #[tokio::test]
    async fn test_json_codec_decode_empty() {
        let codec = JsonCodec {
            on_error: OnError::Fail,
        };
        let result = codec.decode(vec![]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_json_codec_decode_invalid_json() {
        let codec = JsonCodec {
            on_error: OnError::Fail,
        };
        let invalid_data = vec![b"{invalid json}".to_vec()];
        let result = codec.decode(invalid_data).await;
        // Invalid JSON must surface as a decode error, never as a silent
        // success (the old `is_err() || is_ok()` form was a tautology).
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_json_codec_builder() {
        let builder = JsonCodecBuilder;
        let result = builder.build(
            Some(&"test-codec".to_string()),
            &None,
            &create_test_resource(),
        );

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_json_codec_builder_with_config() {
        let builder = JsonCodecBuilder;
        let config = serde_json::json!({});

        let result = builder.build(
            Some(&"test-codec".to_string()),
            &Some(config),
            &create_test_resource(),
        );

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_json_codec_encode_single_message() {
        let codec = JsonCodec {
            on_error: OnError::Fail,
        };
        let batch = MessageBatch::new_binary(vec![br#"{"test":"data"}"#.to_vec()]).unwrap();

        let result = codec.encode(batch).await;
        assert!(result.is_ok());
        let encoded = result.unwrap();

        assert!(!encoded.is_empty());
    }

    #[tokio::test]
    async fn test_json_codec_decode_complex_json() {
        let codec = JsonCodec {
            on_error: OnError::Fail,
        };

        let complex_json = vec![
            br#"{"user":{"name":"Alice","tags":["admin","user"]},"active":true}"#.to_vec(),
            br#"{"user":{"name":"Bob","tags":["user"]},"active":false}"#.to_vec(),
        ];

        let result = codec.decode(complex_json).await;
        assert!(result.is_ok());
        let batch = result.unwrap();

        assert!(!batch.is_empty());
    }

    #[tokio::test]
    async fn test_json_codec_skip_isolates_bad_messages() {
        let codec = JsonCodec {
            on_error: OnError::Skip,
        };
        let data = vec![
            br#"{"id":1,"name":"a"}"#.to_vec(),
            b"{invalid".to_vec(),
            br#"{"id":3,"name":"c"}"#.to_vec(),
        ];
        let batch = codec.decode(data).await.unwrap();
        assert_eq!(batch.len(), 2);
        use datafusion::arrow::array::AsArray;
        let id_col = batch.record_batch().column_by_name("id").unwrap();
        assert_eq!(
            id_col
                .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
                .value(0),
            1
        );
        assert_eq!(
            id_col
                .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
                .value(1),
            3
        );
    }

    #[tokio::test]
    async fn test_json_codec_skip_all_bad_errors() {
        let codec = JsonCodec {
            on_error: OnError::Skip,
        };
        let result = codec
            .decode(vec![b"{invalid".to_vec(), b"also bad".to_vec()])
            .await;
        assert!(
            result.is_err(),
            "all-bad batch must error, not return an empty batch"
        );
    }

    #[tokio::test]
    async fn test_json_codec_skip_merges_heterogeneous_good_messages() {
        let codec = JsonCodec {
            on_error: OnError::Skip,
        };
        let data = vec![
            br#"{"id":1,"name":"a"}"#.to_vec(),
            br#"{"id":2}"#.to_vec(), // missing `name`: null-filled on the union schema
        ];
        let batch = codec.decode(data).await.unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.record_batch().num_columns(), 2);
        use datafusion::arrow::array::Array;
        let name_col = batch.record_batch().column_by_name("name").unwrap();
        assert!(name_col.is_null(1));
    }

    #[tokio::test]
    async fn test_json_codec_builder_on_error_config() {
        let builder = JsonCodecBuilder;
        let config = serde_json::json!({"on_error": "skip"});
        let codec = builder
            .build(
                Some(&"test-codec".to_string()),
                &Some(config),
                &create_test_resource(),
            )
            .unwrap();
        let data = vec![b"bad".to_vec(), br#"{"ok":1}"#.to_vec()];
        let batch = codec.decode(data).await.unwrap();
        assert_eq!(batch.len(), 1);
    }

    #[tokio::test]
    async fn test_json_codec_builder_rejects_unknown_policy() {
        let builder = JsonCodecBuilder;
        let config = serde_json::json!({"on_error": "explode"});
        assert!(builder
            .build(
                Some(&"test-codec".to_string()),
                &Some(config),
                &create_test_resource()
            )
            .is_err());
    }
}
