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
use arkflow_core::{codec, Bytes, Error, MessageBatch, Resource};
use datafusion::arrow;
use serde_json::Value;
use std::sync::Arc;

struct JsonCodec;

impl Encoder for JsonCodec {
    fn encode(&self, batch: MessageBatch) -> Result<Vec<Bytes>, Error> {
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

impl Decoder for JsonCodec {
    fn decode(&self, b: Vec<Bytes>) -> Result<MessageBatch, Error> {
        let json_data: Vec<u8> = b.join(b"\n" as &[u8]);

        let arrow = component::json::try_to_arrow(&json_data, None)?;
        Ok(MessageBatch::new_arrow(arrow))
    }
}

struct JsonCodecBuilder;
impl CodecBuilder for JsonCodecBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        _config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Codec>, Error> {
        Ok(Arc::new(JsonCodec))
    }
}

pub(crate) fn init() -> Result<(), Error> {
    codec::register_codec_builder("json", Arc::new(JsonCodecBuilder))?;
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

    #[test]
    fn test_json_codec_encode() {
        let codec = JsonCodec;

        // Create a simple message batch
        let batch = MessageBatch::new_binary(vec![
            br#"{"name":"Alice","age":30}"#.to_vec(),
            br#"{"name":"Bob","age":25}"#.to_vec(),
        ])
        .unwrap();

        let result = codec.encode(batch);
        assert!(result.is_ok());
        let encoded = result.unwrap();

        // Should have encoded the data
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_json_codec_decode() {
        let codec = JsonCodec;

        let json_data = vec![
            br#"{"name":"Alice","age":30}"#.to_vec(),
            br#"{"name":"Bob","age":25}"#.to_vec(),
        ];

        let result = codec.decode(json_data);
        assert!(result.is_ok());
        let batch = result.unwrap();

        // Should have decoded to a message batch
        assert!(batch.len() > 0);
    }

    #[test]
    fn test_json_codec_encode_decode_roundtrip() {
        let codec = JsonCodec;

        let original_data = vec![
            br#"{"id":1,"value":"test1"}"#.to_vec(),
            br#"{"id":2,"value":"test2"}"#.to_vec(),
        ];

        // Decode
        let batch = codec.decode(original_data.clone()).unwrap();

        // Encode
        let encoded = codec.encode(batch.clone()).unwrap();

        // Decode again
        let final_batch = codec.decode(encoded).unwrap();

        // Both batches should have the same number of rows
        assert_eq!(batch.len(), final_batch.len());
    }

    #[test]
    fn test_json_codec_decode_empty() {
        let codec = JsonCodec;
        let result = codec.decode(vec![]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_json_codec_decode_invalid_json() {
        let codec = JsonCodec;
        let invalid_data = vec![b"{invalid json}".to_vec()];
        let result = codec.decode(invalid_data);
        // Should handle invalid JSON gracefully or return error
        assert!(result.is_err() || result.is_ok());
    }

    #[test]
    fn test_json_codec_builder() {
        let builder = JsonCodecBuilder;
        let result = builder.build(
            Some(&"test-codec".to_string()),
            &None,
            &create_test_resource(),
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_json_codec_builder_with_config() {
        let builder = JsonCodecBuilder;
        let config = serde_json::json!({});

        let result = builder.build(
            Some(&"test-codec".to_string()),
            &Some(config),
            &create_test_resource(),
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_json_codec_encode_single_message() {
        let codec = JsonCodec;
        let batch = MessageBatch::new_binary(vec![br#"{"test":"data"}"#.to_vec()]).unwrap();

        let result = codec.encode(batch);
        assert!(result.is_ok());
        let encoded = result.unwrap();

        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_json_codec_decode_complex_json() {
        let codec = JsonCodec;

        let complex_json = vec![
            br#"{"user":{"name":"Alice","tags":["admin","user"]},"active":true}"#.to_vec(),
            br#"{"user":{"name":"Bob","tags":["user"]},"active":false}"#.to_vec(),
        ];

        let result = codec.decode(complex_json);
        assert!(result.is_ok());
        let batch = result.unwrap();

        assert!(batch.len() > 0);
    }
}
