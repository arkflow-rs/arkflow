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

//! Helper functions for codec integration in input components

use arkflow_core::codec::{Codec, CodecConfig};
use arkflow_core::{Bytes, Error, MessageBatch, Resource};
use serde_json::Value;
use std::sync::Arc;

/// Extract codec configuration from input config
///
/// # Arguments
/// * `config` - The input configuration (may contain "codec" field)
/// * `resource` - The resource context for building codec
///
/// # Returns
/// * `Ok(Some(codec))` - If codec is configured
/// * `Ok(None)` - If no codec configured
/// * `Err(Error)` - If codec configuration is invalid
pub fn extract_codec_from_config(
    config: &Option<Value>,
    resource: &Resource,
) -> Result<Option<Arc<dyn Codec>>, Error> {
    if let Some(cfg) = config {
        if let Some(codec_cfg_val) = cfg.get("codec") {
            let codec_config: CodecConfig = serde_json::from_value(codec_cfg_val.clone())
                .map_err(|e| Error::Config(format!("Invalid codec config: {}", e)))?;
            return Ok(Some(codec_config.build(resource)?));
        }
    }
    Ok(None)
}

/// Apply codec to payload bytes
///
/// # Arguments
/// * `payload` - The raw payload bytes
/// * `codec` - Optional codec to apply
///
/// # Returns
/// * `Ok(MessageBatch)` - Decoded or binary-wrapped message batch
/// * `Err(Error)` - If codec application fails
pub fn apply_codec_to_payload(
    payload: &[u8],
    codec: &Option<Arc<dyn Codec>>,
) -> Result<MessageBatch, Error> {
    if let Some(c) = codec {
        c.decode(vec![payload.to_vec()])
    } else {
        MessageBatch::new_binary(vec![payload.to_vec()])
    }
}

/// Apply codec to multiple payload bytes
///
/// # Arguments
/// * `payloads` - Multiple raw payload bytes
/// * `codec` - Optional codec to apply
///
/// # Returns
/// * `Ok(MessageBatch)` - Decoded or binary-wrapped message batch
/// * `Err(Error)` - If codec application fails
pub fn apply_codec_to_payloads(
    payloads: Vec<Bytes>,
    codec: &Option<Arc<dyn Codec>>,
) -> Result<MessageBatch, Error> {
    if let Some(c) = codec {
        c.decode(payloads)
    } else {
        MessageBatch::new_binary(payloads)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_codec_from_config_no_codec() {
        let config: Option<Value> = None;
        let resource = Resource {
            temporary: std::collections::HashMap::new(),
            input_names: std::cell::RefCell::new(Vec::new()),
        };

        let result = extract_codec_from_config(&config, &resource);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_extract_codec_from_config_with_codec() {
        let config = Some(serde_json::json!({
            "codec": {
                "type": "json"
            }
        }));

        let resource = Resource {
            temporary: std::collections::HashMap::new(),
            input_names: std::cell::RefCell::new(Vec::new()),
        };

        let result = extract_codec_from_config(&config, &resource);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_apply_codec_to_payload_no_codec() {
        let payload = b"test data";
        let codec: Option<Arc<dyn Codec>> = None;

        let result = apply_codec_to_payload(payload, &codec);
        assert!(result.is_ok());

        let batch = result.unwrap();
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn test_apply_codec_to_payloads_no_codec() {
        let payloads = vec![b"data1".to_vec(), b"data2".to_vec()];
        let codec: Option<Arc<dyn Codec>> = None;

        let result = apply_codec_to_payloads(payloads, &codec);
        assert!(result.is_ok());

        let batch = result.unwrap();
        assert_eq!(batch.len(), 2);
    }
}
