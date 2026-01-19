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

//! Helper functions for codec integration in output components

use arkflow_core::codec::Codec;
use arkflow_core::{Bytes, Error, MessageBatch, MessageBatchRef, DEFAULT_BINARY_VALUE_FIELD};
use std::sync::Arc;

/// Apply codec encoding to message batch
///
/// # Arguments
/// * `msg` - The message batch to encode
/// * `codec` - Optional codec to apply
///
/// # Returns
/// * `Ok(Vec<Bytes>)` - Encoded bytes or binary representation
/// * `Err(Error)` - If codec application fails
pub fn apply_codec_encode(
    msg: &MessageBatchRef,
    codec: &Option<Arc<dyn Codec>>,
) -> Result<Vec<Bytes>, Error> {
    if let Some(c) = codec {
        // Clone the MessageBatch for encoding
        let msg_clone = (**msg).clone();
        c.encode(msg_clone)
    } else {
        // Default: convert to binary
        let binary_data = msg.to_binary(DEFAULT_BINARY_VALUE_FIELD)?;
        // Convert Vec<&[u8]> to Vec<Vec<u8>>
        Ok(binary_data.into_iter().map(|b| b.to_vec()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::MessageBatch;

    #[test]
    fn test_apply_codec_encode_no_codec() {
        // Create a simple message batch
        let batch = MessageBatch::new_binary(vec![b"test data".to_vec()]).unwrap();
        let msg_ref = std::sync::Arc::new(batch);
        let codec: Option<Arc<dyn Codec>> = None;

        let result = apply_codec_encode(&msg_ref, &codec);
        assert!(result.is_ok());

        let bytes = result.unwrap();
        assert_eq!(bytes.len(), 1);
        assert_eq!(bytes[0], b"test data".to_vec());
    }
}
