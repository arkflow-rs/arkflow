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
use arkflow_core::codec::Codec;
use arkflow_core::{Error, MessageBatch, DEFAULT_BINARY_VALUE_FIELD};

struct JsonCodecConfig {
    value_field: Option<String>,
}
struct JsonCodec {
    config: JsonCodecConfig,
}

impl Codec for JsonCodec {
    fn encode(&self, b: MessageBatch) -> Result<MessageBatch, Error> {
        // let json_data = component::json(&b)?;
        todo!()
    }

    fn decode(&self, b: MessageBatch) -> Result<MessageBatch, Error> {
        let result = b.to_binary(
            self.config
                .value_field
                .as_deref()
                .unwrap_or(DEFAULT_BINARY_VALUE_FIELD),
        )?;
        let json_data: Vec<u8> = result.join(b"\n" as &[u8]);

        let arrow = component::json::try_to_arrow(&json_data, None)?;
        Ok(MessageBatch::new_arrow(arrow))
    }
}
