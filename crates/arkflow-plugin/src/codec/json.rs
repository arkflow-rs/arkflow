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
use arkflow_core::codec::{Codec, CodecBuilder};
use arkflow_core::{codec, Error, MessageBatch, Resource, DEFAULT_BINARY_VALUE_FIELD};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl JsonCodec {
    fn new(config: JsonCodecConfig) -> Result<Self, Error> {
        Ok(JsonCodec { config })
    }
}

struct JsonCodecBuilder;
impl CodecBuilder for JsonCodecBuilder {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<Value>,
        resource: &Resource,
    ) -> Result<Arc<dyn Codec>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Json codec configuration is missing".to_string(),
            ));
        }

        let config: JsonCodecConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(JsonCodec::new(config)?))
    }
}

pub(crate) fn init() -> Result<(), Error> {
    codec::register_codec_builder("json", Arc::new(JsonCodecBuilder))?;
    Ok(())
}
