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
