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
use crate::{Error, MessageBatch, Resource};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

lazy_static::lazy_static! {
    static ref CODEC_BUILDERS: RwLock<HashMap<String, Arc<dyn CodecBuilder>>> = RwLock::new(HashMap::new());
}

pub trait Encoder: Send + Sync {
    fn encode(&self, b: MessageBatch) -> Result<MessageBatch, Error>;
}

pub trait Decoder: Send + Sync {
    fn decode(&self, b: MessageBatch) -> Result<MessageBatch, Error>;
}

pub trait Codec: Encoder + Decoder {}

// Implement the Codec trait for any type that implements Encoder and Decoder
impl<T> Codec for T where T: Encoder + Decoder {}

pub trait CodecBuilder: Send + Sync {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<serde_json::Value>,
        resource: &Resource,
    ) -> Result<Arc<dyn Codec>, Error>;
}

/// Buffer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodecConfig {
    #[serde(rename = "type")]
    pub codec_type: String,
    pub name: Option<String>,
    #[serde(flatten)]
    pub config: Option<serde_json::Value>,
}

impl CodecConfig {
    /// Building codec components
    pub fn build(&self, resource: &Resource) -> Result<Arc<dyn Codec>, Error> {
        let builders = CODEC_BUILDERS.read().unwrap();

        if let Some(builder) = builders.get(&self.codec_type) {
            builder.build(self.name.as_ref(), &self.config, resource)
        } else {
            Err(Error::Config(format!(
                "Unknown codec type: {}",
                self.codec_type
            )))
        }
    }
}

pub fn register_codec_builder(
    type_name: &str,
    builder: Arc<dyn CodecBuilder>,
) -> Result<(), Error> {
    let mut builders = CODEC_BUILDERS.write().unwrap();
    if builders.contains_key(type_name) {
        return Err(Error::Config(format!(
            "Codec type already registered: {}",
            type_name
        )));
    }
    builders.insert(type_name.to_string(), builder);
    Ok(())
}
