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

//! Input component module
//!
//! The input component is responsible for receiving data from various sources such as message queues, file systems, HTTP endpoints, and so on.

use crate::codec::{Codec, CodecConfig, Encoder};
use crate::{Error, MessageBatch, Resource};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock};

lazy_static::lazy_static! {
    static ref INPUT_BUILDERS: RwLock<HashMap<String, Arc<dyn InputBuilder>>> = RwLock::new(HashMap::new());
}

pub trait InputBuilder: Send + Sync {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<serde_json::Value>,
        resource: &mut Resource,
    ) -> Result<Arc<dyn Input>, Error>;
}

#[async_trait]
pub trait Ack: Send + Sync {
    async fn ack(&self);
}

#[async_trait]
pub trait Input: Send + Sync {
    /// Connect to the input source
    async fn connect(&self) -> Result<(), Error>;

    /// Read the message from the input source
    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error>;

    /// Close the input source connection
    async fn close(&self) -> Result<(), Error>;
}

pub struct NoopAck;

#[async_trait]
impl Ack for NoopAck {
    async fn ack(&self) {}
}

pub struct VecAck(pub Vec<Arc<dyn Ack>>);

#[async_trait]
impl Ack for VecAck {
    async fn ack(&self) {
        for ack in &self.0 {
            ack.ack().await;
        }
    }
}

impl Deref for VecAck {
    type Target = Vec<Arc<dyn Ack>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for VecAck {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Arc<dyn Ack>> for VecAck {
    fn from(ack: Arc<dyn Ack>) -> Self {
        VecAck(vec![ack])
    }
}

/// Input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputConfig {
    #[serde(rename = "type")]
    pub input_type: String,
    pub name: Option<String>,
    pub decode: Option<CodecConfig>,
    #[serde(flatten)]
    pub config: Option<serde_json::Value>,
}

impl InputConfig {
    /// Building input components
    pub fn build(&self, resource: &mut Resource) -> Result<Arc<dyn InputEncoder>, Error> {
        let builders = INPUT_BUILDERS.read().unwrap();
        let encoder = if let Some(codec_config) = self.decode.as_ref() {
            let arc = codec_config.build(resource)?;
            Some(arc)
        } else {
            None
        };

        if let Some(builder) = builders.get(&self.input_type) {
            let input = builder.build(self.name.as_ref(), &self.config, resource)?;
            Ok(Arc::new(InputEncode { input, encoder }))
        } else {
            Err(Error::Config(format!(
                "Unknown input type: {}",
                self.input_type
            )))
        }
    }
}

pub struct InputEncode {
    pub input: Arc<dyn Input>,
    pub encoder: Option<Arc<dyn Codec>>,
}

impl InputEncoder for InputEncode {
    fn encode(&self, msg: MessageBatch) -> Result<MessageBatch, Error> {
        if let Some(e) = &self.encoder {
            MessageBatch::new_binary(e.encode(msg)?)
        } else {
            Ok(msg)
        }
    }
}

#[async_trait]
impl Input for InputEncode {
    async fn connect(&self) -> Result<(), Error> {
        self.input.connect().await
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        self.input.read().await
    }

    async fn close(&self) -> Result<(), Error> {
        self.input.close().await
    }
}

// #[async_trait]
pub trait InputEncoder: Input {
    fn encode(&self, msg: MessageBatch) -> Result<MessageBatch, Error>;
}

pub fn register_input_builder(
    type_name: &str,
    builder: Arc<dyn InputBuilder>,
) -> Result<(), Error> {
    let mut builders = INPUT_BUILDERS.write().unwrap();
    if builders.contains_key(type_name) {
        return Err(Error::Config(format!(
            "Input type already registered: {}",
            type_name
        )));
    }
    builders.insert(type_name.to_string(), builder);
    Ok(())
}
