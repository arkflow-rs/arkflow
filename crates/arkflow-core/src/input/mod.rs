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

use crate::{Error, MessageBatch};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock};

lazy_static::lazy_static! {
    static ref INPUT_BUILDERS: RwLock<HashMap<String, Arc<dyn InputBuilder>>> = RwLock::new(HashMap::new());
}

pub trait InputBuilder: Send + Sync {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Input>, Error>;
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

pub struct VecAck(Vec<Arc<dyn Ack>>);

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
    #[serde(flatten)]
    pub config: Option<serde_json::Value>,
}

impl InputConfig {
    /// Building input components
    pub fn build(&self) -> Result<Arc<dyn Input>, Error> {
        let builders = INPUT_BUILDERS.read().unwrap();

        if let Some(builder) = builders.get(&self.input_type) {
            builder.build(&self.config)
        } else {
            Err(Error::Config(format!(
                "Unknown input type: {}",
                self.input_type
            )))
        }
    }
}

pub fn register_input_builder(type_name: &str, builder: Arc<dyn InputBuilder>) {
    let mut builders = INPUT_BUILDERS.write().unwrap();
    if builders.contains_key(type_name) {
        panic!("Input type already registered: {}", type_name)
    }
    builders.insert(type_name.to_string(), builder);
}

pub fn get_registered_input_types() -> Vec<String> {
    let builders = INPUT_BUILDERS.read().unwrap();
    builders.keys().cloned().collect()
}
