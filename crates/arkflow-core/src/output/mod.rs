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

//! Output component module
//!
//! The output component is responsible for sending the processed data to the target system.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::{Error, MessageBatchRef, Resource};

lazy_static::lazy_static! {
    static ref OUTPUT_BUILDERS: RwLock<HashMap<String, Arc<dyn OutputBuilder>>> = RwLock::new(HashMap::new());
}
/// Feature interface of the output component
#[async_trait]
pub trait Output: Send + Sync {
    /// Connect to the output destination
    async fn connect(&self) -> Result<(), Error>;

    /// Write a message using Arc for zero-copy
    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error>;

    /// Close the output destination connection
    async fn close(&self) -> Result<(), Error>;
}

/// Output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    #[serde(rename = "type")]
    pub output_type: String,
    pub name: Option<String>,
    #[serde(flatten)]
    pub config: Option<serde_json::Value>,
}

impl OutputConfig {
    /// Build the output component according to the configuration
    pub fn build(&self, resource: &Resource) -> Result<Arc<dyn Output>, Error> {
        let builders = OUTPUT_BUILDERS.read().unwrap();

        if let Some(builder) = builders.get(&self.output_type) {
            builder.build(self.name.as_ref(), &self.config, resource)
        } else {
            Err(Error::Config(format!(
                "Unknown output type: {}",
                self.output_type
            )))
        }
    }
}

pub trait OutputBuilder: Send + Sync {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<serde_json::Value>,
        resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error>;
}

pub fn register_output_builder(
    type_name: &str,
    builder: Arc<dyn OutputBuilder>,
) -> Result<(), Error> {
    let mut builders = OUTPUT_BUILDERS.write().unwrap();
    if builders.contains_key(type_name) {
        return Err(Error::Config(format!(
            "Output type already registered: {}",
            type_name
        )));
    }
    builders.insert(type_name.to_string(), builder);
    Ok(())
}
