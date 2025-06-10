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

//! Temporary component module
//!
//! This module contains the Temporary trait and its associated builder.

use crate::{Error, MessageBatch, Resource};
use async_trait::async_trait;
use datafusion::logical_expr::ColumnarValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

lazy_static::lazy_static! {
    static ref TEMPORARY_BUILDERS: RwLock<HashMap<String, Arc<dyn TemporaryBuilder>>> = RwLock::new(HashMap::new());
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporaryConfig {
    #[serde(rename = "type")]
    pub temporary_type: String,
    pub name: String,
    #[serde(flatten)]
    pub config: Option<serde_json::Value>,
}

#[async_trait]
pub trait Temporary: Send + Sync {
    async fn connect(&self) -> Result<(), Error>;
    async fn get(&self, keys: &[ColumnarValue]) -> Result<Option<MessageBatch>, Error>;
    async fn close(&self) -> Result<(), Error>;
}

pub trait TemporaryBuilder: Send + Sync {
    fn build(
        &self,
        config: &Option<serde_json::Value>,
        resource: &Resource,
    ) -> Result<Arc<dyn Temporary>, Error>;
}

impl TemporaryConfig {
    /// Build the temporary component according to the configuration
    pub fn build(&self, resource: &Resource) -> Result<Arc<dyn Temporary>, Error> {
        let builders = TEMPORARY_BUILDERS.read().unwrap();

        if let Some(builder) = builders.get(&self.temporary_type) {
            builder.build(&self.config, resource)
        } else {
            Err(Error::Config(format!(
                "Unknown temporary type: {}",
                self.temporary_type
            )))
        }
    }
}

pub fn register_temporary_builder(
    type_name: &str,
    builder: Arc<dyn TemporaryBuilder>,
) -> Result<(), Error> {
    let mut builders = TEMPORARY_BUILDERS.write().unwrap();
    if builders.contains_key(type_name) {
        return Err(Error::Config(format!(
            "Temporary type already registered: {}",
            type_name
        )));
    }
    builders.insert(type_name.to_string(), builder);
    Ok(())
}
