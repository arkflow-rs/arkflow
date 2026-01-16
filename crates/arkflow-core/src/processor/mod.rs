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

//! Processor component module
//!
//! The processor component is responsible for transforming, filtering, enriching, and so on.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::{Error, MessageBatchRef, ProcessResult, Resource};

lazy_static::lazy_static! {
    static ref PROCESSOR_BUILDERS: RwLock<HashMap<String, Arc<dyn ProcessorBuilder>>> = RwLock::new(HashMap::new());
}

/// Characteristic interface of the processor component
#[async_trait]
pub trait Processor: Send + Sync {
    /// Process messages using Arc for zero-copy
    ///
    /// # Zero-Copy Example
    ///
    /// ```rust,no_run
    /// use arkflow_core::{MessageBatchRef, ProcessResult};
    /// use arkflow_core::Error;
    ///
    /// struct MyProcessor;
    ///
    /// impl MyProcessor {
    ///     async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
    ///         // Forward without copying
    ///         Ok(ProcessResult::Single(batch))
    ///     }
    /// }
    /// ```
    ///
    /// # Conditional Processing
    ///
    /// ```rust,no_run
    /// use arkflow_core::{MessageBatch, MessageBatchRef, ProcessResult, Error};
    /// use std::sync::Arc;
    ///
    /// fn needs_transform(_: &MessageBatchRef) -> bool { false }
    /// fn transform(_: &MessageBatch) -> Result<MessageBatch, Error> {
    ///     Ok(MessageBatch::new_binary(vec![]).unwrap())
    /// }
    ///
    /// struct MyProcessor;
    ///
    /// impl MyProcessor {
    ///     async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
    ///         if needs_transform(&batch) {
    ///             let new_batch = transform(&*batch)?;
    ///             Ok(ProcessResult::Single(Arc::new(new_batch)))
    ///         } else {
    ///             Ok(ProcessResult::Single(batch))
    ///         }
    ///     }
    /// }
    /// ```
    async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error>;

    /// Turn off the processor
    async fn close(&self) -> Result<(), Error>;
}

/// Processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorConfig {
    #[serde(rename = "type")]
    pub processor_type: String,
    pub name: Option<String>,
    #[serde(flatten)]
    pub config: Option<serde_json::Value>,
}

impl ProcessorConfig {
    /// Build the processor components according to the configuration
    pub fn build(&self, resource: &Resource) -> Result<Arc<dyn Processor>, Error> {
        let builders = PROCESSOR_BUILDERS.read().unwrap();

        if let Some(builder) = builders.get(&self.processor_type) {
            builder.build(self.name.as_ref(), &self.config, resource)
        } else {
            Err(Error::Config(format!(
                "Unknown processor type: {}",
                self.processor_type
            )))
        }
    }
}

pub trait ProcessorBuilder: Send + Sync {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<serde_json::Value>,
        resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error>;
}

pub fn register_processor_builder(
    type_name: &str,
    builder: Arc<dyn ProcessorBuilder>,
) -> Result<(), Error> {
    let mut builders = PROCESSOR_BUILDERS.write().unwrap();
    if builders.contains_key(type_name) {
        return Err(Error::Config(format!(
            "Processor type already registered: {}",
            type_name
        )));
    }
    builders.insert(type_name.to_string(), builder);
    Ok(())
}
