//! Processor component module
//!
//! The processor component is responsible for transforming, filtering, enriching, and so on.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::{Error, MessageBatch};

mod udf;

lazy_static::lazy_static! {
    static ref PROCESSOR_BUILDERS: RwLock<HashMap<String, Arc<dyn ProcessorBuilder>>> = RwLock::new(HashMap::new());
}

/// Characteristic interface of the processor component
#[async_trait]
pub trait Processor: Send + Sync {
    /// Process messages
    async fn process(&self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error>;

    /// Turn off the processor
    async fn close(&self) -> Result<(), Error>;
}

/// Processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorConfig {
    #[serde(rename = "type")]
    pub processor_type: String,
    #[serde(flatten)]
    pub config: Option<serde_json::Value>,
}

impl ProcessorConfig {
    /// Build the processor components according to the configuration
    pub fn build(&self) -> Result<Arc<dyn Processor>, Error> {
        let builders = PROCESSOR_BUILDERS.read().unwrap();

        if let Some(builder) = builders.get(&self.processor_type) {
            builder.build(&self.config)
        } else {
            Err(Error::Config(format!(
                "Unknown processor type: {}",
                self.processor_type
            )))
        }
    }
}

pub trait ProcessorBuilder: Send + Sync {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Processor>, Error>;
}

pub fn register_processor_builder(type_name: &str, builder: Arc<dyn ProcessorBuilder>) {
    let mut builders = PROCESSOR_BUILDERS.write().unwrap();
    if builders.contains_key(type_name) {
        panic!("Processor type already registered: {}", type_name);
    }
    builders.insert(type_name.to_string(), builder);
}

pub fn get_registered_processor_types() -> Vec<String> {
    let builders = PROCESSOR_BUILDERS.read().unwrap();
    builders.keys().cloned().collect()
}
