//! Output component module
//!
//! The output component is responsible for sending the processed data to the target system.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::{Error, MessageBatch};

lazy_static::lazy_static! {
    static ref OUTPUT_BUILDERS: RwLock<HashMap<String, Arc<dyn OutputBuilder>>> = RwLock::new(HashMap::new());
}
/// Feature interface of the output component
#[async_trait]
pub trait Output: Send + Sync {
    /// Connect to the output destination
    async fn connect(&self) -> Result<(), Error>;

    /// Write a message to the output destination
    async fn write(&self, msg: MessageBatch) -> Result<(), Error>;

    /// Close the output destination connection
    async fn close(&self) -> Result<(), Error>;
}

/// Output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    #[serde(rename = "type")]
    pub output_type: String,
    #[serde(flatten)]
    pub config: Option<serde_json::Value>,
}

impl OutputConfig {
    /// Build the output component according to the configuration
    pub fn build(&self) -> Result<Arc<dyn Output>, Error> {
        let builders = OUTPUT_BUILDERS.read().unwrap();

        if let Some(builder) = builders.get(&self.output_type) {
            builder.build(&self.config)
        } else {
            Err(Error::Config(format!(
                "Unknown output type: {}",
                self.output_type
            )))
        }
    }
}

pub trait OutputBuilder: Send + Sync {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Output>, Error>;
}

pub fn register_output_builder(type_name: &str, builder: Arc<dyn OutputBuilder>) {
    let mut builders = OUTPUT_BUILDERS.write().unwrap();
    if builders.contains_key(type_name) {
        panic!("Output type already registered: {}", type_name);
    }
    builders.insert(type_name.to_string(), builder);
}

pub fn get_registered_output_types() -> Vec<String> {
    let builders = OUTPUT_BUILDERS.read().unwrap();
    builders.keys().cloned().collect()
}
