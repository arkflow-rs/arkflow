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

use crate::{codec::Codec, Error, MessageBatchRef, Resource};

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

    /// Write a batch of messages that share one acknowledgement.
    ///
    /// One `write_batch` call corresponds to one ack range and therefore one
    /// transaction unit. The default implementation writes each message via
    /// [`write`](Self::write) in order and returns `Err` if any write failed
    /// (continue-on-error, retaining the last error), preserving the
    /// historical per-message behavior. Outputs that need exactly-once
    /// semantics override this to commit the whole batch atomically.
    async fn write_batch(&self, msgs: &[MessageBatchRef]) -> Result<(), Error> {
        let mut err = None;
        for msg in msgs {
            if let Err(e) = self.write(msg.clone()).await {
                err = Some(e);
            }
        }
        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

/// Output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    #[serde(rename = "type")]
    pub output_type: String,
    pub name: Option<String>,
    pub codec: Option<CodecConfig>,
    #[serde(flatten)]
    pub config: Option<serde_json::Value>,
}

use crate::codec::CodecConfig;

impl OutputConfig {
    /// Build the output component according to the configuration
    pub fn build(&self, resource: &Resource) -> Result<Arc<dyn Output>, Error> {
        let builders = OUTPUT_BUILDERS.read().unwrap();

        if let Some(builder) = builders.get(&self.output_type) {
            // Build codec if configured
            let codec = if let Some(codec_config) = &self.codec {
                Some(codec_config.build(resource)?)
            } else {
                None
            };

            builder.build(self.name.as_deref(), &self.config, codec, resource)
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
        name: Option<&str>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error>;

    /// Placement affinity key this component contributes to the job plan
    /// (e.g. the Kafka L3 transactional pairing). Sinks returning the same
    /// key as other components are pinned onto one node by placement; the
    /// default of `None` means no affinity. Derives from configuration only —
    /// implementations MUST NOT take side effects (no connections).
    fn placement_affinity(
        &self,
        config: &Option<serde_json::Value>,
    ) -> Result<Option<String>, Error> {
        let _ = config;
        Ok(None)
    }
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

/// Names of all registered output builders, sorted. Used by the registry
/// consistency test to detect builders without docs metadata.
pub fn registered_names() -> Vec<String> {
    let mut names: Vec<String> = OUTPUT_BUILDERS.read().unwrap().keys().cloned().collect();
    names.sort();
    names
}

/// Resolve the placement affinity of an output type through its registered
/// builder. An unregistered type contributes no affinity (`Ok(None)`) — it
/// cannot be built on any node, so the existing build path stays the
/// fail-closed boundary; a registered builder's affinity error (malformed
/// config) propagates so placement fails before dispatch.
pub fn placement_affinity(
    output_type: &str,
    config: &Option<serde_json::Value>,
) -> Result<Option<String>, Error> {
    let builders = OUTPUT_BUILDERS.read().unwrap();
    match builders.get(output_type) {
        Some(builder) => builder.placement_affinity(config),
        None => Ok(None),
    }
}

#[cfg(test)]
mod affinity_tests {
    use super::*;

    struct StubBuilder {
        affinity: Option<String>,
        fail: bool,
    }

    impl OutputBuilder for StubBuilder {
        fn build(
            &self,
            _name: Option<&str>,
            _config: &Option<serde_json::Value>,
            _codec: Option<Arc<dyn Codec>>,
            _resource: &Resource,
        ) -> Result<Arc<dyn Output>, Error> {
            Err(Error::Config("stub output never builds".to_string()))
        }

        fn placement_affinity(
            &self,
            _config: &Option<serde_json::Value>,
        ) -> Result<Option<String>, Error> {
            if self.fail {
                Err(Error::Config("stub affinity config error".to_string()))
            } else {
                Ok(self.affinity.clone())
            }
        }
    }

    fn unique_type(prefix: &str) -> String {
        format!("{prefix}-{}", std::process::id())
    }

    #[test]
    fn placement_affinity_resolves_through_the_registry() {
        let declared = unique_type("affinity-out-declared");
        register_output_builder(
            &declared,
            Arc::new(StubBuilder {
                affinity: Some("kafka-txn:g1".into()),
                fail: false,
            }),
        )
        .unwrap();
        let failing = unique_type("affinity-out-fail");
        register_output_builder(
            &failing,
            Arc::new(StubBuilder {
                affinity: None,
                fail: true,
            }),
        )
        .unwrap();

        let config = Some(serde_json::json!({"offset_commit_group": "g1"}));
        assert_eq!(
            placement_affinity(&declared, &config).unwrap(),
            Some("kafka-txn:g1".to_string())
        );
        assert_eq!(
            placement_affinity("affinity-out-unregistered", &config).unwrap(),
            None
        );
        assert!(placement_affinity(&failing, &config).is_err());
    }
}
