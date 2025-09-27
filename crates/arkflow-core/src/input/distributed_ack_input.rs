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

//! Distributed Acknowledgment Input
//!
//! An input source that provides distributed acknowledgment support.

use crate::distributed_ack_config::DistributedAckConfig;
use crate::distributed_ack_integration::DistributedAckBuilder;
use crate::distributed_ack_processor::DistributedAckProcessor;
use crate::input::{Input, InputBuilder};
use crate::{Error, MessageBatch, Resource};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Distributed acknowledgment input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedAckInputConfig {
    /// Inner input configuration
    pub inner_input: crate::input::InputConfig,
    /// Distributed acknowledgment configuration
    pub distributed_ack: DistributedAckConfig,
}

/// Distributed acknowledgment input builder
pub struct DistributedAckInputBuilder;

#[async_trait]
impl InputBuilder for DistributedAckInputBuilder {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<serde_json::Value>,
        resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        let config: DistributedAckInputConfig =
            serde_json::from_value(config.clone().unwrap_or_default()).map_err(|e| {
                Error::Config(format!("Invalid distributed ack input config: {}", e))
            })?;

        // Build the inner input
        let inner_input = config.inner_input.build(resource)?;

        // Create distributed acknowledgment processor
        let tracker = tokio_util::task::TaskTracker::new();
        let cancellation_token = CancellationToken::new();

        let distributed_processor = tokio::runtime::Handle::current()
            .block_on(async {
                DistributedAckProcessor::new(
                    &tracker,
                    cancellation_token.clone(),
                    config.distributed_ack.clone(),
                )
                .await
            })
            .map_err(|e| {
                Error::Config(format!("Failed to create distributed ack processor: {}", e))
            })?;

        // Wrap the input with distributed acknowledgment support
        let builder = DistributedAckBuilder::new(config.distributed_ack);
        let wrapped_input = builder.wrap_input(inner_input, Arc::new(distributed_processor));

        Ok(wrapped_input)
    }
}

/// Register the distributed acknowledgment input builder
pub fn register_distributed_ack_input_builder() -> Result<(), Error> {
    crate::input::register_input_builder(
        "distributed_ack_input",
        Arc::new(DistributedAckInputBuilder),
    )
}
