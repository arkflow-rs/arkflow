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

//! Distributed Acknowledgment Processor
//!
//! A processor that adds distributed acknowledgment support to existing processors.

use crate::distributed_ack_config::DistributedAckConfig;
use crate::distributed_ack_integration::DistributedAckBuilder;
use crate::distributed_ack_processor::DistributedAckProcessor;
use crate::processor::{Processor, ProcessorBuilder};
use crate::{Error, MessageBatch, Resource};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Distributed acknowledgment processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedAckProcessorConfig {
    /// Inner processor configuration
    pub inner_processor: crate::processor::ProcessorConfig,
    /// Distributed acknowledgment configuration
    pub distributed_ack: DistributedAckConfig,
}

/// Distributed acknowledgment processor builder
pub struct DistributedAckProcessorBuilder;

#[async_trait]
impl ProcessorBuilder for DistributedAckProcessorBuilder {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<serde_json::Value>,
        resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        let config: DistributedAckProcessorConfig =
            serde_json::from_value(config.clone().unwrap_or_default()).map_err(|e| {
                Error::Config(format!("Invalid distributed ack processor config: {}", e))
            })?;

        // Build the inner processor
        let inner_processor = config.inner_processor.build(resource)?;

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

        // Wrap the processor with distributed acknowledgment support
        let builder = DistributedAckBuilder::new(config.distributed_ack);
        let wrapped_processor =
            builder.wrap_processor(inner_processor, Arc::new(distributed_processor));

        Ok(wrapped_processor)
    }
}

/// Register the distributed acknowledgment processor builder
pub fn register_distributed_ack_processor_builder() -> Result<(), Error> {
    crate::processor::register_processor_builder(
        "distributed_ack_processor",
        Arc::new(DistributedAckProcessorBuilder),
    )
}
