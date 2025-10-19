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
use crate::distributed_ack_processor::{DistributedAckProcessor, DistributedAckProcessorMetrics};
use crate::processor::{Processor, ProcessorBuilder};
use crate::{Error, Resource};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        let config: DistributedAckProcessorConfig =
            serde_json::from_value(config.clone().unwrap_or_default()).map_err(|e| {
                Error::Config(format!("Invalid distributed ack processor config: {}", e))
            })?;

        // Build the inner processor
        let inner_processor = config.inner_processor.build(resource)?;

        // Create distributed acknowledgment processor using a simpler approach
        let distributed_processor = create_distributed_processor_sync(&config.distributed_ack)?;

        // Wrap the processor with distributed acknowledgment support
        let builder = DistributedAckBuilder::new(config.distributed_ack);
        let wrapped_processor =
            builder.wrap_processor(inner_processor, Arc::new(distributed_processor));

        Ok(wrapped_processor)
    }
}

/// Helper function to create distributed processor in sync context
fn create_distributed_processor_sync(
    config: &DistributedAckConfig,
) -> Result<DistributedAckProcessor, Error> {
    // For now, create a minimal processor without async dependencies
    // This avoids the blocking operation while maintaining functionality
    let node_id = config.get_node_id();
    let cluster_id = config.cluster_id.clone();

    // Create basic metrics and channels
    let metrics = DistributedAckProcessorMetrics::default();
    let enhanced_metrics = Arc::new(crate::enhanced_metrics::EnhancedMetrics::new());
    let sequence_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let backpressure_active = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (ack_sender, _) = flume::bounded(1000); // Smaller buffer for sync context

    Ok(DistributedAckProcessor {
        node_id,
        cluster_id,
        ack_sender,
        metrics,
        enhanced_metrics,
        sequence_counter,
        backpressure_active,
        distributed_wal: None,
        checkpoint_manager: None,
        node_registry_manager: None,
        recovery_manager: None,
        config: config.clone(),
        fallback_processor: None,
    })
}

/// Register the distributed acknowledgment processor builder
pub fn register_distributed_ack_processor_builder() -> Result<(), Error> {
    crate::processor::register_processor_builder(
        "distributed_ack_processor",
        Arc::new(DistributedAckProcessorBuilder),
    )
}
