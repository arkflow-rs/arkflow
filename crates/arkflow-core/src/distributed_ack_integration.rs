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

//! Distributed Acknowledgment Integration Module
//!
//! This module provides seamless integration between the distributed acknowledgment system
//! and Arkflow's stream processing pipeline.

use crate::distributed_ack_config::DistributedAckConfig;
use crate::distributed_ack_processor::DistributedAckProcessor;
use crate::input::{Ack, Input};
use crate::processor::Processor;
use crate::{Error, MessageBatch};
use async_trait::async_trait;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Wrapper that implements Ack trait for distributed acknowledgment
pub struct DistributedAck {
    inner: Arc<dyn Ack>,
    distributed_processor: Arc<DistributedAckProcessor>,
    ack_id: String,
    ack_type: String,
}

impl DistributedAck {
    pub fn new(
        ack: Arc<dyn Ack>,
        distributed_processor: Arc<DistributedAckProcessor>,
        ack_id: String,
        ack_type: String,
    ) -> Self {
        Self {
            inner: ack,
            distributed_processor,
            ack_id,
            ack_type,
        }
    }
}

#[async_trait]
impl Ack for DistributedAck {
    async fn ack(&self) {
        // Submit acknowledgment to distributed processor
        if let Err(e) = self
            .distributed_processor
            .submit_ack(
                self.ack_id.clone(),
                self.ack_type.clone(),
                self.inner.clone(),
            )
            .await
        {
            log::error!("Failed to submit distributed ack: {}", e);
        }
    }
}

/// Input wrapper that adds distributed acknowledgment support
pub struct DistributedAckInput {
    inner: Arc<dyn Input>,
    distributed_processor: Arc<DistributedAckProcessor>,
}

impl DistributedAckInput {
    pub fn new(input: Arc<dyn Input>, distributed_processor: Arc<DistributedAckProcessor>) -> Self {
        Self {
            inner: input,
            distributed_processor,
        }
    }
}

#[async_trait]
impl Input for DistributedAckInput {
    async fn connect(&self) -> Result<(), Error> {
        self.inner.connect().await
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        let (batch, original_ack) = self.inner.read().await?;

        // Generate unique ack ID for this message
        let ack_id = uuid::Uuid::new_v4().to_string();

        // Wrap the original ack with distributed acknowledgment
        let distributed_ack = Arc::new(DistributedAck::new(
            original_ack,
            self.distributed_processor.clone(),
            ack_id,
            "distributed_input".to_string(),
        ));

        Ok((batch, distributed_ack))
    }

    async fn close(&self) -> Result<(), Error> {
        self.inner.close().await
    }
}

/// Processor wrapper that adds distributed acknowledgment support
pub struct DistributedAckProcessorWrapper {
    inner: Arc<dyn Processor>,
    distributed_processor: Arc<DistributedAckProcessor>,
}

impl DistributedAckProcessorWrapper {
    pub fn new(
        processor: Arc<dyn Processor>,
        distributed_processor: Arc<DistributedAckProcessor>,
    ) -> Self {
        Self {
            inner: processor,
            distributed_processor,
        }
    }
}

#[async_trait]
impl Processor for DistributedAckProcessorWrapper {
    async fn process(&self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        self.inner.process(batch).await
    }

    async fn close(&self) -> Result<(), Error> {
        self.inner.close().await
    }
}

/// Distributed acknowledgment processor that implements ReliableAckProcessor trait
pub struct DistributedReliableAckProcessor {
    distributed_processor: Arc<DistributedAckProcessor>,
}

impl DistributedReliableAckProcessor {
    pub fn new(distributed_processor: Arc<DistributedAckProcessor>) -> Self {
        Self {
            distributed_processor,
        }
    }

    pub async fn ack(
        &self,
        ack: Arc<dyn Ack>,
        ack_type: String,
        _payload: Vec<u8>,
    ) -> Result<(), Error> {
        let ack_id = uuid::Uuid::new_v4().to_string();
        self.distributed_processor
            .submit_ack(ack_id, ack_type, ack)
            .await
            .map_err(|e| Error::Process(format!("Distributed ack failed: {}", e)))
    }
}

/// Builder for creating distributed acknowledgment components
pub struct DistributedAckBuilder {
    config: DistributedAckConfig,
}

impl DistributedAckBuilder {
    pub fn new(config: DistributedAckConfig) -> Self {
        Self { config }
    }

    /// Create a new distributed acknowledgment processor
    pub async fn build_processor(
        &self,
        tracker: &tokio_util::task::TaskTracker,
        cancellation_token: CancellationToken,
    ) -> Result<Arc<DistributedAckProcessor>, Error> {
        let processor =
            DistributedAckProcessor::new(tracker, cancellation_token.clone(), self.config.clone())
                .await
                .map_err(|e| {
                    Error::Config(format!("Failed to create distributed ack processor: {}", e))
                })?;

        Ok(Arc::new(processor))
    }

    /// Wrap an input with distributed acknowledgment support
    pub fn wrap_input(
        &self,
        input: Arc<dyn Input>,
        processor: Arc<DistributedAckProcessor>,
    ) -> Arc<dyn Input> {
        Arc::new(DistributedAckInput::new(input, processor))
    }

    /// Wrap a processor with distributed acknowledgment support
    pub fn wrap_processor(
        &self,
        processor: Arc<dyn Processor>,
        distributed_processor: Arc<DistributedAckProcessor>,
    ) -> Arc<dyn Processor> {
        Arc::new(DistributedAckProcessorWrapper::new(
            processor,
            distributed_processor,
        ))
    }

    /// Create a reliable ack processor wrapper
    pub fn create_reliable_ack_processor(
        &self,
        distributed_processor: Arc<DistributedAckProcessor>,
    ) -> DistributedReliableAckProcessor {
        DistributedReliableAckProcessor::new(distributed_processor)
    }
}
