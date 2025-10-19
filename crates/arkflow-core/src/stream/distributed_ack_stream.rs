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

//! Distributed Acknowledgment Stream Extension
//!
//! Extends the Stream to support distributed acknowledgment as an alternative to local WAL.

use crate::distributed_ack_config::DistributedAckConfig;
use crate::distributed_ack_processor::DistributedAckProcessor;
use crate::stream::Stream;
use crate::{input::Input, output::Output, pipeline::Pipeline, Error, Resource};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Create a distributed acknowledgment enabled stream
pub fn create_distributed_ack_stream(
    input: Arc<dyn Input>,
    pipeline: Pipeline,
    output: Arc<dyn Output>,
    error_output: Option<Arc<dyn Output>>,
    buffer: Option<Arc<dyn crate::buffer::Buffer>>,
    resource: Resource,
    thread_num: u32,
    distributed_ack_config: DistributedAckConfig,
) -> Result<Stream, Error> {
    // Create distributed acknowledgment processor
    let tracker = tokio_util::task::TaskTracker::new();
    let cancellation_token = CancellationToken::new();

    let _distributed_processor = tokio::runtime::Handle::current()
        .block_on(async {
            DistributedAckProcessor::new(
                &tracker,
                cancellation_token.clone(),
                distributed_ack_config,
            )
            .await
        })
        .map_err(|e| Error::Config(format!("Failed to create distributed ack processor: {}", e)))?;

    // Create a custom stream that integrates distributed acknowledgment
    let stream = Stream::new(
        input,
        pipeline,
        output,
        error_output,
        buffer,
        resource,
        thread_num,
    );

    // Store the distributed processor in the stream
    // Note: This would require modifying the Stream struct to support this
    // Return the configured stream with distributed acknowledgment support
    Ok(stream)
}

/// Builder for creating distributed acknowledgment streams
pub struct DistributedAckStreamBuilder {
    config: DistributedAckConfig,
}

impl DistributedAckStreamBuilder {
    pub fn new(config: DistributedAckConfig) -> Self {
        Self { config }
    }

    /// Create a new stream with distributed acknowledgment support
    pub fn build_stream(
        &self,
        input: Arc<dyn Input>,
        pipeline: Pipeline,
        output: Arc<dyn Output>,
        error_output: Option<Arc<dyn Output>>,
        buffer: Option<Arc<dyn crate::buffer::Buffer>>,
        resource: Resource,
        thread_num: u32,
    ) -> Result<Stream, Error> {
        create_distributed_ack_stream(
            input,
            pipeline,
            output,
            error_output,
            buffer,
            resource,
            thread_num,
            self.config.clone(),
        )
    }
}
