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

//! Pipeline Component Module
//!
//! A pipeline is an ordered collection of processors that defines how data flows from input to output, through a series of processing steps.

use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{processor::Processor, Error, MessageBatchRef, ProcessResult, Resource};

pub struct Pipeline {
    processors: Vec<Arc<dyn Processor>>,
}

impl Pipeline {
    /// Create a new pipeline
    pub fn new(processors: Vec<Arc<dyn Processor>>) -> Self {
        Self { processors }
    }

    /// Process messages using Arc for zero-copy
    ///
    /// # Processing Flow
    /// 1. Start with a single message batch
    /// 2. For each processor in the pipeline:
    ///    - If current result is Single: apply processor
    ///    - If current result is Multiple: apply processor to each batch
    ///    - If current result is None: skip (filtered)
    /// 3. Return final result
    ///
    /// # Example
    /// ```rust,no_run
    /// use arkflow_core::pipeline::Pipeline;
    /// use arkflow_core::{MessageBatch, ProcessResult, MessageBatchRef};
    /// use std::sync::Arc;
    ///
    /// async fn example() -> Result<(), Box<dyn std::error::Error>> {
    ///     let pipeline = Pipeline::new(vec![]);  // Empty pipeline
    ///     let batch = Arc::new(MessageBatch::new_binary(vec![b"test".to_vec()])?);
    ///     let result = pipeline.process(batch).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn process(&self, msg: MessageBatchRef) -> Result<ProcessResult, Error> {
        let mut current = ProcessResult::Single(msg);

        for processor in &self.processors {
            current = match current {
                ProcessResult::Single(batch) => processor.process(batch).await?,
                ProcessResult::Multiple(batches) => {
                    let mut results = Vec::new();
                    for batch in batches {
                        match processor.process(batch).await? {
                            ProcessResult::Single(result) => results.push(result),
                            ProcessResult::Multiple(mut res) => results.append(&mut res),
                            ProcessResult::None => {} // Filtered out
                        }
                    }
                    if results.is_empty() {
                        ProcessResult::None
                    } else if results.len() == 1 {
                        ProcessResult::Single(results.pop().unwrap())
                    } else {
                        ProcessResult::Multiple(results)
                    }
                }
                ProcessResult::None => ProcessResult::None, // Already filtered
            };
        }

        Ok(current)
    }

    /// Shut down all processors in the pipeline
    pub async fn close(&self) -> Result<(), Error> {
        for processor in &self.processors {
            processor.close().await?
        }
        Ok(())
    }
}

/// Pipeline configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    #[serde(default = "default_thread_num")]
    pub thread_num: u32,
    pub processors: Vec<crate::processor::ProcessorConfig>,
}

impl PipelineConfig {
    /// Build pipelines based on your configuration
    pub fn build(&self, resource: &Resource) -> Result<(Pipeline, u32), Error> {
        let mut processors = Vec::with_capacity(self.processors.len());
        for processor_config in &self.processors {
            processors.push(processor_config.build(resource)?);
        }
        Ok((Pipeline::new(processors), self.thread_num))
    }
}

fn default_thread_num() -> u32 {
    num_cpus::get() as u32
}
