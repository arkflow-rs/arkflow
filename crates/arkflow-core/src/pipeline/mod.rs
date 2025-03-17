//! Pipeline Component Module
//!
//! A pipeline is an ordered collection of processors that defines how data flows from input to output, through a series of processing steps.

use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{processor::Processor, Error, MessageBatch};

pub struct Pipeline {
    processors: Vec<Arc<dyn Processor>>,
}

impl Pipeline {
    /// Create a new pipeline
    pub fn new(processors: Vec<Arc<dyn Processor>>) -> Self {
        Self { processors }
    }

    /// Process messages
    pub async fn process(&self, msg: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        let mut msgs = vec![msg];
        for processor in &self.processors {
            let mut new_msgs = Vec::new();
            for msg in msgs {
                match processor.process(msg).await {
                    Ok(processed) => new_msgs.extend(processed),
                    Err(e) => return Err(e),
                }
            }
            msgs = new_msgs;
        }
        Ok(msgs)
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
    pub thread_num: i32,
    pub processors: Vec<crate::processor::ProcessorConfig>,
}

impl PipelineConfig {
    /// Build pipelines based on your configuration
    pub fn build(&self) -> Result<(Pipeline, i32), Error> {
        let mut processors = Vec::new();
        for processor_config in &self.processors {
            processors.push(processor_config.build()?);
        }
        Ok((Pipeline::new(processors), self.thread_num))
    }
}
