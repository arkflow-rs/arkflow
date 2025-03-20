use arkflow_core::input::Ack;
use arkflow_core::{Error, MessageBatch};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MemoryBufferConfig {}
struct MemoryBuffer {
    config: MemoryBufferConfig,
    queue: Arc<Mutex<VecDeque<(MessageBatch, Arc<dyn Ack>)>>>,
}

impl MemoryBuffer {
    fn new(config: MemoryBufferConfig) -> Result<Self, Error> {
        Ok(Self {
            config,
            queue: Arc::new(Mutex::new(VecDeque::new())),
        })
    }
}

struct MemoryBufferBuilder;
// impl BufferBuilder for MemoryBufferBuilder {
//     fn build(&self, config: &Option<Value>) -> Result<Arc<dyn Buffer>, Error> {
//
//     }
// }
