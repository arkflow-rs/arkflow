use crate::input::Ack;
use crate::MessageBatch;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MemoryBufferConfig {}
struct MemoryBuffer {
    config: MemoryBufferConfig,
    queue: Arc<Mutex<VecDeque<(MessageBatch, Arc<dyn Ack>)>>>,
}
