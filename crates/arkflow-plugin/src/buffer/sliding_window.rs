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

use crate::time::deserialize_duration;
use arkflow_core::buffer::{register_buffer_builder, Buffer, BufferBuilder};
use arkflow_core::input::{Ack, VecAck};
use arkflow_core::{Error, MessageBatch};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time;
use tokio::sync::{Notify, RwLock};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlidingWindowConfig {
    window_size: u32,
    #[serde(deserialize_with = "deserialize_duration")]
    interval: time::Duration,
    slide_size: u32,
}

pub struct SlidingWindow {
    config: SlidingWindowConfig,
    queue: Arc<RwLock<VecDeque<(MessageBatch, Arc<dyn Ack>)>>>,
    notify: Arc<Notify>,
    close: CancellationToken,
}

impl SlidingWindow {
    fn new(config: SlidingWindowConfig) -> Result<Self, Error> {
        let notify = Arc::new(Notify::new());
        let notify_clone = Arc::clone(&notify);
        let interval = config.interval;
        let close = CancellationToken::new();
        let close_clone = close.clone();

        tokio::spawn(async move {
            loop {
                let timer = sleep(interval);
                tokio::select! {
                    _ = timer => {
                        notify_clone.notify_waiters();
                    }
                    _ = close_clone.cancelled() => {
                        notify_clone.notify_waiters();
                        break;
                    }
                    _ = notify_clone.notified() => {
                        if close_clone.is_cancelled(){
                            break;
                        }
                    }
                }
            }
        });

        Ok(Self {
            close,
            notify,
            config,
            queue: Arc::new(Default::default()),
        })
    }

    async fn process_slide(&self) -> Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error> {
        let mut queue_lock = self.queue.write().await;
        if queue_lock.len() < self.config.window_size as usize {
            return Ok(None);
        }

        let window_messages: Vec<_> = queue_lock
            .iter()
            .take(self.config.window_size as usize)
            .cloned()
            .collect();
        let size = window_messages.len();
        let mut messages = Vec::with_capacity(size);
        let mut acks = Vec::with_capacity(size);

        for (msg, ack) in window_messages {
            messages.push(msg);
            acks.push(ack);
        }

        if messages.is_empty() {
            return Ok(None);
        }

        let schema = messages[0].schema();
        let batches: Vec<RecordBatch> = messages.into_iter().map(|batch| batch.into()).collect();
        let new_batch = arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| Error::Process(format!("Merge batches failed: {}", e)))?;

        let new_ack = Arc::new(VecAck(acks));

        // Remove slide_size messages from front
        for _ in 0..self.config.slide_size {
            if queue_lock.pop_front().is_none() {
                break;
            }
        }

        Ok(Some((MessageBatch::new_arrow(new_batch), new_ack)))
    }
}

#[async_trait]
impl Buffer for SlidingWindow {
    async fn write(&self, msg: MessageBatch, ack: Arc<dyn Ack>) -> Result<(), Error> {
        let mut queue_lock = self.queue.write().await;
        queue_lock.push_back((msg, ack));
        Ok(())
    }

    async fn read(&self) -> Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error> {
        loop {
            {
                let queue_arc = Arc::clone(&self.queue);
                let queue_lock = queue_arc.read().await;
                if queue_lock.len() >= self.config.window_size as usize {
                    break;
                }
                if self.close.is_cancelled() {
                    return Ok(None);
                }
            }
            let notify = Arc::clone(&self.notify);
            notify.notified().await;
        }
        self.process_slide().await
    }

    async fn flush(&self) -> Result<(), Error> {
        self.close.cancel();
        let queue_arc = Arc::clone(&self.queue);
        let queue_lock = queue_arc.read().await;
        if !queue_lock.is_empty() {
            let notify = Arc::clone(&self.notify);
            notify.notify_waiters();
        }
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        self.close.cancel();
        Ok(())
    }
}

struct SlidingWindowBuilder;

impl BufferBuilder for SlidingWindowBuilder {
    fn build(&self, config: &Option<Value>) -> Result<Arc<dyn Buffer>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Sliding window configuration is missing".to_string(),
            ));
        }

        let config: SlidingWindowConfig = serde_json::from_value(config.clone().unwrap())?;
        if config.window_size == 0 {
            return Err(Error::Config(
                "Sliding window window_size must be greater than 0".to_string(),
            ));
        }
        if config.slide_size == 0 {
            return Err(Error::Config(
                "Sliding window slide_size must be greater than 0".to_string(),
            ));
        }
        if config.window_size < config.slide_size {
            return Err(Error::Config(
                "Sliding window window_size must be greater than slide_size".to_string(),
            ));
        }

        Ok(Arc::new(SlidingWindow::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_buffer_builder("sliding_window", Arc::new(SlidingWindowBuilder))
}
