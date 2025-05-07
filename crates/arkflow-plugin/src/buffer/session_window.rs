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
use tokio::time::{sleep, Instant};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionWindowConfig {
    #[serde(deserialize_with = "deserialize_duration")]
    gap: time::Duration,
}

pub struct SessionWindow {
    config: SessionWindowConfig,
    queue: Arc<RwLock<VecDeque<(MessageBatch, Arc<dyn Ack>)>>>,
    notify: Arc<Notify>,
    close: CancellationToken,
    last_message_time: Arc<RwLock<Instant>>,
}

impl SessionWindow {
    fn new(config: SessionWindowConfig) -> Result<Self, Error> {
        let notify = Arc::new(Notify::new());
        let notify_clone = Arc::clone(&notify);
        let gap = config.gap;
        let close = CancellationToken::new();
        let close_clone = close.clone();
        let last_message_time = Arc::new(RwLock::new(Instant::now()));

        tokio::spawn(async move {
            loop {
                let timer = sleep(gap);
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
            last_message_time,
        })
    }

    async fn process_session(&self) -> Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error> {
        let mut queue_lock = self.queue.write().await;
        if queue_lock.is_empty() {
            return Ok(None);
        }

        let size = queue_lock.len();
        let mut messages = Vec::with_capacity(size);
        let mut acks = Vec::with_capacity(size);

        while let Some((msg, ack)) = queue_lock.pop_back() {
            messages.push(msg);
            acks.push(ack);
        }

        if messages.is_empty() {
            return Ok(None);
        }

        let schema = messages[0].schema();
        let batches: Vec<RecordBatch> = messages.into_iter().map(|batch| batch.into()).collect();

        if batches.is_empty() {
            return Ok(None);
        }

        let new_batch = arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| Error::Process(format!("Merge batches failed: {}", e)))?;

        let new_ack = Arc::new(VecAck(acks));
        Ok(Some((MessageBatch::new_arrow(new_batch), new_ack)))
    }
}

#[async_trait]
impl Buffer for SessionWindow {
    async fn write(&self, msg: MessageBatch, ack: Arc<dyn Ack>) -> Result<(), Error> {
        let mut queue_lock = self.queue.write().await;
        queue_lock.push_front((msg, ack));
        *self.last_message_time.write().await = Instant::now();
        Ok(())
    }

    async fn read(&self) -> Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error> {
        loop {
            {
                let queue_arc = Arc::clone(&self.queue);
                let queue_lock = queue_arc.read().await;
                if !queue_lock.is_empty() {
                    let last_time = *self.last_message_time.read().await;
                    if last_time.elapsed() >= self.config.gap {
                        break;
                    }
                } else if self.close.is_cancelled() {
                    return Ok(None);
                }
            }

            let notify = Arc::clone(&self.notify);
            notify.notified().await;
        }
        self.process_session().await
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

struct SessionWindowBuilder;

impl BufferBuilder for SessionWindowBuilder {
    fn build(&self, config: &Option<Value>) -> Result<Arc<dyn Buffer>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Session window configuration is missing".to_string(),
            ));
        }

        let config: SessionWindowConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(SessionWindow::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_buffer_builder("session_window", Arc::new(SessionWindowBuilder))
}
