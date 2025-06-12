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
use crate::buffer::join::{JoinConfig, JoinOperation};
use crate::component;
use arkflow_core::input::{Ack, VecAck};
use arkflow_core::{Error, MessageBatch, Resource};
use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time;
use tokio::sync::{Notify, RwLock};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

pub(crate) struct BaseWindow {
    /// Thread-safe queue to store message batches and their acknowledgments
    queue: Arc<RwLock<HashMap<String, Arc<RwLock<VecDeque<(MessageBatch, Arc<dyn Ack>)>>>>>>,
    /// Notification mechanism for signaling between threads
    notify: Arc<Notify>,
    /// Token for cancellation of background tasks
    close: CancellationToken,
    // join_config: Option<JoinConfig>,
    join_operation: Option<JoinOperation>,
}
impl BaseWindow {
    pub(crate) fn new(
        join_config: Option<JoinConfig>,
        notify: Arc<Notify>,
        close: CancellationToken,
        gap: time::Duration,
        resource: &Resource,
    ) -> Result<Self, Error> {
        let notify_clone = Arc::clone(&notify);
        let close_clone = close.clone();
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

        let join_operation = join_config
            .map(|config| {
                let codec = config.codec.build(resource)?;
                let input_names = resource
                    .input_names
                    .borrow()
                    .iter()
                    .map(|name| name.clone())
                    .collect::<HashSet<String>>();

                JoinOperation::new(
                    config.query,
                    config.value_field,
                    config.thread_num,
                    codec,
                    input_names,
                )
            })
            .transpose()?;

        Ok(Self {
            join_operation,
            queue: Arc::new(RwLock::new(HashMap::new())),
            notify,
            close,
        })
    }

    pub(crate) async fn process_window(
        &self,
    ) -> Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error> {
        let queue_arc = Arc::clone(&self.queue);
        let queue_arc = queue_arc.write().await;
        let queue_len = queue_arc.len();
        let mut all_messages = Vec::with_capacity(queue_len);
        let mut all_acks: Vec<Arc<dyn Ack>> = Vec::with_capacity(queue_len);

        for (input_name, queue) in queue_arc.iter() {
            let queue = Arc::clone(queue);
            let mut queue_lock = queue.write().await;
            if queue_lock.is_empty() {
                continue;
            }

            let size = queue_lock.len();
            let mut messages = Vec::with_capacity(size);
            let mut acks = Vec::with_capacity(size);
            while let Some((msg, ack)) = queue_lock.pop_front() {
                messages.push(msg);
                acks.push(ack);
            }

            let schema = messages[0].schema();
            let batches: Vec<RecordBatch> =
                messages.into_iter().map(|batch| batch.into()).collect();
            let new_batch = arrow::compute::concat_batches(&schema, &batches)
                .map_err(|e| Error::Process(format!("Merge batches failed: {}", e)))?;
            let mut new_batch: MessageBatch = new_batch.into();
            new_batch.set_input_name(Some(input_name.clone()));
            let new_ack = Arc::new(VecAck(acks));
            all_messages.push(new_batch);
            all_acks.push(new_ack);
        }

        if all_messages.is_empty() {
            return Ok(None);
        }

        let new_ack = Arc::new(VecAck(all_acks));

        match &self.join_operation {
            None => {
                let schema = all_messages[0].schema();
                let batches: Vec<RecordBatch> =
                    all_messages.into_iter().map(|batch| batch.into()).collect();

                if batches.is_empty() {
                    return Ok(None);
                }

                let new_batch = arrow::compute::concat_batches(&schema, &batches)
                    .map_err(|e| Error::Process(format!("Merge batches failed: {}", e)))?;

                Ok(Some((MessageBatch::new_arrow(new_batch), new_ack)))
            }
            Some(join) => {
                let ctx = component::sql::create_session_context()?;
                let new_batch = join.join_operation(&ctx, all_messages).await?;
                Ok(Some((MessageBatch::new_arrow(new_batch), new_ack)))
            }
        }
    }

    pub(crate) async fn write(&self, msg: MessageBatch, ack: Arc<dyn Ack>) -> Result<(), Error> {
        let input_name = msg.get_input_name().unwrap_or("".to_string());
        let queue_arc = Arc::clone(&self.queue);
        let mut queue_arc = queue_arc.write().await;
        let queue = queue_arc
            .entry(input_name)
            .or_insert(Arc::new(RwLock::new(VecDeque::new())));

        let mut queue_lock = queue.write().await;
        queue_lock.push_front((msg, ack));
        Ok(())
    }

    pub(crate) async fn queue_is_empty(&self) -> bool {
        let queue_arc = Arc::clone(&self.queue);
        let queue_arc = queue_arc.read().await;
        if queue_arc.is_empty() {
            return true;
        }

        for (_, q) in queue_arc.iter() {
            let q = Arc::clone(&q);
            if !q.read().await.is_empty() {
                return false;
            };
        }
        true
    }

    pub(crate) async fn flush(&self) -> Result<(), Error> {
        self.close.cancel();
        if !self.queue_is_empty().await {
            // Notify any waiting readers to process remaining messages
            let notify = Arc::clone(&self.notify);
            notify.notify_waiters();
        }
        Ok(())
    }

    pub(crate) async fn close(&self) -> Result<(), Error> {
        self.close.cancel();
        Ok(())
    }
}
