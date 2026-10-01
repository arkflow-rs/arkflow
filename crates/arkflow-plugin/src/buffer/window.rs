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
use arkflow_core::{Error, MessageBatch, MessageBatchRef, Resource};
use dashmap::DashMap;
use datafusion::arrow::array::RecordBatch;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time;
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

pub(crate) struct BaseWindow {
    /// Thread-safe queue to store message batches and their acknowledgments
    /// Using DashMap instead of nested RwLock for better performance
    /// This eliminates the nested lock bottleneck: Arc<RwLock<HashMap<String, Arc<RwLock<VecDeque...>>>>
    #[allow(clippy::type_complexity)]
    pub(crate) queue: Arc<DashMap<String, VecDeque<(MessageBatchRef, Arc<dyn Ack>)>>>,
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
                    .cloned()
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
            queue: Arc::new(DashMap::new()),
            notify,
            close,
        })
    }

    pub(crate) async fn process_window(
        &self,
    ) -> Result<Option<(MessageBatchRef, Arc<dyn Ack>)>, Error> {
        let mut all_messages: Vec<MessageBatchRef> = Vec::new();
        let mut all_acks: Vec<Arc<dyn Ack>> = Vec::new();

        // DashMap provides efficient concurrent iteration
        // Collect all input names first to avoid holding locks during iteration
        let input_names: Vec<String> = self.queue.iter().map(|entry| entry.key().clone()).collect();

        for input_name in input_names {
            // Try to remove and process the queue for this input
            // Using remove() avoids holding locks while processing
            if let Some((_, mut queue)) = self.queue.remove(&input_name) {
                if queue.is_empty() {
                    continue;
                }

                let size = queue.len();
                let mut messages = Vec::with_capacity(size);
                let mut acks = Vec::with_capacity(size);

                while let Some((msg, ack)) = queue.pop_front() {
                    messages.push(msg);
                    acks.push(ack);
                }

                // Per-input merge: batches of one input may legitimately
                // differ in schema (drift); normalize to the field union
                // instead of failing the concat. On failure everything
                // drained so far goes back — this input's items and the
                // merged batches of the inputs processed before it — so a
                // later read retries the whole round and no message or ack
                // is dropped. `write` pushes to the front, so restored
                // (older) items are appended to the back.
                let batches: Vec<RecordBatch> = messages
                    .iter()
                    .map(|batch| (**batch).clone().into())
                    .collect();
                let new_batch = match crate::component::batch_merge::normalize_and_concat(&batches)
                {
                    Ok(b) => b,
                    Err(e) => {
                        self.queue
                            .entry(input_name)
                            .or_default()
                            .extend(messages.into_iter().zip(acks));
                        for (msg, ack) in all_messages.into_iter().zip(all_acks) {
                            let name = msg.get_input_name().unwrap_or_default();
                            self.queue.entry(name).or_default().push_back((msg, ack));
                        }
                        return Err(e);
                    }
                };
                let mut new_batch: MessageBatch = new_batch.into();
                new_batch.set_input_name(Some(input_name.clone()));
                let new_ack = Arc::new(VecAck(acks));
                all_messages.push(Arc::new(new_batch));
                all_acks.push(new_ack);
            }
        }

        if all_messages.is_empty() {
            return Ok(None);
        }

        let new_ack = Arc::new(VecAck(all_acks.clone()));

        let merged = match &self.join_operation {
            None => {
                let batches: Vec<RecordBatch> = all_messages
                    .iter()
                    .map(|batch| (**batch).clone().into())
                    .collect();
                // Cross-input merge: heterogeneous input schemas are
                // null-filled on the union; a genuine type conflict errors.
                crate::component::batch_merge::normalize_and_concat(&batches)
            }
            Some(join) => {
                let ctx = component::sql::create_session_context()?;
                let messages: Vec<_> = all_messages.iter().map(|m| (**m).clone()).collect();
                join.join_operation(&ctx, messages).await
            }
        };

        match merged {
            Ok(new_batch) => Ok(Some((
                Arc::new(MessageBatch::new_arrow(new_batch)),
                new_ack,
            ))),
            Err(e) => {
                // Cross-input failure (type conflict or join error): the
                // per-input merged batches and their acks go back to their
                // queues — content-equivalent to the original messages — so
                // nothing is dropped and a later read can retry. Appended to
                // the back: `write` pushes newer items to the front.
                for (msg, ack) in all_messages.into_iter().zip(all_acks) {
                    let name = msg.get_input_name().unwrap_or_default();
                    self.queue.entry(name).or_default().push_back((msg, ack));
                }
                Err(e)
            }
        }
    }

    pub(crate) async fn write(&self, msg: MessageBatchRef, ack: Arc<dyn Ack>) -> Result<(), Error> {
        let input_name = msg.get_input_name().unwrap_or("".to_string());

        // DashMap provides efficient concurrent write operations
        // No nested locks needed - this is a major performance improvement
        self.queue
            .entry(input_name)
            .or_default()
            .push_front((msg, ack));

        Ok(())
    }

    pub(crate) async fn queue_is_empty(&self) -> bool {
        // DashMap provides efficient concurrent iteration
        // No locks needed for iteration
        for entry in self.queue.iter() {
            if !entry.value().is_empty() {
                return false;
            }
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
