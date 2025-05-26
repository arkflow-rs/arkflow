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
use crate::buffer::join::JoinConfig;
use crate::component;
use arkflow_core::input::{Ack, VecAck};
use arkflow_core::{Error, MessageBatch};
use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
use std::collections::{HashMap, VecDeque};
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
    join_config: Option<JoinConfig>,
}
impl BaseWindow {
    pub(crate) fn new(
        join_config: Option<JoinConfig>,
        notify: Arc<Notify>,
        close: CancellationToken,
        gap: time::Duration,
    ) -> Self {
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

        Self {
            join_config,
            queue: Arc::new(RwLock::new(HashMap::new())),
            notify,
            close,
        }
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
            while let Some((msg, ack)) = queue_lock.pop_back() {
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

        match &self.join_config {
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
            Some(join_config) => {
                let ctx = component::sql::create_session_context()?;
                let new_batch =
                    JoinConfig::join_operation(&ctx, &join_config.query, all_messages).await?;
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
        if !queue_arc.is_empty() {
            false
        } else {
            let mut cnt = 0u64;
            for (_, q) in queue_arc.iter() {
                let q = Arc::clone(&q);
                cnt += q.read().await.len() as u64;
            }

            cnt != 0
        }
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
