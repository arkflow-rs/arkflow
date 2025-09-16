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

//! Asynchronous acknowledgment processor
//!
//! This module provides a high-performance async acknowledgment processor
//! that handles message acknowledgments asynchronously with retry mechanisms.

use crate::input::Ack;
use flume::{Receiver, Sender};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

const MAX_RETRIES: u32 = 3;
const RETRY_DELAY_MS: u64 = 100;
const ACK_TIMEOUT_MS: u64 = 5000;
const BATCH_SIZE: usize = 100;
const MAX_PENDING_ACKS: usize = 10000;

#[derive(Clone)]
pub struct AckTask {
    ack: Arc<dyn Ack>,
    retry_count: u32,
    created_at: Instant,
    sequence: u64,
}

impl std::fmt::Debug for AckTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AckTask")
            .field("retry_count", &self.retry_count)
            .field("created_at", &self.created_at)
            .field("sequence", &self.sequence)
            .field("ack", &"Arc<dyn Ack>")
            .finish()
    }
}

impl AckTask {
    pub fn new(ack: Arc<dyn Ack>, sequence: u64) -> Self {
        Self {
            ack,
            retry_count: 0,
            created_at: Instant::now(),
            sequence,
        }
    }

    pub fn is_expired(&self) -> bool {
        self.created_at.elapsed() > Duration::from_millis(ACK_TIMEOUT_MS)
    }

    pub fn should_retry(&self) -> bool {
        self.retry_count < MAX_RETRIES
    }

    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
    }
}

#[derive(Debug, Clone)]
pub struct AckProcessorMetrics {
    pub total_acks: Arc<AtomicU64>,
    pub successful_acks: Arc<AtomicU64>,
    pub failed_acks: Arc<AtomicU64>,
    pub retried_acks: Arc<AtomicU64>,
    pub pending_acks: Arc<AtomicU64>,
}

impl Default for AckProcessorMetrics {
    fn default() -> Self {
        Self {
            total_acks: Arc::new(AtomicU64::new(0)),
            successful_acks: Arc::new(AtomicU64::new(0)),
            failed_acks: Arc::new(AtomicU64::new(0)),
            retried_acks: Arc::new(AtomicU64::new(0)),
            pending_acks: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[derive(Clone)]
pub struct AsyncAckProcessor {
    ack_sender: Sender<AckTask>,
    metrics: AckProcessorMetrics,
    sequence_counter: Arc<AtomicU64>,
}

impl AsyncAckProcessor {
    pub fn new(tracker: &TaskTracker, cancellation_token: CancellationToken) -> Self {
        let (ack_sender, ack_receiver) = flume::bounded(MAX_PENDING_ACKS);
        let metrics = AckProcessorMetrics::default();
        let sequence_counter = Arc::new(AtomicU64::new(0));

        let processor = AckProcessorWorker {
            ack_receiver,
            metrics: metrics.clone(),
            cancellation_token: cancellation_token.clone(),
        };

        tracker.spawn(processor.run());

        Self {
            ack_sender,
            metrics,
            sequence_counter,
        }
    }

    pub async fn ack(&self, ack: Arc<dyn Ack>) -> Result<(), crate::Error> {
        let sequence = self.sequence_counter.fetch_add(1, Ordering::SeqCst);
        let task = AckTask::new(ack, sequence);

        self.metrics.total_acks.fetch_add(1, Ordering::Relaxed);
        self.metrics.pending_acks.fetch_add(1, Ordering::Relaxed);

        match self.ack_sender.send_async(task).await {
            Ok(_) => Ok(()),
            Err(e) => {
                self.metrics.pending_acks.fetch_sub(1, Ordering::Relaxed);
                self.metrics.failed_acks.fetch_add(1, Ordering::Relaxed);
                Err(crate::Error::Unknown(format!(
                    "Failed to send ack task: {}",
                    e
                )))
            }
        }
    }

    pub fn get_metrics(&self) -> AckProcessorMetrics {
        self.metrics.clone()
    }
}

struct AckProcessorWorker {
    ack_receiver: Receiver<AckTask>,
    metrics: AckProcessorMetrics,
    cancellation_token: CancellationToken,
}

impl AckProcessorWorker {
    async fn run(self) {
        info!("Async ack processor started");

        let mut pending_tasks = Vec::with_capacity(BATCH_SIZE);

        loop {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    break;
                }
                result = self.ack_receiver.recv_async() => {
                    match result {
                        Ok(task) => {
                            pending_tasks.push(task);

                            if pending_tasks.len() >= BATCH_SIZE {
                                self.process_batch(&mut pending_tasks).await;
                            }
                        }
                        Err(_) => {
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    if !pending_tasks.is_empty() {
                        self.process_batch(&mut pending_tasks).await;
                    }
                }
            }
        }

        // Process remaining tasks before shutdown
        if !pending_tasks.is_empty() {
            self.process_batch(&mut pending_tasks).await;
        }

        info!("Async ack processor stopped");
    }

    async fn process_batch(&self, tasks: &mut Vec<AckTask>) {
        let batch_size = tasks.len();
        debug!("Processing batch of {} ack tasks", batch_size);

        let mut successful_count = 0;
        let mut failed_count = 0;
        let mut retried_count = 0;
        let mut tasks_to_remove = Vec::new();

        for (i, task) in tasks.iter_mut().enumerate() {
            if task.is_expired() {
                warn!(
                    "Ack task expired after {}ms",
                    task.created_at.elapsed().as_millis()
                );
                self.metrics.failed_acks.fetch_add(1, Ordering::Relaxed);
                self.metrics.pending_acks.fetch_sub(1, Ordering::Relaxed);
                failed_count += 1;
                tasks_to_remove.push(i);
                continue;
            }

            let result =
                tokio::time::timeout(Duration::from_millis(ACK_TIMEOUT_MS), task.ack.ack()).await;

            match result {
                Ok(_) => {
                    self.metrics.successful_acks.fetch_add(1, Ordering::Relaxed);
                    self.metrics.pending_acks.fetch_sub(1, Ordering::Relaxed);
                    successful_count += 1;
                    tasks_to_remove.push(i); // Remove from pending
                }
                Err(_) => {
                    // Timeout occurred
                    if task.should_retry() {
                        task.increment_retry();
                        self.metrics.retried_acks.fetch_add(1, Ordering::Relaxed);
                        retried_count += 1;
                        // Keep in pending for retry
                    } else {
                        error!("Ack task failed after {} retries", task.retry_count);
                        self.metrics.failed_acks.fetch_add(1, Ordering::Relaxed);
                        self.metrics.pending_acks.fetch_sub(1, Ordering::Relaxed);
                        failed_count += 1;
                        tasks_to_remove.push(i); // Remove from pending
                    }
                }
            }
        }

        // Remove tasks in reverse order to maintain correct indices
        for &i in tasks_to_remove.iter().rev() {
            tasks.remove(i);
        }

        if successful_count > 0 {
            debug!("Successfully acked {} messages", successful_count);
        }
        if failed_count > 0 {
            error!("Failed to ack {} messages", failed_count);
        }
        if retried_count > 0 {
            warn!("Retrying {} ack tasks", retried_count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::NoopAck;

    #[tokio::test]
    async fn test_ack_task_creation() {
        let ack = Arc::new(NoopAck);
        let task = AckTask::new(ack, 1);

        assert_eq!(task.retry_count, 0);
        assert_eq!(task.sequence, 1);
        assert!(!task.is_expired());
        assert!(task.should_retry());
    }

    #[tokio::test]
    async fn test_ack_task_retry() {
        let ack = Arc::new(NoopAck);
        let mut task = AckTask::new(ack, 1);

        for _ in 0..MAX_RETRIES {
            task.increment_retry();
        }

        assert!(!task.should_retry());
    }

    #[tokio::test]
    async fn test_ack_processor_metrics() {
        let metrics = AckProcessorMetrics::default();

        metrics.total_acks.fetch_add(10, Ordering::Relaxed);
        metrics.successful_acks.fetch_add(8, Ordering::Relaxed);
        metrics.failed_acks.fetch_add(2, Ordering::Relaxed);

        assert_eq!(metrics.total_acks.load(Ordering::Relaxed), 10);
        assert_eq!(metrics.successful_acks.load(Ordering::Relaxed), 8);
        assert_eq!(metrics.failed_acks.load(Ordering::Relaxed), 2);
    }
}
