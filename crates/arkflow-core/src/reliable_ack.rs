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

//! Reliable asynchronous acknowledgment processor
//!
//! This module provides a high-performance, reliable async acknowledgment processor
//! that handles message acknowledgments asynchronously with persistence, recovery,
//! and backpressure control to prevent data loss.

use crate::input::Ack;
use flume::{Receiver, Sender};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

const MAX_RETRIES: u32 = 5;
const RETRY_DELAY_MS: u64 = 1000;
const ACK_TIMEOUT_MS: u64 = 10000;
const BATCH_SIZE: usize = 50;
const MAX_PENDING_ACKS: usize = 5000;
const BACKPRESSURE_THRESHOLD: usize = 3000;
const PERSIST_INTERVAL_MS: u64 = 5000;
const MAX_WAL_SIZE: u64 = 100 * 1024 * 1024; // 100MB

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AckRecord {
    pub sequence: u64,
    pub ack_type: String,
    pub payload: Vec<u8>,
    pub retry_count: u32,
    pub created_at: std::time::SystemTime,
    pub last_retry: Option<std::time::SystemTime>,
}

#[derive(Clone)]
pub struct AckTask {
    ack: Arc<dyn Ack>,
    retry_count: u32,
    created_at: Instant,
    sequence: u64,
    ack_type: String,
    payload: Vec<u8>,
}

impl std::fmt::Debug for AckTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AckTask")
            .field("retry_count", &self.retry_count)
            .field("created_at", &self.created_at)
            .field("sequence", &self.sequence)
            .field("ack_type", &self.ack_type)
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

impl AckTask {
    pub fn new(ack: Arc<dyn Ack>, sequence: u64, ack_type: String, payload: Vec<u8>) -> Self {
        Self {
            ack,
            retry_count: 0,
            created_at: Instant::now(),
            sequence,
            ack_type,
            payload,
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

    pub fn to_record(&self) -> AckRecord {
        AckRecord {
            sequence: self.sequence,
            ack_type: self.ack_type.clone(),
            payload: self.payload.clone(),
            retry_count: self.retry_count,
            created_at: std::time::SystemTime::now(),
            last_retry: Some(std::time::SystemTime::now()),
        }
    }

    pub fn from_record(record: AckRecord, ack: Arc<dyn Ack>) -> Self {
        Self {
            ack,
            retry_count: record.retry_count,
            created_at: Instant::now(),
            sequence: record.sequence,
            ack_type: record.ack_type,
            payload: record.payload,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReliableAckProcessorMetrics {
    pub total_acks: Arc<AtomicU64>,
    pub successful_acks: Arc<AtomicU64>,
    pub failed_acks: Arc<AtomicU64>,
    pub retried_acks: Arc<AtomicU64>,
    pub pending_acks: Arc<AtomicU64>,
    pub persisted_acks: Arc<AtomicU64>,
    pub recovered_acks: Arc<AtomicU64>,
    pub backpressure_events: Arc<AtomicU64>,
}

impl Default for ReliableAckProcessorMetrics {
    fn default() -> Self {
        Self {
            total_acks: Arc::new(AtomicU64::new(0)),
            successful_acks: Arc::new(AtomicU64::new(0)),
            failed_acks: Arc::new(AtomicU64::new(0)),
            retried_acks: Arc::new(AtomicU64::new(0)),
            pending_acks: Arc::new(AtomicU64::new(0)),
            persisted_acks: Arc::new(AtomicU64::new(0)),
            recovered_acks: Arc::new(AtomicU64::new(0)),
            backpressure_events: Arc::new(AtomicU64::new(0)),
        }
    }
}

pub struct AckWAL {
    file: Mutex<File>,
    path: PathBuf,
    current_size: Arc<AtomicU64>,
}

impl AckWAL {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        Ok(Self {
            file: Mutex::new(file),
            path,
            current_size: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn append(&self, record: &AckRecord) -> io::Result<()> {
        let mut file = self.file.lock().await;
        let data =
            serde_json::to_vec(record).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let len = data.len() as u64;

        // Check WAL size
        if self.current_size.load(Ordering::Relaxed) + len > MAX_WAL_SIZE {
            self.rotate_wal().await?;
        }

        file.seek(SeekFrom::End(0))?;
        file.write_all(&data)?;
        file.write_all(b"\n")?;

        self.current_size.fetch_add(len + 1, Ordering::Relaxed);
        Ok(())
    }

    pub async fn recover(&self) -> io::Result<Vec<AckRecord>> {
        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(0))?;

        let mut records = Vec::new();
        let mut buffer = String::new();

        while file.read_to_string(&mut buffer)? > 0 {
            for line in buffer.lines() {
                if let Ok(record) = serde_json::from_str::<AckRecord>(line) {
                    records.push(record);
                }
            }
            buffer.clear();
        }

        Ok(records)
    }

    pub async fn clear(&self) -> io::Result<()> {
        let mut file = self.file.lock().await;
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        self.current_size.store(0, Ordering::Relaxed);
        Ok(())
    }

    async fn rotate_wal(&self) -> io::Result<()> {
        let mut file = self.file.lock().await;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let new_path = self.path.with_extension(format!("wal.{}", timestamp));
        std::fs::rename(&self.path, &new_path)?;

        *file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.path)?;

        self.current_size.store(0, Ordering::Relaxed);
        Ok(())
    }
}

pub struct ReliableAckProcessor {
    ack_sender: Sender<AckTask>,
    metrics: ReliableAckProcessorMetrics,
    sequence_counter: Arc<AtomicU64>,
    backpressure_active: Arc<AtomicBool>,
    wal: Arc<AckWAL>,
    ack_registry: Arc<Mutex<HashMap<String, Box<dyn Fn(&Vec<u8>) -> Arc<dyn Ack> + Send + Sync>>>>,
}

impl ReliableAckProcessor {
    pub fn new(
        tracker: &TaskTracker,
        cancellation_token: CancellationToken,
        wal_path: &Path,
    ) -> Result<Self, crate::Error> {
        let (ack_sender, ack_receiver) = flume::bounded(MAX_PENDING_ACKS);
        let metrics = ReliableAckProcessorMetrics::default();
        let sequence_counter = Arc::new(AtomicU64::new(0));
        let backpressure_active = Arc::new(AtomicBool::new(false));

        let wal = Arc::new(
            AckWAL::new(wal_path)
                .map_err(|e| crate::Error::Unknown(format!("Failed to create WAL: {}", e)))?,
        );

        let ack_registry = Arc::new(Mutex::new(HashMap::new()));

        // Register default ack types
        let ack_registry_for_spawn = ack_registry.clone();
        tokio::spawn(async move {
            Self::register_default_ack_types(&ack_registry_for_spawn).await;
        });

        let processor = ReliableAckProcessorWorker {
            ack_receiver,
            ack_sender: ack_sender.clone(),
            metrics: metrics.clone(),
            cancellation_token: cancellation_token.clone(),
            wal: wal.clone(),
            ack_registry: ack_registry.clone(),
            backpressure_active: backpressure_active.clone(),
        };

        tracker.spawn(processor.run());

        Ok(Self {
            ack_sender,
            metrics,
            sequence_counter,
            backpressure_active,
            wal,
            ack_registry,
        })
    }

    async fn register_default_ack_types(
        registry: &Arc<Mutex<HashMap<String, Box<dyn Fn(&Vec<u8>) -> Arc<dyn Ack> + Send + Sync>>>>,
    ) {
        let mut registry = registry.lock().await;
        registry.insert(
            "noop".to_string(),
            Box::new(|_| Arc::new(crate::input::NoopAck)),
        );
    }

    pub async fn ack(
        &self,
        ack: Arc<dyn Ack>,
        ack_type: String,
        payload: Vec<u8>,
    ) -> Result<(), crate::Error> {
        // Check backpressure
        if self.backpressure_active.load(Ordering::Relaxed) {
            self.metrics
                .backpressure_events
                .fetch_add(1, Ordering::Relaxed);
            return Err(crate::Error::Unknown(
                "Backpressure active - rejecting ack".to_string(),
            ));
        }

        let sequence = self.sequence_counter.fetch_add(1, Ordering::SeqCst);
        let task = AckTask::new(ack, sequence, ack_type, payload);

        self.metrics.total_acks.fetch_add(1, Ordering::Relaxed);
        self.metrics.pending_acks.fetch_add(1, Ordering::Relaxed);

        // Persist to WAL before sending to processor
        let record = task.to_record();
        if let Err(e) = self.wal.append(&record).await {
            error!("Failed to persist ack to WAL: {}", e);
            return Err(crate::Error::Unknown(format!(
                "WAL persistence failed: {}",
                e
            )));
        }
        self.metrics.persisted_acks.fetch_add(1, Ordering::Relaxed);

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

    pub async fn register_ack_type<F>(&self, ack_type: &str, factory: F)
    where
        F: Fn(&Vec<u8>) -> Arc<dyn Ack> + Send + Sync + 'static,
    {
        let mut registry = self.ack_registry.lock().await;
        registry.insert(ack_type.to_string(), Box::new(factory));
    }

    pub fn get_metrics(&self) -> ReliableAckProcessorMetrics {
        self.metrics.clone()
    }

    pub fn is_backpressure_active(&self) -> bool {
        self.backpressure_active.load(Ordering::Relaxed)
    }
}

struct ReliableAckProcessorWorker {
    ack_receiver: Receiver<AckTask>,
    ack_sender: Sender<AckTask>,
    metrics: ReliableAckProcessorMetrics,
    cancellation_token: CancellationToken,
    wal: Arc<AckWAL>,
    ack_registry: Arc<Mutex<HashMap<String, Box<dyn Fn(&Vec<u8>) -> Arc<dyn Ack> + Send + Sync>>>>,
    backpressure_active: Arc<AtomicBool>,
}

impl ReliableAckProcessorWorker {
    async fn run(self) {
        info!("Reliable ack processor started");

        // Recover unprocessed acks from WAL
        if let Err(e) = self.recover_from_wal().await {
            error!("Failed to recover from WAL: {}", e);
        }

        let mut pending_tasks = Vec::with_capacity(BATCH_SIZE);
        let mut last_persist = Instant::now();

        loop {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    break;
                }
                result = self.ack_receiver.recv_async() => {
                    match result {
                        Ok(task) => {
                            pending_tasks.push(task);

                            // Check backpressure
                            if pending_tasks.len() > BACKPRESSURE_THRESHOLD {
                                self.backpressure_active.store(true, Ordering::Relaxed);
                                warn!("Backpressure activated - {} pending acks", pending_tasks.len());
                            }

                            if pending_tasks.len() >= BATCH_SIZE ||
                               last_persist.elapsed() > Duration::from_millis(PERSIST_INTERVAL_MS) {
                                self.process_batch(&mut pending_tasks).await;
                                last_persist = Instant::now();
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
                        last_persist = Instant::now();
                    }
                }
            }
        }

        // Process remaining tasks before shutdown
        if !pending_tasks.is_empty() {
            self.process_batch(&mut pending_tasks).await;
        }

        info!("Reliable ack processor stopped");
    }

    async fn recover_from_wal(&self) -> Result<(), crate::Error> {
        let records = self
            .wal
            .recover()
            .await
            .map_err(|e| crate::Error::Unknown(format!("Failed to read WAL: {}", e)))?;

        let mut recovered_count = 0;
        let registry = self.ack_registry.lock().await;

        for record in records {
            if let Some(factory) = registry.get(&record.ack_type) {
                let ack = factory(&record.payload);
                let task = AckTask::from_record(record, ack);

                // Re-add to processing queue
                if let Err(e) = self.ack_sender.send_async(task).await {
                    error!("Failed to queue recovered ack: {}", e);
                } else {
                    recovered_count += 1;
                    self.metrics.recovered_acks.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        if recovered_count > 0 {
            info!("Recovered {} unprocessed acks from WAL", recovered_count);
        }

        // Clear WAL after successful recovery
        self.wal
            .clear()
            .await
            .map_err(|e| crate::Error::Unknown(format!("Failed to clear WAL: {}", e)))?;

        Ok(())
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
                    tasks_to_remove.push(i);
                }
                Err(_) => {
                    if task.should_retry() {
                        task.increment_retry();
                        self.metrics.retried_acks.fetch_add(1, Ordering::Relaxed);
                        retried_count += 1;
                        // Add exponential backoff
                        tokio::time::sleep(Duration::from_millis(
                            RETRY_DELAY_MS * (task.retry_count as u64).min(10),
                        ))
                        .await;
                    } else {
                        error!("Ack task failed after {} retries", task.retry_count);
                        self.metrics.failed_acks.fetch_add(1, Ordering::Relaxed);
                        self.metrics.pending_acks.fetch_sub(1, Ordering::Relaxed);
                        failed_count += 1;
                        tasks_to_remove.push(i);
                    }
                }
            }
        }

        // Remove completed tasks
        for &i in tasks_to_remove.iter().rev() {
            tasks.remove(i);
        }

        // Update backpressure status
        if tasks.len() < BACKPRESSURE_THRESHOLD / 2 {
            self.backpressure_active.store(false, Ordering::Relaxed);
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
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_ack_task_creation() {
        let ack = Arc::new(NoopAck);
        let task = AckTask::new(ack, 1, "test".to_string(), vec![1, 2, 3]);

        assert_eq!(task.retry_count, 0);
        assert_eq!(task.sequence, 1);
        assert!(!task.is_expired());
        assert!(task.should_retry());
    }

    #[tokio::test]
    async fn test_wal_operations() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("test.wal");
        let wal = AckWAL::new(&wal_path).unwrap();

        let record = AckRecord {
            sequence: 1,
            ack_type: "test".to_string(),
            payload: vec![1, 2, 3],
            retry_count: 0,
            created_at: std::time::SystemTime::now(),
            last_retry: None,
        };

        wal.append(&record).await.unwrap();
        let recovered = wal.recover().await.unwrap();

        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].sequence, 1);
    }
}
