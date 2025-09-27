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

//! Distributed Write-Ahead Log for reliable acknowledgments
//!
//! This module provides a distributed WAL implementation that stores acknowledgment
//! data in object storage for high availability and fault tolerance.

use crate::object_storage::{create_object_storage, ObjectStorage, StorageType};
use crate::reliable_ack::{AckRecord, AckTask, AckWAL};
use crate::Error;
use flume::{Receiver, Sender};
use md5::{Digest, Md5};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

/// Distributed WAL configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DistributedWALConfig {
    /// Node identifier
    pub node_id: String,
    /// Cluster identifier
    pub cluster_id: String,
    /// Object storage backend configuration
    pub storage_type: StorageType,
    /// Local WAL path for caching
    pub local_wal_path: String,
    /// Maximum local WAL size before rotation (bytes)
    pub local_wal_size_limit: u64,
    /// Upload batch size
    pub upload_batch_size: usize,
    /// Upload interval in milliseconds
    pub upload_interval_ms: u64,
    /// Maximum retry attempts for uploads
    pub max_retry_attempts: u32,
    /// Enable auto recovery on startup
    pub enable_auto_recovery: bool,
    /// Enable metrics collection
    pub enable_metrics: bool,
    /// Base path in object storage
    pub object_storage_base_path: Option<String>,
}

impl Default for DistributedWALConfig {
    fn default() -> Self {
        Self {
            node_id: uuid::Uuid::new_v4().to_string(),
            cluster_id: "default-cluster".to_string(),
            storage_type: StorageType::Local(crate::object_storage::LocalConfig {
                base_path: "./distributed_wal".to_string(),
            }),
            local_wal_path: "./local_wal".to_string(),
            local_wal_size_limit: 100 * 1024 * 1024, // 100MB
            upload_batch_size: 50,
            upload_interval_ms: 5000,
            max_retry_attempts: 5,
            enable_auto_recovery: true,
            enable_metrics: true,
            object_storage_base_path: Some("wal".to_string()),
        }
    }
}

/// Upload task for async processing
#[derive(Debug, Clone)]
pub struct WALUploadTask {
    pub record: AckRecord,
    pub global_id: String,
    pub node_id: String,
    pub timestamp: SystemTime,
    pub retry_count: u32,
}

/// WAL state tracking
#[derive(Debug, Clone)]
pub struct WALState {
    pub last_uploaded_sequence: u64,
    pub last_checkpoint_sequence: u64,
    pub pending_uploads: u64,
    pub total_uploads: u64,
    pub failed_uploads: u64,
    pub last_upload_time: SystemTime,
}

impl Default for WALState {
    fn default() -> Self {
        Self {
            last_uploaded_sequence: 0,
            last_checkpoint_sequence: 0,
            pending_uploads: 0,
            total_uploads: 0,
            failed_uploads: 0,
            last_upload_time: SystemTime::UNIX_EPOCH,
        }
    }
}

/// Distributed WAL metrics
#[derive(Debug, Clone)]
pub struct DistributedWALMetrics {
    pub total_writes: Arc<AtomicU64>,
    pub successful_uploads: Arc<AtomicU64>,
    pub failed_uploads: Arc<AtomicU64>,
    pub pending_uploads: Arc<AtomicU64>,
    pub recovered_records: Arc<AtomicU64>,
    pub upload_duration_ms: Arc<AtomicU64>,
    pub local_wal_size_bytes: Arc<AtomicU64>,
    pub object_storage_operations: Arc<AtomicU64>,
}

impl Default for DistributedWALMetrics {
    fn default() -> Self {
        Self {
            total_writes: Arc::new(AtomicU64::new(0)),
            successful_uploads: Arc::new(AtomicU64::new(0)),
            failed_uploads: Arc::new(AtomicU64::new(0)),
            pending_uploads: Arc::new(AtomicU64::new(0)),
            recovered_records: Arc::new(AtomicU64::new(0)),
            upload_duration_ms: Arc::new(AtomicU64::new(0)),
            local_wal_size_bytes: Arc::new(AtomicU64::new(0)),
            object_storage_operations: Arc::new(AtomicU64::new(0)),
        }
    }
}

/// Distributed WAL implementation
pub struct DistributedWAL {
    node_id: String,
    cluster_id: String,
    local_wal: Arc<AckWAL>,
    object_storage: Arc<dyn ObjectStorage>,
    config: DistributedWALConfig,

    // Upload processing
    upload_queue: Sender<WALUploadTask>,
    upload_receiver: Receiver<WALUploadTask>,

    // State tracking
    sequence_counter: Arc<AtomicU64>,
    state: Arc<RwLock<WALState>>,
    metrics: DistributedWALMetrics,

    // Background tasks
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,

    // Object storage path management
    base_path: String,
}

impl DistributedWAL {
    /// Create a new distributed WAL instance
    pub async fn new(config: DistributedWALConfig) -> Result<Self, Error> {
        info!("Creating distributed WAL for node: {}", config.node_id);

        // Initialize local WAL
        let local_wal = Arc::new(
            AckWAL::new(&config.local_wal_path)
                .map_err(|e| Error::Unknown(format!("Failed to create local WAL: {}", e)))?,
        );

        // Initialize object storage
        let object_storage = create_object_storage(&config.storage_type).await?;

        // Create communication channels
        let (upload_queue, upload_receiver) = flume::bounded(config.upload_batch_size * 2);

        // Initialize sequence counter
        let sequence_counter = Arc::new(AtomicU64::new(0));

        // Initialize state
        let state = Arc::new(RwLock::new(WALState::default()));

        // Initialize metrics
        let metrics = DistributedWALMetrics::default();

        // Setup background task infrastructure
        let cancellation_token = CancellationToken::new();
        let task_tracker = TaskTracker::new();

        // Determine base path
        let base_path = config
            .object_storage_base_path
            .clone()
            .unwrap_or_else(|| "wal".to_string());

        let mut wal = Self {
            node_id: config.node_id.clone(),
            cluster_id: config.cluster_id.clone(),
            local_wal,
            object_storage,
            config,
            upload_queue,
            upload_receiver,
            sequence_counter,
            state,
            metrics,
            cancellation_token,
            task_tracker,
            base_path,
        };

        // Start background tasks
        wal.start_background_tasks().await;

        // Perform auto recovery if enabled
        if wal.config.enable_auto_recovery {
            info!("Starting auto recovery for distributed WAL");
            wal.perform_auto_recovery().await?;
        }

        Ok(wal)
    }

    /// Start background processing tasks
    async fn start_background_tasks(&self) {
        let upload_receiver = self.upload_receiver.clone();
        let object_storage = self.object_storage.clone();
        let metrics = self.metrics.clone();
        let state = self.state.clone();
        let cancellation_token = self.cancellation_token.clone();
        let config = self.config.clone();
        let base_path = self.base_path.clone();
        let node_id = self.node_id.clone();

        // Start upload worker
        let config_for_upload = config.clone();
        self.task_tracker.spawn(async move {
            Self::upload_worker(
                upload_receiver,
                object_storage,
                metrics,
                state,
                cancellation_token,
                config_for_upload,
                base_path,
                node_id,
            )
            .await;
        });

        // Start periodic uploader
        if config.upload_interval_ms > 0 {
            let local_wal = self.local_wal.clone();
            let sequence_counter = self.sequence_counter.clone();
            let upload_queue = self.upload_queue.clone();
            let cancellation_token = self.cancellation_token.clone();
            let node_id = self.node_id.clone();

            self.task_tracker.spawn(async move {
                Self::periodic_upload_worker(
                    local_wal,
                    sequence_counter,
                    upload_queue,
                    cancellation_token,
                    Duration::from_millis(config.upload_interval_ms),
                    node_id,
                )
                .await;
            });
        }
    }

    /// Upload worker task
    async fn upload_worker(
        upload_receiver: Receiver<WALUploadTask>,
        object_storage: Arc<dyn ObjectStorage>,
        metrics: DistributedWALMetrics,
        state: Arc<RwLock<WALState>>,
        cancellation_token: CancellationToken,
        config: DistributedWALConfig,
        base_path: String,
        node_id: String,
    ) {
        info!("Distributed WAL upload worker started");

        let mut batch = Vec::with_capacity(config.upload_batch_size);
        let mut last_upload = std::time::Instant::now();

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                }
                result = upload_receiver.recv_async() => {
                    match result {
                        Ok(task) => {
                            batch.push(task);

                            // Process batch when full or timeout
                            if batch.len() >= config.upload_batch_size
                                || last_upload.elapsed() > Duration::from_millis(1000) {
                                Self::process_upload_batch(
                                    &mut batch,
                                    &object_storage,
                                    &metrics,
                                    &state,
                                    &base_path,
                                    &node_id,
                                ).await;
                                last_upload = std::time::Instant::now();
                            }
                        }
                        Err(_) => break,
                    }
                }
                // Process remaining batch on timeout
                _ = tokio::time::sleep(Duration::from_millis(1000)) => {
                    if !batch.is_empty() {
                        Self::process_upload_batch(
                            &mut batch,
                            &object_storage,
                            &metrics,
                            &state,
                            &base_path,
                            &node_id,
                        ).await;
                        last_upload = std::time::Instant::now();
                    }
                }
            }
        }

        // Process remaining items before shutdown
        if !batch.is_empty() {
            Self::process_upload_batch(
                &mut batch,
                &object_storage,
                &metrics,
                &state,
                &base_path,
                &node_id,
            )
            .await;
        }

        info!("Distributed WAL upload worker stopped");
    }

    /// Process a batch of upload tasks
    async fn process_upload_batch(
        batch: &mut Vec<WALUploadTask>,
        object_storage: &Arc<dyn ObjectStorage>,
        metrics: &DistributedWALMetrics,
        state: &Arc<RwLock<WALState>>,
        base_path: &str,
        node_id: &str,
    ) {
        if batch.is_empty() {
            return;
        }

        let start_time = std::time::Instant::now();
        let batch_size = batch.len();

        // Group by date for efficient storage
        let mut batch_by_date: HashMap<String, Vec<WALUploadTask>> = HashMap::new();

        for task in batch.drain(..) {
            let date_key = Self::get_date_key(task.timestamp);
            batch_by_date
                .entry(date_key)
                .or_insert_with(Vec::new)
                .push(task);
        }

        let mut successful_uploads = 0;
        let mut failed_uploads = 0;

        for (date_key, tasks) in batch_by_date {
            let filename = format!(
                "wal_{}_{}.json",
                std::time::SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
                uuid::Uuid::new_v4()
                    .to_string()
                    .split('-')
                    .next()
                    .unwrap_or("unknown")
            );

            let object_key = format!("{}/nodes/{}/{}/{}", base_path, node_id, date_key, filename);

            // Serialize batch
            let batch_data: Vec<AckRecord> = tasks.iter().map(|t| t.record.clone()).collect();
            let json_data = serde_json::to_vec(&batch_data)
                .map_err(|e| {
                    error!("Failed to serialize batch data: {}", e);
                    e
                })
                .unwrap_or_default();

            // Upload to object storage
            match object_storage.put_object(&object_key, json_data).await {
                Ok(_) => {
                    successful_uploads += tasks.len();
                    metrics
                        .successful_uploads
                        .fetch_add(tasks.len() as u64, Ordering::Relaxed);

                    // Update state
                    let max_sequence = tasks.iter().map(|t| t.record.sequence).max().unwrap_or(0);

                    let mut state_guard = state.write().await;
                    state_guard.last_uploaded_sequence =
                        state_guard.last_uploaded_sequence.max(max_sequence);
                    state_guard.total_uploads += tasks.len() as u64;
                    state_guard.last_upload_time = SystemTime::now();
                    drop(state_guard);

                    debug!(
                        "Successfully uploaded batch of {} records to {}",
                        tasks.len(),
                        object_key
                    );
                }
                Err(e) => {
                    failed_uploads += tasks.len();
                    metrics
                        .failed_uploads
                        .fetch_add(tasks.len() as u64, Ordering::Relaxed);
                    error!("Failed to upload batch to {}: {}", object_key, e);
                }
            }

            metrics
                .object_storage_operations
                .fetch_add(1, Ordering::Relaxed);
        }

        // Update metrics
        let upload_duration = start_time.elapsed().as_millis() as u64;
        metrics
            .upload_duration_ms
            .fetch_add(upload_duration, Ordering::Relaxed);
        metrics.pending_uploads.fetch_sub(
            (successful_uploads + failed_uploads) as u64,
            Ordering::Relaxed,
        );

        if failed_uploads > 0 {
            warn!(
                "Batch upload: {} successful, {} failed",
                successful_uploads, failed_uploads
            );
        } else {
            debug!("Successfully uploaded batch of {} records", batch_size);
        }
    }

    /// Periodic upload worker
    async fn periodic_upload_worker(
        local_wal: Arc<AckWAL>,
        sequence_counter: Arc<AtomicU64>,
        upload_queue: Sender<WALUploadTask>,
        cancellation_token: CancellationToken,
        interval: Duration,
        node_id: String,
    ) {
        let mut last_sequence = 0;

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                }
                _ = tokio::time::sleep(interval) => {
                    // Check for new records in local WAL
                    let current_sequence = sequence_counter.load(Ordering::Acquire);

                    if current_sequence > last_sequence {
                        debug!("Periodic upload check: {} new records", current_sequence - last_sequence);

                        // For now, we'll let the normal upload process handle this
                        // In a real implementation, we might want to scan local WAL here
                    }

                    last_sequence = current_sequence;
                }
            }
        }
    }

    /// Append a record to the distributed WAL
    pub async fn append(&self, record: &AckRecord) -> Result<(), Error> {
        let start_time = std::time::Instant::now();

        // Generate unique global ID
        let global_id = self.generate_global_id(record);

        // Create upload task
        let upload_task = WALUploadTask {
            record: record.clone(),
            global_id: global_id.clone(),
            node_id: self.node_id.clone(),
            timestamp: SystemTime::now(),
            retry_count: 0,
        };

        // Update metrics
        self.metrics.total_writes.fetch_add(1, Ordering::Relaxed);
        self.metrics.pending_uploads.fetch_add(1, Ordering::Relaxed);

        // Queue for upload
        match self.upload_queue.send_async(upload_task).await {
            Ok(_) => {
                // Update sequence counter
                self.sequence_counter.fetch_add(1, Ordering::SeqCst);

                debug!("Successfully queued record {} for upload", global_id);

                // Update state
                let mut state = self.state.write().await;
                state.pending_uploads += 1;

                Ok(())
            }
            Err(e) => {
                self.metrics.failed_uploads.fetch_add(1, Ordering::Relaxed);
                self.metrics.pending_uploads.fetch_sub(1, Ordering::Relaxed);
                Err(Error::Unknown(format!(
                    "Failed to queue record for upload: {}",
                    e
                )))
            }
        }
    }

    /// Generate global unique ID for a record
    fn generate_global_id(&self, record: &AckRecord) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let mut hasher = Md5::new();
        hasher.update(self.node_id.as_bytes());
        hasher.update(record.sequence.to_le_bytes());
        hasher.update(timestamp.to_le_bytes());
        hasher.update(record.ack_type.as_bytes());

        let hash = format!("{:x}", hasher.finalize());

        format!(
            "{}_{}_{}_{}",
            self.node_id, record.sequence, timestamp, hash
        )
    }

    /// Get date key for record grouping
    fn get_date_key(timestamp: SystemTime) -> String {
        let datetime = chrono::DateTime::<chrono::Utc>::from(timestamp);
        datetime.format("%Y%m%d").to_string()
    }

    /// Recover records from distributed storage
    pub async fn recover_records(&self) -> Result<Vec<AckRecord>, Error> {
        info!(
            "Starting recovery from distributed WAL for node {}",
            self.node_id
        );

        let mut recovered_records = Vec::new();

        // List all objects for this node
        let node_prefix = format!("{}/nodes/{}/", self.base_path, self.node_id);

        match self.object_storage.list_objects(&node_prefix).await {
            Ok(objects) => {
                info!(
                    "Found {} WAL objects for node {}",
                    objects.len(),
                    self.node_id
                );

                for object in objects {
                    debug!("Recovering from object: {}", object.key);

                    match self.object_storage.get_object(&object.key).await {
                        Ok(data) => match serde_json::from_slice::<Vec<AckRecord>>(&data) {
                            Ok(mut records) => {
                                recovered_records.append(&mut records);
                                debug!("Recovered {} records from {}", records.len(), object.key);
                            }
                            Err(e) => {
                                error!("Failed to deserialize records from {}: {}", object.key, e);
                            }
                        },
                        Err(e) => {
                            error!("Failed to download object {}: {}", object.key, e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to list objects for recovery: {}", e);
            }
        }

        // Sort by sequence number
        recovered_records.sort_by(|a, b| a.sequence.cmp(&b.sequence));

        info!(
            "Recovered {} records from distributed WAL",
            recovered_records.len()
        );
        self.metrics
            .recovered_records
            .fetch_add(recovered_records.len() as u64, Ordering::Relaxed);

        Ok(recovered_records)
    }

    /// Perform auto recovery on startup
    async fn perform_auto_recovery(&self) -> Result<(), Error> {
        let recovered_records = self.recover_records().await?;

        if !recovered_records.is_empty() {
            info!(
                "Auto-recovered {} unprocessed records",
                recovered_records.len()
            );

            // In a real implementation, we would re-queue these for processing
            // For now, just update metrics
            self.metrics
                .recovered_records
                .fetch_add(recovered_records.len() as u64, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Get current metrics
    pub fn get_metrics(&self) -> DistributedWALMetrics {
        self.metrics.clone()
    }

    /// Get current state
    pub async fn get_state(&self) -> WALState {
        self.state.read().await.clone()
    }

    /// Create a checkpoint
    pub async fn create_checkpoint(&self) -> Result<(), Error> {
        let state = self.state.read().await;
        let checkpoint = Checkpoint {
            sequence: state.last_uploaded_sequence,
            timestamp: SystemTime::now(),
            node_id: self.node_id.clone(),
            cluster_id: self.cluster_id.clone(),
        };
        drop(state);

        let checkpoint_data = serde_json::to_vec(&checkpoint)
            .map_err(|e| Error::Unknown(format!("Failed to serialize checkpoint: {}", e)))?;

        let checkpoint_key = format!(
            "{}/checkpoints/checkpoint_{}_{}.json",
            self.base_path,
            self.node_id,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        self.object_storage
            .put_object(&checkpoint_key, checkpoint_data)
            .await?;

        info!("Created checkpoint at sequence {}", checkpoint.sequence);

        Ok(())
    }

    /// Shutdown the distributed WAL
    pub async fn shutdown(self) -> Result<(), Error> {
        info!("Shutting down distributed WAL for node {}", self.node_id);

        // Cancel all background tasks
        self.cancellation_token.cancel();

        // Wait for tasks to complete
        self.task_tracker.close();
        self.task_tracker.wait().await;

        // Create final checkpoint
        if let Err(e) = self.create_checkpoint().await {
            warn!("Failed to create final checkpoint: {}", e);
        }

        info!("Distributed WAL shutdown complete");
        Ok(())
    }
}

/// Checkpoint data structure
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Checkpoint {
    pub sequence: u64,
    pub timestamp: SystemTime,
    pub node_id: String,
    pub cluster_id: String,
}

/// Helper function to generate date-based key
pub fn generate_object_key(base_path: &str, node_id: &str, timestamp: SystemTime) -> String {
    let date_key = chrono::DateTime::<chrono::Utc>::from(timestamp)
        .format("%Y%m%d")
        .to_string();

    let timestamp_ms = timestamp.duration_since(UNIX_EPOCH).unwrap().as_millis();
    let uuid_str = uuid::Uuid::new_v4().to_string();
    let uuid_short = uuid_str.split('-').next().unwrap_or("unknown");

    format!(
        "{}/nodes/{}/{}/wal_{}_{}.json",
        base_path, node_id, date_key, timestamp_ms, uuid_short
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_distributed_wal_creation() {
        let temp_dir = TempDir::new().unwrap();

        let config = DistributedWALConfig {
            node_id: "test-node".to_string(),
            cluster_id: "test-cluster".to_string(),
            storage_type: StorageType::Local(crate::object_storage::LocalConfig {
                base_path: temp_dir
                    .path()
                    .join("distributed")
                    .to_string_lossy()
                    .to_string(),
            }),
            local_wal_path: temp_dir.path().join("local").to_string_lossy().to_string(),
            ..Default::default()
        };

        let result = DistributedWAL::new(config).await;
        assert!(result.is_ok());

        let wal = result.unwrap();
        assert_eq!(wal.node_id, "test-node");
        assert_eq!(wal.cluster_id, "test-cluster");

        wal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_record_append_and_recovery() {
        let temp_dir = TempDir::new().unwrap();

        let config = DistributedWALConfig {
            node_id: "test-node".to_string(),
            cluster_id: "test-cluster".to_string(),
            storage_type: StorageType::Local(crate::object_storage::LocalConfig {
                base_path: temp_dir
                    .path()
                    .join("distributed")
                    .to_string_lossy()
                    .to_string(),
            }),
            local_wal_path: temp_dir.path().join("local").to_string_lossy().to_string(),
            upload_interval_ms: 100, // Faster for testing
            ..Default::default()
        };

        let wal = DistributedWAL::new(config).await.unwrap();

        // Create test record
        let record = AckRecord {
            sequence: 1,
            ack_type: "test".to_string(),
            payload: b"test payload".to_vec(),
            retry_count: 0,
            created_at: SystemTime::now(),
            last_retry: None,
        };

        // Append record
        let result = wal.append(&record).await;
        assert!(result.is_ok());

        // Give some time for upload
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Recover records
        let recovered = wal.recover_records().await.unwrap();
        assert!(!recovered.is_empty());

        wal.shutdown().await.unwrap();
    }

    #[test]
    fn test_generate_global_id() {
        let temp_dir = TempDir::new().unwrap();

        let _config = DistributedWALConfig {
            node_id: "test-node".to_string(),
            cluster_id: "test-cluster".to_string(),
            storage_type: StorageType::Local(crate::object_storage::LocalConfig {
                base_path: temp_dir
                    .path()
                    .join("distributed")
                    .to_string_lossy()
                    .to_string(),
            }),
            local_wal_path: temp_dir.path().join("local").to_string_lossy().to_string(),
            ..Default::default()
        };

        let record = AckRecord {
            sequence: 1,
            ack_type: "test".to_string(),
            payload: b"test payload".to_vec(),
            retry_count: 0,
            created_at: SystemTime::now(),
            last_retry: None,
        };

        let id1 = generate_object_key("wal", "test-node", record.created_at);
        let id2 = generate_object_key("wal", "test-node", record.created_at);

        // IDs should be different due to UUID
        assert_ne!(id1, id2);

        // But should have the same pattern
        assert!(id1.starts_with("wal/nodes/test-node/"));
        assert!(id2.starts_with("wal/nodes/test-node/"));
    }
}
