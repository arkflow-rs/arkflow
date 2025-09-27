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

//! Checkpoint management for distributed WAL
//!
//! This module provides checkpoint creation, management, and recovery
//! functionality for distributed WAL systems.

use crate::object_storage::{create_object_storage, ObjectStorage, StorageType};
use crate::Error;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// Checkpoint configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CheckpointConfig {
    /// Object storage configuration
    pub storage_type: StorageType,
    /// Base path for checkpoint storage
    pub base_path: String,
    /// Checkpoint creation interval
    pub checkpoint_interval_ms: u64,
    /// Maximum number of checkpoints to retain
    pub max_checkpoints: usize,
    /// Enable automatic checkpoint creation
    pub auto_checkpoint: bool,
    /// Checkpoint compression
    pub enable_compression: bool,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            storage_type: StorageType::Local(crate::object_storage::LocalConfig {
                base_path: "./checkpoints".to_string(),
            }),
            base_path: "checkpoints".to_string(),
            checkpoint_interval_ms: 300000, // 5 minutes
            max_checkpoints: 10,
            auto_checkpoint: true,
            enable_compression: true,
        }
    }
}

/// Checkpoint metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CheckpointMetadata {
    pub sequence: u64,
    pub timestamp: SystemTime,
    pub node_id: String,
    pub cluster_id: String,
    pub checksum: String,
    pub size_bytes: u64,
    pub compressed: bool,
    pub previous_checkpoint: Option<String>,
}

/// Checkpoint information with additional details
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CheckpointInfo {
    pub checkpoint_id: String,
    pub metadata: CheckpointMetadata,
    pub nodes_included: Vec<String>,
    pub total_records: u64,
    pub creation_duration_ms: u64,
}

/// Checkpoint manager
pub struct CheckpointManager {
    cluster_id: String,
    object_storage: Arc<dyn ObjectStorage>,
    config: CheckpointConfig,
    checkpoints: Arc<RwLock<BTreeMap<SystemTime, CheckpointInfo>>>,
    cancellation_token: CancellationToken,
    task_tracker: tokio_util::task::TaskTracker,
}

impl CheckpointManager {
    /// Create a new checkpoint manager
    pub async fn new(cluster_id: String, config: CheckpointConfig) -> Result<Self, Error> {
        let object_storage = create_object_storage(&config.storage_type).await?;

        let manager = Self {
            cluster_id: cluster_id.clone(),
            object_storage,
            config: config.clone(),
            checkpoints: Arc::new(RwLock::new(BTreeMap::new())),
            cancellation_token: CancellationToken::new(),
            task_tracker: tokio_util::task::TaskTracker::new(),
        };

        // Load existing checkpoints
        manager.load_checkpoints().await?;

        // Start automatic checkpoint creation if enabled
        if config.auto_checkpoint {
            manager.start_auto_checkpoint().await;
        }

        Ok(manager)
    }

    /// Load existing checkpoints from storage
    async fn load_checkpoints(&self) -> Result<(), Error> {
        let checkpoints_prefix = format!("{}/", self.config.base_path);
        let mut checkpoints = BTreeMap::new();

        match self.object_storage.list_objects(&checkpoints_prefix).await {
            Ok(objects) => {
                for object in objects {
                    if object.key.ends_with("_checkpoint.json") {
                        match self.load_checkpoint_from_object(&object.key).await {
                            Ok(checkpoint_info) => {
                                checkpoints
                                    .insert(checkpoint_info.metadata.timestamp, checkpoint_info);
                            }
                            Err(e) => {
                                error!("Failed to load checkpoint from {}: {}", object.key, e);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to list checkpoints: {}", e);
            }
        }

        // Update checkpoints cache
        let mut checkpoints_cache = self.checkpoints.write().await;
        *checkpoints_cache = checkpoints;

        info!("Loaded {} existing checkpoints", checkpoints_cache.len());
        Ok(())
    }

    /// Load checkpoint from object
    async fn load_checkpoint_from_object(&self, object_key: &str) -> Result<CheckpointInfo, Error> {
        let data = self.object_storage.get_object(object_key).await?;

        let checkpoint_info: CheckpointInfo = serde_json::from_slice(&data)
            .map_err(|e| Error::Unknown(format!("Failed to deserialize checkpoint info: {}", e)))?;

        Ok(checkpoint_info)
    }

    /// Start automatic checkpoint creation
    async fn start_auto_checkpoint(&self) {
        let object_storage = self.object_storage.clone();
        let checkpoints = self.checkpoints.clone();
        let cancellation_token = self.cancellation_token.clone();
        let config = self.config.clone();
        let cluster_id = self.cluster_id.clone();

        self.task_tracker.spawn(async move {
            info!("Starting automatic checkpoint creation for cluster: {}", cluster_id);

            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(config.checkpoint_interval_ms)) => {
                        if let Err(e) = Self::create_auto_checkpoint(
                            &object_storage,
                            &checkpoints,
                            &config,
                            &cluster_id,
                        ).await {
                            error!("Failed to create automatic checkpoint: {}", e);
                        }
                    }
                }
            }

            info!("Automatic checkpoint creation stopped");
        });
    }

    /// Create automatic checkpoint
    async fn create_auto_checkpoint(
        object_storage: &Arc<dyn ObjectStorage>,
        checkpoints: &Arc<RwLock<BTreeMap<SystemTime, CheckpointInfo>>>,
        config: &CheckpointConfig,
        cluster_id: &str,
    ) -> Result<(), Error> {
        debug!("Creating automatic checkpoint for cluster: {}", cluster_id);

        // Get latest checkpoint info for sequence
        let checkpoints_guard = checkpoints.read().await;
        let latest_sequence = checkpoints_guard
            .values()
            .map(|cp| cp.metadata.sequence)
            .max()
            .unwrap_or(0);

        let previous_checkpoint = checkpoints_guard
            .values()
            .max_by_key(|cp| cp.metadata.timestamp)
            .map(|cp| cp.checkpoint_id.clone());
        drop(checkpoints_guard);

        // Create checkpoint metadata
        let checkpoint_id = format!(
            "checkpoint_{}_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            uuid::Uuid::new_v4()
                .to_string()
                .split('-')
                .next()
                .unwrap_or("unknown")
        );

        let metadata = CheckpointMetadata {
            sequence: latest_sequence,
            timestamp: SystemTime::now(),
            node_id: "auto".to_string(),
            cluster_id: cluster_id.to_string(),
            checksum: "auto".to_string(),
            size_bytes: 0,
            compressed: config.enable_compression,
            previous_checkpoint,
        };

        let checkpoint_info = CheckpointInfo {
            checkpoint_id: checkpoint_id.clone(),
            metadata: metadata.clone(),
            nodes_included: vec!["auto".to_string()],
            total_records: 0,
            creation_duration_ms: 0,
        };

        // Save checkpoint
        let checkpoint_key = format!("{}/{}_checkpoint.json", config.base_path, checkpoint_id);
        let data = serde_json::to_vec(&checkpoint_info)
            .map_err(|e| Error::Unknown(format!("Failed to serialize checkpoint info: {}", e)))?;

        object_storage.put_object(&checkpoint_key, data).await?;

        // Update cache
        {
            let mut checkpoints_guard = checkpoints.write().await;
            checkpoints_guard.insert(metadata.timestamp, checkpoint_info.clone());
        }

        // Cleanup old checkpoints (note: this is a static function, cleanup is handled by caller)
        debug!("Created automatic checkpoint: {}", checkpoint_id);
        Ok(())
    }

    /// Create a manual checkpoint
    pub async fn create_checkpoint(
        &self,
        sequence: u64,
        node_id: String,
        additional_data: Option<HashMap<String, serde_json::Value>>,
    ) -> Result<String, Error> {
        let start_time = SystemTime::now();

        info!(
            "Creating checkpoint at sequence {} for node {}",
            sequence, node_id
        );

        // Get previous checkpoint
        let checkpoints_guard = self.checkpoints.read().await;
        let previous_checkpoint = checkpoints_guard
            .values()
            .max_by_key(|cp| cp.metadata.timestamp)
            .map(|cp| cp.checkpoint_id.clone());
        drop(checkpoints_guard);

        // Create checkpoint ID
        let checkpoint_id = format!(
            "checkpoint_{}_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            uuid::Uuid::new_v4()
                .to_string()
                .split('-')
                .next()
                .unwrap_or("unknown")
        );

        // Create metadata
        let metadata = CheckpointMetadata {
            sequence,
            timestamp: SystemTime::now(),
            node_id: node_id.clone(),
            cluster_id: self.cluster_id.clone(),
            checksum: self.calculate_checksum(sequence, &node_id),
            size_bytes: 0,
            compressed: self.config.enable_compression,
            previous_checkpoint,
        };

        let mut checkpoint_info = CheckpointInfo {
            checkpoint_id: checkpoint_id.clone(),
            metadata: metadata.clone(),
            nodes_included: vec![node_id.clone()],
            total_records: 0,
            creation_duration_ms: 0,
        };

        // Save checkpoint
        let checkpoint_key = format!(
            "{}/{}_checkpoint.json",
            self.config.base_path, checkpoint_id
        );
        let mut data = serde_json::to_vec(&checkpoint_info)
            .map_err(|e| Error::Unknown(format!("Failed to serialize checkpoint info: {}", e)))?;

        // Add additional data if provided
        if let Some(additional) = additional_data {
            let mut full_data = serde_json::Map::new();
            full_data.insert(
                "checkpoint".to_string(),
                serde_json::to_value(&checkpoint_info).unwrap(),
            );
            full_data.insert(
                "additional".to_string(),
                serde_json::to_value(additional).unwrap(),
            );

            data = serde_json::to_vec(&full_data).map_err(|e| {
                Error::Unknown(format!("Failed to serialize full checkpoint data: {}", e))
            })?;
        }

        if self.config.enable_compression {
            data = Self::compress_data(&data)?;
        }

        let data_len = data.len() as u64;
        let creation_duration = start_time.elapsed().unwrap().as_millis() as u64;

        self.object_storage
            .put_object(&checkpoint_key, data)
            .await?;

        // Update cache
        {
            let mut checkpoints_guard = self.checkpoints.write().await;
            checkpoint_info.metadata.size_bytes = data_len;
            checkpoint_info.creation_duration_ms = creation_duration;
            checkpoints_guard.insert(metadata.timestamp, checkpoint_info.clone());
        }

        // Cleanup old checkpoints
        self.cleanup_old_checkpoints().await?;

        info!(
            "Created checkpoint: {} at sequence {}",
            checkpoint_id, sequence
        );
        Ok(checkpoint_id)
    }

    /// Calculate checksum for checkpoint
    fn calculate_checksum(&self, sequence: u64, node_id: &str) -> String {
        use md5::{Digest, Md5};

        let mut hasher = Md5::new();
        hasher.update(self.cluster_id.as_bytes());
        hasher.update(node_id.as_bytes());
        hasher.update(sequence.to_le_bytes());
        hasher.update(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .to_le_bytes(),
        );

        format!("{:x}", hasher.finalize())
    }

    /// Compress data using flate2
    fn compress_data(data: &[u8]) -> Result<Vec<u8>, Error> {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(data)
            .map_err(|e| Error::Unknown(format!("Failed to compress checkpoint data: {}", e)))?;

        encoder
            .finish()
            .map_err(|e| Error::Unknown(format!("Failed to finish compression: {}", e)))
    }

    /// Decompress data
    fn decompress_data(compressed_data: &[u8]) -> Result<Vec<u8>, Error> {
        use flate2::read::GzDecoder;
        use std::io::Read;

        let mut decoder = GzDecoder::new(compressed_data);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| Error::Unknown(format!("Failed to decompress checkpoint data: {}", e)))?;

        Ok(decompressed)
    }

    /// Cleanup old checkpoints
    async fn cleanup_old_checkpoints(&self) -> Result<(), Error> {
        let mut checkpoints_guard = self.checkpoints.write().await;

        if checkpoints_guard.len() <= self.config.max_checkpoints {
            return Ok(());
        }

        let to_remove = checkpoints_guard.len() - self.config.max_checkpoints;
        let mut removed_count = 0;

        for (_, checkpoint_info) in checkpoints_guard.range(..).take(to_remove) {
            let checkpoint_key = format!(
                "{}/{}_checkpoint.json",
                self.config.base_path, checkpoint_info.checkpoint_id
            );

            if let Err(e) = self.object_storage.delete_object(&checkpoint_key).await {
                error!(
                    "Failed to delete old checkpoint {}: {}",
                    checkpoint_info.checkpoint_id, e
                );
            } else {
                removed_count += 1;
                debug!("Removed old checkpoint: {}", checkpoint_info.checkpoint_id);
            }
        }

        // Remove from cache
        let mut new_checkpoints = BTreeMap::new();
        for (timestamp, checkpoint_info) in checkpoints_guard.range(..) {
            if removed_count == 0 {
                new_checkpoints.insert(*timestamp, checkpoint_info.clone());
            } else {
                removed_count -= 1;
            }
        }

        *checkpoints_guard = new_checkpoints;

        if removed_count > 0 {
            info!("Cleaned up {} old checkpoints", removed_count);
        }

        Ok(())
    }

    /// Get the latest checkpoint
    pub async fn get_latest_checkpoint(&self) -> Result<Option<CheckpointInfo>, Error> {
        let checkpoints_guard = self.checkpoints.read().await;
        Ok(checkpoints_guard
            .values()
            .max_by_key(|cp| cp.metadata.timestamp)
            .cloned())
    }

    /// Get checkpoint by ID
    pub async fn get_checkpoint(
        &self,
        checkpoint_id: &str,
    ) -> Result<Option<CheckpointInfo>, Error> {
        let checkpoints_guard = self.checkpoints.read().await;
        Ok(checkpoints_guard
            .values()
            .find(|cp| cp.checkpoint_id == checkpoint_id)
            .cloned())
    }

    /// Get all checkpoints
    pub async fn get_all_checkpoints(&self) -> Result<Vec<CheckpointInfo>, Error> {
        let checkpoints_guard = self.checkpoints.read().await;
        Ok(checkpoints_guard.values().cloned().collect())
    }

    /// Get checkpoints after a specific sequence
    pub async fn get_checkpoints_after_sequence(
        &self,
        sequence: u64,
    ) -> Result<Vec<CheckpointInfo>, Error> {
        let checkpoints_guard = self.checkpoints.read().await;
        Ok(checkpoints_guard
            .values()
            .filter(|cp| cp.metadata.sequence > sequence)
            .cloned()
            .collect())
    }

    /// Restore from checkpoint
    pub async fn restore_from_checkpoint(
        &self,
        checkpoint_id: &str,
    ) -> Result<CheckpointInfo, Error> {
        info!("Restoring from checkpoint: {}", checkpoint_id);

        let checkpoint_info = self
            .get_checkpoint(checkpoint_id)
            .await?
            .ok_or_else(|| Error::Unknown(format!("Checkpoint not found: {}", checkpoint_id)))?;

        // Validate checksum
        let expected_checksum = checkpoint_info.metadata.checksum.clone();
        let calculated_checksum = self.calculate_checksum(
            checkpoint_info.metadata.sequence,
            &checkpoint_info.metadata.node_id,
        );

        if expected_checksum != calculated_checksum {
            return Err(Error::Unknown(format!(
                "Checksum validation failed for checkpoint: {}",
                checkpoint_id
            )));
        }

        info!("Successfully validated checkpoint: {}", checkpoint_id);
        Ok(checkpoint_info)
    }

    /// Get recovery point (best checkpoint to restore from)
    pub async fn get_recovery_point(&self) -> Result<Option<CheckpointInfo>, Error> {
        let checkpoints_guard = self.checkpoints.read().await;

        // Find the most recent valid checkpoint
        let mut valid_checkpoints: Vec<_> = checkpoints_guard
            .values()
            .filter(|cp| {
                // Basic validation - could be enhanced
                cp.metadata.sequence > 0 && !cp.metadata.checksum.is_empty()
            })
            .collect();

        valid_checkpoints.sort_by(|a, b| b.metadata.timestamp.cmp(&a.metadata.timestamp));

        Ok(valid_checkpoints.first().cloned().cloned())
    }

    /// Create recovery manifest
    pub async fn create_recovery_manifest(&self) -> Result<RecoveryManifest, Error> {
        let latest_checkpoint = self.get_latest_checkpoint().await?;
        let all_checkpoints = self.get_all_checkpoints().await?;
        let total_count = all_checkpoints.len();

        let manifest = RecoveryManifest {
            cluster_id: self.cluster_id.clone(),
            recovery_checkpoint: latest_checkpoint.map(|cp| cp.checkpoint_id),
            available_checkpoints: all_checkpoints
                .into_iter()
                .map(|cp| cp.checkpoint_id)
                .collect(),
            total_checkpoints: total_count,
            created_at: SystemTime::now(),
        };

        Ok(manifest)
    }

    /// Shutdown the checkpoint manager
    pub async fn shutdown(self) -> Result<(), Error> {
        info!(
            "Shutting down checkpoint manager for cluster: {}",
            self.cluster_id
        );

        // Cancel all background tasks
        self.cancellation_token.cancel();

        // Wait for tasks to complete
        self.task_tracker.close();
        self.task_tracker.wait().await;

        // Create final checkpoint
        if let Some(latest) = self.get_latest_checkpoint().await? {
            info!("Final checkpoint available: {}", latest.checkpoint_id);
        }

        Ok(())
    }
}

/// Recovery manifest for cluster restoration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RecoveryManifest {
    pub cluster_id: String,
    pub recovery_checkpoint: Option<String>,
    pub available_checkpoints: Vec<String>,
    pub total_checkpoints: usize,
    pub created_at: SystemTime,
}

/// Checkpoint statistics
#[derive(Debug, Clone)]
pub struct CheckpointStats {
    pub total_checkpoints: usize,
    pub latest_sequence: Option<u64>,
    pub latest_timestamp: Option<SystemTime>,
    pub total_size_bytes: u64,
    pub average_creation_time_ms: f64,
}

impl CheckpointManager {
    /// Get checkpoint statistics
    pub async fn get_stats(&self) -> Result<CheckpointStats, Error> {
        let checkpoints_guard = self.checkpoints.read().await;
        let checkpoints: Vec<_> = checkpoints_guard.values().collect();

        if checkpoints.is_empty() {
            return Ok(CheckpointStats {
                total_checkpoints: 0,
                latest_sequence: None,
                latest_timestamp: None,
                total_size_bytes: 0,
                average_creation_time_ms: 0.0,
            });
        }

        let latest_checkpoint = checkpoints.iter().max_by_key(|cp| cp.metadata.sequence);

        let total_size: u64 = checkpoints.iter().map(|cp| cp.metadata.size_bytes).sum();

        let average_time: f64 = checkpoints
            .iter()
            .map(|cp| cp.creation_duration_ms as f64)
            .sum::<f64>()
            / checkpoints.len() as f64;

        Ok(CheckpointStats {
            total_checkpoints: checkpoints.len(),
            latest_sequence: latest_checkpoint.map(|cp| cp.metadata.sequence),
            latest_timestamp: latest_checkpoint.map(|cp| cp.metadata.timestamp),
            total_size_bytes: total_size,
            average_creation_time_ms: average_time,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_checkpoint_manager_creation() {
        let temp_dir = TempDir::new().unwrap();

        let config = CheckpointConfig {
            storage_type: StorageType::Local(crate::object_storage::LocalConfig {
                base_path: temp_dir.path().to_string_lossy().to_string(),
            }),
            base_path: "checkpoints".to_string(),
            checkpoint_interval_ms: 1000,
            max_checkpoints: 3,
            auto_checkpoint: false,
            enable_compression: false,
        };

        let manager = CheckpointManager::new("test-cluster".to_string(), config)
            .await
            .unwrap();
        assert_eq!(manager.cluster_id, "test-cluster");

        let stats = manager.get_stats().await.unwrap();
        assert_eq!(stats.total_checkpoints, 0);

        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_checkpoint_creation_and_retrieval() {
        let temp_dir = TempDir::new().unwrap();

        let config = CheckpointConfig {
            storage_type: StorageType::Local(crate::object_storage::LocalConfig {
                base_path: temp_dir.path().to_string_lossy().to_string(),
            }),
            base_path: "checkpoints".to_string(),
            checkpoint_interval_ms: 1000,
            max_checkpoints: 5,
            auto_checkpoint: false,
            enable_compression: false,
        };

        let manager = CheckpointManager::new("test-cluster".to_string(), config)
            .await
            .unwrap();

        // Create checkpoint
        let checkpoint_id = manager
            .create_checkpoint(100, "test-node".to_string(), None)
            .await
            .unwrap();

        // Verify checkpoint exists
        let retrieved = manager.get_checkpoint(&checkpoint_id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().metadata.sequence, 100);

        // Verify latest checkpoint
        let latest = manager.get_latest_checkpoint().await.unwrap();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().checkpoint_id, checkpoint_id);

        // Verify stats
        let stats = manager.get_stats().await.unwrap();
        assert_eq!(stats.total_checkpoints, 1);
        assert_eq!(stats.latest_sequence, Some(100));

        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_checkpoint_cleanup() {
        let temp_dir = TempDir::new().unwrap();

        let config = CheckpointConfig {
            storage_type: StorageType::Local(crate::object_storage::LocalConfig {
                base_path: temp_dir.path().to_string_lossy().to_string(),
            }),
            base_path: "checkpoints".to_string(),
            checkpoint_interval_ms: 1000,
            max_checkpoints: 2,
            auto_checkpoint: false,
            enable_compression: false,
        };

        let manager = CheckpointManager::new("test-cluster".to_string(), config)
            .await
            .unwrap();

        // Create multiple checkpoints
        manager
            .create_checkpoint(100, "test-node".to_string(), None)
            .await
            .unwrap();
        manager
            .create_checkpoint(200, "test-node".to_string(), None)
            .await
            .unwrap();
        manager
            .create_checkpoint(300, "test-node".to_string(), None)
            .await
            .unwrap();

        // Wait a bit for async operations
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Should only have 2 checkpoints now
        let stats = manager.get_stats().await.unwrap();
        assert_eq!(stats.total_checkpoints, 2);

        // Latest should still be available
        let latest = manager.get_latest_checkpoint().await.unwrap();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().metadata.sequence, 300);

        manager.shutdown().await.unwrap();
    }
}
