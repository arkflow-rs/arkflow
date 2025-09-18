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

//! Data recovery and deduplication manager for distributed WAL
//!
//! This module provides comprehensive data recovery, deduplication, and
//! consistency checking functionality for distributed WAL systems.

use crate::checkpoint_manager::{CheckpointInfo, CheckpointManager, RecoveryManifest};
use crate::distributed_wal::DistributedWAL;
use crate::node_registry::{NodeInfo, NodeRegistry};
use crate::object_storage::{create_object_storage, ObjectStorage, StorageType};
use crate::reliable_ack::AckRecord;
use crate::{distributed_wal::Checkpoint, Error};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Recovery configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RecoveryConfig {
    /// Object storage configuration
    pub storage_type: StorageType,
    /// Base path for recovery data
    pub base_path: String,
    /// Enable automatic recovery on startup
    pub auto_recovery: bool,
    /// Recovery strategy
    pub recovery_strategy: RecoveryStrategy,
    /// Maximum number of records to recover in one batch
    pub recovery_batch_size: usize,
    /// Enable consistency checking
    pub enable_consistency_check: bool,
    /// Timeout for recovery operations
    pub recovery_timeout_ms: u64,
    /// Enable deduplication
    pub enable_deduplication: bool,
    /// Maximum age for duplicate tracking
    pub duplicate_tracking_age_hours: u64,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            storage_type: StorageType::Local(crate::object_storage::LocalConfig {
                base_path: "./recovery".to_string(),
            }),
            base_path: "recovery".to_string(),
            auto_recovery: true,
            recovery_strategy: RecoveryStrategy::FromLatestCheckpoint,
            recovery_batch_size: 1000,
            enable_consistency_check: true,
            recovery_timeout_ms: 300000, // 5 minutes
            enable_deduplication: true,
            duplicate_tracking_age_hours: 24,
        }
    }
}

/// Recovery strategy
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RecoveryStrategy {
    /// Recover from the latest checkpoint
    FromLatestCheckpoint,
    /// Recover from a specific checkpoint
    FromCheckpoint(String),
    /// Recover from a specific timestamp
    FromTimestamp(SystemTime),
    /// Merge data from multiple nodes
    MergeNodes(Vec<String>),
    /// Recover all available data
    RecoverAll,
}

/// Recovery status
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RecoveryStatus {
    /// Recovery not started
    NotStarted,
    /// Recovery in progress
    InProgress {
        progress: f64,
        recovered_records: u64,
    },
    /// Recovery completed successfully
    Completed {
        recovered_records: u64,
        duplicates_removed: u64,
    },
    /// Recovery failed
    Failed { error: String },
}

/// Recovery information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RecoveryInfo {
    pub recovery_id: String,
    pub cluster_id: String,
    pub node_id: String,
    pub strategy: RecoveryStrategy,
    pub status: RecoveryStatus,
    pub started_at: SystemTime,
    pub completed_at: Option<SystemTime>,
    pub checkpoints_used: Vec<String>,
    pub nodes_consulted: Vec<String>,
    pub statistics: RecoveryStatistics,
}

/// Recovery statistics
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RecoveryStatistics {
    pub total_records_found: u64,
    pub valid_records: u64,
    pub duplicate_records: u64,
    pub corrupted_records: u64,
    pub recovered_records: u64,
    pub nodes_consulted: usize,
    pub checkpoints_consulted: usize,
    pub recovery_duration_ms: u64,
    pub bytes_processed: u64,
}

/// Deduplication manager
pub struct DeduplicationManager {
    processed_ids: Arc<RwLock<HashSet<String>>>,
    recovery_config: RecoveryConfig,
    max_age: Duration,
}

impl DeduplicationManager {
    /// Create a new deduplication manager
    pub fn new(recovery_config: RecoveryConfig) -> Self {
        let max_age = Duration::from_secs(recovery_config.duplicate_tracking_age_hours * 3600);

        Self {
            processed_ids: Arc::new(RwLock::new(HashSet::new())),
            recovery_config,
            max_age,
        }
    }

    /// Check if a record is a duplicate
    pub async fn is_duplicate(&self, record: &AckRecord) -> bool {
        if !self.recovery_config.enable_deduplication {
            return false;
        }

        let record_id = self.generate_record_id(record);
        let processed_ids = self.processed_ids.read().await;
        processed_ids.contains(&record_id)
    }

    /// Mark a record as processed
    pub async fn mark_processed(&self, record: &AckRecord) {
        if !self.recovery_config.enable_deduplication {
            return;
        }

        let record_id = self.generate_record_id(record);
        let mut processed_ids = self.processed_ids.write().await;
        processed_ids.insert(record_id);

        // Cleanup old entries if needed
        if processed_ids.len() > 10000 {
            self.cleanup_old_entries().await;
        }
    }

    /// Generate unique record ID for deduplication
    fn generate_record_id(&self, record: &AckRecord) -> String {
        use md5::{Digest, Md5};

        let mut hasher = Md5::new();
        hasher.update(record.ack_type.as_bytes());
        hasher.update(record.sequence.to_le_bytes());
        hasher.update(
            record
                .created_at
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .to_le_bytes(),
        );
        hasher.update(&record.payload);

        format!("{:x}", hasher.finalize())
    }

    /// Cleanup old entries from the processed set
    async fn cleanup_old_entries(&self) {
        let mut processed_ids = self.processed_ids.write().await;

        // Since we don't have timestamps in the HashSet, we'll just limit the size
        if processed_ids.len() > 50000 {
            // Remove oldest entries (simple approach)
            let mut entries: Vec<_> = processed_ids.iter().cloned().collect();
            entries.sort(); // Sort by hash value (roughly chronological)
            entries.truncate(25000);

            *processed_ids = entries.into_iter().collect();
        }
    }

    /// Get statistics
    pub async fn get_stats(&self) -> DeduplicationStats {
        let processed_ids = self.processed_ids.read().await;
        DeduplicationStats {
            total_processed: processed_ids.len(),
            tracking_enabled: self.recovery_config.enable_deduplication,
            max_age_hours: self.recovery_config.duplicate_tracking_age_hours,
        }
    }

    /// Clear all entries
    pub async fn clear(&self) {
        let mut processed_ids = self.processed_ids.write().await;
        processed_ids.clear();
    }
}

/// Deduplication statistics
#[derive(Debug, Clone)]
pub struct DeduplicationStats {
    pub total_processed: usize,
    pub tracking_enabled: bool,
    pub max_age_hours: u64,
}

/// Recovery manager
pub struct RecoveryManager {
    cluster_id: String,
    node_id: String,
    object_storage: Arc<dyn ObjectStorage>,
    checkpoint_manager: Arc<CheckpointManager>,
    node_registry: Arc<dyn NodeRegistry>,
    deduplication_manager: Arc<DeduplicationManager>,
    recovery_config: RecoveryConfig,
    recovery_history: Arc<RwLock<Vec<RecoveryInfo>>>,
}

impl RecoveryManager {
    /// Create a new recovery manager
    pub async fn new(
        cluster_id: String,
        node_id: String,
        checkpoint_manager: Arc<CheckpointManager>,
        node_registry: Arc<dyn NodeRegistry>,
        recovery_config: RecoveryConfig,
    ) -> Result<Self, Error> {
        let object_storage = create_object_storage(&recovery_config.storage_type).await?;
        let deduplication_manager = Arc::new(DeduplicationManager::new(recovery_config.clone()));

        let manager = Self {
            cluster_id: cluster_id.clone(),
            node_id: node_id.clone(),
            object_storage,
            checkpoint_manager,
            node_registry,
            deduplication_manager,
            recovery_config: recovery_config.clone(),
            recovery_history: Arc::new(RwLock::new(Vec::new())),
        };

        // Perform auto recovery if enabled
        if recovery_config.auto_recovery {
            info!("Starting auto recovery for node: {}", node_id);
            if let Err(e) = manager.perform_auto_recovery().await {
                error!("Auto recovery failed: {}", e);
            }
        }

        Ok(manager)
    }

    /// Perform automatic recovery
    async fn perform_auto_recovery(&self) -> Result<RecoveryInfo, Error> {
        let strategy = match self.recovery_config.recovery_strategy {
            RecoveryStrategy::FromLatestCheckpoint => {
                // Get latest checkpoint
                match self.checkpoint_manager.get_recovery_point().await? {
                    Some(checkpoint) => RecoveryStrategy::FromCheckpoint(checkpoint.checkpoint_id),
                    None => RecoveryStrategy::RecoverAll,
                }
            }
            _ => self.recovery_config.recovery_strategy.clone(),
        };

        self.perform_recovery(strategy).await
    }

    /// Perform recovery with specified strategy
    pub async fn perform_recovery(
        &self,
        strategy: RecoveryStrategy,
    ) -> Result<RecoveryInfo, Error> {
        let recovery_id = format!("recovery_{}_{}", self.node_id, uuid::Uuid::new_v4());
        let start_time = SystemTime::now();

        info!(
            "Starting recovery {} with strategy: {:?}",
            recovery_id, strategy
        );

        let mut recovery_info = RecoveryInfo {
            recovery_id: recovery_id.clone(),
            cluster_id: self.cluster_id.clone(),
            node_id: self.node_id.clone(),
            strategy: strategy.clone(),
            status: RecoveryStatus::InProgress {
                progress: 0.0,
                recovered_records: 0,
            },
            started_at: start_time,
            completed_at: None,
            checkpoints_used: Vec::new(),
            nodes_consulted: Vec::new(),
            statistics: RecoveryStatistics {
                total_records_found: 0,
                valid_records: 0,
                duplicate_records: 0,
                corrupted_records: 0,
                recovered_records: 0,
                nodes_consulted: 0,
                checkpoints_consulted: 0,
                recovery_duration_ms: 0,
                bytes_processed: 0,
            },
        };

        // Update status in history
        {
            let mut history = self.recovery_history.write().await;
            history.push(recovery_info.clone());
        }

        let result = match strategy {
            RecoveryStrategy::FromLatestCheckpoint => {
                self.recover_from_latest_checkpoint(&mut recovery_info)
                    .await
            }
            RecoveryStrategy::FromCheckpoint(checkpoint_id) => {
                self.recover_from_checkpoint(&mut recovery_info, &checkpoint_id)
                    .await
            }
            RecoveryStrategy::FromTimestamp(timestamp) => {
                self.recover_from_timestamp(&mut recovery_info, timestamp)
                    .await
            }
            RecoveryStrategy::MergeNodes(node_ids) => {
                self.recover_from_multiple_nodes(&mut recovery_info, &node_ids)
                    .await
            }
            RecoveryStrategy::RecoverAll => {
                self.recover_all_available_data(&mut recovery_info).await
            }
        };

        let completion_time = SystemTime::now();
        recovery_info.completed_at = Some(completion_time);
        recovery_info.statistics.recovery_duration_ms = completion_time
            .duration_since(start_time)
            .unwrap()
            .as_millis() as u64;

        match result {
            Ok(recovered_records) => {
                recovery_info.status = RecoveryStatus::Completed {
                    recovered_records,
                    duplicates_removed: recovery_info.statistics.duplicate_records,
                };
                info!("Recovery {} completed successfully", recovery_id);
            }
            Err(e) => {
                recovery_info.status = RecoveryStatus::Failed {
                    error: e.to_string(),
                };
                error!("Recovery {} failed: {}", recovery_id, e);
                return Err(e);
            }
        }

        // Update history with final status
        {
            let mut history = self.recovery_history.write().await;
            if let Some(last) = history.last_mut() {
                *last = recovery_info.clone();
            }
        }

        Ok(recovery_info)
    }

    /// Recover from the latest checkpoint
    async fn recover_from_latest_checkpoint(
        &self,
        recovery_info: &mut RecoveryInfo,
    ) -> Result<u64, Error> {
        let recovery_point = self.checkpoint_manager.get_recovery_point().await?;

        match recovery_point {
            Some(checkpoint) => {
                self.recover_from_checkpoint(recovery_info, &checkpoint.checkpoint_id)
                    .await
            }
            None => {
                info!("No checkpoints found, recovering all available data");
                self.recover_all_available_data(recovery_info).await
            }
        }
    }

    /// Recover from a specific checkpoint
    async fn recover_from_checkpoint(
        &self,
        recovery_info: &mut RecoveryInfo,
        checkpoint_id: &str,
    ) -> Result<u64, Error> {
        info!("Recovering from checkpoint: {}", checkpoint_id);

        let checkpoint_info = self
            .checkpoint_manager
            .restore_from_checkpoint(checkpoint_id)
            .await?;
        recovery_info
            .checkpoints_used
            .push(checkpoint_id.to_string());

        // Get records from all nodes after the checkpoint sequence
        let active_nodes = self.node_registry.get_active_nodes().await?;
        recovery_info.nodes_consulted = active_nodes.iter().map(|n| n.node_id.clone()).collect();
        recovery_info.statistics.nodes_consulted = active_nodes.len();

        let mut all_records = Vec::new();
        let base_path = self.recovery_config.base_path.clone();

        for node in active_nodes {
            match self
                .recover_records_from_node(
                    &node.node_id,
                    &base_path,
                    checkpoint_info.metadata.sequence,
                )
                .await
            {
                Ok(mut records) => {
                    all_records.append(&mut records);
                }
                Err(e) => {
                    warn!(
                        "Failed to recover records from node {}: {}",
                        node.node_id, e
                    );
                }
            }
        }

        recovery_info.statistics.total_records_found = all_records.len() as u64;

        // Process and deduplicate records
        let processed_records = self
            .process_recovered_records(recovery_info, all_records)
            .await?;

        Ok(processed_records.len() as u64)
    }

    /// Recover from a specific timestamp
    async fn recover_from_timestamp(
        &self,
        recovery_info: &mut RecoveryInfo,
        timestamp: SystemTime,
    ) -> Result<u64, Error> {
        info!("Recovering from timestamp: {:?}", timestamp);

        // Find checkpoints after the timestamp
        let checkpoints_after = self
            .checkpoint_manager
            .get_checkpoints_after_sequence(
                timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs() as u64
            )
            .await?;

        if let Some(checkpoint) = checkpoints_after.first() {
            // Use the earliest checkpoint after the timestamp
            self.recover_from_checkpoint(recovery_info, &checkpoint.checkpoint_id)
                .await
        } else {
            // No checkpoints after timestamp, recover all data
            self.recover_all_available_data(recovery_info).await
        }
    }

    /// Recover from multiple nodes
    async fn recover_from_multiple_nodes(
        &self,
        recovery_info: &mut RecoveryInfo,
        node_ids: &[String],
    ) -> Result<u64, Error> {
        info!("Recovering from multiple nodes: {:?}", node_ids);

        let mut all_records = Vec::new();
        let base_path = self.recovery_config.base_path.clone();

        for node_id in node_ids {
            recovery_info.nodes_consulted.push(node_id.clone());

            match self
                .recover_all_records_from_node(node_id, &base_path)
                .await
            {
                Ok(mut records) => {
                    all_records.append(&mut records);
                }
                Err(e) => {
                    warn!("Failed to recover records from node {}: {}", node_id, e);
                }
            }
        }

        recovery_info.statistics.total_records_found = all_records.len() as u64;
        recovery_info.statistics.nodes_consulted = node_ids.len();

        // Process and deduplicate records
        let processed_records = self
            .process_recovered_records(recovery_info, all_records)
            .await?;

        Ok(processed_records.len() as u64)
    }

    /// Recover all available data
    async fn recover_all_available_data(
        &self,
        recovery_info: &mut RecoveryInfo,
    ) -> Result<u64, Error> {
        info!("Recovering all available data");

        let active_nodes = self.node_registry.get_active_nodes().await?;
        recovery_info.nodes_consulted = active_nodes.iter().map(|n| n.node_id.clone()).collect();
        recovery_info.statistics.nodes_consulted = active_nodes.len();

        let mut all_records = Vec::new();
        let base_path = self.recovery_config.base_path.clone();

        for node in active_nodes {
            match self
                .recover_all_records_from_node(&node.node_id, &base_path)
                .await
            {
                Ok(mut records) => {
                    all_records.append(&mut records);
                }
                Err(e) => {
                    warn!(
                        "Failed to recover records from node {}: {}",
                        node.node_id, e
                    );
                }
            }
        }

        recovery_info.statistics.total_records_found = all_records.len() as u64;

        // Process and deduplicate records
        let processed_records = self
            .process_recovered_records(recovery_info, all_records)
            .await?;

        Ok(processed_records.len() as u64)
    }

    /// Recover records from a specific node after a sequence
    async fn recover_records_from_node(
        &self,
        node_id: &str,
        base_path: &str,
        after_sequence: u64,
    ) -> Result<Vec<AckRecord>, Error> {
        let node_prefix = format!("{}/nodes/{}/", base_path, node_id);
        let mut records = Vec::new();

        match self.object_storage.list_objects(&node_prefix).await {
            Ok(objects) => {
                for object in objects {
                    if object.key.ends_with(".json") {
                        match self.object_storage.get_object(&object.key).await {
                            Ok(data) => {
                                match serde_json::from_slice::<Vec<AckRecord>>(&data) {
                                    Ok(mut node_records) => {
                                        // Filter records after the specified sequence
                                        node_records.retain(|r| r.sequence > after_sequence);
                                        records.extend(node_records);
                                    }
                                    Err(e) => {
                                        error!(
                                            "Failed to deserialize records from {}: {}",
                                            object.key, e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to download object {}: {}", object.key, e);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to list objects for node {}: {}", node_id, e);
            }
        }

        // Sort by sequence
        records.sort_by(|a, b| a.sequence.cmp(&b.sequence));

        Ok(records)
    }

    /// Recover all records from a specific node
    async fn recover_all_records_from_node(
        &self,
        node_id: &str,
        base_path: &str,
    ) -> Result<Vec<AckRecord>, Error> {
        self.recover_records_from_node(node_id, base_path, 0).await
    }

    /// Process recovered records with deduplication and validation
    async fn process_recovered_records(
        &self,
        recovery_info: &mut RecoveryInfo,
        records: Vec<AckRecord>,
    ) -> Result<Vec<AckRecord>, Error> {
        let mut processed_records = Vec::new();
        let mut bytes_processed = 0u64;

        for record in records {
            bytes_processed += record.payload.len() as u64;

            // Skip if duplicate
            if self.deduplication_manager.is_duplicate(&record).await {
                recovery_info.statistics.duplicate_records += 1;
                continue;
            }

            // Validate record
            if self.validate_record(&record).await {
                processed_records.push(record.clone());
                recovery_info.statistics.valid_records += 1;
                self.deduplication_manager.mark_processed(&record).await;
            } else {
                recovery_info.statistics.corrupted_records += 1;
            }
        }

        recovery_info.statistics.bytes_processed = bytes_processed;
        recovery_info.statistics.recovered_records = processed_records.len() as u64;

        debug!(
            "Processed records: {} valid, {} duplicates, {} corrupted",
            recovery_info.statistics.valid_records,
            recovery_info.statistics.duplicate_records,
            recovery_info.statistics.corrupted_records
        );

        Ok(processed_records)
    }

    /// Validate a record
    async fn validate_record(&self, record: &AckRecord) -> bool {
        // Basic validation checks
        if record.sequence == 0 {
            return false;
        }

        if record.ack_type.is_empty() {
            return false;
        }

        // Check timestamp is reasonable (not too far in the future)
        let now = SystemTime::now();
        if let Ok(duration) = record.created_at.duration_since(now) {
            if duration.as_secs() > 86400 {
                // More than 1 day in the future
                return false;
            }
        }

        // Validate payload size (prevent memory issues)
        if record.payload.len() > 10 * 1024 * 1024 {
            // 10MB limit
            return false;
        }

        true
    }

    /// Perform consistency check across nodes
    pub async fn perform_consistency_check(&self) -> Result<ConsistencyReport, Error> {
        if !self.recovery_config.enable_consistency_check {
            return Ok(ConsistencyReport {
                cluster_id: self.cluster_id.clone(),
                is_consistent: true,
                discrepancies: Vec::new(),
                checked_nodes: 0,
                checked_records: 0,
                check_duration_ms: 0,
            });
        }

        let start_time = SystemTime::now();
        let active_nodes = self.node_registry.get_active_nodes().await?;
        let mut discrepancies = Vec::new();
        let mut total_records = 0;

        info!(
            "Performing consistency check across {} nodes",
            active_nodes.len()
        );

        // Get sequence numbers from each node
        let mut node_sequences = HashMap::new();
        for node in &active_nodes {
            match self.get_latest_sequence_from_node(&node.node_id).await {
                Ok(sequence) => {
                    node_sequences.insert(node.node_id.clone(), sequence);
                    total_records += sequence;
                }
                Err(e) => {
                    discrepancies.push(ConsistencyDiscrepancy {
                        node_id: node.node_id.clone(),
                        discrepancy_type: "NodeUnavailable".to_string(),
                        description: format!("Failed to get sequence: {}", e),
                        severity: DiscrepancySeverity::Warning,
                    });
                }
            }
        }

        // Check for sequence number consistency
        if let Some(&max_sequence) = node_sequences.values().max() {
            for (node_id, sequence) in node_sequences {
                if sequence < max_sequence {
                    let gap = max_sequence - sequence;
                    if gap > 10 {
                        // Only report significant gaps
                        discrepancies.push(ConsistencyDiscrepancy {
                            node_id: node_id.clone(),
                            discrepancy_type: "SequenceGap".to_string(),
                            description: format!(
                                "Node {} is {} records behind the leader",
                                node_id, gap
                            ),
                            severity: if gap > 100 {
                                DiscrepancySeverity::Error
                            } else {
                                DiscrepancySeverity::Warning
                            },
                        });
                    }
                }
            }
        }

        let duration = start_time.elapsed().unwrap().as_millis() as u64;
        let is_consistent = discrepancies.is_empty();

        if !is_consistent {
            warn!(
                "Consistency check found {} discrepancies",
                discrepancies.len()
            );
        } else {
            info!("Consistency check passed - all nodes are synchronized");
        }

        Ok(ConsistencyReport {
            cluster_id: self.cluster_id.clone(),
            is_consistent,
            discrepancies,
            checked_nodes: active_nodes.len(),
            checked_records: total_records,
            check_duration_ms: duration,
        })
    }

    /// Get latest sequence from a node
    async fn get_latest_sequence_from_node(&self, node_id: &str) -> Result<u64, Error> {
        let node_prefix = format!("{}/nodes/{}/", self.recovery_config.base_path, node_id);
        let mut latest_sequence = 0u64;

        match self.object_storage.list_objects(&node_prefix).await {
            Ok(objects) => {
                for object in objects {
                    if object.key.ends_with(".json") {
                        match self.object_storage.get_object(&object.key).await {
                            Ok(data) => match serde_json::from_slice::<Vec<AckRecord>>(&data) {
                                Ok(records) => {
                                    if let Some(record) = records.last() {
                                        latest_sequence = latest_sequence.max(record.sequence);
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to deserialize records from {}: {}",
                                        object.key, e
                                    );
                                }
                            },
                            Err(e) => {
                                error!("Failed to download object {}: {}", object.key, e);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                return Err(Error::Unknown(format!(
                    "Failed to list objects for node {}: {}",
                    node_id, e
                )));
            }
        }

        Ok(latest_sequence)
    }

    /// Get recovery history
    pub async fn get_recovery_history(&self) -> Vec<RecoveryInfo> {
        let history = self.recovery_history.read().await;
        history.clone()
    }

    /// Get deduplication statistics
    pub async fn get_deduplication_stats(&self) -> DeduplicationStats {
        self.deduplication_manager.get_stats().await
    }

    /// Clear recovery history
    pub async fn clear_history(&self) {
        let mut history = self.recovery_history.write().await;
        history.clear();
    }
}

/// Consistency check report
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConsistencyReport {
    pub cluster_id: String,
    pub is_consistent: bool,
    pub discrepancies: Vec<ConsistencyDiscrepancy>,
    pub checked_nodes: usize,
    pub checked_records: u64,
    pub check_duration_ms: u64,
}

/// Consistency discrepancy information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConsistencyDiscrepancy {
    pub node_id: String,
    pub discrepancy_type: String,
    pub description: String,
    pub severity: DiscrepancySeverity,
}

/// Discrepancy severity level
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum DiscrepancySeverity {
    Info,
    Warning,
    Error,
    Critical,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_recovery_manager_creation() {
        let temp_dir = TempDir::new().unwrap();

        let checkpoint_config = crate::checkpoint_manager::CheckpointConfig {
            storage_type: StorageType::Local(crate::object_storage::LocalConfig {
                base_path: temp_dir
                    .path()
                    .join("checkpoints")
                    .to_string_lossy()
                    .to_string(),
            }),
            base_path: "checkpoints".to_string(),
            auto_checkpoint: false,
            ..Default::default()
        };

        let node_registry = Arc::new(
            crate::node_registry::InMemoryNodeRegistry::new("test-cluster".to_string())
                .await
                .unwrap(),
        );
        let checkpoint_manager = Arc::new(
            crate::checkpoint_manager::CheckpointManager::new(
                "test-cluster".to_string(),
                checkpoint_config,
            )
            .await
            .unwrap(),
        );

        let recovery_config = RecoveryConfig {
            storage_type: StorageType::Local(crate::object_storage::LocalConfig {
                base_path: temp_dir.path().to_string_lossy().to_string(),
            }),
            auto_recovery: false,
            ..Default::default()
        };

        let manager = RecoveryManager::new(
            "test-cluster".to_string(),
            "test-node".to_string(),
            checkpoint_manager,
            node_registry,
            recovery_config,
        )
        .await
        .unwrap();

        assert_eq!(manager.cluster_id, "test-cluster");
        assert_eq!(manager.node_id, "test-node");

        let dedup_stats = manager.get_deduplication_stats().await;
        assert_eq!(dedup_stats.total_processed, 0);
    }

    #[tokio::test]
    async fn test_deduplication_manager() {
        let config = RecoveryConfig::default();
        let dedup_manager = DeduplicationManager::new(config);

        let record = AckRecord {
            sequence: 1,
            ack_type: "test".to_string(),
            payload: b"test".to_vec(),
            retry_count: 0,
            created_at: SystemTime::now(),
            last_retry: None,
        };

        // First check - should not be duplicate
        assert!(!dedup_manager.is_duplicate(&record).await);

        // Mark as processed
        dedup_manager.mark_processed(&record).await;

        // Second check - should be duplicate
        assert!(dedup_manager.is_duplicate(&record).await);

        let stats = dedup_manager.get_stats().await;
        assert_eq!(stats.total_processed, 1);
    }
}
