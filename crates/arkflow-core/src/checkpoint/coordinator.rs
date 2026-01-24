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

//! Checkpoint coordination
//!
//! This module implements the checkpoint coordinator that manages periodic checkpoints,
//! coordinates barrier injection, and handles checkpoint lifecycle.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::{interval, Instant};
use tracing::{debug, error, info, warn};

use super::{
    barrier::BarrierManager, metadata::CheckpointMetadata, state::StateSnapshot, CheckpointId,
    CheckpointResult, CheckpointStorage, LocalFileStorage,
};
use crate::Error;

/// Checkpoint configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointConfig {
    /// Whether checkpointing is enabled
    #[serde(default = "default_checkpoint_enabled")]
    pub enabled: bool,

    /// Checkpoint interval
    #[serde(default = "default_checkpoint_interval")]
    #[serde(with = "humantime_serde")]
    pub interval: Duration,

    /// Maximum number of checkpoints to retain
    #[serde(default = "default_max_checkpoints")]
    pub max_checkpoints: usize,

    /// Minimum age before checkpoint can be deleted
    #[serde(default = "default_min_age")]
    #[serde(with = "humantime_serde")]
    pub min_age: Duration,

    /// Local storage path
    #[serde(default = "default_local_path")]
    pub local_path: String,

    /// Barrier alignment timeout
    #[serde(default = "default_alignment_timeout")]
    #[serde(with = "humantime_serde")]
    pub alignment_timeout: Duration,
}

fn default_checkpoint_enabled() -> bool {
    false
}

fn default_checkpoint_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_max_checkpoints() -> usize {
    10
}

fn default_min_age() -> Duration {
    Duration::from_secs(3600) // 1 hour
}

fn default_local_path() -> String {
    "/var/lib/arkflow/checkpoints".to_string()
}

fn default_alignment_timeout() -> Duration {
    Duration::from_secs(30)
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            enabled: default_checkpoint_enabled(),
            interval: default_checkpoint_interval(),
            max_checkpoints: default_max_checkpoints(),
            min_age: default_min_age(),
            local_path: default_local_path(),
            alignment_timeout: default_alignment_timeout(),
        }
    }
}

/// Checkpoint coordinator that manages periodic checkpoints
pub struct CheckpointCoordinator {
    /// Checkpoint configuration
    config: CheckpointConfig,

    /// Storage backend
    storage: Arc<dyn CheckpointStorage>,

    /// Barrier manager
    barrier_manager: Arc<BarrierManager>,

    /// Next checkpoint ID
    next_checkpoint_id: Arc<RwLock<CheckpointId>>,

    /// Current checkpoint state (if in progress)
    current_checkpoint: Arc<RwLock<Option<CheckpointState>>>,

    /// Whether checkpointing is enabled
    enabled: Arc<RwLock<bool>>,

    /// Checkpoint statistics
    stats: Arc<RwLock<CheckpointStats>>,
}

/// State of an in-progress checkpoint
#[derive(Debug)]
struct CheckpointState {
    /// Checkpoint ID
    id: CheckpointId,

    /// Barrier ID
    barrier_id: super::barrier::BarrierId,

    /// When checkpoint started
    started_at: Instant,

    /// Snapshot data (accumulated from components)
    snapshot: StateSnapshot,
}

/// Checkpoint statistics
#[derive(Debug, Default)]
struct CheckpointStats {
    /// Total checkpoints taken
    total_checkpoints: u64,

    /// Successful checkpoints
    successful_checkpoints: u64,

    /// Failed checkpoints
    failed_checkpoints: u64,

    /// Last checkpoint time
    last_checkpoint_time: Option<Instant>,

    /// Last checkpoint duration
    last_checkpoint_duration: Option<Duration>,
}

impl CheckpointCoordinator {
    /// Create a new checkpoint coordinator
    pub fn new(config: CheckpointConfig) -> CheckpointResult<Self> {
        // Create storage backend
        let storage = Arc::new(LocalFileStorage::new(&config.local_path)?);

        // Create barrier manager
        let barrier_manager = Arc::new(BarrierManager::new(config.alignment_timeout));

        Ok(Self {
            config,
            storage,
            barrier_manager,
            next_checkpoint_id: Arc::new(RwLock::new(1)),
            current_checkpoint: Arc::new(RwLock::new(None)),
            enabled: Arc::new(RwLock::new(true)),
            stats: Arc::new(RwLock::new(CheckpointStats::default())),
        })
    }

    /// Start the checkpoint coordinator background task
    pub async fn run(&self) -> CheckpointResult<()> {
        info!(
            "Starting checkpoint coordinator with interval {:?}",
            self.config.interval
        );

        let mut timer = interval(self.config.interval);
        timer.tick().await; // Skip first immediate tick

        loop {
            timer.tick().await;

            // Check if enabled
            if !self.is_enabled().await {
                debug!("Checkpointing disabled, skipping");
                continue;
            }

            // Check if another checkpoint is in progress
            if self.is_checkpoint_in_progress().await {
                warn!("Previous checkpoint still in progress, skipping");
                continue;
            }

            // Trigger checkpoint
            if let Err(e) = self.trigger_checkpoint().await {
                error!("Failed to trigger checkpoint: {}", e);

                let mut stats = self.stats.write().await;
                stats.failed_checkpoints += 1;
            }
        }
    }

    /// Trigger a checkpoint
    pub async fn trigger_checkpoint(&self) -> CheckpointResult<CheckpointMetadata> {
        let checkpoint_id = self.next_checkpoint_id().await;
        info!("Triggering checkpoint {}", checkpoint_id);

        let start_time = Instant::now();

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.total_checkpoints += 1;
        }

        // 1. Inject barrier
        let expected_acks = 1; // TODO: Calculate based on processor workers
        let barrier = self
            .barrier_manager
            .inject_barrier(checkpoint_id, expected_acks)
            .await;

        // 2. Create checkpoint state
        let checkpoint_state = CheckpointState {
            id: checkpoint_id,
            barrier_id: barrier.id,
            started_at: start_time,
            snapshot: StateSnapshot::new(),
        };

        *self.current_checkpoint.write().await = Some(checkpoint_state);

        // 3. For now, immediately acknowledge barrier (since no processor workers yet)
        // TODO: Remove this when processor workers are integrated
        let _ = self.barrier_manager.acknowledge_barrier(barrier.id).await;

        // 4. Wait for barrier alignment
        match self.barrier_manager.wait_for_barrier(barrier.id).await {
            Ok(_) => {
                debug!(
                    "Barrier {} aligned for checkpoint {}",
                    barrier.id, checkpoint_id
                );

                // 5. Capture state
                let snapshot = self.capture_state().await?;

                // 6. Save checkpoint
                let metadata = self
                    .storage
                    .save_checkpoint(checkpoint_id, &snapshot)
                    .await?;

                // 6. Cleanup
                self.cleanup_after_checkpoint(checkpoint_id, barrier.id)
                    .await;

                // Update stats
                let duration = start_time.elapsed();
                {
                    let mut stats = self.stats.write().await;
                    stats.successful_checkpoints += 1;
                    stats.last_checkpoint_time = Some(start_time);
                    stats.last_checkpoint_duration = Some(duration);
                }

                info!(
                    "Checkpoint {} completed in {:?} ({} bytes)",
                    checkpoint_id, duration, metadata.size_bytes
                );

                // 7. Clean up old checkpoints
                self.cleanup_old_checkpoints().await;

                Ok(metadata)
            }
            Err(e) => {
                error!("Checkpoint {} failed: {}", checkpoint_id, e);

                // Cleanup
                self.cleanup_after_checkpoint(checkpoint_id, barrier.id)
                    .await;

                let mut stats = self.stats.write().await;
                stats.failed_checkpoints += 1;

                Err(e)
            }
        }
    }

    /// Capture current state from all components
    async fn capture_state(&self) -> CheckpointResult<StateSnapshot> {
        let mut snapshot = StateSnapshot::new();

        // Get current checkpoint state
        let checkpoint_state = self.current_checkpoint.read().await;
        if let Some(ref state) = *checkpoint_state {
            snapshot = state.snapshot.clone();
        }

        // TODO: Capture state from input, buffer, processors
        // For now, return empty snapshot

        Ok(snapshot)
    }

    /// Cleanup after checkpoint completion/failure
    async fn cleanup_after_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
        barrier_id: super::barrier::BarrierId,
    ) {
        // Clear current checkpoint
        *self.current_checkpoint.write().await = None;

        // Remove barrier
        self.barrier_manager.remove_barrier(barrier_id).await;

        debug!("Cleanup completed for checkpoint {}", checkpoint_id);
    }

    /// Clean up old checkpoints exceeding retention policy
    async fn cleanup_old_checkpoints(&self) {
        let checkpoints = match self.storage.list_checkpoints().await {
            Ok(cps) => cps,
            Err(e) => {
                error!("Failed to list checkpoints for cleanup: {}", e);
                return;
            }
        };

        if checkpoints.len() <= self.config.max_checkpoints {
            return;
        }

        // Remove oldest checkpoints exceeding max_checkpoints
        let to_remove = checkpoints.len() - self.config.max_checkpoints;

        for (i, metadata) in checkpoints.iter().rev().enumerate() {
            if i >= to_remove {
                break;
            }

            // Check minimum age
            let age_seconds = metadata.age_seconds();
            let min_age_seconds = self.config.min_age.as_secs() as i64;

            if age_seconds >= min_age_seconds {
                info!(
                    "Removing old checkpoint {} (age: {}s)",
                    metadata.id, age_seconds
                );

                if let Err(e) = self.storage.delete_checkpoint(metadata.id).await {
                    warn!("Failed to delete checkpoint {}: {}", metadata.id, e);
                }
            } else {
                debug!(
                    "Keeping checkpoint {} (age: {}s < min_age: {}s)",
                    metadata.id, age_seconds, min_age_seconds
                );
            }
        }
    }

    /// Restore from latest checkpoint
    pub async fn restore_from_checkpoint(&self) -> CheckpointResult<Option<StateSnapshot>> {
        info!("Attempting to restore from latest checkpoint");

        let latest_id = match self.storage.get_latest_checkpoint().await? {
            Some(id) => id,
            None => {
                info!("No checkpoints found, starting fresh");
                return Ok(None);
            }
        };

        info!("Loading checkpoint {}", latest_id);

        let snapshot = self
            .storage
            .load_checkpoint(latest_id)
            .await?
            .ok_or_else(|| Error::Process(format!("Checkpoint {} not found", latest_id)))?;

        info!("Successfully restored from checkpoint {}", latest_id);

        Ok(Some(snapshot))
    }

    /// Get next checkpoint ID
    async fn next_checkpoint_id(&self) -> CheckpointId {
        let mut id = self.next_checkpoint_id.write().await;
        let current = *id;
        *id += 1;
        current
    }

    /// Check if checkpoint is in progress
    async fn is_checkpoint_in_progress(&self) -> bool {
        self.current_checkpoint.read().await.is_some()
    }

    /// Check if checkpointing is enabled
    async fn is_enabled(&self) -> bool {
        *self.enabled.read().await
    }

    /// Enable checkpointing
    pub async fn enable(&self) {
        *self.enabled.write().await = true;
        info!("Checkpointing enabled");
    }

    /// Disable checkpointing
    pub async fn disable(&self) {
        *self.enabled.write().await = false;
        info!("Checkpointing disabled");
    }

    /// Get checkpoint statistics
    pub async fn get_stats(&self) -> CheckpointStatistics {
        let stats = self.stats.read().await;

        CheckpointStatistics {
            total_checkpoints: stats.total_checkpoints,
            successful_checkpoints: stats.successful_checkpoints,
            failed_checkpoints: stats.failed_checkpoints,
            last_checkpoint_time: stats.last_checkpoint_time,
            last_checkpoint_duration: stats.last_checkpoint_duration,
        }
    }

    /// Get barrier manager reference (for integration with stream)
    pub fn barrier_manager(&self) -> Arc<BarrierManager> {
        Arc::clone(&self.barrier_manager)
    }
}

/// Checkpoint statistics
#[derive(Debug, Clone)]
pub struct CheckpointStatistics {
    pub total_checkpoints: u64,
    pub successful_checkpoints: u64,
    pub failed_checkpoints: u64,
    pub last_checkpoint_time: Option<Instant>,
    pub last_checkpoint_duration: Option<Duration>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_coordinator_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = CheckpointConfig {
            local_path: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };

        let coordinator = CheckpointCoordinator::new(config);
        assert!(coordinator.is_ok());

        let coordinator = coordinator.unwrap();
        assert!(coordinator.is_enabled().await);
        assert!(!coordinator.is_checkpoint_in_progress().await);
    }

    #[tokio::test]
    async fn test_checkpoint_enable_disable() {
        let temp_dir = TempDir::new().unwrap();
        let config = CheckpointConfig {
            local_path: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };

        let coordinator = CheckpointCoordinator::new(config).unwrap();

        assert!(coordinator.is_enabled().await);

        coordinator.disable().await;
        assert!(!coordinator.is_enabled().await);

        coordinator.enable().await;
        assert!(coordinator.is_enabled().await);
    }

    #[tokio::test]
    async fn test_checkpoint_trigger() {
        let temp_dir = TempDir::new().unwrap();
        let config = CheckpointConfig {
            local_path: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };

        let coordinator = CheckpointCoordinator::new(config).unwrap();

        // Trigger checkpoint
        let result = coordinator.trigger_checkpoint().await;

        // Should succeed even without component state
        assert!(result.is_ok());

        let metadata = result.unwrap();
        assert_eq!(metadata.id, 1);
        assert!(metadata.is_completed());
    }

    #[tokio::test]
    async fn test_checkpoint_restore() {
        let temp_dir = TempDir::new().unwrap();
        let config = CheckpointConfig {
            local_path: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };

        let coordinator = CheckpointCoordinator::new(config).unwrap();

        // Try to restore when no checkpoints exist
        let result = coordinator.restore_from_checkpoint().await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Create a checkpoint
        coordinator.trigger_checkpoint().await.unwrap();

        // Now restore should succeed
        let result = coordinator.restore_from_checkpoint().await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_checkpoint_stats() {
        let temp_dir = TempDir::new().unwrap();
        let config = CheckpointConfig {
            local_path: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };

        let coordinator = CheckpointCoordinator::new(config).unwrap();

        let stats = coordinator.get_stats().await;
        assert_eq!(stats.total_checkpoints, 0);
        assert_eq!(stats.successful_checkpoints, 0);

        // Trigger a checkpoint
        coordinator.trigger_checkpoint().await.unwrap();

        let stats = coordinator.get_stats().await;
        assert_eq!(stats.total_checkpoints, 1);
        assert_eq!(stats.successful_checkpoints, 1);
        assert!(stats.last_checkpoint_time.is_some());
        assert!(stats.last_checkpoint_duration.is_some());
    }
}
