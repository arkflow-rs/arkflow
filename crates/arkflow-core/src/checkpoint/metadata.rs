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

//! Checkpoint metadata management
//!
//! This module defines metadata structures for tracking checkpoint lifecycle.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Unique identifier for a checkpoint
pub type CheckpointId = u64;

/// Status of a checkpoint
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckpointStatus {
    /// Checkpoint is in progress
    InProgress,
    /// Checkpoint completed successfully
    Completed,
    /// Checkpoint failed
    Failed,
    /// Checkpoint is being restored
    Restoring,
    /// Checkpoint has been restored
    Restored,
}

impl fmt::Display for CheckpointStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CheckpointStatus::InProgress => write!(f, "IN_PROGRESS"),
            CheckpointStatus::Completed => write!(f, "COMPLETED"),
            CheckpointStatus::Failed => write!(f, "FAILED"),
            CheckpointStatus::Restoring => write!(f, "RESTORING"),
            CheckpointStatus::Restored => write!(f, "RESTORED"),
        }
    }
}

/// Metadata for a checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    /// Unique checkpoint identifier
    pub id: CheckpointId,

    /// Current status of the checkpoint
    pub status: CheckpointStatus,

    /// Timestamp when checkpoint was created
    pub created_at: DateTime<Utc>,

    /// Timestamp when checkpoint completed (if applicable)
    pub completed_at: Option<DateTime<Utc>>,

    /// Size of checkpoint data in bytes
    pub size_bytes: u64,

    /// Checkpoint version (for schema evolution)
    pub version: u32,

    /// Optional name/description
    pub name: Option<String>,

    /// Storage location
    pub storage_path: String,

    /// Whether this checkpoint is stored in cloud storage
    pub is_cloud_stored: bool,
}

impl CheckpointMetadata {
    /// Create new checkpoint metadata
    pub fn new(id: CheckpointId, storage_path: String) -> Self {
        Self {
            id,
            status: CheckpointStatus::InProgress,
            created_at: Utc::now(),
            completed_at: None,
            size_bytes: 0,
            version: 1,
            name: None,
            storage_path,
            is_cloud_stored: false,
        }
    }

    /// Mark checkpoint as completed
    pub fn mark_completed(&mut self, size_bytes: u64) {
        self.status = CheckpointStatus::Completed;
        self.completed_at = Some(Utc::now());
        self.size_bytes = size_bytes;
    }

    /// Mark checkpoint as failed
    pub fn mark_failed(&mut self) {
        self.status = CheckpointStatus::Failed;
        self.completed_at = Some(Utc::now());
    }

    /// Check if checkpoint is completed
    pub fn is_completed(&self) -> bool {
        self.status == CheckpointStatus::Completed
    }

    /// Check if checkpoint is in progress
    pub fn is_in_progress(&self) -> bool {
        self.status == CheckpointStatus::InProgress
    }

    /// Get age of checkpoint in seconds
    pub fn age_seconds(&self) -> i64 {
        let now = Utc::now();
        (now - self.created_at).num_seconds()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_metadata_creation() {
        let meta = CheckpointMetadata::new(1, "/tmp/checkpoint-1".to_string());
        assert_eq!(meta.id, 1);
        assert_eq!(meta.status, CheckpointStatus::InProgress);
        assert_eq!(meta.storage_path, "/tmp/checkpoint-1");
        assert!(!meta.is_cloud_stored);
        assert!(meta.is_in_progress());
        assert!(!meta.is_completed());
    }

    #[test]
    fn test_checkpoint_mark_completed() {
        let mut meta = CheckpointMetadata::new(1, "/tmp/checkpoint-1".to_string());
        meta.mark_completed(1024);

        assert!(meta.is_completed());
        assert!(!meta.is_in_progress());
        assert_eq!(meta.size_bytes, 1024);
        assert!(meta.completed_at.is_some());
    }

    #[test]
    fn test_checkpoint_mark_failed() {
        let mut meta = CheckpointMetadata::new(1, "/tmp/checkpoint-1".to_string());
        meta.mark_failed();

        assert_eq!(meta.status, CheckpointStatus::Failed);
        assert!(meta.completed_at.is_some());
    }

    #[test]
    fn test_checkpoint_age() {
        let meta = CheckpointMetadata::new(1, "/tmp/checkpoint-1".to_string());
        let age = meta.age_seconds();
        assert!(age >= 0);
        assert!(age < 1); // Should be very recent
    }
}
