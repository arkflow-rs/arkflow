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

//! Checkpoint event types for tracking progress
//!
//! This module defines the types of checkpoint events that occur during
//! the checkpoint lifecycle, inspired by Arroyo's implementation.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

/// Checkpoint event type representing different stages in the checkpoint lifecycle
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckpointEventType {
    /// Barrier alignment started - processor is waiting for all inputs to reach barrier
    StartedAlignment,
    /// Checkpointing started - processor is taking snapshot of local state
    StartedCheckpointing,
    /// Operator setup finished - operator-specific checkpoint preparation complete
    FinishedOperatorSetup,
    /// Sync phase finished - state has been persisted to durable storage
    FinishedSync,
    /// Pre-commit phase finished - transaction is ready to commit
    FinishedPreCommit,
    /// Commit finished - transaction has been committed
    FinishedCommit,
}

impl CheckpointEventType {
    /// Get the display name for the event type
    pub fn as_str(&self) -> &'static str {
        match self {
            CheckpointEventType::StartedAlignment => "alignment_started",
            CheckpointEventType::StartedCheckpointing => "checkpoint_started",
            CheckpointEventType::FinishedOperatorSetup => "operator_finished",
            CheckpointEventType::FinishedSync => "sync_finished",
            CheckpointEventType::FinishedPreCommit => "precommit_finished",
            CheckpointEventType::FinishedCommit => "commit_finished",
        }
    }
}

/// Checkpoint event reported by a subtask
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointEvent {
    /// Checkpoint ID
    pub checkpoint_id: u64,

    /// Node/Operator ID
    pub operator_id: String,

    /// Subtask index
    pub subtask_index: u32,

    /// When the event occurred
    pub time: SystemTime,

    /// Type of event
    pub event_type: CheckpointEventType,
}

impl CheckpointEvent {
    /// Create a new checkpoint event
    pub fn new(
        checkpoint_id: u64,
        operator_id: String,
        subtask_index: u32,
        event_type: CheckpointEventType,
    ) -> Self {
        Self {
            checkpoint_id,
            operator_id,
            subtask_index,
            time: SystemTime::now(),
            event_type,
        }
    }
}

/// Detailed checkpoint metadata for a subtask
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubtaskCheckpointMetadata {
    /// Checkpoint ID
    pub checkpoint_id: u64,

    /// Operator ID
    pub operator_id: String,

    /// Subtask index
    pub subtask_index: u32,

    /// When checkpointing started
    pub start_time: SystemTime,

    /// When checkpointing finished
    pub finish_time: SystemTime,

    /// Number of bytes in checkpoint data
    pub bytes: u64,

    /// Watermark at checkpoint time (if any)
    pub watermark: Option<u64>,

    /// Table-specific checkpoint metadata (for stateful operators)
    pub table_metadata: HashMap<String, TableCheckpointMetadata>,
}

/// Checkpoint metadata for a specific table/state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCheckpointMetadata {
    /// Table name
    pub table_name: String,

    /// Checkpoint data for each subtask
    pub commit_data_by_subtask: HashMap<u32, Vec<u8>>,
}

/// Checkpoint metadata for an entire operator (all subtasks)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorCheckpointMetadata {
    /// Operator ID
    pub operator_id: String,

    /// Checkpoint ID
    pub checkpoint_id: u64,

    /// When checkpoint started (earliest subtask start)
    pub start_time: SystemTime,

    /// When checkpoint finished (latest subtask finish)
    pub finish_time: SystemTime,

    /// Number of subtasks
    pub parallelism: u32,

    /// Minimum watermark across all subtasks
    pub min_watermark: Option<u64>,

    /// Maximum watermark across all subtasks
    pub max_watermark: Option<u64>,

    /// Table checkpoint metadata for each table
    pub table_checkpoint_metadata: HashMap<String, TableCheckpointMetadata>,
}

/// Task-level checkpoint completion notification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCheckpointCompleted {
    /// Checkpoint ID
    pub checkpoint_id: u64,

    /// Node/Operator ID
    pub operator_id: String,

    /// Subtask index
    pub subtask_index: u32,

    /// Checkpoint metadata
    pub metadata: SubtaskCheckpointMetadata,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_display() {
        assert_eq!(
            CheckpointEventType::StartedAlignment.as_str(),
            "alignment_started"
        );
        assert_eq!(CheckpointEventType::FinishedSync.as_str(), "sync_finished");
    }

    #[test]
    fn test_checkpoint_event_creation() {
        let event = CheckpointEvent::new(
            1,
            "operator-1".to_string(),
            0,
            CheckpointEventType::StartedAlignment,
        );
        assert_eq!(event.checkpoint_id, 1);
        assert_eq!(event.operator_id, "operator-1");
        assert_eq!(event.subtask_index, 0);
        assert_eq!(event.event_type, CheckpointEventType::StartedAlignment);
    }

    #[test]
    fn test_subtask_metadata_serialization() {
        let metadata = SubtaskCheckpointMetadata {
            checkpoint_id: 1,
            operator_id: "operator-1".to_string(),
            subtask_index: 0,
            start_time: SystemTime::now(),
            finish_time: SystemTime::now(),
            bytes: 1024,
            watermark: Some(100),
            table_metadata: HashMap::new(),
        };

        let serialized = bincode::serialize(&metadata).unwrap();
        let deserialized: SubtaskCheckpointMetadata = bincode::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.checkpoint_id, metadata.checkpoint_id);
        assert_eq!(deserialized.bytes, metadata.bytes);
    }
}
