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

//! Committing state management for checkpoint commit phase
//!
//! This module tracks the commit phase of checkpoints, managing which
//! subtasks still need to commit their state. Inspired by Arroyo's CommittingState.

use super::events::{TableCheckpointMetadata, TaskCheckpointCompleted};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tracing::{debug, info};

/// Committing state for a checkpoint
///
/// This tracks which subtasks still need to commit during the commit phase
/// of a two-phase checkpoint protocol.
#[derive(Debug, Clone)]
pub struct CommittingState {
    /// Checkpoint ID
    checkpoint_id: u64,

    /// Set of (operator_id, subtask_index) that still need to commit
    subtasks_to_commit: HashSet<(String, u32)>,

    /// Commit data organized by operator -> table -> subtask -> data
    committing_data: HashMap<String, HashMap<String, HashMap<u32, Vec<u8>>>>,

    /// Number of operators that have finished committing
    operators_committed: usize,

    /// Total number of operators
    total_operators: usize,
}

impl CommittingState {
    /// Create a new committing state
    pub fn new(
        checkpoint_id: u64,
        subtasks_to_commit: HashSet<(String, u32)>,
        committing_data: HashMap<String, HashMap<String, HashMap<u32, Vec<u8>>>>,
        total_operators: usize,
    ) -> Self {
        Self {
            checkpoint_id,
            subtasks_to_commit,
            committing_data,
            operators_committed: 0,
            total_operators,
        }
    }

    /// Get the checkpoint ID
    pub fn checkpoint_id(&self) -> u64 {
        self.checkpoint_id
    }

    /// Mark a subtask as committed
    pub fn subtask_committed(&mut self, operator_id: &str, subtask_index: u32) {
        let key = (operator_id.to_string(), subtask_index);
        if self.subtasks_to_commit.remove(&key) {
            debug!(
                "Subtask {}:{} committed for checkpoint {}",
                operator_id, subtask_index, self.checkpoint_id
            );
        }
    }

    /// Check if all subtasks have committed (all operators done)
    pub fn done(&self) -> bool {
        self.operators_committed >= self.total_operators
    }

    /// Check if all subtasks for a specific operator have committed
    pub fn operator_done(&self, operator_id: &str) -> bool {
        !self
            .subtasks_to_commit
            .iter()
            .any(|(op, _)| op == operator_id)
    }

    /// Get commit data for all operators that are ready to commit
    pub fn get_committing_operators(&self) -> HashSet<String> {
        let operators: HashSet<String> = self
            .subtasks_to_commit
            .iter()
            .map(|(operator_id, _)| operator_id.clone())
            .collect();
        operators
    }

    /// Get commit data for a specific operator
    pub fn get_committing_data(
        &self,
        operator_id: &str,
    ) -> Option<HashMap<String, TableCheckpointMetadata>> {
        self.committing_data.get(operator_id).map(|table_map| {
            let result: HashMap<String, TableCheckpointMetadata> = table_map
                .iter()
                .map(|(table_name, subtask_data)| {
                    (
                        table_name.clone(),
                        TableCheckpointMetadata {
                            table_name: table_name.clone(),
                            commit_data_by_subtask: subtask_data.clone(),
                        },
                    )
                })
                .collect();
            result
        })
    }

    /// Mark an operator as fully committed
    pub fn operator_fully_committed(&mut self, operator_id: &str) {
        if self.operator_done(operator_id) {
            self.operators_committed += 1;
            info!(
                "Operator {} fully committed for checkpoint {} ({}/{})",
                operator_id, self.checkpoint_id, self.operators_committed, self.total_operators
            );
        }
    }

    /// Get remaining subtask count
    pub fn remaining_subtasks(&self) -> usize {
        self.subtasks_to_commit.len()
    }

    /// Get total operators count
    pub fn total_operators(&self) -> usize {
        self.total_operators
    }

    /// Get committed operators count
    pub fn committed_operators(&self) -> usize {
        self.operators_committed
    }
}

/// Checkpoint state that tracks progress through checkpoint lifecycle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointProgress {
    /// Checkpoint ID
    pub checkpoint_id: u64,

    /// Epoch/checkpoint number
    pub epoch: u32,

    /// Minimum epoch to retain
    pub min_epoch: u32,

    /// Start time of checkpoint
    pub start_time: u64,

    /// Number of operators
    pub operators: usize,

    /// Number of operators that have completed checkpoint phase
    pub operators_checkpointed: usize,

    /// Operator-specific checkpoint data
    pub operator_data: HashMap<String, OperatorCheckpointData>,
}

/// Checkpoint data for a single operator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorCheckpointData {
    /// Operator ID
    pub operator_id: String,

    /// Number of subtasks
    pub subtasks: usize,

    /// Number of subtasks that have completed checkpoint
    pub subtasks_checkpointed: usize,

    /// Checkpoint start time
    pub start_time: u64,

    /// Checkpoint finish time
    pub finish_time: Option<u64>,

    /// Bytes checkpointed
    pub bytes: u64,

    /// Table checkpoint metadata
    pub table_metadata: HashMap<String, TableCheckpointMetadata>,
}

impl CheckpointProgress {
    /// Create a new checkpoint progress tracker
    pub fn new(
        checkpoint_id: u64,
        epoch: u32,
        min_epoch: u32,
        operators: Vec<String>,
        subtasks_per_operator: usize,
    ) -> Self {
        let operator_data: HashMap<String, OperatorCheckpointData> = operators
            .into_iter()
            .map(|op_id| {
                (
                    op_id.clone(),
                    OperatorCheckpointData {
                        operator_id: op_id,
                        subtasks: subtasks_per_operator,
                        subtasks_checkpointed: 0,
                        start_time: 0,
                        finish_time: None,
                        bytes: 0,
                        table_metadata: HashMap::new(),
                    },
                )
            })
            .collect();

        Self {
            checkpoint_id,
            epoch,
            min_epoch,
            start_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            operators: operator_data.len(),
            operators_checkpointed: 0,
            operator_data,
        }
    }

    /// Update progress for a subtask
    pub fn update_subtask(&mut self, completed: &TaskCheckpointCompleted) -> bool {
        let metadata = &completed.metadata;

        let operator_data = self
            .operator_data
            .entry(completed.operator_id.clone())
            .or_insert_with(|| OperatorCheckpointData {
                operator_id: completed.operator_id.clone(),
                subtasks: 1,
                subtasks_checkpointed: 0,
                start_time: metadata
                    .start_time
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                finish_time: None,
                bytes: 0,
                table_metadata: HashMap::new(),
            });

        operator_data.subtasks_checkpointed += 1;
        operator_data.bytes += metadata.bytes;
        operator_data.finish_time = Some(
            metadata
                .finish_time
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        );

        // Merge table metadata
        for (table_name, table_meta) in &metadata.table_metadata {
            operator_data
                .table_metadata
                .insert(table_name.clone(), table_meta.clone());
        }

        // Check if operator is done
        if operator_data.subtasks_checkpointed >= operator_data.subtasks {
            self.operators_checkpointed += 1;
            true
        } else {
            false
        }
    }

    /// Check if checkpoint is complete
    pub fn is_complete(&self) -> bool {
        self.operators_checkpointed >= self.operators
    }

    /// Get completion percentage
    pub fn completion_percent(&self) -> f64 {
        if self.operators == 0 {
            return 100.0;
        }
        (self.operators_checkpointed as f64 / self.operators as f64) * 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::super::events::SubtaskCheckpointMetadata;
    use super::*;
    use std::time::SystemTime;

    #[test]
    fn test_committing_state_creation() {
        let mut subtasks = HashSet::new();
        subtasks.insert(("op1".to_string(), 0));
        subtasks.insert(("op1".to_string(), 1));

        let state = CommittingState::new(1, subtasks, HashMap::new(), 2);
        assert_eq!(state.checkpoint_id(), 1);
        assert_eq!(state.remaining_subtasks(), 2);
        assert!(!state.done());
    }

    #[test]
    fn test_subtask_commit() {
        let mut subtasks = HashSet::new();
        subtasks.insert(("op1".to_string(), 0));
        subtasks.insert(("op1".to_string(), 1));

        let mut state = CommittingState::new(1, subtasks, HashMap::new(), 1);

        state.subtask_committed("op1", 0);
        assert_eq!(state.remaining_subtasks(), 1);
        assert!(!state.operator_done("op1"));

        state.subtask_committed("op1", 1);
        assert_eq!(state.remaining_subtasks(), 0);
        assert!(state.operator_done("op1"));
    }

    #[test]
    fn test_checkpoint_progress() {
        let operators = vec!["op1".to_string(), "op2".to_string()];
        let mut progress = CheckpointProgress::new(1, 10, 5, operators, 2);

        assert!(!progress.is_complete());
        assert_eq!(progress.completion_percent(), 0.0);

        // Complete op1
        let subtask_meta = SubtaskCheckpointMetadata {
            checkpoint_id: 1,
            operator_id: "op1".to_string(),
            subtask_index: 0,
            start_time: SystemTime::now(),
            finish_time: SystemTime::now(),
            bytes: 1024,
            watermark: None,
            table_metadata: HashMap::new(),
        };

        let completed = TaskCheckpointCompleted {
            checkpoint_id: 1,
            operator_id: "op1".to_string(),
            subtask_index: 0,
            metadata: subtask_meta.clone(),
        };

        progress.update_subtask(&completed);
        progress.update_subtask(&TaskCheckpointCompleted {
            subtask_index: 1,
            metadata: subtask_meta,
            ..completed
        });

        assert!(!progress.is_complete());
        assert!((progress.completion_percent() - 50.0).abs() < 0.01);
    }
}
