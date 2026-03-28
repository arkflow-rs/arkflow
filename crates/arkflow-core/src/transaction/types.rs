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

//! Transaction types for exactly-once semantics
//!
//! This module defines the core types used for two-phase commit (2PC)
//! and idempotency tracking.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

/// Unique transaction identifier
pub type TransactionId = u64;

/// Transaction state machine
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    /// Transaction initialized
    Init,
    /// First phase: preparing
    Preparing,
    /// First phase: prepared (ready to commit)
    Prepared,
    /// Second phase: committing
    Committing,
    /// Transaction committed successfully
    Committed,
    /// Transaction being rolled back
    RollingBack,
    /// Transaction rolled back
    RolledBack,
    /// Transaction timed out
    TimedOut,
}

/// Transaction record for WAL and state tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionRecord {
    /// Unique transaction ID
    pub id: TransactionId,

    /// Current transaction state
    pub state: TransactionState,

    /// When the transaction was created
    pub created_at: SystemTime,

    /// When the transaction was last updated
    pub updated_at: SystemTime,

    /// Sequence numbers involved in this transaction
    pub sequence_numbers: Vec<u64>,

    /// Idempotency keys for deduplication
    pub idempotency_keys: Vec<String>,

    /// Additional metadata
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl TransactionRecord {
    /// Create a new transaction record
    pub fn new(id: TransactionId, sequence_numbers: Vec<u64>) -> Self {
        let now = SystemTime::now();
        Self {
            id,
            state: TransactionState::Init,
            created_at: now,
            updated_at: now,
            sequence_numbers,
            idempotency_keys: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    /// Transition to a new state
    pub fn transition_to(&mut self, new_state: TransactionState) {
        self.state = new_state;
        self.updated_at = SystemTime::now();
    }

    /// Add an idempotency key
    pub fn add_idempotency_key(&mut self, key: String) {
        self.idempotency_keys.push(key);
    }

    /// Check if transaction is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            TransactionState::Committed | TransactionState::RolledBack | TransactionState::TimedOut
        )
    }

    /// Get transaction age in seconds
    pub fn age_seconds(&self) -> u64 {
        self.updated_at
            .duration_since(self.created_at)
            .unwrap_or_default()
            .as_secs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_state_transitions() {
        let mut record = TransactionRecord::new(1, vec![10, 20, 30]);

        assert_eq!(record.state, TransactionState::Init);
        assert!(!record.is_terminal());

        record.transition_to(TransactionState::Preparing);
        assert_eq!(record.state, TransactionState::Preparing);
        assert!(!record.is_terminal());

        record.transition_to(TransactionState::Prepared);
        assert_eq!(record.state, TransactionState::Prepared);

        record.transition_to(TransactionState::Committing);
        assert_eq!(record.state, TransactionState::Committing);

        record.transition_to(TransactionState::Committed);
        assert_eq!(record.state, TransactionState::Committed);
        assert!(record.is_terminal());
    }

    #[test]
    fn test_transaction_add_keys() {
        let mut record = TransactionRecord::new(1, vec![100]);

        record.add_idempotency_key("key1".to_string());
        record.add_idempotency_key("key2".to_string());

        assert_eq!(record.idempotency_keys.len(), 2);
        assert_eq!(record.idempotency_keys[0], "key1");
        assert_eq!(record.idempotency_keys[1], "key2");
    }

    #[test]
    fn test_transaction_serialization() {
        let record = TransactionRecord {
            id: 42,
            state: TransactionState::Prepared,
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            sequence_numbers: vec![1, 2, 3],
            idempotency_keys: vec!["test-key".to_string()],
            metadata: HashMap::new(),
        };

        let serialized = bincode::serialize(&record).unwrap();
        let deserialized: TransactionRecord = bincode::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.id, 42);
        assert_eq!(deserialized.state, TransactionState::Prepared);
        assert_eq!(deserialized.sequence_numbers, vec![1, 2, 3]);
    }
}
