/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License);
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

//! Checkpoint recovery end-to-end tests
//!
//! This module tests the complete checkpoint save and restore flow

use arkflow_core::checkpoint::{
    CheckpointConfig, CheckpointCoordinator, CheckpointStorage, LocalFileStorage, StateSnapshot,
};
use arkflow_core::input::{Ack, Input};
use arkflow_core::output::Output;
use arkflow_core::stream::Stream;
use arkflow_core::{MessageBatch, Resource};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

/// Mock input for testing
struct MockInput {
    name: Option<String>,
    messages: Vec<MessageBatch>,
    position:
        std::sync::Arc<tokio::sync::RwLock<Option<arkflow_core::checkpoint::state::InputState>>>,
}

impl MockInput {
    fn new(name: Option<String>, messages: Vec<MessageBatch>) -> Self {
        Self {
            name,
            messages,
            position: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
        }
    }
}

#[async_trait::async_trait]
impl Input for MockInput {
    async fn connect(&self) -> Result<(), arkflow_core::Error> {
        Ok(())
    }

    async fn read(&self) -> Result<(Arc<MessageBatch>, Arc<dyn Ack>), arkflow_core::Error> {
        if self.messages.is_empty() {
            sleep(Duration::from_millis(100)).await;
            return Err(arkflow_core::Error::Process("No more messages".to_string()));
        }
        // Return a clone of the first message
        let msg = self.messages.get(0).unwrap().clone();
        Ok((Arc::new(msg), Arc::new(MockAck)))
    }

    async fn close(&self) -> Result<(), arkflow_core::Error> {
        Ok(())
    }

    async fn get_position(
        &self,
    ) -> Result<Option<arkflow_core::checkpoint::state::InputState>, arkflow_core::Error> {
        Ok(self.position.read().await.clone())
    }

    async fn seek(
        &self,
        position: &arkflow_core::checkpoint::state::InputState,
    ) -> Result<(), arkflow_core::Error> {
        *self.position.write().await = Some(position.clone());
        Ok(())
    }
}

struct MockAck;

#[async_trait::async_trait]
impl Ack for MockAck {
    async fn ack(&self) {}
}

/// Mock output for testing
struct MockOutput {
    name: Option<String>,
}

impl MockOutput {
    fn new(name: Option<String>) -> Self {
        Self { name }
    }
}

#[async_trait::async_trait]
impl Output for MockOutput {
    async fn connect(&self) -> Result<(), arkflow_core::Error> {
        Ok(())
    }

    async fn write(&self, _batch: Arc<MessageBatch>) -> Result<(), arkflow_core::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), arkflow_core::Error> {
        Ok(())
    }
}

#[tokio::test]
async fn test_checkpoint_save_and_restore() {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_path).unwrap();

    // Create checkpoint storage
    let storage = LocalFileStorage::new(checkpoint_path.to_str().unwrap()).unwrap();

    // Create a state snapshot
    let mut metadata = HashMap::new();
    metadata.insert("test_key".to_string(), "test_value".to_string());
    metadata.insert("counter".to_string(), "100".to_string());

    let snapshot = StateSnapshot {
        version: 1,
        timestamp: chrono::Utc::now().timestamp(),
        sequence_counter: 100,
        next_seq: 50,
        input_state: Some(arkflow_core::checkpoint::state::InputState::Generic {
            data: metadata.clone(),
        }),
        buffer_state: None,
        metadata: metadata.clone(),
    };

    // Save checkpoint
    let checkpoint_id = 1u64;
    storage
        .save_checkpoint(checkpoint_id, &snapshot)
        .await
        .unwrap();

    // Restore checkpoint
    let restored_snapshot = storage
        .load_checkpoint(checkpoint_id)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(restored_snapshot.version, snapshot.version);
    assert_eq!(
        restored_snapshot.sequence_counter,
        snapshot.sequence_counter
    );
    assert_eq!(restored_snapshot.next_seq, snapshot.next_seq);
    assert!(restored_snapshot.input_state.is_some());
}

#[tokio::test]
async fn test_coordinator_restore_no_checkpoint() {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join("checkpoints");

    let config = CheckpointConfig {
        enabled: true,
        interval: Duration::from_secs(1),
        max_checkpoints: 5,
        min_age: Duration::from_secs(60),
        local_path: checkpoint_path.to_str().unwrap().to_string(),
        alignment_timeout: Duration::from_secs(10),
    };

    let coordinator = CheckpointCoordinator::new(config).unwrap();

    // Try to restore when no checkpoint exists
    let result = coordinator.restore_from_checkpoint().await.unwrap();

    assert!(result.is_none());
}

#[tokio::test]
async fn test_checkpoint_with_kafka_state() {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_path).unwrap();

    let storage = LocalFileStorage::new(checkpoint_path.to_str().unwrap()).unwrap();

    // Create snapshot with Kafka state
    let mut offsets = HashMap::new();
    offsets.insert(0, 100);
    offsets.insert(1, 200);

    let snapshot = StateSnapshot {
        version: 1,
        timestamp: chrono::Utc::now().timestamp(),
        sequence_counter: 500,
        next_seq: 450,
        input_state: Some(arkflow_core::checkpoint::state::InputState::Kafka {
            topic: "test_topic".to_string(),
            offsets,
        }),
        buffer_state: None,
        metadata: HashMap::new(),
    };

    // Save checkpoint
    storage.save_checkpoint(1, &snapshot).await.unwrap();

    // Restore checkpoint
    let restored = storage.load_checkpoint(1).await.unwrap().unwrap();

    match restored.input_state {
        Some(arkflow_core::checkpoint::state::InputState::Kafka {
            topic,
            offsets: restored_offsets,
        }) => {
            assert_eq!(topic, "test_topic");
            assert_eq!(restored_offsets.len(), 2);
            assert_eq!(restored_offsets.get(&0), Some(&100));
            assert_eq!(restored_offsets.get(&1), Some(&200));
        }
        _ => panic!("Expected Kafka state"),
    }
}

#[tokio::test]
async fn test_multiple_checkpoint_restore_latest() {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_path).unwrap();

    let storage = Arc::new(LocalFileStorage::new(checkpoint_path.to_str().unwrap()).unwrap());

    // Save multiple checkpoints
    for i in 1..=3 {
        let mut metadata = HashMap::new();
        metadata.insert("checkpoint_id".to_string(), format!("{}", i));
        metadata.insert("seq".to_string(), format!("{}", i * 100));

        let snapshot = StateSnapshot {
            version: 1,
            timestamp: chrono::Utc::now().timestamp(),
            sequence_counter: i * 100,
            next_seq: i * 100 - 50,
            input_state: Some(arkflow_core::checkpoint::state::InputState::Generic {
                data: metadata.clone(),
            }),
            buffer_state: None,
            metadata: metadata.clone(),
        };

        storage.save_checkpoint(i, &snapshot).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    // Restore should get the latest checkpoint (ID 3)
    let latest_id = storage.get_latest_checkpoint().await.unwrap().unwrap();
    assert_eq!(latest_id, 3);

    let restored = storage.load_checkpoint(latest_id).await.unwrap().unwrap();
    assert_eq!(restored.sequence_counter, 300);
    assert_eq!(restored.next_seq, 250);
}

#[tokio::test]
async fn test_stream_restore_with_mock_input() {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_path).unwrap();

    // Create mock input and output
    let input = Arc::new(MockInput::new(Some("test_input".to_string()), vec![]));
    let output = Arc::new(MockOutput::new(Some("test_output".to_string())));

    // Create stream with correct parameter order
    let mut stream = Stream::new(
        input.clone(),
        arkflow_core::pipeline::Pipeline::new(vec![]),
        output,
        None,
        None,
        Resource {
            temporary: HashMap::new(),
            input_names: std::cell::RefCell::new(Vec::new()),
        },
        1,
    );

    // Restore from checkpoint with input state
    let mut restore_data = HashMap::new();
    restore_data.insert("restore_key".to_string(), "restore_value".to_string());
    restore_data.insert("position".to_string(), "150".to_string());

    let snapshot = StateSnapshot {
        version: 1,
        timestamp: chrono::Utc::now().timestamp(),
        sequence_counter: 200,
        next_seq: 150,
        input_state: Some(arkflow_core::checkpoint::state::InputState::Generic {
            data: restore_data.clone(),
        }),
        buffer_state: None,
        metadata: restore_data.clone(),
    };

    stream.restore_from_checkpoint(&snapshot).await.unwrap();

    // Verify input position was restored
    let position = input.get_position().await.unwrap();
    assert!(position.is_some());

    // Verify the restored state
    match position {
        Some(arkflow_core::checkpoint::state::InputState::Generic {
            data: restored_data,
        }) => {
            assert_eq!(
                restored_data.get("restore_key"),
                Some(&"restore_value".to_string())
            );
            assert_eq!(restored_data.get("position"), Some(&"150".to_string()));
        }
        _ => panic!("Expected Generic state"),
    }
}
