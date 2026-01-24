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

//! State serialization and deserialization
//!
//! This module handles serialization of stream processing state using MessagePack format
//! with optional zstd compression for efficient storage.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use zstd;

/// Current state serialization format version
pub const STATE_VERSION: u32 = 1;

/// Snapshot of stream processing state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateSnapshot {
    /// State format version
    pub version: u32,

    /// Timestamp when snapshot was taken
    pub timestamp: i64,

    /// Sequence counter value
    pub sequence_counter: u64,

    /// Next sequence number
    pub next_seq: u64,

    /// Input-specific state (e.g., Kafka offset, file position)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_state: Option<InputState>,

    /// Buffer state (cached messages)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer_state: Option<BufferState>,

    /// Additional metadata
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// Input-specific state for recovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InputState {
    /// Kafka input state
    Kafka {
        /// Topic name
        topic: String,
        /// Partition -> Offset mapping
        offsets: HashMap<i32, i64>,
    },
    /// File input state
    File {
        /// File path
        path: String,
        /// Byte offset in file
        offset: u64,
    },
    /// Redis input state
    Redis {
        /// Stream name
        stream: String,
        /// Last sequence ID
        sequence: String,
    },
    /// Generic state
    Generic {
        /// State data
        data: HashMap<String, String>,
    },
}

/// Buffer state for recovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferState {
    /// Number of messages in buffer
    pub message_count: usize,

    /// Serialized message data (optional, for small buffers)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages: Option<Vec<u8>>,

    /// Buffer type identifier
    pub buffer_type: String,
}

impl StateSnapshot {
    /// Create a new state snapshot
    pub fn new() -> Self {
        Self {
            version: STATE_VERSION,
            timestamp: chrono::Utc::now().timestamp(),
            sequence_counter: 0,
            next_seq: 0,
            input_state: None,
            buffer_state: None,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata key-value pair
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// Validate snapshot version compatibility
    pub fn is_compatible(&self) -> bool {
        self.version <= STATE_VERSION
    }
}

impl Default for StateSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

/// State serializer using MessagePack + zstd compression
pub struct StateSerializer {
    /// Compression level (1-21, default 3)
    compression_level: i32,
}

impl StateSerializer {
    /// Create a new serializer with default compression level (3)
    pub fn new() -> Self {
        Self {
            compression_level: 3,
        }
    }

    /// Create a new serializer with custom compression level
    pub fn with_compression(level: i32) -> Self {
        assert!(
            (1..=21).contains(&level),
            "Compression level must be between 1 and 21"
        );
        Self {
            compression_level: level,
        }
    }

    /// Serialize state snapshot to bytes (MessagePack + zstd)
    pub fn serialize(&self, state: &StateSnapshot) -> Result<Vec<u8>, String> {
        // 1. Serialize to MessagePack (using named fields for better compatibility)
        let msgpack_bytes = rmp_serde::to_vec_named(state)
            .map_err(|e| format!("Failed to serialize state: {}", e))?;

        // 2. Compress with zstd
        let compressed = self.compress(&msgpack_bytes)?;

        Ok(compressed)
    }

    /// Deserialize state snapshot from bytes
    pub fn deserialize(&self, bytes: &[u8]) -> Result<StateSnapshot, String> {
        // 1. Decompress
        let decompressed = self.decompress(bytes)?;

        // 2. Deserialize from MessagePack (using named fields)
        let state: StateSnapshot = rmp_serde::from_slice(&decompressed)
            .map_err(|e| format!("Failed to deserialize state: {}", e))?;

        // 3. Validate version
        if !state.is_compatible() {
            return Err(format!(
                "Incompatible state version: got {}, expected <= {}",
                state.version, STATE_VERSION
            ));
        }

        Ok(state)
    }

    /// Compress bytes using zstd
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        let compressed = zstd::bulk::compress(data, self.compression_level)
            .map_err(|e| format!("Compression failed: {}", e))?;
        Ok(compressed)
    }

    /// Decompress bytes using zstd
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        // Use a reasonable maximum size (100MB) instead of usize::MAX
        const MAX_DECOMPRESSED_SIZE: usize = 100 * 1024 * 1024;
        let decompressed = zstd::bulk::decompress(data, MAX_DECOMPRESSED_SIZE)
            .map_err(|e| format!("Decompression failed: {}", e))?;
        Ok(decompressed)
    }

    /// Get compression ratio (compressed_size / original_size)
    pub fn compression_ratio(&self, original: &[u8], compressed: &[u8]) -> f64 {
        if original.is_empty() {
            return 1.0;
        }
        compressed.len() as f64 / original.len() as f64
    }
}

impl Default for StateSerializer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_snapshot_creation() {
        let snapshot = StateSnapshot::new();
        assert_eq!(snapshot.version, STATE_VERSION);
        assert_eq!(snapshot.sequence_counter, 0);
        assert!(snapshot.input_state.is_none());
        assert!(snapshot.buffer_state.is_none());
    }

    #[test]
    fn test_state_snapshot_metadata() {
        let mut snapshot = StateSnapshot::new();
        snapshot.add_metadata("key1".to_string(), "value1".to_string());
        snapshot.add_metadata("key2".to_string(), "value2".to_string());

        assert_eq!(snapshot.metadata.len(), 2);
        assert_eq!(snapshot.metadata.get("key1"), Some(&"value1".to_string()));
    }

    #[test]
    fn test_input_state_kafka() {
        let mut offsets = HashMap::new();
        offsets.insert(0, 100);
        offsets.insert(1, 200);

        let state = InputState::Kafka {
            topic: "test-topic".to_string(),
            offsets,
        };

        match state {
            InputState::Kafka { topic, offsets } => {
                assert_eq!(topic, "test-topic");
                assert_eq!(offsets.len(), 2);
            }
            _ => panic!("Expected Kafka state"),
        }
    }

    #[test]
    fn test_serialization_roundtrip() {
        let serializer = StateSerializer::new();

        let mut original = StateSnapshot::new();
        original.sequence_counter = 42;
        original.next_seq = 43;
        original.add_metadata("test".to_string(), "data".to_string());

        // Serialize
        let bytes = serializer.serialize(&original).unwrap();

        // Deserialize
        let restored = serializer.deserialize(&bytes).unwrap();

        assert_eq!(restored.version, original.version);
        assert_eq!(restored.sequence_counter, original.sequence_counter);
        assert_eq!(restored.next_seq, original.next_seq);
        assert_eq!(restored.metadata, original.metadata);
    }

    #[test]
    fn test_compression() {
        let serializer = StateSerializer::new();

        // Create some data
        let data = vec![b'x'; 10000];

        // Compress
        let compressed = serializer.compress(&data).unwrap();

        // Should achieve significant compression for repetitive data
        assert!(compressed.len() < data.len() / 2);

        // Decompress
        let decompressed = serializer.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_serialization_compression_ratio() {
        let serializer = StateSerializer::new();

        let mut snapshot = StateSnapshot::new();
        // Add a lot of metadata to test compression
        for i in 0..1000 {
            snapshot.add_metadata(format!("key{}", i), format!("value{}", i));
        }

        let msgpack = rmp_serde::to_vec(&snapshot).unwrap();
        let compressed = serializer.serialize(&snapshot).unwrap();

        let ratio = serializer.compression_ratio(&msgpack, &compressed);
        println!("Compression ratio: {:.2}%", ratio * 100.0);

        // Should achieve some compression
        assert!(ratio < 1.0);
    }

    #[test]
    fn test_invalid_compression_level() {
        let result = std::panic::catch_unwind(|| {
            StateSerializer::with_compression(0);
        });
        assert!(result.is_err());
    }
}
