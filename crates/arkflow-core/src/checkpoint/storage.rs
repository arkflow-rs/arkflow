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

//! Checkpoint storage backends
//!
//! This module provides storage abstraction for checkpoints, supporting:
//! - Local filesystem storage (fast path)
//! - Cloud storage (S3, GCS, Azure) for durability

use super::{metadata::CheckpointMetadata, state::StateSnapshot, CheckpointId, CheckpointResult};
use crate::Error;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Trait for checkpoint storage backends
#[async_trait]
pub trait CheckpointStorage: Send + Sync {
    /// Save checkpoint (atomic operation)
    async fn save_checkpoint(
        &self,
        id: CheckpointId,
        state: &StateSnapshot,
    ) -> CheckpointResult<CheckpointMetadata>;

    /// Load checkpoint
    async fn load_checkpoint(&self, id: CheckpointId) -> CheckpointResult<Option<StateSnapshot>>;

    /// List available checkpoints
    async fn list_checkpoints(&self) -> CheckpointResult<Vec<CheckpointMetadata>>;

    /// Delete checkpoint
    async fn delete_checkpoint(&self, id: CheckpointId) -> CheckpointResult<()>;

    /// Get latest checkpoint ID
    async fn get_latest_checkpoint(&self) -> CheckpointResult<Option<CheckpointId>>;
}

/// Local filesystem storage for checkpoints
pub struct LocalFileStorage {
    /// Base directory for checkpoints
    base_path: PathBuf,
    /// State serializer
    serializer: super::state::StateSerializer,
}

impl LocalFileStorage {
    /// Create new local file storage
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self, Error> {
        let path = PathBuf::from(base_path.as_ref());

        // Create directory if it doesn't exist
        std::fs::create_dir_all(&path)
            .map_err(|e| Error::Config(format!("Failed to create checkpoint directory: {}", e)))?;

        Ok(Self {
            base_path: path,
            serializer: super::state::StateSerializer::new(),
        })
    }

    /// Get checkpoint file path
    fn checkpoint_path(&self, id: CheckpointId) -> PathBuf {
        self.base_path.join(format!("checkpoint-{}.dat", id))
    }

    /// Get metadata file path
    fn metadata_path(&self, id: CheckpointId) -> PathBuf {
        self.base_path.join(format!("checkpoint-{}.meta", id))
    }

    /// Save metadata atomically using write-then-rename
    async fn save_metadata_atomic(
        &self,
        id: CheckpointId,
        metadata: &CheckpointMetadata,
    ) -> Result<(), Error> {
        let meta_path = self.metadata_path(id);
        let temp_path = meta_path.with_extension("tmp");

        // Serialize metadata to JSON
        let json = serde_json::to_string_pretty(metadata)
            .map_err(|e| Error::Process(format!("Failed to serialize metadata: {}", e)))?;

        // Write to temporary file
        let mut file = fs::File::create(&temp_path)
            .await
            .map_err(|e| Error::Read(format!("Failed to create temp file: {}", e)))?;

        file.write_all(json.as_bytes())
            .await
            .map_err(|e| Error::Read(format!("Failed to write metadata: {}", e)))?;

        file.sync_all()
            .await
            .map_err(|e| Error::Read(format!("Failed to sync metadata: {}", e)))?;

        // Atomic rename
        fs::rename(&temp_path, &meta_path)
            .await
            .map_err(|e| Error::Read(format!("Failed to rename metadata file: {}", e)))?;

        Ok(())
    }

    /// Load metadata from file
    async fn load_metadata(&self, id: CheckpointId) -> Result<Option<CheckpointMetadata>, Error> {
        let meta_path = self.metadata_path(id);

        // Check if file exists
        if !meta_path.exists() {
            return Ok(None);
        }

        // Read metadata
        let mut file = fs::File::open(&meta_path)
            .await
            .map_err(|e| Error::Read(format!("Failed to open metadata: {}", e)))?;

        let mut contents = Vec::new();
        file.read_to_end(&mut contents)
            .await
            .map_err(|e| Error::Read(format!("Failed to read metadata: {}", e)))?;

        // Deserialize
        let metadata: CheckpointMetadata = serde_json::from_slice(&contents)
            .map_err(|e| Error::Process(format!("Failed to deserialize metadata: {}", e)))?;

        Ok(Some(metadata))
    }
}

#[async_trait]
impl CheckpointStorage for LocalFileStorage {
    /// Save checkpoint atomically using write-then-rename
    async fn save_checkpoint(
        &self,
        id: CheckpointId,
        state: &StateSnapshot,
    ) -> CheckpointResult<CheckpointMetadata> {
        let checkpoint_path = self.checkpoint_path(id);
        let temp_path = checkpoint_path.with_extension("tmp");

        // 1. Serialize state
        let serialized = self
            .serializer
            .serialize(state)
            .map_err(|e| Error::Process(format!("Serialization failed: {}", e)))?;

        // 2. Write to temporary file
        {
            let mut file = fs::File::create(&temp_path).await.map_err(|e| {
                Error::Read(format!("Failed to create temp checkpoint file: {}", e))
            })?;

            file.write_all(&serialized)
                .await
                .map_err(|e| Error::Read(format!("Failed to write checkpoint: {}", e)))?;

            file.sync_all()
                .await
                .map_err(|e| Error::Read(format!("Failed to sync checkpoint: {}", e)))?;
        }

        // 3. Atomic rename
        fs::rename(&temp_path, &checkpoint_path)
            .await
            .map_err(|e| Error::Read(format!("Failed to rename checkpoint file: {}", e)))?;

        // 4. Create and save metadata
        let mut metadata =
            CheckpointMetadata::new(id, checkpoint_path.to_string_lossy().to_string());
        metadata.mark_completed(serialized.len() as u64);

        self.save_metadata_atomic(id, &metadata).await?;

        Ok(metadata)
    }

    /// Load checkpoint from disk
    async fn load_checkpoint(&self, id: CheckpointId) -> CheckpointResult<Option<StateSnapshot>> {
        let checkpoint_path = self.checkpoint_path(id);

        // Check if checkpoint exists
        if !checkpoint_path.exists() {
            return Ok(None);
        }

        // Read checkpoint file
        let mut file = fs::File::open(&checkpoint_path)
            .await
            .map_err(|e| Error::Read(format!("Failed to open checkpoint: {}", e)))?;

        let mut contents = Vec::new();
        file.read_to_end(&mut contents)
            .await
            .map_err(|e| Error::Read(format!("Failed to read checkpoint: {}", e)))?;

        // Deserialize
        let state = self
            .serializer
            .deserialize(&contents)
            .map_err(|e| Error::Process(format!("Deserialization failed: {}", e)))?;

        Ok(Some(state))
    }

    /// List all available checkpoints
    async fn list_checkpoints(&self) -> CheckpointResult<Vec<CheckpointMetadata>> {
        let mut checkpoints = Vec::new();

        // Read directory
        let mut entries = fs::read_dir(&self.base_path)
            .await
            .map_err(|e| Error::Read(format!("Failed to read checkpoint directory: {}", e)))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| Error::Read(format!("Failed to read directory entry: {}", e)))?
        {
            let path = entry.path();

            // Look for .meta files
            if path.extension().and_then(|s| s.to_str()) == Some("meta") {
                // Extract checkpoint ID from filename
                let filename = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");

                if let Some(id_str) = filename.strip_prefix("checkpoint-") {
                    if let Ok(id) = id_str.parse::<CheckpointId>() {
                        // Load metadata
                        if let Some(metadata) = self.load_metadata(id).await? {
                            checkpoints.push(metadata);
                        }
                    }
                }
            }
        }

        // Sort by ID descending (newest first)
        checkpoints.sort_by(|a, b| b.id.cmp(&a.id));

        Ok(checkpoints)
    }

    /// Delete checkpoint
    async fn delete_checkpoint(&self, id: CheckpointId) -> CheckpointResult<()> {
        let checkpoint_path = self.checkpoint_path(id);
        let metadata_path = self.metadata_path(id);

        // Delete checkpoint file
        if checkpoint_path.exists() {
            fs::remove_file(&checkpoint_path)
                .await
                .map_err(|e| Error::Read(format!("Failed to delete checkpoint: {}", e)))?;
        }

        // Delete metadata file
        if metadata_path.exists() {
            fs::remove_file(&metadata_path)
                .await
                .map_err(|e| Error::Read(format!("Failed to delete metadata: {}", e)))?;
        }

        Ok(())
    }

    /// Get latest checkpoint ID
    async fn get_latest_checkpoint(&self) -> CheckpointResult<Option<CheckpointId>> {
        let checkpoints = self.list_checkpoints().await?;

        if checkpoints.is_empty() {
            Ok(None)
        } else {
            // Already sorted by ID descending, so first is latest
            Ok(Some(checkpoints[0].id))
        }
    }
}

/// Cloud storage for checkpoints (placeholder for future implementation)
pub struct CloudStorage {
    /// Cloud storage type (s3, gcs, azure)
    storage_type: String,
    /// Bucket/container name
    bucket: String,
    /// Prefix/path within bucket
    prefix: String,
}

impl CloudStorage {
    /// Create new cloud storage (placeholder)
    pub fn new(storage_type: String, bucket: String, prefix: String) -> Self {
        Self {
            storage_type,
            bucket,
            prefix,
        }
    }
}

#[async_trait]
impl CheckpointStorage for CloudStorage {
    async fn save_checkpoint(
        &self,
        _id: CheckpointId,
        _state: &StateSnapshot,
    ) -> CheckpointResult<CheckpointMetadata> {
        Err(Error::Process(
            "Cloud storage not yet implemented".to_string(),
        ))
    }

    async fn load_checkpoint(&self, _id: CheckpointId) -> CheckpointResult<Option<StateSnapshot>> {
        Err(Error::Process(
            "Cloud storage not yet implemented".to_string(),
        ))
    }

    async fn list_checkpoints(&self) -> CheckpointResult<Vec<CheckpointMetadata>> {
        Err(Error::Process(
            "Cloud storage not yet implemented".to_string(),
        ))
    }

    async fn delete_checkpoint(&self, _id: CheckpointId) -> CheckpointResult<()> {
        Err(Error::Process(
            "Cloud storage not yet implemented".to_string(),
        ))
    }

    async fn get_latest_checkpoint(&self) -> CheckpointResult<Option<CheckpointId>> {
        Err(Error::Process(
            "Cloud storage not yet implemented".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_local_storage_save_and_load() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalFileStorage::new(temp_dir.path()).unwrap();

        // Create state
        let mut state = StateSnapshot::new();
        state.sequence_counter = 42;
        state.next_seq = 43;

        // Save checkpoint
        let id = 1;
        let metadata = storage.save_checkpoint(id, &state).await.unwrap();

        assert_eq!(metadata.id, id);
        assert!(metadata.is_completed());
        assert!(metadata.size_bytes > 0);

        // Load checkpoint
        let loaded = storage.load_checkpoint(id).await.unwrap();
        assert!(loaded.is_some());

        let loaded_state = loaded.unwrap();
        assert_eq!(loaded_state.sequence_counter, state.sequence_counter);
        assert_eq!(loaded_state.next_seq, state.next_seq);
    }

    #[tokio::test]
    async fn test_local_storage_list_checkpoints() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalFileStorage::new(temp_dir.path()).unwrap();

        // Save multiple checkpoints
        for i in 1..=3 {
            let state = StateSnapshot::new();
            storage.save_checkpoint(i, &state).await.unwrap();
        }

        // List checkpoints
        let checkpoints = storage.list_checkpoints().await.unwrap();

        assert_eq!(checkpoints.len(), 3);
        // Should be sorted by ID descending
        assert_eq!(checkpoints[0].id, 3);
        assert_eq!(checkpoints[1].id, 2);
        assert_eq!(checkpoints[2].id, 1);
    }

    #[tokio::test]
    async fn test_local_storage_delete_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalFileStorage::new(temp_dir.path()).unwrap();

        // Save checkpoint
        let state = StateSnapshot::new();
        let id = 1;
        storage.save_checkpoint(id, &state).await.unwrap();

        // Verify it exists
        let loaded = storage.load_checkpoint(id).await.unwrap();
        assert!(loaded.is_some());

        // Delete checkpoint
        storage.delete_checkpoint(id).await.unwrap();

        // Verify it's gone
        let loaded = storage.load_checkpoint(id).await.unwrap();
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn test_local_storage_get_latest() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalFileStorage::new(temp_dir.path()).unwrap();

        // No checkpoints initially
        let latest = storage.get_latest_checkpoint().await.unwrap();
        assert!(latest.is_none());

        // Save multiple checkpoints
        for i in 1..=5 {
            let state = StateSnapshot::new();
            storage.save_checkpoint(i, &state).await.unwrap();
        }

        // Get latest
        let latest = storage.get_latest_checkpoint().await.unwrap();
        assert_eq!(latest, Some(5));
    }

    #[tokio::test]
    async fn test_local_storage_nonexistent_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalFileStorage::new(temp_dir.path()).unwrap();

        // Try to load non-existent checkpoint
        let loaded = storage.load_checkpoint(999).await.unwrap();
        assert!(loaded.is_none());
    }
}
