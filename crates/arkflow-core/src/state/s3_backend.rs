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

//! S3-based state backend implementation

use crate::state::helper::SimpleMemoryState;
use crate::Error;
use futures_util::stream::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// S3 state backend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3StateBackendConfig {
    /// S3 bucket name
    pub bucket: String,
    /// S3 region
    pub region: String,
    /// S3 endpoint (for non-AWS S3)
    pub endpoint: Option<String>,
    /// Access key ID
    pub access_key_id: Option<String>,
    /// Secret access key
    pub secret_access_key: Option<String>,
    /// Path prefix for state storage
    pub prefix: Option<String>,
    /// Enable SSL
    pub use_ssl: bool,
}

impl Default for S3StateBackendConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            prefix: Some("arkflow/state".to_string()),
            use_ssl: true,
        }
    }
}

/// S3-based state backend
pub struct S3StateBackend {
    config: S3StateBackendConfig,
    pub client: Arc<dyn ObjectStore>,
    local_cache: HashMap<String, SimpleMemoryState>,
    checkpoint_base_path: Path,
}

impl S3StateBackend {
    /// Create new S3 state backend
    pub async fn new(config: S3StateBackendConfig) -> Result<Self, Error> {
        // Build S3 client
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region);

        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }

        if let Some(access_key_id) = &config.access_key_id {
            builder = builder.with_access_key_id(access_key_id);
        }

        if let Some(secret_access_key) = &config.secret_access_key {
            builder = builder.with_secret_access_key(secret_access_key);
        }

        if !config.use_ssl {
            builder = builder.with_allow_http(true);
        }

        let client = Arc::new(builder.build()?);

        // Determine base path
        let checkpoint_base_path = config
            .prefix
            .clone()
            .unwrap_or_else(|| "checkpoints".to_string())
            .into();

        Ok(Self {
            config,
            client,
            local_cache: HashMap::new(),
            checkpoint_base_path,
        })
    }

    /// Get S3 path for checkpoint
    fn checkpoint_path(&self, checkpoint_id: u64) -> Path {
        self.checkpoint_base_path
            .child(format!("chk-{:020}", checkpoint_id))
    }

    /// Get S3 path for state file
    pub fn state_path(&self, checkpoint_id: u64, operator_id: &str, state_name: &str) -> Path {
        self.checkpoint_path(checkpoint_id)
            .child("state")
            .child(operator_id)
            .child(format!("{}.json", state_name))
    }

    /// Get S3 path for metadata
    fn metadata_path(&self, checkpoint_id: u64) -> Path {
        self.checkpoint_path(checkpoint_id).child("_metadata.json")
    }

    /// List all checkpoints
    async fn list_checkpoints(&self) -> Result<Vec<u64>, Error> {
        let mut checkpoints = Vec::new();

        // List objects in checkpoint directory
        let mut stream = self.client.list(Some(&self.checkpoint_base_path));

        while let Some(object) = stream.try_next().await? {
            // Extract checkpoint ID from path
            if let Some(name) = object.location.filename() {
                if let Some(rest) = name.strip_prefix("chk-") {
                    if let Ok(id) = rest.parse::<u64>() {
                        checkpoints.push(id);
                    }
                }
            }
        }

        // Sort in descending order (newest first)
        checkpoints.sort_by(|a: &u64, b: &u64| b.cmp(a));
        Ok(checkpoints)
    }

    /// Save state to S3
    async fn save_state(
        &self,
        checkpoint_id: u64,
        operator_id: &str,
        state_name: &str,
        state: &SimpleMemoryState,
    ) -> Result<(), Error> {
        let path = self.state_path(checkpoint_id, operator_id, state_name);

        // Serialize state
        let state_data = serde_json::to_vec(state).map_err(|e| Error::Serialization(e))?;

        // Upload to S3
        self.client
            .put(&path, state_data.into())
            .await
            .map_err(|e| Error::Process(format!("Failed to save state to S3: {}", e)))?;

        Ok(())
    }

    /// Load state from S3
    async fn load_state(
        &self,
        checkpoint_id: u64,
        operator_id: &str,
        state_name: &str,
    ) -> Result<Option<SimpleMemoryState>, Error> {
        let path = self.state_path(checkpoint_id, operator_id, state_name);

        // Try to get from S3
        match self.client.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;

                let state: SimpleMemoryState =
                    serde_json::from_slice(&bytes).map_err(|e| Error::Serialization(e))?;

                Ok(Some(state))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(Error::Process(format!(
                "Failed to load state from S3: {}",
                e
            ))),
        }
    }

    /// Save checkpoint metadata
    async fn save_metadata(
        &self,
        checkpoint_id: u64,
        metadata: &CheckpointMetadata,
    ) -> Result<(), Error> {
        let path = self.metadata_path(checkpoint_id);

        let metadata_data = serde_json::to_vec(metadata).map_err(|e| Error::Serialization(e))?;

        self.client
            .put(&path, metadata_data.into())
            .await
            .map_err(|e| Error::Process(format!("Failed to save metadata to S3: {}", e)))?;

        Ok(())
    }

    /// Load checkpoint metadata
    async fn load_metadata(&self, checkpoint_id: u64) -> Result<Option<CheckpointMetadata>, Error> {
        let path = self.metadata_path(checkpoint_id);

        match self.client.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;

                let metadata: CheckpointMetadata =
                    serde_json::from_slice(&bytes).map_err(|e| Error::Serialization(e))?;

                Ok(Some(metadata))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(Error::Process(format!(
                "Failed to load metadata from S3: {}",
                e
            ))),
        }
    }

    /// Delete a checkpoint
    async fn delete_checkpoint(&self, checkpoint_id: u64) -> Result<(), Error> {
        let checkpoint_path = self.checkpoint_path(checkpoint_id);

        // List all files in checkpoint directory
        let mut stream = self.client.list(Some(&checkpoint_path));

        while let Some(object) = stream.try_next().await? {
            self.client
                .delete(&object.location)
                .await
                .map_err(|e| Error::Process(format!("Failed to delete checkpoint file: {}", e)))?;
        }

        Ok(())
    }
}

/// Checkpoint metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    pub checkpoint_id: u64,
    pub timestamp: u64,
    pub operators: Vec<OperatorStateInfo>,
    pub status: CheckpointStatus,
}

/// Operator state information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorStateInfo {
    pub operator_id: String,
    pub state_names: Vec<String>,
    pub byte_size: u64,
}

/// Checkpoint status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CheckpointStatus {
    InProgress,
    Completed,
    Failed,
}

/// S3 state store
pub struct S3StateStore {
    backend: Arc<S3StateBackend>,
    operator_id: String,
    state_name: String,
    local_state: SimpleMemoryState,
    current_checkpoint_id: Option<u64>,
}

impl S3StateStore {
    /// Create new S3 state store
    pub fn new(backend: Arc<S3StateBackend>, operator_id: String, state_name: String) -> Self {
        Self {
            backend,
            operator_id,
            state_name,
            local_state: SimpleMemoryState::new(),
            current_checkpoint_id: None,
        }
    }

    /// Get local state (for fast access)
    pub fn local_state(&self) -> &SimpleMemoryState {
        &self.local_state
    }

    /// Get mutable local state
    pub fn local_state_mut(&mut self) -> &mut SimpleMemoryState {
        &mut self.local_state
    }

    /// Save current state to S3
    async fn persist_to_s3(&mut self, checkpoint_id: u64) -> Result<(), Error> {
        self.backend
            .save_state(
                checkpoint_id,
                &self.operator_id,
                &self.state_name,
                &self.local_state,
            )
            .await
    }

    /// Load state from S3
    async fn load_from_s3(&mut self, checkpoint_id: u64) -> Result<(), Error> {
        if let Some(state) = self
            .backend
            .load_state(checkpoint_id, &self.operator_id, &self.state_name)
            .await?
        {
            self.local_state = state;
            self.current_checkpoint_id = Some(checkpoint_id);
        }
        Ok(())
    }
}

impl crate::state::StateHelper for S3StateStore {
    fn get_typed<V>(&self, key: &str) -> Result<Option<V>, Error>
    where
        V: for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        self.local_state.get_typed(key)
    }

    fn put_typed<V>(&mut self, key: &str, value: V) -> Result<(), Error>
    where
        V: serde::Serialize + Send + Sync + 'static,
    {
        self.local_state.put_typed(key, value)
    }
}

/// S3 checkpoint coordinator
pub struct S3CheckpointCoordinator {
    backend: Arc<S3StateBackend>,
    active_checkpoints: HashMap<u64, CheckpointInProgress>,
    checkpoint_timeout: std::time::Duration,
}

/// Checkpoint in progress
#[derive(Debug)]
struct CheckpointInProgress {
    pub checkpoint_id: u64,
    pub start_time: std::time::Instant,
    pub participants: Vec<String>,
    pub completed_participants: Vec<String>,
}

impl S3CheckpointCoordinator {
    /// Create new S3 checkpoint coordinator
    pub fn new(backend: Arc<S3StateBackend>) -> Self {
        Self {
            backend,
            active_checkpoints: HashMap::new(),
            checkpoint_timeout: std::time::Duration::from_secs(300), // 5 minutes
        }
    }

    /// Start a new checkpoint
    pub async fn start_checkpoint(&mut self) -> Result<u64, Error> {
        // Get next checkpoint ID
        let checkpoints = self.backend.list_checkpoints().await?;
        let checkpoint_id = checkpoints.first().map_or(1, |id| id + 1);

        // Create metadata
        let metadata = CheckpointMetadata {
            checkpoint_id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            operators: Vec::new(),
            status: CheckpointStatus::InProgress,
        };

        // Save metadata
        self.backend.save_metadata(checkpoint_id, &metadata).await?;

        // Track checkpoint
        self.active_checkpoints.insert(
            checkpoint_id,
            CheckpointInProgress {
                checkpoint_id,
                start_time: std::time::Instant::now(),
                participants: Vec::new(),
                completed_participants: Vec::new(),
            },
        );

        Ok(checkpoint_id)
    }

    /// Register participant for checkpoint
    pub fn register_participant(
        &mut self,
        checkpoint_id: u64,
        participant_id: String,
    ) -> Result<(), Error> {
        if let Some(checkpoint) = self.active_checkpoints.get_mut(&checkpoint_id) {
            if !checkpoint.participants.contains(&participant_id) {
                checkpoint.participants.push(participant_id.clone());
            }
            Ok(())
        } else {
            Err(Error::Process(format!(
                "Checkpoint {} not found",
                checkpoint_id
            )))
        }
    }

    /// Mark participant as completed
    pub async fn complete_participant(
        &mut self,
        checkpoint_id: u64,
        participant_id: &str,
        operator_states: Vec<(String, SimpleMemoryState)>,
    ) -> Result<(), Error> {
        if let Some(checkpoint) = self.active_checkpoints.get_mut(&checkpoint_id) {
            // Save operator states
            for (state_name, state) in operator_states {
                self.backend
                    .save_state(checkpoint_id, participant_id, &state_name, &state)
                    .await?;
            }

            // Mark as completed
            if !checkpoint
                .completed_participants
                .contains(&participant_id.to_string())
            {
                checkpoint
                    .completed_participants
                    .push(participant_id.to_string());
            }

            // Check if all participants completed
            if checkpoint.completed_participants.len() == checkpoint.participants.len() {
                self.complete_checkpoint(checkpoint_id).await?;
            }

            Ok(())
        } else {
            Err(Error::Process(format!(
                "Checkpoint {} not found",
                checkpoint_id
            )))
        }
    }

    /// Complete checkpoint
    async fn complete_checkpoint(&mut self, checkpoint_id: u64) -> Result<(), Error> {
        // Update metadata
        if let Some(mut metadata) = self.backend.load_metadata(checkpoint_id).await? {
            metadata.status = CheckpointStatus::Completed;
            self.backend.save_metadata(checkpoint_id, &metadata).await?;
        }

        // Remove from active checkpoints
        self.active_checkpoints.remove(&checkpoint_id);

        Ok(())
    }

    /// Abort checkpoint
    async fn abort_checkpoint(&mut self, checkpoint_id: u64) -> Result<(), Error> {
        // Update metadata
        if let Some(mut metadata) = self.backend.load_metadata(checkpoint_id).await? {
            metadata.status = CheckpointStatus::Failed;
            self.backend.save_metadata(checkpoint_id, &metadata).await?;
        }

        // Remove from active checkpoints
        self.active_checkpoints.remove(&checkpoint_id);

        Ok(())
    }

    /// Get latest completed checkpoint
    pub async fn get_latest_checkpoint(&self) -> Result<Option<u64>, Error> {
        let checkpoints = self.backend.list_checkpoints().await?;

        for checkpoint_id in checkpoints {
            if let Some(metadata) = self.backend.load_metadata(checkpoint_id).await? {
                if metadata.status == CheckpointStatus::Completed {
                    return Ok(Some(checkpoint_id));
                }
            }
        }

        Ok(None)
    }

    /// Cleanup old checkpoints (keep latest N)
    pub async fn cleanup_old_checkpoints(&self, keep_latest: usize) -> Result<(), Error> {
        let checkpoints = self.backend.list_checkpoints().await?;

        if checkpoints.len() > keep_latest {
            for &checkpoint_id in &checkpoints[keep_latest..] {
                self.backend.delete_checkpoint(checkpoint_id).await?;
            }
        }

        Ok(())
    }
}
