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

//! Node registry and discovery for distributed WAL
//!
//! This module provides node registration, heartbeat, and discovery mechanisms
//! for distributed WAL coordination.

use crate::object_storage::{create_object_storage, ObjectStorage, StorageType};
use crate::Error;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// Node information structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub cluster_id: String,
    pub address: Option<String>,
    pub port: Option<u16>,
    pub last_heartbeat: SystemTime,
    pub status: NodeStatus,
    pub capabilities: HashSet<String>,
    pub metadata: HashMap<String, String>,
    pub started_at: SystemTime,
}

/// Node status enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeStatus {
    /// Node is starting up
    Starting,
    /// Node is healthy and active
    Active,
    /// Node is degraded but still functional
    Degraded,
    /// Node is shutting down
    ShuttingDown,
    /// Node is dead/unresponsive
    Dead,
}

impl Default for NodeStatus {
    fn default() -> Self {
        NodeStatus::Starting
    }
}

/// Coordinator configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CoordinatorType {
    /// Object storage based coordination
    ObjectStorage(ObjectStorageCoordinatorConfig),
    /// In-memory coordination (for testing)
    InMemory,
    /// ZooKeeper based coordination
    ZooKeeper(ZooKeeperConfig),
}

/// Object storage coordinator configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectStorageCoordinatorConfig {
    pub storage_type: StorageType,
    pub base_path: String,
    pub heartbeat_interval_ms: u64,
    pub node_timeout_ms: u64,
    pub cleanup_interval_ms: u64,
}

impl Default for ObjectStorageCoordinatorConfig {
    fn default() -> Self {
        Self {
            storage_type: StorageType::Local(crate::object_storage::LocalConfig {
                base_path: "./coordinator".to_string(),
            }),
            base_path: "coordinator".to_string(),
            heartbeat_interval_ms: 30000, // 30 seconds
            node_timeout_ms: 90000,       // 90 seconds
            cleanup_interval_ms: 60000,   // 60 seconds
        }
    }
}

/// ZooKeeper coordinator configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZooKeeperConfig {
    pub servers: Vec<String>,
    pub base_path: String,
    pub session_timeout_ms: u64,
    pub connection_timeout_ms: u64,
}

/// Node registry trait
#[async_trait]
pub trait NodeRegistry: Send + Sync {
    /// Register a new node in the cluster
    async fn register_node(&self, node_info: NodeInfo) -> Result<(), Error>;

    /// Update node heartbeat
    async fn update_heartbeat(&self, node_id: &str) -> Result<(), Error>;

    /// Unregister a node from the cluster
    async fn unregister_node(&self, node_id: &str) -> Result<(), Error>;

    /// Get information about a specific node
    async fn get_node_info(&self, node_id: &str) -> Result<Option<NodeInfo>, Error>;

    /// Get all active nodes in the cluster
    async fn get_active_nodes(&self) -> Result<Vec<NodeInfo>, Error>;

    /// Get all nodes in the cluster (including inactive)
    async fn get_all_nodes(&self) -> Result<Vec<NodeInfo>, Error>;

    /// Check if a node is still alive
    async fn is_node_alive(&self, node_id: &str) -> Result<bool, Error>;

    /// Get cluster membership information
    async fn get_cluster_info(&self) -> Result<ClusterInfo, Error>;
}

/// Cluster information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub cluster_id: String,
    pub total_nodes: usize,
    pub active_nodes: usize,
    pub coordinator_type: String,
    pub last_updated: SystemTime,
}

/// Object storage based node registry implementation
pub struct ObjectStorageNodeRegistry {
    cluster_id: String,
    object_storage: Arc<dyn ObjectStorage>,
    base_path: String,
    node_timeout: Duration,
    local_nodes: Arc<RwLock<HashMap<String, NodeInfo>>>,
}

impl ObjectStorageNodeRegistry {
    /// Create a new object storage based node registry
    pub async fn new(
        cluster_id: String,
        config: ObjectStorageCoordinatorConfig,
    ) -> Result<Self, Error> {
        let object_storage = create_object_storage(&config.storage_type).await?;

        let registry = Self {
            cluster_id: cluster_id.clone(),
            object_storage,
            base_path: config.base_path,
            node_timeout: Duration::from_millis(config.node_timeout_ms),
            local_nodes: Arc::new(RwLock::new(HashMap::new())),
        };

        // Initialize cluster if it doesn't exist
        registry.initialize_cluster().await?;

        Ok(registry)
    }

    /// Initialize cluster metadata
    async fn initialize_cluster(&self) -> Result<(), Error> {
        let cluster_info_key = format!("{}/cluster_info.json", self.base_path);

        if !self.object_storage.exists(&cluster_info_key).await? {
            let cluster_info = ClusterInfo {
                cluster_id: self.cluster_id.clone(),
                total_nodes: 0,
                active_nodes: 0,
                coordinator_type: "ObjectStorage".to_string(),
                last_updated: SystemTime::now(),
            };

            let data = serde_json::to_vec(&cluster_info)
                .map_err(|e| Error::Unknown(format!("Failed to serialize cluster info: {}", e)))?;

            self.object_storage
                .put_object(&cluster_info_key, data)
                .await?;
            info!("Initialized cluster: {}", self.cluster_id);
        }

        Ok(())
    }

    /// Get node key in object storage
    fn get_node_key(&self, node_id: &str) -> String {
        format!("{}/nodes/{}.json", self.base_path, node_id)
    }

    /// Load nodes from object storage
    async fn load_nodes(&self) -> Result<Vec<NodeInfo>, Error> {
        let nodes_prefix = format!("{}/nodes/", self.base_path);
        let mut nodes = Vec::new();

        match self.object_storage.list_objects(&nodes_prefix).await {
            Ok(objects) => {
                for object in objects {
                    if object.key.ends_with(".json") {
                        match self.object_storage.get_object(&object.key).await {
                            Ok(data) => match serde_json::from_slice::<NodeInfo>(&data) {
                                Ok(node) => {
                                    nodes.push(node);
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to deserialize node info from {}: {}",
                                        object.key, e
                                    );
                                }
                            },
                            Err(e) => {
                                error!("Failed to download node info from {}: {}", object.key, e);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to list nodes: {}", e);
            }
        }

        Ok(nodes)
    }

    /// Save node info to object storage
    async fn save_node(&self, node_info: &NodeInfo) -> Result<(), Error> {
        let node_key = self.get_node_key(&node_info.node_id);
        let data = serde_json::to_vec(node_info)
            .map_err(|e| Error::Unknown(format!("Failed to serialize node info: {}", e)))?;

        self.object_storage.put_object(&node_key, data).await?;
        Ok(())
    }

    /// Delete node from object storage
    async fn delete_node(&self, node_id: &str) -> Result<(), Error> {
        let node_key = self.get_node_key(node_id);
        self.object_storage.delete_object(&node_key).await?;
        Ok(())
    }

    /// Update cluster information
    async fn update_cluster_info(&self) -> Result<(), Error> {
        let nodes = self.load_nodes().await?;
        let active_nodes = nodes
            .iter()
            .filter(|node| self.is_node_alive_internal(node))
            .count();

        let cluster_info = ClusterInfo {
            cluster_id: self.cluster_id.clone(),
            total_nodes: nodes.len(),
            active_nodes,
            coordinator_type: "ObjectStorage".to_string(),
            last_updated: SystemTime::now(),
        };

        let cluster_info_key = format!("{}/cluster_info.json", self.base_path);
        let data = serde_json::to_vec(&cluster_info)
            .map_err(|e| Error::Unknown(format!("Failed to serialize cluster info: {}", e)))?;

        self.object_storage
            .put_object(&cluster_info_key, data)
            .await?;
        Ok(())
    }

    /// Check if node is alive (internal implementation)
    fn is_node_alive_internal(&self, node: &NodeInfo) -> bool {
        match node.last_heartbeat.duration_since(SystemTime::now()) {
            Ok(duration) => duration <= self.node_timeout,
            Err(_) => false,
        }
    }

    /// Cleanup dead nodes
    pub async fn cleanup_dead_nodes(&self) -> Result<usize, Error> {
        let nodes = self.load_nodes().await?;
        let mut removed_count = 0;

        for node in nodes {
            if !self.is_node_alive_internal(&node) {
                debug!("Removing dead node: {}", node.node_id);
                if let Err(e) = self.delete_node(&node.node_id).await {
                    error!("Failed to delete dead node {}: {}", node.node_id, e);
                } else {
                    removed_count += 1;
                }
            }
        }

        if removed_count > 0 {
            self.update_cluster_info().await?;
            info!("Cleaned up {} dead nodes", removed_count);
        }

        Ok(removed_count)
    }
}

#[async_trait]
impl NodeRegistry for ObjectStorageNodeRegistry {
    async fn register_node(&self, node_info: NodeInfo) -> Result<(), Error> {
        info!(
            "Registering node: {} in cluster: {}",
            node_info.node_id, self.cluster_id
        );

        // Save to object storage
        self.save_node(&node_info).await?;

        // Update local cache
        {
            let mut nodes = self.local_nodes.write().await;
            nodes.insert(node_info.node_id.clone(), node_info.clone());
        }

        // Update cluster info
        self.update_cluster_info().await?;

        Ok(())
    }

    async fn update_heartbeat(&self, node_id: &str) -> Result<(), Error> {
        // Update in object storage
        let mut nodes = self.load_nodes().await?;
        if let Some(node) = nodes.iter_mut().find(|n| n.node_id == node_id) {
            node.last_heartbeat = SystemTime::now();
            node.status = NodeStatus::Active;
            self.save_node(node).await?;
        }

        // Update local cache
        {
            let mut local_nodes = self.local_nodes.write().await;
            if let Some(node) = local_nodes.get_mut(node_id) {
                node.last_heartbeat = SystemTime::now();
                node.status = NodeStatus::Active;
            }
        }

        debug!("Updated heartbeat for node: {}", node_id);
        Ok(())
    }

    async fn unregister_node(&self, node_id: &str) -> Result<(), Error> {
        info!("Unregistering node: {}", node_id);

        // Delete from object storage
        self.delete_node(node_id).await?;

        // Remove from local cache
        {
            let mut nodes = self.local_nodes.write().await;
            nodes.remove(node_id);
        }

        // Update cluster info
        self.update_cluster_info().await?;

        Ok(())
    }

    async fn get_node_info(&self, node_id: &str) -> Result<Option<NodeInfo>, Error> {
        // Check local cache first
        {
            let nodes = self.local_nodes.read().await;
            if let Some(node) = nodes.get(node_id) {
                return Ok(Some(node.clone()));
            }
        }

        // Load from object storage
        let nodes = self.load_nodes().await?;
        let node = nodes.into_iter().find(|n| n.node_id == node_id);

        if let Some(ref node) = node {
            // Update local cache
            let mut local_nodes = self.local_nodes.write().await;
            local_nodes.insert(node_id.to_string(), node.clone());
        }

        Ok(node)
    }

    async fn get_active_nodes(&self) -> Result<Vec<NodeInfo>, Error> {
        let nodes = self.load_nodes().await?;
        let active_nodes: Vec<NodeInfo> = nodes
            .into_iter()
            .filter(|node| self.is_node_alive_internal(node))
            .collect();

        // Update local cache
        {
            let mut local_nodes = self.local_nodes.write().await;
            for node in active_nodes.iter() {
                local_nodes.insert(node.node_id.clone(), node.clone());
            }
        }

        Ok(active_nodes)
    }

    async fn get_all_nodes(&self) -> Result<Vec<NodeInfo>, Error> {
        let nodes = self.load_nodes().await?;

        // Update local cache
        {
            let mut local_nodes = self.local_nodes.write().await;
            for node in &nodes {
                local_nodes.insert(node.node_id.clone(), node.clone());
            }
        }

        Ok(nodes)
    }

    async fn is_node_alive(&self, node_id: &str) -> Result<bool, Error> {
        if let Some(node) = self.get_node_info(node_id).await? {
            Ok(self.is_node_alive_internal(&node))
        } else {
            Ok(false)
        }
    }

    async fn get_cluster_info(&self) -> Result<ClusterInfo, Error> {
        let cluster_info_key = format!("{}/cluster_info.json", self.base_path);

        match self.object_storage.get_object(&cluster_info_key).await {
            Ok(data) => {
                let cluster_info = serde_json::from_slice(&data).map_err(|e| {
                    Error::Unknown(format!("Failed to deserialize cluster info: {}", e))
                })?;
                Ok(cluster_info)
            }
            Err(e) => Err(Error::Unknown(format!(
                "Failed to load cluster info: {}",
                e
            ))),
        }
    }
}

/// Node registry manager with automatic heartbeat
pub struct NodeRegistryManager {
    node_id: String,
    registry: Arc<dyn NodeRegistry>,
    cancellation_token: CancellationToken,
    heartbeat_interval: Duration,
    task_tracker: tokio_util::task::TaskTracker,
}

impl NodeRegistryManager {
    /// Create a new node registry manager
    pub async fn new(
        node_id: String,
        registry: Arc<dyn NodeRegistry>,
        coordinator_config: ObjectStorageCoordinatorConfig,
    ) -> Result<Self, Error> {
        let cancellation_token = CancellationToken::new();
        let heartbeat_interval = Duration::from_millis(coordinator_config.heartbeat_interval_ms);

        Ok(Self {
            node_id,
            registry,
            cancellation_token,
            heartbeat_interval,
            task_tracker: tokio_util::task::TaskTracker::new(),
        })
    }

    /// Register this node and start heartbeat
    pub async fn start(&self, node_info: NodeInfo) -> Result<(), Error> {
        info!("Starting node registry manager for node: {}", self.node_id);

        // Register the node
        self.registry.register_node(node_info).await?;

        // Start heartbeat task
        let registry = self.registry.clone();
        let node_id = self.node_id.clone();
        let cancellation_token = self.cancellation_token.clone();
        let heartbeat_interval = self.heartbeat_interval;

        self.task_tracker.spawn(async move {
            Self::heartbeat_task(registry, node_id, cancellation_token, heartbeat_interval).await;
        });

        Ok(())
    }

    /// Heartbeat task
    async fn heartbeat_task(
        registry: Arc<dyn NodeRegistry>,
        node_id: String,
        cancellation_token: CancellationToken,
        interval: Duration,
    ) {
        info!("Starting heartbeat task for node: {}", node_id);

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                }
                _ = tokio::time::sleep(interval) => {
                    if let Err(e) = registry.update_heartbeat(&node_id).await {
                        error!("Failed to update heartbeat for node {}: {}", node_id, e);
                    } else {
                        debug!("Heartbeat updated for node: {}", node_id);
                    }
                }
            }
        }

        info!("Heartbeat task stopped for node: {}", node_id);
    }

    /// Stop the registry manager
    pub async fn stop(self) -> Result<(), Error> {
        info!("Stopping node registry manager for node: {}", self.node_id);

        // Cancel heartbeat task
        self.cancellation_token.cancel();

        // Unregister node
        self.registry.unregister_node(&self.node_id).await?;

        // Wait for tasks to complete
        self.task_tracker.close();
        self.task_tracker.wait().await;

        Ok(())
    }

    /// Get a reference to the node registry
    pub fn registry(&self) -> &Arc<dyn NodeRegistry> {
        &self.registry
    }

    /// Get the node ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

/// Factory function to create node registry
pub async fn create_node_registry(
    coordinator_type: CoordinatorType,
    cluster_id: String,
) -> Result<Arc<dyn NodeRegistry>, Error> {
    match coordinator_type {
        CoordinatorType::ObjectStorage(config) => {
            let registry = ObjectStorageNodeRegistry::new(cluster_id, config).await?;
            Ok(Arc::new(registry))
        }
        CoordinatorType::InMemory => {
            let registry = InMemoryNodeRegistry::new(cluster_id).await?;
            Ok(Arc::new(registry))
        }
        CoordinatorType::ZooKeeper(_config) => {
            return Err(Error::Unknown(
                "ZooKeeper coordinator not yet implemented".to_string(),
            ));
        }
    }
}

/// In-memory node registry (for testing)
pub struct InMemoryNodeRegistry {
    cluster_id: String,
    nodes: Arc<RwLock<HashMap<String, NodeInfo>>>,
}

impl InMemoryNodeRegistry {
    pub async fn new(cluster_id: String) -> Result<Self, Error> {
        Ok(Self {
            cluster_id,
            nodes: Arc::new(RwLock::new(HashMap::new())),
        })
    }
}

#[async_trait]
impl NodeRegistry for InMemoryNodeRegistry {
    async fn register_node(&self, node_info: NodeInfo) -> Result<(), Error> {
        let mut nodes = self.nodes.write().await;
        nodes.insert(node_info.node_id.clone(), node_info);
        Ok(())
    }

    async fn update_heartbeat(&self, node_id: &str) -> Result<(), Error> {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.last_heartbeat = SystemTime::now();
            node.status = NodeStatus::Active;
        }
        Ok(())
    }

    async fn unregister_node(&self, node_id: &str) -> Result<(), Error> {
        let mut nodes = self.nodes.write().await;
        nodes.remove(node_id);
        Ok(())
    }

    async fn get_node_info(&self, node_id: &str) -> Result<Option<NodeInfo>, Error> {
        let nodes = self.nodes.read().await;
        Ok(nodes.get(node_id).cloned())
    }

    async fn get_active_nodes(&self) -> Result<Vec<NodeInfo>, Error> {
        let nodes = self.nodes.read().await;
        let now = SystemTime::now();
        let active_nodes = nodes
            .values()
            .filter(|node| {
                node.last_heartbeat
                    .duration_since(now)
                    .map(|d| d.as_secs() < 90)
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        Ok(active_nodes)
    }

    async fn get_all_nodes(&self) -> Result<Vec<NodeInfo>, Error> {
        let nodes = self.nodes.read().await;
        Ok(nodes.values().cloned().collect())
    }

    async fn is_node_alive(&self, node_id: &str) -> Result<bool, Error> {
        let nodes = self.nodes.read().await;
        if let Some(node) = nodes.get(node_id) {
            let now = SystemTime::now();
            Ok(node
                .last_heartbeat
                .duration_since(now)
                .map(|d| d.as_secs() < 90)
                .unwrap_or(false))
        } else {
            Ok(false)
        }
    }

    async fn get_cluster_info(&self) -> Result<ClusterInfo, Error> {
        let nodes = self.nodes.read().await;
        let now = SystemTime::now();
        let active_nodes = nodes
            .values()
            .filter(|node| {
                node.last_heartbeat
                    .duration_since(now)
                    .map(|d| d.as_secs() < 90)
                    .unwrap_or(false)
            })
            .count();

        Ok(ClusterInfo {
            cluster_id: self.cluster_id.clone(),
            total_nodes: nodes.len(),
            active_nodes,
            coordinator_type: "InMemory".to_string(),
            last_updated: now,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_in_memory_node_registry() {
        let registry = InMemoryNodeRegistry::new("test-cluster".to_string())
            .await
            .unwrap();

        let node_info = NodeInfo {
            node_id: "test-node".to_string(),
            cluster_id: "test-cluster".to_string(),
            address: Some("127.0.0.1".to_string()),
            port: Some(8080),
            last_heartbeat: SystemTime::now(),
            status: NodeStatus::Active,
            capabilities: HashSet::new(),
            metadata: HashMap::new(),
            started_at: SystemTime::now(),
        };

        // Test registration
        registry.register_node(node_info.clone()).await.unwrap();

        // Test retrieval
        let retrieved = registry.get_node_info("test-node").await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().node_id, "test-node");

        // Test active nodes
        let active_nodes = registry.get_active_nodes().await.unwrap();
        assert_eq!(active_nodes.len(), 1);

        // Test node alive check
        assert!(registry.is_node_alive("test-node").await.unwrap());

        // Test unregistration
        registry.unregister_node("test-node").await.unwrap();
        assert!(registry.get_node_info("test-node").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_object_storage_node_registry() {
        let temp_dir = TempDir::new().unwrap();

        let config = ObjectStorageCoordinatorConfig {
            storage_type: StorageType::Local(crate::object_storage::LocalConfig {
                base_path: temp_dir.path().to_string_lossy().to_string(),
            }),
            base_path: "coordinator".to_string(),
            heartbeat_interval_ms: 1000,
            node_timeout_ms: 5000,
            cleanup_interval_ms: 2000,
        };

        let registry = ObjectStorageNodeRegistry::new("test-cluster".to_string(), config)
            .await
            .unwrap();

        let node_info = NodeInfo {
            node_id: "test-node".to_string(),
            cluster_id: "test-cluster".to_string(),
            address: Some("127.0.0.1".to_string()),
            port: Some(8080),
            last_heartbeat: SystemTime::now(),
            status: NodeStatus::Active,
            capabilities: HashSet::new(),
            metadata: HashMap::new(),
            started_at: SystemTime::now(),
        };

        // Test registration
        registry.register_node(node_info.clone()).await.unwrap();

        // Test retrieval
        let retrieved = registry.get_node_info("test-node").await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().node_id, "test-node");

        // Test cluster info
        let cluster_info = registry.get_cluster_info().await.unwrap();
        assert_eq!(cluster_info.cluster_id, "test-cluster");
        assert_eq!(cluster_info.total_nodes, 1);
        assert_eq!(cluster_info.active_nodes, 1);

        // Test heartbeat update
        registry.update_heartbeat("test-node").await.unwrap();
    }
}
