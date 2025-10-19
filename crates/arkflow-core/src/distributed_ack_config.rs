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

//! Distributed acknowledgment configuration
//!
//! This module provides configuration structures for distributed acknowledgment
//! processing with object storage backing.

use crate::checkpoint_manager::CheckpointConfig;
use crate::distributed_wal::DistributedWALConfig;
use crate::node_registry::{CoordinatorType, ObjectStorageCoordinatorConfig};
use crate::object_storage::StorageType;
use crate::recovery_manager::RecoveryConfig;
use serde::{Deserialize, Serialize};

/// Distributed acknowledgment configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedAckConfig {
    /// Enable distributed acknowledgment processing
    pub enabled: bool,
    /// Node identifier
    pub node_id: Option<String>,
    /// Cluster identifier
    pub cluster_id: String,
    /// Distributed WAL configuration
    pub wal: DistributedWALConfig,
    /// Checkpoint configuration
    pub checkpoint: CheckpointConfig,
    /// Recovery configuration
    pub recovery: RecoveryConfig,
    /// Node registry configuration
    pub node_registry: NodeRegistryConfig,
}

impl Default for DistributedAckConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: None,
            cluster_id: "default-cluster".to_string(),
            wal: DistributedWALConfig::default(),
            checkpoint: CheckpointConfig::default(),
            recovery: RecoveryConfig::default(),
            node_registry: NodeRegistryConfig::default(),
        }
    }
}

/// Node registry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegistryConfig {
    /// Coordinator type
    pub coordinator_type: CoordinatorType,
    /// Node information
    pub node_info: NodeInfoConfig,
}

impl Default for NodeRegistryConfig {
    fn default() -> Self {
        Self {
            coordinator_type: CoordinatorType::ObjectStorage(
                ObjectStorageCoordinatorConfig::default(),
            ),
            node_info: NodeInfoConfig::default(),
        }
    }
}

/// Node information configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfoConfig {
    /// Node address
    pub address: Option<String>,
    /// Node port
    pub port: Option<u16>,
    /// Node capabilities
    pub capabilities: Vec<String>,
    /// Additional metadata
    pub metadata: std::collections::HashMap<String, String>,
}

impl Default for NodeInfoConfig {
    fn default() -> Self {
        Self {
            address: None,
            port: None,
            capabilities: vec!["ack_processing".to_string()],
            metadata: std::collections::HashMap::new(),
        }
    }
}

impl DistributedAckConfig {
    /// Create a new distributed acknowledgment configuration with defaults
    pub fn new(cluster_id: String) -> Self {
        Self {
            enabled: true,
            node_id: None,
            cluster_id,
            wal: DistributedWALConfig::default(),
            checkpoint: CheckpointConfig::default(),
            recovery: RecoveryConfig::default(),
            node_registry: NodeRegistryConfig::default(),
        }
    }

    /// Set node ID
    pub fn with_node_id(mut self, node_id: String) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// Set storage backend
    pub fn with_storage(mut self, storage_type: StorageType) -> Self {
        self.wal.storage_type = storage_type.clone();
        self.checkpoint.storage_type = storage_type.clone();
        self.recovery.storage_type = storage_type.clone();
        if let CoordinatorType::ObjectStorage(ref mut config) = self.node_registry.coordinator_type
        {
            config.storage_type = storage_type;
        }
        self
    }

    /// Set base path for object storage
    pub fn with_base_path(mut self, base_path: String) -> Self {
        self.wal.object_storage_base_path = Some(format!("{}/wal", base_path));
        self.checkpoint.base_path = format!("{}/checkpoints", base_path);
        self.recovery.base_path = format!("{}/recovery", base_path);
        if let CoordinatorType::ObjectStorage(ref mut config) = self.node_registry.coordinator_type
        {
            config.base_path = format!("{}/coordinator", base_path);
        }
        self
    }

    /// Set local WAL path
    pub fn with_local_wal_path(mut self, path: String) -> Self {
        self.wal.local_wal_path = path;
        self
    }

    /// Set heartbeat interval
    pub fn with_heartbeat_interval_ms(mut self, interval_ms: u64) -> Self {
        if let CoordinatorType::ObjectStorage(ref mut config) = self.node_registry.coordinator_type
        {
            config.heartbeat_interval_ms = interval_ms;
        }
        self
    }

    /// Set checkpoint interval
    pub fn with_checkpoint_interval_ms(mut self, interval_ms: u64) -> Self {
        self.checkpoint.checkpoint_interval_ms = interval_ms;
        self
    }

    /// Set upload batch size
    pub fn with_upload_batch_size(mut self, batch_size: usize) -> Self {
        self.wal.upload_batch_size = batch_size;
        self
    }

    /// Enable/disable auto recovery
    pub fn with_auto_recovery(mut self, enabled: bool) -> Self {
        self.wal.enable_auto_recovery = enabled;
        self.recovery.auto_recovery = enabled;
        self
    }

    /// Enable/disable compression
    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.checkpoint.enable_compression = enabled;
        self
    }

    /// Set maximum checkpoints to retain
    pub fn with_max_checkpoints(mut self, max_checkpoints: usize) -> Self {
        self.checkpoint.max_checkpoints = max_checkpoints;
        self
    }

    /// Set node timeout
    pub fn with_node_timeout_ms(mut self, timeout_ms: u64) -> Self {
        if let CoordinatorType::ObjectStorage(ref mut config) = self.node_registry.coordinator_type
        {
            config.node_timeout_ms = timeout_ms;
        }
        self
    }

    /// Get the effective node ID
    pub fn get_node_id(&self) -> String {
        self.node_id.clone().unwrap_or_else(|| {
            format!(
                "node-{}",
                uuid::Uuid::new_v4()
                    .to_string()
                    .split('-')
                    .next()
                    .unwrap_or("unknown")
            )
        })
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.enabled {
            if self.cluster_id.is_empty() {
                return Err(
                    "Cluster ID cannot be empty when distributed ack is enabled".to_string()
                );
            }

            if self.wal.upload_batch_size == 0 {
                return Err("Upload batch size must be greater than 0".to_string());
            }

            if self.checkpoint.max_checkpoints == 0 {
                return Err("Max checkpoints must be greater than 0".to_string());
            }

            if self.recovery.recovery_batch_size == 0 {
                return Err("Recovery batch size must be greater than 0".to_string());
            }

            if let CoordinatorType::ObjectStorage(config) = &self.node_registry.coordinator_type {
                if config.heartbeat_interval_ms == 0 {
                    return Err("Heartbeat interval must be greater than 0".to_string());
                }
                if config.node_timeout_ms < config.heartbeat_interval_ms {
                    return Err("Node timeout must be greater than heartbeat interval".to_string());
                }
            }
        }

        Ok(())
    }

    /// Create a configuration for local testing
    pub fn for_local_testing(cluster_id: String) -> Self {
        Self {
            enabled: true,
            node_id: Some(format!(
                "test-node-{}",
                uuid::Uuid::new_v4()
                    .to_string()
                    .split('-')
                    .next()
                    .unwrap_or("unknown")
            )),
            cluster_id,
            wal: DistributedWALConfig {
                node_id: "test-node".to_string(),
                cluster_id: "test-cluster".to_string(),
                storage_type: StorageType::Local(crate::object_storage::LocalConfig {
                    base_path: "./test_distributed_wal".to_string(),
                }),
                local_wal_path: "./test_local_wal".to_string(),
                local_wal_size_limit: 10 * 1024 * 1024, // 10MB for testing
                upload_batch_size: 10,
                upload_interval_ms: 1000, // Faster for testing
                max_retry_attempts: 3,
                enable_auto_recovery: true,
                enable_metrics: true,
                object_storage_base_path: Some("test_wal".to_string()),
            },
            checkpoint: CheckpointConfig {
                storage_type: StorageType::Local(crate::object_storage::LocalConfig {
                    base_path: "./test_checkpoints".to_string(),
                }),
                base_path: "test_checkpoints".to_string(),
                checkpoint_interval_ms: 5000, // Faster for testing
                max_checkpoints: 3,
                auto_checkpoint: true,
                enable_compression: false,
            },
            recovery: RecoveryConfig {
                storage_type: StorageType::Local(crate::object_storage::LocalConfig {
                    base_path: "./test_recovery".to_string(),
                }),
                base_path: "test_recovery".to_string(),
                auto_recovery: true,
                recovery_strategy: crate::recovery_manager::RecoveryStrategy::FromLatestCheckpoint,
                recovery_batch_size: 50,
                enable_consistency_check: true,
                recovery_timeout_ms: 60000, // 1 minute for testing
                enable_deduplication: true,
                duplicate_tracking_age_hours: 1, // Shorter for testing
            },
            node_registry: NodeRegistryConfig {
                coordinator_type: CoordinatorType::ObjectStorage(ObjectStorageCoordinatorConfig {
                    storage_type: StorageType::Local(crate::object_storage::LocalConfig {
                        base_path: "./test_coordinator".to_string(),
                    }),
                    base_path: "test_coordinator".to_string(),
                    heartbeat_interval_ms: 2000, // Faster for testing
                    node_timeout_ms: 10000,      // Faster for testing
                    cleanup_interval_ms: 5000,   // Faster for testing
                }),
                node_info: NodeInfoConfig {
                    address: Some("127.0.0.1".to_string()),
                    port: Some(8080),
                    capabilities: vec!["ack_processing".to_string(), "test".to_string()],
                    metadata: {
                        let mut metadata = std::collections::HashMap::new();
                        metadata.insert("environment".to_string(), "testing".to_string());
                        metadata
                    },
                },
            },
        }
    }

    /// Create a configuration for production use
    pub fn for_production(
        cluster_id: String,
        storage_type: StorageType,
        base_path: String,
    ) -> Self {
        Self {
            enabled: true,
            node_id: None, // Will be auto-generated
            cluster_id,
            wal: DistributedWALConfig {
                node_id: String::new(),    // Will be set later
                cluster_id: String::new(), // Will be set later
                storage_type: storage_type.clone(),
                local_wal_path: "/var/lib/arkflow/local_wal".to_string(),
                local_wal_size_limit: 1024 * 1024 * 1024, // 1GB
                upload_batch_size: 100,
                upload_interval_ms: 30000, // 30 seconds
                max_retry_attempts: 5,
                enable_auto_recovery: true,
                enable_metrics: true,
                object_storage_base_path: Some(format!("{}/wal", base_path)),
            },
            checkpoint: CheckpointConfig {
                storage_type: storage_type.clone(),
                base_path: format!("{}/checkpoints", base_path),
                checkpoint_interval_ms: 300000, // 5 minutes
                max_checkpoints: 10,
                auto_checkpoint: true,
                enable_compression: true,
            },
            recovery: RecoveryConfig {
                storage_type: storage_type.clone(),
                base_path: format!("{}/recovery", base_path),
                auto_recovery: true,
                recovery_strategy: crate::recovery_manager::RecoveryStrategy::FromLatestCheckpoint,
                recovery_batch_size: 1000,
                enable_consistency_check: true,
                recovery_timeout_ms: 300000, // 5 minutes
                enable_deduplication: true,
                duplicate_tracking_age_hours: 48, // 2 days
            },
            node_registry: NodeRegistryConfig {
                coordinator_type: CoordinatorType::ObjectStorage(ObjectStorageCoordinatorConfig {
                    storage_type,
                    base_path: format!("{}/coordinator", base_path),
                    heartbeat_interval_ms: 30000, // 30 seconds
                    node_timeout_ms: 90000,       // 90 seconds
                    cleanup_interval_ms: 60000,   // 60 seconds
                }),
                node_info: NodeInfoConfig {
                    address: None, // Will be detected automatically
                    port: None,    // Will use default
                    capabilities: vec!["ack_processing".to_string()],
                    metadata: {
                        let mut metadata = std::collections::HashMap::new();
                        metadata.insert("environment".to_string(), "production".to_string());
                        metadata
                    },
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = DistributedAckConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.cluster_id, "default-cluster");
    }

    #[test]
    fn test_config_builder() {
        let config = DistributedAckConfig::new("test-cluster".to_string())
            .with_node_id("test-node".to_string())
            .with_upload_batch_size(100)
            .with_checkpoint_interval_ms(60000)
            .with_auto_recovery(true)
            .with_compression(true);

        assert!(config.enabled);
        assert_eq!(config.node_id, Some("test-node".to_string()));
        assert_eq!(config.cluster_id, "test-cluster");
        assert_eq!(config.wal.upload_batch_size, 100);
        assert_eq!(config.checkpoint.checkpoint_interval_ms, 60000);
        assert!(config.wal.enable_auto_recovery);
        assert!(config.checkpoint.enable_compression);
    }

    #[test]
    fn test_config_validation() {
        let mut config = DistributedAckConfig::default();
        config.enabled = true;

        // Should fail with empty cluster ID
        config.cluster_id = String::new();
        assert!(config.validate().is_err());

        // Should pass with valid config
        config.cluster_id = "test-cluster".to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_node_id_generation() {
        let config = DistributedAckConfig::default();
        let node_id = config.get_node_id();
        assert!(!node_id.is_empty());
        assert!(node_id.starts_with("node-"));
    }

    #[test]
    fn test_local_testing_config() {
        let config = DistributedAckConfig::for_local_testing("test-cluster".to_string());
        assert!(config.enabled);
        assert!(config.node_id.is_some());
        assert_eq!(config.cluster_id, "test-cluster");
        assert_eq!(config.wal.upload_batch_size, 10);
        assert_eq!(config.checkpoint.max_checkpoints, 3);
    }

    #[test]
    fn test_production_config() {
        let storage_type = StorageType::Local(crate::object_storage::LocalConfig {
            base_path: "./production".to_string(),
        });

        let config = DistributedAckConfig::for_production(
            "production-cluster".to_string(),
            storage_type,
            "arkflow".to_string(),
        );

        assert!(config.enabled);
        assert_eq!(config.cluster_id, "production-cluster");
        assert_eq!(config.wal.local_wal_size_limit, 1024 * 1024 * 1024);
        assert_eq!(config.checkpoint.max_checkpoints, 10);
        assert!(config.checkpoint.enable_compression);
    }
}
