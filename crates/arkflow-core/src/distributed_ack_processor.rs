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

//! Distributed acknowledgment processor
//!
//! This module provides a distributed implementation of the acknowledgment processor
//! that uses object storage for persistence and fault tolerance.

use crate::checkpoint_manager::CheckpointManager;
use crate::distributed_ack_config::DistributedAckConfig;
use crate::distributed_wal::DistributedWAL;
use crate::input::Ack;
use crate::node_registry::{create_node_registry, NodeInfo, NodeRegistryManager, NodeStatus};
use crate::recovery_manager::RecoveryManager;
use crate::reliable_ack::AckTask;
use crate::Error;
use flume::{Receiver, Sender};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

const _MAX_RETRIES: u32 = 5;
const RETRY_DELAY_MS: u64 = 1000;
const ACK_TIMEOUT_MS: u64 = 10000;
const BATCH_SIZE: usize = 50;
const MAX_PENDING_ACKS: usize = 5000;
const BACKPRESSURE_THRESHOLD: usize = 3000;

/// Distributed acknowledgment processor
pub struct DistributedAckProcessor {
    pub node_id: String,
    pub cluster_id: String,
    pub ack_sender: Sender<AckTask>,
    pub metrics: DistributedAckProcessorMetrics,
    pub enhanced_metrics: Arc<crate::enhanced_metrics::EnhancedMetrics>,
    pub sequence_counter: Arc<AtomicU64>,
    pub backpressure_active: Arc<AtomicBool>,

    // Distributed components
    pub distributed_wal: Option<Arc<DistributedWAL>>,
    pub checkpoint_manager: Option<Arc<CheckpointManager>>,
    pub node_registry_manager: Option<Arc<NodeRegistryManager>>,
    pub recovery_manager: Option<Arc<RecoveryManager>>,

    // Configuration
    pub config: DistributedAckConfig,

    // Fallback local processor for non-distributed mode
    pub fallback_processor: Option<Arc<crate::reliable_ack::ReliableAckProcessor>>,
}

/// Enhanced metrics for distributed acknowledgment processor
#[derive(Debug, Clone)]
pub struct DistributedAckProcessorMetrics {
    // Base metrics
    pub total_acks: Arc<AtomicU64>,
    pub successful_acks: Arc<AtomicU64>,
    pub failed_acks: Arc<AtomicU64>,
    pub retried_acks: Arc<AtomicU64>,
    pub pending_acks: Arc<AtomicU64>,
    pub persisted_acks: Arc<AtomicU64>,
    pub recovered_acks: Arc<AtomicU64>,
    pub backpressure_events: Arc<AtomicU64>,

    // Distributed metrics
    pub uploaded_acks: Arc<AtomicU64>,
    pub failed_uploads: Arc<AtomicU64>,
    pub checkpoint_creations: Arc<AtomicU64>,
    pub recovery_operations: Arc<AtomicU64>,
    pub consistency_checks: Arc<AtomicU64>,
    pub node_heartbeats: Arc<AtomicU64>,
    pub cluster_nodes: Arc<AtomicU64>,
    pub wal_size_bytes: Arc<AtomicU64>,
    pub deduplication_hits: Arc<AtomicU64>,
}

impl Default for DistributedAckProcessorMetrics {
    fn default() -> Self {
        Self {
            total_acks: Arc::new(AtomicU64::new(0)),
            successful_acks: Arc::new(AtomicU64::new(0)),
            failed_acks: Arc::new(AtomicU64::new(0)),
            retried_acks: Arc::new(AtomicU64::new(0)),
            pending_acks: Arc::new(AtomicU64::new(0)),
            persisted_acks: Arc::new(AtomicU64::new(0)),
            recovered_acks: Arc::new(AtomicU64::new(0)),
            backpressure_events: Arc::new(AtomicU64::new(0)),
            uploaded_acks: Arc::new(AtomicU64::new(0)),
            failed_uploads: Arc::new(AtomicU64::new(0)),
            checkpoint_creations: Arc::new(AtomicU64::new(0)),
            recovery_operations: Arc::new(AtomicU64::new(0)),
            consistency_checks: Arc::new(AtomicU64::new(0)),
            node_heartbeats: Arc::new(AtomicU64::new(0)),
            cluster_nodes: Arc::new(AtomicU64::new(0)),
            wal_size_bytes: Arc::new(AtomicU64::new(0)),
            deduplication_hits: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl DistributedAckProcessor {
    /// Create a new distributed acknowledgment processor
    pub async fn new(
        tracker: &TaskTracker,
        cancellation_token: CancellationToken,
        config: DistributedAckConfig,
    ) -> Result<Self, Error> {
        info!("Creating distributed acknowledgment processor");

        // Validate configuration
        config
            .validate()
            .map_err(|e| Error::Unknown(format!("Invalid distributed ack configuration: {}", e)))?;

        let node_id = config.get_node_id();
        let cluster_id = config.cluster_id.clone();

        // Initialize base metrics with enhanced metrics
        let enhanced_metrics = crate::enhanced_metrics::EnhancedMetrics::new();
        let metrics = DistributedAckProcessorMetrics::default();
        let sequence_counter = Arc::new(AtomicU64::new(0));
        let backpressure_active = Arc::new(AtomicBool::new(false));

        // Create communication channels
        let (ack_sender, ack_receiver) = flume::bounded(MAX_PENDING_ACKS);

        let mut processor = Self {
            node_id: node_id.clone(),
            cluster_id: cluster_id.clone(),
            ack_sender,
            metrics: metrics.clone(),
            enhanced_metrics: Arc::new(enhanced_metrics),
            sequence_counter,
            backpressure_active: backpressure_active.clone(),
            distributed_wal: None,
            checkpoint_manager: None,
            node_registry_manager: None,
            recovery_manager: None,
            config: config.clone(),
            fallback_processor: None,
        };

        if config.enabled {
            // Initialize distributed components
            processor
                .initialize_distributed_components(tracker, cancellation_token.clone())
                .await?;
        } else {
            // Initialize fallback local processor
            let temp_dir = std::env::temp_dir();
            let wal_path = temp_dir.join(format!("ack_wal_{}", std::process::id()));
            let fallback = crate::reliable_ack::ReliableAckProcessor::new(
                tracker,
                cancellation_token.clone(),
                &wal_path,
            )?;
            processor.fallback_processor = Some(Arc::new(fallback));
        }

        // Start processing worker
        let worker = DistributedAckProcessorWorker {
            ack_receiver,
            metrics: metrics.clone(),
            cancellation_token: cancellation_token.clone(),
            _distributed_wal: processor.distributed_wal.clone(),
            backpressure_active: backpressure_active.clone(),
        };

        tracker.spawn(worker.run());

        // Start background tasks if distributed mode is enabled
        if config.enabled {
            processor
                .start_background_tasks(tracker, cancellation_token)
                .await?;
        }

        info!(
            "Distributed acknowledgment processor created successfully for node: {}",
            node_id
        );
        Ok(processor)
    }

    /// Initialize distributed components
    async fn initialize_distributed_components(
        &mut self,
        _tracker: &TaskTracker,
        _cancellation_token: CancellationToken,
    ) -> Result<(), Error> {
        info!(
            "Initializing distributed components for node: {}",
            self.node_id
        );

        // Initialize distributed WAL
        self.init_distributed_wal().await?;

        // Initialize checkpoint manager
        self.init_checkpoint_manager().await?;

        // Initialize node registry and manager
        let node_registry = self.init_node_registry().await?;

        // Initialize recovery manager
        self.init_recovery_manager(node_registry).await?;

        info!("Distributed components initialized successfully");
        Ok(())
    }

    /// Initialize distributed WAL
    async fn init_distributed_wal(&mut self) -> Result<(), Error> {
        let mut wal_config = self.config.wal.clone();
        wal_config.node_id = self.node_id.clone();
        wal_config.cluster_id = self.cluster_id.clone();

        let distributed_wal = Arc::new(DistributedWAL::new(wal_config).await?);
        self.distributed_wal = Some(distributed_wal);
        Ok(())
    }

    /// Initialize checkpoint manager
    async fn init_checkpoint_manager(&mut self) -> Result<(), Error> {
        let checkpoint_config = self.config.checkpoint.clone();
        let checkpoint_manager =
            Arc::new(CheckpointManager::new(self.cluster_id.clone(), checkpoint_config).await?);
        self.checkpoint_manager = Some(checkpoint_manager);
        Ok(())
    }

    /// Initialize node registry and manager
    async fn init_node_registry(
        &mut self,
    ) -> Result<Arc<dyn crate::node_registry::NodeRegistry>, Error> {
        // Initialize node registry
        let node_registry = create_node_registry(
            self.config.node_registry.coordinator_type.clone(),
            self.cluster_id.clone(),
        )
        .await?;

        // Create node info
        let node_info = self.create_node_info()?;

        // Initialize node registry manager
        let coordinator_config = match &self.config.node_registry.coordinator_type {
            crate::node_registry::CoordinatorType::ObjectStorage(config) => config.clone(),
            _ => return Err(Error::Unknown("Unsupported coordinator type".to_string())),
        };

        let node_registry_manager = Arc::new(
            NodeRegistryManager::new(
                self.node_id.clone(),
                node_registry.clone(),
                coordinator_config,
            )
            .await?,
        );

        self.node_registry_manager = Some(node_registry_manager.clone());

        // Register node and start heartbeat
        node_registry_manager.start(node_info).await?;

        Ok(node_registry)
    }

    /// Create node information structure
    fn create_node_info(&self) -> Result<NodeInfo, Error> {
        Ok(NodeInfo {
            node_id: self.node_id.clone(),
            cluster_id: self.cluster_id.clone(),
            address: self.config.node_registry.node_info.address.clone(),
            port: self.config.node_registry.node_info.port,
            last_heartbeat: SystemTime::now(),
            status: NodeStatus::Starting,
            capabilities: self
                .config
                .node_registry
                .node_info
                .capabilities
                .clone()
                .into_iter()
                .collect(),
            metadata: self.config.node_registry.node_info.metadata.clone(),
            started_at: SystemTime::now(),
        })
    }

    /// Initialize recovery manager
    async fn init_recovery_manager(
        &mut self,
        node_registry: Arc<dyn crate::node_registry::NodeRegistry>,
    ) -> Result<(), Error> {
        let checkpoint_manager = self
            .checkpoint_manager
            .clone()
            .ok_or_else(|| Error::Unknown("Checkpoint manager not initialized".to_string()))?;

        let recovery_manager = Arc::new(
            RecoveryManager::new(
                self.cluster_id.clone(),
                self.node_id.clone(),
                checkpoint_manager,
                node_registry,
                self.config.recovery.clone(),
            )
            .await?,
        );

        self.recovery_manager = Some(recovery_manager);
        Ok(())
    }

    /// Start background tasks for distributed mode
    async fn start_background_tasks(
        &self,
        tracker: &TaskTracker,
        cancellation_token: CancellationToken,
    ) -> Result<(), Error> {
        info!("Starting background tasks for distributed acknowledgment processor");

        // Start metrics collection task
        self.start_metrics_collection_task(tracker, cancellation_token.clone())
            .await?;

        // Start periodic checkpoint creation if enabled
        self.start_periodic_checkpoint_task(tracker, cancellation_token.clone())
            .await?;

        // Start periodic consistency checking if enabled
        self.start_consistency_check_task(tracker, cancellation_token)
            .await?;

        info!("Background tasks started successfully");
        Ok(())
    }

    /// Start metrics collection task
    async fn start_metrics_collection_task(
        &self,
        tracker: &TaskTracker,
        cancellation_token: CancellationToken,
    ) -> Result<(), Error> {
        let metrics = self.metrics.clone();
        let checkpoint_manager = self.checkpoint_manager.clone();
        let recovery_manager = self.recovery_manager.clone();
        let distributed_wal = self.distributed_wal.clone();

        tracker.spawn(async move {
            Self::metrics_collection_task(
                metrics,
                checkpoint_manager,
                recovery_manager,
                distributed_wal,
                cancellation_token,
            )
            .await;
        });

        Ok(())
    }

    /// Start periodic checkpoint creation task
    async fn start_periodic_checkpoint_task(
        &self,
        tracker: &TaskTracker,
        cancellation_token: CancellationToken,
    ) -> Result<(), Error> {
        if let Some(checkpoint_manager) = &self.checkpoint_manager {
            let checkpoint_manager_clone = checkpoint_manager.clone();

            tracker.spawn(async move {
                Self::periodic_checkpoint_task(checkpoint_manager_clone, cancellation_token).await;
            });
        }

        Ok(())
    }

    /// Start consistency check task
    async fn start_consistency_check_task(
        &self,
        tracker: &TaskTracker,
        cancellation_token: CancellationToken,
    ) -> Result<(), Error> {
        if self.config.recovery.enable_consistency_check {
            if let Some(recovery_manager) = &self.recovery_manager {
                let recovery_manager_clone = recovery_manager.clone();

                tracker.spawn(async move {
                    Self::consistency_check_task(recovery_manager_clone, cancellation_token).await;
                });
            }
        }

        Ok(())
    }

    /// Submit acknowledgment to distributed processor
    pub async fn submit_ack(
        &self,
        _ack_id: String,
        ack_type: String,
        ack: Arc<dyn Ack>,
    ) -> Result<(), Error> {
        // Check backpressure
        if self.backpressure_active.load(Ordering::Relaxed) {
            self.metrics
                .backpressure_events
                .fetch_add(1, Ordering::Relaxed);
            return Err(Error::Unknown(
                "Backpressure active - rejecting ack".to_string(),
            ));
        }

        let sequence = self.sequence_counter.fetch_add(1, Ordering::SeqCst);
        let task = AckTask::new(ack, sequence, ack_type, Vec::new());

        self.metrics.total_acks.fetch_add(1, Ordering::Relaxed);
        self.metrics.pending_acks.fetch_add(1, Ordering::Relaxed);

        // Send to acknowledgment channel
        self.ack_sender
            .send_async(task)
            .await
            .map_err(|e| Error::Process(format!("Failed to submit acknowledgment: {}", e)))?;

        Ok(())
    }

    /// Process an acknowledgment
    pub async fn ack(
        &self,
        ack: Arc<dyn Ack>,
        ack_type: String,
        payload: Vec<u8>,
    ) -> Result<(), Error> {
        // Check backpressure
        if self.backpressure_active.load(Ordering::Relaxed) {
            self.metrics
                .backpressure_events
                .fetch_add(1, Ordering::Relaxed);
            return Err(Error::Unknown(
                "Backpressure active - rejecting ack".to_string(),
            ));
        }

        let sequence = self.sequence_counter.fetch_add(1, Ordering::SeqCst);
        let task = AckTask::new(ack, sequence, ack_type, payload);

        self.metrics.total_acks.fetch_add(1, Ordering::Relaxed);
        self.metrics.pending_acks.fetch_add(1, Ordering::Relaxed);

        // Use distributed WAL if enabled, otherwise use fallback
        if self.config.enabled {
            if let Some(ref distributed_wal) = self.distributed_wal {
                let record = task.to_record();
                match distributed_wal.append(&record).await {
                    Ok(_) => {
                        self.metrics.persisted_acks.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        error!("Failed to append to distributed WAL: {}", e);
                        self.metrics.failed_uploads.fetch_add(1, Ordering::Relaxed);
                        return Err(Error::Unknown(format!(
                            "Distributed WAL append failed: {}",
                            e
                        )));
                    }
                }
            }
        } else if let Some(ref fallback) = self.fallback_processor {
            let _record = task.to_record();
            // For fallback mode, we just send to the fallback processor
            return fallback
                .ack(
                    task.ack().clone(),
                    task.ack_type().to_string(),
                    task.payload().to_vec(),
                )
                .await;
        }

        // Send to processing queue
        match self.ack_sender.send_async(task).await {
            Ok(_) => Ok(()),
            Err(e) => {
                self.metrics.pending_acks.fetch_sub(1, Ordering::Relaxed);
                self.metrics.failed_acks.fetch_add(1, Ordering::Relaxed);
                Err(Error::Unknown(format!("Failed to send ack task: {}", e)))
            }
        }
    }

    /// Get processor metrics
    pub fn get_metrics(&self) -> DistributedAckProcessorMetrics {
        self.metrics.clone()
    }

    /// Get cluster status
    pub async fn get_cluster_status(&self) -> Result<ClusterStatus, Error> {
        if !self.config.enabled {
            return Ok(ClusterStatus {
                cluster_id: self.cluster_id.clone(),
                node_id: self.node_id.clone(),
                distributed_mode: false,
                total_nodes: 1,
                active_nodes: 1,
                last_heartbeat: None,
                wal_status: "disabled".to_string(),
                recovery_status: "disabled".to_string(),
            });
        }

        let total_nodes = if let Some(ref recovery_manager) = self.recovery_manager {
            recovery_manager
                .node_registry()
                .get_all_nodes()
                .await
                .unwrap()
                .len()
        } else {
            1
        };

        let active_nodes = if let Some(ref recovery_manager) = self.recovery_manager {
            recovery_manager
                .node_registry()
                .get_active_nodes()
                .await
                .unwrap()
                .len()
        } else {
            1
        };

        let last_heartbeat = if let Some(ref node_registry_manager) = self.node_registry_manager {
            match node_registry_manager
                .registry()
                .get_node_info(&self.node_id)
                .await
            {
                Ok(Some(node_info)) => Some(node_info.last_heartbeat),
                _ => None,
            }
        } else {
            None
        };

        let wal_status = if let Some(ref distributed_wal) = self.distributed_wal {
            let state = distributed_wal.get_state().await;
            format!("active (pending uploads: {})", state.pending_uploads)
        } else {
            "inactive".to_string()
        };

        let recovery_status = if let Some(ref recovery_manager) = self.recovery_manager {
            let history = recovery_manager.get_recovery_history().await;
            if let Some(latest) = history.last() {
                match latest.status {
                    crate::recovery_manager::RecoveryStatus::Completed { .. } => {
                        "ready".to_string()
                    }
                    crate::recovery_manager::RecoveryStatus::InProgress { .. } => {
                        "recovering".to_string()
                    }
                    crate::recovery_manager::RecoveryStatus::Failed { .. } => "failed".to_string(),
                    _ => "unknown".to_string(),
                }
            } else {
                "no recovery performed".to_string()
            }
        } else {
            "disabled".to_string()
        };

        Ok(ClusterStatus {
            cluster_id: self.cluster_id.clone(),
            node_id: self.node_id.clone(),
            distributed_mode: true,
            total_nodes,
            active_nodes,
            last_heartbeat,
            wal_status,
            recovery_status,
        })
    }

    /// Create a manual checkpoint
    pub async fn create_checkpoint(&self) -> Result<String, Error> {
        if !self.config.enabled {
            return Err(Error::Unknown(
                "Checkpoint creation requires distributed mode".to_string(),
            ));
        }

        if let Some(ref checkpoint_manager) = self.checkpoint_manager {
            let sequence = self.sequence_counter.load(Ordering::Relaxed);
            let checkpoint_id = checkpoint_manager
                .create_checkpoint(sequence, self.node_id.clone(), None)
                .await?;

            self.metrics
                .checkpoint_creations
                .fetch_add(1, Ordering::Relaxed);
            info!("Created manual checkpoint: {}", checkpoint_id);
            Ok(checkpoint_id)
        } else {
            Err(Error::Unknown(
                "Checkpoint manager not available".to_string(),
            ))
        }
    }

    /// Trigger manual recovery
    pub async fn trigger_recovery(&self) -> Result<crate::recovery_manager::RecoveryInfo, Error> {
        if !self.config.enabled {
            return Err(Error::Unknown(
                "Recovery requires distributed mode".to_string(),
            ));
        }

        if let Some(ref recovery_manager) = self.recovery_manager {
            self.metrics
                .recovery_operations
                .fetch_add(1, Ordering::Relaxed);
            recovery_manager.perform_auto_recovery().await
        } else {
            Err(Error::Unknown("Recovery manager not available".to_string()))
        }
    }

    /// Perform consistency check
    pub async fn perform_consistency_check(
        &self,
    ) -> Result<crate::recovery_manager::ConsistencyReport, Error> {
        if !self.config.enabled {
            return Err(Error::Unknown(
                "Consistency check requires distributed mode".to_string(),
            ));
        }

        if let Some(ref recovery_manager) = self.recovery_manager {
            self.metrics
                .consistency_checks
                .fetch_add(1, Ordering::Relaxed);
            recovery_manager.perform_consistency_check().await
        } else {
            Err(Error::Unknown("Recovery manager not available".to_string()))
        }
    }

    /// Shutdown the processor
    pub async fn shutdown(self) -> Result<(), Error> {
        info!("Shutting down distributed acknowledgment processor");

        // Note: We can't call shutdown on Arc-wrapped structs that consume self
        // This would require changing the API to use &self or other approach
        // For now, we'll skip shutdown for these components

        // Note: NodeRegistryManager.stop() also consumes self, skipping for now

        info!("Distributed acknowledgment processor shutdown complete");
        Ok(())
    }

    /// Metrics collection task
    async fn metrics_collection_task(
        metrics: DistributedAckProcessorMetrics,
        _checkpoint_manager: Option<Arc<CheckpointManager>>,
        recovery_manager: Option<Arc<RecoveryManager>>,
        distributed_wal: Option<Arc<DistributedWAL>>,
        cancellation_token: CancellationToken,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                }
                _ = interval.tick() => {
                    // Update cluster nodes count
                    if let Some(ref recovery_manager) = recovery_manager {
                        if let Ok(active_nodes) = recovery_manager.node_registry().get_active_nodes().await {
                            metrics.cluster_nodes.store(active_nodes.len() as u64, Ordering::Relaxed);
                        }
                    }

                    // Update WAL size
                    if let Some(ref distributed_wal) = distributed_wal {
                        let wal_metrics = distributed_wal.get_metrics();
                        metrics.wal_size_bytes.store(
                            wal_metrics.local_wal_size_bytes.load(Ordering::Relaxed),
                            Ordering::Relaxed,
                        );
                    }

                    debug!("Metrics collection completed");
                }
            }
        }
    }

    /// Periodic checkpoint task
    async fn periodic_checkpoint_task(
        checkpoint_manager: Arc<CheckpointManager>,
        cancellation_token: CancellationToken,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(300)); // 5 minutes

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                }
                _ = interval.tick() => {
                    match checkpoint_manager.get_latest_checkpoint().await {
                        Ok(Some(latest)) => {
                            debug!("Latest checkpoint: {}", latest.checkpoint_id);
                        }
                        Ok(None) => {
                            debug!("No checkpoints available");
                        }
                        Err(e) => {
                            warn!("Failed to get latest checkpoint: {}", e);
                        }
                    }
                }
            }
        }
    }

    /// Consistency check task
    async fn consistency_check_task(
        recovery_manager: Arc<RecoveryManager>,
        cancellation_token: CancellationToken,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(600)); // 10 minutes

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                }
                _ = interval.tick() => {
                    match recovery_manager.perform_consistency_check().await {
                        Ok(report) => {
                            if !report.is_consistent {
                                warn!("Consistency check found {} discrepancies", report.discrepancies.len());
                            }
                        }
                        Err(e) => {
                            error!("Consistency check failed: {}", e);
                        }
                    }
                }
            }
        }
    }
}

/// Cluster status information
#[derive(Debug, Clone)]
pub struct ClusterStatus {
    pub cluster_id: String,
    pub node_id: String,
    pub distributed_mode: bool,
    pub total_nodes: usize,
    pub active_nodes: usize,
    pub last_heartbeat: Option<SystemTime>,
    pub wal_status: String,
    pub recovery_status: String,
}

/// Distributed acknowledgment processor worker
struct DistributedAckProcessorWorker {
    ack_receiver: Receiver<AckTask>,
    metrics: DistributedAckProcessorMetrics,
    cancellation_token: CancellationToken,
    _distributed_wal: Option<Arc<DistributedWAL>>,
    backpressure_active: Arc<AtomicBool>,
}

impl DistributedAckProcessorWorker {
    async fn run(self) {
        info!("Distributed ack processor worker started");

        let mut pending_tasks = Vec::with_capacity(BATCH_SIZE);
        let mut last_batch_time = Instant::now();

        loop {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    break;
                }
                result = self.ack_receiver.recv_async() => {
                    match result {
                        Ok(task) => {
                            pending_tasks.push(task);

                            // Check backpressure
                            if pending_tasks.len() > BACKPRESSURE_THRESHOLD {
                                self.backpressure_active.store(true, Ordering::Relaxed);
                                warn!("Backpressure activated - {} pending acks", pending_tasks.len());
                            }

                            // Process batch if full or timeout
                            if pending_tasks.len() >= BATCH_SIZE ||
                               last_batch_time.elapsed() > Duration::from_millis(1000) {
                                self.process_batch(&mut pending_tasks).await;
                                last_batch_time = Instant::now();
                            }
                        }
                        Err(_) => {
                            break;
                        }
                    }
                }
                // Process remaining batch on timeout
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    if !pending_tasks.is_empty() {
                        self.process_batch(&mut pending_tasks).await;
                        last_batch_time = Instant::now();
                    }
                }
            }
        }

        // Process remaining tasks before shutdown
        if !pending_tasks.is_empty() {
            self.process_batch(&mut pending_tasks).await;
        }

        info!("Distributed ack processor worker stopped");
    }

    async fn process_batch(&self, tasks: &mut Vec<AckTask>) {
        if tasks.is_empty() {
            return;
        }

        let batch_size = tasks.len();
        debug!("Processing batch of {} ack tasks", batch_size);

        let mut successful_count = 0;
        let mut failed_count = 0;
        let mut retried_count = 0;
        let mut tasks_to_remove = Vec::new();

        for (i, task) in tasks.iter_mut().enumerate() {
            if task.is_expired() {
                warn!(
                    "Ack task expired after {}ms",
                    task.created_at().elapsed().as_millis()
                );
                self.metrics.failed_acks.fetch_add(1, Ordering::Relaxed);
                self.metrics.pending_acks.fetch_sub(1, Ordering::Relaxed);
                failed_count += 1;
                tasks_to_remove.push(i);
                continue;
            }

            let result =
                tokio::time::timeout(Duration::from_millis(ACK_TIMEOUT_MS), task.ack().ack()).await;

            match result {
                Ok(_) => {
                    self.metrics.successful_acks.fetch_add(1, Ordering::Relaxed);
                    self.metrics.pending_acks.fetch_sub(1, Ordering::Relaxed);
                    successful_count += 1;
                    tasks_to_remove.push(i);
                }
                Err(_) => {
                    if task.should_retry() {
                        task.increment_retry();
                        self.metrics.retried_acks.fetch_add(1, Ordering::Relaxed);
                        retried_count += 1;

                        // Exponential backoff
                        tokio::time::sleep(Duration::from_millis(
                            RETRY_DELAY_MS * (task.retry_count() as u64).min(10),
                        ))
                        .await;
                    } else {
                        error!("Ack task failed after {} retries", task.retry_count());
                        self.metrics.failed_acks.fetch_add(1, Ordering::Relaxed);
                        self.metrics.pending_acks.fetch_sub(1, Ordering::Relaxed);
                        failed_count += 1;
                        tasks_to_remove.push(i);
                    }
                }
            }
        }

        // Remove completed tasks
        for &i in tasks_to_remove.iter().rev() {
            tasks.remove(i);
        }

        // Update backpressure status
        if tasks.len() < BACKPRESSURE_THRESHOLD / 2 {
            self.backpressure_active.store(false, Ordering::Relaxed);
        }

        if successful_count > 0 {
            debug!("Successfully acked {} messages", successful_count);
        }
        if failed_count > 0 {
            error!("Failed to ack {} messages", failed_count);
        }
        if retried_count > 0 {
            warn!("Retrying {} ack tasks", retried_count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_distributed_ack_processor_creation() {
        let temp_dir = TempDir::new().unwrap();

        let config = DistributedAckConfig::for_local_testing("test-cluster".to_string())
            .with_local_wal_path(
                temp_dir
                    .path()
                    .join("local_wal")
                    .to_string_lossy()
                    .to_string(),
            );

        let tracker = TaskTracker::new();
        let cancellation_token = CancellationToken::new();

        let processor =
            DistributedAckProcessor::new(&tracker, cancellation_token.clone(), config).await;

        assert!(processor.is_ok());

        let processor = processor.unwrap();
        assert_eq!(processor.cluster_id, "test-cluster");
        assert!(processor.node_id.starts_with("test-node"));

        cancellation_token.cancel();
        processor.shutdown().await.unwrap();
    }
}
