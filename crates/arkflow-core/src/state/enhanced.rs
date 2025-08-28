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

//! 带有 S3 后端和精确一次语义的增强状态管理器

use crate::state::helper::SimpleMemoryState;
use crate::state::helper::StateHelper;
use crate::state::s3_backend::{S3CheckpointCoordinator, S3StateBackend, S3StateBackendConfig};
use crate::state::simple::SimpleBarrierInjector;
use crate::state::transaction::TransactionContext;
use crate::{Error, MessageBatch};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::RwLock;

/// 增强状态管理器，支持持久化和精确一次保证
pub struct EnhancedStateManager {
    /// S3 后端，用于状态持久化
    s3_backend: Option<Arc<S3StateBackend>>,
    /// 检查点协调器
    checkpoint_coordinator: Option<S3CheckpointCoordinator>,
    /// 本地状态缓存
    local_states: HashMap<String, SimpleMemoryState>,
    /// 屏障注入器
    barrier_injector: Arc<SimpleBarrierInjector>,
    /// 配置
    config: EnhancedStateConfig,
    /// 当前检查点 ID
    current_checkpoint_id: Arc<AtomicU64>,
    /// 活跃事务
    active_transactions: Arc<RwLock<HashMap<String, TransactionInfo>>>,
}

/// 事务信息
#[derive(Debug, Clone)]
pub struct TransactionInfo {
    /// 事务 ID
    pub transaction_id: String,
    /// 检查点 ID
    pub checkpoint_id: u64,
    /// 参与者列表
    pub participants: Vec<String>,
    /// 创建时间
    pub created_at: std::time::SystemTime,
}

/// 增强状态配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnhancedStateConfig {
    /// 是否启用状态管理
    pub enabled: bool,
    /// 状态后端类型
    pub backend_type: StateBackendType,
    /// S3 配置（如果使用 S3 后端）
    pub s3_config: Option<S3StateBackendConfig>,
    /// 检查点间隔（毫秒）
    pub checkpoint_interval_ms: u64,
    /// 保留的检查点数量
    pub retained_checkpoints: usize,
    /// 是否启用精确一次语义
    pub exactly_once: bool,
    /// 状态超时时间（毫秒）
    pub state_timeout_ms: u64,
}

impl Default for EnhancedStateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backend_type: StateBackendType::Memory,
            s3_config: None,
            checkpoint_interval_ms: 60000,
            retained_checkpoints: 5,
            exactly_once: false,
            state_timeout_ms: 86400000, // 24 hours
        }
    }
}

/// 状态后端类型
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StateBackendType {
    /// 内存后端
    Memory,
    /// S3 后端
    S3,
    /// 混合后端
    Hybrid,
}

impl EnhancedStateManager {
    /// 创建新的增强状态管理器
    pub async fn new(config: EnhancedStateConfig) -> Result<Self, Error> {
        // 如果启用了状态管理且不是内存后端，则初始化 S3 后端
        let (s3_backend, checkpoint_coordinator) =
            if config.enabled && config.backend_type != StateBackendType::Memory {
                if let Some(s3_config) = &config.s3_config {
                    // 创建 S3 后端
                    let backend = Arc::new(S3StateBackend::new(s3_config.clone()).await?);
                    // 创建检查点协调器
                    let coordinator = S3CheckpointCoordinator::new(backend.clone());
                    (Some(backend), Some(coordinator))
                } else {
                    return Err(Error::Config(
                        "使用 S3 后端需要提供 S3 配置".to_string(),
                    ));
                }
            } else {
                (None, None)
            };

        // 创建屏障注入器
        let barrier_injector = Arc::new(SimpleBarrierInjector::new(config.checkpoint_interval_ms));

        Ok(Self {
            s3_backend,
            checkpoint_coordinator,
            local_states: HashMap::new(),
            barrier_injector,
            config,
            current_checkpoint_id: Arc::new(AtomicU64::new(1)),
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// 处理带有状态管理的消息批次
    pub async fn process_batch(&mut self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        // 如果未启用状态管理，直接返回原始批次
        if !self.config.enabled {
            return Ok(vec![batch]);
        }

        // 如果需要，注入屏障
        let processed_batch = self.barrier_injector.maybe_inject_barrier(batch).await?;

        // 检查是否有事务上下文
        if let Some(tx_ctx) = processed_batch.transaction_context() {
            // 处理事务批次
            self.process_transactional_batch(processed_batch, tx_ctx)
                .await
        } else {
            // 非事务批次，直接返回
            Ok(vec![processed_batch])
        }
    }

    /// 处理事务批次
    async fn process_transactional_batch(
        &mut self,
        batch: MessageBatch,
        tx_ctx: TransactionContext,
    ) -> Result<Vec<MessageBatch>, Error> {
        // 注册事务
        self.register_transaction(&tx_ctx).await?;

        // 如果这是检查点屏障，触发检查点
        if tx_ctx.is_checkpoint() {
            self.trigger_checkpoint(tx_ctx.checkpoint_id).await?;
        }

        // 处理批次（目前原样返回）
        // 在实际实现中，这里会应用状态转换
        Ok(vec![batch])
    }

    /// 注册新事务
    async fn register_transaction(&self, tx_ctx: &TransactionContext) -> Result<(), Error> {
        let mut transactions = self.active_transactions.write().await;
        transactions.insert(
            tx_ctx.transaction_id.clone(),
            TransactionInfo {
                transaction_id: tx_ctx.transaction_id.clone(),
                checkpoint_id: tx_ctx.checkpoint_id,
                participants: Vec::new(),
                created_at: std::time::SystemTime::now(),
            },
        );
        Ok(())
    }

    /// 触发检查点
    async fn trigger_checkpoint(&mut self, checkpoint_id: u64) -> Result<(), Error> {
        if let Some(ref mut coordinator) = self.checkpoint_coordinator {
            // 开始检查点
            coordinator.start_checkpoint().await?;

            // 在实际实现中，你需要：
            // 1. 通知所有操作符准备检查点
            // 2. 从所有操作符收集状态
            // 3. 将状态持久化到 S3
            // 4. 完成检查点

            // 目前，只保存当前的本地状态
            for (operator_id, state) in &self.local_states {
                coordinator
                    .complete_participant(
                        checkpoint_id,
                        operator_id,
                        vec![("default".to_string(), state.clone())],
                    )
                    .await?;
            }

            // 清理旧检查点
            coordinator
                .cleanup_old_checkpoints(self.config.retained_checkpoints)
                .await?;
        }

        Ok(())
    }

    /// 获取或创建操作符的状态
    pub fn get_operator_state(&mut self, operator_id: &str) -> &mut SimpleMemoryState {
        self.local_states
            .entry(operator_id.to_string())
            .or_insert_with(SimpleMemoryState::new)
    }

    /// 获取状态值
    pub async fn get_state_value<K, V>(
        &self,
        operator_id: &str,
        key: &K,
    ) -> Result<Option<V>, Error>
    where
        K: ToString + Send + Sync,
        V: for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        if let Some(state) = self.local_states.get(operator_id) {
            state.get_typed(&key.to_string())
        } else {
            Ok(None)
        }
    }

    /// 设置状态值
    pub async fn set_state_value<K, V>(
        &mut self,
        operator_id: &str,
        key: &K,
        value: V,
    ) -> Result<(), Error>
    where
        K: ToString + Send + Sync,
        V: serde::Serialize + Send + Sync + 'static,
    {
        let state = self.get_operator_state(operator_id);
        state.put_typed(&key.to_string(), value)?;
        Ok(())
    }

    /// 手动创建检查点
    pub async fn create_checkpoint(&mut self) -> Result<u64, Error> {
        let checkpoint_id = self.current_checkpoint_id.fetch_add(1, Ordering::SeqCst);
        self.trigger_checkpoint(checkpoint_id).await?;
        Ok(checkpoint_id)
    }

    /// 从最新检查点恢复
    pub async fn recover_from_latest_checkpoint(&mut self) -> Result<Option<u64>, Error> {
        if let Some(ref coordinator) = self.checkpoint_coordinator {
            if let Some(checkpoint_id) = coordinator.get_latest_checkpoint().await? {
                // 从检查点加载状态
                // 在实际实现中，你会恢复所有操作符状态
                println!("从检查点恢复: {}", checkpoint_id);
                return Ok(Some(checkpoint_id));
            }
        }
        Ok(None)
    }

    /// 获取当前状态统计信息
    pub async fn get_state_stats(&self) -> StateStats {
        let transactions = self.active_transactions.read().await;
        StateStats {
            active_transactions: transactions.len(),
            local_states_count: self.local_states.len(),
            current_checkpoint_id: self.current_checkpoint_id.load(Ordering::SeqCst),
            backend_type: self.config.backend_type.clone(),
            enabled: self.config.enabled,
        }
    }

    /// 获取后端类型
    pub fn get_backend_type(&self) -> StateBackendType {
        self.config.backend_type.clone()
    }

    /// 关闭状态管理器
    pub async fn shutdown(&mut self) -> Result<(), Error> {
        // 如果启用，创建最终检查点
        if self.config.enabled {
            self.create_checkpoint().await?;
        }
        Ok(())
    }
}

/// 状态统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateStats {
    /// 活跃事务数量
    pub active_transactions: usize,
    /// 本地状态数量
    pub local_states_count: usize,
    /// 当前检查点 ID
    pub current_checkpoint_id: u64,
    /// 后端类型
    pub backend_type: StateBackendType,
    /// 是否启用
    pub enabled: bool,
}

/// 精确一次处理器包装器
pub struct ExactlyOnceProcessor<P> {
    /// 内部处理器
    inner: P,
    /// 状态管理器
    state_manager: Arc<RwLock<EnhancedStateManager>>,
    /// 操作符 ID
    operator_id: String,
}

impl<P> ExactlyOnceProcessor<P> {
    /// 创建新的精确一次处理器包装器
    pub fn new(
        inner: P,
        state_manager: Arc<RwLock<EnhancedStateManager>>,
        operator_id: String,
    ) -> Self {
        Self {
            inner,
            state_manager,
            operator_id,
        }
    }

    /// 带有精确一次保证的处理
    pub async fn process(&self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error>
    where
        P: crate::processor::Processor,
    {
        // 让状态管理器处理屏障和事务
        let mut state_manager = self.state_manager.write().await;
        let processed_batches = state_manager.process_batch(batch).await?;

        // 应用实际处理
        let mut results = Vec::new();
        for batch in processed_batches {
            // 使用内部处理器处理
            let inner_results = self.inner.process(batch.clone()).await?;

            // 如果需要，更新状态
            if let Some(tx_ctx) = batch.transaction_context() {
                // 示例：更新处理计数
                let state_key = format!("processed_count_{}", tx_ctx.checkpoint_id);
                let mut state_manager = self.state_manager.write().await;
                state_manager
                    .set_state_value(&self.operator_id, &state_key, batch.len())
                    .await?;
            }

            results.extend(inner_results);
        }

        Ok(results)
    }

    /// 获取状态值
    pub async fn get_state<K, V>(&self, key: &K) -> Result<Option<V>, Error>
    where
        K: ToString + Send + Sync,
        V: for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    {
        let state_manager = self.state_manager.read().await;
        state_manager.get_state_value(&self.operator_id, key).await
    }

    /// 设置状态值
    pub async fn set_state<K, V>(&self, key: &K, value: V) -> Result<(), Error>
    where
        K: ToString + Send + Sync,
        V: serde::Serialize + Send + Sync + 'static,
    {
        let mut state_manager = self.state_manager.write().await;
        state_manager
            .set_state_value(&self.operator_id, key, value)
            .await
    }
}

/// 两阶段提交输出包装器
pub struct TwoPhaseCommitOutput<O> {
    /// 内部输出
    inner: O,
    /// 状态管理器
    state_manager: Arc<tokio::sync::RwLock<EnhancedStateManager>>,
    /// 事务日志
    transaction_log: Arc<RwLock<Vec<TransactionLogEntry>>>,
    /// 待处理事务
    pending_transactions: HashMap<String, Vec<MessageBatch>>,
}

impl<O> TwoPhaseCommitOutput<O> {
    /// 创建新的两阶段提交输出
    pub fn new(inner: O, state_manager: Arc<tokio::sync::RwLock<EnhancedStateManager>>) -> Self {
        Self {
            inner,
            state_manager,
            transaction_log: Arc::new(RwLock::new(Vec::new())),
            pending_transactions: HashMap::new(),
        }
    }

    /// 带有两阶段提交的写入
    pub async fn write(&self, batch: MessageBatch) -> Result<(), Error>
    where
        O: crate::output::Output,
    {
        if let Some(tx_ctx) = batch.transaction_context() {
            if tx_ctx.is_checkpoint() {
                // 第一阶段：准备
                self.prepare_transaction(&tx_ctx, &batch).await?;

                // 第二阶段：提交（检查点完成后）
                self.commit_transaction(&tx_ctx).await?;
            } else {
                // 普通写入
                self.inner.write(batch).await?;
            }
        } else {
            // 非事务写入
            self.inner.write(batch).await?;
        }

        Ok(())
    }

    /// 两阶段提交的准备阶段
    async fn prepare_transaction(
        &self,
        tx_ctx: &TransactionContext,
        batch: &MessageBatch,
    ) -> Result<(), Error> {
        // 记录事务日志
        let log_entry = TransactionLogEntry {
            transaction_id: tx_ctx.transaction_id.clone(),
            checkpoint_id: tx_ctx.checkpoint_id,
            timestamp: std::time::SystemTime::now(),
            status: TransactionStatus::Prepared,
            batch_size: batch.len(),
        };

        self.transaction_log.write().await.push(log_entry);

        // 在实际实现中，你需要：
        // 1. 写入临时/暂存区域
        // 2. 确保所有数据都是持久的
        // 3. 准备提交

        Ok(())
    }

    /// 两阶段提交的提交阶段
    async fn commit_transaction(&self, tx_ctx: &TransactionContext) -> Result<(), Error> {
        // 更新事务日志
        let mut log = self.transaction_log.write().await;
        if let Some(entry) = log
            .iter_mut()
            .find(|e| e.transaction_id == tx_ctx.transaction_id)
        {
            entry.status = TransactionStatus::Committed;
        }

        // 在实际实现中，你需要：
        // 1. 使数据对消费者可见
        // 2. 与事务协调器确认

        Ok(())
    }

    /// 获取事务日志
    pub async fn get_transaction_log(&self) -> Vec<TransactionLogEntry> {
        self.transaction_log.read().await.clone()
    }
}

/// 事务日志条目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionLogEntry {
    /// 事务 ID
    pub transaction_id: String,
    /// 检查点 ID
    pub checkpoint_id: u64,
    /// 时间戳
    pub timestamp: std::time::SystemTime,
    /// 事务状态
    pub status: TransactionStatus,
    /// 批次大小
    pub batch_size: usize,
}

/// 事务状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TransactionStatus {
    /// 已准备
    Prepared,
    /// 已提交
    Committed,
    /// 已中止
    Aborted,
}
