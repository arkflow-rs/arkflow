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

use super::enhanced::TransactionInfo;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

/// 用于精确一次处理的事务上下文
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionContext {
    /// 唯一事务标识符
    pub transaction_id: String,
    /// 检查点标识符
    pub checkpoint_id: u64,
    /// 屏障类型
    pub barrier_type: BarrierType,
    /// 事务创建时的时间戳
    pub timestamp: u64,
}

/// 用于检查点对齐的屏障类型
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum BarrierType {
    /// 普通处理
    None,
    /// 检查点屏障 - 触发状态快照
    Checkpoint,
    /// 保存点屏障 - 手动触发的检查点
    Savepoint,
    /// 对齐屏障 - 等待所有消息处理完成
    AlignedCheckpoint,
}

impl TransactionContext {
    /// 创建新的事务上下文
    pub fn new(checkpoint_id: u64, barrier_type: BarrierType) -> Self {
        Self {
            transaction_id: Uuid::new_v4().to_string(),
            checkpoint_id,
            barrier_type,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }

    /// 创建检查点屏障
    pub fn checkpoint(checkpoint_id: u64) -> Self {
        Self::new(checkpoint_id, BarrierType::Checkpoint)
    }

    /// 创建对齐检查点屏障
    pub fn aligned_checkpoint(checkpoint_id: u64) -> Self {
        Self::new(checkpoint_id, BarrierType::AlignedCheckpoint)
    }

    /// 创建保存点屏障
    pub fn savepoint(checkpoint_id: u64) -> Self {
        Self::new(checkpoint_id, BarrierType::Savepoint)
    }

    /// 检查是否是检查点屏障
    pub fn is_checkpoint(&self) -> bool {
        matches!(
            self.barrier_type,
            BarrierType::Checkpoint | BarrierType::AlignedCheckpoint
        )
    }

    /// 检查是否需要对齐
    pub fn requires_alignment(&self) -> bool {
        self.barrier_type == BarrierType::AlignedCheckpoint
    }
}

/// 用于管理两阶段提交的事务协调器
pub struct TransactionCoordinator {
    /// 下一个检查点 ID
    next_checkpoint_id: AtomicU64,
    /// 活跃事务
    active_transactions: Arc<tokio::sync::RwLock<HashMap<String, TransactionInfo>>>,
    /// 检查点间隔（毫秒）
    checkpoint_interval: u64,
}

/// 事务参与者
#[derive(Debug, Clone)]
pub struct TransactionParticipant {
    /// 参与者 ID
    pub id: String,
    /// 参与者状态
    pub state: ParticipantState,
}

/// 两阶段提交中的参与者状态
#[derive(Debug, Clone, PartialEq)]
pub enum ParticipantState {
    /// 已准备
    Prepared,
    /// 已提交
    Committed,
    /// 已中止
    Aborted,
}

impl TransactionCoordinator {
    /// 创建新的事务协调器
    pub fn new(checkpoint_interval_ms: u64) -> Self {
        Self {
            next_checkpoint_id: AtomicU64::new(1),
            active_transactions: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            checkpoint_interval: checkpoint_interval_ms,
        }
    }

    /// 获取下一个检查点 ID
    pub fn next_checkpoint_id(&self) -> u64 {
        self.next_checkpoint_id.fetch_add(1, Ordering::SeqCst)
    }

    /// 开始新事务
    pub async fn begin_transaction(&self, barrier_type: BarrierType) -> TransactionContext {
        let checkpoint_id = self.next_checkpoint_id();
        let tx_ctx = TransactionContext::new(checkpoint_id, barrier_type);

        let mut transactions = self.active_transactions.write().await;
        transactions.insert(
            tx_ctx.transaction_id.clone(),
            TransactionInfo {
                transaction_id: tx_ctx.transaction_id.clone(),
                checkpoint_id,
                participants: Vec::new(),
                created_at: std::time::SystemTime::now(),
            },
        );

        tx_ctx
    }

    /// 为事务注册参与者
    pub async fn register_participant(
        &self,
        transaction_id: &str,
        participant_id: String,
    ) -> Result<(), crate::Error> {
        let mut transactions = self.active_transactions.write().await;
        if let Some(tx_info) = transactions.get_mut(transaction_id) {
            tx_info.participants.push(participant_id);
            Ok(())
        } else {
            Err(crate::Error::Process(format!(
                "未找到事务 {}",
                transaction_id
            )))
        }
    }

    /// 完成事务（提交或中止）
    pub async fn complete_transaction(
        &self,
        transaction_id: &str,
        _success: bool,
    ) -> Result<(), crate::Error> {
        let mut transactions = self.active_transactions.write().await;
        if let Some(_tx_info) = transactions.remove(transaction_id) {
            // 事务已完成 - 所有参与者现在只是字符串
            // 在实际实现中，你会通知每个参与者
            Ok(())
        } else {
            Err(crate::Error::Process(format!(
                "未找到事务 {}",
                transaction_id
            )))
        }
    }

    /// 获取检查点间隔
    pub fn checkpoint_interval(&self) -> u64 {
        self.checkpoint_interval
    }
}

/// 用于向流中插入屏障的屏障注入器
pub struct BarrierInjector {
    /// 事务协调器
    coordinator: Arc<TransactionCoordinator>,
    /// 上次检查点时间
    last_checkpoint_time: Arc<tokio::sync::RwLock<std::time::Instant>>,
}

impl BarrierInjector {
    /// 创建新的屏障注入器
    pub fn new(coordinator: Arc<TransactionCoordinator>) -> Self {
        Self {
            coordinator,
            last_checkpoint_time: Arc::new(tokio::sync::RwLock::new(std::time::Instant::now())),
        }
    }

    /// 检查是否应该注入屏障
    pub async fn should_inject_barrier(&self) -> Option<TransactionContext> {
        let last_time = *self.last_checkpoint_time.read().await;
        let elapsed = last_time.elapsed();

        // 如果距离上次检查点的时间超过了间隔，则注入屏障
        if elapsed.as_millis() as u64 >= self.coordinator.checkpoint_interval() {
            let tx_ctx = self
                .coordinator
                .begin_transaction(BarrierType::AlignedCheckpoint)
                .await;
            *self.last_checkpoint_time.write().await = std::time::Instant::now();
            Some(tx_ctx)
        } else {
            None
        }
    }

    /// 将屏障注入到消息批次元数据中
    pub async fn inject_into_batch(
        &self,
        batch: &crate::MessageBatch,
    ) -> Option<(crate::MessageBatch, TransactionContext)> {
        if let Some(tx_ctx) = self.should_inject_barrier().await {
            // 从批次中提取或创建元数据
            let mut metadata =
                crate::state::Metadata::extract_from_batch(batch).unwrap_or_default();
            metadata.transaction = Some(tx_ctx.clone());

            // 将元数据嵌入到批次中
            match metadata.embed_to_batch(batch.clone()) {
                Ok(new_batch) => Some((new_batch, tx_ctx)),
                Err(_) => None,
            }
        } else {
            None
        }
    }
}
