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

//! 无需修改 trait 签名的基本状态管理示例

use super::enhanced::{TransactionLogEntry, TransactionStatus};
use crate::{Error, MessageBatch};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

/// 示例处理器，无需修改 trait 签名即可维护状态
pub struct StatefulExampleProcessor {
    /// 内部状态存储
    state: Arc<tokio::sync::RwLock<HashMap<String, serde_json::Value>>>,
    /// 处理器名称
    name: String,
}

impl StatefulExampleProcessor {
    /// 创建新的有状态处理器
    pub fn new(name: String) -> Self {
        Self {
            state: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            name,
        }
    }

    /// 处理带有状态访问的消息
    pub async fn process(&self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        // 检查元数据中的事务上下文
        if let Some(tx_ctx) = batch.transaction_context() {
            println!(
                "处理带有事务的批次: checkpoint_id={}",
                tx_ctx.checkpoint_id
            );
        }

        // 示例：按输入源统计消息数量
        if let Some(input_name) = batch.get_input_name() {
            let count_key = format!("count_{}", input_name);

            let mut state = self.state.write().await;
            let current_count = state.get(&count_key).and_then(|v| v.as_u64()).unwrap_or(0);
            let new_count = current_count + batch.len() as u64;

            state.insert(
                count_key,
                serde_json::Value::Number(serde_json::Number::from(new_count)),
            );

            println!(
                "处理器 {}: 更新 {} 的计数为 {}",
                self.name, input_name, new_count
            );
        }

        // 处理批次（通常在这里进行实际转换）
        Ok(vec![batch])
    }

    /// 获取输入的当前计数
    pub async fn get_count(&self, input_name: &str) -> Result<u64, Error> {
        let state = self.state.read().await;
        let count_key = format!("count_{}", input_name);
        Ok(state.get(&count_key).and_then(|v| v.as_u64()).unwrap_or(0))
    }

    /// 获取所有当前状态（用于调试/监控）
    pub async fn get_state_snapshot(&self) -> HashMap<String, serde_json::Value> {
        self.state.read().await.clone()
    }
}

/// 事务感知的现有输出实现包装器
pub struct TransactionalOutputWrapper<O> {
    /// 内部输出
    inner: O,
    /// 事务日志
    transaction_log: Arc<tokio::sync::RwLock<Vec<TransactionLogEntry>>>,
}

impl<O> TransactionalOutputWrapper<O> {
    /// 创建新的事务输出包装器
    pub fn new(inner: O) -> Self {
        Self {
            inner,
            transaction_log: Arc::new(tokio::sync::RwLock::new(Vec::new())),
        }
    }

    /// 带有事务支持的写入
    pub async fn write(&self, msg: MessageBatch) -> Result<(), Error>
    where
        O: crate::output::Output,
    {
        // 检查这是否是事务批次
        if let Some(tx_ctx) = msg.transaction_context() {
            // 记录事务
            let log_entry = TransactionLogEntry {
                transaction_id: tx_ctx.transaction_id.clone(),
                checkpoint_id: tx_ctx.checkpoint_id,
                timestamp: std::time::SystemTime::now(),
                status: TransactionStatus::Prepared,
                batch_size: msg.len(),
            };

            self.transaction_log.write().await.push(log_entry);

            // 写入实际输出
            self.inner.write(msg).await?;

            // 标记为已提交
            if let Some(entry) = self.transaction_log.write().await.last_mut() {
                entry.status = TransactionStatus::Committed;
            }
        } else {
            // 非事务写入
            self.inner.write(msg).await?;
        }

        Ok(())
    }

    /// 获取事务日志
    pub async fn get_transaction_log(&self) -> Vec<TransactionLogEntry> {
        self.transaction_log.read().await.clone()
    }
}

/// 用于向流中插入检查点的屏障注入器
pub struct SimpleBarrierInjector {
    /// 间隔
    interval: std::time::Duration,
    /// 上次注入时间
    last_injection: Arc<tokio::sync::RwLock<std::time::Instant>>,
    /// 下一个检查点 ID
    next_checkpoint_id: Arc<AtomicU64>,
}

impl SimpleBarrierInjector {
    /// 创建新的屏障注入器
    pub fn new(interval_ms: u64) -> Self {
        Self {
            interval: std::time::Duration::from_millis(interval_ms),
            last_injection: Arc::new(tokio::sync::RwLock::new(std::time::Instant::now())),
            next_checkpoint_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// 检查是否应该注入屏障
    pub async fn should_inject(&self) -> bool {
        let last = *self.last_injection.read().await;
        last.elapsed() >= self.interval
    }

    /// 如果需要，将屏障注入到批次中
    pub async fn maybe_inject_barrier(&self, batch: MessageBatch) -> Result<MessageBatch, Error> {
        if self.should_inject().await {
            let checkpoint_id = self.next_checkpoint_id.fetch_add(1, Ordering::SeqCst);

            // 创建事务上下文
            let tx_ctx =
                crate::state::transaction::TransactionContext::aligned_checkpoint(checkpoint_id);

            // 创建带有事务的元数据
            let mut metadata = crate::state::Metadata::new();
            metadata.transaction = Some(tx_ctx);

            // 将元数据嵌入到批次中
            *self.last_injection.write().await = std::time::Instant::now();
            batch.with_metadata(metadata)
        } else {
            Ok(batch)
        }
    }
}

/// 状态管理配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleStateConfig {
    /// 启用状态管理功能
    pub enabled: bool,
    /// 检查点间隔（毫秒）
    pub checkpoint_interval_ms: u64,
    /// 启用事务输出
    pub transactional_outputs: bool,
}

impl Default for SimpleStateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            checkpoint_interval_ms: 60000, // 1 分钟
            transactional_outputs: false,
        }
    }
}

/// 使用示例
pub async fn example_usage() -> Result<(), Error> {
    // 创建配置
    let config = SimpleStateConfig {
        enabled: true,
        checkpoint_interval_ms: 5000, // 演示用 5 秒
        transactional_outputs: true,
    };

    if config.enabled {
        // 创建有状态处理器
        let processor = StatefulExampleProcessor::new("example_processor".to_string());

        // 创建屏障注入器
        let barrier_injector = SimpleBarrierInjector::new(config.checkpoint_interval_ms);

        // 处理一些消息
        let batch1 = MessageBatch::from_string("hello")?;
        let batch1_with_barrier = barrier_injector.maybe_inject_barrier(batch1).await?;
        let _result = processor.process(batch1_with_barrier).await?;

        let batch2 = MessageBatch::from_string("world")?;
        let batch2_with_barrier = barrier_injector.maybe_inject_barrier(batch2).await?;
        let _result = processor.process(batch2_with_barrier).await?;

        // 检查计数
        let count = processor.get_count("unknown").await?;
        println!("处理的消息总数: {}", count);

        // 显示状态快照
        let snapshot = processor.get_state_snapshot().await;
        println!("当前状态: {:?}", snapshot);
    }

    Ok(())
}
