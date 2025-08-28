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

//! 简化的状态管理示例

use crate::state::SimpleMemoryState;
use crate::state::StateHelper;
use crate::Error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// 在处理器中使用状态管理的示例
pub struct CountingProcessor {
    /// 用于按键计数事件的状态
    count_state: Arc<tokio::sync::RwLock<SimpleMemoryState>>,
    /// 此处理器的操作符 ID
    operator_id: String,
}

impl CountingProcessor {
    /// 创建新的计数处理器
    pub fn new(operator_id: String) -> Self {
        Self {
            count_state: Arc::new(tokio::sync::RwLock::new(SimpleMemoryState::new())),
            operator_id,
        }
    }

    /// 处理批次并计数事件
    pub async fn process_batch(&self, batch: &crate::MessageBatch) -> Result<(), Error> {
        // 如果存在，提取事务上下文
        if let Some(tx_ctx) = batch.transaction_context() {
            println!(
                "在事务中处理批次: checkpoint_id={}",
                tx_ctx.checkpoint_id
            );
        }

        // 示例：按输入源计数消息
        if let Some(input_name) = batch.get_input_name() {
            let key = format!("count_{}", input_name);

            let mut state = self.count_state.write().await;
            let current_count: Option<u64> = state.get_typed(&key)?;
            let new_count = current_count.unwrap_or(0) + batch.len() as u64;
            state.put_typed(&key, new_count)?;

            println!("更新 {} 的计数: {}", input_name, new_count);
        }

        Ok(())
    }

    /// 获取输入源的当前计数
    pub async fn get_count(&self, input_name: &str) -> Result<u64, Error> {
        let key = format!("count_{}", input_name);
        let state = self.count_state.read().await;
        state.get_typed(&key)?.unwrap_or(0)
    }

    /// 键控状态处理的示例
    pub async fn process_keyed_batch<K, V>(
        &self,
        batch: &crate::MessageBatch,
        key_column: &str,
    ) -> Result<(), Error>
    where
        K: for<'de> Deserialize<'de> + Send + Sync + 'static + ToString,
        V: for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        // 这是一个简化的示例 - 实际上你会从批次中提取键
        // 现在，我们只是演示模式

        // 模拟从批次中提取键和值
        let mut state = self.count_state.write().await;

        // 示例：处理批次中的每一行
        for _ in 0..batch.len() {
            // 在实际实现中，你会从批次中提取实际的键值对
            let dummy_key = "example_key".to_string();
            let dummy_value: u64 = 42;

            let state_key = format!("keyed_{}_{}", self.operator_id, dummy_key);
            let current: Option<u64> = state.get_typed(&state_key)?;
            let updated = current.unwrap_or(0) + dummy_value;
            state.put_typed(&state_key, updated)?;
        }

        Ok(())
    }
}

/// 状态管理的示例配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    /// 是否启用状态管理
    pub enabled: bool,
    /// 状态后端类型
    pub backend: StateBackendType,
    /// 检查点间隔（毫秒）
    pub checkpoint_interval_ms: u64,
    /// 状态 TTL（毫秒，0 = 不过期）
    pub state_ttl_ms: u64,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backend: StateBackendType::Memory,
            checkpoint_interval_ms: 60000, // 1 分钟
            state_ttl_ms: 0,               // 不过期
        }
    }
}

/// 状态后端类型
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StateBackendType {
    /// 内存状态后端
    Memory,
    /// 文件系统状态后端
    FileSystem,
    /// S3 状态后端
    S3,
}

/// 使用配置的状态管理示例
pub struct StatefulProcessor<T> {
    /// 内部处理器
    inner: T,
    /// 状态存储
    state: Arc<tokio::sync::RwLock<SimpleMemoryState>>,
    /// 配置
    config: StateConfig,
    /// 操作符 ID
    operator_id: String,
}

impl<T> StatefulProcessor<T> {
    /// 创建新的有状态处理器包装器
    pub fn new(inner: T, config: StateConfig, operator_id: String) -> Self {
        Self {
            inner,
            state: Arc::new(tokio::sync::RwLock::new(SimpleMemoryState::new())),
            config,
            operator_id,
        }
    }

    /// 获取状态访问权限
    pub fn state(&self) -> Arc<tokio::sync::RwLock<SimpleMemoryState>> {
        self.state.clone()
    }

    /// 获取配置
    pub fn config(&self) -> &StateConfig {
        &self.config
    }
}

/// 使用示例
#[tokio::main]
async fn example_usage() -> Result<(), Error> {
    // 创建计数处理器
    let processor = CountingProcessor::new("counter_1".to_string());

    // 创建示例消息批次
    let batch = crate::MessageBatch::from_string("hello world")?;

    // 处理批次
    processor.process_batch(&batch).await?;

    // 获取计数
    let count = processor.get_count("unknown").await?;
    println!("总计数: {}", count);

    Ok(())
}
