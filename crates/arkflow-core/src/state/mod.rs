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

//! ArkFlow 的状态管理和事务支持

use crate::{Error, MessageBatch};
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

pub mod enhanced;
pub mod helper;
pub mod integration_tests;
pub mod monitoring;
pub mod performance;
pub mod s3_backend;
pub mod simple;
pub mod tests;
pub mod transaction;

pub use enhanced::*;
pub use helper::*;
pub use monitoring::*;
pub use performance::*;
pub use s3_backend::*;
pub use simple::*;
pub use transaction::*;

/// 可以附加到 MessageBatch 的元数据
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Metadata {
    /// 用于精确一次处理的事务上下文
    pub transaction: Option<TransactionContext>,
    /// 自定义元数据字段
    pub custom: HashMap<String, MetadataValue>,
}

/// 可以保存不同类型的元数据值
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MetadataValue {
    String(String),
    Bytes(Vec<u8>),
    Int64(i64),
    Float64(f64),
    Bool(bool),
    Json(serde_json::Value),
}

impl Metadata {
    /// 创建新的空元数据
    pub fn new() -> Self {
        Self {
            transaction: None,
            custom: HashMap::new(),
        }
    }

    /// 从 MessageBatch 提取元数据
    pub fn extract_from_batch(batch: &MessageBatch) -> Option<Self> {
        // 使用特殊的字段名称存储元数据
        batch
            .schema()
            .metadata()
            .get("__arkflow_metadata__")
            .and_then(|v| serde_json::from_str(v).ok())
    }

    /// 将元数据嵌入到 MessageBatch
    pub fn embed_to_batch(&self, batch: MessageBatch) -> Result<MessageBatch, Error> {
        let metadata_json = serde_json::to_string(self).map_err(|e| Error::Serialization(e))?;

        let mut metadata = batch.schema().metadata().clone();
        metadata.insert("__arkflow_metadata__".to_string(), metadata_json);

        let schema = batch.schema().as_ref().clone().with_metadata(metadata);
        let record_batch = RecordBatch::try_new(Arc::new(schema), batch.columns().to_vec())
            .map_err(|e| {
                Error::Process(format!(
                    "创建带有元数据的记录批次失败: {}",
                    e
                ))
            })?;

        Ok(MessageBatch::new_arrow(record_batch))
    }
}

impl From<HashMap<String, MetadataValue>> for Metadata {
    fn from(custom: HashMap<String, MetadataValue>) -> Self {
        Self {
            transaction: None,
            custom,
        }
    }
}
