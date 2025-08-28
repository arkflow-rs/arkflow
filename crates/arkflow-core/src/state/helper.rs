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

//! 用于处理类型安全状态操作的状态管理工具

use crate::Error;
use serde::{Deserialize, Serialize};

/// 用于类型安全状态操作的辅助 trait
pub trait StateHelper {
    /// 从字节获取类型化值
    fn get_typed<V>(&self, key: &str) -> Result<Option<V>, Error>
    where
        V: for<'de> Deserialize<'de> + Send + Sync + 'static;

    /// 将类型化值存储为字节
    fn put_typed<V>(&mut self, key: &str, value: V) -> Result<(), Error>
    where
        V: Serialize + Send + Sync + 'static;
}

/// 简单的内存状态存储，可与任何可序列化类型一起使用
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleMemoryState {
    /// 数据存储
    data: std::collections::HashMap<String, Vec<u8>>,
}

impl SimpleMemoryState {
    /// 创建新的简单内存状态
    pub fn new() -> Self {
        Self {
            data: std::collections::HashMap::new(),
        }
    }

    /// 将值序列化为字节
    fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(value).map_err(|e| Error::Serialization(e))
    }

    /// 从字节反序列化值
    fn deserialize<V: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<V, Error> {
        serde_json::from_slice(data).map_err(|e| Error::Serialization(e))
    }
}

impl StateHelper for SimpleMemoryState {
    fn get_typed<V>(&self, key: &str) -> Result<Option<V>, Error>
    where
        V: for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        match self.data.get(key) {
            Some(data) => {
                // 反序列化数据
                let value = Self::deserialize(data)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    fn put_typed<V>(&mut self, key: &str, value: V) -> Result<(), Error>
    where
        V: Serialize + Send + Sync + 'static,
    {
        // 序列化值
        let data = Self::serialize(&value)?;
        self.data.insert(key.to_string(), data);
        Ok(())
    }
}

impl SimpleMemoryState {
    /// 获取原始字节
    pub fn get(&self, key: &str) -> Option<&[u8]> {
        self.data.get(key).map(|d| d.as_slice())
    }

    /// 存储原始字节
    pub fn put(&mut self, key: &str, value: Vec<u8>) {
        self.data.insert(key.to_string(), value);
    }

    /// 删除键
    pub fn delete(&mut self, key: &str) {
        self.data.remove(key);
    }

    /// 获取所有键
    pub fn keys(&self) -> Vec<&str> {
        self.data.keys().map(|s| s.as_str()).collect()
    }

    /// 清除所有数据
    pub fn clear(&mut self) {
        self.data.clear();
    }
}

impl Default for SimpleMemoryState {
    fn default() -> Self {
        Self::new()
    }
}
