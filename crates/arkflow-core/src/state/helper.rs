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

//! State management utilities for handling type-safe state operations

use crate::Error;
use serde::{Deserialize, Serialize};

/// Helper trait for type-safe state operations
pub trait StateHelper {
    /// Get a typed value from bytes
    fn get_typed<V>(&self, key: &str) -> Result<Option<V>, Error>
    where
        V: for<'de> Deserialize<'de> + Send + Sync + 'static;

    /// Put a typed value as bytes
    fn put_typed<V>(&mut self, key: &str, value: V) -> Result<(), Error>
    where
        V: Serialize + Send + Sync + 'static;
}

/// Simple in-memory state store that works with any serializable types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleMemoryState {
    data: std::collections::HashMap<String, Vec<u8>>,
}

impl SimpleMemoryState {
    /// Create new simple memory state
    pub fn new() -> Self {
        Self {
            data: std::collections::HashMap::new(),
        }
    }

    /// Serialize a value to bytes
    fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(value).map_err(|e| Error::Serialization(e))
    }

    /// Deserialize a value from bytes
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
        let data = Self::serialize(&value)?;
        self.data.insert(key.to_string(), data);
        Ok(())
    }
}

impl SimpleMemoryState {
    /// Get raw bytes
    pub fn get(&self, key: &str) -> Option<&[u8]> {
        self.data.get(key).map(|d| d.as_slice())
    }

    /// Put raw bytes
    pub fn put(&mut self, key: &str, value: Vec<u8>) {
        self.data.insert(key.to_string(), value);
    }

    /// Delete a key
    pub fn delete(&mut self, key: &str) {
        self.data.remove(key);
    }

    /// Get all keys
    pub fn keys(&self) -> Vec<&str> {
        self.data.keys().map(|s| s.as_str()).collect()
    }

    /// Clear all data
    pub fn clear(&mut self) {
        self.data.clear();
    }
}

impl Default for SimpleMemoryState {
    fn default() -> Self {
        Self::new()
    }
}
