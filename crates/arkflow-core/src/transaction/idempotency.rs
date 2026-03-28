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

//! Idempotency cache for exactly-once semantics
//!
//! The idempotency cache tracks processed messages to prevent duplicates
//! during recovery scenarios.

use crate::Error;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

/// Idempotency cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotencyConfig {
    /// Maximum number of entries in cache
    pub cache_size: usize,

    /// Time-to-live for entries
    pub ttl: Duration,

    /// Persistence file path (optional)
    pub persist_path: Option<String>,

    /// Interval for persisting to disk
    pub persist_interval: Duration,
}

impl Default for IdempotencyConfig {
    fn default() -> Self {
        Self {
            cache_size: 100_000,
            ttl: Duration::from_secs(24 * 60 * 60), // 24 hours
            persist_path: Some("/var/lib/arkflow/idempotency.json".to_string()),
            persist_interval: Duration::from_secs(60),
        }
    }
}

/// Idempotency entry with timestamp
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IdempotencyEntry {
    /// Timestamp when entry was created
    created_at: SystemTime,

    /// Number of times this key was accessed
    access_count: u64,
}

impl IdempotencyEntry {
    fn new() -> Self {
        Self {
            created_at: SystemTime::now(),
            access_count: 0,
        }
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        self.created_at.elapsed().unwrap_or_default().as_millis() > ttl.as_millis()
    }
}

/// In-memory idempotency cache with optional persistence
pub struct IdempotencyCache {
    cache: Arc<RwLock<LruCache<String, IdempotencyEntry>>>,
    config: IdempotencyConfig,
}

impl IdempotencyCache {
    /// Create a new idempotency cache
    pub fn new(config: IdempotencyConfig) -> Self {
        let capacity = NonZeroUsize::new(config.cache_size)
            .unwrap_or_else(|| unsafe { NonZeroUsize::new_unchecked(1) });

        Self {
            cache: Arc::new(RwLock::new(LruCache::new(capacity))),
            config,
        }
    }

    /// Check if a key has been processed and mark it as processed
    ///
    /// Returns Ok(true) if the key was already processed (duplicate)
    /// Returns Ok(false) if this is the first time seeing the key
    pub async fn check_and_mark(&self, key: &str) -> Result<bool, Error> {
        let mut cache = self.cache.write().await;

        // Check if key exists
        if let Some(entry) = cache.get(key) {
            // Check if expired
            if entry.is_expired(self.config.ttl) {
                // Remove expired entry and treat as new
                cache.pop(key);
                cache.put(key.to_string(), IdempotencyEntry::new());
                return Ok(false);
            }

            // Key exists and not expired - this is a duplicate
            return Ok(true);
        }

        // Mark as processed
        cache.put(key.to_string(), IdempotencyEntry::new());
        Ok(false)
    }

    /// Get the number of entries in the cache
    pub async fn len(&self) -> usize {
        self.cache.read().await.len()
    }

    /// Clear all entries
    pub async fn clear(&self) {
        self.cache.write().await.clear();
    }

    /// Remove expired entries
    pub async fn cleanup_expired(&self) {
        let mut cache = self.cache.write().await;
        let ttl = self.config.ttl;

        // Collect expired keys
        let expired_keys: Vec<String> = cache
            .iter()
            .filter(|(_, entry)| entry.is_expired(ttl))
            .map(|(key, _)| key.clone())
            .collect();

        // Remove expired entries
        let expired_count = expired_keys.len();
        for key in &expired_keys {
            cache.pop(key);
        }

        if !expired_keys.is_empty() {
            tracing::debug!("Cleaned up {} expired idempotency entries", expired_count);
        }
    }

    /// Persist cache to disk
    pub async fn persist(&self) -> Result<(), Error> {
        let persist_path = match &self.config.persist_path {
            Some(path) => path.clone(),
            None => return Ok(()),
        };

        let cache = self.cache.read().await;

        // Create a map for serialization
        let map: HashMap<String, (u64, u64)> = cache
            .iter()
            .map(|(key, entry)| {
                let timestamp = entry
                    .created_at
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                (key.clone(), (timestamp, entry.access_count))
            })
            .collect();

        // Serialize to JSON
        let json = serde_json::to_string_pretty(&map)
            .map_err(|e| Error::Process(format!("Failed to serialize idempotency cache: {}", e)))?;

        // Write to temp file first
        let temp_path = format!("{}.tmp", persist_path);
        let mut file = File::create(&temp_path)
            .await
            .map_err(|e| Error::Read(format!("Failed to create idempotency temp file: {}", e)))?;

        file.write_all(json.as_bytes())
            .await
            .map_err(|e| Error::Read(format!("Failed to write idempotency cache: {}", e)))?;

        file.sync_all()
            .await
            .map_err(|e| Error::Read(format!("Failed to sync idempotency cache: {}", e)))?;

        // Atomic rename
        tokio::fs::rename(&temp_path, &persist_path)
            .await
            .map_err(|e| Error::Read(format!("Failed to rename idempotency cache: {}", e)))?;

        tracing::debug!(
            "Persisted {} idempotency entries to {}",
            cache.len(),
            persist_path
        );
        Ok(())
    }

    /// Restore cache from disk
    pub async fn restore(&self) -> Result<(), Error> {
        let persist_path = match &self.config.persist_path {
            Some(path) => path.clone(),
            None => return Ok(()),
        };

        // Check if file exists
        if !std::path::Path::new(&persist_path).exists() {
            return Ok(());
        }

        // Read file
        let contents = tokio::fs::read_to_string(&persist_path)
            .await
            .map_err(|e| Error::Read(format!("Failed to read idempotency cache: {}", e)))?;

        // Deserialize
        let map: HashMap<String, (u64, u64)> = serde_json::from_str(&contents).map_err(|e| {
            Error::Process(format!("Failed to deserialize idempotency cache: {}", e))
        })?;

        let mut cache = self.cache.write().await;

        // Restore entries
        for (key, (timestamp, _access_count)) in map {
            let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(timestamp);

            // Skip expired entries
            let entry = IdempotencyEntry {
                created_at,
                access_count: 0,
            };
            if !entry.is_expired(self.config.ttl) {
                cache.put(key, entry);
            }
        }

        tracing::info!(
            "Restored {} idempotency entries from {}",
            cache.len(),
            persist_path
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_idempotency_check_and_mark() {
        let config = IdempotencyConfig::default();
        let cache = IdempotencyCache::new(config);

        // First check - not processed
        let is_duplicate = cache.check_and_mark("key1").await.unwrap();
        assert!(!is_duplicate);

        // Second check - should be marked as processed
        let is_duplicate = cache.check_and_mark("key1").await.unwrap();
        assert!(is_duplicate);
    }

    #[tokio::test]
    async fn test_idempotency_multiple_keys() {
        let config = IdempotencyConfig::default();
        let cache = IdempotencyCache::new(config);

        assert!(!cache.check_and_mark("key1").await.unwrap());
        assert!(!cache.check_and_mark("key2").await.unwrap());
        assert!(cache.check_and_mark("key1").await.unwrap());
        assert!(cache.check_and_mark("key2").await.unwrap());
    }

    #[tokio::test]
    async fn test_idempotency_cache_size() {
        let config = IdempotencyConfig {
            cache_size: 2,
            ..Default::default()
        };
        let cache = IdempotencyCache::new(config);

        cache.check_and_mark("key1").await.unwrap();
        cache.check_and_mark("key2").await.unwrap();
        assert_eq!(cache.len().await, 2);

        // Adding third key should evict oldest
        cache.check_and_mark("key3").await.unwrap();
        assert_eq!(cache.len().await, 2);

        // key1 should have been evicted
        assert!(!cache.check_and_mark("key1").await.unwrap());
    }

    #[tokio::test]
    async fn test_idempotency_cleanup_expired() {
        let config = IdempotencyConfig {
            ttl: Duration::from_millis(100),
            ..Default::default()
        };
        let cache = IdempotencyCache::new(config);

        cache.check_and_mark("key1").await.unwrap();
        assert_eq!(cache.len().await, 1);

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(150)).await;

        cache.cleanup_expired().await;
        assert_eq!(cache.len().await, 0);
    }

    #[tokio::test]
    async fn test_idempotency_persistence() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let persist_path = temp_dir.path().join("idempotency.json");
        let config = IdempotencyConfig {
            persist_path: Some(persist_path.to_str().unwrap().to_string()),
            ..Default::default()
        };

        let cache1 = IdempotencyCache::new(config);

        // Add some entries
        cache1.check_and_mark("key1").await.unwrap();
        cache1.check_and_mark("key2").await.unwrap();

        // Persist
        cache1.persist().await.unwrap();

        // Create new cache and restore
        let config2 = IdempotencyConfig {
            persist_path: Some(persist_path.to_str().unwrap().to_string()),
            ..Default::default()
        };
        let cache2 = IdempotencyCache::new(config2);
        cache2.restore().await.unwrap();

        // Check that entries were restored
        assert!(cache2.check_and_mark("key1").await.unwrap());
        assert!(cache2.check_and_mark("key2").await.unwrap());
        assert!(!cache2.check_and_mark("key3").await.unwrap());
    }
}
