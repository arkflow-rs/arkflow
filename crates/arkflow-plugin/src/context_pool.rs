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

//! SessionContext object pool for SQL processors
//!
//! This module provides a pooling mechanism for DataFusion SessionContext
//! to avoid the overhead of repeatedly creating new contexts.

use arkflow_core::Error;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use tokio::sync::Mutex;

/// SessionContext object pool
///
/// This pool manages a fixed number of SessionContext instances
/// that can be reused across SQL operations, significantly reducing
/// the overhead of context creation.
pub struct SessionContextPool {
    contexts: Vec<Arc<SessionContext>>,
    available: Arc<Mutex<Vec<usize>>>,
}

impl SessionContextPool {
    /// Create a new SessionContext pool
    ///
    /// # Arguments
    ///
    /// * `pool_size` - Number of SessionContext instances to maintain
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use arkflow_plugin::context_pool::SessionContextPool;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = SessionContextPool::new(4)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(pool_size: usize) -> Result<Self, Error> {
        if pool_size == 0 {
            return Err(Error::Config(
                "Pool size must be greater than 0".to_string(),
            ));
        }

        let mut contexts = Vec::with_capacity(pool_size);
        let mut available = Vec::with_capacity(pool_size);

        for i in 0..pool_size {
            let ctx = create_session_context()?;
            contexts.push(Arc::new(ctx));
            available.push(i);
        }

        Ok(Self {
            contexts,
            available: Arc::new(Mutex::new(available)),
        })
    }

    /// Acquire a SessionContext from the pool
    ///
    /// This method will wait until a context is available.
    /// Returns an Arc<SessionContext> that should be released back to the pool
    /// when done by calling `release_context()`.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use arkflow_plugin::context_pool::SessionContextPool;
    /// # async fn example(pool: SessionContextPool) -> Result<(), Box<dyn std::error::Error>> {
    /// let ctx = pool.acquire().await?;
    /// // Use the context...
    /// pool.release_context(ctx).await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn acquire(&self) -> Result<Arc<SessionContext>, Error> {
        loop {
            // Try to get an available context
            let mut available = self.available.lock().await;
            if let Some(index) = available.pop() {
                return Ok(self.contexts[index].clone());
            }

            // No contexts available, wait a bit and retry
            drop(available);
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        }
    }

    /// Release a context back to the pool
    ///
    /// # Arguments
    ///
    /// * `context` - The context to release (must be from this pool)
    pub async fn release_context(&self, context: Arc<SessionContext>) {
        // Find the index of this context
        for (i, ctx) in self.contexts.iter().enumerate() {
            if Arc::ptr_eq(ctx, &context) {
                let mut available = self.available.lock().await;
                available.push(i);
                return;
            }
        }
    }

    /// Get the current pool size
    pub fn pool_size(&self) -> usize {
        self.contexts.len()
    }

    /// Get the number of available contexts (not currently in use)
    pub async fn available_count(&self) -> usize {
        let available = self.available.lock().await;
        available.len()
    }
}

/// Create a new SessionContext with default configuration
///
/// This is the same function used by SQL processors, ensuring
/// consistency across the application.
fn create_session_context() -> Result<SessionContext, Error> {
    crate::component::sql::create_session_context()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_creation() {
        let pool = SessionContextPool::new(4).unwrap();
        assert_eq!(pool.pool_size(), 4);
    }

    #[test]
    fn test_pool_invalid_size() {
        let result = SessionContextPool::new(0);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pool_acquire() {
        let pool = SessionContextPool::new(2).unwrap();

        // Acquire a context
        let ctx1 = pool.acquire().await.unwrap();
        let count = pool.available_count().await;
        assert_eq!(count, 1);

        // Acquire another context
        let ctx2 = pool.acquire().await.unwrap();
        let count = pool.available_count().await;
        assert_eq!(count, 0);

        // Release one context
        pool.release_context(ctx1).await;
        let count = pool.available_count().await;
        assert_eq!(count, 1);

        // Release the other
        pool.release_context(ctx2).await;
        let count = pool.available_count().await;
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_pool_concurrent_usage() {
        let pool = Arc::new(SessionContextPool::new(4).unwrap());

        let mut handles = Vec::new();

        // Spawn multiple tasks using the pool
        for _ in 0..10 {
            let pool_clone = Arc::clone(&pool);
            let handle = tokio::spawn(async move {
                let ctx = pool_clone.acquire().await.unwrap();
                // Simulate some work
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                pool_clone.release_context(ctx).await;
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // All contexts should be returned to the pool
        let count = pool.available_count().await;
        assert_eq!(count, 4);
    }
}
