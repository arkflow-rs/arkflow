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
use std::time::Duration;

/// How long a pooled-context acquisition may wait before failing. The pool
/// is sized for the processor's worker parallelism, so an exhausted pool
/// means entries leaked on error paths (a bug this module's guard prevents)
/// or an undersized pool; either way an unbounded busy-wait would park the
/// chain silently, which the kernel's no-silent-park contract forbids.
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(10);

/// SessionContext object pool
///
/// This pool manages a fixed number of SessionContext instances
/// that can be reused across SQL operations, significantly reducing
/// the overhead of context creation.
pub struct SessionContextPool {
    contexts: Vec<Arc<SessionContext>>,
    available: std::sync::Mutex<Vec<usize>>,
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
            available: std::sync::Mutex::new(available),
        })
    }

    fn pop_available(&self) -> Option<usize> {
        self.available.lock().expect("context pool lock").pop()
    }

    fn push_available(&self, index: usize) {
        self.available
            .lock()
            .expect("context pool lock")
            .push(index);
    }

    /// Wait for a free slot until the acquisition deadline. An exhausted
    /// pool is a leak or a sizing bug — surface it as an explicit failure
    /// instead of busy-waiting forever.
    async fn wait_for_slot(&self) -> Result<usize, Error> {
        let deadline = tokio::time::Instant::now() + ACQUIRE_TIMEOUT;
        loop {
            if let Some(index) = self.pop_available() {
                return Ok(index);
            }
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(
                    pool_size = self.contexts.len(),
                    waited = ?ACQUIRE_TIMEOUT,
                    "session context pool exhausted: contexts are being leaked on error paths or the pool is undersized"
                );
                return Err(Error::Process(format!(
                    "timed out after {ACQUIRE_TIMEOUT:?} waiting for a pooled SQL session \
                     context (pool size {}): repeated processing errors must not exhaust the pool",
                    self.contexts.len()
                )));
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Acquire a SessionContext from the pool, bounded by the acquisition
    /// deadline.
    ///
    /// Returns an Arc<SessionContext> that must be released back to the pool
    /// with `release_context()`. Prefer `acquire_guarded()`, whose guard
    /// releases on drop and cannot leak on error or cancellation paths.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use arkflow_plugin::context_pool::SessionContextPool;
    /// # async fn example(pool: SessionContextPool) -> Result<(), Box<dyn std::error::Error>> {
    /// let ctx = pool.acquire().await?;
    /// // Use the context...
    /// pool.release_context(ctx);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn acquire(&self) -> Result<Arc<SessionContext>, Error> {
        let index = self.wait_for_slot().await?;
        Ok(self.contexts[index].clone())
    }

    /// Acquire a SessionContext behind a cancellation-safe guard. The slot
    /// is returned when the guard drops — on the success path, on every
    /// error path (`?` early returns), and when the surrounding future is
    /// cancelled mid-flight — so a processing error can never leak pool
    /// entries and silently wedge the chain.
    pub async fn acquire_guarded(self: &Arc<Self>) -> Result<PooledContext, Error> {
        let index = self.wait_for_slot().await?;
        Ok(PooledContext {
            pool: self.clone(),
            context: self.contexts[index].clone(),
            index,
        })
    }

    /// Release a context obtained from [`SessionContextPool::acquire`].
    ///
    /// # Arguments
    ///
    /// * `context` - The context to release (must be from this pool)
    pub fn release_context(&self, context: Arc<SessionContext>) {
        // Find the index of this context
        for (i, ctx) in self.contexts.iter().enumerate() {
            if Arc::ptr_eq(ctx, &context) {
                self.push_available(i);
                return;
            }
        }
    }

    /// Get the current pool size
    pub fn pool_size(&self) -> usize {
        self.contexts.len()
    }

    /// Get the number of available contexts (not currently in use)
    pub fn available_count(&self) -> usize {
        self.available.lock().expect("context pool lock").len()
    }
}

/// A cancellation-safe lease on a pooled session context (see
/// [`SessionContextPool::acquire_guarded`]).
pub struct PooledContext {
    pool: Arc<SessionContextPool>,
    context: Arc<SessionContext>,
    index: usize,
}

impl std::ops::Deref for PooledContext {
    type Target = SessionContext;

    fn deref(&self) -> &Self::Target {
        &self.context
    }
}

impl PooledContext {
    /// The leased context as a plain `Arc`, for signatures that take
    /// `&Arc<SessionContext>`.
    pub fn context(&self) -> &Arc<SessionContext> {
        &self.context
    }
}

impl Drop for PooledContext {
    fn drop(&mut self) {
        self.pool.push_available(self.index);
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
        let count = pool.available_count();
        assert_eq!(count, 1);

        // Acquire another context
        let ctx2 = pool.acquire().await.unwrap();
        let count = pool.available_count();
        assert_eq!(count, 0);

        // Release one context
        pool.release_context(ctx1);
        let count = pool.available_count();
        assert_eq!(count, 1);

        // Release the other
        pool.release_context(ctx2);
        let count = pool.available_count();
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
                pool_clone.release_context(ctx);
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // All contexts should be returned to the pool
        let count = pool.available_count();
        assert_eq!(count, 4);
    }
}
