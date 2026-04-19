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

//! Common window infrastructure
//!
//! This module provides shared utilities for window implementations
//! to reduce code duplication across different window types.

use crate::buffer::join::JoinConfig;
use crate::buffer::window::BaseWindow;
use arkflow_core::{Error, Resource};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

/// Common window context
///
/// Provides shared infrastructure for window implementations including
/// timer management, notification, and cancellation handling.
pub struct CommonWindowContext {
    /// Notification mechanism for signaling between threads
    pub notify: Arc<Notify>,
    /// Token for cancellation of background tasks
    pub close_token: CancellationToken,
    /// Last time the window was triggered
    pub last_trigger: Arc<std::sync::RwLock<Instant>>,
}

impl CommonWindowContext {
    /// Create a new common window context
    ///
    /// # Returns
    ///
    /// A new CommonWindowContext instance
    pub fn new() -> Self {
        Self {
            notify: Arc::new(Notify::new()),
            close_token: CancellationToken::new(),
            last_trigger: Arc::new(std::sync::RwLock::new(Instant::now())),
        }
    }

    /// Start the background timer task
    ///
    /// This spawns a background task that periodically notifies waiters
    /// based on the check interval. The task runs until cancelled.
    ///
    /// # Arguments
    ///
    /// * `check_interval` - How often to check if window should trigger
    pub fn start_timer(&self, check_interval: std::time::Duration) {
        let notify = Arc::clone(&self.notify);
        let close = self.close_token.clone();

        tokio::spawn(async move {
            loop {
                let timer = sleep(check_interval);
                tokio::select! {
                    _ = timer => {
                        notify.notify_waiters();
                    }
                    _ = close.cancelled() => {
                        notify.notify_waiters();
                        break;
                    }
                    _ = notify.notified() => {
                        if close.is_cancelled(){
                            break;
                        }
                    }
                }
            }
        });
    }

    /// Update the last trigger time
    ///
    /// # Arguments
    ///
    /// * `time` - The new last trigger time
    pub fn update_last_trigger(&self, time: Instant) {
        if let Ok(mut last) = self.last_trigger.write() {
            *last = time;
        }
    }

    /// Get the last trigger time
    ///
    /// # Returns
    ///
    /// The last trigger time
    pub fn get_last_trigger(&self) -> Instant {
        self.last_trigger
            .read()
            .map(|t| *t)
            .unwrap_or_else(|_| Instant::now())
    }

    /// Check if the window is closed
    ///
    /// # Returns
    ///
    /// * `bool` - true if closed, false otherwise
    pub fn is_closed(&self) -> bool {
        self.close_token.is_cancelled()
    }

    /// Close the window context
    ///
    /// Cancels the background timer task
    pub fn close(&self) {
        self.close_token.cancel();
    }

    /// Create a BaseWindow with join support
    ///
    /// # Arguments
    ///
    /// * `join_config` - Optional join configuration
    /// * `gap` - Time interval for the timer
    /// * `resource` - Resource reference
    ///
    /// # Returns
    ///
    /// A BaseWindow instance or an error
    pub fn create_base_window(
        &self,
        join_config: Option<JoinConfig>,
        gap: std::time::Duration,
        resource: &Resource,
    ) -> Result<BaseWindow, Error> {
        BaseWindow::new(
            join_config,
            Arc::clone(&self.notify),
            self.close_token.clone(),
            gap,
            resource,
        )
    }
}

impl Default for CommonWindowContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_common_window_context_creation() {
        let ctx = CommonWindowContext::new();
        assert!(!ctx.is_closed());
        assert!(ctx.get_last_trigger() <= Instant::now());
    }

    #[test]
    fn test_common_window_context_close() {
        let ctx = CommonWindowContext::new();
        assert!(!ctx.is_closed());
        ctx.close();
        assert!(ctx.is_closed());
    }

    #[test]
    fn test_common_window_context_update_trigger() {
        let ctx = CommonWindowContext::new();
        let now = Instant::now();
        ctx.update_last_trigger(now);
        assert!(ctx.get_last_trigger() >= now);
    }

    #[test]
    fn test_common_window_context_default() {
        let ctx = CommonWindowContext::default();
        assert!(!ctx.is_closed());
    }
}
