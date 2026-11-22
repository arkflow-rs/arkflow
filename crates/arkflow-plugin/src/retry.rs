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

//! Shared retry/backoff utilities for HTTP- and RPC-backed components.
//!
//! Moved here from `pulsar/common.rs` (consolidate-connector-skeleton): the
//! retry policy was validated in Pulsar but hand-rolled (or missing) in the
//! siblings — qdrant/milvus outputs kept two copy-pasted backoff loops with
//! an unbounded `2u64.pow` delay, and the AI processors had no retry at all.
//! Components must use this implementation instead of writing their own
//! loops (pinned by the connector skeleton hygiene test).

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Retry configuration for Pulsar operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Initial delay between retries (in milliseconds)
    pub initial_delay_ms: u64,
    /// Maximum delay between retries (in milliseconds)
    pub max_delay_ms: u64,
    /// Backoff multiplier for exponential backoff
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay_ms: 100,
            max_delay_ms: 5000,
            backoff_multiplier: 2.0,
        }
    }
}

/// Retry utility for Pulsar operations
pub struct RetryUtils;

impl RetryUtils {
    /// Execute an operation with retry logic
    pub async fn retry_with_backoff<F, Fut, T, E>(
        operation: F,
        config: &RetryConfig,
        operation_name: &str,
    ) -> Result<T, E>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let mut attempt = 0;
        let mut delay = Duration::from_millis(config.initial_delay_ms);

        loop {
            attempt += 1;

            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if attempt >= config.max_attempts {
                        tracing::error!(
                            "Failed {} after {} attempts: {}",
                            operation_name,
                            attempt,
                            e
                        );
                        return Err(e);
                    }

                    tracing::warn!(
                        "Attempt {} for {} failed, retrying in {:?}: {}",
                        attempt,
                        operation_name,
                        delay,
                        e
                    );

                    tokio::time::sleep(delay).await;

                    // Exponential backoff with cap
                    delay = Duration::from_millis(
                        (delay.as_millis() as f64 * config.backoff_multiplier)
                            .min(config.max_delay_ms as f64) as u64,
                    );
                }
            }
        }
    }
}
