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

/// Retry configuration for retrying components.
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
    /// Optional de-correlation jitter as a fraction of the delay (0.0-1.0).
    /// The effective sleep is `delay × (1 − uniform(0, jitter))`, so the
    /// backoff never exceeds the un-jittered value; 0 (the default) keeps
    /// the exact exponential curve.
    #[serde(default)]
    pub jitter: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay_ms: 100,
            max_delay_ms: 5000,
            backoff_multiplier: 2.0,
            jitter: 0.0,
        }
    }
}

/// Whether an HTTP status code is worth retrying (connector-resilience
/// contract): 429 (rate limited) and 5xx (server-side/availability) are
/// transient; every other 4xx is a deterministic client rejection.
// Adopters wire this in the same change (qdrant/milvus outputs, AI
// processors); remove the allow once they land.
#[allow(dead_code)]
pub(crate) fn is_retryable_status(status: u16) -> bool {
    status == 429 || (500..=599).contains(&status)
}

/// Error wrapper that tells [`RetryUtils`] whether a failure is worth
/// retrying. HTTP-backed components wrap status/transport failures here:
/// `retryable: false` aborts the retry loop immediately with the error.
pub struct OperationError<E> {
    /// The underlying failure (kept verbatim so the reason survives).
    pub error: E,
    /// Whether retrying on a fresh attempt/connection can succeed.
    pub retryable: bool,
}

impl<E: std::fmt::Display> std::fmt::Display for OperationError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

/// Apply the configured jitter: `delay × (1 − uniform(0, jitter))`, clamped
/// at the floor so the sleep stays non-negative. `rng` returns values in
/// `[0, 1)` and is injectable for tests.
fn apply_jitter(delay_ms: f64, jitter: f64, rng: impl Fn() -> f64) -> f64 {
    if jitter <= 0.0 {
        return delay_ms;
    }
    let fraction = rng() * jitter.min(1.0);
    delay_ms * (1.0 - fraction)
}

/// Next backoff delay: exponential growth capped at `max_delay_ms`, then
/// jittered. Multiplicative float arithmetic means no power overflow for
/// any attempt count — arbitrary user-configured `max_attempts` values are
/// safe (connector-resilience contract).
fn next_delay(current_ms: f64, config: &RetryConfig, rng: impl Fn() -> f64) -> Duration {
    let grown = current_ms * config.backoff_multiplier;
    let capped = grown.min(config.max_delay_ms as f64);
    let jittered = apply_jitter(capped, config.jitter, rng);
    Duration::from_millis(jittered.max(0.0) as u64)
}

/// Retry utility for retrying components.
pub struct RetryUtils;

impl RetryUtils {
    /// Execute an operation with retry logic. Every failure is retried up
    /// to `max_attempts`; use [`RetryUtils::retry_with_backoff_classified`]
    /// when deterministic failures must abort the loop.
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
        let rng = || {
            #[cfg(test)]
            {
                0.0
            }
            #[cfg(not(test))]
            rand::Rng::random(&mut rand::rng())
        };
        let mut attempt = 0;
        let mut current_ms = config.initial_delay_ms as f64;

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

                    let delay = next_delay(current_ms, config, rng);
                    current_ms = delay.as_millis() as f64;

                    tracing::warn!(
                        "Attempt {} for {} failed, retrying in {:?}: {}",
                        attempt,
                        operation_name,
                        delay,
                        e
                    );

                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Like [`RetryUtils::retry_with_backoff`], but the operation classifies
    /// its own failures: a [`OperationError`] with `retryable: false` (a
    /// 4xx rejection, a decode error, …) aborts the loop immediately with
    /// the original error; `retryable: true` failures keep retrying.
    pub async fn retry_with_backoff_classified<F, Fut, T, E>(
        operation: F,
        config: &RetryConfig,
        operation_name: &str,
    ) -> Result<T, E>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, OperationError<E>>>,
        E: std::fmt::Display,
    {
        let rng = || {
            #[cfg(test)]
            {
                0.0
            }
            #[cfg(not(test))]
            rand::Rng::random(&mut rand::rng())
        };
        let mut attempt = 0;
        let mut current_ms = config.initial_delay_ms as f64;

        loop {
            attempt += 1;

            match operation().await {
                Ok(result) => return Ok(result),
                Err(OperationError {
                    error,
                    retryable: false,
                }) => {
                    tracing::error!(
                        "{} failed deterministically (not retrying): {}",
                        operation_name,
                        error
                    );
                    return Err(error);
                }
                Err(OperationError {
                    error,
                    retryable: true,
                }) => {
                    if attempt >= config.max_attempts {
                        tracing::error!(
                            "Failed {} after {} attempts: {}",
                            operation_name,
                            attempt,
                            error
                        );
                        return Err(error);
                    }

                    let delay = next_delay(current_ms, config, rng);
                    current_ms = delay.as_millis() as f64;

                    tracing::warn!(
                        "Attempt {} for {} failed, retrying in {:?}: {}",
                        attempt,
                        operation_name,
                        delay,
                        error
                    );

                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_status_classification() {
        // Transient per the connector-resilience contract.
        for status in [429u16, 500, 502, 503, 599] {
            assert!(is_retryable_status(status), "should retry: {status}");
        }
        // Deterministic client rejections.
        for status in [400u16, 401, 403, 404, 408, 422, 499] {
            assert!(!is_retryable_status(status), "should not retry: {status}");
        }
    }

    #[test]
    fn backoff_delay_caps_and_never_overflows() {
        let config = RetryConfig {
            max_attempts: 10_000,
            initial_delay_ms: 100,
            max_delay_ms: 5_000,
            backoff_multiplier: 2.0,
            jitter: 0.0,
        };
        // Walk the recurrence far past any sane attempt count: the delay
        // saturates at max_delay_ms and the u64 conversion cannot overflow.
        let mut current = config.initial_delay_ms as f64;
        for _ in 0..100 {
            current = next_delay(current, &config, || 0.0).as_millis() as f64;
            assert!(current <= 5_000.0);
        }
        assert_eq!(current, 5_000.0);
    }

    #[test]
    fn jitter_shrinks_or_keeps_delay_and_stays_bounded() {
        // rng returning 0.0 keeps the exact delay; rng returning 0.999 with
        // jitter 1.0 approaches zero but never goes negative or exceeds the
        // un-jittered value.
        assert_eq!(apply_jitter(100.0, 0.0, || 0.9), 100.0);
        assert_eq!(apply_jitter(100.0, 0.5, || 0.0), 100.0);
        let shrunk = apply_jitter(100.0, 1.0, || 0.999);
        assert!(
            (0.0..=100.0).contains(&shrunk),
            "jittered delay out of range: {shrunk}"
        );
        // jitter > 1 clamps to 1.0; rng = 1.0 then shrinks the delay to the
        // floor (never negative — the caller clamps at 0).
        assert_eq!(apply_jitter(100.0, 2.0, || 1.0), 0.0);
    }

    #[tokio::test]
    async fn classified_retry_stops_on_deterministic_failure() {
        let config = RetryConfig {
            max_attempts: 5,
            initial_delay_ms: 1,
            max_delay_ms: 2,
            backoff_multiplier: 2.0,
            jitter: 0.0,
        };
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = std::sync::Arc::clone(&calls);
        let result: Result<(), String> = RetryUtils::retry_with_backoff_classified(
            move || {
                let counter = std::sync::Arc::clone(&counter);
                std::future::ready(Err(OperationError {
                    error: {
                        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        "rejected".to_string()
                    },
                    retryable: false,
                }))
            },
            &config,
            "deterministic op",
        )
        .await;
        assert_eq!(result.unwrap_err(), "rejected");
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "deterministic failure must not be retried"
        );
    }

    #[tokio::test]
    async fn classified_retry_exhausts_transient_failures() {
        let config = RetryConfig {
            max_attempts: 3,
            initial_delay_ms: 1,
            max_delay_ms: 2,
            backoff_multiplier: 2.0,
            jitter: 0.0,
        };
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = std::sync::Arc::clone(&calls);
        let result: Result<u8, String> = RetryUtils::retry_with_backoff_classified(
            move || {
                let counter = std::sync::Arc::clone(&counter);
                std::future::ready(Err(OperationError {
                    error: {
                        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        "503".to_string()
                    },
                    retryable: true,
                }))
            },
            &config,
            "transient op",
        )
        .await;
        assert_eq!(result.unwrap_err(), "503");
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "transient failures retry up to max_attempts"
        );
    }

    #[tokio::test]
    async fn any_attempt_count_is_safe_from_overflow() {
        // Spec: retry_count: 64 (or any value) must not panic or wrap.
        let config = RetryConfig {
            max_attempts: 64,
            initial_delay_ms: 1,
            max_delay_ms: 1,
            backoff_multiplier: 2.0,
            jitter: 0.0,
        };
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = std::sync::Arc::clone(&calls);
        let result: Result<(), String> = RetryUtils::retry_with_backoff(
            move || {
                let counter = std::sync::Arc::clone(&counter);
                std::future::ready(Err({
                    counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    "down".to_string()
                }))
            },
            &config,
            "overflow probe",
        )
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 64);
    }
}
