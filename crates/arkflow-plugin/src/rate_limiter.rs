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

//! Rate limiting utilities for HTTP inputs
//!
//! This module provides simple rate limiting functionality to prevent abuse.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

/// Simple rate limiter for HTTP requests
///
/// This provides a thread-safe rate limiter using a token bucket algorithm.
pub struct SimpleRateLimiter {
    requests_per_second: u32,
    burst_size: u32,
    state: Arc<RateLimiterState>,
}

struct RateLimiterState {
    tokens: AtomicU32,
    last_update: AtomicU64, // Stores nanoseconds since epoch
}

impl SimpleRateLimiter {
    /// Create a new rate limiter with specified parameters
    ///
    /// # Arguments
    ///
    /// * `requests_per_second` - Maximum number of requests allowed per second
    /// * `burst_size` - Maximum number of requests allowed in a burst
    pub fn new(requests_per_second: u32, burst_size: u32) -> Self {
        Self {
            requests_per_second,
            burst_size,
            state: Arc::new(RateLimiterState {
                tokens: AtomicU32::new(burst_size),
                last_update: AtomicU64::new(Self::now_nanos()),
            }),
        }
    }

    /// Check if a request is allowed
    ///
    /// Returns `true` if the request is within the rate limit,
    /// `false` if the rate limit has been exceeded.
    pub fn check(&self) -> bool {
        self.refill_tokens();

        // Try to consume one token
        let mut current = self.state.tokens.load(Ordering::Relaxed);
        loop {
            if current == 0 {
                return false;
            }
            let new = current - 1;
            match self.state.tokens.compare_exchange(
                current,
                new,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    /// Get the number of requests remaining in the current period
    pub fn remaining_capacity(&self) -> u32 {
        self.refill_tokens();
        self.state.tokens.load(Ordering::Relaxed)
    }

    fn refill_tokens(&self) {
        let now = Self::now_nanos();
        let last_update = self.state.last_update.load(Ordering::Relaxed);

        if now <= last_update {
            return;
        }

        let elapsed_nanos = now - last_update;
        let elapsed_seconds = elapsed_nanos as f64 / 1_000_000_000.0;

        // Calculate how many tokens to add based on elapsed time
        let tokens_to_add = (elapsed_seconds * self.requests_per_second as f64) as u32;

        if tokens_to_add == 0 {
            return;
        }

        // Update tokens, but don't exceed burst_size
        let current = self.state.tokens.load(Ordering::Relaxed);
        let new_tokens = std::cmp::min(current + tokens_to_add, self.burst_size);

        self.state.tokens.store(new_tokens, Ordering::Relaxed);
        self.state.last_update.store(now, Ordering::Relaxed);
    }

    fn now_nanos() -> u64 {
        use std::time::SystemTime;
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}

impl Clone for SimpleRateLimiter {
    fn clone(&self) -> Self {
        Self {
            requests_per_second: self.requests_per_second,
            burst_size: self.burst_size,
            state: Arc::clone(&self.state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_basic() {
        let limiter = SimpleRateLimiter::new(10, 20);

        // First few requests should be allowed
        for _ in 0..10 {
            assert!(limiter.check(), "Requests within limit should pass");
        }

        // Check remaining capacity
        let remaining = limiter.remaining_capacity();
        assert!(remaining >= 10);
    }

    #[test]
    fn test_rate_limiter_burst() {
        let limiter = SimpleRateLimiter::new(5, 10);

        // Should allow burst_size requests
        let mut count = 0;
        for _ in 0..15 {
            if limiter.check() {
                count += 1;
            }
        }

        assert_eq!(count, 10, "Should allow exactly burst_size requests");
    }

    #[test]
    fn test_rate_limiter_clone() {
        let limiter1 = SimpleRateLimiter::new(5, 10);
        let limiter2 = limiter1.clone();

        // Both limiters should share the same state
        assert!(limiter1.check());
        assert!(limiter2.check());

        let remaining1 = limiter1.remaining_capacity();
        let remaining2 = limiter2.remaining_capacity();
        assert_eq!(remaining1, remaining2);
        assert!(remaining1 < 10);
    }

    #[test]
    fn test_rate_limiter_remaining_capacity() {
        let limiter = SimpleRateLimiter::new(10, 20);

        // Make some requests
        for _ in 0..5 {
            limiter.check();
        }

        let remaining = limiter.remaining_capacity();
        assert!(remaining >= 15);
    }
}
