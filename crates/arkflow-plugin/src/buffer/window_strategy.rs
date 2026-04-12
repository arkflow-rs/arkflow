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

//! Window strategy trait for buffer implementations
//!
//! This module provides a unified interface for different windowing strategies
//! to reduce code duplication across tumbling, sliding, and session windows.

use std::time::Instant;

/// Window strategy trait
///
/// Defines how windows should be triggered and managed.
/// Different window types implement this trait with their specific logic.
pub trait WindowStrategy: Send + Sync {
    /// Check if the window should trigger based on current time and last trigger
    ///
    /// # Arguments
    ///
    /// * `current_time` - Current time instant
    /// * `last_trigger` - Time when the window was last triggered
    ///
    /// # Returns
    ///
    /// * `bool` - true if window should trigger, false otherwise
    fn should_trigger(&self, current_time: Instant, last_trigger: Instant) -> bool;

    /// Get the interval for window checking
    ///
    /// # Returns
    ///
    /// * `Duration` - How often to check if window should trigger
    fn check_interval(&self) -> std::time::Duration;

    /// Reset the strategy state (called after window triggers)
    ///
    /// Some strategies may need to reset state after triggering
    fn reset(&mut self);
}

/// Tumbling window strategy
///
/// Windows are fixed-size, non-overlapping, and consecutive.
/// Example: 5-minute windows trigger at 0, 5, 10, 15...
#[derive(Clone, Debug)]
pub struct TumblingWindowStrategy {
    /// Fixed duration of each window
    window_size: std::time::Duration,
}

impl TumblingWindowStrategy {
    /// Create a new tumbling window strategy
    ///
    /// # Arguments
    ///
    /// * `window_size` - Fixed duration of each window
    pub fn new(window_size: std::time::Duration) -> Self {
        Self { window_size }
    }
}

impl WindowStrategy for TumblingWindowStrategy {
    fn should_trigger(&self, current_time: Instant, last_trigger: Instant) -> bool {
        current_time.duration_since(last_trigger) >= self.window_size
    }

    fn check_interval(&self) -> std::time::Duration {
        // Check more frequently than window size to ensure timely triggers
        std::time::Duration::from_millis(self.window_size.as_millis().min(1000) as u64)
    }

    fn reset(&mut self) {
        // Tumbling windows don't need to reset state
        // Each trigger is independent
    }
}

/// Sliding window strategy
///
/// Windows overlap and slide forward by a fixed interval.
/// Example: 5-minute windows sliding every 1 minute
#[derive(Clone, Debug)]
pub struct SlidingWindowStrategy {
    /// Size of each window
    window_size: std::time::Duration,
    /// How often to slide the window
    slide_interval: std::time::Duration,
}

impl SlidingWindowStrategy {
    /// Create a new sliding window strategy
    ///
    /// # Arguments
    ///
    /// * `window_size` - Size of each window
    /// * `slide_interval` - How often to slide the window
    pub fn new(window_size: std::time::Duration, slide_interval: std::time::Duration) -> Self {
        Self {
            window_size,
            slide_interval,
        }
    }
}

impl WindowStrategy for SlidingWindowStrategy {
    fn should_trigger(&self, current_time: Instant, last_trigger: Instant) -> bool {
        current_time.duration_since(last_trigger) >= self.slide_interval
    }

    fn check_interval(&self) -> std::time::Duration {
        // Check at slide interval to ensure timely triggers
        self.slide_interval
    }

    fn reset(&mut self) {
        // Sliding windows don't need to reset state
    }
}

/// Session window strategy
///
/// Windows are dynamic and close after a period of inactivity (gap).
#[derive(Clone, Debug)]
pub struct SessionWindowStrategy {
    /// Gap duration of inactivity before closing a session
    gap_duration: std::time::Duration,
}

impl SessionWindowStrategy {
    /// Create a new session window strategy
    ///
    /// # Arguments
    ///
    /// * `gap_duration` - Period of inactivity before closing session
    pub fn new(gap_duration: std::time::Duration) -> Self {
        Self { gap_duration }
    }
}

impl WindowStrategy for SessionWindowStrategy {
    fn should_trigger(&self, current_time: Instant, last_trigger: Instant) -> bool {
        // Trigger when gap duration has passed since last activity
        current_time.duration_since(last_trigger) >= self.gap_duration
    }

    fn check_interval(&self) -> std::time::Duration {
        // Check frequently to detect session gaps quickly
        std::time::Duration::from_millis(self.gap_duration.as_millis().min(500) as u64)
    }

    fn reset(&mut self) {
        // Session windows reset on each new message
        // (managed externally via last_trigger updates)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_tumbling_window_strategy() {
        let strategy = TumblingWindowStrategy::new(Duration::from_secs(5));
        let base = Instant::now();

        // Should not trigger immediately
        assert!(!strategy.should_trigger(base, base));

        // Should trigger after 5 seconds
        assert!(strategy.should_trigger(base + Duration::from_secs(5), base));

        // Check interval should be reasonable
        assert!(strategy.check_interval() <= Duration::from_secs(1));
    }

    #[test]
    fn test_sliding_window_strategy() {
        let strategy = SlidingWindowStrategy::new(Duration::from_secs(5), Duration::from_secs(1));
        let base = Instant::now();

        // Should not trigger immediately
        assert!(!strategy.should_trigger(base, base));

        // Should trigger after 1 second (slide interval)
        assert!(strategy.should_trigger(base + Duration::from_secs(1), base));

        // Check interval should match slide interval
        assert_eq!(strategy.check_interval(), Duration::from_secs(1));
    }

    #[test]
    fn test_session_window_strategy() {
        let strategy = SessionWindowStrategy::new(Duration::from_secs(10));
        let base = Instant::now();

        // Should not trigger immediately
        assert!(!strategy.should_trigger(base, base));

        // Should trigger after 10 seconds of inactivity
        assert!(strategy.should_trigger(base + Duration::from_secs(10), base));

        // Check interval should be frequent
        assert!(strategy.check_interval() <= Duration::from_millis(500));
    }
}
