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

//! Authentication middleware for account lockout and security
//!
//! This module provides account lockout functionality to prevent
//! brute force attacks and enhance security.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Failed login attempt tracking
#[derive(Clone)]
struct FailedLoginAttempt {
    pub count: u32,
    pub last_attempt: Instant,
    pub locked_until: Option<Instant>,
}

/// Authentication middleware
///
/// Tracks failed login attempts and implements account lockout
/// to prevent brute force attacks.
pub struct AuthMiddleware {
    failed_attempts: Arc<RwLock<HashMap<String, FailedLoginAttempt>>>,
    max_attempts: u32,
    lock_duration: Duration,
}

impl AuthMiddleware {
    /// Create a new authentication middleware
    ///
    /// # Arguments
    ///
    /// * `max_attempts` - Maximum number of failed attempts before lockout
    /// * `lock_duration` - How long to lock the account after max attempts
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use arkflow_plugin::auth_middleware::AuthMiddleware;
    /// use std::time::Duration;
    ///
    /// let middleware = AuthMiddleware::new(3, Duration::from_secs(300));
    /// ```
    pub fn new(max_attempts: u32, lock_duration: Duration) -> Self {
        Self {
            failed_attempts: Arc::new(RwLock::new(HashMap::new())),
            max_attempts,
            lock_duration,
        }
    }

    /// Record a failed login attempt
    ///
    /// # Arguments
    ///
    /// * `identifier` - Unique identifier for the account/IP (e.g., "user@192.168.1.1")
    pub async fn record_failure(&self, identifier: &str) {
        let mut attempts = self.failed_attempts.write().await;
        let entry = attempts
            .entry(identifier.to_string())
            .or_insert(Failed_login_attempt {
                count: 0,
                last_attempt: Instant::now(),
                locked_until: None,
            });

        entry.count += 1;
        entry.last_attempt = Instant::now();

        if entry.count >= self.max_attempts {
            entry.locked_until = Some(Instant::now() + self.lock_duration);
        }
    }

    /// Record a successful login attempt
    ///
    /// This resets the failed attempt counter for the account.
    ///
    /// # Arguments
    ///
    /// * `identifier` - Unique identifier for the account/IP
    pub async fn record_success(&self, identifier: &str) {
        let mut attempts = self.failed_attempts.write().await;
        attempts.remove(identifier);
    }

    /// Check if an account/IP is currently locked
    ///
    /// # Arguments
    ///
    /// * `identifier` - Unique identifier for the account/IP
    ///
    /// Returns true if locked, false otherwise
    pub async fn is_locked(&self, identifier: &str) -> bool {
        let attempts = self.failed_attempts.read().await;
        if let Some(entry) = attempts.get(identifier) {
            if let Some(locked_until) = entry.locked_until {
                if Instant::now() < locked_until {
                    return true;
                }
            }
        }
        false
    }

    /// Clean up expired lockouts
    ///
    /// Removes entries that have passed their lock duration.
    /// Should be called periodically to prevent memory buildup.
    pub async fn cleanup_expired(&self) {
        let mut attempts = self.failed_attempts.write().await;
        attempts.retain(|_, entry| {
            if let Some(locked_until) = entry.locked_until {
                if Instant::now() >= locked_until {
                    return false;
                }
            }
            true
        });
    }

    /// Get the number of failed attempts for an identifier
    ///
    /// # Arguments
    ///
    /// * `identifier` - Unique identifier for the account/IP
    ///
    /// Returns the count of failed attempts, or 0 if none
    pub async fn failed_count(&self, identifier: &str) -> u32 {
        let attempts = self.failed_attempts.read().await;
        attempts
            .get(identifier)
            .map(|entry| entry.count)
            .unwrap_or(0)
    }

    /// Get the remaining lock time in seconds
    ///
    /// # Arguments
    ///
    /// * `identifier` - Unique identifier for the account/IP
    ///
    /// Returns seconds remaining, or 0 if not locked
    pub async fn remaining_lock_time(&self, identifier: &str) -> u64 {
        let attempts = self.failed_attempts.read().await;
        if let Some(entry) = attempts.get(identifier) {
            if let Some(locked_until) = entry.locked_until {
                let now = Instant::now();
                if now < locked_until {
                    return (locked_until - now).as_secs();
                }
            }
        }
        0
    }
}

impl Clone for AuthMiddleware {
    fn clone(&self) -> Self {
        Self {
            failed_attempts: Arc::clone(&self.failed_attempts),
            max_attempts: self.max_attempts,
            lock_duration: self.lock_duration,
        }
    }
}

/// Check credentials from environment variables
///
/// This allows secure credential storage outside of configuration files.
///
/// # Arguments
///
/// * `username` - Username to look up
/// * `password` - Password to verify
///
/// # Returns true if credentials match, false otherwise
///
/// # Examples
///
/// ```rust,no_run
/// // Environment variables: ADMIN_USER=admin, ADMIN_PASSWORD=secret
/// let valid = check_credentials_from_env("admin", "secret");
/// ```
pub fn check_credentials_from_env(username: &str, password: &str) -> bool {
    // Convert username to uppercase for environment variable lookup
    let env_username = format!("{}_USER", username.to_uppercase().replace('-', "_"));
    let env_password = format!("{}_PASSWORD", username.to_uppercase().replace('-', "_"));

    if let (Ok(env_user), Ok(env_pass)) =
        (std::env::var(&env_username), std::env::var(&env_password))
    {
        username == env_user && password == env_pass
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_auth_middleware_lockout() {
        let middleware = AuthMiddleware::new(3, Duration::from_secs(300));

        let id = "user@192.168.1.1";

        // Record 2 failures - should not lock
        middleware.record_failure(id).await;
        middleware.record_failure(id).await;
        assert!(!middleware.is_locked(id).await);

        // Record 3rd failure - should lock
        middleware.record_failure(id).await;
        assert!(middleware.is_locked(id).await);

        // Verify lock time
        let remaining = middleware.remaining_lock_time(id).await;
        assert!(remaining > 0);

        // Reset with success
        middleware.record_success(id).await;
        assert!(!middleware.is_locked(id).await);
    }

    #[tokio::test]
    async fn test_auth_middleware_failed_count() {
        let middleware = AuthMiddleware::new(5, Duration::from_secs(600));

        let id = "admin@test";

        assert_eq!(middleware.failed_count(id).await, 0);

        middleware.record_failure(id).await;
        assert_eq!(middleware.failed_count(id).await, 1);

        middleware.record_failure(id).await;
        middleware.record_failure(id).await;
        assert_eq!(middleware.failed_count(id).await, 3);

        // Success resets count
        middleware.record_success(id).await;
        assert_eq!(middleware.failed_count(id).await, 0);
    }

    #[tokio::test]
    async fn test_auth_middleware_cleanup() {
        let middleware = AuthMiddleware::new(3, Duration::from_secs(1));

        let id = "test@localhost";

        // Lock an account
        middleware.record_failure(id).await;
        middleware.record_failure(id).await;
        middleware.record_failure(id).await;
        assert!(middleware.is_locked(id).await);

        // Wait for lock to expire
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Cleanup should remove expired entries
        middleware.cleanup_expired().await;

        // Should no longer be locked
        assert!(!middleware.is_locked(id).await);
        assert_eq!(middleware.failed_count(id).await, 0);
    }

    #[test]
    fn test_check_credentials_from_env() {
        // Set test environment variables
        std::env::set_var("TEST_USER", "testuser");
        std::env::set_var("TEST_PASSWORD", "testpass");

        // Test successful match
        assert!(check_credentials_from_env("test", "testpass"));
        assert!(check_credentials_from_env("TEST", "testpass"));

        // Test failed match
        assert!(!check_credentials_from_env("test", "wrongpass"));
        assert!(!check_credentials_from_env("wrong", "testpass"));

        // Cleanup
        std::env::remove_var("TEST_USER");
        stdenv::remove_var("TEST_PASSWORD");
    }

    #[test]
    fn test_check_credentials_from_env_missing() {
        // When env vars are not set, should return false
        assert!(!check_credentials_from_env("nonexistent", "anypassword"));
    }

    #[tokio::test]
    async fn test_auth_middleware_concurrent() {
        let middleware = Arc::new(AuthMiddleware::new(3, Duration::from_secs(10)));

        let mut handles = Vec::new();

        // Spawn multiple tasks trying to authenticate
        for i in 0..10 {
            let middleware_clone = Arc::clone(&middleware);
            let id = format!("user{}", i % 3); // Only 3 unique users

            let handle = tokio::spawn(async move {
                for _ in 0..2 {
                    middleware_clone.record_failure(&id).await;
                    // Simulate some processing
                    tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify lockouts
        assert!(middleware.is_locked("user0").await);
        assert!(middleware.is_locked("user1").await);
        assert!(middleware.is_locked("user2").await);
    }
}
