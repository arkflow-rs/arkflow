//! Session window example using ArkFlow state management
//!
//! This example demonstrates how to implement session window aggregations
//! with inactivity timeouts and stateful processing.

use arkflow_core::state::{EnhancedStateConfig, EnhancedStateManager, StateBackendType};
use arkflow_core::{Error, MessageBatch};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::time::{sleep, Instant};

/// Session event data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionEvent {
    pub session_id: String,
    pub user_id: String,
    pub event_type: String,
    pub timestamp: u64,
    pub data: serde_json::Value,
}

/// Session window state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionWindow {
    pub session_id: String,
    pub user_id: String,
    pub start_time: u64,
    pub end_time: u64,
    pub event_count: u32,
    pub last_event_type: String,
    pub custom_metrics: HashMap<String, f64>,
}

impl SessionWindow {
    pub fn new(session_id: String, user_id: String, timestamp: u64) -> Self {
        Self {
            session_id,
            user_id,
            start_time: timestamp,
            end_time: timestamp,
            event_count: 0,
            last_event_type: String::new(),
            custom_metrics: HashMap::new(),
        }
    }

    pub fn update(&mut self, event: &SessionEvent) {
        self.end_time = self.end_time.max(event.timestamp);
        self.event_count += 1;
        self.last_event_type = event.event_type.clone();

        // Update custom metrics
        if let Some(duration) = event.data.get("duration_ms").and_then(|v| v.as_f64()) {
            *self
                .custom_metrics
                .entry("total_duration".to_string())
                .or_insert(0.0) += duration;
        }

        if let Some(value) = event.data.get("value").and_then(|v| v.as_f64()) {
            *self
                .custom_metrics
                .entry("total_value".to_string())
                .or_insert(0.0) += value;
        }
    }

    pub fn duration_ms(&self) -> u64 {
        self.end_time - self.start_time
    }

    pub fn is_expired(&self, current_time: u64, timeout_ms: u64) -> bool {
        current_time.saturating_sub(self.end_time) > timeout_ms
    }
}

/// Session window processor with state management
pub struct SessionWindowProcessor {
    state_manager: Arc<RwLock<EnhancedStateManager>>,
    operator_id: String,
    session_timeout_ms: u64,
}

impl SessionWindowProcessor {
    pub fn new(
        state_manager: Arc<RwLock<EnhancedStateManager>>,
        operator_id: String,
        session_timeout_ms: u64,
    ) -> Self {
        Self {
            state_manager,
            operator_id,
            session_timeout_ms,
        }
    }

    /// Get active session count
    pub async fn get_active_session_count(&self) -> Result<usize, Error> {
        let state_manager = self.state_manager.read().await;
        state_manager
            .get_state_value(&self.operator_id, &"active_sessions")
            .await
    }

    /// Get session window by ID
    pub async fn get_session(&self, session_id: &str) -> Result<Option<SessionWindow>, Error> {
        let state_manager = self.state_manager.read().await;
        state_manager
            .get_state_value(&self.operator_id, &format!("session_{}", session_id))
            .await
    }

    /// Get all expired sessions
    pub async fn get_expired_sessions(
        &self,
        current_time: u64,
    ) -> Result<Vec<SessionWindow>, Error> {
        let state_manager = self.state_manager.read().await;
        let mut expired = Vec::new();

        // In a real implementation, you'd maintain a list of active sessions
        // For this example, we'll check a prefix
        // Note: This is inefficient - production code would use a better data structure
        if let Some(active_sessions) = state_manager
            .get_state_value::<Vec<String>>(&self.operator_id, &"active_session_list")
            .await?
        {
            for session_id in active_sessions {
                if let Some(window) = state_manager
                    .get_state_value::<SessionWindow>(
                        &self.operator_id,
                        &format!("session_{}", session_id),
                    )
                    .await?
                {
                    if window.is_expired(current_time, self.session_timeout_ms) {
                        expired.push(window);
                    }
                }
            }
        }

        Ok(expired)
    }

    /// Clean up expired sessions
    pub async fn cleanup_expired_sessions(&self) -> Result<Vec<SessionWindow>, Error> {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let expired = self.get_expired_sessions(current_time).await?;

        let mut state_manager = self.state_manager.write().await;

        // Remove expired sessions
        for window in &expired {
            state_manager
                .set_state_value(
                    &self.operator_id,
                    &format!("session_{}", window.session_id),
                    None::<SessionWindow>,
                )
                .await?;
        }

        // Update active session list
        let mut active_sessions: Vec<String> = state_manager
            .get_state_value(&self.operator_id, &"active_session_list")
            .await?
            .unwrap_or_default();

        active_sessions.retain(|session_id| !expired.iter().any(|w| w.session_id == *session_id));

        state_manager
            .set_state_value(&self.operator_id, &"active_session_list", active_sessions)
            .await?;

        // Update active count
        let count = state_manager
            .get_state_value::<usize>(&self.operator_id, &"active_sessions")
            .await?
            .unwrap_or(0)
            .saturating_sub(expired.len());

        state_manager
            .set_state_value(&self.operator_id, &"active_sessions", count)
            .await?;

        Ok(expired)
    }
}

#[async_trait::async_trait]
impl arkflow_core::processor::Processor for SessionWindowProcessor {
    async fn process(&self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        let mut results = Vec::new();
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Extract events from messages
        if let Ok(events_data) = batch.to_binary("__value__") {
            for event_data in events_data {
                let event: SessionEvent =
                    serde_json::from_slice(&event_data).map_err(|e| Error::Serialization(e))?;

                let mut state_manager = self.state_manager.write().await;

                // Get or create session window
                let mut window = state_manager
                    .get_state_value::<SessionWindow>(
                        &self.operator_id,
                        &format!("session_{}", event.session_id),
                    )
                    .await?
                    .unwrap_or_else(|| {
                        SessionWindow::new(
                            event.session_id.clone(),
                            event.user_id.clone(),
                            event.timestamp,
                        )
                    });

                // Update window
                window.update(&event);

                // Save updated window
                state_manager
                    .set_state_value(
                        &self.operator_id,
                        &format!("session_{}", event.session_id),
                        window.clone(),
                    )
                    .await?;

                // Update active session tracking
                let mut active_sessions: Vec<String> = state_manager
                    .get_state_value(&self.operator_id, &"active_session_list")
                    .await?
                    .unwrap_or_default();

                if !active_sessions.contains(&event.session_id) {
                    active_sessions.push(event.session_id.clone());
                    state_manager
                        .set_state_value(&self.operator_id, &"active_session_list", active_sessions)
                        .await?;
                }

                // Create result message
                let result = serde_json::to_vec(&window).map_err(|e| Error::Serialization(e))?;

                let result_batch = MessageBatch::new_binary(vec![result])?;
                results.push(result_batch);
            }
        }

        // Check for expired sessions
        if let Ok(expired) = self.cleanup_expired_sessions().await {
            if !expired.is_empty() {
                println!("Cleaned up {} expired sessions", expired.len());
            }
        }

        Ok(results)
    }

    async fn close(&self) -> Result<(), Error> {
        // Final cleanup of all sessions
        let expired = self.cleanup_expired_sessions().await?;
        println!("Final cleanup: {} sessions closed", expired.len());
        Ok(())
    }
}

/// Generate sample session events
pub fn generate_session_events() -> Vec<SessionEvent> {
    let mut events = Vec::new();
    let base_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    // Session 1: Active session with multiple events
    events.push(SessionEvent {
        session_id: "session_1".to_string(),
        user_id: "user_1".to_string(),
        event_type: "page_view".to_string(),
        timestamp: base_time,
        data: serde_json::json!({"page": "/home", "duration_ms": 1500}),
    });

    events.push(SessionEvent {
        session_id: "session_1".to_string(),
        user_id: "user_1".to_string(),
        event_type: "click".to_string(),
        timestamp: base_time + 2000,
        data: serde_json::json!({"element": "button", "value": 1.0}),
    });

    // Session 2: Short session
    events.push(SessionEvent {
        session_id: "session_2".to_string(),
        user_id: "user_2".to_string(),
        event_type: "page_view".to_string(),
        timestamp: base_time + 1000,
        data: serde_json::json!({"page": "/login", "duration_ms": 800}),
    });

    // Session 3: Will be expired (old timestamp)
    events.push(SessionEvent {
        session_id: "session_3".to_string(),
        user_id: "user_3".to_string(),
        event_type: "page_view".to_string(),
        timestamp: base_time - 70000, // 70 seconds ago
        data: serde_json::json!({"page": "/old", "duration_ms": 2000}),
    });

    events
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Initialize logging
    env_logger::init();

    // Create state manager
    let state_config = EnhancedStateConfig {
        enabled: true,
        backend_type: StateBackendType::Memory,
        checkpoint_interval_ms: 10000, // 10 seconds for demo
        exactly_once: true,
        ..Default::default()
    };

    let state_manager = Arc::new(RwLock::new(EnhancedStateManager::new(state_config).await?));

    // Create session window processor with 30-second timeout
    let processor = SessionWindowProcessor::new(
        state_manager.clone(),
        "session_window_operator".to_string(),
        30000, // 30 seconds
    );

    // Process events
    let events = generate_session_events();

    println!("Processing {} events...", events.len());

    for event in events {
        let event_data = serde_json::to_vec(&event).map_err(|e| Error::Serialization(e))?;

        let batch = MessageBatch::new_binary(vec![event_data])?;
        let results = processor.process(batch).await?;

        // Print results
        for result in results {
            if let Ok(windows) = result.to_binary("__value__") {
                for window_data in windows {
                    let window: SessionWindow = serde_json::from_slice(&window_data)?;
                    println!(
                        "Session {}: {} events, duration: {}ms",
                        window.session_id,
                        window.event_count,
                        window.duration_ms()
                    );
                }
            }
        }

        // Small delay between events
        sleep(Duration::from_millis(500)).await;
    }

    // Wait a bit more to trigger session expiration
    println!("\nWaiting for session expiration...");
    sleep(Duration::from_secs(2)).await;

    // Process one more event to trigger cleanup
    let event = SessionEvent {
        session_id: "session_4".to_string(),
        user_id: "user_4".to_string(),
        event_type: "page_view".to_string(),
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        data: serde_json::json!({"page": "/new", "duration_ms": 1000}),
    };

    let event_data = serde_json::to_vec(&event).map_err(|e| Error::Serialization(e))?;

    let batch = MessageBatch::new_binary(vec![event_data])?;
    processor.process(batch).await?;

    // Print final statistics
    let active_count = processor.get_active_session_count().await?;
    println!("\nFinal active session count: {:?}", active_count);

    // Check specific sessions
    if let Some(session1) = processor.get_session("session_1").await? {
        println!("Session 1 duration: {}ms", session1.duration_ms());
        println!(
            "Session 1 total duration: {:?}",
            session1.custom_metrics.get("total_duration")
        );
    }

    // Create final checkpoint
    let mut state_manager_write = state_manager.write().await;
    let checkpoint_id = state_manager_write.create_checkpoint().await?;
    println!("Created final checkpoint: {}", checkpoint_id);

    Ok(())
}
