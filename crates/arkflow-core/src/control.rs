//! Shared control-plane data types.
//!
//! This module intentionally contains serializable snapshots and command
//! results only. Runtime supervision remains separate so the HTTP layer can
//! consume stable data without owning Stream internals.

use serde::{Deserialize, Serialize};

/// Lifecycle state of a configured Stream runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamState {
    Created,
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
    Restarting,
}

/// A bounded recent runtime error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeErrorEvent {
    pub occurred_at_ms: u64,
    pub stage: String,
    pub message: String,
}

/// Runtime counters exposed by the control API and metrics endpoint.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamMetricsSnapshot {
    pub input_batches: u64,
    pub input_messages: u64,
    pub processing_errors: u64,
    pub output_batches: u64,
    pub output_messages: u64,
    pub input_errors: u64,
    pub input_reconnects: u64,
    pub output_errors: u64,
    pub restarts: u64,
}

/// Public snapshot for one Stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamStatus {
    pub id: String,
    pub state: StreamState,
    pub started_at_ms: Option<u64>,
    pub last_error: Option<RuntimeErrorEvent>,
    pub metrics: StreamMetricsSnapshot,
}

/// Process-level control-plane status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineStatus {
    pub version: String,
    pub state: String,
    pub uptime_seconds: u64,
    pub streams_total: usize,
    pub streams_running: usize,
    pub streams_failed: usize,
}

/// Result returned by a lifecycle command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationResult {
    pub operation: String,
    pub stream_id: String,
    pub state: StreamState,
    pub message: Option<String>,
}

/// Standard JSON error payload for control-plane routes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
}

/// Lifecycle or operational event retained for console polling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlEvent {
    pub occurred_at_ms: u64,
    pub event_type: String,
    pub stream_id: Option<String>,
    pub outcome: String,
    pub message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_types_use_stable_wire_names() {
        assert_eq!(
            serde_json::to_string(&StreamState::Restarting).unwrap(),
            "\"restarting\""
        );

        let error = ApiError {
            code: "not_found".into(),
            message: "missing".into(),
            field: None,
            stream_id: Some("orders".into()),
        };
        let value = serde_json::to_value(error).unwrap();
        assert_eq!(value["stream_id"], "orders");
        assert!(value.get("field").is_none());
    }
}
