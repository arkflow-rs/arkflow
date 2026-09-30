//! Hub error surface: storage-unaware callers get a stable enum.

use super::*;

#[derive(Debug, thiserror::Error)]
pub enum HubError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("node unavailable")]
    NodeUnavailable,
    #[error("resource not found")]
    NotFound,
    #[error("hub capacity exceeded")]
    Capacity,
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("an orchestration already owns this resource")]
    OrchestrationInProgress,
    #[error("the orchestration phase changed concurrently")]
    OrchestrationPhaseConflict,
    #[error("durable storage is unavailable")]
    StorageUnavailable,
    #[error("desired state generation conflict: expected {expected}, current {current}")]
    GenerationConflict { expected: u64, current: u64 },
    #[error("idempotency key was already used for a different mutation")]
    IdempotencyKeyReused,
    #[error("storage error: {0}")]
    Storage(String),
    #[error("stale leader: claimed epoch {claimed}, current lease epoch {current}")]
    StaleLeader { claimed: u64, current: u64 },
}

impl HubError {
    pub fn failure_class(&self) -> &'static str {
        match self {
            Self::Unauthorized => "authorization",
            Self::NodeUnavailable => "node_unavailable",
            Self::StorageUnavailable | Self::Storage(_) => "repository",
            Self::StaleLeader { .. } => "stale_leader",
            Self::NotFound => "not_found",
            Self::OrchestrationInProgress => "orchestration_in_progress",
            Self::OrchestrationPhaseConflict => "orchestration_conflict",
            _ => "invalid",
        }
    }
}

impl From<StorageError> for HubError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::GenerationConflict { expected, current } => {
                Self::GenerationConflict { expected, current }
            }
            StorageError::IdempotencyKeyReused => Self::IdempotencyKeyReused,
            StorageError::ActorClosed => Self::StorageUnavailable,
            StorageError::StaleLeader {
                claimed_epoch,
                current_epoch,
            } => Self::StaleLeader {
                claimed: claimed_epoch,
                current: current_epoch,
            },
            other => Self::Storage(other.to_string()),
        }
    }
}
