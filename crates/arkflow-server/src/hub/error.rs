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
    #[error("durable storage is unavailable")]
    StorageUnavailable,
    #[error("desired state generation conflict: expected {expected}, current {current}")]
    GenerationConflict { expected: u64, current: u64 },
    #[error("idempotency key was already used for a different mutation")]
    IdempotencyKeyReused,
    #[error("storage error: {0}")]
    Storage(String),
}

impl HubError {
    pub fn failure_class(&self) -> &'static str {
        match self {
            Self::Unauthorized => "authorization",
            Self::NodeUnavailable => "node_unavailable",
            Self::StorageUnavailable | Self::Storage(_) => "repository",
            Self::NotFound => "not_found",
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
            other => Self::Storage(other.to_string()),
        }
    }
}
