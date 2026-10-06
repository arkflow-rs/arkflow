//! Hub error surface: storage-unaware callers get a stable enum.

use crate::storage::StorageError;

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

#[cfg(test)]
mod tests {
    use super::HubError;
    use crate::storage::StorageError;

    /// Every variant lands in exactly one failure class: operators route on
    /// these strings, so an unknown variant must not silently invent one.
    #[test]
    fn failure_class_partitions_the_error_surface() {
        assert_eq!(HubError::Unauthorized.failure_class(), "authorization");
        assert_eq!(
            HubError::NodeUnavailable.failure_class(),
            "node_unavailable"
        );
        assert_eq!(HubError::NotFound.failure_class(), "not_found");
        // Both storage flavors aggregate into the repository class.
        assert_eq!(HubError::StorageUnavailable.failure_class(), "repository");
        assert_eq!(
            HubError::Storage("backend exploded".into()).failure_class(),
            "repository"
        );
        assert_eq!(
            HubError::StaleLeader {
                claimed: 3,
                current: 4
            }
            .failure_class(),
            "stale_leader"
        );
        assert_eq!(
            HubError::OrchestrationInProgress.failure_class(),
            "orchestration_in_progress"
        );
        assert_eq!(
            HubError::OrchestrationPhaseConflict.failure_class(),
            "orchestration_conflict"
        );
        // Everything else funnels into the shared invalid-request bucket.
        assert_eq!(HubError::Capacity.failure_class(), "invalid");
        assert_eq!(
            HubError::Invalid("bad input".into()).failure_class(),
            "invalid"
        );
        assert_eq!(
            HubError::GenerationConflict {
                expected: 1,
                current: 2
            }
            .failure_class(),
            "invalid"
        );
        assert_eq!(HubError::IdempotencyKeyReused.failure_class(), "invalid");
    }

    /// Storage errors translate losslessly where a stable Hub variant
    /// exists, and collapse into the opaque storage class otherwise.
    #[test]
    fn storage_errors_translate_into_stable_hub_variants() {
        assert!(matches!(
            HubError::from(StorageError::GenerationConflict {
                expected: 7,
                current: 9
            }),
            HubError::GenerationConflict {
                expected: 7,
                current: 9
            }
        ));
        assert!(matches!(
            HubError::from(StorageError::IdempotencyKeyReused),
            HubError::IdempotencyKeyReused
        ));
        assert!(matches!(
            HubError::from(StorageError::ActorClosed),
            HubError::StorageUnavailable
        ));
        assert!(matches!(
            HubError::from(StorageError::StaleLeader {
                claimed_epoch: 3,
                current_epoch: 4
            }),
            HubError::StaleLeader {
                claimed: 3,
                current: 4
            }
        ));
        // Backend-specific failures keep their message but lose their type.
        assert!(matches!(
            HubError::from(StorageError::Unsupported("pg-only path")),
            HubError::Storage(_)
        ));
        assert!(matches!(
            HubError::from(StorageError::Poisoned),
            HubError::Storage(_)
        ));
    }
}
