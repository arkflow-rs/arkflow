//! Compute-node Agent client for the Hub pull protocol.

mod checkpoint;
mod commands;
mod config;
mod kernel;
mod resources;
mod session;
#[cfg(test)]
mod tests;

pub use config::NodeAgentConfig;
pub use session::run;

// Paths referenced by `crate::hub`.
pub(crate) use checkpoint::{delete_checkpoint_artifact, recovery_record_is_valid};

// Names the moved test module reaches through `use super::*`.
#[cfg(test)]
use checkpoint::{
    checkpoint_repository, checkpoint_worker, parse_recovery_payload, recovery_artifact,
    restore_recovery_state, validate_recovery_manifest, validate_recovery_snapshots, CheckpointJob,
    SharedCheckpointStore,
};
#[cfg(test)]
use commands::{
    agent_auth_query, bearer_auth, command_expired, command_is_stale, execute_command,
    execute_job_operation,
};
#[cfg(test)]
use config::agent_capabilities;
#[cfg(test)]
use kernel::{
    await_previous_teardown, durable_recovery_marker, ephemeral_state_nonce, persist_start_marker,
    remove_start_marker, safe_path_component, JobRuntime, JobTask, SplitPlacementPayload,
};
#[cfg(test)]
use resources::{
    data_plane_tls_from_env, merge_resource_gauges, spawn_resource_sampler, ResourceSampler,
    ResourceSnapshot, MIN_RESOURCE_SAMPLE_INTERVAL,
};
#[cfg(test)]
use session::{
    build_agent_client, is_loopback_host, jump_to_candidate, remember_completed_command,
    replay_cached_command, rotate_candidates, run_session, CompletedCommandCache,
};

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}
