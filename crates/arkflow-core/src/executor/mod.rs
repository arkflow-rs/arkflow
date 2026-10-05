//! Unified streaming execution kernel.
//!
//! One execution model for local YAML streams and distributed Jobs: a JobPlan
//! compiles into chains of operators joined by bounded in-process channels
//! (see [`graph::ExecutionGraphBuilder`]); every chain runs its own event loop
//! (see [`task::run_graph`]) so stages pipeline and backpressure propagates
//! through the channels. Checkpoint barriers travel the same channels as
//! control envelopes (see [`envelope::Envelope`]).

pub mod barrier;
pub mod commit;
pub mod envelope;
pub mod event_time_gate;
pub mod graph;
pub mod job_runner_adapter;
pub mod join;
pub mod kernel_handle;
pub mod metrics;
pub mod remote;
pub mod resource_guard;
pub mod state_journal;
pub mod stateful;
pub mod stream_adapter;
pub mod stream_compiler;
pub mod task;
pub mod window;

#[cfg(test)]
mod tests;

pub use commit::{AckAdvance, CommitFrontier};
pub use envelope::Envelope;
pub use graph::{Chain, ExecutionGraphBuilder};
pub use job_runner_adapter::run_job;
pub use stream_adapter::StreamJobAdapter;

// Internal aliases kept for in-crate call sites (tests, runtime) that import
// through this module.
#[cfg(test)]
pub(crate) use barrier::BarrierCoordinator;
#[cfg(test)]
pub(crate) use graph::ExecutionGraph;
pub(crate) use job_runner_adapter::run_job_with_metrics_started;
#[cfg(test)]
pub(crate) use task::run_graph;
