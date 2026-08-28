//! Unified streaming execution kernel.
//!
//! One execution model for local YAML streams and distributed Jobs: a JobPlan
//! compiles into chains of operators joined by bounded in-process channels
//! (see [`graph::ExecutionGraphBuilder`]); every chain runs its own event loop
//! (see [`task::run_graph`]) so stages pipeline and backpressure propagates
//! through the channels. Checkpoint barriers travel the same channels as
//! control envelopes (see [`envelope::Envelope`]).

pub mod barrier;
pub mod envelope;
pub mod event_time_gate;
pub mod graph;
pub mod job_runner_adapter;
pub mod kernel_handle;
pub mod metrics;
pub mod stateful;
pub mod stream_adapter;
pub mod stream_compiler;
pub mod task;
pub mod window;

#[cfg(test)]
mod tests;

pub use barrier::{Aligner, BarrierCoordinator, ChainSnapshot};
pub use envelope::Envelope;
pub use graph::{Chain, EdgeTarget, ExecutionGraph, ExecutionGraphBuilder, DEFAULT_CHANNEL_CAPACITY};
pub use job_runner_adapter::{
    run_job, run_job_tasks, run_job_with_checkpoints, run_job_with_hooks, run_job_with_metrics,
};
pub use stream_adapter::{StreamJobAdapter, WalInput};
pub use task::{run_graph, run_graph_with_hooks, run_graph_with_metrics, CheckpointHook};
pub use window::{AggregateBuffer, ColumnarWindowOperator, WindowKind, WindowOperatorConfig, WindowTrigger};
