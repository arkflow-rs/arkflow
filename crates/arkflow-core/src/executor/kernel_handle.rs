//! Command-driven snapshots for kernel-driven Jobs (Agent mode).
//!
//! The Agent protocol checkpoints on command (Hub `checkpoint` dispatch),
//! unlike local mode's interval-driven barriers. `KernelJobHandle` runs a
//! graph and exposes a synchronous snapshot entry point: the graph's chains
//! observe a read-write gate — data processing takes read shares, snapshots
//! take the write share — so a snapshot observes a quiescent instant of the
//! graph (positions, watermarks, state) without killing the run. This keeps
//! the Agent's existing checkpoint contract while the kernel executes the
//! data plane; the barrier path (`barrier.rs`) remains the long-term
//! home for fully asynchronous snapshots.

use crate::Error;
use crate::executor::graph::ExecutionGraph;
use crate::input::Input;
use crate::state::{StateBackend, StateSnapshot};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

/// Shared gate every chain's event loop acquires (read) per envelope and the
/// snapshot acquires (write).
pub type SnapshotGate = Arc<RwLock<()>>;

/// Shared completion signal for the run: resolved with the graph's result
/// when every chain exits (cancellation or end-of-stream). The result sits
/// behind an `Arc` because `Error` is not `Clone`; observers clone the Arc.
type Completion = Arc<tokio::sync::Mutex<Option<Arc<Result<(), Error>>>>>;

/// Handle to a running kernel Job: snapshot/restore/stop without consuming
/// the handle; completion is observed via `watcher`.
pub struct KernelJobHandle {
    cancellation: CancellationToken,
    gate: SnapshotGate,
    /// Source inputs (for current_positions and restore).
    inputs: Vec<Arc<dyn Input>>,
    /// State backends by task namespace (keyed by chain entry task id).
    states: BTreeMap<String, Arc<dyn StateBackend>>,
    /// Event-time watermarks keyed by source task id.
    watermark_gates:
        BTreeMap<String, Arc<tokio::sync::Mutex<Option<super::event_time_gate::EventTimeGate>>>>,
    /// Notified once with the graph's result when the runner task finishes.
    completion: Completion,
}

impl KernelJobHandle {
    /// Snapshot the running graph: hold the write gate (chains finish their
    /// in-flight envelope), then read state + positions + watermarks.
    pub async fn checkpoint_snapshot(
        &self,
    ) -> Result<(StateSnapshot, Vec<crate::checkpoint::SourcePosition>, BTreeMap<String, i64>), Error>
    {
        let _guard = self.gate.write().await;
        let mut positions = Vec::new();
        for input in &self.inputs {
            positions.extend(input.current_positions().await?);
        }
        // Merge every chain's state entries into one snapshot (the Agent
        // protocol stores one snapshot per node).
        let mut entries = Vec::new();
        for backend in self.states.values() {
            entries.extend(backend.snapshot()?.entries);
        }
        let snapshot = StateSnapshot::new(1, entries);
        let mut watermarks = BTreeMap::new();
        for (task_id, gate) in &self.watermark_gates {
            if let Some(watermark) = gate.lock().await.as_ref().and_then(|gate| gate.watermark()) {
                watermarks.insert(task_id.clone(), watermark);
            }
        }
        Ok((snapshot, positions, watermarks))
    }

    /// Restore source positions before the run starts (recovery).
    pub async fn restore_positions(
        &self,
        positions: &[crate::checkpoint::SourcePosition],
    ) -> Result<(), Error> {
        for input in &self.inputs {
            input.restore_positions(positions).await?;
        }
        Ok(())
    }

    /// Restore watermarks into the source gates (recovery).
    pub async fn restore_watermarks(
        &self,
        watermarks_ms: &BTreeMap<String, i64>,
    ) -> Result<(), Error> {
        for (task_id, watermark) in watermarks_ms {
            if let Some(gate) = self.watermark_gates.get(task_id) {
                gate.lock()
                    .await
                    .as_mut()
                    .map(|gate| gate.restore_partition(0, *watermark));
            }
        }
        Ok(())
    }

    /// Request shutdown (idempotent). The graph's chains observe the token
    /// and exit; `watcher` tasks surface the final result.
    pub fn stop(&self) {
        self.cancellation.cancel();
    }

    /// Spawn a watcher that resolves when the Job finishes. Callers keep
    /// `self` for snapshots while the watcher surfaces completion — the
    /// Agent stores the handle in its JobTask and the watcher in its map.
    pub fn watcher(&self) -> tokio::task::JoinHandle<Result<(), Error>> {
        let completion = self.completion.clone();
        tokio::spawn(async move {
            loop {
                let resolved = { completion.lock().await.clone() };
                if let Some(result) = resolved {
                    // Deref the shared result into an owned copy via the
                    // Arc; on failure clone the display form to rebuild an
                    // Error (Arc<Result> cannot be returned directly).
                    return match &*result {
                        Ok(()) => Ok(()),
                        Err(error) => Err(Error::Process(error.to_string())),
                    };
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        })
    }

    /// Resolve the completion slot (called once by the runner task's guard).
    async fn complete(completion: &Completion, result: Result<(), Error>) {
        completion.lock().await.replace(Arc::new(result));
    }

    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn gate(&self) -> SnapshotGate {
        self.gate.clone()
    }
}

/// Spawns a graph for command-driven snapshot supervision.
pub struct KernelJobRunner;

impl KernelJobRunner {
    /// Spawn the graph with a fresh cancellation token.
    pub async fn spawn(
        graph: ExecutionGraph,
        inputs: Vec<Arc<dyn Input>>,
        states: BTreeMap<String, Arc<dyn StateBackend>>,
        watermark_gates: BTreeMap<
            String,
            Arc<tokio::sync::Mutex<Option<super::event_time_gate::EventTimeGate>>>,
        >,
        connect_inputs: bool,
    ) -> Result<KernelJobHandle, Error> {
        Self::spawn_with_cancellation(
            graph,
            inputs,
            states,
            watermark_gates,
            connect_inputs,
            CancellationToken::new(),
        )
        .await
    }

    /// Spawn the graph bound to a caller-owned cancellation token (the Agent
    /// shares one token between its JobTask bookkeeping and the kernel run).
    pub async fn spawn_with_cancellation(
        graph: ExecutionGraph,
        inputs: Vec<Arc<dyn Input>>,
        states: BTreeMap<String, Arc<dyn StateBackend>>,
        watermark_gates: BTreeMap<
            String,
            Arc<tokio::sync::Mutex<Option<super::event_time_gate::EventTimeGate>>>,
        >,
        connect_inputs: bool,
        cancellation: CancellationToken,
    ) -> Result<KernelJobHandle, Error> {
        let gate: SnapshotGate = Arc::new(RwLock::new(()));
        if connect_inputs {
            for input in &inputs {
                input.connect().await?;
            }
        }
        let completion: Completion = Arc::new(tokio::sync::Mutex::new(None));
        {
            let cancellation = cancellation.clone();
            let gate = gate.clone();
            let completion = completion.clone();
            tokio::spawn(async move {
                let result = super::task::run_graph_with_gate(graph, cancellation, gate).await;
                KernelJobHandle::complete(&completion, result).await;
            });
        }
        Ok(KernelJobHandle {
            cancellation,
            gate,
            inputs,
            states,
            watermark_gates,
            completion,
        })
    }
}
