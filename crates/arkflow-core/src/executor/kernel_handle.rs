//! Command-driven snapshots for kernel-driven Jobs (Agent mode).
//!
//! The Agent protocol checkpoints on command (Hub `checkpoint` dispatch),
//! unlike local mode's interval-driven barriers. `KernelJobHandle` runs a
//! graph and exposes a barrier snapshot entry point: the graph's chains carry
//! the marker through their FIFO data channels, align multi-input vertices,
//! and snapshot state asynchronously without a job-wide read/write gate.

use crate::executor::graph::ExecutionGraph;
use crate::executor::metrics::KernelMetrics;
use crate::input::Input;
use crate::state::{StateBackend, StateEntry, StateSnapshot};
use crate::Error;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

/// Compatibility type for callers that still mention the retired global gate.
/// The kernel no longer reads or writes this lock; barriers are in-band.
pub type SnapshotGate = Arc<RwLock<()>>;

/// Shared completion signal for the run: resolved with the graph's result
/// when every chain exits (cancellation or end-of-stream). The result sits
/// behind an `Arc` because `Error` is not `Clone`; observers clone the Arc.
type Completion = Arc<tokio::sync::Mutex<Option<Arc<Result<(), Error>>>>>;

/// Handle to a running kernel Job: snapshot/restore/stop without consuming
/// the handle; completion is observed via `watcher`.
pub struct KernelJobHandle {
    cancellation: CancellationToken,
    /// Source inputs (for current_positions and restore).
    inputs: Vec<Arc<dyn Input>>,
    /// State backends by task namespace (keyed by chain entry task id).
    states: BTreeMap<String, Arc<dyn StateBackend>>,
    /// Event-time watermarks keyed by source task id.
    watermark_gates:
        BTreeMap<String, Arc<tokio::sync::Mutex<Option<super::event_time_gate::EventTimeGate>>>>,
    /// Physical source partition of each gated source chain: restoring a
    /// checkpointed watermark installs it for the task's REAL partition
    /// instead of synthesizing progress on partition 0.
    gate_partitions: BTreeMap<String, u32>,
    /// Barrier injection channels keyed by source chain entry task.
    barrier_senders: BTreeMap<String, flume::Sender<super::envelope::Envelope>>,
    /// Every chain reports once for a barrier round.
    participants: BTreeSet<String>,
    reports: Arc<
        tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<super::barrier::ChainSnapshot>>,
    >,
    checkpoint_errors: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Error>>>,
    /// Exit notifications from chains, used to exempt ended chains from
    /// barrier rounds. The handle holds a keep-alive sender so the receiver
    /// only reports real notifications, never channel closure.
    chain_finished: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<String>>>,
    _finished_keepalive: tokio::sync::mpsc::UnboundedSender<String>,
    checkpoint_lock: Arc<tokio::sync::Mutex<()>>,
    next_snapshot_id: AtomicU64,
    /// Shared job metrics.  The runner installs the corresponding
    /// `RuntimeMetrics` in every chain hook, so Agent jobs and local streams
    /// observe the same per-chain counters and checkpoint timings.
    metrics: Arc<KernelMetrics>,
    /// State format configured by the Job, including stateless Jobs whose
    /// checkpoint reports carry empty snapshots.
    state_format: u32,
    /// Notified once with the graph's result when the runner task finishes.
    completion: Completion,
}

impl KernelJobHandle {
    /// Start a barrier round and collect one snapshot from every execution
    /// chain. Data continues through the graph while each chain reaches its
    /// own FIFO barrier position; there is no global write gate in this path.
    pub async fn checkpoint_snapshot(
        &self,
    ) -> Result<
        (
            StateSnapshot,
            Vec<crate::checkpoint::SourcePosition>,
            BTreeMap<String, i64>,
        ),
        Error,
    > {
        let id = format!(
            "kernel-snapshot-{}",
            self.next_snapshot_id.fetch_add(1, Ordering::Relaxed)
        );
        self.checkpoint_barrier(id, 0).await
    }

    /// Inject one checkpoint barrier into every source chain and wait until
    /// all chains have acknowledged the same barrier. The returned state is
    /// assembled from the barrier-time chain snapshots, not from a later
    /// quiescent read of mutable state.
    pub async fn checkpoint_barrier(
        &self,
        checkpoint_id: impl Into<String>,
        generation: u64,
    ) -> Result<
        (
            StateSnapshot,
            Vec<crate::checkpoint::SourcePosition>,
            BTreeMap<String, i64>,
        ),
        Error,
    > {
        self.checkpoint_barrier_with_details(checkpoint_id, generation)
            .await
            .map(|(snapshot, positions, watermarks, _)| (snapshot, positions, watermarks))
    }

    /// Detailed checkpoint variant retaining every physical event-time
    /// watermark. The three-value method above remains compatible with older
    /// callers; Agent/local recovery uses this method for multi-topic and
    /// multi-partition restore.
    pub async fn checkpoint_barrier_with_details(
        &self,
        checkpoint_id: impl Into<String>,
        generation: u64,
    ) -> Result<
        (
            StateSnapshot,
            Vec<crate::checkpoint::SourcePosition>,
            BTreeMap<String, i64>,
            BTreeMap<String, Vec<crate::checkpoint::WatermarkPosition>>,
        ),
        Error,
    > {
        let started = Instant::now();
        let result = self
            .checkpoint_barrier_inner(checkpoint_id.into(), generation)
            .await;
        match &result {
            Ok(_) => self
                .metrics
                .record_checkpoint(started.elapsed().as_millis() as u64),
            Err(_) => self.metrics.record_checkpoint_failure(),
        }
        result
    }

    async fn checkpoint_barrier_inner(
        &self,
        checkpoint_id: String,
        generation: u64,
    ) -> Result<
        (
            StateSnapshot,
            Vec<crate::checkpoint::SourcePosition>,
            BTreeMap<String, i64>,
            BTreeMap<String, Vec<crate::checkpoint::WatermarkPosition>>,
        ),
        Error,
    > {
        let _round = self.checkpoint_lock.lock().await;
        let barrier = crate::checkpoint::CheckpointBarrier {
            checkpoint_id,
            generation,
        };
        if self.barrier_senders.is_empty() {
            return Err(Error::Process(
                "kernel graph has no source barrier channel".into(),
            ));
        }
        // Chains whose event loop already exited can neither receive barriers
        // (their barrier receiver dropped with the hook) nor send reports.
        // Exempt them from this round instead of parking the wait loop on a
        // report that will never arrive.
        let mut ended = BTreeSet::new();
        {
            let mut finished = self.chain_finished.lock().await;
            while let Ok(task_id) = finished.try_recv() {
                ended.insert(task_id);
            }
        }
        for (task_id, sender) in &self.barrier_senders {
            if ended.contains(task_id) {
                continue;
            }
            sender
                .send_async(super::envelope::Envelope::Barrier(barrier.clone()))
                .await
                .map_err(|_| Error::Process("source barrier channel is closed".into()))?;
        }
        let mut remaining: BTreeSet<String> = self
            .participants
            .iter()
            .filter(|task_id| !ended.contains(*task_id))
            .cloned()
            .collect();
        if remaining.is_empty() {
            return Err(Error::Process(
                "kernel ended before checkpoint completed".into(),
            ));
        }

        let mut snapshots = BTreeMap::new();
        while !remaining.is_empty() {
            let report = {
                let mut reports = self.reports.lock().await;
                let mut checkpoint_errors = self.checkpoint_errors.lock().await;
                let mut chain_finished = self.chain_finished.lock().await;
                tokio::select! {
                    _ = self.cancellation.cancelled() => {
                        return Err(Error::Process("kernel cancelled during checkpoint".into()));
                    }
                    error = checkpoint_errors.recv() => {
                        if let Some(error) = error {
                            return Err(error);
                        }
                        return Err(Error::Process("checkpoint error channel closed".into()));
                    }
                    finished = chain_finished.recv() => {
                        // Unreachable None while the handle holds a keep-alive
                        // sender; treat it defensively as full termination.
                        let Some(task_id) = finished else {
                            return Err(Error::Process(
                                "kernel ended before checkpoint completed".into(),
                            ));
                        };
                        // A chain sends its report before its finished
                        // notification, so once the exit is observed the
                        // report is already queued (possibly behind reports
                        // from other chains). Drain it here: removing the
                        // task without its report could seal a checkpoint
                        // that is missing this chain's state and source
                        // positions, and the sealed manifest would still
                        // pass validation as the recovery point.
                        loop {
                            match reports.try_recv() {
                                Ok(report) => {
                                    if report.barrier != barrier {
                                        tracing::warn!(
                                            task = %report.task_id,
                                            checkpoint = %report.barrier.checkpoint_id,
                                            generation = report.barrier.generation,
                                            expected = %barrier.checkpoint_id,
                                            expected_generation = barrier.generation,
                                            "ignoring stale checkpoint report"
                                        );
                                        continue;
                                    }
                                    if !self.participants.contains(&report.task_id) {
                                        return Err(Error::Config(format!(
                                            "checkpoint report from unknown chain '{}'",
                                            report.task_id
                                        )));
                                    }
                                    let reported_task_id = report.task_id.clone();
                                    if snapshots
                                        .insert(reported_task_id.clone(), report)
                                        .is_some()
                                    {
                                        return Err(Error::Process(
                                            "duplicate chain checkpoint report".into(),
                                        ));
                                    }
                                    remaining.remove(&reported_task_id);
                                }
                                Err(
                                    tokio::sync::mpsc::error::TryRecvError::Disconnected,
                                ) => break,
                                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                            }
                        }
                        remaining.remove(&task_id);
                        continue;
                    }
                    report = reports.recv() => report,
                }
            };
            let Some(report) = report else {
                return Err(Error::Process(
                    "kernel ended before checkpoint completed".into(),
                ));
            };
            if report.barrier != barrier {
                // A failed prior round may have detached snapshots still
                // completing after the caller has moved on.  Do not let that
                // stale report poison the next barrier; the current round
                // still requires one matching report from every participant.
                tracing::warn!(
                    task = %report.task_id,
                    checkpoint = %report.barrier.checkpoint_id,
                    generation = report.barrier.generation,
                    expected = %barrier.checkpoint_id,
                    expected_generation = barrier.generation,
                    "ignoring stale checkpoint report"
                );
                continue;
            }
            if !self.participants.contains(&report.task_id) {
                return Err(Error::Config(format!(
                    "checkpoint report from unknown chain '{}'",
                    report.task_id
                )));
            }
            let reported_task_id = report.task_id.clone();
            if snapshots.insert(reported_task_id.clone(), report).is_some() {
                return Err(Error::Process("duplicate chain checkpoint report".into()));
            }
            remaining.remove(&reported_task_id);
        }

        // The checkpoint's state format is the CONFIGURED Job/backend
        // contract, not whatever the first report happened to carry: a
        // stateless chain's empty default-format snapshot never vetoes a Job
        // configured with another state format.
        let format_version = self.state_format;
        let mut state_entries = BTreeMap::<(String, Vec<u8>), StateEntry>::new();
        let mut positions = Vec::new();
        let mut watermarks = BTreeMap::new();
        let mut watermark_partitions = BTreeMap::new();
        for report in snapshots.values() {
            if !report.state.verify() {
                return Err(Error::Process(format!(
                    "invalid state snapshot from chain '{}'",
                    report.task_id
                )));
            }
            let stateless_default_format =
                report.state.entries.is_empty() && report.state.format_version == 1;
            if !stateless_default_format && report.state.format_version != format_version {
                return Err(Error::Process(format!(
                    "chain '{}' reported state format {} but the Job's configured state format is {}",
                    report.task_id, report.state.format_version, format_version
                )));
            }
            for entry in &report.state.entries {
                state_entries.insert((entry.namespace.clone(), entry.key.clone()), entry.clone());
            }
            positions.extend(report.source_positions.clone());
            if let Some(watermark) = report.watermark_ms {
                watermarks.insert(report.task_id.clone(), watermark);
            }
            if !report.watermark_partitions.is_empty() {
                watermark_partitions
                    .insert(report.task_id.clone(), report.watermark_partitions.clone());
            }
        }
        Ok((
            StateSnapshot::new(format_version, state_entries.into_values().collect()),
            positions,
            watermarks,
            watermark_partitions,
        ))
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

    /// Restore watermarks into the source gates (recovery). Each watermark
    /// is installed for that source task's ACTUAL assigned partition —
    /// defaulting to partition 0 would synthesize progress on a partition
    /// the task never reads and skew the minimum-watermark computation.
    pub async fn restore_watermarks(
        &self,
        watermarks_ms: &BTreeMap<String, i64>,
    ) -> Result<(), Error> {
        self.restore_watermarks_with_partitions(watermarks_ms, &BTreeMap::new())
            .await
    }

    /// Restore physical watermark progress for every assigned topic/partition,
    /// falling back to the legacy task-level value for older checkpoints.
    pub async fn restore_watermarks_with_partitions(
        &self,
        watermarks_ms: &BTreeMap<String, i64>,
        watermark_partitions: &BTreeMap<String, Vec<crate::checkpoint::WatermarkPosition>>,
    ) -> Result<(), Error> {
        for (task_id, partitions) in watermark_partitions {
            if let Some(gate) = self.watermark_gates.get(task_id) {
                let mut gate = gate.lock().await;
                if let Some(gate) = gate.as_mut() {
                    for partition in partitions {
                        gate.restore_partition_key(
                            &crate::event_time::EventTimePartition::new(
                                partition.topic.clone(),
                                partition.partition,
                            )
                            .with_source_identity(task_id),
                            partition.watermark_ms,
                        );
                    }
                }
            }
        }
        for (task_id, watermark) in watermarks_ms {
            if watermark_partitions
                .get(task_id)
                .is_some_and(|partitions| !partitions.is_empty())
            {
                continue;
            }
            if let Some(gate) = self.watermark_gates.get(task_id) {
                let mut gate_guard = gate.lock().await;
                if let Some(gate) = gate_guard.as_mut() {
                    let known = gate.known_partitions();
                    if known.is_empty() {
                        let partition = self
                            .gate_partitions
                            .get(task_id)
                            .copied()
                            .unwrap_or_default();
                        let partition =
                            crate::event_time::EventTimePartition::for_source(task_id, partition);
                        gate.restore_partition_key(&partition, *watermark);
                    } else {
                        for partition in known {
                            gate.restore_partition_key(&partition, *watermark);
                        }
                    }
                }
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

    /// Read-only view of the source watermark gates (restore verification
    /// and tests).
    pub fn watermark_gates(
        &self,
    ) -> &BTreeMap<String, Arc<tokio::sync::Mutex<Option<super::event_time_gate::EventTimeGate>>>>
    {
        &self.watermark_gates
    }

    pub fn metrics(&self) -> Arc<KernelMetrics> {
        self.metrics.clone()
    }

    pub fn gate(&self) -> SnapshotGate {
        // Source-compatible no-op for clients compiled against the legacy
        // API.  It is deliberately detached from the running graph.
        Arc::new(RwLock::new(()))
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
        Self::spawn_with_cancellation_mode(
            graph,
            inputs,
            states,
            watermark_gates,
            connect_inputs,
            false,
            None,
            cancellation,
        )
        .await
    }

    /// Spawn a graph after the caller has connected every source input and
    /// restored its checkpoint position.  The graph bootstrap skips source
    /// reconnects in this mode, preserving connector-specific assignments.
    pub async fn spawn_prepared_with_cancellation(
        graph: ExecutionGraph,
        inputs: Vec<Arc<dyn Input>>,
        states: BTreeMap<String, Arc<dyn StateBackend>>,
        watermark_gates: BTreeMap<
            String,
            Arc<tokio::sync::Mutex<Option<super::event_time_gate::EventTimeGate>>>,
        >,
        cancellation: CancellationToken,
    ) -> Result<KernelJobHandle, Error> {
        Self::spawn_with_cancellation_mode(
            graph,
            inputs,
            states,
            watermark_gates,
            false,
            true,
            None,
            cancellation,
        )
        .await
    }

    /// Spawn a graph while retaining the Job's configured state format even
    /// when the graph is stateless and therefore has no state backend entries.
    pub async fn spawn_with_cancellation_and_state_format(
        graph: ExecutionGraph,
        inputs: Vec<Arc<dyn Input>>,
        states: BTreeMap<String, Arc<dyn StateBackend>>,
        watermark_gates: BTreeMap<
            String,
            Arc<tokio::sync::Mutex<Option<super::event_time_gate::EventTimeGate>>>,
        >,
        connect_inputs: bool,
        state_format: u32,
        cancellation: CancellationToken,
    ) -> Result<KernelJobHandle, Error> {
        Self::spawn_with_cancellation_mode(
            graph,
            inputs,
            states,
            watermark_gates,
            connect_inputs,
            false,
            Some(state_format),
            cancellation,
        )
        .await
    }

    /// Prepared-source variant of
    /// [`KernelJobRunner::spawn_with_cancellation_and_state_format`].
    pub async fn spawn_prepared_with_cancellation_and_state_format(
        graph: ExecutionGraph,
        inputs: Vec<Arc<dyn Input>>,
        states: BTreeMap<String, Arc<dyn StateBackend>>,
        watermark_gates: BTreeMap<
            String,
            Arc<tokio::sync::Mutex<Option<super::event_time_gate::EventTimeGate>>>,
        >,
        state_format: u32,
        cancellation: CancellationToken,
    ) -> Result<KernelJobHandle, Error> {
        Self::spawn_with_cancellation_mode(
            graph,
            inputs,
            states,
            watermark_gates,
            false,
            true,
            Some(state_format),
            cancellation,
        )
        .await
    }

    async fn spawn_with_cancellation_mode(
        graph: ExecutionGraph,
        inputs: Vec<Arc<dyn Input>>,
        states: BTreeMap<String, Arc<dyn StateBackend>>,
        watermark_gates: BTreeMap<
            String,
            Arc<tokio::sync::Mutex<Option<super::event_time_gate::EventTimeGate>>>,
        >,
        connect_inputs: bool,
        sources_preconnected: bool,
        configured_state_format: Option<u32>,
        cancellation: CancellationToken,
    ) -> Result<KernelJobHandle, Error> {
        if connect_inputs {
            for input in &inputs {
                if let Err(error) = input.connect().await {
                    for connected in inputs.iter().rev() {
                        let _ = connected.close().await;
                    }
                    return Err(error);
                }
            }
        }
        let completion: Completion = Arc::new(tokio::sync::Mutex::new(None));
        let (report_tx, report_rx) = tokio::sync::mpsc::unbounded_channel();
        let (checkpoint_error_tx, checkpoint_error_rx) = tokio::sync::mpsc::unbounded_channel();
        let (chain_finished_tx, chain_finished_rx) = tokio::sync::mpsc::unbounded_channel();
        let reports = Arc::new(tokio::sync::Mutex::new(report_rx));
        let checkpoint_errors = Arc::new(tokio::sync::Mutex::new(checkpoint_error_rx));
        let chain_finished = Arc::new(tokio::sync::Mutex::new(chain_finished_rx));
        let runtime_metrics = Arc::new(crate::runtime::RuntimeMetrics::default());
        let mut barrier_senders = BTreeMap::new();
        let mut participants = BTreeSet::new();
        let mut hooks = BTreeMap::new();
        let mut watermark_gates = watermark_gates;
        let mut gate_partitions = BTreeMap::new();
        for chain in &graph.chains {
            let entry_task_id = chain.entry_task_id().to_owned();
            participants.insert(entry_task_id.clone());
            let event_time_gate = if let Some(gate) = watermark_gates.get(&entry_task_id).cloned() {
                gate
            } else {
                let gate = if chain
                    .source_time
                    .as_ref()
                    .is_some_and(|time| time.mode == crate::job::TimeMode::EventTime)
                {
                    let source_time = chain.source_time.as_ref().expect("checked above");
                    let gate = super::event_time_gate::EventTimeGate::new(
                        source_time,
                        chain.window_timings.clone(),
                    );
                    let gate = match gate {
                        Ok(gate) => gate,
                        Err(error) => {
                            // At this point source inputs may already be
                            // connected (normal startup) or restored and
                            // connected by the caller (recovery). Do not leave
                            // either those handles or the state backend alive
                            // when hook construction aborts before the graph
                            // resource guard takes ownership.
                            if connect_inputs || sources_preconnected {
                                for input in inputs.iter().rev() {
                                    let _ = input.close().await;
                                }
                            }
                            for state in states.values() {
                                let _ = state.close();
                            }
                            return Err(error);
                        }
                    };
                    Some(gate)
                } else {
                    None
                };
                let gate = Arc::new(tokio::sync::Mutex::new(gate));
                watermark_gates.insert(entry_task_id.clone(), gate.clone());
                gate
            };
            let state = chain
                .task_ids
                .iter()
                .find_map(|task_id| states.get(task_id).cloned());
            let barrier_rx = if chain.is_source() {
                let (sender, receiver) = flume::bounded(8);
                barrier_senders.insert(entry_task_id.clone(), sender);
                Some(Arc::new(tokio::sync::Mutex::new(receiver)))
            } else {
                None
            };
            if chain.is_source() {
                if let Some(partition) = chain.source_partition {
                    gate_partitions.insert(entry_task_id.clone(), partition);
                }
            }
            hooks.insert(
                entry_task_id.clone(),
                super::task::CheckpointHook {
                    reporter: Some(report_tx.clone()),
                    failure_reporter: Some(checkpoint_error_tx.clone()),
                    barrier_rx,
                    state,
                    task_id: Some(entry_task_id),
                    event_time_gate,
                    partition: chain.source_partition,
                    metrics: Some(runtime_metrics.clone()),
                    finished_reporter: Some(chain_finished_tx.clone()),
                },
            );
        }
        let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
        {
            let cancellation = cancellation.clone();
            let completion = completion.clone();
            tokio::spawn(async move {
                // Catch a panicking graph run: the completion slot must
                // resolve on every path, or every watcher() task spins
                // forever (a task leak per panicking startup).
                let result = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
                    super::task::run_graph_with_hooks_startup(
                        graph,
                        cancellation,
                        hooks,
                        sources_preconnected || connect_inputs,
                        Some(startup_tx),
                    ),
                ))
                .await
                .unwrap_or_else(|panic| {
                    Err(Error::Process(format!(
                        "kernel graph task panicked: {}",
                        crate::executor::task::panic_payload(&panic)
                    )))
                });
                KernelJobHandle::complete(&completion, result).await;
            });
        }
        match startup_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(message)) => {
                cancellation.cancel();
                return Err(Error::Process(message));
            }
            Err(_) => {
                cancellation.cancel();
                return Err(Error::Process(
                    "kernel graph startup task exited before readiness".into(),
                ));
            }
        }
        let state_format = configured_state_format
            .or_else(|| states.values().next().map(|state| state.format_version()))
            .unwrap_or(1);
        Ok(KernelJobHandle {
            cancellation,
            inputs,
            states,
            watermark_gates,
            gate_partitions,
            completion,
            barrier_senders,
            participants,
            reports,
            checkpoint_errors,
            chain_finished,
            _finished_keepalive: chain_finished_tx,
            checkpoint_lock: Arc::new(tokio::sync::Mutex::new(())),
            next_snapshot_id: AtomicU64::new(0),
            metrics: runtime_metrics.kernel.clone(),
            state_format,
        })
    }
}
