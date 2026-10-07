//! Command-driven snapshots for kernel-driven Jobs (Agent mode).
//!
//! The Agent protocol checkpoints on command (Hub `checkpoint` dispatch),
//! unlike local mode's interval-driven barriers. `KernelJobHandle` runs a
//! graph and exposes a barrier snapshot entry point: the graph's chains carry
//! the marker through their FIFO data channels, align multi-input vertices,
//! and snapshot state asynchronously without a job-wide read/write gate.

/// Bound on one checkpoint round's report-collection phase: catches
/// hung-not-slow pipelines (a barrier stuck behind sustained backpressure
/// would otherwise park the serialized round loop forever). Far above any
/// legitimate round (barrier transit + snapshot are seconds).
const CHECKPOINT_ROUND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10 * 60);

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
pub(crate) type SnapshotGate = Arc<RwLock<()>>;

/// Shared completion signal for the run: resolved with the graph's result
/// when every chain exits (cancellation or end-of-stream). The result sits
/// behind an `Arc` because `Error` is not `Clone`; observers clone the Arc.
/// The watch channel is a lossless wakeup for watchers: a write that lands
/// between a watcher's state check and its `changed().await` is still
/// observed, which a bare `Notify` cannot guarantee.
type Completion = Arc<CompletionSlot>;

struct CompletionSlot {
    result: tokio::sync::Mutex<Option<Arc<Result<(), Error>>>>,
    version: tokio::sync::watch::Sender<()>,
}

impl CompletionSlot {
    fn unresolved() -> Self {
        let (version, _) = tokio::sync::watch::channel(());
        Self {
            result: tokio::sync::Mutex::new(None),
            version,
        }
    }

    fn subscribe(&self) -> tokio::sync::watch::Receiver<()> {
        self.version.subscribe()
    }
}

/// Handle to a running kernel Job: snapshot/restore/stop without consuming
/// the handle; completion is observed via `watcher`.
pub struct KernelJobHandle {
    cancellation: CancellationToken,
    /// Source inputs (for current_positions and restore).
    inputs: Vec<Arc<dyn Input>>,
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
    /// Round deadline in milliseconds (see CHECKPOINT_ROUND_TIMEOUT);
    /// AtomicU64 so tests can shrink it without waiting 10 minutes.
    checkpoint_round_timeout_ms: AtomicU64,
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

    /// The current round deadline as a Duration.
    fn round_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(
            self.checkpoint_round_timeout_ms
                .load(std::sync::atomic::Ordering::Acquire),
        )
    }

    /// Shrink the round deadline. Test-only.
    #[cfg(test)]
    pub(crate) fn override_round_timeout_for_tests(&mut self, timeout: std::time::Duration) {
        self.checkpoint_round_timeout_ms.store(
            timeout.as_millis() as u64,
            std::sync::atomic::Ordering::Release,
        );
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
            trace_context: None,
        };
        // Split placement: a node hosting no source tasks receives its
        // barriers from upstream nodes over the data plane. Skip local
        // injection and wait passively — the report matching below enforces
        // that the arriving barrier carries exactly this round's identity.
        let inject_locally = !self.barrier_senders.is_empty();
        if !inject_locally && self.participants.is_empty() {
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
        if inject_locally {
            for (task_id, sender) in &self.barrier_senders {
                if ended.contains(task_id) {
                    continue;
                }
                sender
                    .send_async(super::envelope::Envelope::Barrier(barrier.clone()))
                    .await
                    .map_err(|_| Error::Process("source barrier channel is closed".into()))?;
            }
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
        // The whole collection phase is bounded: a hung-not-slow pipeline
        // fails the round explicitly instead of parking the serialized
        // round loop forever. Stragglers from an abandoned round are
        // absorbed by the stale-report handling below.
        let round_deadline = tokio::time::Instant::now() + self.round_timeout();
        while !remaining.is_empty() {
            let report = {
                let mut reports = self.reports.lock().await;
                let mut checkpoint_errors = self.checkpoint_errors.lock().await;
                let mut chain_finished = self.chain_finished.lock().await;
                tokio::select! {
                    _ = self.cancellation.cancelled() => {
                        return Err(Error::Process("kernel cancelled during checkpoint".into()));
                    }
                    _ = tokio::time::sleep_until(round_deadline) => {
                        return Err(Error::Process(format!(
                            "checkpoint round timed out after {:?} waiting for chains {:?}",
                            self.round_timeout(),
                            remaining
                        )));
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
                        // A chain that exits without a report for THIS round
                        // — an error exit in particular never reports — must
                        // not be exempted: exempting it would seal a manifest
                        // that is missing a participant while validation
                        // still passes. Fail the round instead; the next one
                        // exempts the ended chain up front.
                        if !snapshots.contains_key(&task_id) {
                            return Err(Error::Process(format!(
                                "chain '{}' ended during the checkpoint round without reporting a snapshot",
                                task_id
                            )));
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

        // Every report arrived, but the select above may never have visited a
        // ready error branch: a snapshot error queued alongside the reports
        // would otherwise be bypassed and the round would persist as
        // successful. Drain the queue before declaring the cut consistent —
        // the chain loop sends at most one error instead of its report, so an
        // error queued for this round invalidates it.
        if let Ok(error) = self.checkpoint_errors.lock().await.try_recv() {
            return Err(error);
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
            // Subscribe before the first check: a receiver created after a
            // resolution would start at the current version and never fire,
            // so ordering the subscription first keeps the wakeup lossless.
            let mut version = completion.subscribe();
            loop {
                let resolved = { completion.result.lock().await.clone() };
                if let Some(result) = resolved {
                    // Deref the shared result into an owned copy via the
                    // Arc; on failure clone the display form to rebuild an
                    // Error (Arc<Result> cannot be returned directly).
                    return match &*result {
                        Ok(()) => Ok(()),
                        Err(error) => Err(Error::Process(error.to_string())),
                    };
                }
                let _ = version.changed().await;
            }
        })
    }

    /// Resolve the completion slot (called once by the runner task's guard).
    async fn complete(completion: &Completion, result: Result<(), Error>) {
        completion.result.lock().await.replace(Arc::new(result));
        let _ = completion.version.send(());
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

    #[allow(clippy::too_many_arguments)]
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
            // Startup validation precedes even the source pre-connection on
            // this path (the guard's `connect` below re-runs it, but the
            // preconnected inputs would otherwise violate the documented
            // "validation precedes any connection" contract).
            super::resource_guard::run_startup_validators()?;
            for input in &inputs {
                if let Err(error) = input.connect().await {
                    for connected in inputs.iter().rev() {
                        let _ = connected.close().await;
                    }
                    return Err(error);
                }
            }
        }
        let completion: Completion = Arc::new(CompletionSlot::unresolved());
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
                super::task::ChainHooks {
                    chain_metrics: Some(runtime_metrics.kernel.chain(&entry_task_id)),
                    checkpoint: super::task::CheckpointHook {
                        reporter: Some(report_tx.clone()),
                        failure_reporter: Some(checkpoint_error_tx.clone()),
                        barrier_rx,
                        state,
                        task_id: Some(entry_task_id),
                        finished_reporter: Some(chain_finished_tx.clone()),
                    },
                    event_time: super::task::EventTimeBinding {
                        gate: event_time_gate,
                        partition: chain.source_partition,
                    },
                    metrics: Some(runtime_metrics.clone()),
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
            checkpoint_round_timeout_ms: AtomicU64::new(CHECKPOINT_ROUND_TIMEOUT.as_millis() as u64),
            next_snapshot_id: AtomicU64::new(0),
            metrics: runtime_metrics.kernel.clone(),
            state_format,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{CheckpointBarrier, SourcePosition, WatermarkPosition};
    use crate::executor::barrier::ChainSnapshot;
    use crate::executor::graph::ExecutionGraphBuilder;
    use crate::input::{Ack, Input};
    use crate::job::{
        EdgeSpec, JobComponentAdapter, JobId, JobPlan, JobSpec, JobVersion, OperatorKind,
        OperatorSpec, SinkSpec, SourceSpec, TimeMode, TimeSpec, WatermarkSpec, WatermarkStrategy,
    };
    use crate::output::Output;
    use crate::processor::Processor;
    use crate::{Error, MessageBatch, MessageBatchRef, ProcessResult, Resource};
    use async_trait::async_trait;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    // ---------- synthetic handle construction ----------
    //
    // The checkpoint round loop reacts to report/error/finished channel
    // traffic that real chains produce in specific interleavings. Building
    // the handle directly lets each interleaving be staged deterministically.

    /// Senders paired with a synthetically constructed handle's receivers.
    struct SyntheticChannels {
        report: tokio::sync::mpsc::UnboundedSender<ChainSnapshot>,
        error: tokio::sync::mpsc::UnboundedSender<Error>,
        finished: tokio::sync::mpsc::UnboundedSender<String>,
    }

    fn synthetic_handle(
        participants: &[&str],
        cancellation: CancellationToken,
    ) -> (KernelJobHandle, SyntheticChannels) {
        let (report_tx, report_rx) = tokio::sync::mpsc::unbounded_channel();
        let (error_tx, error_rx) = tokio::sync::mpsc::unbounded_channel();
        let (finished_tx, finished_rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = KernelJobHandle {
            cancellation,
            inputs: Vec::new(),
            watermark_gates: BTreeMap::new(),
            gate_partitions: BTreeMap::new(),
            barrier_senders: BTreeMap::new(),
            participants: participants.iter().map(|id| (*id).to_string()).collect(),
            reports: Arc::new(tokio::sync::Mutex::new(report_rx)),
            checkpoint_errors: Arc::new(tokio::sync::Mutex::new(error_rx)),
            chain_finished: Arc::new(tokio::sync::Mutex::new(finished_rx)),
            _finished_keepalive: finished_tx.clone(),
            checkpoint_lock: Arc::new(tokio::sync::Mutex::new(())),
            checkpoint_round_timeout_ms: AtomicU64::new(10 * 60 * 1000),
            next_snapshot_id: AtomicU64::new(0),
            metrics: Arc::new(KernelMetrics::default()),
            state_format: 1,
            completion: Arc::new(CompletionSlot::unresolved()),
        };
        (
            handle,
            SyntheticChannels {
                report: report_tx,
                error: error_tx,
                finished: finished_tx,
            },
        )
    }

    /// Like [`synthetic_handle`] but without a keep-alive on the finished
    /// channel: the finished receiver observes closure, which the round loop
    /// must treat as full termination.
    fn synthetic_handle_without_keepalive(
        participants: &[&str],
    ) -> (KernelJobHandle, SyntheticChannels) {
        let (report_tx, report_rx) = tokio::sync::mpsc::unbounded_channel();
        let (error_tx, error_rx) = tokio::sync::mpsc::unbounded_channel();
        let (finished_tx, finished_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(finished_tx);
        let handle = KernelJobHandle {
            cancellation: CancellationToken::new(),
            inputs: Vec::new(),
            watermark_gates: BTreeMap::new(),
            gate_partitions: BTreeMap::new(),
            barrier_senders: BTreeMap::new(),
            participants: participants.iter().map(|id| (*id).to_string()).collect(),
            reports: Arc::new(tokio::sync::Mutex::new(report_rx)),
            checkpoint_errors: Arc::new(tokio::sync::Mutex::new(error_rx)),
            chain_finished: Arc::new(tokio::sync::Mutex::new(finished_rx)),
            _finished_keepalive: {
                let (unrelated_tx, _unrelated_rx) =
                    tokio::sync::mpsc::unbounded_channel::<String>();
                unrelated_tx
            },
            checkpoint_lock: Arc::new(tokio::sync::Mutex::new(())),
            checkpoint_round_timeout_ms: AtomicU64::new(10 * 60 * 1000),
            next_snapshot_id: AtomicU64::new(0),
            metrics: Arc::new(KernelMetrics::default()),
            state_format: 1,
            completion: Arc::new(CompletionSlot::unresolved()),
        };
        (
            handle,
            SyntheticChannels {
                report: report_tx,
                error: error_tx,
                finished: {
                    let (unrelated_tx, _unrelated_rx) =
                        tokio::sync::mpsc::unbounded_channel::<String>();
                    unrelated_tx
                },
            },
        )
    }

    fn snapshot_report(task: &str, checkpoint: &str, generation: u64) -> ChainSnapshot {
        ChainSnapshot {
            task_id: task.to_string(),
            barrier: CheckpointBarrier {
                checkpoint_id: checkpoint.to_string(),
                generation,
                trace_context: None,
            },
            state: crate::state::StateSnapshot::new(1, Vec::new()),
            source_positions: Vec::new(),
            watermark_ms: None,
            watermark_partitions: Vec::new(),
        }
    }

    #[tokio::test]
    async fn checkpoint_fails_without_source_barrier_channels() {
        let (handle, _channels) = synthetic_handle(&[], CancellationToken::new());
        let error = handle
            .checkpoint_barrier("cp-1", 0)
            .await
            .expect_err("a graph with no chains must fail the round");
        assert!(
            error.to_string().contains("no source barrier channel"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn checkpoint_fails_when_every_chain_already_ended() {
        let (handle, channels) = synthetic_handle(&["a"], CancellationToken::new());
        // The exit notification is observed before the round starts: the
        // chain is exempted up front and no report can arrive.
        channels.finished.send("a".into()).unwrap();
        let error = handle
            .checkpoint_barrier("cp-1", 0)
            .await
            .expect_err("an all-ended graph must fail the round");
        assert!(
            error.to_string().contains("kernel ended before checkpoint"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn checkpoint_fails_when_the_source_barrier_channel_is_closed() {
        let (mut handle, _channels) = synthetic_handle(&["a"], CancellationToken::new());
        // A source chain whose barrier receiver was dropped with its event
        // loop, but whose exit notification has not been observed yet.
        let (sender, receiver) = flume::bounded(8);
        drop(receiver);
        handle.barrier_senders.insert("a".to_string(), sender);
        let error = handle
            .checkpoint_barrier("cp-1", 0)
            .await
            .expect_err("a closed barrier channel must fail the round");
        assert!(
            error
                .to_string()
                .contains("source barrier channel is closed"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn checkpoint_round_times_out_and_records_a_failure_metric() {
        let (mut handle, _channels) = synthetic_handle(&["a"], CancellationToken::new());
        handle.override_round_timeout_for_tests(std::time::Duration::from_millis(20));
        let error = handle
            .checkpoint_barrier("cp-1", 0)
            .await
            .expect_err("a round with no reports must time out");
        assert!(error.to_string().contains("timed out"), "{error}");
        assert_eq!(
            handle.metrics.snapshot().checkpoint_failures,
            1,
            "a failed round must bump the failure counter"
        );
    }

    #[tokio::test]
    async fn checkpoint_fails_when_cancelled_during_the_round() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let (handle, _channels) = synthetic_handle(&["a"], cancellation);
        let error = handle
            .checkpoint_barrier("cp-1", 0)
            .await
            .expect_err("a cancelled kernel must fail the round");
        assert!(
            error.to_string().contains("cancelled during checkpoint"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn checkpoint_surfaces_chain_snapshot_errors() {
        let (handle, channels) = synthetic_handle(&["a"], CancellationToken::new());
        channels
            .error
            .send(Error::Process("injected snapshot failure".into()))
            .unwrap();
        let error = handle
            .checkpoint_barrier("cp-1", 0)
            .await
            .expect_err("a chain snapshot error must fail the round");
        assert!(
            error.to_string().contains("injected snapshot failure"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn checkpoint_fails_when_the_error_channel_closes() {
        let (handle, channels) = synthetic_handle(&["a"], CancellationToken::new());
        drop(channels.error);
        let error = handle
            .checkpoint_barrier("cp-1", 0)
            .await
            .expect_err("a closed error channel must fail the round");
        assert!(
            error
                .to_string()
                .contains("checkpoint error channel closed"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn checkpoint_fails_when_a_chain_ends_without_a_report() {
        let (handle, channels) = synthetic_handle(&["a"], CancellationToken::new());
        let task = tokio::spawn(async move { handle.checkpoint_barrier("cp-1", 0).await });
        // Let the round start (past its up-front exit drain) before the
        // notification arrives, so the chain is not simply exempted.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        channels.finished.send("a".into()).unwrap();
        let error = task
            .await
            .unwrap()
            .expect_err("an ended chain without a report must fail the round");
        assert!(
            error
                .to_string()
                .contains("ended during the checkpoint round without reporting"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn checkpoint_fails_when_the_finished_channel_closes() {
        let (handle, channels) = synthetic_handle_without_keepalive(&["a"]);
        // A stale report is consumed first; with the reports and error
        // channels still alive, the closed finished channel is then the only
        // termination signal the loop can observe.
        channels
            .report
            .send(snapshot_report("a", "stale", 9))
            .unwrap();
        let error = handle
            .checkpoint_barrier("cp-1", 0)
            .await
            .expect_err("finished-channel closure must fail the round");
        assert!(
            error.to_string().contains("kernel ended before checkpoint"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn checkpoint_ignores_stale_reports_from_an_abandoned_round() {
        let (handle, channels) = synthetic_handle(&["a"], CancellationToken::new());
        // A detached snapshot from a previously failed round sits in the
        // queue ahead of the current round's reports.
        channels
            .report
            .send(snapshot_report("a", "abandoned-round", 7))
            .unwrap();
        let task = tokio::spawn(async move { handle.checkpoint_barrier("cp-2", 0).await });
        channels
            .report
            .send(snapshot_report("a", "cp-2", 0))
            .unwrap();
        task.await.unwrap().expect("stale reports must be ignored");
    }

    #[tokio::test]
    async fn checkpoint_rejects_reports_from_unknown_chains() {
        let (handle, channels) = synthetic_handle(&["a"], CancellationToken::new());
        channels
            .report
            .send(snapshot_report("zzz", "cp-1", 0))
            .unwrap();
        let error = handle
            .checkpoint_barrier("cp-1", 0)
            .await
            .expect_err("a report from a non-participant must fail the round");
        assert!(
            error
                .to_string()
                .contains("checkpoint report from unknown chain"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn checkpoint_rejects_duplicate_chain_reports() {
        let (handle, channels) = synthetic_handle(&["a", "b"], CancellationToken::new());
        let task = tokio::spawn(async move { handle.checkpoint_barrier("cp-1", 0).await });
        channels
            .report
            .send(snapshot_report("a", "cp-1", 0))
            .unwrap();
        channels
            .report
            .send(snapshot_report("a", "cp-1", 0))
            .unwrap();
        channels
            .report
            .send(snapshot_report("b", "cp-1", 0))
            .unwrap();
        let error = task
            .await
            .unwrap()
            .expect_err("a duplicate report must fail the round");
        assert!(
            error
                .to_string()
                .contains("duplicate chain checkpoint report"),
            "{error}"
        );
    }

    /// Whether the round's post-collection drain or the in-select error
    /// branch observes the queued error is up to `tokio::select!`'s random
    /// branch order; both must surface the error. Repeating the interleaving
    /// keeps the outcome deterministic while exercising both paths.
    #[tokio::test]
    async fn checkpoint_surfaces_errors_queued_alongside_the_reports() {
        for _ in 0..8 {
            let (handle, channels) = synthetic_handle(&["a"], CancellationToken::new());
            channels
                .error
                .send(Error::Process("injected post-report failure".into()))
                .unwrap();
            channels
                .report
                .send(snapshot_report("a", "cp-1", 0))
                .unwrap();
            let error = handle
                .checkpoint_barrier("cp-1", 0)
                .await
                .expect_err("an error queued with the reports must fail the round");
            assert!(
                error.to_string().contains("injected post-report failure"),
                "{error}"
            );
        }
    }

    #[tokio::test]
    async fn checkpoint_rejects_invalid_state_snapshots() {
        let (handle, channels) = synthetic_handle(&["a"], CancellationToken::new());
        let mut report = snapshot_report("a", "cp-1", 0);
        // Tamper with the checksum: the snapshot no longer verifies.
        report.state.checksum = report.state.checksum.wrapping_add(1);
        channels.report.send(report).unwrap();
        let error = handle
            .checkpoint_barrier("cp-1", 0)
            .await
            .expect_err("a corrupted snapshot must fail the round");
        assert!(
            error.to_string().contains("invalid state snapshot"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn checkpoint_rejects_state_format_mismatches_but_allows_stateless_defaults() {
        let (mut handle, channels) = synthetic_handle(&["a"], CancellationToken::new());
        handle.state_format = 3;
        let mut mismatched = snapshot_report("a", "cp-1", 0);
        mismatched.state = crate::state::StateSnapshot::new(
            2,
            vec![crate::state::StateEntry {
                namespace: "job:agg".into(),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                expires_at_ms: None,
            }],
        );
        channels.report.send(mismatched).unwrap();
        let error = handle
            .checkpoint_barrier("cp-1", 0)
            .await
            .expect_err("a foreign state format must fail the round");
        assert!(
            error.to_string().contains("reported state format 2"),
            "{error}"
        );

        // A stateless chain's default-format snapshot never vetoes the Job's
        // configured format: the round succeeds and adopts the configured
        // format for the assembled snapshot.
        let (mut handle, channels) = synthetic_handle(&["a"], CancellationToken::new());
        handle.state_format = 3;
        channels
            .report
            .send(snapshot_report("a", "cp-1", 0))
            .unwrap();
        let (snapshot, _, _) = handle
            .checkpoint_barrier("cp-1", 0)
            .await
            .expect("a stateless default snapshot must not veto the round");
        assert_eq!(snapshot.format_version, 3);
    }

    #[tokio::test]
    async fn checkpoint_assembles_positions_and_watermarks_from_reports() {
        let (handle, channels) = synthetic_handle(&["a"], CancellationToken::new());
        let mut report = snapshot_report("a", "kernel-snapshot-0", 0);
        report.source_positions = vec![SourcePosition {
            topic: Some("orders".into()),
            partition: 2,
            offset: 11,
        }];
        report.watermark_ms = Some(4_200);
        report.watermark_partitions = vec![WatermarkPosition::new(Some("orders".into()), 2, 4_100)];
        channels.report.send(report).unwrap();
        let (snapshot, positions, watermarks) = handle
            .checkpoint_snapshot()
            .await
            .expect("a complete round must succeed");
        assert_eq!(snapshot.format_version, 1);
        assert_eq!(
            positions,
            vec![SourcePosition {
                topic: Some("orders".into()),
                partition: 2,
                offset: 11,
            }]
        );
        assert_eq!(watermarks.get("a"), Some(&4_200));
        // The success path also records the checkpoint duration metric.
        assert_eq!(handle.metrics.snapshot().checkpoint_failures, 0);
    }

    #[tokio::test]
    async fn checkpoint_succeeds_through_the_finished_chain_report_drain() {
        // A chain reports and then exits; whichever order the select observes
        // them in, the report must be collected and the round sealed.
        for _ in 0..6 {
            let (handle, channels) = synthetic_handle(&["a"], CancellationToken::new());
            let task = tokio::spawn(async move { handle.checkpoint_barrier("cp-1", 0).await });
            // Let the round start before the report and its exit notification
            // arrive together.
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            channels
                .report
                .send(snapshot_report("a", "cp-1", 0))
                .unwrap();
            channels.finished.send("a".into()).unwrap();
            task.await
                .unwrap()
                .expect("a report queued with its exit notification must complete the round");
        }
    }

    #[tokio::test]
    async fn watcher_resolves_the_completion_slot_in_both_directions() {
        let (handle, _channels) = synthetic_handle(&[], CancellationToken::new());
        KernelJobHandle::complete(&handle.completion, Ok(())).await;
        handle.watcher().await.unwrap().expect("Ok completion");

        let (handle, _channels) = synthetic_handle(&[], CancellationToken::new());
        KernelJobHandle::complete(
            &handle.completion,
            Err(Error::Process("kernel blew up".into())),
        )
        .await;
        let error = handle
            .watcher()
            .await
            .unwrap()
            .expect_err("Err completion must surface");
        assert!(error.to_string().contains("kernel blew up"), "{error}");
    }

    /// Watchers park on the version channel until the slot resolves — no
    /// polling tick. Every parked watcher must wake from one resolution
    /// (fan-out), including one that subscribes after the value was already
    /// written (the pre-subscribe check path).
    #[tokio::test]
    async fn watchers_wake_from_the_version_channel_without_polling() {
        let (handle, _channels) = synthetic_handle(&[], CancellationToken::new());
        let mut watchers = Vec::new();
        for _ in 0..3 {
            watchers.push(handle.watcher());
        }
        // Let the watchers park on `changed()` before resolving; a bounded
        // yield loop is enough — the late-subscribe path is covered by the
        // assertion below regardless of interleaving.
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        KernelJobHandle::complete(&handle.completion, Ok(())).await;
        for watcher in watchers {
            watcher.await.unwrap().expect("every watcher wakes");
        }
        // A watcher created strictly after resolution must return via the
        // initial state check, not wait for another version bump.
        let (late, _channels) = synthetic_handle(&[], CancellationToken::new());
        KernelJobHandle::complete(&late.completion, Ok(())).await;
        late.watcher().await.unwrap().expect("late watcher returns");
    }

    #[tokio::test]
    async fn accessors_expose_the_shared_token_and_detached_gate() {
        let (handle, _channels) = synthetic_handle(&["a"], CancellationToken::new());
        assert!(!handle.cancellation().is_cancelled());
        handle.stop();
        assert!(handle.cancellation().is_cancelled());
        // The legacy gate accessor is deliberately detached: acquiring it
        // must not block or affect the running graph.
        let gate = handle.gate();
        let _guard = gate.read().await;
        drop(_guard);
        let _snapshot = handle.metrics().snapshot();
        assert!(handle.watermark_gates().is_empty());
    }

    // ---------- real graph spawning ----------

    /// An input that never delivers a batch and never ends: the source chain
    /// stays alive so barrier rounds flow through a live kernel.
    struct NeverEndingInput {
        connects: AtomicUsize,
        closes: AtomicUsize,
    }

    #[async_trait]
    impl Input for NeverEndingInput {
        async fn connect(&self) -> Result<(), Error> {
            self.connects.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            std::future::pending().await
        }
        async fn close(&self) -> Result<(), Error> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct OneBatchThenEofInput {
        sent: Mutex<bool>,
    }

    #[async_trait]
    impl Input for OneBatchThenEofInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            let mut sent = self.sent.lock().unwrap();
            if *sent {
                return Err(Error::EOF);
            }
            *sent = true;
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(vec![1]))],
            )
            .unwrap();
            Ok((
                Arc::new(MessageBatch::new_arrow(batch)),
                Arc::new(crate::input::NoopAck),
            ))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct FailingConnectInput {
        closes: AtomicUsize,
    }

    #[async_trait]
    impl Input for FailingConnectInput {
        async fn connect(&self) -> Result<(), Error> {
            Err(Error::Connection("injected source connect failure".into()))
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            Err(Error::EOF)
        }
        async fn close(&self) -> Result<(), Error> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// An input that records restored checkpoint positions.
    struct RecordingPositionsInput {
        restored: Mutex<Vec<SourcePosition>>,
    }

    #[async_trait]
    impl Input for RecordingPositionsInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            std::future::pending().await
        }
        async fn restore_positions(&self, positions: &[SourcePosition]) -> Result<(), Error> {
            self.restored.lock().unwrap().extend_from_slice(positions);
            Ok(())
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct DevNullOutput;

    #[async_trait]
    impl Output for DevNullOutput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn write(&self, _msg: MessageBatchRef) -> Result<(), Error> {
            Ok(())
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct FailingConnectOutput;

    #[async_trait]
    impl Output for FailingConnectOutput {
        async fn connect(&self) -> Result<(), Error> {
            Err(Error::Connection("injected sink connect failure".into()))
        }
        async fn write(&self, _msg: MessageBatchRef) -> Result<(), Error> {
            Ok(())
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct PassThroughProcessor;

    #[async_trait]
    impl Processor for PassThroughProcessor {
        async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
            Ok(ProcessResult::Single(batch))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct KernelAdapter {
        input: Arc<dyn Input>,
        output: Arc<dyn Output>,
        processor: Arc<dyn Processor>,
    }

    impl JobComponentAdapter for KernelAdapter {
        fn build_input(
            &self,
            _source: &SourceSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Input>, Error> {
            Ok(self.input.clone())
        }
        fn build_output(
            &self,
            _sink: &SinkSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Output>, Error> {
            Ok(self.output.clone())
        }
        fn build_processor(
            &self,
            _operator: &OperatorSpec,
            _resource: &Resource,
        ) -> Result<Arc<dyn Processor>, Error> {
            Ok(self.processor.clone())
        }
    }

    fn adapter_with(input: Arc<dyn Input>) -> KernelAdapter {
        KernelAdapter {
            input,
            output: Arc::new(DevNullOutput),
            processor: Arc::new(PassThroughProcessor),
        }
    }

    fn processing_time() -> TimeSpec {
        TimeSpec {
            mode: TimeMode::ProcessingTime,
            timestamp_field: None,
            watermark: None,
            allowed_lateness_ms: 0,
            late_event_policy: Default::default(),
            late_event_route: None,
        }
    }

    fn kernel_spec(stateful: bool, parallelism: u32) -> JobSpec {
        let mut operators = vec![OperatorSpec {
            id: "source".into(),
            kind: OperatorKind::Source,
            stateful: false,
            key_field: None,
            config: serde_json::json!({}),
        }];
        if stateful {
            operators.push(OperatorSpec {
                id: "agg".into(),
                kind: OperatorKind::Aggregate,
                stateful: true,
                key_field: Some("key".into()),
                config: serde_json::json!({}),
            });
        }
        operators.push(OperatorSpec {
            id: "sink".into(),
            kind: OperatorKind::Sink,
            stateful: false,
            key_field: None,
            config: serde_json::json!({}),
        });
        let edges = if stateful {
            vec![
                EdgeSpec {
                    id: "source-agg".into(),
                    from: "source".into(),
                    to: "agg".into(),
                    partitioned: false,
                },
                EdgeSpec {
                    id: "agg-sink".into(),
                    from: "agg".into(),
                    to: "sink".into(),
                    partitioned: false,
                },
            ]
        } else {
            vec![EdgeSpec {
                id: "source-sink".into(),
                from: "source".into(),
                to: "sink".into(),
                partitioned: false,
            }]
        };
        JobSpec {
            resources: Default::default(),
            rescale: false,
            rebalance: None,
            id: JobId::new("kernel-handle-job").unwrap(),
            version: JobVersion(1),
            max_parallelism: 2,
            parallelism,
            operators,
            edges,
            sources: vec![SourceSpec {
                operator_id: "source".into(),
                input_type: "vec".into(),
                codec: None,
                config: serde_json::json!({}),
                time: processing_time(),
            }],
            sinks: vec![SinkSpec {
                operator_id: "sink".into(),
                output_type: "collect".into(),
                codec: None,
                config: serde_json::json!({}),
            }],
            state: stateful.then(|| crate::job::StateSpec {
                backend: "embedded_kv".into(),
                durability: crate::job::StateDurability::Ephemeral,
                root: None,
                namespace: None,
                ttl_ms: None,
                format_version: 1,
                max_pending_transactions: None,
                max_bytes: None,
            }),
            checkpoint: None,
            placement: crate::job::PlacementStrategy::Colocated,
            recovery: Default::default(),
        }
    }

    fn resource() -> Resource {
        Resource {
            temporary: HashMap::new(),
            input_names: std::cell::RefCell::new(Vec::new()),
        }
    }

    fn build_graph(
        spec: JobSpec,
        adapter: &KernelAdapter,
    ) -> (JobPlan, crate::executor::graph::ExecutionGraph) {
        let plan = JobPlan::compile(spec).unwrap();
        let graph = ExecutionGraphBuilder::default()
            .build(&plan, adapter, &resource())
            .unwrap();
        (plan, graph)
    }

    fn eof_adapter() -> KernelAdapter {
        adapter_with(Arc::new(OneBatchThenEofInput {
            sent: Mutex::new(false),
        }))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_runs_a_graph_to_completion_and_the_watcher_resolves() {
        let adapter = eof_adapter();
        let (_plan, graph) = build_graph(kernel_spec(false, 1), &adapter);
        let handle =
            KernelJobRunner::spawn(graph, Vec::new(), BTreeMap::new(), BTreeMap::new(), true)
                .await
                .expect("spawn must succeed");
        handle
            .watcher()
            .await
            .unwrap()
            .expect("an EOF run must complete successfully");
        assert_eq!(handle.state_format, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_with_cancellation_honors_the_caller_owned_token() {
        let adapter = eof_adapter();
        let (_plan, graph) = build_graph(kernel_spec(false, 1), &adapter);
        let cancellation = CancellationToken::new();
        let handle = KernelJobRunner::spawn_with_cancellation(
            graph,
            Vec::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            true,
            cancellation.clone(),
        )
        .await
        .expect("spawn must succeed");
        assert!(!cancellation.is_cancelled());
        handle.stop();
        handle
            .watcher()
            .await
            .unwrap()
            .expect("a cancelled graceful run must complete successfully");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_prepared_with_cancellation_runs_preconnected_sources() {
        let input = Arc::new(OneBatchThenEofInput {
            sent: Mutex::new(false),
        });
        let adapter = adapter_with(input.clone());
        let (_plan, graph) = build_graph(kernel_spec(false, 1), &adapter);
        input.connect().await.unwrap();
        let handle = KernelJobRunner::spawn_prepared_with_cancellation(
            graph,
            Vec::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            CancellationToken::new(),
        )
        .await
        .expect("prepared spawn must succeed");
        handle
            .watcher()
            .await
            .unwrap()
            .expect("an EOF run must complete successfully");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_with_cancellation_and_state_format_retains_the_configured_format() {
        let adapter = eof_adapter();
        let (_plan, graph) = build_graph(kernel_spec(false, 1), &adapter);
        let handle = KernelJobRunner::spawn_with_cancellation_and_state_format(
            graph,
            Vec::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            true,
            7,
            CancellationToken::new(),
        )
        .await
        .expect("spawn must succeed");
        assert_eq!(handle.state_format, 7);
        handle.stop();
        let _ = handle.watcher().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_prepared_with_cancellation_and_state_format_retains_the_format() {
        let input = Arc::new(OneBatchThenEofInput {
            sent: Mutex::new(false),
        });
        let adapter = adapter_with(input.clone());
        let (_plan, graph) = build_graph(kernel_spec(false, 1), &adapter);
        input.connect().await.unwrap();
        let handle = KernelJobRunner::spawn_prepared_with_cancellation_and_state_format(
            graph,
            Vec::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            5,
            CancellationToken::new(),
        )
        .await
        .expect("prepared spawn must succeed");
        assert_eq!(handle.state_format, 5);
        handle.stop();
        let _ = handle.watcher().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_closes_every_input_when_one_connection_fails() {
        let failing = Arc::new(FailingConnectInput {
            closes: AtomicUsize::new(0),
        });
        let healthy = Arc::new(NeverEndingInput {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
        });
        let adapter = adapter_with(healthy.clone());
        let (_plan, graph) = build_graph(kernel_spec(false, 1), &adapter);
        let error = KernelJobRunner::spawn(
            graph,
            vec![failing.clone(), healthy.clone()],
            BTreeMap::new(),
            BTreeMap::new(),
            true,
        )
        .await
        .err()
        .expect("a failing source connection must fail the spawn");
        assert!(
            error
                .to_string()
                .contains("injected source connect failure"),
            "{error}"
        );
        assert_eq!(failing.closes.load(Ordering::SeqCst), 1);
        assert_eq!(healthy.closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_fails_when_the_graph_startup_fails() {
        let adapter = KernelAdapter {
            input: Arc::new(OneBatchThenEofInput {
                sent: Mutex::new(false),
            }),
            output: Arc::new(FailingConnectOutput),
            processor: Arc::new(PassThroughProcessor),
        };
        let (_plan, graph) = build_graph(kernel_spec(false, 1), &adapter);
        let error =
            KernelJobRunner::spawn(graph, Vec::new(), BTreeMap::new(), BTreeMap::new(), false)
                .await
                .err()
                .expect("a failing sink connection must fail the spawn");
        assert!(
            error.to_string().contains("injected sink connect failure"),
            "{error}"
        );
    }

    /// Gate construction aborts after the inputs were already connected or
    /// restored: the cleanup path must close every input and state backend
    /// before surfacing the error. (Plan compilation rejects such a graph, so
    /// the watermark is stripped on an already-built one.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_closes_connected_inputs_when_gate_construction_fails() {
        let input = Arc::new(NeverEndingInput {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
        });
        let adapter = adapter_with(input.clone());
        let (_plan, mut graph) = build_graph(kernel_spec(false, 1), &adapter);
        for chain in &mut graph.chains {
            if let Some(time) = &mut chain.source_time {
                time.mode = TimeMode::EventTime;
                time.watermark = None;
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let state: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(directory.path(), 1).unwrap());
        let error = KernelJobRunner::spawn_with_cancellation(
            graph,
            vec![input.clone()],
            BTreeMap::from([("source-0".to_string(), state)]),
            BTreeMap::new(),
            true,
            CancellationToken::new(),
        )
        .await
        .err()
        .expect("a watermark-less event-time source must fail the spawn");
        assert!(error.to_string().contains("watermark"), "{error}");
        assert_eq!(
            input.closes.load(Ordering::SeqCst),
            1,
            "the connected input must be closed during cleanup"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn checkpoint_barrier_snapshots_a_live_stateful_job() {
        let directory = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(directory.path(), 1).unwrap());
        let adapter = adapter_with(Arc::new(NeverEndingInput {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
        }));
        let (plan, graph) = {
            let plan = JobPlan::compile(kernel_spec(true, 1)).unwrap();
            let graph = ExecutionGraphBuilder::default()
                .with_state(backend.clone())
                .build(&plan, &adapter, &resource())
                .unwrap();
            (plan, graph)
        };
        let states = state_map_for_plan(&plan, backend);
        let handle = KernelJobRunner::spawn_with_cancellation(
            graph,
            Vec::new(),
            states,
            BTreeMap::new(),
            true,
            CancellationToken::new(),
        )
        .await
        .expect("spawn must succeed");
        let (snapshot, _positions, _watermarks) = handle
            .checkpoint_barrier("live-checkpoint", 4)
            .await
            .unwrap_or_else(|error| panic!("a live kernel must complete a barrier round: {error}"));
        assert_eq!(snapshot.format_version, 1);
        handle.stop();
        handle
            .watcher()
            .await
            .unwrap()
            .expect("a stopped run must complete gracefully");
    }

    fn state_map_for_plan(
        plan: &JobPlan,
        state: Arc<dyn StateBackend>,
    ) -> BTreeMap<String, Arc<dyn StateBackend>> {
        plan.tasks
            .iter()
            .filter(|task| {
                plan.spec
                    .operators
                    .iter()
                    .any(|operator| operator.id == task.operator_id && operator.stateful)
            })
            .map(|task| (task.id.clone(), state.clone()))
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restore_positions_restores_every_source_input() {
        let input = Arc::new(RecordingPositionsInput {
            restored: Mutex::new(Vec::new()),
        });
        let adapter = adapter_with(input.clone());
        let (_plan, graph) = build_graph(kernel_spec(false, 1), &adapter);
        let handle = KernelJobRunner::spawn_with_cancellation(
            graph,
            vec![input.clone()],
            BTreeMap::new(),
            BTreeMap::new(),
            true,
            CancellationToken::new(),
        )
        .await
        .expect("spawn must succeed");
        let position = SourcePosition {
            topic: Some("orders".into()),
            partition: 1,
            offset: 22,
        };
        handle
            .restore_positions(std::slice::from_ref(&position))
            .await
            .unwrap();
        assert_eq!(*input.restored.lock().unwrap(), vec![position]);
        handle.stop();
        let _ = handle.watcher().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restore_watermarks_installs_physical_and_legacy_progress() {
        let input = Arc::new(RecordingPositionsInput {
            restored: Mutex::new(Vec::new()),
        });
        let adapter = adapter_with(input.clone());
        let (_plan, graph) = build_graph(kernel_spec(false, 1), &adapter);
        let mut gates = BTreeMap::new();
        for task in ["source-0", "source-1", "source-2"] {
            let time = crate::job::TimeSpec {
                mode: TimeMode::EventTime,
                timestamp_field: Some("ts".into()),
                watermark: Some(WatermarkSpec {
                    strategy: WatermarkStrategy::BoundedOutOfOrderness,
                    out_of_orderness_ms: 0,
                    idle_timeout_ms: None,
                }),
                allowed_lateness_ms: 0,
                late_event_policy: Default::default(),
                late_event_route: None,
            };
            let gate = super::super::event_time_gate::EventTimeGate::new(
                &time,
                Vec::<super::super::event_time_gate::WindowTiming>::new(),
            )
            .unwrap();
            gates.insert(
                task.to_string(),
                Arc::new(tokio::sync::Mutex::new(Some(gate))),
            );
        }
        let mut handle = KernelJobRunner::spawn_with_cancellation(
            graph,
            vec![input],
            BTreeMap::new(),
            gates.clone(),
            true,
            CancellationToken::new(),
        )
        .await
        .expect("spawn must succeed");
        // Bind the fallback partition of the third gate before restoring.
        handle.gate_partitions.insert("source-2".to_string(), 5);

        // Physical per-partition progress restores onto the real partitions.
        handle
            .restore_watermarks_with_partitions(
                &BTreeMap::new(),
                &BTreeMap::from([(
                    "source-0".to_string(),
                    vec![WatermarkPosition::new(Some("orders".into()), 3, 1_000)],
                )]),
            )
            .await
            .unwrap();
        let gate = handle.watermark_gates().get("source-0").unwrap().clone();
        let known = gate.lock().await.as_ref().unwrap().known_partitions();
        assert!(known.contains(
            &crate::event_time::EventTimePartition::new(Some("orders".into()), 3)
                .with_source_identity("source-0")
        ));

        // Legacy task-level restore fans out to every known partition.
        handle
            .restore_watermarks(&BTreeMap::from([("source-0".to_string(), 2_000_i64)]))
            .await
            .unwrap();

        // With no known partitions the restore falls back to the chain's
        // configured physical partition (5), not partition 0.
        handle
            .restore_watermarks(&BTreeMap::from([("source-2".to_string(), 7_000_i64)]))
            .await
            .unwrap();
        let gate = handle.watermark_gates().get("source-2").unwrap().clone();
        let known = gate.lock().await.as_ref().unwrap().known_partitions();
        assert_eq!(
            known,
            vec![crate::event_time::EventTimePartition::for_source(
                "source-2", 5
            )]
        );

        // And without a configured partition the fallback is partition 0.
        handle
            .restore_watermarks(&BTreeMap::from([("source-1".to_string(), 9_000_i64)]))
            .await
            .unwrap();
        let gate = handle.watermark_gates().get("source-1").unwrap().clone();
        let known = gate.lock().await.as_ref().unwrap().known_partitions();
        assert_eq!(
            known,
            vec![crate::event_time::EventTimePartition::for_source(
                "source-1", 0
            )]
        );
        handle.stop();
        let _ = handle.watcher().await;
    }

    /// A task carrying physical watermark progress is skipped by the legacy
    /// fan-out in the same call, and a taken gate is skipped silently.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restore_watermarks_skips_physical_progress_and_taken_gates() {
        let input = Arc::new(RecordingPositionsInput {
            restored: Mutex::new(Vec::new()),
        });
        let adapter = adapter_with(input.clone());
        let (_plan, graph) = build_graph(kernel_spec(false, 1), &adapter);
        let time = crate::job::TimeSpec {
            mode: TimeMode::EventTime,
            timestamp_field: Some("ts".into()),
            watermark: Some(WatermarkSpec {
                strategy: WatermarkStrategy::BoundedOutOfOrderness,
                out_of_orderness_ms: 0,
                idle_timeout_ms: None,
            }),
            allowed_lateness_ms: 0,
            late_event_policy: Default::default(),
            late_event_route: None,
        };
        let gate = super::super::event_time_gate::EventTimeGate::new(
            &time,
            Vec::<super::super::event_time_gate::WindowTiming>::new(),
        )
        .unwrap();
        let mut gates = BTreeMap::new();
        gates.insert(
            "source-0".to_string(),
            Arc::new(tokio::sync::Mutex::new(Some(gate))),
        );
        gates.insert("taken".to_string(), Arc::new(tokio::sync::Mutex::new(None)));
        let handle = KernelJobRunner::spawn_with_cancellation(
            graph,
            vec![input],
            BTreeMap::new(),
            gates,
            true,
            CancellationToken::new(),
        )
        .await
        .expect("spawn must succeed");

        // Both maps mention source-0: the legacy value must be skipped.
        handle
            .restore_watermarks_with_partitions(
                &BTreeMap::from([("source-0".to_string(), 3_000_i64)]),
                &BTreeMap::from([(
                    "source-0".to_string(),
                    vec![WatermarkPosition::new(Some("orders".into()), 1, 1_000)],
                )]),
            )
            .await
            .unwrap();
        // A taken gate is skipped even when it is targeted directly.
        handle
            .restore_watermarks_with_partitions(
                &BTreeMap::from([("taken".to_string(), 3_000_i64)]),
                &BTreeMap::from([(
                    "taken".to_string(),
                    vec![WatermarkPosition::new(Some("orders".into()), 1, 1_000)],
                )]),
            )
            .await
            .unwrap();
        let gate = handle.watermark_gates().get("taken").unwrap().clone();
        assert!(
            gate.lock().await.as_ref().is_none(),
            "a taken gate must stay untouched"
        );
        handle.stop();
        let _ = handle.watcher().await;
    }

    // ---------- barrier round drain interleavings ----------

    /// A stale report queued ahead of the exiting chain's report is drained
    /// and ignored inside the finished-branch drain loop. Whichever order
    /// the select observes the ready branches in, the round must succeed.
    #[tokio::test]
    async fn checkpoint_drain_ignores_stale_reports_before_the_exiting_chain() {
        for _ in 0..16 {
            let (handle, channels) = synthetic_handle(&["a"], CancellationToken::new());
            let task = tokio::spawn(async move { handle.checkpoint_barrier("cp-1", 0).await });
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            channels
                .report
                .send(snapshot_report("a", "abandoned-round", 7))
                .unwrap();
            channels
                .report
                .send(snapshot_report("a", "cp-1", 0))
                .unwrap();
            channels.finished.send("a".into()).unwrap();
            task.await
                .unwrap()
                .expect("the drained round must still seal with the matching report");
        }
    }

    /// The drain loop rejects unknown-chain reports whether they are drained
    /// behind a finished notification or consumed by the main loop first.
    #[tokio::test]
    async fn checkpoint_drain_rejects_reports_from_unknown_chains() {
        for _ in 0..8 {
            let (handle, channels) = synthetic_handle(&["a"], CancellationToken::new());
            let task = tokio::spawn(async move { handle.checkpoint_barrier("cp-1", 0).await });
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            channels
                .report
                .send(snapshot_report("zzz", "cp-1", 0))
                .unwrap();
            channels.finished.send("a".into()).unwrap();
            let error = task
                .await
                .unwrap()
                .expect_err("an unknown chain must fail the round");
            assert!(error.to_string().contains("unknown chain"), "{error}");
        }
    }

    /// A duplicate report fails the round from either the drain loop or the
    /// main loop. A second participant keeps the round open until both
    /// copies of the duplicated report have been consumed.
    #[tokio::test]
    async fn checkpoint_drain_rejects_duplicate_reports() {
        for _ in 0..8 {
            let (handle, channels) = synthetic_handle(&["a", "b"], CancellationToken::new());
            let task = tokio::spawn(async move { handle.checkpoint_barrier("cp-1", 0).await });
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            channels
                .report
                .send(snapshot_report("a", "cp-1", 0))
                .unwrap();
            channels
                .report
                .send(snapshot_report("a", "cp-1", 0))
                .unwrap();
            channels.finished.send("a".into()).unwrap();
            let error = task
                .await
                .unwrap()
                .expect_err("a duplicate report must fail the round");
            assert!(
                error
                    .to_string()
                    .contains("duplicate chain checkpoint report"),
                "{error}"
            );
        }
    }

    /// The drain treats reports-channel closure as termination: whether the
    /// closure surfaces through the finished branch's `try_recv` or the main
    /// loop's `recv`, the round fails with an ended-kernel error.
    #[tokio::test]
    async fn checkpoint_drain_treats_reports_closure_as_termination() {
        for _ in 0..8 {
            let (handle, channels) = synthetic_handle(&["a", "b"], CancellationToken::new());
            let task = tokio::spawn(async move { handle.checkpoint_barrier("cp-1", 0).await });
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            channels
                .report
                .send(snapshot_report("b", "cp-1", 0))
                .unwrap();
            drop(channels.report);
            channels.finished.send("a".into()).unwrap();
            let error = task
                .await
                .unwrap()
                .expect_err("a closed reports channel must fail the round");
            assert!(error.to_string().contains("ended"), "{error}");
        }
    }

    /// The reports channel closing with the error and finished channels
    /// still alive is observed directly by the main loop.
    #[tokio::test]
    async fn checkpoint_fails_when_the_reports_channel_closes() {
        let (handle, channels) = synthetic_handle(&["a"], CancellationToken::new());
        drop(channels.report);
        // Keep the error sender alive so only the reports closure is ready.
        let error_sender = channels.error;
        let result = handle.checkpoint_barrier("cp-1", 0).await;
        drop(error_sender);
        let error = result.expect_err("a closed reports channel must fail the round");
        assert!(
            error
                .to_string()
                .contains("kernel ended before checkpoint completed"),
            "{error}"
        );
    }

    // ---------- startup panics and stateful flows ----------

    struct PanickingOutput;

    #[async_trait]
    impl Output for PanickingOutput {
        async fn connect(&self) -> Result<(), Error> {
            panic!("injected sink startup panic");
        }
        async fn write(&self, _msg: MessageBatchRef) -> Result<(), Error> {
            Ok(())
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    /// A panic inside the graph startup drops the startup oneshot sender:
    /// spawn fails with the exited-before-readiness error, and the panic is
    /// captured into the completion slot instead of tearing down the test.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_fails_when_the_graph_startup_panics() {
        let adapter = KernelAdapter {
            input: Arc::new(OneBatchThenEofInput {
                sent: Mutex::new(false),
            }),
            output: Arc::new(PanickingOutput),
            processor: Arc::new(PassThroughProcessor),
        };
        let (_plan, graph) = build_graph(kernel_spec(false, 1), &adapter);
        let error =
            KernelJobRunner::spawn(graph, Vec::new(), BTreeMap::new(), BTreeMap::new(), false)
                .await
                .err()
                .expect("a panicking startup must fail the spawn");
        assert!(
            error
                .to_string()
                .contains("kernel graph startup task exited before readiness"),
            "{error}"
        );
    }

    /// A batch with the aggregate's key column, so stateful chains accept it.
    struct KeyedBatchThenEofInput {
        sent: Mutex<bool>,
    }

    #[async_trait]
    impl Input for KeyedBatchThenEofInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            let mut sent = self.sent.lock().unwrap();
            if *sent {
                return Err(Error::EOF);
            }
            *sent = true;
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(datafusion::arrow::array::StringArray::from(vec!["k1"])),
                    Arc::new(Int64Array::from(vec![1])),
                ],
            )
            .unwrap();
            Ok((
                Arc::new(MessageBatch::new_arrow(batch)),
                Arc::new(crate::input::NoopAck),
            ))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    /// A stateful graph delivers its batches through the plan's processors.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stateful_graph_flows_batches_through_the_processor() {
        let directory = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(directory.path(), 1).unwrap());
        let input = Arc::new(KeyedBatchThenEofInput {
            sent: Mutex::new(false),
        });
        let adapter = adapter_with(input.clone());
        let (plan, graph) = {
            let plan = JobPlan::compile(kernel_spec(true, 1)).unwrap();
            let graph = ExecutionGraphBuilder::default()
                .with_state(backend.clone())
                .build(&plan, &adapter, &resource())
                .unwrap();
            (plan, graph)
        };
        let handle = KernelJobRunner::spawn_with_cancellation(
            graph,
            vec![input],
            state_map_for_plan(&plan, backend),
            BTreeMap::new(),
            true,
            CancellationToken::new(),
        )
        .await
        .expect("spawn must succeed");
        handle
            .watcher()
            .await
            .unwrap()
            .expect("an EOF stateful run must complete successfully");
    }

    /// Preconnected sources that never managed to connect still surface
    /// end-of-stream through their read path, ending the chain cleanly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn preconnected_source_reports_eof_from_read() {
        let failing = Arc::new(FailingConnectInput {
            closes: AtomicUsize::new(0),
        });
        let adapter = adapter_with(failing.clone());
        let (_plan, graph) = build_graph(kernel_spec(false, 1), &adapter);
        let handle = KernelJobRunner::spawn_prepared_with_cancellation(
            graph,
            Vec::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            CancellationToken::new(),
        )
        .await
        .expect("spawn must succeed");
        handle
            .watcher()
            .await
            .unwrap()
            .expect("an EOF read must complete the run gracefully");
    }
}
