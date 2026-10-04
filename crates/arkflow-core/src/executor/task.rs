//! Per-chain event loops: the pipelined execution core.
//!
//! Each chain runs its own task. Source chains pull from their input, run the
//! chain's processors, and push envelopes downstream; interior/sink chains
//! receive from their inbound channels. Because every chain runs concurrently
//! and edges are bounded, a slow consumer backpressures its producer while
//! other chains keep flowing.

use super::envelope::Envelope;
use super::graph::{Chain, EdgeTarget, ExecutionGraph};
use crate::input::{fanout_ack, Ack, Input};
use crate::output::Output;
use crate::Error;
use crate::ProcessResult;
use datafusion::arrow::array::{
    Array, BinaryArray, Date32Array, Date64Array, Decimal128Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

/// Upper bound on how long a source chain waits for in-flight (non-held)
/// acknowledgements to drain before sealing a checkpoint cut. Exceeding it
/// fails the barrier round (the last valid checkpoint is retained) instead of
/// blocking the source indefinitely behind a wedged sink.
const BARRIER_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Upper bound on how long a chain's control-event fence waits for the
/// processor worker pool to publish an in-flight delivery. A pool whose worker
/// or collector exited without recording a failure cannot advance its sequence,
/// and an unbounded fence would park the chain with no error and no checkpoint
/// progress; exceeding this bound reports the condition instead.
const FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Upper bound on joining a pool's result collector during shutdown. The
/// collector's final drain flushes downstream, which can block on a full edge
/// whose consumer already stopped.
const COLLECTOR_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Bound on one sink write: a hung external system fails the chain (and
/// thereby unblocks shutdown) instead of parking the loop body forever.
/// Generous for legitimately slow bulk uploads — this catches dead
/// connections, not slow ones.
const SINK_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5 * 60);

/// Test override for the sink-write bound (0 = use the default).
#[cfg(test)]
static SINK_WRITE_TIMEOUT_OVERRIDE_MS: AtomicU64 = AtomicU64::new(0);

fn sink_write_timeout() -> std::time::Duration {
    #[cfg(test)]
    {
        let override_ms = SINK_WRITE_TIMEOUT_OVERRIDE_MS.load(Ordering::Acquire);
        if override_ms > 0 {
            return std::time::Duration::from_millis(override_ms);
        }
    }
    SINK_WRITE_TIMEOUT
}

/// Shrink the sink-write bound. Test-only.
#[cfg(test)]
pub(crate) fn override_sink_write_timeout_for_tests(timeout: std::time::Duration) {
    SINK_WRITE_TIMEOUT_OVERRIDE_MS.store(timeout.as_millis() as u64, Ordering::Release);
}

/// Drive every chain in the graph to completion (cancellation or all-source
/// end-of-stream). Connects inputs/outputs first and closes them after.
/// Optional per-chain checkpoint hook: barriers injected at sources are
/// forwarded with data; chains report snapshots through the sender.
#[derive(Clone, Default)]
pub struct CheckpointHook {
    /// Report snapshots for this chain's entry task (source chains report
    /// their input positions; stateful chains report their keyed state).
    pub reporter: Option<tokio::sync::mpsc::UnboundedSender<super::barrier::ChainSnapshot>>,
    /// Report a checkpoint-local snapshot failure without terminating the
    /// data-plane chain. A failed checkpoint must leave normal processing
    /// running so the next barrier can retry.
    pub failure_reporter: Option<tokio::sync::mpsc::UnboundedSender<Error>>,
    /// Barrier reception point for source chains (injected by a coordinator).
    pub barrier_rx: Option<Arc<tokio::sync::Mutex<flume::Receiver<super::envelope::Envelope>>>>,
    /// State backend to snapshot when a barrier passes this chain.
    pub state: Option<Arc<dyn crate::state::StateBackend>>,
    /// Stable task identity for checkpoint reports.
    pub task_id: Option<String>,
    /// Event-time gate for source chains (None = processing time).
    pub event_time_gate: Arc<tokio::sync::Mutex<Option<super::event_time_gate::EventTimeGate>>>,
    /// Source partition bound to this chain (event-time observation).
    pub partition: Option<u32>,
    /// Runtime counters for control-plane snapshots (source chains bump
    /// input counts; dispatch paths bump output/error counts).
    pub metrics: Option<Arc<crate::runtime::RuntimeMetrics>>,
    /// Reports this chain's exit to the coordinator: a chain whose event loop
    /// returned can no longer process barriers or send checkpoint reports, so
    /// barrier rounds must exempt it from the required participant set.
    pub finished_reporter: Option<tokio::sync::mpsc::UnboundedSender<String>>,
}

pub async fn run_graph(
    graph: ExecutionGraph,
    cancellation: CancellationToken,
) -> Result<(), Error> {
    run_graph_with_hooks(graph, cancellation, BTreeMap::new()).await
}

/// Run the graph with runtime metrics: source chains count input
/// batches/rows, processing errors and outputs update on every envelope
/// result, mirroring the legacy runtime counters.
pub async fn run_graph_with_metrics(
    graph: ExecutionGraph,
    cancellation: CancellationToken,
    metrics: Option<Arc<crate::runtime::RuntimeMetrics>>,
) -> Result<(), Error> {
    run_graph_with_metrics_startup(graph, cancellation, metrics, None).await
}

/// Metrics-enabled graph runner variant that reports when all graph resources
/// have connected. Runtime-managed streams use this handshake to publish
/// `Running` only after the asynchronous resource phase has succeeded.
pub(crate) async fn run_graph_with_metrics_startup(
    graph: ExecutionGraph,
    cancellation: CancellationToken,
    metrics: Option<Arc<crate::runtime::RuntimeMetrics>>,
    startup: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
) -> Result<(), Error> {
    // Chain-level metric plumbing rides the hooks map: each source chain
    // gets a metrics-enabled hook so its loop can bump counters.
    let mut hooks = BTreeMap::new();
    if let Some(metrics) = &metrics {
        for chain in &graph.chains {
            hooks.insert(
                chain.entry_task_id().to_owned(),
                CheckpointHook {
                    metrics: Some(metrics.clone()),
                    ..Default::default()
                },
            );
        }
    }
    run_graph_with_hooks_startup(graph, cancellation, hooks, false, startup).await
}

/// Compatibility wrapper for the retired global checkpoint gate. Barriers are
/// now ordered control envelopes; the supplied gate is intentionally ignored.
pub async fn run_graph_with_gate(
    graph: ExecutionGraph,
    cancellation: CancellationToken,
    gate: super::kernel_handle::SnapshotGate,
) -> Result<(), Error> {
    let _ = gate;
    run_graph_with_hooks(graph, cancellation, BTreeMap::new()).await
}

/// Run the graph with per-chain checkpoint hooks keyed by entry task id.
pub async fn run_graph_with_hooks(
    graph: ExecutionGraph,
    cancellation: CancellationToken,
    hooks: BTreeMap<String, CheckpointHook>,
) -> Result<(), Error> {
    run_graph_inner(graph, cancellation, hooks, false, None).await
}

/// Internal runner variant used by command-driven Jobs to await resource
/// startup before returning a handle to the caller.
pub(crate) async fn run_graph_with_hooks_startup(
    graph: ExecutionGraph,
    cancellation: CancellationToken,
    hooks: BTreeMap<String, CheckpointHook>,
    sources_preconnected: bool,
    startup: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
) -> Result<(), Error> {
    run_graph_inner(graph, cancellation, hooks, sources_preconnected, startup).await
}

/// Compatibility wrapper for callers that still pass the retired global gate.
/// The barrier path itself remains fully asynchronous and does not acquire a
/// job-wide read/write lock.
pub async fn run_graph_with_hooks_and_gate(
    graph: ExecutionGraph,
    cancellation: CancellationToken,
    hooks: BTreeMap<String, CheckpointHook>,
    gate: super::kernel_handle::SnapshotGate,
) -> Result<(), Error> {
    let _ = gate;
    run_graph_with_hooks(graph, cancellation, hooks).await
}

async fn run_graph_inner(
    graph: ExecutionGraph,
    cancellation: CancellationToken,
    hooks: BTreeMap<String, CheckpointHook>,
    sources_preconnected: bool,
    startup: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
) -> Result<(), Error> {
    // Root span for this graph execution. The future is instrumented (never
    // enter-guarded across await points), so every span and event created
    // within becomes a child of job.run.
    let job_span = tracing::info_span!("job.run", chains = graph.chains.len() as i64,);
    run_graph_inner_instrumented(graph, cancellation, hooks, sources_preconnected, startup)
        .instrument(job_span)
        .await
}

async fn run_graph_inner_instrumented(
    graph: ExecutionGraph,
    cancellation: CancellationToken,
    hooks: BTreeMap<String, CheckpointHook>,
    sources_preconnected: bool,
    mut startup: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
) -> Result<(), Error> {
    // Connect every resource in dependency order (temporary stores first,
    // then sources and sinks) before any task loop spawns: a processor's
    // first `get` cannot race a temporary's `connect`, and a partial startup
    // closes what it opened in reverse order.
    let sources: Vec<Arc<dyn Input>> = graph
        .chains
        .iter()
        .filter_map(|chain| chain.source.clone())
        .collect();
    let sinks: Vec<Arc<dyn Output>> = graph
        .chains
        .iter()
        .filter_map(|chain| chain.sink.clone())
        .collect();
    let mut states: Vec<(String, Arc<dyn crate::state::StateBackend>)> = Vec::new();
    for hook in hooks.values() {
        if let Some(state) = &hook.state {
            if !states
                .iter()
                .any(|(_, existing)| Arc::ptr_eq(existing, state))
            {
                states.push((hook.task_id.clone().unwrap_or_default(), state.clone()));
            }
        }
    }
    let guard = if sources_preconnected {
        // Recovery path: the caller connected the sources and restored their
        // positions before spawning; connect the remaining sinks.
        match super::resource_guard::JobResourceGuard::connect(
            &graph.temporaries,
            &[],
            &sinks,
            &states,
        )
        .await
        {
            Ok(guard) => guard,
            Err(error) => {
                // These inputs were connected by the recovery preparer and
                // are not yet owned by a chain task.
                for source in sources.iter().rev() {
                    let _ = source.close().await;
                }
                let _ = startup
                    .take()
                    .map(|sender| sender.send(Err(error.to_string())));
                return Err(error);
            }
        }
    } else {
        match super::resource_guard::JobResourceGuard::connect(
            &graph.temporaries,
            &sources,
            &sinks,
            &states,
        )
        .await
        {
            Ok(guard) => guard,
            Err(error) => {
                let _ = startup
                    .take()
                    .map(|sender| sender.send(Err(error.to_string())));
                return Err(error);
            }
        }
    };

    // At this point every resource is connected and the graph can be
    // considered started. Only now let a caller publish readiness.
    let _ = startup.take().map(|sender| sender.send(Ok(())));

    let mut tasks = FuturesUnordered::new();
    for chain in graph.chains {
        let token = cancellation.clone();
        let hook = hooks
            .get(chain.entry_task_id())
            .cloned()
            .unwrap_or_default();
        // Created while the job span is entered, so this span is its child.
        // tokio child tasks do not inherit span context: the span is moved
        // into the task and entered there.
        let chain_span = tracing::info_span!("chain.run", task = chain.entry_task_id());
        tasks.push(tokio::spawn(
            async move {
                let edge_failures = chain.edge_failures.clone();
                let Some(edge_failures) = edge_failures else {
                    return run_chain(chain, hook, token).await;
                };
                // Remote-edge failure watcher: an idle chain (blocked on input)
                // never observes a dead edge on its own send path, so a manager
                // failure cancels the chain and surfaces as its result. The chain
                // still exits through its own cancellation path, closing every
                // owned resource.
                let (failure_tx, mut failure_rx) = tokio::sync::oneshot::channel::<Error>();
                let watcher_token = token.clone();
                let watcher = tokio::spawn(async move {
                    if let Ok(error) = edge_failures.recv_async().await {
                        watcher_token.cancel();
                        let _ = failure_tx.send(error);
                    }
                });
                let result = run_chain(chain, hook, token).await;
                let result = match (result, failure_rx.try_recv()) {
                    (Ok(()), Ok(error)) => Err(error),
                    (result, _) => result,
                };
                watcher.abort();
                result
            }
            .instrument(chain_span),
        ));
    }
    // The chains own their source/sink close paths from here on.
    guard.hand_off_stream_resources();

    let mut first_error = None;
    while let Some(result) = tasks.next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if first_error.is_none() {
                    cancellation.cancel();
                    first_error = Some(error);
                }
            }
            Err(join_error) => {
                if first_error.is_none() {
                    cancellation.cancel();
                    first_error =
                        Some(Error::Process(format!("chain task panicked: {join_error}")));
                }
            }
        }
    }

    // Close temporary stores and state backends after every chain exited
    // (reverse dependency order). A close failure surfaces alongside the
    // chain error rather than being swallowed.
    let close_result = guard.close().await;
    match (first_error, close_result) {
        (Some(error), _) => Err(error),
        (None, Err(error)) => Err(error),
        (None, Ok(())) => Ok(()),
    }
}

/// The event loop of one chain.
async fn run_chain(
    chain: Chain,
    hook: CheckpointHook,
    cancellation: CancellationToken,
) -> Result<(), Error> {
    // Catch a panicking event loop so the owned components are still closed:
    // `hand_off_stream_resources` gave the chains their source/sink close
    // paths, and an unwinding panic would otherwise skip every close and
    // leak the connector/WAL handles for the remaining process lifetime.
    let result = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(run_chain_inner(
        &chain,
        &hook,
        &cancellation,
    )))
    .await
    .unwrap_or_else(|panic| {
        Err(Error::Process(format!(
            "chain task panicked: {}",
            panic_payload(&panic)
        )))
    });

    // The event loop has returned: this chain can no longer process barriers
    // or send checkpoint reports. Tell the coordinator on every exit path so
    // barrier rounds stop requiring (and stop injecting barriers into) it.
    if let Some(finished) = &hook.finished_reporter {
        if let Some(task_id) = hook.task_id.as_deref() {
            let _ = finished.send(task_id.to_owned());
        }
    }

    // Close owned components on every exit path, keeping the first close
    // failure observable while still attempting every remaining component.
    let mut close_error = None;
    for processor in &chain.processors {
        if let Err(error) = processor.close().await {
            tracing::warn!(%error, task = chain.entry_task_id(), "failed to close chain processor");
            close_error.get_or_insert(error);
        }
    }
    if let Some(sink) = &chain.sink {
        if let Err(error) = sink.close().await {
            tracing::warn!(%error, task = chain.entry_task_id(), "failed to close chain sink");
            close_error.get_or_insert(error);
        }
    }
    if let Some(source) = &chain.source {
        if let Err(error) = source.close().await {
            tracing::warn!(%error, task = chain.entry_task_id(), "failed to close chain source");
            close_error.get_or_insert(error);
        }
    }
    match result {
        Err(error) => Err(error),
        Ok(()) => close_error.map_or(Ok(()), Err),
    }
}

async fn run_chain_inner(
    chain: &Chain,
    hook: &CheckpointHook,
    cancellation: &CancellationToken,
) -> Result<(), Error> {
    match (&chain.source, chain.inputs.len()) {
        (Some(source), 0) => run_source_chain(chain, source, hook, cancellation).await,
        (None, _) if !chain.inputs.is_empty() => {
            run_interior_chain(chain, hook, cancellation).await
        }
        _ => Err(Error::Config(format!(
            "chain '{}' has neither a source nor input channels",
            chain.entry_task_id()
        ))),
    }
}

/// Source chain: read batches, run the chain, push downstream. A bounded
/// source ends with `Error::EOF`, which finishes this chain normally; the
/// channel close then drains downstream chains.
///
/// When a checkpoint hook carries a barrier receiver, the coordinator's
/// barriers are interleaved between reads: the barrier flows downstream in
/// FIFO order with the data and the source reports its positions.
///
/// Event-time sources gate batches through `EventTimeGate` (hold/emit/route/
/// drop per row, watermark tracking, deferred acks while rows are held).
async fn run_source_chain(
    chain: &Chain,
    source: &Arc<dyn Input>,
    hook: &CheckpointHook,
    cancellation: &CancellationToken,
) -> Result<(), Error> {
    let mut barrier_rx = hook.barrier_rx.clone();
    let event_gate = hook.event_time_gate.clone();
    // Seals each barrier's acknowledged cut: positions and watermark frozen
    // at injection time, before the barrier flows downstream.
    let frontier = super::commit::CommitFrontier::new();
    // In-flight acknowledgement tracker for barrier draining: the source
    // waits (bounded) until every dispatched, non-held acknowledgement has
    // fully completed — state apply, WAL-cursor advance, and source-side
    // commit — so the sealed positions and the committed state snapshot
    // describe the same acknowledged set.
    let tracker = Arc::new(super::commit::AckTracker::new());
    // Local stream execution and callers that use the plain graph runner do
    // not need to manufacture a gate map themselves.  The graph carries the
    // source time contract, so initialize the same gate used by Agent mode at
    // the source boundary.
    if let Some(time) = &chain.source_time {
        if time.mode == crate::job::TimeMode::EventTime {
            let mut guard = event_gate.lock().await;
            if guard.is_none() {
                *guard = Some(super::event_time_gate::EventTimeGate::new(
                    time,
                    chain.window_timings.clone(),
                )?);
            }
            drop(guard);
            seed_event_time_partitions(source, &event_gate, hook).await?;
        }
    }
    let mut idle_tick = tokio::time::interval(std::time::Duration::from_millis(100));
    idle_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let read = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                // Stop pulling new source data, release any event-time rows
                // already accepted, and close the downstream data plane with
                // an explicit EOS.  Unprocessed input acks are intentionally
                // left unacknowledged so WAL-backed sources can replay them.
                return shutdown_source_chain(chain, hook).await;
            }
            barrier = async {
                match barrier_rx.as_ref() {
                    Some(receiver) => receiver.lock().await.recv_async().await,
                    None => std::future::pending().await,
                }
            } => {
                let Some(barrier) = barrier.ok() else {
                    // Coordinator dropped the barrier channel: stop injecting.
                    barrier_rx = None;
                    continue;
                };
                let Envelope::Barrier(barrier) = barrier else {
                    continue;
                };
                // Drain in-flight (non-held) acknowledgements before sealing.
                // An acknowledgement completes only after its state apply and
                // source-side commit; held acknowledgements (gate/window
                // buffers whose mutations stay staged in the journal) are
                // excluded, so an open window never stalls a barrier.
                {
                    let drain = async {
                        while tracker.blocking() > 0 {
                            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                        }
                    };
                    tokio::select! {
                        _ = cancellation.cancelled() => {
                            return shutdown_source_chain(chain, hook).await;
                        }
                        _ = drain => {}
                        _ = tokio::time::sleep(BARRIER_DRAIN_TIMEOUT) => {
                            if let Some(reporter) = &hook.failure_reporter {
                                let _ = reporter.send(Error::Process(format!(
                                    "checkpoint barrier drain timed out with {} acknowledgements still in flight",
                                    tracker.blocking()
                                )));
                            }
                            // Abort the round: the barrier is consumed but not
                            // sealed and not forwarded, so no cut can persist
                            // positions ahead of acknowledgements still in
                            // flight (recovery would lose that window). The
                            // reported failure fails the round; the next round
                            // injects a fresh barrier once the pipeline drains.
                            continue;
                        }
                    }
                }
                let positions = tokio::select! {
                    _ = cancellation.cancelled() => {
                        return shutdown_source_chain(chain, hook).await;
                    }
                    positions = source.current_positions() => match positions {
                        Ok(positions) => positions,
                        Err(error) => {
                            // Fail the round closed: seeding the frontier with an
                            // empty vec would seal stale (or first-round empty)
                            // positions while the reported error races the report
                            // drain and can be lost. The barrier is consumed but
                            // not sealed and not forwarded, like the drain
                            // timeout, so no cut can persist positions this chain
                            // cannot vouch for.
                            if let Some(reporter) = &hook.failure_reporter {
                                let _ = reporter.send(error);
                            }
                            continue;
                        }
                    },
                };
                let (watermark_ms, watermark_partitions) = hook
                    .event_time_gate
                    .lock()
                    .await
                    .as_ref()
                    .map(|gate| (gate.watermark(), gate.watermark_positions()))
                    .unwrap_or((None, Vec::new()));
                // Seal the acknowledged cut before the barrier flows
                // downstream. The source loop is single-threaded: the cut's
                // positions and watermark are frozen at this instant and the
                // report cannot observe acknowledgements completing after
                // the seal. Pre-cut outputs with pending acknowledgements
                // stay outside the cut; recovery replays them from the
                // sealed positions.
                frontier.seed(&positions);
                let cut = frontier.seal(watermark_ms);
                // Capture the source chain's state before forwarding the
                // barrier. Forwarding first would let the next loop turn
                // around, process post-barrier input, and mutate the same
                // backend while a detached snapshot was still reading it.
                // The snapshot uses spawn_blocking internally, so this only
                // yields this source loop and does not block the async
                // runtime's worker threads.
                let state = match hook.state.clone() {
                    Some(backend) => tokio::select! {
                        _ = cancellation.cancelled() => {
                            return shutdown_source_chain(chain, hook).await;
                        }
                        state = super::barrier::snapshot_state(backend) => state,
                    },
                    None => Ok(crate::state::StateSnapshot::new(1, Vec::new())),
                };
                match state {
                    Ok(state) => {
                        tokio::select! {
                            _ = cancellation.cancelled() => {
                                return shutdown_source_chain(chain, hook).await;
                            }
                            result = send_downstream(
                                chain,
                                Envelope::Barrier(barrier.clone()),
                            ) => result?,
                        }
                        if let Some(reporter) = &hook.reporter {
                            let task_id = hook
                                .task_id
                                .clone()
                                .unwrap_or_else(|| chain.entry_task_id().to_owned());
                            let _ = reporter.send(super::barrier::ChainSnapshot {
                                task_id: task_id.clone(),
                                attempt_id: format!("{task_id}-attempt"),
                                partition: hook.partition.unwrap_or_default(),
                                barrier: barrier.clone(),
                                cut_generation: cut.generation,
                                state,
                                source_positions: cut.positions,
                                watermark_ms: cut.watermark_ms,
                                watermark_partitions,
                            });
                        }
                    }
                    Err(error) => {
                        if let Some(reporter) = &hook.failure_reporter {
                            let _ = reporter.send(error);
                        }
                        tokio::select! {
                            _ = cancellation.cancelled() => {
                                return shutdown_source_chain(chain, hook).await;
                            }
                            result = send_downstream(
                                chain,
                                Envelope::Barrier(barrier.clone()),
                            ) => result?,
                        }
                    }
                }
                continue;
            }
            _ = idle_tick.tick() => {
                // Idle tick: refresh held rows (idle partitions unblock the
                // watermark) and fire acks that are no longer deferred.
                // Kafka group assignment can become visible after connect;
                // discover it before refreshing so an idle physical
                // partition participates in the minimum even before its
                // first record arrives.
                if let Err(error) = seed_event_time_partitions(source, &event_gate, hook).await {
                    let _ = abort_event_time_held(hook).await;
                    return Err(error);
                }
                // `refresh` is synchronous, but extracting its result first
                // lets us release the gate mutex before compensating held
                // acknowledgements on an error.  A connector/backend error
                // must not leave the source delivery permanently held.
                let decision_result = {
                    let mut guard = event_gate.lock().await;
                    match guard.as_mut() {
                        None => Ok(None),
                        Some(gate) => gate.refresh().map(Some),
                    }
                };
                let decision = match decision_result {
                    Ok(Some(decision)) => decision,
                    Ok(None) => continue,
                    Err(error) => {
                        let _ = abort_event_time_held(hook).await;
                        return Err(error);
                    }
                };
                let (
                    ready,
                    ready_acks,
                    ready_invalid_timestamps,
                    dropped_acks,
                    watermark,
                    late_rows,
                    current_ready_start,
                ) = (
                    decision.ready,
                    decision.ready_acks,
                    decision.ready_invalid_timestamps,
                    decision.dropped_acks,
                    decision.watermark_ms,
                    decision.late_event_rows,
                    decision.current_ready_start,
                );
                dispatch_gate_outputs(
                    chain,
                    hook,
                    ready,
                    ready_acks,
                    ready_invalid_timestamps,
                    dropped_acks,
                    watermark,
                    late_rows,
                    true,
                    current_ready_start,
                )
                .await?;
                continue;
            }
            result = source.read() => match result {
                Ok(read) => read,
                Err(Error::EOF) => {
                    let (
                        ready,
                        ready_acks,
                        ready_invalid_timestamps,
                        dropped_acks,
                        watermark,
                        late_rows,
                        current_ready_start,
                    ) = {
                        let mut guard = event_gate.lock().await;
                        match guard.as_mut() {
                            None => (Vec::new(), Vec::new(), Vec::new(), Vec::new(), None, 0, None),
                            Some(gate) => {
                                let decision = gate.finish().await?;
                                (
                                    decision.ready,
                                    decision.ready_acks,
                                    decision.ready_invalid_timestamps,
                                    decision.dropped_acks,
                                    decision.watermark_ms,
                                    decision.late_event_rows,
                                    decision.current_ready_start,
                                )
                            }
                        }
                    };
                    dispatch_gate_outputs(
                        chain,
                        hook,
                        ready,
                        ready_acks,
                        ready_invalid_timestamps,
                        dropped_acks,
                        watermark,
                        late_rows,
                        true,
                        current_ready_start,
                    )
                    .await?;
                    // Preserve the explicit EOS contract for downstream
                    // vertices. Channel closure remains the fallback for
                    // consumers that only use bounded-source completion.
                    send_downstream(chain, Envelope::Eos).await?;
                    return Ok(());
                }
                // Inputs must classify a retryable receive loss as
                // `Disconnection`.  A plain `Connection` is also used for
                // permanent setup/state errors (for example an input that is
                // not connected), so retrying every Connection forever would
                // hide a broken Job and wedge shutdown.  Kafka and the other
                // reconnecting connectors normalize transient receive errors
                // to Disconnection before reaching this loop.
                Err(Error::Disconnection) => {
                    if let Some(metrics) = &hook.metrics {
                        metrics
                            .input_errors
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    // Reconnect with the legacy backoff cadence.
                    loop {
                        let reconnect = tokio::select! {
                            _ = cancellation.cancelled() => {
                                return shutdown_source_chain(chain, hook).await;
                            }
                            reconnect = source.connect() => reconnect,
                        };
                        match reconnect {
                            Ok(()) => {
                                if let Some(metrics) = &hook.metrics {
                                    metrics.input_reconnects.fetch_add(
                                        1,
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                }
                                break;
                            }
                            Err(error) => {
                                if let Some(metrics) = &hook.metrics {
                                    metrics.input_errors.fetch_add(
                                        1,
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                }
                                tracing::warn!(%error, "source reconnect failed");
                                tokio::select! {
                                    _ = cancellation.cancelled() => {
                                        return shutdown_source_chain(chain, hook).await;
                                    }
                                    _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                                }
                            }
                        }
                    }
                    continue;
                }
                Err(error) => {
                    if let Some(metrics) = &hook.metrics {
                        metrics
                            .input_errors
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    return Err(error);
                }
            },
        };
        let (batch, source_ack) = read;
        let event_time_enabled = event_gate.lock().await.is_some();
        if event_time_enabled {
            // A subscription-mode connector may only expose its complete
            // physical assignment after the first receive. Seed it before
            // observing this batch, otherwise a fast partition can advance a
            // tracker that has not yet included an idle sibling.
            if let Err(error) = seed_event_time_partitions(source, &event_gate, hook).await {
                let _ = source_ack.abort().await;
                return Err(error);
            }
        }
        if let Some(metrics) = &hook.metrics {
            use std::sync::atomic::Ordering;
            metrics.input_batches.fetch_add(1, Ordering::Relaxed);
            metrics
                .input_messages
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
        }
        let ack: Arc<dyn crate::input::Ack> =
            Arc::new(super::commit::TrackingAck::new(tracker.clone(), source_ack));
        match event_time_enabled {
            false => {
                dispatch_data(chain, batch, ack, hook.metrics.as_ref()).await?;
            }
            true => {
                let partitions =
                    match super::event_time_gate::split_by_physical_partition_for_source(
                        &batch,
                        hook.partition.unwrap_or(0),
                        Some(chain.entry_task_id()),
                    ) {
                        Ok(partitions) => partitions,
                        Err(error) => {
                            let _ = ack.abort().await;
                            return Err(error);
                        }
                    };
                let parent_ack = ack.clone();
                if partitions.is_empty() {
                    // Only an empty batch produces no partition slices: there
                    // is nothing to observe and nothing to lose, so settle the
                    // delivery. Leaving the tracking ack unsettled would park
                    // every later checkpoint drain at its timeout.
                    ack.ack().await?;
                    continue;
                }
                let child_acks = crate::input::fanout_ack(ack, partitions.len());
                let (
                    ready,
                    ready_acks,
                    ready_invalid_timestamps,
                    dropped_acks,
                    watermark,
                    late_rows,
                    current_ready_start,
                ) = {
                    let mut guard = event_gate.lock().await;
                    let gate = guard
                        .as_mut()
                        .expect("event-time gate disappeared while processing a source batch");
                    let decision_result = gate.observe_physical_partitioned_with_ack(
                        partitions
                            .into_iter()
                            .zip(child_acks)
                            .map(|((partition, slice), slice_ack)| (partition, slice, slice_ack))
                            .collect(),
                    );
                    drop(guard);
                    let decision = match decision_result {
                        Ok(decision) => decision,
                        Err(error) => {
                            let _ = abort_event_time_held(hook).await;
                            let _ = parent_ack.abort().await;
                            return Err(error);
                        }
                    };
                    (
                        decision.ready,
                        decision.ready_acks,
                        decision.ready_invalid_timestamps,
                        decision.dropped_acks,
                        decision.watermark_ms,
                        decision.late_event_rows,
                        decision.current_ready_start,
                    )
                };
                dispatch_gate_outputs(
                    chain,
                    hook,
                    ready,
                    ready_acks,
                    ready_invalid_timestamps,
                    dropped_acks,
                    watermark,
                    late_rows,
                    true,
                    current_ready_start,
                )
                .await?;
            }
        }
    }
}

async fn seed_event_time_partitions(
    source: &Arc<dyn Input>,
    event_gate: &Arc<tokio::sync::Mutex<Option<super::event_time_gate::EventTimeGate>>>,
    hook: &CheckpointHook,
) -> Result<(), Error> {
    let source_id = hook.task_id.as_deref().unwrap_or("source");
    let mut assigned = source
        .watermark_partitions()
        .await?
        .into_iter()
        .map(|partition| partition.with_source_identity(source_id))
        .collect::<Vec<_>>();
    if assigned.is_empty() {
        if let Some(partition) = hook.partition {
            assigned.push(crate::event_time::EventTimePartition::for_source(
                source_id, partition,
            ));
        }
    }
    if !assigned.is_empty() {
        let mut guard = event_gate.lock().await;
        if let Some(gate) = guard.as_mut() {
            gate.seed_partitions(&assigned);
        }
    }
    Ok(())
}

/// Dispatch one event-time gate decision without leaking acknowledgements when
/// a later ready slice or a dropped slice fails. A gate can split one source
/// delivery into several ready/held/dropped outcomes; returning on the first
/// error must still settle every outcome that will no longer be dispatched.
#[allow(clippy::too_many_arguments)]
async fn dispatch_gate_outputs(
    chain: &Chain,
    hook: &CheckpointHook,
    ready: Vec<(crate::MessageBatchRef, crate::event_time::WindowAction)>,
    ready_acks: Vec<Arc<dyn Ack>>,
    ready_invalid_timestamps: Vec<bool>,
    dropped_acks: Vec<Arc<dyn Ack>>,
    watermark: Option<i64>,
    late_rows: u64,
    forward_watermark: bool,
    current_ready_start: Option<usize>,
) -> Result<(), Error> {
    record_watermark_lag(hook, watermark);
    record_late_event_rows(hook, late_rows);

    if ready.len() != ready_acks.len() || ready.len() != ready_invalid_timestamps.len() {
        let error = Error::Process(format!(
            "event-time gate returned mismatched output vectors: ready={}, acks={}, invalid={}",
            ready.len(),
            ready_acks.len(),
            ready_invalid_timestamps.len()
        ));
        let mut acknowledgements = ready_acks;
        acknowledgements.extend(dropped_acks);
        let _ = abort_event_time_held(hook).await;
        return Err(error_after_ack_abort(error, acknowledgements).await);
    }

    let dynamic_session = chain
        .window_timings
        .iter()
        .any(|timing| matches!(timing, super::event_time_gate::WindowTiming::Session { .. }));
    let current_ready_start = dynamic_session.then_some(current_ready_start).flatten();
    let mut watermark_forwarded = false;
    let mut ready = ready
        .into_iter()
        .zip(ready_acks)
        .zip(ready_invalid_timestamps);
    let mut published = Vec::new();
    let mut ready_index = 0;
    while let Some((((batch, action), ack), invalid_timestamp)) = ready.next() {
        let current_ack = ack.clone();
        if current_ready_start == Some(ready_index) {
            if let Some(watermark) = watermark {
                if let Err(error) = send_downstream(chain, Envelope::Watermark(watermark)).await {
                    let mut acknowledgements = std::mem::take(&mut published);
                    acknowledgements.push(current_ack);
                    acknowledgements.extend(ready.map(|((_, ack), _)| ack));
                    acknowledgements.extend(dropped_acks);
                    let _ = abort_event_time_held(hook).await;
                    return Err(error_after_ack_abort(error, acknowledgements).await);
                }
                watermark_forwarded = true;
            }
        }
        if let Err(error) = dispatch_gated(chain, hook, batch, action, ack, invalid_timestamp).await
        {
            // `dispatch_gated` owns the current acknowledgement on every
            // failure path. Compensate already-published outcomes as well as
            // aborting outcomes that have not reached a downstream consumer;
            // an earlier invalid/drop outcome may already have completed the
            // fan-out parent before a later slice failed.
            let mut acknowledgements = std::mem::take(&mut published);
            acknowledgements.extend(ready.map(|((_, ack), _)| ack));
            acknowledgements.extend(dropped_acks);
            let _ = abort_event_time_held(hook).await;
            return Err(error_after_ack_abort(error, acknowledgements).await);
        }
        published.push(current_ack);
        ready_index += 1;
    }

    // A current source batch may contain only Drop/Route outcomes. There is no
    // ready slice at which to insert the control envelope, but a dynamic
    // Session still needs its latest watermark before the next data delivery.
    if current_ready_start == Some(ready_index) {
        if let Some(watermark) = watermark {
            if let Err(error) = send_downstream(chain, Envelope::Watermark(watermark)).await {
                let mut acknowledgements = std::mem::take(&mut published);
                acknowledgements.extend(dropped_acks);
                let _ = abort_event_time_held(hook).await;
                return Err(error_after_ack_abort(error, acknowledgements).await);
            }
            watermark_forwarded = true;
        }
    }

    let mut dropped = dropped_acks.into_iter();
    let mut dropped_published = Vec::new();
    while let Some(ack) = dropped.next() {
        let current_ack = ack.clone();
        if let Err(error) = ack.ack().await {
            let mut acknowledgements = std::mem::take(&mut published);
            acknowledgements.extend(std::mem::take(&mut dropped_published));
            acknowledgements.push(current_ack);
            acknowledgements.extend(dropped);
            let _ = abort_event_time_held(hook).await;
            return Err(error_after_ack_abort(error, acknowledgements).await);
        }
        dropped_published.push(current_ack);
    }

    if forward_watermark && !watermark_forwarded {
        if let Some(watermark) = watermark {
            if let Err(error) = send_downstream(chain, Envelope::Watermark(watermark)).await {
                let mut acknowledgements = std::mem::take(&mut published);
                acknowledgements.extend(dropped_published);
                let _ = abort_event_time_held(hook).await;
                return Err(error_after_ack_abort(error, acknowledgements).await);
            }
        }
    }
    Ok(())
}

/// Abort deliveries that are still retained by the source event-time gate
/// after a refresh or downstream dispatch failure.  The gate lock is released
/// before awaiting acknowledgement futures so cleanup cannot deadlock with a
/// connector or operator that needs another runtime lock.
async fn abort_event_time_held(hook: &CheckpointHook) -> Result<(), Error> {
    let held = {
        let mut guard = hook.event_time_gate.lock().await;
        guard
            .as_mut()
            .map(|gate| gate.take_held_acknowledgements())
            .unwrap_or_default()
    };
    let mut first_error = None;
    for ack in held.into_iter().rev() {
        if let Err(error) = ack.abort().await {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn shutdown_source_chain(chain: &Chain, hook: &CheckpointHook) -> Result<(), Error> {
    let (ready, ready_acks, ready_invalid_timestamps, dropped_acks, late_rows, current_ready_start) = {
        let mut guard = hook.event_time_gate.lock().await;
        match guard.as_mut() {
            Some(gate) => match gate.finish().await {
                Ok(decision) => (
                    decision.ready,
                    decision.ready_acks,
                    decision.ready_invalid_timestamps,
                    decision.dropped_acks,
                    decision.late_event_rows,
                    decision.current_ready_start,
                ),
                Err(error) => {
                    tracing::debug!(%error, task = chain.entry_task_id(), "failed to flush source event-time gate during cancellation");
                    (Vec::new(), Vec::new(), Vec::new(), Vec::new(), 0, None)
                }
            },
            None => (Vec::new(), Vec::new(), Vec::new(), Vec::new(), 0, None),
        }
    };
    if let Err(error) = dispatch_gate_outputs(
        chain,
        hook,
        ready,
        ready_acks,
        ready_invalid_timestamps,
        dropped_acks,
        None,
        late_rows,
        false,
        current_ready_start,
    )
    .await
    {
        tracing::debug!(%error, task = chain.entry_task_id(), "discarding source output during cancellation");
    }
    if let Err(error) = send_downstream(chain, Envelope::Eos).await {
        tracing::debug!(%error, task = chain.entry_task_id(), "downstream already closed during source cancellation");
    }
    Ok(())
}

async fn shutdown_interior_chain(
    chain: &Chain,
    pool: Option<ProcessorWorkerPool>,
) -> Result<(), Error> {
    // Stop and join every worker before flushing or closing the processors.
    // A detached worker must never publish into a chain whose processors are
    // already being torn down.
    if let Some(pool) = pool {
        if let Err(error) = pool.cancel_and_join().await {
            tracing::debug!(
                %error,
                task = chain.entry_task_id(),
                "processor worker failed during cancellation"
            );
        }
    }
    // Do not acknowledge discarded data. Abort each owned acknowledgement so
    // staged state/journal entries are released; durable source/WAL cursors
    // remain unadvanced and can replay the data after a cancelled run.
    for receiver in &chain.inputs {
        while let Ok(envelope) = receiver.recv_async().await {
            if let Envelope::Data(_, ack) = envelope {
                if let Err(error) = ack.abort().await {
                    tracing::debug!(
                        %error,
                        task = chain.entry_task_id(),
                        "failed to abort queued data during cancellation"
                    );
                }
            }
        }
    }
    if let Err(error) = finish_chain(chain).await {
        tracing::debug!(%error, task = chain.entry_task_id(), "failed to flush chain during cancellation");
        let _ = send_downstream(chain, Envelope::Eos).await;
    }
    Ok(())
}

fn record_watermark_lag(hook: &CheckpointHook, watermark: Option<i64>) {
    let Some(watermark) = watermark else { return };
    let now = crate::state::now_ms() as i64;
    let lag = now.saturating_sub(watermark).max(0);
    if let Some(metrics) = &hook.metrics {
        metrics
            .kernel
            .watermark_lag_ms
            .fetch_max(lag, std::sync::atomic::Ordering::Relaxed);
    }
}

fn record_late_event_rows(hook: &CheckpointHook, rows: u64) {
    if rows == 0 {
        return;
    }
    if let Some(metrics) = &hook.metrics {
        metrics
            .kernel
            .late_events
            .fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Dispatch one gated batch slice according to its window action. Route and
/// Update forward with their marker columns attached (legacy semantics);
/// Emit/Hold-equivalent rows flow through the chain normally.
async fn dispatch_gated(
    chain: &Chain,
    hook: &CheckpointHook,
    batch: crate::MessageBatchRef,
    action: crate::event_time::WindowAction,
    ack: Arc<dyn crate::input::Ack>,
    invalid_timestamp: bool,
) -> Result<(), Error> {
    if action == crate::event_time::WindowAction::Route {
        let last_task = chain.task_ids.last().map(String::as_str).unwrap_or("");
        if let Some(targets) = chain.late_event_outputs.get(last_task) {
            let batch = if invalid_timestamp {
                match mark_event_batch(batch, "__arkflow_invalid_timestamp_route") {
                    Ok(batch) => batch,
                    Err(error) => return Err(error_after_ack_abort(error, vec![ack]).await),
                }
            } else {
                batch
            };
            let batch = match mark_event_batch(batch, "__arkflow_late_event_route") {
                Ok(batch) => batch,
                Err(error) => return Err(error_after_ack_abort(error, vec![ack]).await),
            };
            return send_to_targets(Some(targets), Envelope::Data(batch, ack), true).await;
        }
        // An invalid timestamp cannot be admitted to a window. If the route
        // target is absent, acknowledge and drop it rather than sending an
        // impossible update through the main path.
        if invalid_timestamp {
            return match ack.ack().await {
                Ok(()) => Ok(()),
                Err(error) => Err(error_after_ack_abort(error, vec![ack]).await),
            };
        }
        // A valid late row without a declared route target remains on the
        // main path as an Update for backwards compatibility.
        let batch = match mark_event_batch(batch, "__arkflow_late_event_update") {
            Ok(batch) => batch,
            Err(error) => return Err(error_after_ack_abort(error, vec![ack]).await),
        };
        return dispatch_data(chain, batch, ack, hook.metrics.as_ref()).await;
    }
    let batch = match action {
        crate::event_time::WindowAction::Update => {
            match mark_event_batch(batch, "__arkflow_late_event_update") {
                Ok(batch) => batch,
                Err(error) => return Err(error_after_ack_abort(error, vec![ack]).await),
            }
        }
        _ => batch,
    };
    let ack_for_error = ack.clone();
    match dispatch_data(chain, batch, ack, hook.metrics.as_ref()).await {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = ack_for_error.abort().await;
            Err(error)
        }
    }
}

/// Attach a boolean marker column to a batch (late-event routing metadata).
fn mark_event_batch(
    batch: crate::MessageBatchRef,
    marker: &str,
) -> Result<crate::MessageBatchRef, Error> {
    use datafusion::arrow::array::{ArrayRef, BooleanArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    if batch.record_batch().column_by_name(marker).is_some() {
        return Ok(batch);
    }
    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    let mut columns = batch.columns().to_vec();
    fields.push(Arc::new(Field::new(marker, DataType::Boolean, false)));
    columns.push(Arc::new(BooleanArray::from(vec![true; batch.len()])) as ArrayRef);
    let marked = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| Error::Process(format!("mark late event batch: {error}")))?;
    let mut marked = crate::MessageBatch::new_arrow(marked);
    marked.set_input_name(batch.get_input_name());
    Ok(Arc::new(marked))
}

/// Interior/sink chain: consume inbound channels. Barriers align across the
/// chain's inputs (`Aligner`), snapshot the chain's state, and flow onward.
async fn run_interior_chain(
    chain: &Chain,
    hook: &CheckpointHook,
    cancellation: &CancellationToken,
) -> Result<(), Error> {
    let mut pool = ProcessorWorkerPool::start(chain, hook, cancellation);
    let result = run_interior_chain_loop(chain, hook, cancellation, &mut pool).await;
    if let Err(error) = result {
        // Every error path, including a timer, receiver, barrier, or pool
        // submission failure, must stop the workers before the caller closes
        // the chain's processors. The loop consumes the pool only on normal
        // EOS/cancellation paths, so it remains available here for cleanup.
        if let Some(pool) = pool.take() {
            if let Err(cleanup_error) = pool.cancel_and_join().await {
                tracing::debug!(
                    %cleanup_error,
                    task = chain.entry_task_id(),
                    "processor worker cleanup failed after chain error"
                );
            }
        }
        Err(error)
    } else {
        result
    }
}

async fn run_interior_chain_loop(
    chain: &Chain,
    hook: &CheckpointHook,
    cancellation: &CancellationToken,
    pool: &mut Option<ProcessorWorkerPool>,
) -> Result<(), Error> {
    let mut readers = FuturesUnordered::new();
    for (index, receiver) in chain.inputs.iter().enumerate() {
        readers.push(recv_envelope(index, receiver.clone()));
    }
    // Alignment buffering shares one bound across the vertex's inputs. Remote
    // inputs add an RTT-sized in-flight window each, so scale the bound with
    // the input count instead of letting a wide fan-in starve each edge's
    // share and trip checkpoint rounds on slow networks.
    let alignment_bound = 1024usize.max(512 * chain.inputs.len());
    let mut aligner = super::barrier::Aligner::new(chain.inputs.len(), alignment_bound);
    let mut ended_inputs = BTreeSet::new();
    // Watermarks arriving on different input edges represent independent
    // progress. Keep one value per edge and only forward the minimum once
    // every active input has reported; using the latest/max value would let a
    // fast partition close a window while a lagging partition can still
    // deliver data for it.
    // (watermark, last arrival) per input edge; the arrival instant drives
    // idle exclusion in `effective_watermark`.
    let mut upstream_watermarks = BTreeMap::<usize, (i64, std::time::Instant)>::new();
    let chain_started = std::time::Instant::now();
    // Last watermark forwarded downstream; clamps re-joining inputs so the
    // downstream frontier stays monotonic.
    let mut last_forwarded_watermark: Option<i64> = None;
    let mut idle_tick = tokio::time::interval(std::time::Duration::from_millis(100));
    idle_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Bounded, ordered processor worker pool (`pipeline.thread_num`): data
    // deliveries fan out to N workers while the chain's control flow
    // (barriers, watermarks, EOS, ticks) stays in this loop. A reorder
    // collector preserves per-delivery output order.
    // The window operator counts its own session-late/invalid rows (the
    // source gate cannot classify them); surface the counter's delta into
    // the kernel `late_events` metric every iteration (best-effort).
    let window_late_rows = chain.window_late_event_rows.clone();
    let mut surfaced_late_rows = window_late_rows.as_ref().map_or(0, |counter| {
        counter.load(std::sync::atomic::Ordering::Relaxed)
    });
    loop {
        if let Some(counter) = &window_late_rows {
            let current = counter.load(std::sync::atomic::Ordering::Relaxed);
            let delta = current.saturating_sub(surfaced_late_rows);
            if delta > 0 {
                surfaced_late_rows = current;
                record_late_event_rows(hook, delta);
            }
        }
        let read = tokio::select! {
            _ = cancellation.cancelled() => {
                // Upstream chains also observe cancellation and close their
                // senders. Drain all queued envelopes before forwarding EOS so
                // no receiver task is left behind and bounded producers can
                // finish their shutdown path.
                let pool = pool.take();
                return shutdown_interior_chain(chain, pool).await;
            }
            _ = idle_tick.tick() => {
                // The tick is a control event that can generate data; fence
                // the pool first so tick output cannot overtake deliveries
                // the workers are still publishing (same discipline as the
                // barrier and watermark paths). A cancellation that races
                // the fence must surface as the clean shutdown path, not as
                // a chain failure (a SIGTERM would otherwise be recorded as
                // StreamState::Failed depending on tick timing).
                if let Some(pool_ref) = pool.as_ref() {
                    if let Err(error) = pool_ref.flush().await {
                        if cancellation.is_cancelled() {
                            let pool = pool.take();
                            return shutdown_interior_chain(chain, pool).await;
                        }
                        return Err(error);
                    }
                }
                tick_chain(chain).await?;
                continue;
            }
            failure = async {
                match pool.as_ref() {
                    Some(pool) => pool.fail().await,
                    None => std::future::pending().await,
                }
            } => {
                // A pool that exits because the chain is shutting down is not
                // a failure: `cancel_and_join` cancels the pool, its workers
                // and collector drop the failure channel, and the disconnect
                // must not be reported as a fault (the same judgement the tick
                // arm above makes). Only a disconnect while the chain is still
                // running is the silent-death case.
                if cancellation.is_cancelled() {
                    let pool = pool.take();
                    return shutdown_interior_chain(chain, pool).await;
                }
                if let Some(pool) = pool.take() {
                    let _ = pool.cancel_and_join().await;
                }
                return Err(failure);
            }
            read = readers.next() => read,
        };
        let Some((index, read)) = read else {
            // All input channels closed: end of stream. Drain the worker
            // pool first so its outputs precede the terminal flush.
            if let Some(pool) = pool.take() {
                pool.drain().await?;
            }
            finish_chain(chain).await?;
            return Ok(());
        };
        let envelope = match read {
            Ok(envelope) => envelope,
            Err(Error::InputChannelClosed) => {
                // Producer dropped its sender: end-of-stream for this channel.
                // Treat closure as an explicit EOS so processors flush and
                // downstream sinks observe the same terminal ordering as a
                // source that returned Envelope::Eos. Keep polling the
                // remaining channels for a multi-input vertex.
                match aligner.observe(index, Envelope::Eos) {
                    Ok(Some(barrier)) => {
                        if handle_completed_barrier(
                            chain,
                            hook,
                            barrier,
                            &mut aligner,
                            &mut ended_inputs,
                            pool,
                            &mut upstream_watermarks,
                            chain_started,
                            &mut last_forwarded_watermark,
                        )
                        .await?
                        {
                            return Ok(());
                        }
                    }
                    Ok(None) => {
                        if let Some(envelope) = aligner.take_passthrough() {
                            if handle_envelope(
                                chain,
                                hook,
                                index,
                                envelope,
                                &mut ended_inputs,
                                pool.as_ref(),
                                &mut upstream_watermarks,
                                chain_started,
                                &mut last_forwarded_watermark,
                            )
                            .await?
                            {
                                if let Some(pool) = pool.take() {
                                    pool.drain().await?;
                                }
                                finish_chain(chain).await?;
                                return Ok(());
                            }
                        }
                    }
                    Err(error) => return Err(error),
                }
                continue;
            }
            Err(error) => return Err(error),
        };
        // Barrier alignment: hold data back until every input delivered the
        // current barrier. Single-input chains pass straight through because
        // the aligner completes immediately; multi-input chains buffer other
        // inputs' data until the last barrier arrives.
        match aligner.observe(index, envelope) {
            Ok(Some(barrier)) => {
                if handle_completed_barrier(
                    chain,
                    hook,
                    barrier,
                    &mut aligner,
                    &mut ended_inputs,
                    pool,
                    &mut upstream_watermarks,
                    chain_started,
                    &mut last_forwarded_watermark,
                )
                .await?
                {
                    return Ok(());
                }
            }
            Ok(None) => {
                if aligner.is_aligning() {
                    // Envelope retained inside the aligner; replay happens at
                    // alignment completion above.
                } else if let Some(envelope) = aligner.take_passthrough() {
                    if handle_envelope(
                        chain,
                        hook,
                        index,
                        envelope,
                        &mut ended_inputs,
                        pool.as_ref(),
                        &mut upstream_watermarks,
                        chain_started,
                        &mut last_forwarded_watermark,
                    )
                    .await?
                    {
                        if let Some(pool) = pool.take() {
                            pool.drain().await?;
                        }
                        finish_chain(chain).await?;
                        return Ok(());
                    }
                }
            }
            Err(error) => {
                // Alignment overflow: release what was buffered and keep the
                // data flowing; the barrier round fails upstream.
                if let Some(reporter) = &hook.failure_reporter {
                    let _ = reporter.send(Error::Process(error.to_string()));
                }
                if let Some(pool) = pool.as_ref() {
                    pool.flush().await?;
                }
                for (buffered_index, buffered) in aligner.release() {
                    if handle_envelope(
                        chain,
                        hook,
                        buffered_index,
                        buffered,
                        &mut ended_inputs,
                        pool.as_ref(),
                        &mut upstream_watermarks,
                        chain_started,
                        &mut last_forwarded_watermark,
                    )
                    .await?
                    {
                        if let Some(pool) = pool.take() {
                            pool.drain().await?;
                        }
                        finish_chain(chain).await?;
                        return Ok(());
                    }
                }
                tracing::warn!(%error, "barrier alignment overflowed");
            }
        }
        // Restart the reader for the channel just drained so the channel
        // stays continuously polled.
        if !ended_inputs.contains(&index) {
            if let Some(receiver) = chain.inputs.get(index) {
                readers.push(recv_envelope(index, receiver.clone()));
            }
        }
    }
}

/// Extract a printable message from a caught panic payload.
pub(crate) fn panic_payload(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// Finish one aligned barrier before releasing post-barrier envelopes. EOS is
/// allowed to be part of the released buffer when a bounded input ended while
/// another input was still aligning the barrier.
/// Build the `chain.barrier` span for one aligned barrier. When the barrier
/// carries a remote trace context (cross-node propagation), the span parents
/// to it so a trace continues across the hop instead of re-rooting per node.
fn barrier_span(chain: &Chain, barrier: &crate::checkpoint::CheckpointBarrier) -> tracing::Span {
    let span = tracing::info_span!(
        "chain.barrier",
        task = chain.entry_task_id(),
        checkpoint_id = %barrier.checkpoint_id,
        generation = barrier.generation,
    );
    if let Some(trace_context) = &barrier.trace_context {
        if let Some(remote_context) = super::remote::extract_trace_context(trace_context) {
            use tracing_opentelemetry::OpenTelemetrySpanExt as _;
            // Telemetry-only: a failed parent link never affects the barrier.
            if let Err(e) = span.set_parent(remote_context) {
                tracing::debug!("barrier span remote parent not applied: {e}");
            }
        }
    }
    span
}

#[allow(clippy::too_many_arguments)]
async fn handle_completed_barrier(
    chain: &Chain,
    hook: &CheckpointHook,
    barrier: crate::checkpoint::CheckpointBarrier,
    aligner: &mut super::barrier::Aligner,
    ended_inputs: &mut BTreeSet<usize>,
    pool: &mut Option<ProcessorWorkerPool>,
    upstream_watermarks: &mut BTreeMap<usize, (i64, std::time::Instant)>,
    chain_started: std::time::Instant,
    last_forwarded_watermark: &mut Option<i64>,
) -> Result<bool, Error> {
    let span = barrier_span(chain, &barrier);
    return async move {
        handle_completed_barrier_inner(
            chain,
            hook,
            barrier,
            aligner,
            ended_inputs,
            pool,
            upstream_watermarks,
            chain_started,
            last_forwarded_watermark,
        )
        .await
    }
    .instrument(span)
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn handle_completed_barrier_inner(
    chain: &Chain,
    hook: &CheckpointHook,
    barrier: crate::checkpoint::CheckpointBarrier,
    aligner: &mut super::barrier::Aligner,
    ended_inputs: &mut BTreeSet<usize>,
    pool: &mut Option<ProcessorWorkerPool>,
    upstream_watermarks: &mut BTreeMap<usize, (i64, std::time::Instant)>,
    chain_started: std::time::Instant,
    last_forwarded_watermark: &mut Option<i64>,
) -> Result<bool, Error> {
    // A barrier is a control fence for the data submitted before it. Do not
    // snapshot or forward it while a worker can still publish pre-barrier
    // output.
    if let Some(pool) = pool.as_ref() {
        pool.flush().await?;
    }
    // Capture the committed state epoch BEFORE releasing buffered post-barrier
    // data. The snapshot therefore cannot observe a mutation from after this
    // barrier's cut.
    let state = match hook.state.clone() {
        Some(backend) => super::barrier::snapshot_state(backend).await,
        None => Ok(crate::state::StateSnapshot::new(1, Vec::new())),
    };
    match state {
        Ok(state) => {
            send_downstream(chain, Envelope::Barrier(barrier.clone())).await?;
            if let Some(reporter) = &hook.reporter {
                let task_id = hook
                    .task_id
                    .clone()
                    .unwrap_or_else(|| chain.entry_task_id().to_owned());
                let _ = reporter.send(super::barrier::ChainSnapshot {
                    task_id: task_id.clone(),
                    attempt_id: format!("{task_id}-attempt"),
                    partition: hook.partition.unwrap_or_default(),
                    barrier: barrier.clone(),
                    cut_generation: barrier.generation,
                    state,
                    source_positions: Vec::new(),
                    watermark_ms: None,
                    watermark_partitions: Vec::new(),
                });
            }
        }
        Err(error) => {
            if let Some(reporter) = &hook.failure_reporter {
                let _ = reporter.send(error);
            }
            send_downstream(chain, Envelope::Barrier(barrier.clone())).await?;
        }
    }
    for (buffered_index, buffered) in aligner.release() {
        if handle_envelope(
            chain,
            hook,
            buffered_index,
            buffered,
            ended_inputs,
            pool.as_ref(),
            upstream_watermarks,
            chain_started,
            last_forwarded_watermark,
        )
        .await?
        {
            if let Some(pool) = pool.take() {
                pool.drain().await?;
            }
            finish_chain(chain).await?;
            return Ok(true);
        }
    }
    Ok(false)
}

/// Append the join input-side tag column (`__meta_input_index`) to a batch.
fn tag_input_index(
    batch: datafusion::arrow::record_batch::RecordBatch,
    input_index: u32,
) -> Result<datafusion::arrow::record_batch::RecordBatch, Error> {
    use datafusion::arrow::array::UInt32Array;
    let schema = batch.schema();
    if schema
        .index_of(crate::executor::join::META_INPUT_INDEX)
        .is_ok()
    {
        return Ok(batch);
    }
    let mut fields: Vec<datafusion::arrow::datatypes::Field> =
        schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(datafusion::arrow::datatypes::Field::new(
        crate::executor::join::META_INPUT_INDEX,
        datafusion::arrow::datatypes::DataType::UInt32,
        false,
    ));
    let mut columns = batch.columns().to_vec();
    columns.push(std::sync::Arc::new(UInt32Array::from(vec![
        input_index;
        batch.num_rows()
    ])));
    datafusion::arrow::record_batch::RecordBatch::try_new(
        std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new(fields)),
        columns,
    )
    .map_err(|error| Error::Process(format!("failed to tag join input index: {error}")))
}

/// How long an input may go without emitting a watermark before it is
/// excluded from the multi-input watermark frontier. Generous by design:
/// normal watermark cadences are seconds, so only genuinely silent sources
/// (e.g. processing-time inputs mixed into an event-time fan-in) hit it.
/// Excluding them keeps watermark-driven progress — gate releases, window
/// firing, state cleanup — from freezing forever; their late rows follow the
/// configured late policy when they resume.
const WATERMARK_INPUT_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5 * 60);

/// The downstream watermark frontier never moves backwards: a re-joining
/// idle input can carry a stale value, and downstream consumers (window
/// firing, state cleanup) assume monotonicity.
fn clamp_forwarded_watermark(previous: Option<i64>, candidate: i64) -> i64 {
    previous.map_or(candidate, |last| last.max(candidate))
}

/// The effective watermark across a vertex's inputs: the minimum over every
/// active input that reported recently, or `None` while some recently-active
/// input has never reported. Inputs idle longer than `idle_timeout` (never
/// reported counts from `started`) and ended inputs are excluded, mirroring
/// the upstream contract but bounded in time.
fn effective_watermark(
    watermarks: &BTreeMap<usize, (i64, std::time::Instant)>,
    ended_inputs: &BTreeSet<usize>,
    input_count: usize,
    now: std::time::Instant,
    started: std::time::Instant,
    idle_timeout: std::time::Duration,
) -> Option<i64> {
    let mut minimum = None;
    for index in 0..input_count {
        if ended_inputs.contains(&index) {
            continue;
        }
        let last_arrival = watermarks
            .get(&index)
            .map(|(_, arrived)| *arrived)
            .unwrap_or(started);
        if now.duration_since(last_arrival) > idle_timeout {
            continue; // idle: outside both the gate and the minimum
        }
        let Some((watermark, _)) = watermarks.get(&index) else {
            return None; // recently active but has not reported yet
        };
        let watermark = *watermark;
        minimum = Some(minimum.map_or(watermark, |current: i64| current.min(watermark)));
    }
    minimum
}

#[allow(clippy::too_many_arguments)]
async fn handle_envelope(
    chain: &Chain,
    hook: &CheckpointHook,
    input_index: usize,
    envelope: Envelope,
    ended_inputs: &mut BTreeSet<usize>,
    pool: Option<&ProcessorWorkerPool>,
    upstream_watermarks: &mut BTreeMap<usize, (i64, std::time::Instant)>,
    chain_started: std::time::Instant,
    last_forwarded_watermark: &mut Option<i64>,
) -> Result<bool, Error> {
    match envelope {
        Envelope::Data(batch, ack) => {
            // Join chains consume multiple inbound edges through one
            // processor; tag the batch with its input index (0 = left,
            // 1 = right) so the join operator can tell the sides apart.
            let batch = if chain.tags_input_index {
                std::sync::Arc::new(crate::MessageBatch::new_arrow(tag_input_index(
                    batch.record_batch().clone(),
                    input_index as u32,
                )?))
            } else {
                batch
            };
            if let Some(pool) = pool {
                // The pool owns this delivery's processing; ordering and
                // backpressure are the pool's contract.
                let abort_ack = ack.clone();
                if let Err(error) = pool.submit((batch, ack)).await {
                    // `send_async` consumes the delivery before reporting a
                    // closed queue.  The pool cannot process it after that
                    // point, so settle its source/journal acknowledgement
                    // explicitly instead of leaking a fan-out parent or a
                    // staged state transaction.
                    let _ = abort_ack.abort().await;
                    return Err(error);
                }
            } else {
                dispatch_data(chain, batch, ack, hook.metrics.as_ref()).await?;
            }
            Ok(false)
        }
        Envelope::Eos => {
            ended_inputs.insert(input_index);
            // An ended input can no longer contribute data behind its last
            // watermark. Remove it from the active watermark frontier so one
            // finite upstream does not pin a multi-input window forever after
            // the other upstreams continue to advance.
            upstream_watermarks.remove(&input_index);
            // The caller drains the worker pool and flushes the chain when
            // this returns true (ordered end of stream).
            Ok(ended_inputs.len() == chain.inputs.len())
        }
        Envelope::Barrier(_) => {
            // Barrier handling lands with the checkpoint task; forward
            // transparently so ordering is preserved. The span keeps a
            // passthrough barrier on the remote trace it arrived with.
            let span = match &envelope {
                Envelope::Barrier(barrier) => barrier_span(chain, barrier),
                _ => tracing::Span::none(),
            };
            async {
                if let Some(pool) = pool {
                    pool.flush().await?;
                }
                send_downstream(chain, envelope).await?;
                Ok(false)
            }
            .instrument(span)
            .await
        }
        Envelope::Watermark(watermark) => {
            upstream_watermarks.insert(input_index, (watermark, std::time::Instant::now()));
            // Every still-active, non-idle input must have reported before
            // the first aggregate fires; subsequent values use the slowest
            // such input's progress. Ended inputs were removed above, and an
            // input idle beyond WATERMARK_INPUT_IDLE_TIMEOUT is excluded so a
            // source that never emits watermarks cannot freeze
            // watermark-driven progress forever.
            let Some(candidate) = effective_watermark(
                upstream_watermarks,
                ended_inputs,
                chain.inputs.len(),
                std::time::Instant::now(),
                chain_started,
                WATERMARK_INPUT_IDLE_TIMEOUT,
            ) else {
                return Ok(false);
            };
            // A re-joining idle input can carry a stale value; never move
            // the downstream frontier backwards.
            let watermark = clamp_forwarded_watermark(*last_forwarded_watermark, candidate);
            *last_forwarded_watermark = Some(watermark);
            if let Some(pool) = pool {
                // Watermarks must not overtake data already accepted by the
                // worker pool.
                pool.flush().await?;
            }
            dispatch_watermark(chain, watermark).await?;
            send_downstream(chain, Envelope::Watermark(watermark)).await?;
            Ok(false)
        }
    }
}

/// One data delivery submitted to a worker pool.
type PoolDelivery = (crate::MessageBatchRef, Arc<dyn crate::input::Ack>);

/// Bounded, ordered, cancellable processor worker pool for one chain.
/// Present only when the chain's configured concurrency exceeds 1; the data
/// path otherwise stays on the chain's single event loop.
struct ProcessorWorkerPool {
    work: flume::Sender<(u64, PoolDelivery)>,
    pending_work: flume::Receiver<(u64, PoolDelivery)>,
    failure: flume::Receiver<Error>,
    submitted: std::sync::atomic::AtomicU64,
    flushed: Arc<std::sync::atomic::AtomicU64>,
    failed: Arc<std::sync::atomic::AtomicBool>,
    progress: Arc<tokio::sync::Notify>,
    cancellation: CancellationToken,
    /// Reorder collector: awaited on the chain's end-of-stream path so the
    /// terminal EOS never overtakes in-flight pool deliveries.
    collector: tokio::task::JoinHandle<()>,
    /// Worker handles are retained so cancellation and EOS can wait until all
    /// processor calls have stopped before the chain closes its components.
    workers: Vec<tokio::task::JoinHandle<()>>,
}

enum PoolResult {
    Outputs(Vec<ProcessedBatch>),
    ProcessorFailure(ProcessorFailure),
}

impl ProcessorWorkerPool {
    /// Start a pool for `chain` (no-op returning `None` when the configured
    /// concurrency is 1 or the chain has no processors). Workers are
    /// detached tasks bounded by the cancellation token; the reorder
    /// collector is joined on end-of-stream so ordered delivery finishes
    /// before the terminal control envelope flows downstream.
    fn start(
        chain: &Chain,
        hook: &CheckpointHook,
        cancellation: &CancellationToken,
    ) -> Option<Self> {
        if chain.processor_parallelism <= 1 || chain.processors.is_empty() {
            return None;
        }
        let parallelism = chain.processor_parallelism;
        let shared = Arc::new(chain.share_for_workers());
        let metrics = hook.metrics.clone();
        let (work_tx, work_rx) = flume::bounded::<(u64, PoolDelivery)>(64.min(parallelism * 8));
        // Bounded so a blocked reorder collector backpressures the workers
        // and, through the submit queue, the chain loop and the source. When
        // the collector exits (cancellation), dropping its receiver fails
        // every blocked send with `SendError`, which the workers handle by
        // aborting the delivery's acknowledgements.
        let (done_tx, done_rx) = flume::bounded::<(u64, PoolResult)>(parallelism * 2);
        let (fail_tx, fail_rx) = flume::bounded::<Error>(1);
        let error_targets = chain.error_outputs.clone();
        let flushed = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let progress = Arc::new(tokio::sync::Notify::new());
        let mut workers = Vec::with_capacity(parallelism);
        for _ in 0..parallelism {
            let shared = shared.clone();
            let metrics = metrics.clone();
            let work_rx = work_rx.clone();
            let done_tx = done_tx.clone();
            let fail_tx = fail_tx.clone();
            let failed = failed.clone();
            let progress = progress.clone();
            let cancellation = cancellation.clone();
            workers.push(tokio::spawn(async move {
                loop {
                    let submit = tokio::select! {
                        _ = cancellation.cancelled() => return,
                        submit = work_rx.recv_async() => match submit {
                            Ok(submit) => submit,
                            Err(_) => return,
                        },
                    };
                    let (sequence, (batch, ack)) = submit;
                    match process_chain(&shared, batch, ack, metrics.as_ref()).await {
                        Ok(outputs) => {
                            // Publish asynchronously: a blocking send here
                            // would park a tokio worker thread and, once the
                            // result channel fills, freeze the whole runtime
                            // (the collector shares the same worker pool).
                            if let Err(flume::SendError((_, PoolResult::Outputs(outputs)))) =
                                done_tx
                                    .send_async((sequence, PoolResult::Outputs(outputs)))
                                    .await
                            {
                                let _ = abort_acknowledgements(
                                    outputs.into_iter().map(|output| output.ack).collect(),
                                )
                                .await;
                                return;
                            }
                        }
                        Err(error) => match error {
                            ProcessChainError::Processor(failure) => {
                                // Keep the failed batch and its source
                                // acknowledgement in the ordered result
                                // stream. The collector routes it through
                                // the configured error edge at the same
                                // sequence position as ordinary output.
                                if let Err(flume::SendError((
                                    _,
                                    PoolResult::ProcessorFailure(failure),
                                ))) = done_tx
                                    .send_async((sequence, PoolResult::ProcessorFailure(failure)))
                                    .await
                                {
                                    let _ = abort_processor_failure(failure).await;
                                    return;
                                }
                            }
                            ProcessChainError::Fatal(failure) => {
                                // Keep draining so the bounded queue never
                                // deadlocks the chain loop; the chain
                                // observes the fatal failure through
                                // `fail` and cancels.
                                failed.store(true, std::sync::atomic::Ordering::Release);
                                progress.notify_waiters();
                                let _ = fail_tx.try_send(abort_fatal_failure(failure).await);
                            }
                        },
                    }
                }
            }));
        }
        // Reorder collector: outputs leave in submission order (the legacy
        // Stream contract requires ordered delivery).
        let collector_shared = shared.clone();
        let collector_cancellation = cancellation.clone();
        let collector_flushed = flushed.clone();
        let collector_failed = failed.clone();
        let collector_progress = progress.clone();
        let collector_fail_tx = fail_tx.clone();
        let collector = tokio::spawn(async move {
            let mut pending: BTreeMap<u64, PoolResult> = BTreeMap::new();
            let mut next_sequence = 0u64;
            loop {
                let done = tokio::select! {
                    _ = collector_cancellation.cancelled() => {
                        abort_pool_results(&mut pending).await;
                        return;
                    }
                    done = done_rx.recv_async() => match done {
                        Ok(done) => done,
                        Err(_) => {
                            // All workers exited: flush what is left.
                            while let Some(result) = pending.remove(&next_sequence) {
                                if let Err(error) = flush_pool_result(
                                    &collector_shared,
                                    result,
                                    &error_targets,
                                )
                                .await
                                {
                                    collector_failed.store(
                                        true,
                                        std::sync::atomic::Ordering::Release,
                                    );
                                    let _ = collector_fail_tx.try_send(error);
                                    abort_pool_results(&mut pending).await;
                                    collector_progress.notify_waiters();
                                    return;
                                }
                                next_sequence += 1;
                                collector_flushed
                                    .store(next_sequence, std::sync::atomic::Ordering::Release);
                                collector_progress.notify_waiters();
                            }
                            abort_pool_results(&mut pending).await;
                            return;
                        }
                    },
                };
                let (sequence, result) = done;
                pending.insert(sequence, result);
                while let Some(result) = pending.remove(&next_sequence) {
                    if let Err(error) =
                        flush_pool_result(&collector_shared, result, &error_targets).await
                    {
                        collector_failed.store(true, std::sync::atomic::Ordering::Release);
                        let _ = collector_fail_tx.try_send(error);
                        abort_pool_results(&mut pending).await;
                        collector_progress.notify_waiters();
                        return;
                    }
                    next_sequence += 1;
                    collector_flushed.store(next_sequence, std::sync::atomic::Ordering::Release);
                    collector_progress.notify_waiters();
                }
            }
        });
        Some(Self {
            work: work_tx,
            pending_work: work_rx.clone(),
            failure: fail_rx,
            submitted: std::sync::atomic::AtomicU64::new(0),
            flushed,
            failed,
            progress,
            cancellation: cancellation.clone(),
            collector,
            workers,
        })
    }

    /// Drain the pool: close the submission queue, let every worker finish
    /// its in-flight delivery, and wait until the reorder collector has
    /// forwarded all outputs. Called on the chain's end-of-stream path.
    async fn drain(self) -> Result<(), Error> {
        let ProcessorWorkerPool {
            work,
            pending_work,
            failure,
            failed,
            collector,
            workers,
            ..
        } = self;
        drop(work);
        let mut join_error = None;
        for worker in workers {
            if let Err(error) = worker.await {
                join_error.get_or_insert_with(|| {
                    Error::Process(format!("processor worker task failed: {error}"))
                });
            }
        }
        let pending_work = pending_work;
        while let Ok((_, (_, ack))) = pending_work.try_recv() {
            let _ = ack.abort().await;
        }
        // The collector's shutdown drain flushes downstream over an edge that
        // may itself be blocked (a full channel whose consumer already
        // stopped), which would park this join forever on the failure path
        // where the cancellation token is not set. Bound the join: exceeding
        // it fails the chain — the drain never completed, so outputs may be
        // missing — and stops the collector before the caller closes the
        // sink, so a retired collector can neither write into a closed sink
        // nor publish deliveries past EOS.
        let mut collector = collector;
        match tokio::time::timeout(COLLECTOR_DRAIN_TIMEOUT, &mut collector).await {
            Ok(Ok(())) => {}
            // The collector panicked: its buffered deliveries were never
            // published and their acknowledgements were never settled, which
            // is a failure the chain must not report as a clean drain.
            Ok(Err(error)) => {
                join_error.get_or_insert_with(|| {
                    Error::Process(format!("processor result collector failed: {error}"))
                });
            }
            Err(_elapsed) => {
                tracing::warn!(
                    timeout_secs = COLLECTOR_DRAIN_TIMEOUT.as_secs(),
                    "processor result collector did not finish draining; aborting the join"
                );
                collector.abort();
                let _ = collector.await;
                join_error.get_or_insert_with(|| {
                    Error::Process(format!(
                        "processor result collector did not finish draining within {}s",
                        COLLECTOR_DRAIN_TIMEOUT.as_secs()
                    ))
                });
            }
        }
        if let Some(error) = join_error {
            return Err(error);
        }
        if let Ok(error) = failure.try_recv() {
            return Err(error);
        }
        if failed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::Process("processor worker pool failed".into()));
        }
        Ok(())
    }

    /// Cancel and join all workers before the chain's processors are closed.
    /// In-flight calls are allowed to finish, but no new queued delivery is
    /// started and the collector cannot publish after cancellation.
    async fn cancel_and_join(self) -> Result<(), Error> {
        self.cancellation.cancel();
        self.drain().await
    }

    /// Submit one data delivery; the bounded queue applies backpressure to
    /// the chain loop exactly like a bounded inter-chain channel.
    async fn submit(&self, delivery: PoolDelivery) -> Result<(), Error> {
        // Sequence numbers are local to this pool. A process-global counter
        // makes a newly created pool wait forever for sequence zero after an
        // earlier pool has already consumed it.
        let sequence = self
            .submitted
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.work
            .send_async((sequence, delivery))
            .await
            .map_err(|_| Error::Process("processor worker pool is closed".into()))
    }

    /// Wait until every delivery submitted before this control event has been
    /// published downstream.
    ///
    /// The wait is bounded: a worker that panicked without recording a failure
    /// (or one otherwise stuck on a poisoned shared lock) leaves the collector
    /// unable to advance its sequence, and an unbounded fence would park the
    /// chain forever with no error, no output, and no checkpoint progress.
    async fn flush(&self) -> Result<(), Error> {
        let target = self.submitted.load(std::sync::atomic::Ordering::Acquire);
        let deadline = tokio::time::Instant::now() + FLUSH_TIMEOUT;
        loop {
            if self.failed.load(std::sync::atomic::Ordering::Acquire) {
                return Err(Error::Process(
                    "processor worker pool failed before control fence".into(),
                ));
            }
            // Register interest before checking the condition: the collector
            // signals with `notify_waiters`, which stores no permit, so a
            // store+notify landing between a condition check and waiter
            // registration would otherwise be lost and park this fence
            // forever.
            let notified = self.progress.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.flushed.load(std::sync::atomic::Ordering::Acquire) >= target {
                return Ok(());
            }
            let tick = tokio::time::sleep_until(deadline);
            tokio::pin!(tick);
            tokio::select! {
                _ = self.cancellation.cancelled() => {
                    return Err(Error::Process("processor worker pool was cancelled".into()));
                }
                _ = &mut tick => {
                    return Err(Error::Process(format!(
                        "processor worker pool did not publish delivery {target} within {}s",
                        FLUSH_TIMEOUT.as_secs()
                    )));
                }
                _ = &mut notified => {}
            }
        }
    }

    /// Await the first worker failure (called from the chain loop's select).
    ///
    /// A disconnected failure channel is a failure too: it means every worker
    /// and the collector exited without recording an error (a panicking worker
    /// drops its sender). Treating that as a clean shutdown dropped the pool
    /// without settling its queued deliveries, which strands the source
    /// frontier and silently disables this chain's ordering fences.
    async fn fail(&self) -> Error {
        pool_failure(&self.failure).await
    }
}

/// Map one failure-channel outcome to the chain's error. A disconnected channel
/// is a failure, not a clean shutdown (see `ProcessorWorkerPool::fail`).
async fn pool_failure(failure: &flume::Receiver<Error>) -> Error {
    match failure.recv_async().await {
        Ok(error) => error,
        Err(_) => Error::Process("processor worker pool exited without recording a failure".into()),
    }
}

/// Publish one ordered worker result. Processor failures retain their failed
/// delivery and use the same error-only routing path as the single-threaded
/// chain instead of being converted into an opaque error and dropped.
async fn flush_pool_result(
    chain: &Chain,
    result: PoolResult,
    error_targets: &BTreeMap<String, Vec<EdgeTarget>>,
) -> Result<(), Error> {
    match result {
        PoolResult::Outputs(outputs) => flush_outputs(chain, outputs).await,
        PoolResult::ProcessorFailure(failure) => {
            let targets = error_targets.get(&failure.failed_task_id);
            let Some(targets) = targets else {
                return Err(abort_processor_failure(failure).await);
            };
            let failure_message = failure.error.to_string();
            route_processor_failure(targets, failure)
                .await
                .map_err(|route_error| {
                    Error::Process(format!(
                        "processor failed and error output routing failed: {failure_message}; route error: {route_error}"
                    ))
                })
        }
    }
}

async fn abort_pool_results(pending: &mut BTreeMap<u64, PoolResult>) {
    let results = std::mem::take(pending);
    for (_, result) in results {
        match result {
            PoolResult::Outputs(outputs) => {
                let _ =
                    abort_acknowledgements(outputs.into_iter().map(|output| output.ack).collect())
                        .await;
            }
            PoolResult::ProcessorFailure(failure) => {
                let _ = abort_processor_failure(failure).await;
            }
        }
    }
}

/// Forward pool outputs through the chain's outbound edges.
async fn flush_outputs(chain: &Chain, outputs: Vec<ProcessedBatch>) -> Result<(), Error> {
    let mut published = Vec::new();
    let mut remaining = outputs.into_iter();
    while let Some(ProcessedBatch { batch, ack }) = remaining.next() {
        let current_ack = ack.clone();
        let last_task = chain.task_ids.last().map(String::as_str).unwrap_or("");
        let is_late_route = batch
            .record_batch()
            .column_by_name("__arkflow_late_event_route")
            .is_some();
        let result = if is_late_route {
            // Dynamic Session lateness is decided by the window operator, so
            // its marked copy must leave through the window task's late side
            // edge rather than re-entering the normal downstream aggregate.
            send_to_targets(
                chain.late_event_outputs.get(last_task),
                Envelope::Data(batch, ack),
                true,
            )
            .await
        } else {
            send_downstream(chain, Envelope::Data(batch, ack)).await
        };
        match result {
            Ok(()) => published.push(current_ack),
            Err(error) => {
                let mut acknowledgements = published;
                acknowledgements.push(current_ack);
                acknowledgements.extend(remaining.map(|output| output.ack));
                let abort_error = abort_acknowledgements(acknowledgements).await;
                return match abort_error {
                    Some(abort_error) => Err(Error::Process(format!(
                        "downstream routing failed: {error}; acknowledgement abort failed: {abort_error}"
                    ))),
                    None => Err(error),
                };
            }
        }
    }
    Ok(())
}

/// Undo a set of acknowledgements in reverse publication order.  A failed
/// composite delivery must settle every sibling, including outputs which
/// were already queued before a later edge failed.
async fn abort_acknowledgements(acknowledgements: Vec<Arc<dyn Ack>>) -> Option<Error> {
    let mut first_error = None;
    for ack in acknowledgements.into_iter().rev() {
        if let Err(error) = ack.abort().await {
            first_error.get_or_insert(error);
        }
    }
    first_error
}

/// Abort every acknowledgement owned by a failed processor delivery.  The
/// processor failure path deliberately retains the failed input, already
/// generated siblings, and still-unprocessed inputs; dropping any of those
/// Arcs would leave a fan-out parent or a staged journal transaction pending
/// forever.
async fn abort_processor_failure(failure: ProcessorFailure) -> Error {
    let ProcessorFailure {
        error,
        batch: _,
        ack,
        siblings,
        ..
    } = failure;
    let failure_message = error.to_string();
    let mut acknowledgements = Vec::with_capacity(siblings.len() + 1);
    acknowledgements.push(ack);
    acknowledgements.extend(siblings.into_iter().map(|output| output.ack));
    let abort_error = abort_acknowledgements(acknowledgements).await;
    match abort_error {
        Some(abort_error) => Error::Process(format!(
            "processor failed: {failure_message}; acknowledgement abort failed: {abort_error}"
        )),
        None => error,
    }
}

async fn abort_fatal_failure(failure: FatalFailure) -> Error {
    let FatalFailure {
        error,
        acknowledgements,
    } = failure;
    let failure_message = error.to_string();
    match abort_acknowledgements(acknowledgements).await {
        Some(abort_error) => Error::Process(format!(
            "processing failed: {failure_message}; acknowledgement abort failed: {abort_error}"
        )),
        None => error,
    }
}

fn result_to_batches(result: ProcessResult) -> Vec<ProcessedBatch> {
    match result {
        ProcessResult::Single(batch) => {
            vec![ProcessedBatch {
                batch,
                ack: Arc::new(crate::input::NoopAck),
            }]
        }
        ProcessResult::Multiple(batches) => batches
            .into_iter()
            .map(|batch| ProcessedBatch {
                batch,
                ack: Arc::new(crate::input::NoopAck),
            })
            .collect(),
        ProcessResult::SingleWithAck(batch, ack) => {
            vec![ProcessedBatch { batch, ack }]
        }
        ProcessResult::MultipleWithAck(batches) => batches
            .into_iter()
            .map(|(batch, ack)| ProcessedBatch { batch, ack })
            .collect(),
        ProcessResult::Deferred | ProcessResult::None => Vec::new(),
    }
}

async fn process_generated_batches(
    processor: &Arc<dyn crate::processor::Processor>,
    batches: Vec<ProcessedBatch>,
) -> Result<Vec<ProcessedBatch>, Error> {
    let mut next: Vec<ProcessedBatch> = Vec::new();
    let mut remaining = batches.into_iter();
    while let Some(ProcessedBatch { batch, ack }) = remaining.next() {
        let result = match processor.process_with_ack(batch, ack.clone()).await {
            Ok(result) => result,
            Err(error) => {
                let mut acknowledgements: Vec<Arc<dyn Ack>> =
                    next.into_iter().map(|output| output.ack).collect();
                acknowledgements.push(ack);
                acknowledgements.extend(remaining.map(|output| output.ack));
                return Err(error_after_ack_abort(error, acknowledgements).await);
            }
        };
        match result {
            ProcessResult::Single(output) => next.push(ProcessedBatch { batch: output, ack }),
            ProcessResult::Multiple(outputs) => {
                if outputs.is_empty() {
                    if let Err(error) = ack.ack().await {
                        let mut acknowledgements = next
                            .into_iter()
                            .map(|output| output.ack)
                            .collect::<Vec<_>>();
                        acknowledgements.push(ack);
                        acknowledgements.extend(remaining.map(|output| output.ack));
                        return Err(error_after_ack_abort(error, acknowledgements).await);
                    }
                } else {
                    let output_count = outputs.len();
                    next.extend(
                        outputs
                            .into_iter()
                            .zip(fanout_ack(ack, output_count))
                            .map(|(batch, ack)| ProcessedBatch { batch, ack }),
                    );
                }
            }
            ProcessResult::SingleWithAck(output, replacement_ack) => next.push(ProcessedBatch {
                batch: output,
                ack: replacement_ack,
            }),
            ProcessResult::MultipleWithAck(outputs) => next.extend(
                outputs
                    .into_iter()
                    .map(|(batch, ack)| ProcessedBatch { batch, ack }),
            ),
            ProcessResult::None => {
                if let Err(error) = ack.ack().await {
                    let mut acknowledgements = next
                        .into_iter()
                        .map(|output| output.ack)
                        .collect::<Vec<_>>();
                    acknowledgements.push(ack);
                    acknowledgements.extend(remaining.map(|output| output.ack));
                    return Err(error_after_ack_abort(error, acknowledgements).await);
                }
            }
            ProcessResult::Deferred => {}
        }
    }
    Ok(next)
}

async fn error_after_ack_abort(error: Error, acknowledgements: Vec<Arc<dyn Ack>>) -> Error {
    match abort_acknowledgements(acknowledgements).await {
        Some(abort_error) => Error::Process(format!(
            "processing failed: {error}; acknowledgement abort failed: {abort_error}"
        )),
        None => error,
    }
}

#[derive(Clone, Copy)]
enum ProcessorControl {
    Finish,
    Tick,
    Watermark(i64),
}

/// Run a control event through a fused processor chain in order.  Generated
/// batches from an earlier processor are processed by every later processor
/// before that later processor's own control hook runs; this preserves the
/// same zero-channel semantics as ordinary data processing.
async fn dispatch_processor_control(chain: &Chain, control: ProcessorControl) -> Result<(), Error> {
    let mut pending = Vec::new();
    for processor in &chain.processors {
        pending = process_generated_batches(processor, pending).await?;
        let control_result = match control {
            ProcessorControl::Finish => processor.finish().await,
            ProcessorControl::Tick => processor.on_tick().await,
            ProcessorControl::Watermark(watermark_ms) => processor.on_watermark(watermark_ms).await,
        };
        let result = match control_result {
            Ok(result) => result,
            Err(error) => {
                let acknowledgements = pending.into_iter().map(|output| output.ack).collect();
                return Err(error_after_ack_abort(error, acknowledgements).await);
            }
        };
        pending.extend(result_to_batches(result));
    }
    flush_outputs(chain, pending).await
}

async fn dispatch_watermark(chain: &Chain, watermark_ms: i64) -> Result<(), Error> {
    dispatch_processor_control(chain, ProcessorControl::Watermark(watermark_ms)).await
}

/// Flush processor-owned buffers before forwarding EOS.  Generated output is
/// passed through the remaining fused processors before the terminal EOS.
async fn finish_chain(chain: &Chain) -> Result<(), Error> {
    dispatch_processor_control(chain, ProcessorControl::Finish).await?;
    send_downstream(chain, Envelope::Eos).await
}

/// Run processor-owned processing-time timers while an interior chain has no
/// inbound data. Timer outputs use the same downstream acknowledgement path as
/// normal processor results.
async fn tick_chain(chain: &Chain) -> Result<(), Error> {
    dispatch_processor_control(chain, ProcessorControl::Tick).await
}

async fn recv_envelope(
    index: usize,
    receiver: flume::Receiver<Envelope>,
) -> (usize, Result<Envelope, Error>) {
    match receiver.recv_async().await {
        Ok(envelope) => (index, Ok(envelope)),
        // A closed channel is end-of-stream when the producer finished; only
        // a mid-flight cancellation surfaces it as an error via the caller.
        Err(_) => (index, Err(Error::InputChannelClosed)),
    }
}

/// One batch that is ready to leave a non-sink chain, carrying the
/// acknowledgement for its logical source delivery.
struct ProcessedBatch {
    batch: crate::MessageBatchRef,
    ack: Arc<dyn crate::input::Ack>,
}

/// A processor failure retains the failed delivery and every sibling delivery
/// that was produced or queued before the chain could publish them. The error
/// side output must settle the whole logical source delivery, otherwise a
/// dropped sibling can leave a fan-out acknowledgement pending forever.
struct ProcessorFailure {
    error: Error,
    batch: crate::MessageBatchRef,
    ack: Arc<dyn crate::input::Ack>,
    siblings: Vec<ProcessedBatch>,
    failed_task_id: String,
}

enum ProcessChainError {
    Processor(ProcessorFailure),
    Fatal(FatalFailure),
}

struct FatalFailure {
    error: Error,
    acknowledgements: Vec<Arc<dyn Ack>>,
}

impl From<Error> for ProcessChainError {
    fn from(error: Error) -> Self {
        Self::Fatal(FatalFailure {
            error,
            acknowledgements: Vec::new(),
        })
    }
}

/// Process one data envelope and either route successful outputs normally or
/// route a processor failure through the error-only side edges. A configured
/// error output is a side route: successful data never enters it and the
/// source acknowledgement is committed only after that output writes.
async fn dispatch_data(
    chain: &Chain,
    batch: crate::MessageBatchRef,
    ack: Arc<dyn crate::input::Ack>,
    metrics: Option<&Arc<crate::runtime::RuntimeMetrics>>,
) -> Result<(), Error> {
    let chain_metrics = metrics.map(|metrics| metrics.kernel.chain(chain.entry_task_id()));
    if let Some(chain_metrics) = &chain_metrics {
        chain_metrics
            .in_flight
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    // Batch-level trace slice covering processing through downstream flush.
    // `Instrument` keeps the span entered only while this dispatch polls; with
    // tracing disabled the callsite is off and the span is never created.
    let rows = batch.len();
    let result = async {
        match process_chain(chain, batch, ack, metrics).await {
            Ok(outputs) => flush_outputs(chain, outputs).await,
            Err(ProcessChainError::Processor(failure)) => {
                tracing::info!(
                    operator = %failure.failed_task_id,
                    error = %failure.error,
                    "operator processing failed; routing to error outputs"
                );
                match chain.error_outputs.get(&failure.failed_task_id) {
                    None => Err(abort_processor_failure(failure).await),
                    Some(targets) => {
                        let failure_message = failure.error.to_string();
                        route_processor_failure(targets, failure)
                            .await
                            .map_err(|route_error| {
                                Error::Process(format!(
                                    "processor failed and error output routing failed: {failure_message}; route error: {route_error}"
                                ))
                            })
                    }
                }
            }
            Err(ProcessChainError::Fatal(failure)) => Err(abort_fatal_failure(failure).await),
        }
    }
    .instrument(tracing::info_span!(
        "chain.batch",
        rows,
        task = chain.entry_task_id(),
    ))
    .await;
    if let Some(chain_metrics) = &chain_metrics {
        chain_metrics
            .in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
    result
}

/// Run a batch through the chain's processors. Sink chains write the final
/// result and acknowledge only after the write succeeds; interior chains
/// return output/ack pairs for downstream routing.
async fn process_chain(
    chain: &Chain,
    batch: crate::MessageBatchRef,
    ack: Arc<dyn crate::input::Ack>,
    metrics: Option<&Arc<crate::runtime::RuntimeMetrics>>,
) -> Result<Vec<ProcessedBatch>, ProcessChainError> {
    let chain_metrics = metrics.map(|metrics| metrics.kernel.chain(chain.entry_task_id()));
    let started = Instant::now();
    if let Some(chain_metrics) = &chain_metrics {
        chain_metrics
            .batches_in
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        chain_metrics
            .rows
            .fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
    }
    let mut batches = vec![(batch, ack)];
    for (processor_index, processor) in chain.processors.iter().enumerate() {
        let mut next = Vec::with_capacity(batches.len());
        let mut remaining = batches.into_iter();
        while let Some((batch, ack)) = remaining.next() {
            let failed_batch = batch.clone();
            match processor.process_with_ack(batch, ack.clone()).await {
                Ok(ProcessResult::Single(output)) => next.push((output, ack)),
                Ok(ProcessResult::Multiple(outputs)) => {
                    if outputs.is_empty() {
                        if let Err(error) = ack.ack().await {
                            let mut acknowledgements =
                                next.into_iter().map(|(_, ack)| ack).collect::<Vec<_>>();
                            acknowledgements.push(ack);
                            acknowledgements.extend(remaining.map(|(_, ack)| ack));
                            return Err(ProcessChainError::Fatal(FatalFailure {
                                error,
                                acknowledgements,
                            }));
                        }
                    } else {
                        let acks = fanout_ack(ack, outputs.len());
                        next.extend(outputs.into_iter().zip(acks));
                    }
                }
                Ok(ProcessResult::SingleWithAck(output, replacement_ack)) => {
                    next.push((output, replacement_ack));
                }
                Ok(ProcessResult::MultipleWithAck(outputs)) => {
                    next.extend(outputs);
                }
                Ok(ProcessResult::None) => {
                    // A normal filter has consumed its input successfully.
                    // Stateful buffering uses `Deferred` below to retain the
                    // acknowledgement until a later output is committed.
                    if let Err(error) = ack.ack().await {
                        let mut acknowledgements =
                            next.into_iter().map(|(_, ack)| ack).collect::<Vec<_>>();
                        acknowledgements.push(ack);
                        acknowledgements.extend(remaining.map(|(_, ack)| ack));
                        return Err(ProcessChainError::Fatal(FatalFailure {
                            error,
                            acknowledgements,
                        }));
                    }
                }
                Ok(ProcessResult::Deferred) => {}
                Err(error) => {
                    if let Some(metrics) = metrics {
                        metrics
                            .processing_errors
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    if let Some(chain_metrics) = &chain_metrics {
                        chain_metrics
                            .errors
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    let mut siblings = next
                        .into_iter()
                        .map(|(batch, ack)| ProcessedBatch { batch, ack })
                        .collect::<Vec<_>>();
                    siblings.extend(remaining.map(|(batch, ack)| ProcessedBatch { batch, ack }));
                    return Err(ProcessChainError::Processor(ProcessorFailure {
                        error,
                        batch: failed_batch,
                        ack,
                        siblings,
                        failed_task_id: chain
                            .task_ids
                            .get(processor_index)
                            .cloned()
                            .unwrap_or_else(|| chain.entry_task_id().to_owned()),
                    }));
                }
            }
        }
        batches = next;
        if batches.is_empty() {
            break;
        }
    }
    if let Some(sink) = &chain.sink {
        if !batches.is_empty() {
            let output_batches = batches
                .iter()
                .map(|(batch, _)| batch.clone())
                .collect::<Vec<_>>();
            // A hung external sink (dead connection) must not park the
            // chain forever — this await sits in the loop body where the
            // cancellation token is not polled, so it also blocks shutdown.
            // The timeout cancels the write; any partial external effect is
            // absorbed by at-least-once replay (same treatment as a write
            // error).
            let sink_timeout = sink_write_timeout();
            let write = tokio::time::timeout(sink_timeout, sink.write_batch(&output_batches))
                .await
                .map_err(|_| Error::Process(format!("sink write timed out after {sink_timeout:?}")))
                .and_then(|outcome| outcome);
            if let Err(error) = write {
                if let Some(metrics) = metrics {
                    metrics
                        .output_errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                if let Some(chain_metrics) = &chain_metrics {
                    chain_metrics
                        .errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                return Err(ProcessChainError::Fatal(FatalFailure {
                    error,
                    acknowledgements: batches.into_iter().map(|(_, ack)| ack).collect(),
                }));
            }
            if let Some(metrics) = metrics {
                use std::sync::atomic::Ordering;
                metrics.output_batches.fetch_add(1, Ordering::Relaxed);
                metrics.output_messages.fetch_add(
                    output_batches.iter().map(|batch| batch.len() as u64).sum(),
                    Ordering::Relaxed,
                );
            }
            if let Some(chain_metrics) = &chain_metrics {
                chain_metrics
                    .batches_out
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                chain_metrics.processing_us_total.fetch_add(
                    started.elapsed().as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            // The write succeeded as a whole. Only now commit all source
            // acknowledgements represented by the output batches.
            let acknowledgements: Vec<Arc<dyn Ack>> =
                batches.into_iter().map(|(_, ack)| ack).collect();
            let source_ack = crate::input::ConcurrentAck(acknowledgements.clone());
            if let Err(error) = source_ack.ack().await {
                return Err(ProcessChainError::Fatal(FatalFailure {
                    error,
                    acknowledgements,
                }));
            }
        }
        return Ok(Vec::new());
    }
    if let Some(chain_metrics) = &chain_metrics {
        chain_metrics
            .batches_out
            .fetch_add(batches.len() as u64, std::sync::atomic::Ordering::Relaxed);
        chain_metrics.processing_us_total.fetch_add(
            started.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
    Ok(batches
        .into_iter()
        .map(|(batch, ack)| ProcessedBatch { batch, ack })
        .collect())
}

/// Route the failed delivery and all of its siblings through the configured
/// error edges. Each call to `send_to_targets` preserves the normal fan-out
/// acknowledgement semantics for that delivery.
async fn route_processor_failure(
    targets: &Vec<EdgeTarget>,
    failure: ProcessorFailure,
) -> Result<(), Error> {
    let ProcessorFailure {
        error,
        batch,
        ack,
        siblings,
        ..
    } = failure;
    let mut deliveries = Vec::with_capacity(siblings.len() + 1);
    deliveries.push(ProcessedBatch { batch, ack });
    deliveries.extend(siblings);
    let mut published = Vec::new();
    let mut remaining = deliveries.into_iter();
    while let Some(ProcessedBatch { batch, ack }) = remaining.next() {
        let current_ack = ack.clone();
        if let Err(route_error) =
            send_to_targets(Some(targets), Envelope::Data(batch, ack), false).await
        {
            let mut acknowledgements = published;
            acknowledgements.push(current_ack);
            acknowledgements.extend(remaining.map(|delivery| delivery.ack));
            let abort_error = abort_acknowledgements(acknowledgements).await;
            return match abort_error {
                Some(abort_error) => Err(Error::Process(format!(
                    "processor failed and error output routing failed: {error}; route error: {route_error}; acknowledgement abort failed: {abort_error}"
                ))),
                None => Err(Error::Process(format!(
                    "processor failed and error output routing failed: {error}; route error: {route_error}"
                ))),
            };
        }
        published.push(current_ack);
    }
    Ok(())
}

/// Route an envelope to every outbound edge of the chain's last task. Data
/// acknowledgements are split across all actual terminal deliveries so a
/// source is committed only after every fan-out branch succeeds.
async fn send_downstream(chain: &Chain, envelope: Envelope) -> Result<(), Error> {
    let last_task = chain.task_ids.last().map(String::as_str).unwrap_or("");
    if matches!(envelope, Envelope::Data(_, _)) {
        return send_to_targets(chain.outputs.get(last_task), envelope, true).await;
    }

    // Barriers carry the current trace context to the next hop. `capture`
    // returns `None` without tracing (barrier bytes stay identical) and on
    // an overwriting hop the captured context replaces the upstream one so
    // the chain continues from this node's `chain.barrier` span.
    let envelope = match envelope {
        Envelope::Barrier(mut barrier) => {
            if let Some(trace_context) = super::remote::capture_trace_context() {
                barrier.trace_context = Some(trace_context);
            }
            Envelope::Barrier(barrier)
        }
        other => other,
    };

    // Control envelopes belong to the whole graph, including an error side
    // sink. Successful data must stay off that side edge, but barriers and EOS
    // must reach it or a checkpoint would wait forever for an error-sink chain
    // that never participates in the control stream.
    let mut control_targets = chain.outputs.get(last_task).cloned().unwrap_or_default();
    if let Some(error_targets) = chain.error_outputs.get(last_task) {
        control_targets.extend(error_targets.iter().cloned());
    }
    if let Some(late_targets) = chain.late_event_outputs.get(last_task) {
        control_targets.extend(late_targets.iter().cloned());
    }
    if control_targets.is_empty() {
        return Ok(());
    }
    send_to_targets(Some(&control_targets), envelope, false).await
}

/// Shared edge routing for normal and error-only outputs.
async fn send_to_targets(
    targets: Option<&Vec<EdgeTarget>>,
    envelope: Envelope,
    ack_if_unrouted: bool,
) -> Result<(), Error> {
    let Some(targets) = targets else {
        if ack_if_unrouted {
            if let Envelope::Data(_, ack) = envelope {
                ack.ack().await?;
            }
        }
        return Ok(());
    };

    if let Envelope::Data(batch, ack) = envelope {
        let mut deliveries = Vec::new();
        for target in targets {
            match target {
                EdgeTarget::Forward(sender) => deliveries.push((sender.clone(), batch.clone())),
                EdgeTarget::Broadcast(senders) => {
                    deliveries.extend(
                        senders
                            .iter()
                            .cloned()
                            .map(|sender| (sender, batch.clone())),
                    );
                }
                EdgeTarget::Partitioned {
                    channels,
                    key_field,
                    key_group_ranges,
                    max_parallelism,
                } => {
                    if channels.len() <= 1 {
                        if let Some(sender) = channels.first() {
                            deliveries.push((sender.clone(), batch.clone()));
                        }
                        continue;
                    }
                    let groups = partition_batch_by_key_hash(
                        &batch,
                        key_field,
                        channels.len(),
                        key_group_ranges,
                        *max_parallelism,
                    )?;
                    for (subtask, group) in groups.into_iter().enumerate() {
                        if let Some(group) = group {
                            deliveries.push((channels[subtask].clone(), group));
                        }
                    }
                }
            }
        }
        if deliveries.is_empty() {
            if ack_if_unrouted {
                ack.ack().await?;
            } else {
                return Err(Error::Process("error output has no deliveries".into()));
            }
            return Ok(());
        }
        let acks = fanout_ack(ack, deliveries.len());
        let all_acks = acks.clone();
        for ((sender, batch), ack) in deliveries.into_iter().zip(acks) {
            if sender.send_async(Envelope::Data(batch, ack)).await.is_err() {
                // Some sibling deliveries may already be queued when a
                // later channel closes. Abort the group, rather than merely
                // undoing each child: queued siblings must be prevented from
                // decrementing the parent after this routing attempt failed.
                let mut abort_error = None;
                for child in all_acks.iter().rev() {
                    if let Err(error) = child.abort().await {
                        abort_error.get_or_insert(error);
                    }
                }
                return match abort_error {
                    Some(error) => Err(Error::Process(format!(
                        "downstream channel closed; acknowledgement abort failed: {error}"
                    ))),
                    None => Err(Error::Process("downstream channel closed".into())),
                };
            }
        }
        return Ok(());
    }

    for target in targets {
        match target {
            EdgeTarget::Forward(sender) => {
                sender
                    .send_async(envelope.clone())
                    .await
                    .map_err(|_| Error::Process("downstream channel closed".into()))?;
            }
            EdgeTarget::Broadcast(senders) => {
                for sender in senders {
                    sender
                        .send_async(envelope.clone())
                        .await
                        .map_err(|_| Error::Process("downstream channel closed".into()))?;
                }
            }
            EdgeTarget::Partitioned {
                channels,
                key_field: _,
                key_group_ranges: _,
                max_parallelism: _,
            } => {
                // Control envelopes broadcast to every partition so each
                // subtask observes barriers, watermarks, and EOS.
                for sender in channels {
                    sender
                        .send_async(envelope.clone())
                        .await
                        .map_err(|_| Error::Process("downstream channel closed".into()))?;
                }
            }
        }
    }
    Ok(())
}

/// Split a batch into per-subtask groups by hashing the key column. Returns
/// one Option per subtask (None when the subtask receives no rows).
fn partition_batch_by_key_hash(
    batch: &crate::MessageBatch,
    key_field: &str,
    subtasks: usize,
    key_group_ranges: &[crate::job::KeyGroupRange],
    max_parallelism: u32,
) -> Result<Vec<Option<crate::MessageBatchRef>>, Error> {
    use datafusion::arrow::array::BooleanArray;
    use datafusion::arrow::compute::filter_record_batch;

    if key_field.is_empty() {
        return Ok((0..subtasks)
            .map(|index| (index == 0).then(|| Arc::new(batch.clone())))
            .collect());
    }
    if key_group_ranges.len() != subtasks {
        return Err(Error::Process(format!(
            "partition routing has {} channels but {} key-group ranges",
            subtasks,
            key_group_ranges.len()
        )));
    }
    let Some(column) = batch.record_batch().column_by_name(key_field) else {
        return Err(Error::Process(format!(
            "partition key field '{key_field}' is missing from batch"
        )));
    };
    let key_groups = hash_column(column.as_ref(), max_parallelism)?;
    let mut result = Vec::with_capacity(subtasks);
    for key_group_range in key_group_ranges.iter().take(subtasks) {
        let keep: BooleanArray = key_groups
            .iter()
            .map(|key_group| key_group.is_some_and(|key_group| key_group_range.contains(key_group)))
            .collect();
        if keep.false_count() == keep.len() {
            result.push(None);
            continue;
        }
        let filtered = filter_record_batch(batch.record_batch(), &keep)
            .map_err(|error| Error::Process(format!("partition batch: {error}")))?;
        let mut filtered_batch = crate::MessageBatch::new_arrow(filtered);
        filtered_batch.set_input_name(batch.get_input_name());
        result.push(Some(Arc::new(filtered_batch)));
    }
    Ok(result)
}

/// Map every row of a key column to the same stable key-group assignment used
/// by `JobPlan::task_for_key`. Keeping this calculation authoritative is
/// important when `max_parallelism` is larger than the number of physical
/// downstream tasks.
fn hash_column(column: &dyn Array, max_parallelism: u32) -> Result<Vec<Option<u32>>, Error> {
    fn key_group(value: &[u8], max_parallelism: u32) -> Result<u32, Error> {
        crate::job::key_group_for_key(value, max_parallelism)
    }
    macro_rules! integer_column {
        ($array:ty, $tag:literal) => {
            if let Some(values) = column.as_any().downcast_ref::<$array>() {
                return values
                    .iter()
                    .map(|value| {
                        let group = match value {
                            Some(value) => key_group(&value.to_be_bytes(), max_parallelism)?,
                            // StatefulOperator uses the same typed sentinel for
                            // nullable keys.  Null is a valid logical key, so it
                            // must be assigned to one owner rather than being
                            // filtered out of the partitioned edge.
                            None => key_group(concat!("null:", $tag).as_bytes(), max_parallelism)?,
                        };
                        Ok(Some(group))
                    })
                    .collect::<Result<Vec<Option<u32>>, Error>>();
            }
        };
    }
    integer_column!(Int8Array, "i8");
    integer_column!(Int16Array, "i16");
    integer_column!(Int32Array, "i32");
    integer_column!(Int64Array, "i64");
    integer_column!(UInt8Array, "u8");
    integer_column!(UInt16Array, "u16");
    integer_column!(UInt32Array, "u32");
    integer_column!(UInt64Array, "u64");
    // Temporal and decimal keys hash their raw integer representation with a
    // type tag: the column type is fixed per operator schema, so the raw
    // value is deterministic across batches and cannot fabricate collisions
    // between distinct logical keys.
    integer_column!(Date32Array, "date32");
    integer_column!(Date64Array, "date64");
    integer_column!(TimestampSecondArray, "ts_s");
    integer_column!(TimestampMillisecondArray, "ts_ms");
    integer_column!(TimestampMicrosecondArray, "ts_us");
    integer_column!(TimestampNanosecondArray, "ts_ns");
    integer_column!(Decimal128Array, "decimal128");
    if let Some(values) = column
        .as_any()
        .downcast_ref::<datafusion::arrow::array::BooleanArray>()
    {
        return values
            .iter()
            .map(|value| {
                let group = match value {
                    Some(true) => key_group(b"bool:1", max_parallelism)?,
                    Some(false) => key_group(b"bool:0", max_parallelism)?,
                    None => key_group(b"null:bool", max_parallelism)?,
                };
                Ok(Some(group))
            })
            .collect::<Result<Vec<Option<u32>>, Error>>();
    }
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return values
            .iter()
            .map(|value| {
                let group = match value {
                    Some(value) => key_group(value.as_bytes(), max_parallelism)?,
                    None => key_group(b"null:utf8", max_parallelism)?,
                };
                Ok(Some(group))
            })
            .collect::<Result<Vec<Option<u32>>, Error>>();
    }
    if let Some(values) = column.as_any().downcast_ref::<BinaryArray>() {
        return values
            .iter()
            .map(|value| {
                let group = match value {
                    Some(value) => key_group(value, max_parallelism)?,
                    None => key_group(b"null:binary", max_parallelism)?,
                };
                Ok(Some(group))
            })
            .collect::<Result<Vec<Option<u32>>, Error>>();
    }
    Err(Error::Process(format!(
        "partition key column has unsupported Arrow type {:?}",
        column.data_type()
    )))
}

#[cfg(test)]
mod routing_tests {
    use super::*;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    #[test]
    fn partitioned_routing_uses_key_group_ranges_not_physical_modulo() {
        let values = ["alpha", "beta", "gamma", "delta", "alpha", "epsilon"];
        let batch = crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)])),
                vec![Arc::new(StringArray::from(values.to_vec()))],
            )
            .unwrap(),
        );
        // The physical parallelism is two, but the JobPlan uses sixteen
        // key-groups. This is deliberately not equivalent to hash % 2 for
        // most keys.
        let ranges = vec![
            crate::job::KeyGroupRange { start: 0, end: 7 },
            crate::job::KeyGroupRange { start: 8, end: 15 },
        ];
        let routed = partition_batch_by_key_hash(&batch, "key", 2, &ranges, 16).unwrap();
        let mut seen = 0;
        for (index, group) in routed.into_iter().enumerate() {
            let Some(group) = group else { continue };
            let keys = group
                .record_batch()
                .column_by_name("key")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for key in keys.iter().flatten() {
                let key_group = crate::job::key_group_for_key(key.as_bytes(), 16).unwrap();
                assert!(ranges[index].contains(key_group));
                seen += 1;
            }
        }
        assert_eq!(seen, values.len());

        // Repeated logical keys must be sent to the same owner, which is the
        // invariant required by keyed state.
        for index in 0..2 {
            let Some(group) = partition_batch_by_key_hash(&batch, "key", 2, &ranges, 16)
                .unwrap()
                .into_iter()
                .nth(index)
                .flatten()
            else {
                continue;
            };
            let keys = group
                .record_batch()
                .column_by_name("key")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            if keys.iter().flatten().any(|key| key == "alpha") {
                assert_eq!(
                    keys.iter().flatten().filter(|key| *key == "alpha").count(),
                    2
                );
            }
        }
    }

    #[test]
    fn partitioned_routing_keeps_nullable_keys_on_one_owner() {
        let batch = crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, true)])),
                vec![Arc::new(StringArray::from(vec![
                    Some("alpha"),
                    None,
                    Some("beta"),
                ]))],
            )
            .unwrap(),
        );
        let ranges = vec![
            crate::job::KeyGroupRange { start: 0, end: 7 },
            crate::job::KeyGroupRange { start: 8, end: 15 },
        ];
        let routed = partition_batch_by_key_hash(&batch, "key", 2, &ranges, 16).unwrap();
        let null_group = crate::job::key_group_for_key(b"null:utf8", 16).unwrap();
        let owner = ranges
            .iter()
            .position(|range| range.contains(null_group))
            .unwrap();
        let nulls = routed[owner]
            .as_ref()
            .unwrap()
            .record_batch()
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(nulls.null_count(), 1);
        assert_eq!(
            routed
                .iter()
                .flatten()
                .map(|batch| batch.len())
                .sum::<usize>(),
            3
        );
    }
}

#[cfg(test)]
mod worker_pool_tests {
    use super::*;
    use crate::executor::graph::Chain;
    use crate::processor::Processor;
    use crate::MessageBatchRef;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A processor that stalls every delivery forever, so the pool's collector
    /// can never advance its ordered sequence.
    struct StallingProcessor {
        started: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Processor for StallingProcessor {
        async fn process(&self, _batch: MessageBatchRef) -> Result<ProcessResult, Error> {
            self.started.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<()>().await;
            unreachable!()
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    fn pool_chain(parallelism: usize, started: Arc<AtomicUsize>) -> Chain {
        Chain::for_pool_test(parallelism, vec![Arc::new(StallingProcessor { started })])
    }

    /// Regression: the failure channel disconnecting means every worker and the
    /// collector exited WITHOUT recording a failure (a panicking worker drops
    /// its sender). Reading that as a clean shutdown retired the pool without
    /// settling its queued deliveries and silently disabled the chain's
    /// ordering fences; it must be reported as a failure.
    #[tokio::test]
    async fn a_disconnected_failure_channel_is_a_failure_not_a_clean_shutdown() {
        let started = Arc::new(AtomicUsize::new(0));
        let chain = pool_chain(2, started.clone());
        let pool = ProcessorWorkerPool::start(
            &chain,
            &CheckpointHook::default(),
            &CancellationToken::new(),
        )
        .expect("a chain with parallelism > 1 owns a pool");
        // Retire the pool: every worker returns, then the collector drains and
        // exits, so the failure channel disconnects with no error recorded —
        // exactly the state a panicking worker leaves behind.
        let failure = pool.failure.clone();
        pool.cancel_and_join().await.expect("the pool drains");
        let error = tokio::time::timeout(std::time::Duration::from_secs(5), pool_failure(&failure))
            .await
            .expect("the failure report must resolve once the pool exits");
        assert!(
            error
                .to_string()
                .contains("exited without recording a failure"),
            "a silently dead pool must be reported: {error}"
        );
    }

    /// Regression: the control-event fence used to wait for the pool with no
    /// bound, so one stalled delivery parked the chain forever (no error, no
    /// output, no checkpoint progress). The fence now reports the condition
    /// within its bound; paused time exercises the real bound instantly.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_pool_fails_the_control_fence_within_its_bound() {
        let started = Arc::new(AtomicUsize::new(0));
        let chain = pool_chain(2, started.clone());
        let pool = ProcessorWorkerPool::start(
            &chain,
            &CheckpointHook::default(),
            &CancellationToken::new(),
        )
        .expect("a chain with parallelism > 1 owns a pool");
        pool.submit((
            Arc::new(crate::MessageBatch::new_arrow(
                datafusion::arrow::array::RecordBatch::new_empty(std::sync::Arc::new(
                    datafusion::arrow::datatypes::Schema::empty(),
                )),
            )),
            Arc::new(crate::input::NoopAck),
        ))
        .await
        .unwrap();
        // The delivery never publishes, so the fence must report the condition
        // once its bound elapses rather than parking the chain.
        let error = pool
            .flush()
            .await
            .expect_err("a delivery that never publishes must fail the fence");
        assert!(
            error.to_string().contains("did not publish delivery"),
            "the fence reports the stalled delivery: {error}"
        );
    }

    /// The stalling double's close path is a plain success.
    #[tokio::test]
    async fn a_stalling_processor_closes_cleanly() {
        let processor = StallingProcessor {
            started: Arc::new(AtomicUsize::new(0)),
        };
        processor.close().await.expect("close must succeed");
    }
}

#[cfg(test)]
mod watermark_idle_tests {
    use super::*;

    fn watermarks(
        now: std::time::Instant,
        entries: &[(usize, i64, std::time::Duration)],
    ) -> BTreeMap<usize, (i64, std::time::Instant)> {
        entries
            .iter()
            .map(|(index, value, age)| (*index, (*value, now - *age)))
            .collect()
    }

    #[test]
    fn silent_input_no_longer_freezes_the_frontier() {
        // Input 1 never reported a watermark and has been idle since the
        // chain started: excluded, so the frontier follows input 0 alone.
        let now = std::time::Instant::now();
        let started = now - std::time::Duration::from_secs(600);
        let marks = watermarks(now, &[(0, 1_000, std::time::Duration::from_secs(1))]);
        assert_eq!(
            effective_watermark(
                &marks,
                &BTreeSet::new(),
                2,
                now,
                started,
                WATERMARK_INPUT_IDLE_TIMEOUT
            ),
            Some(1_000)
        );
    }

    #[test]
    fn all_recent_inputs_must_report_before_the_first_frontier() {
        // Input 1 is recently active but has not reported yet: the gate
        // holds (same semantics as before idle exclusion existed).
        let now = std::time::Instant::now();
        let started = now - std::time::Duration::from_secs(1);
        let marks = watermarks(now, &[(0, 1_000, std::time::Duration::ZERO)]);
        assert_eq!(
            effective_watermark(
                &marks,
                &BTreeSet::new(),
                2,
                now,
                started,
                WATERMARK_INPUT_IDLE_TIMEOUT
            ),
            None
        );
    }

    #[test]
    fn idle_input_is_excluded_until_it_reports_again() {
        let now = std::time::Instant::now();
        let started = now - std::time::Duration::from_secs(400);
        // Input 1's last report is older than the idle timeout: the minimum
        // follows the recent inputs only.
        let idle = watermarks(
            now,
            &[
                (0, 5_000, std::time::Duration::from_secs(1)),
                (1, 2_000, std::time::Duration::from_secs(400)),
            ],
        );
        assert_eq!(
            effective_watermark(
                &idle,
                &BTreeSet::new(),
                2,
                now,
                started,
                WATERMARK_INPUT_IDLE_TIMEOUT
            ),
            Some(5_000)
        );
        // Once it reports again it rejoins the minimum; the caller clamps
        // the forwarded value so the downstream frontier stays monotonic.
        let rejoined = watermarks(
            now,
            &[
                (0, 5_000, std::time::Duration::from_secs(1)),
                (1, 2_000, std::time::Duration::from_secs(1)),
            ],
        );
        assert_eq!(
            effective_watermark(
                &rejoined,
                &BTreeSet::new(),
                2,
                now,
                started,
                WATERMARK_INPUT_IDLE_TIMEOUT
            ),
            Some(2_000)
        );
    }

    #[test]
    fn ended_inputs_stay_excluded() {
        let now = std::time::Instant::now();
        let marks = watermarks(now, &[(0, 9_000, std::time::Duration::from_secs(1))]);
        let mut ended = BTreeSet::new();
        ended.insert(1usize);
        assert_eq!(
            effective_watermark(&marks, &ended, 2, now, now, WATERMARK_INPUT_IDLE_TIMEOUT),
            Some(9_000)
        );
    }
    /// The default 5-minute timeout itself is part of the contract: a silent
    /// input gates the aggregate only while it is plausibly merely slow
    /// (within the threshold); past it, progress must continue.
    #[test]
    fn default_timeout_unfreezes_a_silent_input_only_past_the_threshold() {
        let now = std::time::Instant::now();
        let reported = watermarks(now, &[(0, 2_000, std::time::Duration::ZERO)]);

        // Within the default timeout the silent input still gates.
        let recent_start = now - WATERMARK_INPUT_IDLE_TIMEOUT / 2;
        assert_eq!(
            effective_watermark(
                &reported,
                &BTreeSet::new(),
                2,
                now,
                recent_start,
                WATERMARK_INPUT_IDLE_TIMEOUT
            ),
            None,
            "a silent input within the threshold must keep the aggregate gated"
        );

        // Past the default timeout the silent input is excluded.
        let old_start = now - WATERMARK_INPUT_IDLE_TIMEOUT - std::time::Duration::from_secs(1);
        assert_eq!(
            effective_watermark(
                &reported,
                &BTreeSet::new(),
                2,
                now,
                old_start,
                WATERMARK_INPUT_IDLE_TIMEOUT
            ),
            Some(2_000),
            "a silent input past the default timeout must no longer freeze progress"
        );
    }

    /// The forward clamp is the monotonicity contract of the whole gate
    /// change: a re-joining idle input's stale candidate must not move the
    /// frontier backwards, and genuine progress must pass through.
    #[test]
    fn forwarded_watermark_is_clamped_monotonically() {
        assert_eq!(clamp_forwarded_watermark(None, 1_000), 1_000);
        // Stale rejoin: frozen at the previous frontier.
        assert_eq!(clamp_forwarded_watermark(Some(5_000), 1_000), 5_000);
        // Genuine progress passes through.
        assert_eq!(clamp_forwarded_watermark(Some(5_000), 6_000), 6_000);
        // Equal values are stable.
        assert_eq!(clamp_forwarded_watermark(Some(5_000), 5_000), 5_000);
    }
}

/// Unit coverage for the private dispatch/routing helpers of the chain event
/// loops. These tests construct chains and envelopes directly so the error
/// and compensation arms are reachable without racing a full graph.
#[cfg(test)]
mod task_dispatch_tests {
    use super::*;
    use crate::event_time::WindowAction;
    use crate::input::NoopAck;
    use crate::processor::Processor;
    use crate::ProcessResult;
    use async_trait::async_trait;
    use datafusion::arrow::array::{
        ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    // ---------- doubles ----------

    /// Acknowledgement recording every settlement kind, with optional
    /// failure injection per kind.
    #[derive(Default)]
    struct ProbeAck {
        acks: AtomicUsize,
        aborts: AtomicUsize,
        fail_ack: std::sync::atomic::AtomicBool,
        fail_abort: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl crate::input::Ack for ProbeAck {
        async fn ack(&self) -> Result<(), Error> {
            if self.fail_ack.load(Ordering::SeqCst) {
                return Err(Error::Process("ack failed".into()));
            }
            self.acks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn abort(&self) -> Result<(), Error> {
            if self.fail_abort.load(Ordering::SeqCst) {
                return Err(Error::Process("abort failed".into()));
            }
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// A processor whose `process` and control hooks pop scripted results.
    struct ScriptedProcessor {
        results: Mutex<VecDeque<ProcessResult>>,
        controls: Mutex<VecDeque<ProcessResult>>,
        control_error: Option<String>,
    }

    impl ScriptedProcessor {
        fn new(results: Vec<ProcessResult>) -> Self {
            Self {
                results: Mutex::new(results.into_iter().collect()),
                controls: Mutex::new(VecDeque::new()),
                control_error: None,
            }
        }

        fn with_controls(results: Vec<ProcessResult>, controls: Vec<ProcessResult>) -> Self {
            Self {
                results: Mutex::new(results.into_iter().collect()),
                controls: Mutex::new(controls.into_iter().collect()),
                control_error: None,
            }
        }

        fn failing_control(message: &str) -> Self {
            Self {
                results: Mutex::new(VecDeque::new()),
                controls: Mutex::new(VecDeque::new()),
                control_error: Some(message.to_string()),
            }
        }

        fn control_result(&self) -> Result<ProcessResult, Error> {
            match &self.control_error {
                Some(message) => Err(Error::Process(message.clone())),
                None => Ok(self
                    .controls
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(ProcessResult::None)),
            }
        }
    }

    #[async_trait]
    impl Processor for ScriptedProcessor {
        async fn process(&self, _batch: crate::MessageBatchRef) -> Result<ProcessResult, Error> {
            Ok(self
                .results
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(ProcessResult::None))
        }
        async fn finish(&self) -> Result<ProcessResult, Error> {
            self.control_result()
        }
        async fn on_tick(&self) -> Result<ProcessResult, Error> {
            self.control_result()
        }
        async fn on_watermark(&self, _watermark_ms: i64) -> Result<ProcessResult, Error> {
            self.control_result()
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    fn empty_batch() -> crate::MessageBatchRef {
        Arc::new(crate::MessageBatch::new_arrow(RecordBatch::new_empty(
            Arc::new(Schema::empty()),
        )))
    }

    fn utf8_batch(rows: Vec<Option<&str>>) -> crate::MessageBatchRef {
        Arc::new(crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, true)])),
                vec![Arc::new(StringArray::from(rows)) as ArrayRef],
            )
            .unwrap(),
        ))
    }

    fn chain_with_processor(processor: Arc<dyn Processor>) -> Chain {
        let mut chain = Chain::for_pool_test(1, vec![processor]);
        chain.task_ids = vec!["m".into()];
        chain
    }

    fn hook() -> CheckpointHook {
        CheckpointHook::default()
    }

    // ---------- pure helpers ----------

    #[test]
    fn panic_payload_extracts_known_types() {
        assert_eq!(panic_payload(&"boom"), "boom");
        assert_eq!(panic_payload(&String::from("boom")), "boom");
        assert_eq!(panic_payload(&7u32), "unknown panic");
    }

    #[test]
    fn tag_input_index_is_a_no_op_when_already_tagged() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                crate::executor::join::META_INPUT_INDEX,
                DataType::UInt32,
                false,
            )])),
            vec![Arc::new(UInt32Array::from(vec![1u32]))],
        )
        .unwrap();
        let tagged = tag_input_index(batch, 0).unwrap();
        assert_eq!(tagged.num_columns(), 1);
    }

    #[test]
    fn tag_input_index_appends_the_marker() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1i64, 2i64]))],
        )
        .unwrap();
        let tagged = tag_input_index(batch, 3).unwrap();
        let column = tagged
            .column_by_name(crate::executor::join::META_INPUT_INDEX)
            .unwrap();
        let values = column.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(values.values(), &[3u32, 3u32]);
    }

    #[test]
    fn mark_event_batch_skips_an_existing_marker() {
        let batch = utf8_batch(vec![Some("a")]);
        let marked = mark_event_batch(batch.clone(), "__arkflow_late_event_route").unwrap();
        assert_eq!(marked.num_columns(), batch.num_columns() + 1);
        // Marking again is a no-op (the column already exists).
        let twice = mark_event_batch(marked.clone(), "__arkflow_late_event_route").unwrap();
        assert_eq!(twice.num_columns(), marked.num_columns());
    }

    #[tokio::test]
    async fn a_scripted_processor_closes_cleanly() {
        let processor = ScriptedProcessor::new(vec![]);
        processor.close().await.expect("close must succeed");
    }
    #[test]
    fn result_to_batches_maps_every_variant() {
        let batch = empty_batch();
        let ack: Arc<dyn Ack> = Arc::new(NoopAck);

        assert_eq!(result_to_batches(ProcessResult::Deferred).len(), 0);
        assert_eq!(result_to_batches(ProcessResult::None).len(), 0);
        assert_eq!(
            result_to_batches(ProcessResult::Single(batch.clone())).len(),
            1
        );
        assert_eq!(
            result_to_batches(ProcessResult::Multiple(vec![batch.clone(), batch.clone()])).len(),
            2
        );
        assert_eq!(
            result_to_batches(ProcessResult::SingleWithAck(batch.clone(), ack.clone())).len(),
            1
        );
        assert_eq!(
            result_to_batches(ProcessResult::MultipleWithAck(vec![
                (batch.clone(), ack.clone()),
                (batch, ack)
            ]))
            .len(),
            2
        );
    }

    #[test]
    fn hash_column_supports_every_keyed_arrow_type() {
        let max_parallelism = 16u32;
        let group = |bytes: &[u8]| crate::job::key_group_for_key(bytes, max_parallelism).unwrap();
        // Integer-like keys hash the big-endian representation of their
        // native width; nulls hash a typed sentinel instead.
        let cases: Vec<(&str, ArrayRef, u32)> = vec![
            (
                "i8",
                Arc::new(Int8Array::from(vec![Some(1i8)])),
                group(&1i8.to_be_bytes()),
            ),
            (
                "i16",
                Arc::new(Int16Array::from(vec![Some(1i16)])),
                group(&1i16.to_be_bytes()),
            ),
            (
                "i32",
                Arc::new(Int32Array::from(vec![Some(1i32)])),
                group(&1i32.to_be_bytes()),
            ),
            (
                "i64",
                Arc::new(Int64Array::from(vec![Some(1i64)])),
                group(&1i64.to_be_bytes()),
            ),
            (
                "u8",
                Arc::new(UInt8Array::from(vec![Some(1u8)])),
                group(&1u8.to_be_bytes()),
            ),
            (
                "u16",
                Arc::new(UInt16Array::from(vec![Some(1u16)])),
                group(&1u16.to_be_bytes()),
            ),
            (
                "u32",
                Arc::new(UInt32Array::from(vec![Some(1u32)])),
                group(&1u32.to_be_bytes()),
            ),
            (
                "u64",
                Arc::new(UInt64Array::from(vec![Some(1u64)])),
                group(&1u64.to_be_bytes()),
            ),
            (
                "date32",
                Arc::new(Date32Array::from(vec![Some(1i32)])),
                group(&1i32.to_be_bytes()),
            ),
            (
                "date64",
                Arc::new(Date64Array::from(vec![Some(1i64)])),
                group(&1i64.to_be_bytes()),
            ),
            (
                "ts_s",
                Arc::new(TimestampSecondArray::from(vec![Some(1i64)])),
                group(&1i64.to_be_bytes()),
            ),
            (
                "ts_ms",
                Arc::new(TimestampMillisecondArray::from(vec![Some(1i64)])),
                group(&1i64.to_be_bytes()),
            ),
            (
                "ts_us",
                Arc::new(TimestampMicrosecondArray::from(vec![Some(1i64)])),
                group(&1i64.to_be_bytes()),
            ),
            (
                "ts_ns",
                Arc::new(TimestampNanosecondArray::from(vec![Some(1i64)])),
                group(&1i64.to_be_bytes()),
            ),
            (
                "decimal128",
                Arc::new(Decimal128Array::from(vec![Some(1i128)])),
                group(&1i128.to_be_bytes()),
            ),
        ];
        for (tag, column, expected) in cases {
            let groups = hash_column(column.as_ref(), max_parallelism)
                .unwrap_or_else(|error| panic!("{tag}: {error}"));
            assert_eq!(groups, vec![Some(expected)], "{tag} hashes its raw value");
        }

        // Boolean and string/binary keys use their own sentinels.
        let booleans = hash_column(
            &BooleanArray::from(vec![Some(true), Some(false), None]),
            max_parallelism,
        )
        .unwrap();
        assert_eq!(booleans[0].unwrap(), group(b"bool:1"));
        assert_eq!(booleans[1].unwrap(), group(b"bool:0"));
        assert_eq!(booleans[2].unwrap(), group(b"null:bool"));

        let strings =
            hash_column(&StringArray::from(vec![Some("a"), None]), max_parallelism).unwrap();
        assert_eq!(strings[0].unwrap(), group(b"a"));
        assert_eq!(strings[1].unwrap(), group(b"null:utf8"));

        let binaries = hash_column(
            &datafusion::arrow::array::BinaryArray::from_iter_values([b"b"]),
            max_parallelism,
        )
        .unwrap();
        assert_eq!(binaries[0].unwrap(), group(b"b"));

        let unsupported = hash_column(&Float64Array::from(vec![1.0f64]), max_parallelism);
        assert!(unsupported.is_err());
    }

    #[test]
    fn partition_batch_by_key_hash_validates_its_inputs() {
        let batch = utf8_batch(vec![Some("a"), Some("b")]);

        // An empty key field routes everything to the first subtask.
        let routed = partition_batch_by_key_hash(&batch, "", 2, &[], 16).unwrap();
        assert!(routed[0].is_some());
        assert!(routed[1].is_none());

        // Channel/range mismatch fails closed.
        let error = partition_batch_by_key_hash(
            &batch,
            "key",
            2,
            &[crate::job::KeyGroupRange { start: 0, end: 7 }],
            16,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("2 channels but 1 key-group ranges"),
            "{error}"
        );

        // A missing key field fails closed.
        let error = partition_batch_by_key_hash(
            &batch,
            "absent",
            1,
            &[crate::job::KeyGroupRange { start: 0, end: 15 }],
            16,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("'absent' is missing from batch"),
            "{error}"
        );

        // Ranges that cover no key group produce an empty delivery per
        // subtask.
        let empty_ranges = vec![
            crate::job::KeyGroupRange { start: 1, end: 0 },
            crate::job::KeyGroupRange { start: 1, end: 0 },
        ];
        let routed = partition_batch_by_key_hash(&batch, "key", 2, &empty_ranges, 16).unwrap();
        assert!(routed.iter().all(Option::is_none));
    }

    // ---------- gate output dispatch ----------

    fn ready_slice(action: WindowAction) -> (crate::MessageBatchRef, WindowAction) {
        (empty_batch(), action)
    }

    #[tokio::test]
    async fn mismatched_gate_vectors_fail_and_abort_every_acknowledgement() {
        let chain = chain_with_processor(Arc::new(ScriptedProcessor::new(vec![])));
        let probe = Arc::new(ProbeAck::default());
        let error = dispatch_gate_outputs(
            &chain,
            &hook(),
            vec![ready_slice(WindowAction::Emit)],
            vec![Arc::new(ProbeAck::default()), Arc::new(ProbeAck::default())],
            vec![true],
            vec![probe.clone()],
            None,
            0,
            true,
            None,
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("mismatched output vectors"),
            "{error}"
        );
        // The dropped acknowledgement is aborted alongside the ready ones.
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn watermark_at_the_ready_slice_fails_the_dispatch() {
        // The watermark envelope must leave before the current ready slice;
        // a closed downstream channel fails the whole dispatch and aborts
        // every acknowledgement it still owns.
        let mut chain = chain_with_processor(Arc::new(ScriptedProcessor::new(vec![])));
        let (dead_tx, dead_rx) = flume::bounded::<Envelope>(1);
        drop(dead_rx);
        chain.outputs.insert(
            "m".into(),
            vec![super::super::graph::EdgeTarget::Forward(dead_tx)],
        );
        let probe = Arc::new(ProbeAck::default());
        let error = dispatch_gate_outputs(
            &chain,
            &hook(),
            vec![ready_slice(WindowAction::Emit)],
            vec![probe.clone()],
            vec![false],
            vec![],
            Some(1_000),
            0,
            true,
            Some(0),
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("downstream channel closed"),
            "{error}"
        );
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn watermark_after_the_last_slice_is_forwarded() {
        // A drop-only decision still forwards the watermark after the ready
        // slices when `current_ready_start` points past them.
        let mut chain = chain_with_processor(Arc::new(ScriptedProcessor::new(vec![])));
        let (tx, rx) = flume::bounded::<Envelope>(4);
        chain.outputs.insert(
            "m".into(),
            vec![super::super::graph::EdgeTarget::Forward(tx)],
        );
        dispatch_gate_outputs(
            &chain,
            &hook(),
            vec![],
            vec![],
            vec![],
            vec![],
            Some(2_000),
            0,
            true,
            Some(0),
        )
        .await
        .unwrap();
        assert!(matches!(rx.try_recv(), Ok(Envelope::Watermark(2_000))));

        // With the channel closed the same tail fails the dispatch.
        let mut chain = chain_with_processor(Arc::new(ScriptedProcessor::new(vec![])));
        let (dead_tx, dead_rx) = flume::bounded::<Envelope>(1);
        drop(dead_rx);
        chain.outputs.insert(
            "m".into(),
            vec![super::super::graph::EdgeTarget::Forward(dead_tx)],
        );
        let error = dispatch_gate_outputs(
            &chain,
            &hook(),
            vec![],
            vec![],
            vec![],
            vec![],
            Some(2_000),
            0,
            true,
            Some(0),
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("downstream channel closed"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_failed_dropped_acknowledgement_fails_the_dispatch() {
        let chain = chain_with_processor(Arc::new(ScriptedProcessor::new(vec![])));
        let probe = Arc::new(ProbeAck::default());
        probe.fail_ack.store(true, Ordering::SeqCst);
        let error = dispatch_gate_outputs(
            &chain,
            &hook(),
            vec![],
            vec![],
            vec![],
            vec![probe.clone()],
            None,
            0,
            false,
            None,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("ack failed"), "{error}");
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn the_forwarded_tail_watermark_fails_on_a_closed_channel() {
        let mut chain = chain_with_processor(Arc::new(ScriptedProcessor::new(vec![])));
        let (dead_tx, dead_rx) = flume::bounded::<Envelope>(1);
        drop(dead_rx);
        chain.outputs.insert(
            "m".into(),
            vec![super::super::graph::EdgeTarget::Forward(dead_tx)],
        );
        let error = dispatch_gate_outputs(
            &chain,
            &hook(),
            vec![],
            vec![],
            vec![],
            vec![],
            Some(3_000),
            0,
            true,
            None,
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("downstream channel closed"),
            "{error}"
        );
    }

    // ---------- gated (late-event) dispatch ----------

    #[tokio::test]
    async fn an_invalid_timestamp_routes_marked_to_the_late_edge() {
        let mut chain = chain_with_processor(Arc::new(ScriptedProcessor::new(vec![])));
        let (tx, rx) = flume::bounded::<Envelope>(2);
        chain.late_event_outputs.insert(
            "m".into(),
            vec![super::super::graph::EdgeTarget::Forward(tx)],
        );
        let probe = Arc::new(ProbeAck::default());
        dispatch_gated(
            &chain,
            &hook(),
            utf8_batch(vec![Some("late")]),
            WindowAction::Route,
            probe.clone(),
            true,
        )
        .await
        .unwrap();
        let Envelope::Data(batch, _) = rx.try_recv().unwrap() else {
            panic!("the invalid timestamp must reach the late edge");
        };
        assert!(batch
            .record_batch()
            .column_by_name("__arkflow_invalid_timestamp_route")
            .is_some());
        assert!(batch
            .record_batch()
            .column_by_name("__arkflow_late_event_route")
            .is_some());
    }

    #[tokio::test]
    async fn an_invalid_timestamp_without_a_route_target_is_dropped() {
        let chain = chain_with_processor(Arc::new(ScriptedProcessor::new(vec![])));
        let probe = Arc::new(ProbeAck::default());
        dispatch_gated(
            &chain,
            &hook(),
            utf8_batch(vec![Some("bad")]),
            WindowAction::Route,
            probe.clone(),
            true,
        )
        .await
        .unwrap();
        assert_eq!(probe.acks.load(Ordering::SeqCst), 1);
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_valid_late_row_without_a_route_target_stays_on_the_main_path() {
        let chain = chain_with_processor(Arc::new(ScriptedProcessor::new(vec![
            ProcessResult::Deferred,
        ])));
        let probe = Arc::new(ProbeAck::default());
        dispatch_gated(
            &chain,
            &hook(),
            utf8_batch(vec![Some("late")]),
            WindowAction::Route,
            probe.clone(),
            false,
        )
        .await
        .unwrap();
        // The main path re-marked the row as a late update and the chain's
        // processor retained the acknowledgement (Deferred).
        assert_eq!(probe.acks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn an_update_action_marks_the_batch() {
        let chain = chain_with_processor(Arc::new(ScriptedProcessor::new(vec![])));
        let probe = Arc::new(ProbeAck::default());
        dispatch_gated(
            &chain,
            &hook(),
            utf8_batch(vec![Some("stale")]),
            WindowAction::Update,
            probe.clone(),
            false,
        )
        .await
        .unwrap();
        // The marked update flowed through the chain (no outputs: the data
        // acknowledgement settles immediately).
        assert_eq!(probe.acks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_failing_chain_processor_aborts_the_gated_acknowledgement() {
        struct FailingProcessor;
        #[async_trait]
        impl Processor for FailingProcessor {
            async fn process(
                &self,
                _batch: crate::MessageBatchRef,
            ) -> Result<ProcessResult, Error> {
                Err(Error::Process("injected processor failure".into()))
            }
            async fn close(&self) -> Result<(), Error> {
                Ok(())
            }
        }
        let chain = chain_with_processor(Arc::new(FailingProcessor));
        let probe = Arc::new(ProbeAck::default());
        let error = dispatch_gated(
            &chain,
            &hook(),
            utf8_batch(vec![Some("row")]),
            WindowAction::Emit,
            probe.clone(),
            false,
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("injected processor failure"),
            "{error}"
        );
        // The delivery is aborted by the dispatch error path and again by
        // the gate-output compensation pass.
        assert!(probe.aborts.load(Ordering::SeqCst) >= 1);
    }

    // ---------- output flushing ----------

    #[tokio::test]
    async fn late_routed_pool_output_leaves_through_the_late_edge() {
        let mut chain = chain_with_processor(Arc::new(ScriptedProcessor::new(vec![])));
        let (tx, rx) = flume::bounded::<Envelope>(2);
        chain.late_event_outputs.insert(
            "m".into(),
            vec![super::super::graph::EdgeTarget::Forward(tx)],
        );
        let batch =
            mark_event_batch(utf8_batch(vec![Some("late")]), "__arkflow_late_event_route").unwrap();
        flush_outputs(
            &chain,
            vec![ProcessedBatch {
                batch,
                ack: Arc::new(ProbeAck::default()),
            }],
        )
        .await
        .unwrap();
        assert!(matches!(rx.try_recv(), Ok(Envelope::Data(_, _))));
    }

    #[tokio::test]
    async fn a_failed_late_route_aborts_the_remaining_outputs() {
        let mut chain = chain_with_processor(Arc::new(ScriptedProcessor::new(vec![])));
        let (dead_tx, dead_rx) = flume::bounded::<Envelope>(1);
        drop(dead_rx);
        chain.late_event_outputs.insert(
            "m".into(),
            vec![super::super::graph::EdgeTarget::Forward(dead_tx)],
        );
        let first = Arc::new(ProbeAck::default());
        let second = Arc::new(ProbeAck::default());
        let marked =
            mark_event_batch(utf8_batch(vec![Some("late")]), "__arkflow_late_event_route").unwrap();
        let error = flush_outputs(
            &chain,
            vec![
                ProcessedBatch {
                    batch: marked,
                    ack: first.clone(),
                },
                ProcessedBatch {
                    batch: empty_batch(),
                    ack: second.clone(),
                },
            ],
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("downstream channel closed"),
            "{error}"
        );
        // Both the failed delivery and the un-routed sibling settle. The
        // failed delivery's acknowledgement is aborted by both the failed
        // fan-out and the compensation pass, so "at least once" is the
        // contract that matters here.
        assert!(first.aborts.load(Ordering::SeqCst) >= 1);
        assert!(second.aborts.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn a_failed_flush_reports_a_failing_abort() {
        let mut chain = chain_with_processor(Arc::new(ScriptedProcessor::new(vec![])));
        let (dead_tx, dead_rx) = flume::bounded::<Envelope>(1);
        drop(dead_rx);
        chain.outputs.insert(
            "m".into(),
            vec![super::super::graph::EdgeTarget::Forward(dead_tx)],
        );
        let probe = Arc::new(ProbeAck::default());
        probe.fail_abort.store(true, Ordering::SeqCst);
        let error = flush_outputs(
            &chain,
            vec![ProcessedBatch {
                batch: empty_batch(),
                ack: probe.clone(),
            }],
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("acknowledgement abort failed"),
            "{error}"
        );
    }

    // ---------- generated-batch processing ----------

    fn processed(ack: Arc<dyn Ack>) -> Vec<ProcessedBatch> {
        vec![ProcessedBatch {
            batch: empty_batch(),
            ack,
        }]
    }

    #[tokio::test]
    async fn generated_multiple_empty_output_settles_the_acknowledgement() {
        let processor: Arc<dyn Processor> =
            Arc::new(ScriptedProcessor::new(vec![ProcessResult::Multiple(
                vec![],
            )]));
        let probe = Arc::new(ProbeAck::default());
        process_generated_batches(&processor, processed(probe.clone()))
            .await
            .unwrap();
        assert_eq!(probe.acks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn generated_multiple_empty_output_fails_when_the_ack_fails() {
        let processor: Arc<dyn Processor> =
            Arc::new(ScriptedProcessor::new(vec![ProcessResult::Multiple(
                vec![],
            )]));
        let probe = Arc::new(ProbeAck::default());
        probe.fail_ack.store(true, Ordering::SeqCst);
        let Err(error) = process_generated_batches(&processor, processed(probe.clone())).await
        else {
            panic!("a failing acknowledgement must fail the stage");
        };
        assert!(error.to_string().contains("ack failed"), "{error}");
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn generated_multiple_output_fans_out_the_acknowledgement() {
        let processor: Arc<dyn Processor> = Arc::new(ScriptedProcessor::new(vec![
            ProcessResult::Multiple(vec![empty_batch(), empty_batch()]),
        ]));
        let probe = Arc::new(ProbeAck::default());
        let outputs = process_generated_batches(&processor, processed(probe.clone()))
            .await
            .unwrap();
        assert_eq!(outputs.len(), 2);
        // Settling one branch is not enough; both must ack.
        outputs[0].ack.ack().await.unwrap();
        assert_eq!(probe.acks.load(Ordering::SeqCst), 0);
        outputs[1].ack.ack().await.unwrap();
        assert_eq!(probe.acks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn generated_with_ack_results_reuse_the_replacement() {
        let replacement = Arc::new(ProbeAck::default());
        let processor: Arc<dyn Processor> = Arc::new(ScriptedProcessor::new(vec![
            ProcessResult::SingleWithAck(empty_batch(), replacement.clone()),
            ProcessResult::MultipleWithAck(vec![
                (empty_batch(), Arc::new(ProbeAck::default())),
                (empty_batch(), Arc::new(ProbeAck::default())),
            ]),
        ]));
        let next = process_generated_batches(&processor, processed(Arc::new(ProbeAck::default())))
            .await
            .unwrap();
        // The first stage's replacement flows into the second stage's input.
        assert_eq!(next.len(), 1);
        let next = process_generated_batches(&processor, next).await.unwrap();
        assert_eq!(next.len(), 2);
        for output in &next {
            output.ack.ack().await.unwrap();
        }
        replacement.ack().await.unwrap();
        assert_eq!(replacement.acks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn generated_none_and_deferred_results() {
        let probe = Arc::new(ProbeAck::default());
        let processor: Arc<dyn Processor> =
            Arc::new(ScriptedProcessor::new(vec![ProcessResult::None]));
        process_generated_batches(&processor, processed(probe.clone()))
            .await
            .unwrap();
        assert_eq!(probe.acks.load(Ordering::SeqCst), 1);

        let probe = Arc::new(ProbeAck::default());
        let processor: Arc<dyn Processor> =
            Arc::new(ScriptedProcessor::new(vec![ProcessResult::Deferred]));
        let retained = process_generated_batches(&processor, processed(probe.clone()))
            .await
            .unwrap();
        assert!(retained.is_empty());
        assert_eq!(probe.acks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn generated_none_output_fails_when_the_ack_fails() {
        let processor: Arc<dyn Processor> =
            Arc::new(ScriptedProcessor::new(vec![ProcessResult::None]));
        let probe = Arc::new(ProbeAck::default());
        probe.fail_ack.store(true, Ordering::SeqCst);
        let Err(error) = process_generated_batches(&processor, processed(probe.clone())).await
        else {
            panic!("a failing acknowledgement must fail the stage");
        };
        assert!(error.to_string().contains("ack failed"), "{error}");
    }

    #[tokio::test]
    async fn generated_processing_failure_aborts_later_batches() {
        struct FailAfterFirst;
        #[async_trait]
        impl Processor for FailAfterFirst {
            async fn process(
                &self,
                _batch: crate::MessageBatchRef,
            ) -> Result<ProcessResult, Error> {
                Err(Error::Process("second stage failed".into()))
            }
            async fn close(&self) -> Result<(), Error> {
                Ok(())
            }
        }
        let processor: Arc<dyn Processor> = Arc::new(FailAfterFirst);
        let probe = Arc::new(ProbeAck::default());
        let Err(error) = process_generated_batches(&processor, processed(probe.clone())).await
        else {
            panic!("a failing stage must fail the batch run");
        };
        assert!(error.to_string().contains("second stage failed"), "{error}");
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_failing_abort_during_compensation_is_reported() {
        struct FailAfterFirst;
        #[async_trait]
        impl Processor for FailAfterFirst {
            async fn process(
                &self,
                _batch: crate::MessageBatchRef,
            ) -> Result<ProcessResult, Error> {
                Err(Error::Process("stage failed".into()))
            }
            async fn close(&self) -> Result<(), Error> {
                Ok(())
            }
        }
        let processor: Arc<dyn Processor> = Arc::new(FailAfterFirst);
        let probe = Arc::new(ProbeAck::default());
        probe.fail_abort.store(true, Ordering::SeqCst);
        let Err(error) = process_generated_batches(&processor, processed(probe.clone())).await
        else {
            panic!("a failing stage must fail the batch run");
        };
        assert!(
            error.to_string().contains("acknowledgement abort failed"),
            "{error}"
        );
    }

    // ---------- control dispatch ----------

    #[tokio::test]
    async fn a_failing_control_hook_fails_the_dispatch() {
        let processor = Arc::new(ScriptedProcessor::failing_control("control failed"));
        let chain = chain_with_processor(processor);
        for control in [
            ProcessorControl::Finish,
            ProcessorControl::Tick,
            ProcessorControl::Watermark(500),
        ] {
            let error = dispatch_processor_control(&chain, control)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("control failed"), "{error}");
        }
    }

    #[tokio::test]
    async fn tick_and_watermark_controls_flush_through_the_chain() {
        let (tx, rx) = flume::bounded::<Envelope>(4);
        let mut chain = chain_with_processor(Arc::new(ScriptedProcessor::with_controls(
            vec![],
            vec![
                ProcessResult::Single(empty_batch()),
                ProcessResult::Single(empty_batch()),
            ],
        )));
        chain.outputs.insert(
            "m".into(),
            vec![super::super::graph::EdgeTarget::Forward(tx)],
        );
        dispatch_processor_control(&chain, ProcessorControl::Tick)
            .await
            .unwrap();
        assert!(matches!(rx.try_recv(), Ok(Envelope::Data(_, _))));
        dispatch_processor_control(&chain, ProcessorControl::Watermark(500))
            .await
            .unwrap();
        assert!(matches!(rx.try_recv(), Ok(Envelope::Data(_, _))));
    }

    // ---------- failure routing ----------

    fn processor_failure(ack: Arc<dyn Ack>) -> ProcessorFailure {
        ProcessorFailure {
            error: Error::Process("operator failed".into()),
            batch: empty_batch(),
            ack,
            siblings: Vec::new(),
            failed_task_id: "m".into(),
        }
    }

    #[tokio::test]
    async fn a_failed_error_route_without_abort_failure_reports_both() {
        let (dead_tx, dead_rx) = flume::bounded::<Envelope>(1);
        drop(dead_rx);
        let probe = Arc::new(ProbeAck::default());
        let error = route_processor_failure(
            &vec![super::super::graph::EdgeTarget::Forward(dead_tx)],
            processor_failure(probe.clone()),
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("error output routing failed")
                && error.to_string().contains("operator failed")
                && error.to_string().contains("downstream channel closed"),
            "{error}"
        );
        // The failed fan-out aborts the delivery once and the route failure
        // compensation aborts it again.
        assert!(probe.aborts.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn a_failed_error_route_with_a_failing_abort_reports_the_triple() {
        let (dead_tx, dead_rx) = flume::bounded::<Envelope>(1);
        drop(dead_rx);
        let probe = Arc::new(ProbeAck::default());
        probe.fail_abort.store(true, Ordering::SeqCst);
        let error = route_processor_failure(
            &vec![super::super::graph::EdgeTarget::Forward(dead_tx)],
            processor_failure(probe.clone()),
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("acknowledgement abort failed"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_pooled_failure_without_error_targets_aborts() {
        let chain = chain_with_processor(Arc::new(ScriptedProcessor::new(vec![])));
        let probe = Arc::new(ProbeAck::default());
        let error = flush_pool_result(
            &chain,
            PoolResult::ProcessorFailure(processor_failure(probe.clone())),
            &BTreeMap::new(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("operator failed"), "{error}");
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_pooled_failure_routes_through_the_error_targets() {
        let (tx, rx) = flume::bounded::<Envelope>(2);
        let chain = chain_with_processor(Arc::new(ScriptedProcessor::new(vec![])));
        let error_targets = BTreeMap::from([(
            "m".to_string(),
            vec![super::super::graph::EdgeTarget::Forward(tx)],
        )]);
        let probe = Arc::new(ProbeAck::default());
        flush_pool_result(
            &chain,
            PoolResult::ProcessorFailure(processor_failure(probe.clone())),
            &error_targets,
        )
        .await
        .unwrap();
        assert!(matches!(rx.try_recv(), Ok(Envelope::Data(_, _))));
        assert_eq!(probe.acks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_failed_pooled_error_route_combines_the_errors() {
        let (dead_tx, dead_rx) = flume::bounded::<Envelope>(1);
        drop(dead_rx);
        let chain = chain_with_processor(Arc::new(ScriptedProcessor::new(vec![])));
        let error_targets = BTreeMap::from([(
            "m".to_string(),
            vec![super::super::graph::EdgeTarget::Forward(dead_tx)],
        )]);
        let error = flush_pool_result(
            &chain,
            PoolResult::ProcessorFailure(processor_failure(Arc::new(ProbeAck::default()))),
            &error_targets,
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("error output routing failed"),
            "{error}"
        );
    }

    // ---------- data dispatch with a failing error route ----------

    #[tokio::test]
    async fn dispatch_data_reports_a_failing_error_route() {
        struct FailingProcessor;
        #[async_trait]
        impl Processor for FailingProcessor {
            async fn process(
                &self,
                _batch: crate::MessageBatchRef,
            ) -> Result<ProcessResult, Error> {
                Err(Error::Process("operator failed".into()))
            }
            async fn close(&self) -> Result<(), Error> {
                Ok(())
            }
        }
        let (dead_tx, dead_rx) = flume::bounded::<Envelope>(1);
        drop(dead_rx);
        let mut chain = chain_with_processor(Arc::new(FailingProcessor));
        chain.error_outputs.insert(
            "m".into(),
            vec![super::super::graph::EdgeTarget::Forward(dead_tx)],
        );
        let error = dispatch_data(
            &chain,
            utf8_batch(vec![Some("row")]),
            Arc::new(ProbeAck::default()),
            None,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("processor failed and error output routing failed"),
            "{error}"
        );
    }

    // ---------- edge routing ----------

    #[tokio::test]
    async fn partitioned_data_routing_spreads_rows_across_channels() {
        let batch = utf8_batch(vec![Some("alpha"), Some("beta"), Some("gamma")]);
        let (tx0, rx0) = flume::bounded::<Envelope>(4);
        let (tx1, rx1) = flume::bounded::<Envelope>(4);
        let probe = Arc::new(ProbeAck::default());
        send_to_targets(
            Some(&vec![super::super::graph::EdgeTarget::Partitioned {
                channels: vec![tx0, tx1],
                key_field: "key".into(),
                key_group_ranges: vec![
                    crate::job::KeyGroupRange { start: 0, end: 7 },
                    crate::job::KeyGroupRange { start: 8, end: 15 },
                ],
                max_parallelism: 16,
            }]),
            Envelope::Data(batch, probe.clone()),
            true,
        )
        .await
        .unwrap();
        let mut received = 0;
        while let Ok(Envelope::Data(batch, _)) = rx0.try_recv() {
            received += batch.len();
        }
        while let Ok(Envelope::Data(batch, _)) = rx1.try_recv() {
            received += batch.len();
        }
        assert_eq!(received, 3, "every row must arrive on one of the channels");
        assert_eq!(probe.acks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unroutable_partitioned_data_settles_its_acknowledgement() {
        let batch = utf8_batch(vec![Some("alpha")]);
        let (tx0, _rx0) = flume::bounded::<Envelope>(1);
        let (tx1, _rx1) = flume::bounded::<Envelope>(1);
        let probe = Arc::new(ProbeAck::default());
        send_to_targets(
            Some(&vec![super::super::graph::EdgeTarget::Partitioned {
                channels: vec![tx0, tx1],
                key_field: "key".into(),
                key_group_ranges: vec![
                    crate::job::KeyGroupRange { start: 1, end: 0 },
                    crate::job::KeyGroupRange { start: 1, end: 0 },
                ],
                max_parallelism: 16,
            }]),
            Envelope::Data(batch, probe.clone()),
            true,
        )
        .await
        .unwrap();
        assert_eq!(probe.acks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_closed_sibling_channel_aborts_the_whole_fanout() {
        let batch = utf8_batch(vec![Some("alpha")]);
        let (live_tx, _live_rx) = flume::bounded::<Envelope>(1);
        let (dead_tx, dead_rx) = flume::bounded::<Envelope>(1);
        drop(dead_rx);
        let probe = Arc::new(ProbeAck::default());
        let error = send_to_targets(
            Some(&vec![super::super::graph::EdgeTarget::Broadcast(vec![
                live_tx, dead_tx,
            ])]),
            Envelope::Data(batch, probe.clone()),
            true,
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("downstream channel closed"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn control_envelopes_broadcast_to_every_partition_channel() {
        let (tx0, rx0) = flume::bounded::<Envelope>(1);
        let (tx1, rx1) = flume::bounded::<Envelope>(1);
        send_to_targets(
            Some(&vec![super::super::graph::EdgeTarget::Partitioned {
                channels: vec![tx0, tx1],
                key_field: "key".into(),
                key_group_ranges: vec![
                    crate::job::KeyGroupRange { start: 0, end: 7 },
                    crate::job::KeyGroupRange { start: 8, end: 15 },
                ],
                max_parallelism: 16,
            }]),
            Envelope::Eos,
            false,
        )
        .await
        .unwrap();
        assert!(matches!(rx0.try_recv(), Ok(Envelope::Eos)));
        assert!(matches!(rx1.try_recv(), Ok(Envelope::Eos)));
    }

    #[tokio::test]
    async fn unrouted_error_data_returns_an_error() {
        // An error-only edge that routes to zero deliveries must fail rather
        // than silently dropping the failed delivery.
        let batch = utf8_batch(vec![Some("alpha")]);
        let (tx0, _rx0) = flume::bounded::<Envelope>(1);
        let (tx1, _rx1) = flume::bounded::<Envelope>(1);
        let error = send_to_targets(
            Some(&vec![super::super::graph::EdgeTarget::Partitioned {
                channels: vec![tx0, tx1],
                key_field: "key".into(),
                key_group_ranges: vec![
                    crate::job::KeyGroupRange { start: 1, end: 0 },
                    crate::job::KeyGroupRange { start: 1, end: 0 },
                ],
                max_parallelism: 16,
            }]),
            Envelope::Data(batch, Arc::new(NoopAck)),
            false,
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("error output has no deliveries"),
            "{error}"
        );
    }
}

/// Coverage for the chain event loops themselves (source and interior):
/// drain timeouts, reconnect cancellation, event-time gate failures, worker
/// pool supervision, alignment, and shutdown paths that the dispatch-level
/// tests above cannot reach. Everything drives the private loop functions
/// directly over hand-built chains, so no graph construction is needed.
#[cfg(test)]
mod task_loop_tests {
    use super::*;
    use crate::event_time::WatermarkTracker;
    use crate::executor::event_time_gate::{EventTimeGate, WindowTiming};
    use crate::executor::graph::EdgeTarget;
    use crate::input::Ack;
    use crate::job::WatermarkStrategy;
    use crate::job::{LateEventPolicy, TimeMode, TimeSpec, WatermarkSpec};
    use crate::processor::Processor;
    use async_trait::async_trait;
    use datafusion::arrow::array::{ArrayRef, BinaryArray, Int64Array, Int8Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    const BUDGET: Duration = Duration::from_secs(30);

    // ---------- doubles ----------

    /// Acknowledgement recording settlements, with injectable failures.
    #[derive(Default)]
    struct ProbeAck {
        acks: AtomicUsize,
        aborts: AtomicUsize,
        fail_ack: AtomicBool,
        fail_abort: AtomicBool,
    }

    #[async_trait]
    impl Ack for ProbeAck {
        async fn ack(&self) -> Result<(), Error> {
            if self.fail_ack.load(Ordering::SeqCst) {
                return Err(Error::Process("ack failed".into()));
            }
            self.acks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn abort(&self) -> Result<(), Error> {
            if self.fail_abort.load(Ordering::SeqCst) {
                return Err(Error::Process("abort failed".into()));
            }
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    enum SourceStep {
        Batch(crate::MessageBatchRef, Arc<dyn Ack>),
        Disconnection,
    }

    /// A source whose `read` sequence and `connect` outcome are scripted.
    struct ScriptSource {
        steps: Mutex<VecDeque<SourceStep>>,
        connect_fails: bool,
    }

    impl ScriptSource {
        fn with_steps(steps: Vec<SourceStep>) -> Arc<Self> {
            Arc::new(Self {
                steps: Mutex::new(steps.into_iter().collect()),
                connect_fails: true,
            })
        }

        fn idle() -> Arc<Self> {
            Self::with_steps(Vec::new())
        }
    }

    #[async_trait]
    impl crate::input::Input for ScriptSource {
        async fn connect(&self) -> Result<(), Error> {
            if self.connect_fails {
                Err(Error::Connection("scripted connect failure".into()))
            } else {
                Ok(())
            }
        }
        async fn read(&self) -> Result<(crate::MessageBatchRef, Arc<dyn Ack>), Error> {
            let step = self.steps.lock().unwrap().pop_front();
            match step {
                Some(SourceStep::Batch(batch, ack)) => Ok((batch, ack)),
                Some(SourceStep::Disconnection) => Err(Error::Disconnection),
                None => std::future::pending().await,
            }
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    /// Processor returning `Deferred`: the acknowledgement stays unsettled,
    /// which keeps a barrier's acknowledgement drain permanently blocking.
    struct RetainAckProcessor {
        seen: Arc<AtomicUsize>,
    }

    /// A source whose `current_positions` never resolves: the barrier branch
    /// parks inside the positions await until cancellation.
    struct ParkingPositionsSource;

    #[async_trait]
    impl crate::input::Input for ParkingPositionsSource {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn read(&self) -> Result<(crate::MessageBatchRef, Arc<dyn Ack>), Error> {
            std::future::pending().await
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn current_positions(&self) -> Result<Vec<crate::checkpoint::SourcePosition>, Error> {
            std::future::pending().await
        }
    }

    #[async_trait]
    impl Processor for RetainAckProcessor {
        async fn process(&self, _batch: crate::MessageBatchRef) -> Result<ProcessResult, Error> {
            self.seen.fetch_add(1, Ordering::SeqCst);
            Ok(ProcessResult::Deferred)
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    /// Processor returning `None` (a filter), optionally failing `close`.
    struct NoneProcessor {
        close_fails: bool,
    }

    #[async_trait]
    impl Processor for NoneProcessor {
        async fn process(&self, _batch: crate::MessageBatchRef) -> Result<ProcessResult, Error> {
            Ok(ProcessResult::None)
        }
        async fn close(&self) -> Result<(), Error> {
            if self.close_fails {
                Err(Error::Process("close failed".into()))
            } else {
                Ok(())
            }
        }
    }

    /// Processor that blocks the delivery whose `ts` column matches
    /// `gate_ts` on an externally held gate. Results are keyed by the same
    /// match, so which worker claims which sequence stays irrelevant.
    struct GatedScriptProcessor {
        gate: Arc<tokio::sync::Mutex<()>>,
        gate_ts: i64,
        gated: Mutex<Option<Result<ProcessResult, Error>>>,
        other: Mutex<Option<Result<ProcessResult, Error>>>,
    }

    impl GatedScriptProcessor {
        fn new(
            gate: Arc<tokio::sync::Mutex<()>>,
            gate_ts: i64,
            gated: Option<Result<ProcessResult, Error>>,
            other: Option<Result<ProcessResult, Error>>,
        ) -> Arc<Self> {
            Arc::new(Self {
                gate,
                gate_ts,
                gated: Mutex::new(gated),
                other: Mutex::new(other),
            })
        }

        /// Every delivery forwards its batch unchanged (the empty slots fall
        /// back to `Single(batch)`); only `gate_ts` blocks.
        fn passthrough(gate: Arc<tokio::sync::Mutex<()>>, gate_ts: i64) -> Arc<Self> {
            Self::new(gate, gate_ts, None, None)
        }

        fn is_gated(&self, batch: &crate::MessageBatchRef) -> bool {
            let column = batch
                .record_batch()
                .column_by_name("ts")
                .and_then(|column| {
                    column
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::Int64Array>()
                })
                .expect("gated batches carry an int64 ts column");
            column.value(0) == self.gate_ts
        }
    }

    #[async_trait]
    impl Processor for GatedScriptProcessor {
        async fn process(&self, batch: crate::MessageBatchRef) -> Result<ProcessResult, Error> {
            let gated = self.is_gated(&batch);
            if gated {
                let _held = self.gate.lock().await;
            }
            let slot = if gated { &self.gated } else { &self.other };
            match slot.lock().unwrap().take() {
                Some(result) => result,
                None => Ok(ProcessResult::Single(batch)),
            }
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct FailingProcessor;

    #[async_trait]
    impl Processor for FailingProcessor {
        async fn process(&self, _batch: crate::MessageBatchRef) -> Result<ProcessResult, Error> {
            Err(Error::Process("injected processor failure".into()))
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    /// A sink that accepts every write.
    struct NullOutput;

    #[async_trait]
    impl crate::output::Output for NullOutput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn write(&self, _msg: crate::MessageBatchRef) -> Result<(), Error> {
            Ok(())
        }
        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    fn ts_batch(value: i64) -> crate::MessageBatchRef {
        Arc::new(crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from(vec![value])) as ArrayRef],
            )
            .unwrap(),
        ))
    }

    fn event_time_spec() -> TimeSpec {
        TimeSpec {
            mode: TimeMode::EventTime,
            timestamp_field: Some("ts".into()),
            watermark: Some(WatermarkSpec {
                strategy: WatermarkStrategy::BoundedOutOfOrderness,
                out_of_orderness_ms: 100,
                idle_timeout_ms: Some(1_000),
            }),
            allowed_lateness_ms: 100,
            late_event_policy: LateEventPolicy::Update,
            late_event_route: None,
        }
    }

    fn barrier(id: &str, generation: u64) -> Envelope {
        Envelope::Barrier(crate::checkpoint::CheckpointBarrier {
            checkpoint_id: id.into(),
            generation,
            trace_context: None,
        })
    }

    /// Build an interior chain with `inputs` inbound channels.
    fn interior_chain(
        parallelism: usize,
        processors: Vec<Arc<dyn Processor>>,
        inputs: usize,
    ) -> (Chain, Vec<flume::Sender<Envelope>>) {
        let mut chain = Chain::for_pool_test(parallelism, processors);
        chain.task_ids = vec!["m".into()];
        let mut senders = Vec::with_capacity(inputs);
        for _ in 0..inputs {
            let (tx, rx) = flume::bounded::<Envelope>(8192);
            chain.inputs.push(rx);
            senders.push(tx);
        }
        (chain, senders)
    }

    async fn settle(task: tokio::task::JoinHandle<Result<(), Error>>) -> Result<(), Error> {
        tokio::time::timeout(BUDGET, task)
            .await
            .expect("chain settles within timeout")
            .expect("chain task must not panic")
    }

    // ---------- source loop: barrier drain timeout ----------

    /// A checkpoint barrier whose in-flight (non-held) acknowledgements never
    /// drain must abort the round through the bounded timeout instead of
    /// parking the source forever. Paused time walks the real 30-second bound
    /// instantly.
    #[tokio::test(start_paused = true)]
    async fn barrier_drain_timeout_aborts_the_round_and_reports() {
        let seen = Arc::new(AtomicUsize::new(0));
        let mut chain =
            Chain::for_pool_test(1, vec![Arc::new(RetainAckProcessor { seen: seen.clone() })]);
        chain.task_ids = vec!["src".into()];
        let (barrier_tx, barrier_rx) = flume::bounded::<Envelope>(4);
        let (fail_tx, mut fail_rx) = tokio::sync::mpsc::unbounded_channel();
        let hook = CheckpointHook {
            failure_reporter: Some(fail_tx),
            barrier_rx: Some(Arc::new(tokio::sync::Mutex::new(barrier_rx))),
            ..Default::default()
        };
        let source = ScriptSource::with_steps(vec![SourceStep::Batch(
            ts_batch(1_000),
            Arc::new(ProbeAck::default()),
        )]);
        chain.source = Some(source);
        let token = CancellationToken::new();
        let task = tokio::spawn(run_chain(chain, hook, token.clone()));
        // The delivery must be dispatched (and retained by the Deferred
        // processor) before the barrier arrives, so the drain is blocking.
        tokio::time::timeout(BUDGET, async {
            while seen.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the batch is dispatched before the barrier");
        barrier_tx
            .send_async(barrier("c-1", 1))
            .await
            .expect("barrier queued");
        let report = tokio::time::timeout(4 * BUDGET, fail_rx.recv())
            .await
            .expect("the drain timeout reports a checkpoint failure")
            .expect("reporter alive");
        assert!(
            report
                .to_string()
                .contains("drain timed out with 1 acknowledgements"),
            "{report}"
        );
        token.cancel();
        let result = settle(task).await;
        assert!(
            result.is_ok(),
            "an aborted round must not fail the chain: {result:?}"
        );
    }

    // ---------- source loop: cancellation inside the barrier branch ----------

    /// A barrier branch parked on `current_positions` (or the snapshot and
    /// barrier-delivery awaits that follow it) must observe the Job's
    /// cancellation token and take the clean shutdown path instead of
    /// parking the branch forever.
    #[tokio::test]
    async fn cancellation_during_barrier_positions_wait_unblocks_the_chain() {
        let mut chain = Chain::for_pool_test(1, vec![]);
        chain.task_ids = vec!["src".into()];
        chain.source = Some(Arc::new(ParkingPositionsSource));
        let (barrier_tx, barrier_rx) = flume::bounded::<Envelope>(4);
        let hook = CheckpointHook {
            barrier_rx: Some(Arc::new(tokio::sync::Mutex::new(barrier_rx))),
            ..Default::default()
        };
        let token = CancellationToken::new();
        let task = tokio::spawn(run_chain(chain, hook, token.clone()));
        barrier_tx
            .send_async(barrier("c-1", 1))
            .await
            .expect("barrier queued");
        // Let the branch park inside the positions await, then cancel: the
        // chain must settle through the shutdown path, not hang.
        tokio::time::sleep(Duration::from_millis(20)).await;
        token.cancel();
        let result = settle(task).await;
        assert!(result.is_ok(), "{result:?}");
    }

    // ---------- source loop: structured input-channel closure ----------

    /// A closed input channel surfaces as the dedicated `Error`
    /// variant — downstream control flow matches the type, never the
    /// message string.
    #[tokio::test]
    async fn closed_input_channel_maps_to_the_dedicated_variant() {
        let (sender, receiver) = flume::unbounded::<Envelope>();
        drop(sender);
        let (_, result) = recv_envelope(0, receiver).await;
        assert!(matches!(result, Err(Error::InputChannelClosed)));
    }

    // ---------- source loop: cancellation during reconnect backoff ----------

    /// Cancelling the chain while a failed reconnect sleeps in its backoff
    /// must take the clean shutdown path, not wait out the five-second sleep.
    #[tokio::test(start_paused = true)]
    async fn cancellation_during_reconnect_backoff_shuts_the_source_down() {
        let mut chain = Chain::for_pool_test(1, vec![]);
        chain.task_ids = vec!["src".into()];
        chain.source = Some(ScriptSource::with_steps(vec![SourceStep::Disconnection]));
        let token = CancellationToken::new();
        let task = tokio::spawn(run_chain(chain, CheckpointHook::default(), token.clone()));
        // Let the chain park inside the reconnect backoff sleep, then cancel
        // well before the (fake) five-second timer can fire.
        tokio::time::sleep(Duration::from_millis(1)).await;
        token.cancel();
        let result = settle(task).await;
        assert!(result.is_ok(), "{result:?}");
    }

    // ---------- source loop: event-time gate failures ----------

    fn poisoned_gate() -> (Arc<std::sync::Mutex<WatermarkTracker>>, EventTimeGate) {
        let tracker = Arc::new(std::sync::Mutex::new(
            WatermarkTracker::from_time_spec(&event_time_spec()).unwrap(),
        ));
        let gate = EventTimeGate::new_with_shared_tracker(
            &event_time_spec(),
            vec![WindowTiming::Tumbling { size_ms: 1_000 }],
            tracker.clone(),
        )
        .unwrap();
        let poison = tracker.clone();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = poison.lock().unwrap();
            panic!("poison the shared watermark tracker");
        }));
        (tracker, gate)
    }

    #[tokio::test]
    async fn a_poisoned_event_time_tracker_fails_the_source_chain_at_the_idle_tick() {
        let (_tracker, gate) = poisoned_gate();
        let mut chain = Chain::for_pool_test(1, vec![]);
        chain.task_ids = vec!["src".into()];
        chain.source_time = Some(event_time_spec());
        chain.source = Some(ScriptSource::idle());
        let hook = CheckpointHook {
            event_time_gate: Arc::new(tokio::sync::Mutex::new(Some(gate))),
            partition: Some(0),
            ..Default::default()
        };
        let error = settle(tokio::spawn(run_chain(
            chain,
            hook,
            CancellationToken::new(),
        )))
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("tracker lock is poisoned"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_poisoned_gate_flush_failure_during_cancellation_is_tolerated() {
        let (_tracker, gate) = poisoned_gate();
        let mut chain = Chain::for_pool_test(1, vec![]);
        chain.task_ids = vec!["src".into()];
        chain.source_time = Some(event_time_spec());
        chain.source = Some(ScriptSource::idle());
        let hook = CheckpointHook {
            event_time_gate: Arc::new(tokio::sync::Mutex::new(Some(gate))),
            ..Default::default()
        };
        let token = CancellationToken::new();
        token.cancel();
        let result = settle(tokio::spawn(run_chain(chain, hook, token))).await;
        assert!(result.is_ok(), "{result:?}");
    }

    /// Held gate rows flushed during cancellation whose dispatch fails are
    /// discarded (debug-logged), never surfaced as a chain failure.
    #[tokio::test]
    async fn held_rows_flushed_during_cancellation_ignore_dispatch_failures() {
        let mut gate = EventTimeGate::new(
            &event_time_spec(),
            vec![WindowTiming::Tumbling { size_ms: 60_000 }],
        )
        .unwrap();
        let probe = Arc::new(ProbeAck::default());
        gate.observe_with_ack(0, ts_batch(1_000), probe.clone())
            .unwrap();
        assert!(gate.has_held(), "the row must be held by its open window");
        let mut chain = Chain::for_pool_test(1, vec![Arc::new(FailingProcessor)]);
        chain.task_ids = vec!["src".into()];
        chain.source_time = Some(event_time_spec());
        chain.source = Some(ScriptSource::idle());
        let hook = CheckpointHook {
            event_time_gate: Arc::new(tokio::sync::Mutex::new(Some(gate))),
            ..Default::default()
        };
        let token = CancellationToken::new();
        token.cancel();
        let result = settle(tokio::spawn(run_chain(chain, hook, token))).await;
        assert!(result.is_ok(), "{result:?}");
        assert!(probe.aborts.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn aborting_held_gate_acknowledgements_reports_the_first_failure() {
        let mut gate = EventTimeGate::new(
            &event_time_spec(),
            vec![WindowTiming::Tumbling { size_ms: 60_000 }],
        )
        .unwrap();
        let probe = Arc::new(ProbeAck::default());
        probe.fail_abort.store(true, Ordering::SeqCst);
        gate.observe_with_ack(0, ts_batch(1_000), probe.clone())
            .unwrap();
        let hook = CheckpointHook {
            event_time_gate: Arc::new(tokio::sync::Mutex::new(Some(gate))),
            ..Default::default()
        };
        let error = abort_event_time_held(&hook).await.unwrap_err();
        assert!(error.to_string().contains("abort failed"), "{error}");
    }

    // ---------- interior loop: worker pool supervision ----------

    /// A fatal worker failure (acknowledgement failure on a consumed batch)
    /// surfaces through the chain's failure arm, and the pool's cleanup error
    /// (drain observes the recorded failure flag) is reported through the
    /// bounded flush fence first.
    #[tokio::test(start_paused = true)]
    async fn a_fatal_worker_failure_fails_the_idle_tick_fence_and_the_chain() {
        let (chain, senders) =
            interior_chain(2, vec![Arc::new(NoneProcessor { close_fails: false })], 1);
        let probe = Arc::new(ProbeAck::default());
        probe.fail_ack.store(true, Ordering::SeqCst);
        senders[0]
            .send_async(Envelope::Data(ts_batch(1), probe.clone()))
            .await
            .unwrap();
        let token = CancellationToken::new();
        let task = tokio::spawn(run_chain(chain, CheckpointHook::default(), token));
        let error = settle(task).await.unwrap_err();
        assert!(
            error.to_string().contains("failed before control fence")
                || error.to_string().contains("ack failed"),
            "the fatal failure must fail the chain: {error}"
        );
        assert!(probe.aborts.load(Ordering::SeqCst) >= 1);
    }

    /// Cancellation while a worker sits on a gated delivery: the shutdown
    /// path's `cancel_and_join` observes the fatal failure recorded just
    /// before the worker exits, and the queued-but-unsubmitted delivery's
    /// failing abort is only debug-logged.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_with_a_wedged_pool_tolerates_cleanup_failures() {
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let held = gate.clone().lock_owned().await;
        let processor = GatedScriptProcessor::new(
            gate,
            1,
            Some(Ok(ProcessResult::None)),
            Some(Ok(ProcessResult::None)),
        );
        let (chain, mut senders) = interior_chain(2, vec![processor], 1);
        let wedged = Arc::new(ProbeAck::default());
        wedged.fail_ack.store(true, Ordering::SeqCst);
        senders[0]
            .send_async(Envelope::Data(ts_batch(1), wedged.clone()))
            .await
            .unwrap();
        let token = CancellationToken::new();
        let task = tokio::spawn(run_chain(chain, CheckpointHook::default(), token.clone()));
        tokio::time::sleep(Duration::from_millis(200)).await;
        // Close the inputs first so the shutdown drain of queued envelopes
        // terminates; the wedged worker is then released into a fatal
        // acknowledgement failure whose cleanup error must be tolerated.
        drop(senders.swap_remove(0));
        token.cancel();
        drop(held);
        let result = settle(task).await;
        assert!(
            result.is_ok(),
            "cleanup failures must not fail shutdown: {result:?}"
        );
    }

    /// Shutdown with envelopes still queued on the inbound channels: each is
    /// aborted (not acknowledged), and a failing abort is only debug-logged.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_envelopes_are_aborted_on_cancellation() {
        let (chain, mut senders) = interior_chain(1, vec![], 1);
        let queued = Arc::new(ProbeAck::default());
        queued.fail_abort.store(true, Ordering::SeqCst);
        senders[0]
            .send_async(Envelope::Data(ts_batch(1), queued.clone()))
            .await
            .unwrap();
        drop(senders.swap_remove(0));
        let token = CancellationToken::new();
        token.cancel();
        let result = settle(tokio::spawn(run_chain(
            chain,
            CheckpointHook::default(),
            token,
        )))
        .await;
        assert!(result.is_ok(), "{result:?}");
    }

    /// End of stream on a pooled chain drains the worker pool in order: the
    /// collector sees its result channel disconnect and flushes whatever is
    /// left before the terminal EOS flows downstream.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn channel_close_eos_drains_the_pool_in_order_and_finishes() {
        let (tx, rx) = flume::bounded::<Envelope>(64);
        let (mut chain, mut senders) = interior_chain(
            2,
            vec![GatedScriptProcessor::passthrough(
                Arc::new(tokio::sync::Mutex::new(())),
                1,
            )],
            1,
        );
        chain
            .outputs
            .insert("m".into(), vec![EdgeTarget::Forward(tx)]);
        let first = Arc::new(ProbeAck::default());
        let second = Arc::new(ProbeAck::default());
        senders[0]
            .send_async(Envelope::Data(ts_batch(1), first.clone()))
            .await
            .unwrap();
        senders[0]
            .send_async(Envelope::Data(ts_batch(2), second.clone()))
            .await
            .unwrap();
        drop(senders.swap_remove(0));
        let result = settle(tokio::spawn(run_chain(
            chain,
            CheckpointHook::default(),
            CancellationToken::new(),
        )))
        .await;
        assert!(result.is_ok(), "{result:?}");
        assert!(matches!(rx.try_recv(), Ok(Envelope::Data(_, _))));
        assert!(matches!(rx.try_recv(), Ok(Envelope::Data(_, _))));
        assert!(matches!(rx.try_recv(), Ok(Envelope::Eos)));
        // The source acknowledgements settle only when the downstream
        // consumer acknowledges its fan-out branches, which this test does
        // not model.
        drop((first, second));
    }

    /// The collector's shutdown join is bounded: a collector stuck flushing
    /// into a full, unwatched edge must fail the chain after the bound
    /// instead of parking the EOS path forever. Paused time exercises the
    /// real 30-second bound instantly.
    #[tokio::test(start_paused = true)]
    async fn a_collector_stuck_on_a_full_edge_fails_the_drain_within_its_bound() {
        let (tx, _undrained_rx) = flume::bounded::<Envelope>(1);
        let (mut chain, mut senders) = interior_chain(
            2,
            vec![GatedScriptProcessor::passthrough(
                Arc::new(tokio::sync::Mutex::new(())),
                1,
            )],
            1,
        );
        chain
            .outputs
            .insert("m".into(), vec![EdgeTarget::Forward(tx)]);
        for value in 1..=3 {
            senders[0]
                .send_async(Envelope::Data(
                    ts_batch(value),
                    Arc::new(ProbeAck::default()),
                ))
                .await
                .unwrap();
        }
        drop(senders.swap_remove(0));
        let error = tokio::time::timeout(
            4 * BUDGET,
            run_chain(chain, CheckpointHook::default(), CancellationToken::new()),
        )
        .await
        .expect("the bounded drain fails the chain")
        .unwrap_err();
        // Either bound proves the contract: the collector's join times out
        // when the EOS drain reached it first, or the control fence times
        // out when the idle tick fenced the stuck pool first.
        assert!(
            error
                .to_string()
                .contains("did not finish draining within 30s")
                || error.to_string().contains("did not publish delivery"),
            "{error}"
        );
    }

    /// Workers publishing into a collector that already exited (cancellation)
    /// settle their results through the abort path; a processor failure is
    /// aborted exactly like ordinary output.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_collector_aborts_a_pending_processor_failure() {
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let held = gate.clone().lock_owned().await;
        let failure = || Error::Process("pooled operator failure".into());
        let processor =
            GatedScriptProcessor::new(gate.clone(), 1, Some(Err(failure())), Some(Err(failure())));
        let (chain, _senders) = interior_chain(2, vec![processor], 1);
        let token = CancellationToken::new();
        let pool = ProcessorWorkerPool::start(&chain, &CheckpointHook::default(), &token)
            .expect("pooled chain");
        let first = Arc::new(ProbeAck::default());
        let second = Arc::new(ProbeAck::default());
        pool.submit((ts_batch(1), first.clone())).await.unwrap();
        pool.submit((ts_batch(2), second.clone())).await.unwrap();
        // Sequence 1 fails fast and parks in the collector's reorder buffer
        // (sequence 0 is still gated); cancellation then aborts it.
        tokio::time::sleep(Duration::from_millis(200)).await;
        token.cancel();
        // Let the collector observe cancellation (aborting the buffered
        // sequence) before the gated sequence publishes into the void.
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(held);
        pool.cancel_and_join()
            .await
            .expect("cancellation drains the pool");
        assert!(first.aborts.load(Ordering::SeqCst) >= 1);
        assert!(second.aborts.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_collector_aborts_pending_outputs() {
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let held = gate.clone().lock_owned().await;
        let processor = GatedScriptProcessor::passthrough(gate, 1);
        let (chain, _senders) = interior_chain(2, vec![processor], 1);
        let token = CancellationToken::new();
        let pool = ProcessorWorkerPool::start(&chain, &CheckpointHook::default(), &token)
            .expect("pooled chain");
        let first = Arc::new(ProbeAck::default());
        let second = Arc::new(ProbeAck::default());
        pool.submit((ts_batch(1), first.clone())).await.unwrap();
        pool.submit((ts_batch(2), second.clone())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        token.cancel();
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(held);
        pool.cancel_and_join()
            .await
            .expect("cancellation drains the pool");
        assert!(first.aborts.load(Ordering::SeqCst) >= 1);
        assert!(second.aborts.load(Ordering::SeqCst) >= 1);
    }

    /// A fatal worker failure surfaces through the chain loop's failure arm:
    /// the loop consumes the failure report, and the cleanup drain then sees
    /// only the recorded failure flag (the message itself is spent).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_fatal_worker_failure_surfaces_through_the_failure_arm() {
        let (chain, mut senders) =
            interior_chain(2, vec![Arc::new(NoneProcessor { close_fails: false })], 1);
        let probe = Arc::new(ProbeAck::default());
        probe.fail_ack.store(true, Ordering::SeqCst);
        senders[0]
            .send_async(Envelope::Data(ts_batch(1), probe.clone()))
            .await
            .unwrap();
        drop(senders.swap_remove(0));
        let error = settle(tokio::spawn(run_chain(
            chain,
            CheckpointHook::default(),
            CancellationToken::new(),
        )))
        .await
        .unwrap_err();
        // The same fatal ack failure races to be observed first through one of
        // two surfaces: the chain loop's failure arm ("ack failed") or the
        // control fence noticing the pool's failure flag. Scheduler order
        // decides, so both are valid proofs that the failure surfaced.
        let message = error.to_string();
        assert!(
            message.contains("ack failed")
                || message.contains("worker pool failed before control fence"),
            "the fatal failure must surface: {error}"
        );
        assert!(probe.aborts.load(Ordering::SeqCst) >= 1);
    }

    /// Deliveries still queued when the pool is cancelled are aborted by the
    /// drain instead of leaking their acknowledgements.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_pool_deliveries_are_aborted_by_the_drain() {
        let processor = GatedScriptProcessor::passthrough(Arc::new(tokio::sync::Mutex::new(())), 1);
        let (chain, _senders) = interior_chain(2, vec![processor], 1);
        let token = CancellationToken::new();
        let pool = ProcessorWorkerPool::start(&chain, &CheckpointHook::default(), &token)
            .expect("pooled chain");
        // Cancel first: the workers exit without claiming, so the submitted
        // delivery stays in the work queue for the drain to abort.
        token.cancel();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let queued = Arc::new(ProbeAck::default());
        pool.submit((ts_batch(1), queued.clone())).await.unwrap();
        pool.cancel_and_join()
            .await
            .expect("the drained pool reports no failure");
        assert!(queued.aborts.load(Ordering::SeqCst) >= 1);
    }

    /// The control fence reports a pool that already recorded a fatal failure
    /// instead of waiting for its publication deadline.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_control_fence_reports_an_already_failed_pool() {
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let held = gate.clone().lock_owned().await;
        let processor = GatedScriptProcessor::new(
            gate,
            1,
            Some(Ok(ProcessResult::None)),
            Some(Ok(ProcessResult::None)),
        );
        let (chain, _senders) = interior_chain(2, vec![processor], 1);
        let token = CancellationToken::new();
        let pool = ProcessorWorkerPool::start(&chain, &CheckpointHook::default(), &token)
            .expect("pooled chain");
        let probe = Arc::new(ProbeAck::default());
        probe.fail_ack.store(true, Ordering::SeqCst);
        pool.submit((ts_batch(1), probe.clone())).await.unwrap();
        drop(held);
        tokio::time::timeout(BUDGET, async {
            while !pool.failed.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the pool records the fatal failure");
        let error = pool.flush().await.unwrap_err();
        assert!(
            error.to_string().contains("failed before control fence"),
            "{error}"
        );
        token.cancel();
        let _ = pool.cancel_and_join().await;
    }

    // ---------- interior loop: watermark late-row surfacing ----------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn window_late_row_counters_surface_into_the_kernel_metrics() {
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (mut chain, mut senders) = interior_chain(1, vec![], 1);
        chain.window_late_event_rows = Some(counter.clone());
        let metrics = Arc::new(crate::runtime::RuntimeMetrics::default());
        let hook = CheckpointHook {
            metrics: Some(metrics.clone()),
            ..Default::default()
        };
        let token = CancellationToken::new();
        let task = tokio::spawn(run_chain(chain, hook, token));
        tokio::time::sleep(Duration::from_millis(50)).await;
        counter.store(7, Ordering::SeqCst);
        tokio::time::timeout(BUDGET, async {
            while metrics.kernel.late_events.load(Ordering::SeqCst) < 7 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the delta surfaces through the idle tick");
        drop(senders.swap_remove(0));
        let result = settle(task).await;
        assert!(result.is_ok(), "{result:?}");
    }

    // ---------- interior loop: alignment ----------

    /// A channel that closes while another input still aligns a barrier lets
    /// the EOS complete the alignment; a released buffer that ends every
    /// input finishes the chain through the ordered EOS path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closed_inputs_complete_alignment_and_finish_the_chain() {
        let (tx, rx) = flume::bounded::<Envelope>(64);
        let seen = Arc::new(AtomicUsize::new(0));
        let (mut chain, mut senders) = interior_chain(
            2,
            vec![Arc::new(RetainAckProcessor { seen: seen.clone() })],
            2,
        );
        chain
            .outputs
            .insert("m".into(), vec![EdgeTarget::Forward(tx)]);
        let token = CancellationToken::new();
        let task = tokio::spawn(run_chain(chain, CheckpointHook::default(), token));
        // Input 0: one data envelope, then the barrier, then the producer
        // drops; input 1 closes without a barrier. The data envelope's
        // processing is the deterministic ordering signal.
        senders[0]
            .send_async(Envelope::Data(ts_batch(1), probe()))
            .await
            .unwrap();
        tokio::time::timeout(BUDGET, async {
            while seen.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the pre-barrier data is processed first");
        senders[0].send_async(barrier("c-2", 2)).await.unwrap();
        drop(senders.swap_remove(0));
        // Input 0's channel-close surfaces as an EOS buffered behind the
        // in-flight barrier; input 1's close then completes the alignment and
        // the released EOS pair ends the chain.
        tokio::time::sleep(Duration::from_millis(150)).await;
        drop(senders.pop().unwrap());
        let result = settle(task).await;
        assert!(result.is_ok(), "{result:?}");
        let mut saw_barrier = false;
        while let Ok(envelope) = rx.try_recv() {
            if matches!(envelope, Envelope::Barrier(_)) {
                saw_barrier = true;
            }
        }
        assert!(saw_barrier, "the aligned barrier must be forwarded");
    }

    /// Exceeding the alignment bound fails the round through the failure
    /// reporter, flushes the pool, and releases the buffered envelopes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn alignment_overflow_fails_the_round_and_releases_the_buffer() {
        let (tx, rx) = flume::bounded::<Envelope>(8192);
        let (mut chain, mut senders) = interior_chain(
            2,
            vec![GatedScriptProcessor::passthrough(
                Arc::new(tokio::sync::Mutex::new(())),
                1,
            )],
            2,
        );
        chain
            .outputs
            .insert("m".into(), vec![EdgeTarget::Forward(tx)]);
        let (fail_tx, mut fail_rx) = tokio::sync::mpsc::unbounded_channel();
        let hook = CheckpointHook {
            failure_reporter: Some(fail_tx),
            ..Default::default()
        };
        // Input 1 aligns a barrier; input 0 floods enough data to exceed the
        // 1024-envelope bound regardless of the reader interleaving.
        senders[1].send_async(barrier("c-3", 3)).await.unwrap();
        for value in 0..1_100 {
            senders[0]
                .send_async(Envelope::Data(
                    ts_batch(value),
                    Arc::new(ProbeAck::default()),
                ))
                .await
                .unwrap();
        }
        let task = tokio::spawn(run_chain(chain, hook, CancellationToken::new()));
        let report = tokio::time::timeout(BUDGET, fail_rx.recv())
            .await
            .expect("the overflow reports a checkpoint failure")
            .expect("reporter alive");
        assert!(report.to_string().contains("exceeded the"), "{report}");
        drop(senders.swap_remove(0));
        drop(senders.pop().unwrap());
        let result = settle(task).await;
        assert!(result.is_ok(), "{result:?}");
        let mut datas = 0;
        while let Ok(envelope) = rx.try_recv() {
            if matches!(envelope, Envelope::Data(_, _)) {
                datas += 1;
            }
        }
        assert!(datas >= 1_000, "released envelopes must flow: {datas}");
    }

    // ---------- dispatch helpers ----------

    fn probe() -> Arc<ProbeAck> {
        Arc::new(ProbeAck::default())
    }

    fn ready_slice(
        action: crate::event_time::WindowAction,
    ) -> (crate::MessageBatchRef, crate::event_time::WindowAction) {
        (ts_batch(1), action)
    }

    fn session_chain(processor: Arc<dyn Processor>) -> Chain {
        let mut chain = Chain::for_pool_test(1, vec![processor]);
        chain.task_ids = vec!["m".into()];
        chain.window_timings = vec![WindowTiming::Session { gap_ms: 100 }];
        chain
    }

    #[tokio::test]
    async fn dynamic_sessions_forward_the_watermark_before_the_current_slice() {
        let (tx, rx) = flume::bounded::<Envelope>(8);
        let mut chain = session_chain(Arc::new(NoneProcessor { close_fails: false }));
        chain
            .outputs
            .insert("m".into(), vec![EdgeTarget::Forward(tx)]);
        dispatch_gate_outputs(
            &chain,
            &CheckpointHook::default(),
            vec![ready_slice(crate::event_time::WindowAction::Emit)],
            vec![probe()],
            vec![false],
            vec![],
            Some(1_000),
            0,
            true,
            Some(0),
        )
        .await
        .unwrap();
        assert!(matches!(rx.try_recv(), Ok(Envelope::Watermark(1_000))));

        // The same insertion on a closed edge fails the whole dispatch and
        // aborts every acknowledgement it still owns.
        let (dead_tx, dead_rx) = flume::bounded::<Envelope>(1);
        drop(dead_rx);
        let mut chain = session_chain(Arc::new(FailingProcessor));
        chain
            .outputs
            .insert("m".into(), vec![EdgeTarget::Forward(dead_tx)]);
        let ack = probe();
        let error = dispatch_gate_outputs(
            &chain,
            &CheckpointHook::default(),
            vec![ready_slice(crate::event_time::WindowAction::Emit)],
            vec![ack.clone()],
            vec![false],
            vec![],
            Some(1_000),
            0,
            true,
            Some(0),
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("downstream channel closed"),
            "{error}"
        );
        assert!(ack.aborts.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn dynamic_sessions_append_the_tail_watermark_past_the_last_slice() {
        let (tx, rx) = flume::bounded::<Envelope>(8);
        let mut chain = session_chain(Arc::new(NoneProcessor { close_fails: false }));
        chain
            .outputs
            .insert("m".into(), vec![EdgeTarget::Forward(tx)]);
        dispatch_gate_outputs(
            &chain,
            &CheckpointHook::default(),
            vec![],
            vec![],
            vec![],
            vec![],
            Some(2_000),
            0,
            true,
            Some(0),
        )
        .await
        .unwrap();
        assert!(matches!(rx.try_recv(), Ok(Envelope::Watermark(2_000))));

        let (dead_tx, dead_rx) = flume::bounded::<Envelope>(1);
        drop(dead_rx);
        let mut chain = session_chain(Arc::new(FailingProcessor));
        chain
            .outputs
            .insert("m".into(), vec![EdgeTarget::Forward(dead_tx)]);
        let error = dispatch_gate_outputs(
            &chain,
            &CheckpointHook::default(),
            vec![],
            vec![],
            vec![],
            vec![],
            Some(2_000),
            0,
            true,
            Some(0),
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("downstream channel closed"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_failing_gated_slice_compensates_published_and_dropped_acknowledgements() {
        let chain = session_chain(Arc::new(FailingProcessor));
        let ready = probe();
        let dropped = probe();
        let error = dispatch_gate_outputs(
            &chain,
            &CheckpointHook::default(),
            vec![ready_slice(crate::event_time::WindowAction::Emit)],
            vec![ready.clone()],
            vec![false],
            vec![dropped.clone()],
            None,
            0,
            true,
            None,
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("injected processor failure"),
            "{error}"
        );
        assert!(ready.aborts.load(Ordering::SeqCst) >= 1);
        assert!(dropped.aborts.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn barrier_spans_parent_onto_a_remote_trace_context() {
        let chain = interior_chain(1, vec![], 0).0;
        let mut with_context = crate::checkpoint::CheckpointBarrier {
            checkpoint_id: "c-9".into(),
            generation: 9,
            trace_context: Some("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01".into()),
        };
        let _span = barrier_span(&chain, &with_context);
        with_context.trace_context = Some("garbage".into());
        let _span = barrier_span(&chain, &with_context);
        with_context.trace_context = None;
        let _span = barrier_span(&chain, &with_context);
    }

    // ---------- process_chain / process_generated_batches arms ----------

    fn simple_chain(processor: Arc<dyn Processor>) -> Chain {
        let mut chain = Chain::for_pool_test(1, vec![processor]);
        chain.task_ids = vec!["m".into()];
        chain
    }

    #[tokio::test]
    async fn an_empty_multiple_result_with_a_failing_ack_is_fatal() {
        struct EmptyMultiple;
        #[async_trait]
        impl Processor for EmptyMultiple {
            async fn process(
                &self,
                _batch: crate::MessageBatchRef,
            ) -> Result<ProcessResult, Error> {
                Ok(ProcessResult::Multiple(Vec::new()))
            }
            async fn close(&self) -> Result<(), Error> {
                Ok(())
            }
        }
        let chain = simple_chain(Arc::new(EmptyMultiple));
        let ack = probe();
        ack.fail_ack.store(true, Ordering::SeqCst);
        let error = dispatch_data(&chain, ts_batch(1), ack.clone(), None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("ack failed"), "{error}");
    }

    #[tokio::test]
    async fn a_none_result_with_a_failing_ack_is_fatal() {
        let chain = simple_chain(Arc::new(NoneProcessor { close_fails: false }));
        let ack = probe();
        ack.fail_ack.store(true, Ordering::SeqCst);
        let error = dispatch_data(&chain, ts_batch(1), ack.clone(), None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("ack failed"), "{error}");
    }

    #[tokio::test]
    async fn multiple_with_ack_results_reuse_their_replacement_acknowledgements() {
        struct Replacing;
        #[async_trait]
        impl Processor for Replacing {
            async fn process(&self, batch: crate::MessageBatchRef) -> Result<ProcessResult, Error> {
                let replacement = probe();
                Ok(ProcessResult::MultipleWithAck(vec![(batch, replacement)]))
            }
            async fn close(&self) -> Result<(), Error> {
                Ok(())
            }
        }
        let chain = simple_chain(Arc::new(Replacing));
        dispatch_data(&chain, ts_batch(1), probe(), None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn processor_errors_bump_the_runtime_and_chain_metrics() {
        let chain = simple_chain(Arc::new(FailingProcessor));
        let metrics = Arc::new(crate::runtime::RuntimeMetrics::default());
        let error = dispatch_data(&chain, ts_batch(1), probe(), Some(&metrics))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("injected processor failure"),
            "{error}"
        );
        assert_eq!(metrics.processing_errors.load(Ordering::SeqCst), 1);
        let chain_metrics = metrics.kernel.chain("m");
        assert_eq!(chain_metrics.errors.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_sink_acknowledgement_failure_after_a_successful_write_is_fatal() {
        let mut chain = Chain::for_pool_test(1, vec![]);
        chain.task_ids = vec!["m".into()];
        chain.sink = Some(Arc::new(NullOutput));
        let ack = probe();
        ack.fail_ack.store(true, Ordering::SeqCst);
        let error = dispatch_data(&chain, ts_batch(1), ack.clone(), None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("ack failed"), "{error}");
    }

    #[tokio::test]
    async fn generated_single_results_carry_their_acknowledgement_forward() {
        struct SingleOnce {
            used: AtomicBool,
        }
        #[async_trait]
        impl Processor for SingleOnce {
            async fn process(&self, batch: crate::MessageBatchRef) -> Result<ProcessResult, Error> {
                assert!(!self.used.swap(true, Ordering::SeqCst));
                Ok(ProcessResult::Single(batch))
            }
            async fn close(&self) -> Result<(), Error> {
                Ok(())
            }
        }
        let processor: Arc<dyn Processor> = Arc::new(SingleOnce {
            used: AtomicBool::new(false),
        });
        let ack = probe();
        let outputs = process_generated_batches(
            &processor,
            vec![ProcessedBatch {
                batch: ts_batch(1),
                ack: ack.clone(),
            }],
        )
        .await
        .unwrap();
        assert_eq!(outputs.len(), 1);
        outputs[0].ack.ack().await.unwrap();
        assert_eq!(ack.acks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn process_chain_errors_convert_into_fatal_failures() {
        let error = ProcessChainError::from(Error::Process("conversion".into()));
        match error {
            ProcessChainError::Fatal(failure) => {
                assert!(failure.error.to_string().contains("conversion"));
                assert!(failure.acknowledgements.is_empty());
            }
            ProcessChainError::Processor(_) => panic!("plain errors are fatal, not routed"),
        }
    }

    // ---------- failure routing / compensation ----------

    fn processor_failure_with(ack: Arc<dyn Ack>) -> ProcessorFailure {
        ProcessorFailure {
            error: Error::Process("operator failed".into()),
            batch: ts_batch(1),
            ack,
            siblings: Vec::new(),
            failed_task_id: "m".into(),
        }
    }

    #[tokio::test]
    async fn aborting_a_pooled_failure_with_a_failing_abort_reports_both() {
        let chain = simple_chain(Arc::new(NoneProcessor { close_fails: false }));
        let ack = probe();
        ack.fail_abort.store(true, Ordering::SeqCst);
        let error = flush_pool_result(
            &chain,
            PoolResult::ProcessorFailure(processor_failure_with(ack)),
            &BTreeMap::new(),
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("operator failed")
                && error.to_string().contains("acknowledgement abort failed"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn aborting_a_fatal_failure_with_a_failing_abort_reports_both() {
        let ack = probe();
        ack.fail_abort.store(true, Ordering::SeqCst);
        let error = abort_fatal_failure(FatalFailure {
            error: Error::Process("fatal".into()),
            acknowledgements: vec![ack],
        })
        .await;
        assert!(
            error.to_string().contains("fatal")
                && error.to_string().contains("acknowledgement abort failed"),
            "{error}"
        );
    }

    // ---------- edge routing ----------

    #[tokio::test]
    async fn partition_mismatch_errors_propagate_from_data_routing() {
        let (tx0, _rx0) = flume::bounded::<Envelope>(1);
        let (tx1, _rx1) = flume::bounded::<Envelope>(1);
        let error = send_to_targets(
            Some(&vec![EdgeTarget::Partitioned {
                channels: vec![tx0, tx1],
                key_field: "ts".into(),
                key_group_ranges: vec![crate::job::KeyGroupRange { start: 0, end: 15 }],
                max_parallelism: 16,
            }]),
            Envelope::Data(ts_batch(1), probe()),
            true,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("2 channels but 1 key-group ranges"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_single_channel_partitioned_edge_routes_without_hashing() {
        let (tx, rx) = flume::bounded::<Envelope>(4);
        let probe = Arc::new(ProbeAck::default());
        send_to_targets(
            Some(&vec![EdgeTarget::Partitioned {
                channels: vec![tx],
                key_field: "absent".into(),
                key_group_ranges: vec![crate::job::KeyGroupRange { start: 0, end: 15 }],
                max_parallelism: 16,
            }]),
            Envelope::Data(ts_batch(1), probe.clone()),
            true,
        )
        .await
        .unwrap();
        assert!(matches!(rx.try_recv(), Ok(Envelope::Data(_, _))));
    }

    #[test]
    fn routing_skips_subtasks_whose_key_group_range_is_empty() {
        let batch = crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)])),
                vec![Arc::new(datafusion::arrow::array::StringArray::from(vec![
                    "alpha",
                ]))],
            )
            .unwrap(),
        );
        // The second range covers no key group: its subtask is skipped.
        let routed = partition_batch_by_key_hash(
            &batch,
            "key",
            2,
            &[
                crate::job::KeyGroupRange { start: 0, end: 15 },
                crate::job::KeyGroupRange { start: 1, end: 0 },
            ],
            16,
        )
        .unwrap();
        assert!(routed[0].is_some());
        assert!(routed[1].is_none());
    }

    #[test]
    fn hash_column_covers_the_remaining_null_sentinels() {
        let max_parallelism = 16u32;
        let group = |bytes: &[u8]| crate::job::key_group_for_key(bytes, max_parallelism).unwrap();
        let small_ints =
            hash_column(&Int8Array::from(vec![Some(1i8), None]), max_parallelism).unwrap();
        assert_eq!(small_ints[0].unwrap(), group(&1i8.to_be_bytes()));
        assert_eq!(small_ints[1].unwrap(), group(b"null:i8"));

        let binaries = hash_column(
            &BinaryArray::from_opt_vec(vec![Some(&b"b"[..]), None]),
            max_parallelism,
        )
        .unwrap();
        assert_eq!(binaries[0].unwrap(), group(b"b"));
        assert_eq!(binaries[1].unwrap(), group(b"null:binary"));
    }
}
