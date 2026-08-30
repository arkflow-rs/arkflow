//! Per-chain event loops: the pipelined execution core.
//!
//! Each chain runs its own task. Source chains pull from their input, run the
//! chain's processors, and push envelopes downstream; interior/sink chains
//! receive from their inbound channels. Because every chain runs concurrently
//! and edges are bounded, a slow consumer backpressures its producer while
//! other chains keep flowing.

use super::envelope::Envelope;
use super::graph::{Chain, EdgeTarget, ExecutionGraph};
use crate::input::{fanout_ack, Input};
use crate::output::Output;
use crate::Error;
use crate::ProcessResult;
use datafusion::arrow::array::{
    Array, BinaryArray, Int16Array, Int32Array, Int64Array, Int8Array, StringArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

/// Upper bound on how long a source chain waits for in-flight (non-held)
/// acknowledgements to drain before sealing a checkpoint cut. Exceeding it
/// fails the barrier round (the last valid checkpoint is retained) instead of
/// blocking the source indefinitely behind a wedged sink.
const BARRIER_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

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
        tasks.push(tokio::spawn(
            async move { run_chain(chain, hook, token).await },
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
    let result = run_chain_inner(&chain, &hook, &cancellation).await;

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
                        }
                    }
                }
                let positions = match source.current_positions().await {
                    Ok(positions) => positions,
                    Err(error) => {
                        if let Some(reporter) = &hook.failure_reporter {
                            let _ = reporter.send(error);
                        }
                        Vec::new()
                    }
                };
                let watermark_ms = hook
                    .event_time_gate
                    .lock()
                    .await
                    .as_ref()
                    .and_then(super::event_time_gate::EventTimeGate::watermark);
                // Seal the acknowledged cut before the barrier flows
                // downstream. The source loop is single-threaded: the cut's
                // positions and watermark are frozen at this instant and the
                // report cannot observe acknowledgements completing after
                // the seal. Pre-cut outputs with pending acknowledgements
                // stay outside the cut; recovery replays them from the
                // sealed positions.
                frontier.seed(&positions);
                let cut = frontier.seal(watermark_ms);
                send_downstream(chain, Envelope::Barrier(barrier.clone())).await?;
                spawn_barrier_snapshot(
                    hook,
                    barrier,
                    cut,
                    None,
                    chain.entry_task_id().to_owned(),
                );
                continue;
            }
            _ = idle_tick.tick() => {
                // Idle tick: refresh held rows (idle partitions unblock the
                // watermark) and fire acks that are no longer deferred.
                let (ready, ready_acks, dropped_acks, watermark) = {
                    let mut guard = event_gate.lock().await;
                    match guard.as_mut() {
                        None => (Vec::new(), Vec::new(), Vec::new(), None),
                        Some(gate) => {
                            let decision = gate.refresh()?;
                            (
                                decision.ready,
                                decision.ready_acks,
                                decision.dropped_acks,
                                decision.watermark_ms,
                            )
                        }
                    }
                };
                record_watermark_lag(hook, watermark);
                for ((batch, action), ack) in ready.into_iter().zip(ready_acks) {
                    dispatch_gated(chain, hook, batch, action, ack).await?;
                }
                for ack in dropped_acks {
                    ack.ack().await?;
                }
                if let Some(watermark) = watermark {
                    send_downstream(chain, Envelope::Watermark(watermark)).await?;
                }
                continue;
            }
            result = source.read() => match result {
                Ok(read) => read,
                Err(Error::EOF) => {
                    let (ready, ready_acks, dropped_acks, watermark) = {
                        let mut guard = event_gate.lock().await;
                        match guard.as_mut() {
                            None => (Vec::new(), Vec::new(), Vec::new(), None),
                            Some(gate) => {
                                let decision = gate.finish();
                                (
                                    decision.ready,
                                    decision.ready_acks,
                                    decision.dropped_acks,
                                    decision.watermark_ms,
                                )
                            }
                        }
                    };
                    for ((batch, action), ack) in ready.into_iter().zip(ready_acks) {
                        dispatch_gated(chain, hook, batch, action, ack).await?;
                    }
                    for ack in dropped_acks {
                        ack.ack().await?;
                    }
                    if let Some(watermark) = watermark {
                        send_downstream(chain, Envelope::Watermark(watermark)).await?;
                    }
                    // Preserve the explicit EOS contract for downstream
                    // vertices. Channel closure remains the fallback for
                    // consumers that only use bounded-source completion.
                    send_downstream(chain, Envelope::Eos).await?;
                    return Ok(());
                }
                Err(Error::Disconnection) => {
                    if let Some(metrics) = &hook.metrics {
                        metrics
                            .input_errors
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    // Reconnect with the legacy backoff cadence.
                    loop {
                        match source.connect().await {
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
                                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
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
        let (batch, ack) = read;
        if let Some(metrics) = &hook.metrics {
            use std::sync::atomic::Ordering;
            metrics.input_batches.fetch_add(1, Ordering::Relaxed);
            metrics
                .input_messages
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
        }
        let ack: Arc<dyn crate::input::Ack> =
            Arc::new(super::commit::TrackingAck::new(tracker.clone(), ack));
        let event_time_enabled = event_gate.lock().await.is_some();
        match event_time_enabled {
            false => {
                dispatch_data(chain, batch, ack, hook.metrics.as_ref()).await?;
            }
            true => {
                let partition = hook.partition.unwrap_or(0);
                let (ready, ready_acks, dropped_acks, watermark) = {
                    let mut guard = event_gate.lock().await;
                    let gate = guard
                        .as_mut()
                        .expect("event-time gate disappeared while processing a source batch");
                    let decision = gate.observe_with_ack(partition, batch, ack)?;
                    (
                        decision.ready,
                        decision.ready_acks,
                        decision.dropped_acks,
                        decision.watermark_ms,
                    )
                };
                record_watermark_lag(hook, watermark);
                for ((slice, action), slice_ack) in ready.into_iter().zip(ready_acks) {
                    dispatch_gated(chain, hook, slice, action, slice_ack).await?;
                }
                for dropped in dropped_acks {
                    dropped.ack().await?;
                }
                if let Some(watermark) = watermark {
                    send_downstream(chain, Envelope::Watermark(watermark)).await?;
                }
            }
        }
    }
}

async fn shutdown_source_chain(chain: &Chain, hook: &CheckpointHook) -> Result<(), Error> {
    let (ready, ready_acks, dropped_acks) = {
        let mut guard = hook.event_time_gate.lock().await;
        match guard.as_mut() {
            Some(gate) => {
                let decision = gate.finish();
                (decision.ready, decision.ready_acks, decision.dropped_acks)
            }
            None => (Vec::new(), Vec::new(), Vec::new()),
        }
    };
    for ((batch, action), ack) in ready.into_iter().zip(ready_acks) {
        if let Err(error) = dispatch_gated(chain, hook, batch, action, ack).await {
            tracing::debug!(%error, task = chain.entry_task_id(), "discarding source output during cancellation");
        }
    }
    for ack in dropped_acks {
        if let Err(error) = ack.ack().await {
            tracing::debug!(%error, task = chain.entry_task_id(), "failed to acknowledge dropped source data during cancellation");
        }
    }
    if let Err(error) = send_downstream(chain, Envelope::Eos).await {
        tracing::debug!(%error, task = chain.entry_task_id(), "downstream already closed during source cancellation");
    }
    Ok(())
}

async fn shutdown_interior_chain(chain: &Chain) -> Result<(), Error> {
    // Do not acknowledge discarded data. The ownership of each Data envelope
    // remains with its Ack, allowing a durable source/WAL to replay it after a
    // cancelled run.
    for receiver in &chain.inputs {
        while receiver.recv_async().await.is_ok() {}
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

/// Dispatch one gated batch slice according to its window action. Route and
/// Update forward with their marker columns attached (legacy semantics);
/// Emit/Hold-equivalent rows flow through the chain normally.
async fn dispatch_gated(
    chain: &Chain,
    hook: &CheckpointHook,
    batch: crate::MessageBatchRef,
    action: crate::event_time::WindowAction,
    ack: Arc<dyn crate::input::Ack>,
) -> Result<(), Error> {
    if matches!(
        action,
        crate::event_time::WindowAction::Route | crate::event_time::WindowAction::Update
    ) {
        if let Some(metrics) = &hook.metrics {
            metrics
                .kernel
                .late_events
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    if action == crate::event_time::WindowAction::Route {
        let last_task = chain.task_ids.last().map(String::as_str).unwrap_or("");
        if let Some(targets) = chain.late_event_outputs.get(last_task) {
            let batch = mark_event_batch(batch, "__arkflow_late_event_route")?;
            return send_to_targets(Some(targets), Envelope::Data(batch, ack), true).await;
        }
        // A Route policy without a declared route target has no safe side
        // branch. Preserve the row on the main path as an Update instead of
        // silently acknowledging and dropping it.
        let batch = mark_event_batch(batch, "__arkflow_late_event_update")?;
        return dispatch_data(chain, batch, ack, hook.metrics.as_ref()).await;
    }
    let batch = match action {
        crate::event_time::WindowAction::Update => {
            mark_event_batch(batch, "__arkflow_late_event_update")?
        }
        _ => batch,
    };
    dispatch_data(chain, batch, ack, hook.metrics.as_ref()).await
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

/// Start an asynchronous barrier snapshot for a source chain. The cut is
/// sealed by the single-threaded event loop at the marker — positions and
/// watermark cannot race with the report — while the potentially blocking
/// state read stays detached so the data plane is never held behind a global
/// checkpoint lock.
fn spawn_barrier_snapshot(
    hook: &CheckpointHook,
    barrier: crate::checkpoint::CheckpointBarrier,
    cut: super::commit::CheckpointCut,
    task_id_override: Option<String>,
    default_task_id: String,
) {
    let Some(reporter) = hook.reporter.clone() else {
        return;
    };
    let failure_reporter = hook.failure_reporter.clone();
    let state_backend = hook.state.clone();
    let task_id = task_id_override
        .or_else(|| hook.task_id.clone())
        .unwrap_or(default_task_id);
    let attempt_id = format!("{task_id}-attempt");
    let partition = hook.partition.unwrap_or_default();
    tokio::spawn(async move {
        let state = match state_backend {
            Some(backend) => super::barrier::snapshot_state(backend).await,
            None => Ok(crate::state::StateSnapshot::new(1, Vec::new())),
        };
        match state {
            Ok(state) => {
                let _ = reporter.send(super::barrier::ChainSnapshot {
                    task_id,
                    attempt_id,
                    partition,
                    barrier,
                    cut_generation: cut.generation,
                    state,
                    source_positions: cut.positions,
                    watermark_ms: cut.watermark_ms,
                });
            }
            Err(error) => {
                if let Some(failure_reporter) = failure_reporter {
                    let _ = failure_reporter.send(error);
                }
            }
        }
    });
}

/// Interior/sink chain: consume inbound channels. Barriers align across the
/// chain's inputs (`Aligner`), snapshot the chain's state, and flow onward.
async fn run_interior_chain(
    chain: &Chain,
    hook: &CheckpointHook,
    cancellation: &CancellationToken,
) -> Result<(), Error> {
    let mut readers = FuturesUnordered::new();
    for (index, receiver) in chain.inputs.iter().enumerate() {
        readers.push(recv_envelope(index, receiver.clone()));
    }
    let mut aligner = super::barrier::Aligner::new(chain.inputs.len(), 1024);
    let mut ended_inputs = BTreeSet::new();
    // Watermarks arriving on different input edges represent independent
    // progress. Keep one value per edge and only forward the minimum once
    // every active input has reported; using the latest/max value would let a
    // fast partition close a window while a lagging partition can still
    // deliver data for it.
    let mut upstream_watermarks = BTreeMap::<usize, i64>::new();
    let mut idle_tick = tokio::time::interval(std::time::Duration::from_millis(100));
    idle_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Bounded, ordered processor worker pool (`pipeline.thread_num`): data
    // deliveries fan out to N workers while the chain's control flow
    // (barriers, watermarks, EOS, ticks) stays in this loop. A reorder
    // collector preserves per-delivery output order.
    let pool = ProcessorWorkerPool::start(chain, hook, cancellation);
    loop {
        let read = tokio::select! {
            _ = cancellation.cancelled() => {
                // Upstream chains also observe cancellation and close their
                // senders. Drain all queued envelopes before forwarding EOS so
                // no receiver task is left behind and bounded producers can
                // finish their shutdown path.
                return shutdown_interior_chain(chain).await;
            }
            _ = idle_tick.tick() => {
                tick_chain(chain).await?;
                continue;
            }
            failure = async {
                match pool.as_ref() {
                    Some(pool) => pool.fail().await,
                    None => std::future::pending().await,
                }
            } => {
                return Err(failure);
            }
            read = readers.next() => read,
        };
        let Some((index, read)) = read else {
            // All input channels closed: end of stream. Drain the worker
            // pool first so its outputs precede the terminal flush.
            if let Some(pool) = pool {
                pool.drain().await;
            }
            finish_chain(chain).await?;
            return Ok(());
        };
        let envelope = match read {
            Ok(envelope) => envelope,
            Err(Error::Process(message)) if message == "input channel closed" => {
                // Producer dropped its sender: end-of-stream for this channel.
                // Treat closure as an explicit EOS so processors flush and
                // downstream sinks observe the same terminal ordering as a
                // source that returned Envelope::Eos. Keep polling the
                // remaining channels for a multi-input vertex.
                if handle_envelope(
                    chain,
                    hook,
                    index,
                    Envelope::Eos,
                    &mut ended_inputs,
                    pool.as_ref(),
                    &mut upstream_watermarks,
                )
                .await?
                {
                    if let Some(pool) = pool {
                        pool.drain().await;
                    }
                    finish_chain(chain).await?;
                    return Ok(());
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
                // A barrier is a control fence for the data submitted before
                // it. Do not snapshot or forward it while a worker can still
                // publish pre-barrier output.
                if let Some(pool) = pool.as_ref() {
                    pool.flush().await?;
                }
                // Capture the committed state epoch BEFORE releasing the
                // buffered post-barrier data: a detached read could observe
                // a mutation that belongs after the barrier. The journal
                // guarantees the backend holds only committed (acknowledged)
                // state, so this read is the checkpoint's immutable view;
                // report delivery stays non-blocking (unbounded channel).
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
                                // Interior chains inherit the source's cut
                                // through the barrier identity.
                                cut_generation: barrier.generation,
                                state,
                                source_positions: Vec::new(),
                                watermark_ms: None,
                            });
                        }
                    }
                    Err(error) => {
                        // Snapshot failure fails this barrier round upstream;
                        // the data plane keeps flowing through the barrier.
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
                        &mut ended_inputs,
                        pool.as_ref(),
                        &mut upstream_watermarks,
                    )
                    .await?
                    {
                        if let Some(pool) = pool {
                            pool.drain().await;
                        }
                        finish_chain(chain).await?;
                        return Ok(());
                    }
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
                    )
                    .await?
                    {
                        if let Some(pool) = pool {
                            pool.drain().await;
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
                    )
                    .await?
                    {
                        if let Some(pool) = pool {
                            pool.drain().await;
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

async fn handle_envelope(
    chain: &Chain,
    hook: &CheckpointHook,
    input_index: usize,
    envelope: Envelope,
    ended_inputs: &mut BTreeSet<usize>,
    pool: Option<&ProcessorWorkerPool>,
    upstream_watermarks: &mut BTreeMap<usize, i64>,
) -> Result<bool, Error> {
    match envelope {
        Envelope::Data(batch, ack) => {
            if let Some(pool) = pool {
                // The pool owns this delivery's processing; ordering and
                // backpressure are the pool's contract.
                pool.submit((batch, ack)).await?;
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
            // transparently so ordering is preserved.
            if let Some(pool) = pool {
                pool.flush().await?;
            }
            send_downstream(chain, envelope).await?;
            Ok(false)
        }
        Envelope::Watermark(watermark) => {
            upstream_watermarks.insert(input_index, watermark);
            // An input that has not emitted a watermark yet is still active;
            // wait for every still-active input before allowing the first
            // aggregate to fire. Subsequent values use the slowest active
            // input's progress; ended inputs were removed above.
            if !(0..chain.inputs.len())
                .filter(|index| !ended_inputs.contains(index))
                .all(|index| upstream_watermarks.contains_key(&index))
            {
                return Ok(false);
            }
            let watermark = upstream_watermarks
                .iter()
                .filter(|(index, _)| !ended_inputs.contains(index))
                .map(|(_, watermark)| *watermark)
                .min()
                .unwrap_or(watermark);
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
    failure: flume::Receiver<Error>,
    submitted: std::sync::atomic::AtomicU64,
    flushed: Arc<std::sync::atomic::AtomicU64>,
    failed: Arc<std::sync::atomic::AtomicBool>,
    progress: Arc<tokio::sync::Notify>,
    cancellation: CancellationToken,
    /// Reorder collector: awaited on the chain's end-of-stream path so the
    /// terminal EOS never overtakes in-flight pool deliveries.
    collector: tokio::task::JoinHandle<()>,
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
        let (done_tx, done_rx) = flume::unbounded::<(u64, Vec<ProcessedBatch>)>();
        let (fail_tx, fail_rx) = flume::bounded::<Error>(1);
        let flushed = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let progress = Arc::new(tokio::sync::Notify::new());
        for _ in 0..parallelism {
            let shared = shared.clone();
            let metrics = metrics.clone();
            let work_rx = work_rx.clone();
            let done_tx = done_tx.clone();
            let fail_tx = fail_tx.clone();
            let failed = failed.clone();
            let progress = progress.clone();
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
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
                            if done_tx.send((sequence, outputs)).is_err() {
                                return;
                            }
                        }
                        Err(error) => {
                            let error = match error {
                                ProcessChainError::Processor(failure) => {
                                    if let Some(metrics) = &metrics {
                                        metrics
                                            .processing_errors
                                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    }
                                    Error::Process(format!(
                                        "processor worker failure: {}",
                                        failure.error
                                    ))
                                }
                                ProcessChainError::Fatal(error) => error,
                            };
                            // Keep draining so the bounded queue never
                            // deadlocks the chain loop; the chain observes
                            // the failure through `fail` and cancels.
                            failed.store(true, std::sync::atomic::Ordering::Release);
                            progress.notify_waiters();
                            let _ = fail_tx.try_send(error);
                        }
                    }
                }
            });
        }
        // Reorder collector: outputs leave in submission order (the legacy
        // Stream contract requires ordered delivery).
        let collector_shared = shared.clone();
        let collector_cancellation = cancellation.clone();
        let collector_flushed = flushed.clone();
        let collector_failed = failed.clone();
        let collector_progress = progress.clone();
        let collector = tokio::spawn(async move {
            let mut pending: BTreeMap<u64, Vec<ProcessedBatch>> = BTreeMap::new();
            let mut next_sequence = 0u64;
            loop {
                let done = tokio::select! {
                    _ = collector_cancellation.cancelled() => return,
                    done = done_rx.recv_async() => match done {
                        Ok(done) => done,
                        Err(_) => {
                            // All workers exited: flush what is left.
                            while let Some(outputs) = pending.remove(&next_sequence) {
                                if flush_outputs(&collector_shared, outputs).await.is_err() {
                                    collector_failed.store(
                                        true,
                                        std::sync::atomic::Ordering::Release,
                                    );
                                    collector_progress.notify_waiters();
                                    return;
                                }
                                next_sequence += 1;
                                collector_flushed
                                    .store(next_sequence, std::sync::atomic::Ordering::Release);
                                collector_progress.notify_waiters();
                            }
                            return;
                        }
                    },
                };
                let (sequence, outputs) = done;
                pending.insert(sequence, outputs);
                while let Some(outputs) = pending.remove(&next_sequence) {
                    if flush_outputs(&collector_shared, outputs).await.is_err() {
                        collector_failed.store(true, std::sync::atomic::Ordering::Release);
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
            failure: fail_rx,
            submitted: std::sync::atomic::AtomicU64::new(0),
            flushed,
            failed,
            progress,
            cancellation: cancellation.clone(),
            collector,
        })
    }

    /// Drain the pool: close the submission queue, let every worker finish
    /// its in-flight delivery, and wait until the reorder collector has
    /// forwarded all outputs. Called on the chain's end-of-stream path.
    async fn drain(self) {
        drop(self.work);
        let _ = self.collector.await;
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
    async fn flush(&self) -> Result<(), Error> {
        let target = self.submitted.load(std::sync::atomic::Ordering::Acquire);
        loop {
            if self.failed.load(std::sync::atomic::Ordering::Acquire) {
                return Err(Error::Process(
                    "processor worker pool failed before control fence".into(),
                ));
            }
            if self.flushed.load(std::sync::atomic::Ordering::Acquire) >= target {
                return Ok(());
            }
            let notified = self.progress.notified();
            tokio::select! {
                _ = self.cancellation.cancelled() => {
                    return Err(Error::Process("processor worker pool was cancelled".into()));
                }
                _ = notified => {}
            }
        }
    }

    /// Await the first worker failure (called from the chain loop's select).
    async fn fail(&self) -> Error {
        match self.failure.recv_async().await {
            Ok(error) => error,
            Err(_) => Error::Process("processor worker pool failed".into()),
        }
    }
}

/// Forward pool outputs through the chain's outbound edges.
async fn flush_outputs(chain: &Chain, outputs: Vec<ProcessedBatch>) -> Result<(), Error> {
    for ProcessedBatch { batch, ack } in outputs {
        send_downstream(chain, Envelope::Data(batch, ack)).await?;
    }
    Ok(())
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
    let mut next = Vec::new();
    for ProcessedBatch { batch, ack } in batches {
        match processor.process_with_ack(batch, ack.clone()).await? {
            ProcessResult::Single(output) => next.push(ProcessedBatch { batch: output, ack }),
            ProcessResult::Multiple(outputs) => {
                if outputs.is_empty() {
                    ack.ack().await?;
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
            ProcessResult::None => ack.ack().await?,
            ProcessResult::Deferred => {}
        }
    }
    Ok(next)
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
        let result = match control {
            ProcessorControl::Finish => processor.finish().await?,
            ProcessorControl::Tick => processor.on_tick().await?,
            ProcessorControl::Watermark(watermark_ms) => {
                processor.on_watermark(watermark_ms).await?
            }
        };
        pending.extend(result_to_batches(result));
    }
    for ProcessedBatch { batch, ack } in pending {
        send_downstream(chain, Envelope::Data(batch, ack)).await?;
    }
    Ok(())
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
        Err(_) => (index, Err(Error::Process("input channel closed".into()))),
    }
}

/// One batch that is ready to leave a non-sink chain, carrying the
/// acknowledgement for its logical source delivery.
struct ProcessedBatch {
    batch: crate::MessageBatchRef,
    ack: Arc<dyn crate::input::Ack>,
}

/// A processor failure retains the input batch and its acknowledgement so the
/// configured error side output can receive exactly the failed delivery.
struct ProcessorFailure {
    error: Error,
    batch: crate::MessageBatchRef,
    ack: Arc<dyn crate::input::Ack>,
}

enum ProcessChainError {
    Processor(ProcessorFailure),
    Fatal(Error),
}

impl From<Error> for ProcessChainError {
    fn from(error: Error) -> Self {
        Self::Fatal(error)
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
    let result = match process_chain(chain, batch, ack, metrics).await {
        Ok(outputs) => {
            for output in outputs {
                if let Err(error) =
                    send_downstream(chain, Envelope::Data(output.batch, output.ack)).await
                {
                    if let Some(chain_metrics) = &chain_metrics {
                        chain_metrics
                            .in_flight
                            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    return Err(error);
                }
            }
            Ok(())
        }
        Err(ProcessChainError::Processor(failure)) => {
            let last_task = chain.task_ids.last().map(String::as_str).unwrap_or("");
            let Some(targets) = chain.error_outputs.get(last_task) else {
                return Err(failure.error);
            };
            send_to_targets(
                Some(targets),
                Envelope::Data(failure.batch, failure.ack),
                false,
            )
            .await
            .map_err(|route_error| {
                Error::Process(format!(
                    "processor failed and error output routing failed: {}; route error: {route_error}",
                    failure.error
                ))
            })
        }
        Err(ProcessChainError::Fatal(error)) => Err(error),
    };
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
    for processor in &chain.processors {
        let mut next = Vec::with_capacity(batches.len());
        for (batch, ack) in batches {
            let failed_batch = batch.clone();
            match processor.process_with_ack(batch, ack.clone()).await {
                Ok(ProcessResult::Single(output)) => next.push((output, ack)),
                Ok(ProcessResult::Multiple(outputs)) => {
                    if outputs.is_empty() {
                        ack.ack().await?;
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
                    ack.ack().await?;
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
                    return Err(ProcessChainError::Processor(ProcessorFailure {
                        error,
                        batch: failed_batch,
                        ack,
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
            if let Err(error) = sink.write_batch(&output_batches).await {
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
                return Err(ProcessChainError::Fatal(error));
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
            for (_, ack) in batches {
                ack.ack().await.map_err(ProcessChainError::Fatal)?;
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

/// Route an envelope to every outbound edge of the chain's last task. Data
/// acknowledgements are split across all actual terminal deliveries so a
/// source is committed only after every fan-out branch succeeds.
async fn send_downstream(chain: &Chain, envelope: Envelope) -> Result<(), Error> {
    let last_task = chain.task_ids.last().map(String::as_str).unwrap_or("");
    if matches!(envelope, Envelope::Data(_, _)) {
        return send_to_targets(chain.outputs.get(last_task), envelope, true).await;
    }

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
                } => {
                    if channels.len() <= 1 {
                        if let Some(sender) = channels.first() {
                            deliveries.push((sender.clone(), batch.clone()));
                        }
                        continue;
                    }
                    let groups = partition_batch_by_key_hash(&batch, key_field, channels.len())?;
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
        for ((sender, batch), ack) in deliveries.into_iter().zip(acks) {
            sender
                .send_async(Envelope::Data(batch, ack))
                .await
                .map_err(|_| Error::Process("downstream channel closed".into()))?;
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
) -> Result<Vec<Option<crate::MessageBatchRef>>, Error> {
    use datafusion::arrow::array::BooleanArray;
    use datafusion::arrow::compute::filter_record_batch;

    if key_field.is_empty() {
        return Ok((0..subtasks)
            .map(|index| (index == 0).then(|| Arc::new(batch.clone())))
            .collect());
    }
    let Some(column) = batch.record_batch().column_by_name(key_field) else {
        return Err(Error::Process(format!(
            "partition key field '{key_field}' is missing from batch"
        )));
    };
    let hashes = hash_column(column.as_ref())?;
    let mut result = Vec::with_capacity(subtasks);
    for subtask in 0..subtasks {
        let keep: BooleanArray = hashes
            .iter()
            .map(|hash| hash.is_some_and(|hash| (hash % subtasks as u64) == subtask as u64))
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

/// Hash every row of a key column with FNV-1a over its canonical encoding.
fn hash_column(column: &dyn Array) -> Result<Vec<Option<u64>>, Error> {
    fn hash_bytes(value: &[u8]) -> u64 {
        value.iter().fold(0xcbf29ce484222325u64, |hash, byte| {
            hash.wrapping_mul(0x100000001b3) ^ u64::from(*byte)
        })
    }
    macro_rules! integer_column {
        ($array:ty) => {
            if let Some(values) = column.as_any().downcast_ref::<$array>() {
                return Ok(values
                    .iter()
                    .map(|value| value.map(|value| hash_bytes(&value.to_be_bytes())))
                    .collect());
            }
        };
    }
    integer_column!(Int8Array);
    integer_column!(Int16Array);
    integer_column!(Int32Array);
    integer_column!(Int64Array);
    integer_column!(UInt8Array);
    integer_column!(UInt16Array);
    integer_column!(UInt32Array);
    integer_column!(UInt64Array);
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return Ok(values
            .iter()
            .map(|value| value.map(|value| hash_bytes(value.as_bytes())))
            .collect());
    }
    if let Some(values) = column.as_any().downcast_ref::<BinaryArray>() {
        return Ok(values
            .iter()
            .map(|value| value.map(|value| hash_bytes(value)))
            .collect());
    }
    Err(Error::Process(format!(
        "partition key column has unsupported Arrow type {:?}",
        column.data_type()
    )))
}
