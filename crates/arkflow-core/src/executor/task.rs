//! Per-chain event loops: the pipelined execution core.
//!
//! Each chain runs its own task. Source chains pull from their input, run the
//! chain's processors, and push envelopes downstream; interior/sink chains
//! receive from their inbound channels. Because every chain runs concurrently
//! and edges are bounded, a slow consumer backpressures its producer while
//! other chains keep flowing.

use super::envelope::Envelope;
use super::graph::{Chain, EdgeTarget, ExecutionGraph};
use crate::Error;
use crate::input::Input;
use crate::ProcessResult;
use datafusion::arrow::array::{
    Array, BinaryArray, Int16Array, Int32Array, Int64Array, Int8Array, StringArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Drive every chain in the graph to completion (cancellation or all-source
/// end-of-stream). Connects inputs/outputs first and closes them after.
/// Optional per-chain checkpoint hook: barriers injected at sources are
/// forwarded with data; chains report snapshots through the sender.
#[derive(Clone, Default)]
pub struct CheckpointHook {
    /// Report snapshots for this chain's entry task (source chains report
    /// their input positions; stateful chains report their keyed state).
    pub reporter: Option<tokio::sync::mpsc::UnboundedSender<super::barrier::ChainSnapshot>>,
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
}

pub async fn run_graph(
    graph: ExecutionGraph,
    cancellation: CancellationToken,
) -> Result<(), Error> {
    run_graph_with_hooks(graph, cancellation, BTreeMap::new()).await
}

/// Run the graph with a snapshot gate: chains take read shares per envelope,
/// a command-driven snapshot (see `kernel_handle`) takes the write share to
/// observe a quiescent instant.
pub async fn run_graph_with_gate(
    graph: ExecutionGraph,
    cancellation: CancellationToken,
    gate: super::kernel_handle::SnapshotGate,
) -> Result<(), Error> {
    run_graph_inner(graph, cancellation, BTreeMap::new(), Some(gate)).await
}

/// Run the graph with per-chain checkpoint hooks keyed by entry task id.
pub async fn run_graph_with_hooks(
    graph: ExecutionGraph,
    cancellation: CancellationToken,
    hooks: BTreeMap<String, CheckpointHook>,
) -> Result<(), Error> {
    run_graph_inner(graph, cancellation, hooks, None).await
}

async fn run_graph_inner(
    graph: ExecutionGraph,
    cancellation: CancellationToken,
    hooks: BTreeMap<String, CheckpointHook>,
    gate: Option<super::kernel_handle::SnapshotGate>,
) -> Result<(), Error> {
    for chain in &graph.chains {
        if let Some(source) = &chain.source {
            source.connect().await?;
        }
        if let Some(sink) = &chain.sink {
            sink.connect().await?;
        }
    }

    let mut tasks = FuturesUnordered::new();
    for chain in graph.chains {
        let token = cancellation.clone();
        let hook = hooks.get(chain.entry_task_id()).cloned().unwrap_or_default();
        let gate = gate.clone();
        tasks.push(tokio::spawn(async move {
            run_chain(chain, hook, gate, token).await
        }));
    }

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
                    first_error = Some(Error::Process(format!(
                        "chain task panicked: {join_error}"
                    )));
                }
            }
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// The event loop of one chain.
async fn run_chain(
    chain: Chain,
    hook: CheckpointHook,
    gate: Option<super::kernel_handle::SnapshotGate>,
    cancellation: CancellationToken,
) -> Result<(), Error> {
    let result = run_chain_inner(&chain, &hook, gate.as_ref(), &cancellation).await;

    // Close owned components on every exit path, matching the legacy
    // close-time policy: log per-component errors, keep closing.
    for processor in &chain.processors {
        if let Err(error) = processor.close().await {
            tracing::warn!(%error, task = chain.entry_task_id(), "failed to close chain processor");
        }
    }
    if let Some(sink) = &chain.sink {
        if let Err(error) = sink.close().await {
            tracing::warn!(%error, task = chain.entry_task_id(), "failed to close chain sink");
        }
    }
    if let Some(source) = &chain.source {
        if let Err(error) = source.close().await {
            tracing::warn!(%error, task = chain.entry_task_id(), "failed to close chain source");
        }
    }
    result
}

async fn run_chain_inner(
    chain: &Chain,
    hook: &CheckpointHook,
    snapshot_gate: Option<&super::kernel_handle::SnapshotGate>,
    cancellation: &CancellationToken,
) -> Result<(), Error> {
    match (&chain.source, chain.inputs.len()) {
        (Some(source), 0) => run_source_chain(chain, source, hook, snapshot_gate, cancellation).await,
        (None, _) if !chain.inputs.is_empty() => {
            run_interior_chain(chain, hook, snapshot_gate, cancellation).await
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
    snapshot_gate: Option<&super::kernel_handle::SnapshotGate>,
    cancellation: &CancellationToken,
) -> Result<(), Error> {
    let mut barrier_rx = hook.barrier_rx.clone();
    let mut event_gate = hook.event_time_gate.lock().await;
    let mut idle_tick = tokio::time::interval(std::time::Duration::from_millis(100));
    idle_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let read = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            _ = idle_tick.tick() => {
                // Idle tick: refresh held rows (idle partitions unblock the
                // watermark) and fire acks that are no longer deferred.
                if let Some(gate) = event_gate.as_mut() {
                    let decision = gate.refresh()?;
                    for (batch, action) in decision.ready {
                        dispatch_gated(chain, batch, action).await?;
                    }
                    for ack in gate.take_ready_acks() {
                        ack.ack().await?;
                    }
                }
                continue;
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
                report_source_barrier(chain, source, hook, barrier.clone()).await?;
                send_downstream(chain, barrier).await?;
                continue;
            }
            result = source.read() => match result {
                Ok(read) => read,
                Err(Error::EOF) => return Ok(()),
                Err(Error::Disconnection) => {
                    // Reconnect with the legacy backoff cadence.
                    loop {
                        match source.connect().await {
                            Ok(()) => break,
                            Err(error) => {
                                tracing::warn!(%error, "source reconnect failed");
                                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                            }
                        }
                    }
                    continue;
                }
                Err(error) => return Err(error),
            },
        };
        let (batch, ack) = read;
        // Snapshot gate: read share while this envelope is in flight so a
        // command-driven snapshot observes a quiescent graph.
        let _gate_guard = match snapshot_gate {
            Some(gate) => Some(gate.read().await),
            None => None,
        };
        match event_gate.as_mut() {
            None => {
                for output in process_chain(chain, batch).await? {
                    send_downstream(chain, Envelope::Data(output, ack.clone())).await?;
                }
            }
            Some(gate) => {
                let partition = hook.partition.unwrap_or(0);
                let decision = gate.observe(partition, batch)?;
                for (slice, action) in decision.ready {
                    dispatch_gated(chain, slice, action).await?;
                }
                if gate.has_held() {
                    gate.defer_ack(ack);
                } else {
                    ack.ack().await?;
                }
                for ready in gate.take_ready_acks() {
                    ready.ack().await?;
                }
            }
        }
    }
}

/// Dispatch one gated batch slice according to its window action. Route and
/// Update forward with their marker columns attached (legacy semantics);
/// Emit/Hold-equivalent rows flow through the chain normally.
async fn dispatch_gated(
    chain: &Chain,
    batch: crate::MessageBatchRef,
    action: crate::event_time::WindowAction,
) -> Result<(), Error> {
    let batch = match action {
        crate::event_time::WindowAction::Route => mark_event_batch(batch, "__arkflow_late_event_route")?,
        crate::event_time::WindowAction::Update => mark_event_batch(batch, "__arkflow_late_event_update")?,
        _ => batch,
    };
    for output in process_chain(chain, batch).await? {
        send_downstream(chain, Envelope::Data(output, Arc::new(crate::input::NoopAck))).await?;
    }
    Ok(())
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

/// Report a barrier's source positions (and state, when the source chain owns
/// any) to the coordinator.
async fn report_source_barrier(
    _chain: &Chain,
    source: &Arc<dyn Input>,
    hook: &CheckpointHook,
    barrier: Envelope,
) -> Result<(), Error> {
    let Envelope::Barrier(barrier) = barrier else {
        return Ok(());
    };
    let Some(reporter) = &hook.reporter else {
        return Ok(());
    };
    let positions = source.current_positions().await?;
    let state = match &hook.state {
        Some(backend) => super::barrier::snapshot_state(backend.clone()).await?,
        None => crate::state::StateSnapshot::new(1, Vec::new()),
    };
    let _ = reporter.send(super::barrier::ChainSnapshot {
        task_id: hook.task_id.clone().unwrap_or_default(),
        attempt_id: format!("{}-attempt", hook.task_id.clone().unwrap_or_default()),
        partition: 0,
        barrier,
        state,
        source_positions: positions,
        watermark_ms: None,
    });
    Ok(())
}

/// Interior/sink chain: consume inbound channels. Barriers align across the
/// chain's inputs (`Aligner`), snapshot the chain's state, and flow onward.
async fn run_interior_chain(
    chain: &Chain,
    hook: &CheckpointHook,
    snapshot_gate: Option<&super::kernel_handle::SnapshotGate>,
    cancellation: &CancellationToken,
) -> Result<(), Error> {
    let mut readers = FuturesUnordered::new();
    for (index, receiver) in chain.inputs.iter().enumerate() {
        readers.push(recv_envelope(index, receiver.clone()));
    }
    let mut aligner = super::barrier::Aligner::new(chain.inputs.len(), 1024);
    loop {
        let read = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            read = readers.next() => read,
        };
        let Some((index, read)) = read else {
            // All input channels closed: end of stream.
            return Ok(());
        };
        let envelope = match read {
            Ok(envelope) => envelope,
            Err(Error::Process(message)) if message == "input channel closed" => {
                // Producer dropped its sender: end-of-stream for this channel.
                // Keep polling the remaining channels; when all have closed,
                // `readers` empties and the loop exits below.
                if readers.is_empty() {
                    return Ok(());
                }
                continue;
            }
            Err(error) => return Err(error),
        };
        // Snapshot gate: read share while this envelope is in flight so a
        // command-driven snapshot observes a quiescent graph.
        let _gate_guard = match snapshot_gate {
            Some(gate) => Some(gate.read().await),
            None => None,
        };
        // Barrier alignment: hold data back until every input delivered the
        // current barrier. Single-input chains pass straight through because
        // the aligner completes immediately; multi-input chains buffer other
        // inputs' data until the last barrier arrives.
        match aligner.observe(index, envelope) {
            Ok(Some(barrier)) => {
                report_chain_barrier(chain, hook, barrier.clone()).await?;
                send_downstream(chain, Envelope::Barrier(barrier)).await?;
                for (_, buffered) in aligner.release() {
                    handle_envelope(chain, buffered).await?;
                }
            }
            Ok(None) => {
                if aligner.is_aligning() {
                    // Envelope retained inside the aligner; replay happens at
                    // alignment completion above.
                } else if let Some(envelope) = aligner.take_passthrough() {
                    handle_envelope(chain, envelope).await?;
                }
            }
            Err(error) => {
                // Alignment overflow: release what was buffered and keep the
                // data flowing; the barrier round fails upstream.
                for (_, buffered) in aligner.release() {
                    handle_envelope(chain, buffered).await?;
                }
                tracing::warn!(%error, "barrier alignment overflowed");
            }
        }
        // Restart the reader for the channel just drained so the channel
        // stays continuously polled.
        if let Some(receiver) = chain.inputs.get(index) {
            readers.push(recv_envelope(index, receiver.clone()));
        }
    }
}

async fn handle_envelope(chain: &Chain, envelope: Envelope) -> Result<(), Error> {
    match envelope {
        Envelope::Data(batch, ack) => {
            for output in process_chain(chain, batch).await? {
                send_downstream(chain, Envelope::Data(output, ack.clone())).await?;
            }
            Ok(())
        }
        Envelope::Eos => Ok(()),
        Envelope::Barrier(_) | Envelope::Watermark(_) => {
            // Barrier/watermark handling lands with the checkpoint task;
            // forward transparently so ordering is preserved.
            send_downstream(chain, envelope).await
        }
    }
}

/// Snapshot the chain's keyed state for a barrier and report it.
async fn report_chain_barrier(
    chain: &Chain,
    hook: &CheckpointHook,
    barrier: crate::checkpoint::CheckpointBarrier,
) -> Result<(), Error> {
    let Some(reporter) = &hook.reporter else {
        return Ok(());
    };
    let state = match &hook.state {
        Some(backend) => super::barrier::snapshot_state(backend.clone()).await?,
        None => crate::state::StateSnapshot::new(1, Vec::new()),
    };
    let _ = reporter.send(super::barrier::ChainSnapshot {
        task_id: hook.task_id.clone().unwrap_or_else(|| chain.entry_task_id().to_owned()),
        attempt_id: format!("{}-attempt", chain.entry_task_id()),
        partition: 0,
        barrier,
        state,
        source_positions: Vec::new(),
        watermark_ms: None,
    });
    Ok(())
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

/// Run a batch through the chain's processors. Sink chains write the final
/// result and ack; interior chains return the outputs for downstream routing.
async fn process_chain(
    chain: &Chain,
    batch: crate::MessageBatchRef,
) -> Result<Vec<crate::MessageBatchRef>, Error> {
    let mut batches = vec![batch];
    for processor in &chain.processors {
        let mut next = Vec::with_capacity(batches.len());
        for batch in batches {
            match processor.process(batch).await? {
                ProcessResult::Single(output) => next.push(output),
                ProcessResult::Multiple(outputs) => next.extend(outputs),
                ProcessResult::None => {}
            }
        }
        batches = next;
        if batches.is_empty() {
            break;
        }
    }
    if let Some(sink) = &chain.sink {
        if !batches.is_empty() {
            sink.write_batch(&batches).await?;
        }
        return Ok(Vec::new());
    }
    Ok(batches)
}

/// Route an envelope to every outbound edge of the chain's last task.
async fn send_downstream(chain: &Chain, envelope: Envelope) -> Result<(), Error> {
    let last_task = chain.task_ids.last().map(String::as_str).unwrap_or("");
    let Some(targets) = chain.outputs.get(last_task) else {
        return Ok(());
    };
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
            EdgeTarget::Partitioned { channels, key_field } => {
                if channels.len() <= 1 {
                    if let Some(sender) = channels.first() {
                        sender
                            .send_async(envelope.clone())
                            .await
                            .map_err(|_| Error::Process("downstream channel closed".into()))?;
                    }
                    continue;
                }
                let Envelope::Data(batch, ack) = envelope.clone() else {
                    // Control envelopes broadcast to every partition so each
                    // subtask observes barriers and watermarks.
                    for sender in channels {
                        sender
                            .send_async(envelope.clone())
                            .await
                            .map_err(|_| Error::Process("downstream channel closed".into()))?;
                    }
                    continue;
                };
                let Envelope::Data(..) = envelope else { unreachable!() };
                // Partition data batches by key hash. Splitting a batch per
                // subtask is only necessary with >1 downstream subtask.
                let groups = partition_batch_by_key_hash(&batch, key_field, channels.len())?;
                for (subtask, group) in groups.into_iter().enumerate() {
                    let Some(group) = group else { continue };
                    channels[subtask]
                        .send_async(Envelope::Data(group, ack.clone()))
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
