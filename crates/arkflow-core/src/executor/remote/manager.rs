//! Node-side network manager: the inbound routing registry, outbound edges,
//! session-scoped receipt routing, and the per-connection pumps.

use super::auth::{
    verify_handshake_mac, DataPlaneCredentials, EdgeSessionKey, HandshakePayload, JobSessionKey,
    PeerExpectation, SessionAuth, DATA_PLANE_PROTOCOL_VERSION, HANDSHAKE_NONCE_LEN,
};
use super::codec::{DataDecoder, DataEncoder};
use super::transport::{
    connect_backoff, read_frame_with_limits, read_receipt_frame, write_frame_with_limit,
    AcceptedQueue, ConnectionFailure, DataPlaneTlsConfig, EdgeTransport, RemoteStream,
};
use super::wire::{
    FrameHeader, FrameKind, Quad, ReceiptFrame, ReceiptKind, TieredFrame, WireSignal, MAX_FRAME_LEN,
};
use crate::executor::envelope::Envelope;
use crate::Error;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};

// ---------------------------------------------------------------------------
// Node-side network manager
// ---------------------------------------------------------------------------

pub(crate) struct FailureChannel {
    sender: flume::Sender<Error>,
    receiver: flume::Receiver<Error>,
}

/// Bounded resource and authentication policy for one node's data plane.
#[derive(Clone)]
pub struct NetworkManagerConfig {
    pub channel_capacity: usize,
    pub max_connections: usize,
    pub max_pending_receipts: usize,
    pub max_receipt_queue: usize,
    pub max_failure_queue: usize,
    pub max_frame_len: u32,
    pub read_idle_timeout: std::time::Duration,
    /// Receipt-read idle budget while batches still await receipts. A slow
    /// downstream withholds receipts for in-flight work; the short
    /// `read_idle_timeout` is for dead-peer discovery on otherwise idle
    /// connections and must not tear down a busy one.
    pub receipt_wait_timeout: std::time::Duration,
    /// Approximate byte budget for retained replay batches. Rejection takes
    /// the existing send-failure path; the bound is an order-of-magnitude
    /// guard, not an exact quota.
    pub max_pending_bytes: u64,
    /// Bounded transparent reconnect: how many times an upstream edge
    /// redials the peer after a stream failure before failing the edge
    /// closed. 0 disables reconnect and preserves the immediate fail-closed
    /// behavior byte for byte.
    pub reconnect_attempts: usize,
    /// How long the downstream side waits for a re-registering connection
    /// after losing one without Eos before reporting the loss as a failure.
    pub reconnect_grace: std::time::Duration,
    pub registration_grace: std::time::Duration,
    /// How long a successfully authenticated client nonce remains rejected.
    /// The cache closes the replay window without retaining handshake state
    /// forever; its capacity is bounded by the connection limit below.
    pub handshake_replay_ttl: std::time::Duration,
    pub credentials: Option<DataPlaneCredentials>,
    /// Fleet-CA-anchored mTLS for every cross-node connection. `None`
    /// (the default) keeps the plaintext protocol byte for byte.
    pub tls: Option<DataPlaneTlsConfig>,
}

impl Default for NetworkManagerConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 1024,
            max_connections: 256,
            max_pending_receipts: 4096,
            max_receipt_queue: 4096,
            max_failure_queue: 1024,
            max_frame_len: MAX_FRAME_LEN,
            read_idle_timeout: std::time::Duration::from_secs(30),
            receipt_wait_timeout: std::time::Duration::from_secs(10 * 60),
            max_pending_bytes: 256 * 1024 * 1024,
            reconnect_attempts: 5,
            reconnect_grace: std::time::Duration::from_secs(10),
            registration_grace: std::time::Duration::from_secs(10),
            handshake_replay_ttl: std::time::Duration::from_secs(10 * 60),
            credentials: None,
            tls: None,
        }
    }
}

impl NetworkManagerConfig {
    pub fn validate(&self) -> Result<(), Error> {
        if self.channel_capacity == 0
            || self.max_connections == 0
            || self.max_pending_receipts == 0
            || self.max_receipt_queue == 0
            || self.max_failure_queue == 0
            || self.max_frame_len == 0
            || self.read_idle_timeout.is_zero()
            || self.receipt_wait_timeout.is_zero()
            || self.max_pending_bytes == 0
            || self.reconnect_grace.is_zero()
            || self.registration_grace.is_zero()
            || self.handshake_replay_ttl.is_zero()
        {
            return Err(Error::Config(
                "network shuffle resource limits must be positive".into(),
            ));
        }
        if self.max_frame_len > MAX_FRAME_LEN {
            return Err(Error::Config(format!(
                "network shuffle max_frame_len must not exceed {MAX_FRAME_LEN}"
            )));
        }
        if self
            .credentials
            .as_ref()
            .is_some_and(|credentials| credentials.local_node.trim().is_empty())
        {
            return Err(Error::Config(
                "network shuffle credential local_node must not be empty".into(),
            ));
        }
        Ok(())
    }
}

/// Mirrors one branch acknowledgement's lifecycle back across the wire. The
/// downstream chain wraps every decoded remote batch in one of these; the
/// kernel's existing fan-out and state wrappers compose on top of it exactly
/// as they do around local acknowledgements.
pub(crate) struct RemoteAck {
    pub(crate) outbox: flume::Sender<(Quad, ReceiptFrame)>,
    pub(crate) quad: Quad,
    pub(crate) seq: u64,
    pub(crate) failures: flume::Sender<Error>,
}

#[async_trait::async_trait]
impl crate::input::Ack for RemoteAck {
    async fn ack(&self) -> Result<(), Error> {
        let result = self
            .outbox
            .send_async((
                self.quad,
                ReceiptFrame {
                    kind: ReceiptKind::Acked,
                    seq: self.seq,
                },
            ))
            .await;
        result.map_err(|_| Error::Process("remote edge receipt channel closed".into()))
    }

    async fn abort(&self) -> Result<(), Error> {
        // Mirror the abort to the upstream pending map as a Failed receipt so
        // the branch acknowledgement aborts immediately; the barrier-drain
        // timeout stays only as the fallback for a lost frame.
        self.outbox
            .send_async((
                self.quad,
                ReceiptFrame {
                    kind: ReceiptKind::Failed,
                    seq: self.seq,
                },
            ))
            .await
            .map_err(|_| Error::Process("remote edge receipt channel closed".into()))
    }

    fn mark_held(&self) {
        // Synchronous by trait contract (called from the Aligner's sync
        // context).  A full failure queue must not block a tokio worker: a
        // full queue means the drain side has already stalled, and the edge
        // is torn down by the stalled read loop regardless.  Escalate
        // non-blockingly and let the connection teardown carry the signal.
        if self
            .outbox
            .try_send((
                self.quad,
                ReceiptFrame {
                    kind: ReceiptKind::Held,
                    seq: self.seq,
                },
            ))
            .is_err()
            && self
                .failures
                .try_send(Error::Process(format!(
                    "remote edge receipt queue overflow for quad {:?}",
                    self.quad
                )))
                .is_err()
        {
            tracing::error!(
                quad = ?self.quad,
                seq = self.seq,
                "remote edge receipt queue and failure queue both full; dropping the overflow signal"
            );
        }
    }

    fn release_held(&self) {
        if self
            .outbox
            .try_send((
                self.quad,
                ReceiptFrame {
                    kind: ReceiptKind::Released,
                    seq: self.seq,
                },
            ))
            .is_err()
            && self
                .failures
                .try_send(Error::Process(format!(
                    "remote edge receipt queue overflow for quad {:?}",
                    self.quad
                )))
                .is_err()
        {
            tracing::error!(
                quad = ?self.quad,
                seq = self.seq,
                "remote edge receipt queue and failure queue both full; dropping the overflow signal"
            );
        }
    }
}

/// One in-flight batch awaiting its replicas' receipts on a quad.
struct PendingBatch {
    branch: Arc<dyn crate::input::Ack>,
    /// Retained for transparent-reconnect replay until the acknowledgement
    /// completes; entries leave the map on completion, so an acknowledged
    /// frame never replays.
    replay: Option<crate::MessageBatchRef>,
    /// Approximate Arrow memory of `replay`, accounted against the byte
    /// budget while this entry lives in the map.
    bytes: u64,
    remaining_replicas: usize,
    held_replicas: usize,
}

/// Receipts for one outbound quad, shared between the outbound pump (which
/// registers pending batches) and the connection's receipt read loop.
pub(crate) struct PendingReceipts {
    map: std::sync::Mutex<BTreeMap<u64, PendingBatch>>,
    next_seq: std::sync::atomic::AtomicU64,
    pending_bytes: std::sync::atomic::AtomicU64,
    max_entries: usize,
    max_pending_bytes: u64,
}

impl PendingReceipts {
    #[cfg(test)]
    pub(crate) fn new(max_entries: usize) -> Self {
        Self::with_byte_budget(max_entries, u64::MAX)
    }

    pub(crate) fn with_byte_budget(max_entries: usize, max_pending_bytes: u64) -> Self {
        Self {
            map: std::sync::Mutex::new(BTreeMap::new()),
            next_seq: std::sync::atomic::AtomicU64::new(0),
            pending_bytes: std::sync::atomic::AtomicU64::new(0),
            max_entries,
            max_pending_bytes,
        }
    }

    pub(crate) fn register(
        &self,
        branch: &Arc<dyn crate::input::Ack>,
        replicas: usize,
        replay: Option<crate::MessageBatchRef>,
    ) -> Result<u64, Error> {
        // Approximate Arrow memory (buffers + validity), an order-of-magnitude
        // guard against a handful of huge retained batches blowing the heap.
        let bytes = replay
            .as_ref()
            .map(|batch| batch.record_batch().get_array_memory_size() as u64)
            .unwrap_or(0);
        let mut map = self.map.lock().expect("pending receipts lock");
        if map.len() >= self.max_entries {
            return Err(Error::Process(format!(
                "remote edge pending receipt limit {} reached",
                self.max_entries
            )));
        }
        let retained = self
            .pending_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        if retained.saturating_add(bytes) > self.max_pending_bytes {
            return Err(Error::Process(format!(
                "remote edge pending replay byte budget {} reached ({} bytes retained)",
                self.max_pending_bytes, retained
            )));
        }
        let seq = self
            .next_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.pending_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        map.insert(
            seq,
            PendingBatch {
                branch: branch.clone(),
                replay,
                bytes,
                remaining_replicas: replicas,
                held_replicas: 0,
            },
        );
        Ok(seq)
    }

    /// Release a completed entry's byte accounting back to the budget.
    fn release_bytes(&self, bytes: u64) {
        self.pending_bytes
            .fetch_sub(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.map.lock().expect("pending receipts lock").is_empty()
    }

    /// Every unacknowledged batch in sequence order, for replay after a
    /// transparent reconnect. Entries leave the map when their acknowledgement
    /// completes, so this is exactly the set the peer may not have processed.
    fn replay_snapshot(&self) -> Vec<(u64, crate::MessageBatchRef)> {
        let map = self.map.lock().expect("pending receipts lock");
        map.iter()
            .filter_map(|(seq, pending)| pending.replay.clone().map(|batch| (*seq, batch)))
            .collect()
    }

    pub(crate) fn apply(&self, receipt: ReceiptFrame, failures: &flume::Sender<Error>) {
        let mut map = self.map.lock().expect("pending receipts lock");
        let Some(pending) = map.get_mut(&receipt.seq) else {
            // Unknown or already-completed sequence: a duplicate receipt from a
            // retried acknowledgement. The branch itself is idempotent; drop.
            return;
        };
        match receipt.kind {
            ReceiptKind::Acked => {
                pending.remaining_replicas = pending.remaining_replicas.saturating_sub(1);
                if pending.remaining_replicas == 0 {
                    let PendingBatch { branch, bytes, .. } =
                        map.remove(&receipt.seq).expect("checked");
                    self.release_bytes(bytes);
                    // Acknowledge off the read loop: a durable commit can
                    // block, and the connection must keep draining.
                    let failures = failures.clone();
                    tokio::spawn(async move {
                        if let Err(error) = branch.ack().await {
                            let _ = failures
                                .send_async(Error::Process(format!(
                                    "remote edge branch acknowledgement failed: {error}"
                                )))
                                .await;
                        }
                    });
                }
            }
            ReceiptKind::Held => {
                // The branch's hold flag is transition-tracked; forwarding
                // mark_held on every Held would re-assert the parent hold on
                // each replica, so forward only the 0→1 transition.
                pending.held_replicas += 1;
                if pending.held_replicas == 1 {
                    pending.branch.mark_held();
                }
            }
            ReceiptKind::Released => {
                pending.held_replicas = pending.held_replicas.saturating_sub(1);
                if pending.held_replicas == 0 {
                    pending.branch.release_held();
                }
            }
            ReceiptKind::Failed => {
                let PendingBatch { branch, bytes, .. } = map.remove(&receipt.seq).expect("checked");
                self.release_bytes(bytes);
                // Abort off the read loop: branch compensation may block on
                // journal/WAL undo, and the connection must keep draining.
                let failures = failures.clone();
                tokio::spawn(async move {
                    if let Err(error) = branch.abort().await {
                        let _ = failures
                            .send_async(Error::Process(format!(
                                "remote edge branch abort failed: {error}"
                            )))
                            .await;
                    }
                });
            }
        }
    }

    fn abort_all(&self) {
        let mut map = self.map.lock().expect("pending receipts lock");
        let drained: BTreeMap<u64, PendingBatch> = std::mem::take(&mut *map);
        // The whole table is gone; zero the byte accounting so the budget
        // cannot outlive the entries it was charging for (every caller drops
        // or replaces this instance right after, but the invariant should
        // not depend on instance death).
        self.pending_bytes
            .store(0, std::sync::atomic::Ordering::Relaxed);
        for (_, pending) in drained {
            let branch = pending.branch;
            tokio::spawn(async move {
                let _ = branch.abort().await;
            });
        }
    }
}

/// Capture the current span context as a W3C `traceparent` string, or `None`
/// when the current context carries no valid span — including tracing being
/// disabled entirely, which keeps barriers byte-identical on the wire.
pub(crate) fn capture_trace_context() -> Option<String> {
    use opentelemetry::propagation::TextMapPropagator as _;
    use opentelemetry::trace::TraceContextExt as _;
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    let cx = tracing::Span::current().context();
    if !cx.span().span_context().is_valid() {
        return None;
    }
    let mut carrier = std::collections::HashMap::new();
    opentelemetry_sdk::propagation::TraceContextPropagator::new().inject_context(&cx, &mut carrier);
    carrier.get("traceparent").cloned()
}

/// Extract a remote parent context from a barrier's captured `traceparent`.
/// Returns `None` for absent or unparsable values so a malformed hop can
/// never detach a downstream span from its local parent.
pub(crate) fn extract_trace_context(trace_context: &str) -> Option<opentelemetry::Context> {
    use opentelemetry::propagation::TextMapPropagator as _;
    use opentelemetry::trace::TraceContextExt as _;

    let mut carrier = std::collections::HashMap::new();
    carrier.insert("traceparent".to_string(), trace_context.to_string());
    let cx = opentelemetry_sdk::propagation::TraceContextPropagator::new().extract(&carrier);
    if cx.span().span_context().is_valid() {
        Some(cx)
    } else {
        None
    }
}

/// Local end of a remote outbound edge. `graph.rs` places its sender into
/// `EdgeTarget` channel vectors exactly like a local channel sender; the pump
/// task on the receiver side encodes envelopes, sequences data frames, and
/// tracks receipts. Closing the sender closes the edge after draining.
pub struct RemoteEdge {
    pub sender: flume::Sender<Envelope>,
}

/// Per-node exchange: inbound routing registry + outbound edges.
pub struct NetworkManager {
    /// Job/generation/quad → local input channel (downstream side).
    pub(crate) inbound: std::sync::RwLock<BTreeMap<EdgeSessionKey, flume::Sender<Envelope>>>,
    /// Job/generation/quad → pending receipts (upstream side), consulted by
    /// receipt read loops.
    outbound: std::sync::RwLock<BTreeMap<EdgeSessionKey, Arc<PendingReceipts>>>,
    /// Job/generation/quad → expected authenticated upstream identity.
    pub(crate) inbound_auth: std::sync::RwLock<BTreeMap<EdgeSessionKey, PeerExpectation>>,
    /// Fatal edge errors (ack commit failures, aborted edges) for the kernel.
    failures: flume::Sender<Error>,
    pub(crate) failure_receiver: flume::Receiver<Error>,
    /// Per-Job failure channels. The process-wide channel is retained only for
    /// legacy, unauthenticated callers that do not carry a Job identity.
    pub(crate) failure_channels: std::sync::RwLock<BTreeMap<JobSessionKey, FailureChannel>>,
    /// Client nonces that have already completed MAC verification.  A client
    /// nonce is otherwise self-generated and therefore cannot prove freshness
    /// to the server on its own.  The TTL and capacity keep this replay guard
    /// bounded while rejecting duplicate handshakes during a live session.
    handshake_nonces: std::sync::Mutex<BTreeMap<Vec<u8>, std::time::Instant>>,
    /// The unauthenticated in-memory compatibility transport has no Job
    /// identity on the wire.  Keep it explicitly single-Job so two graphs
    /// cannot overwrite one another's quad routes.
    legacy_job: std::sync::RwLock<Option<JobSessionKey>>,
    /// Session-scoped receipt routing: RemoteAcks hand their receipts to a
    /// queue owned by the SESSION, not the connection, so an acknowledgement
    /// started on one connection still completes after a transparent
    /// reconnect swaps the wire underneath it. A forwarder task drains the
    /// queue into whichever connection is currently serving the session.
    session_receipts: std::sync::Mutex<BTreeMap<EdgeSessionKey, SessionReceiptRoute>>,
    /// Delivery-level dedup state per session key: the highest data-frame
    /// sequence number already routed into the local channel. A transparent
    /// reconnect replays every frame the upstream has not receipted, so the
    /// receiver drops (and mirror-acks) sequences at or below this mark
    /// instead of double-delivering them.
    delivered_seq: std::sync::Mutex<BTreeMap<EdgeSessionKey, u64>>,
    /// Per-session-key delivery locks serializing "dedup check → local
    /// channel send → delivered-seq advance". During a transparent
    /// reconnect two connections can serve the same key concurrently (the
    /// old one still parked on a backpressured send); without this lock
    /// both pass the dedup check for the same sequence, and the old
    /// connection's late completion overwrites the delivered mark with a
    /// lower value, letting later replays slip through the dedup.
    delivery_locks: std::sync::Mutex<BTreeMap<EdgeSessionKey, Arc<tokio::sync::Mutex<()>>>>,
    /// Registration generation per session key, bumped whenever a connection
    /// successfully (re)routes the key. The reconnect grace watcher compares
    /// its snapshot against the current generation to decide whether the
    /// upstream came back.
    session_registrations: std::sync::Mutex<BTreeMap<EdgeSessionKey, u64>>,
    /// Accepted streams feed this queue so serving works over any transport.
    accepted: AcceptedQueue,
    config: NetworkManagerConfig,
    active_connections: std::sync::atomic::AtomicUsize,
    pub(crate) shutdown: tokio_util::sync::CancellationToken,
}

/// One session's receipt route: the queue RemoteAcks send into, plus the
/// connection-swappable writer slot the forwarder drains toward.
struct SessionReceiptRoute {
    queue: flume::Sender<(Quad, ReceiptFrame)>,
    current: std::sync::RwLock<Option<flume::Sender<(Quad, ReceiptFrame)>>>,
    forwarder: tokio_util::sync::CancellationToken,
}

impl NetworkManager {
    /// Look up (creating on first use) the session-scoped receipt route.
    /// The bounded queue applies backpressure to acknowledgements when the
    /// peer stops draining, and the forwarder retries failed connection
    /// writes until the next connection takes the slot.
    pub(crate) fn session_receipt_route(
        self: &Arc<Self>,
        key: &EdgeSessionKey,
    ) -> flume::Sender<(Quad, ReceiptFrame)> {
        let mut routes = self.session_receipts.lock().expect("session receipt lock");
        if let Some(route) = routes.get(key) {
            return route.queue.clone();
        }
        let (queue_tx, queue_rx) =
            flume::bounded::<(Quad, ReceiptFrame)>(self.config.max_receipt_queue);
        let cancel = tokio_util::sync::CancellationToken::new();
        let route = SessionReceiptRoute {
            queue: queue_tx.clone(),
            current: std::sync::RwLock::new(None),
            forwarder: cancel.clone(),
        };
        routes.insert(key.clone(), route);
        let manager = self.clone();
        let key = key.clone();
        let shutdown_watch = manager.shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    // The manager's shutdown must also stop a forwarder that
                    // is merely parked waiting for receipts — otherwise the
                    // task (and its strong manager Arc) outlives shutdown.
                    _ = shutdown_watch.cancelled() => break,
                    item = queue_rx.recv_async() => {
                        let Ok(item) = item else { break };
                        // Forward to the current connection's writer; a dead
                        // slot (connection swapped or gone) retries until a
                        // live one appears or the session is torn down.
                        loop {
                            if manager.shutdown.is_cancelled() || cancel.is_cancelled() {
                                break;
                            }
                            let target = {
                                let routes = manager
                                    .session_receipts
                                    .lock()
                                    .expect("session receipt lock");
                                routes
                                    .get(&key)
                                    .and_then(|route| {
                                        route
                                            .current
                                            .read()
                                            .expect("session receipt slot lock")
                                            .clone()
                                    })
                            };
                            let Some(target) = target else {
                                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                                continue;
                            };
                            match target.send_async(item).await {
                                Ok(()) => break,
                                // Writer gone: retry on the next slot.
                                Err(_) => {
                                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                                }
                            }
                        }
                    }
                }
            }
        });
        queue_tx
    }

    /// Install `sender` as the live writer for a session's receipts (called
    /// by each connection that starts serving the key).
    pub(crate) fn set_session_receipt_writer(
        &self,
        key: &EdgeSessionKey,
        sender: Option<flume::Sender<(Quad, ReceiptFrame)>>,
    ) {
        let routes = self.session_receipts.lock().expect("session receipt lock");
        if let Some(route) = routes.get(key) {
            *route.current.write().expect("session receipt slot lock") = sender;
        }
    }

    /// Clear the session's writer slot only when it still belongs to
    /// `ours`. During a transparent reconnect the replacement connection
    /// can install its own sender before the dying connection's cleanup
    /// runs; an unconditional clear would clobber the live slot and stall
    /// every later receipt until the next reconnect.
    pub(crate) fn clear_session_receipt_writer_if_owned(
        &self,
        key: &EdgeSessionKey,
        ours: &flume::Sender<(Quad, ReceiptFrame)>,
    ) {
        let routes = self.session_receipts.lock().expect("session receipt lock");
        let Some(route) = routes.get(key) else {
            return;
        };
        let mut slot = route.current.write().expect("session receipt slot lock");
        if slot
            .as_ref()
            .is_some_and(|current| current.same_channel(ours))
        {
            *slot = None;
        }
    }

    /// Tear a session's receipt route down (session loss confirmed or job
    /// removed): queued acknowledgements no longer have a destination.
    pub(crate) fn remove_session_receipt_route(&self, key: &EdgeSessionKey) {
        let route = self
            .session_receipts
            .lock()
            .expect("session receipt lock")
            .remove(key);
        if let Some(route) = route {
            route.forwarder.cancel();
        }
    }
}

impl NetworkManager {
    pub fn new(channel_capacity: usize) -> Arc<Self> {
        let config = NetworkManagerConfig {
            channel_capacity: channel_capacity.max(1),
            ..NetworkManagerConfig::default()
        };
        Self::with_config(config).expect("default network manager configuration is valid")
    }

    pub fn with_config(config: NetworkManagerConfig) -> Result<Arc<Self>, Error> {
        config.validate()?;
        let (failure_sender, failure_receiver) = flume::bounded(config.max_failure_queue);
        let accepted = flume::bounded(config.max_connections);
        Ok(Arc::new(Self {
            inbound: std::sync::RwLock::new(BTreeMap::new()),
            outbound: std::sync::RwLock::new(BTreeMap::new()),
            inbound_auth: std::sync::RwLock::new(BTreeMap::new()),
            failures: failure_sender,
            failure_receiver,
            failure_channels: std::sync::RwLock::new(BTreeMap::new()),
            handshake_nonces: std::sync::Mutex::new(BTreeMap::new()),
            legacy_job: std::sync::RwLock::new(None),
            session_receipts: std::sync::Mutex::new(BTreeMap::new()),
            delivered_seq: std::sync::Mutex::new(BTreeMap::new()),
            delivery_locks: std::sync::Mutex::new(BTreeMap::new()),
            session_registrations: std::sync::Mutex::new(BTreeMap::new()),
            accepted,
            active_connections: std::sync::atomic::AtomicUsize::new(0),
            config,
            shutdown: tokio_util::sync::CancellationToken::new(),
        }))
    }

    /// Fatal edge failures for the kernel's failure reporter.
    pub fn failure_receiver(&self) -> flume::Receiver<Error> {
        self.failure_receiver.clone()
    }

    /// Fatal failures for one Job attempt.  A process-wide data-plane listener
    /// can serve several Jobs, so their failure notifications must not share a
    /// competing receiver: otherwise one Job can consume another Job's edge
    /// failure and cancel the wrong attempt.
    pub fn failure_receiver_for_job(
        &self,
        job_id: &str,
        generation: u64,
    ) -> flume::Receiver<Error> {
        if self.config.credentials.is_none() {
            // The legacy wire format has no Job/session identity, so its
            // failure path is intentionally process-wide as well.
            return self.failure_receiver();
        }
        let key = JobSessionKey {
            job_id: job_id.to_owned(),
            generation,
        };
        self.failure_receiver_for_job_key(&key)
    }

    fn failure_receiver_for_job_key(&self, key: &JobSessionKey) -> flume::Receiver<Error> {
        let mut channels = self
            .failure_channels
            .write()
            .expect("failure channel registry lock");
        channels
            .entry(key.clone())
            .or_insert_with(|| {
                let (sender, receiver) = flume::bounded(self.config.max_failure_queue);
                FailureChannel { sender, receiver }
            })
            .receiver
            .clone()
    }

    fn failure_sender_for_key(&self, key: &EdgeSessionKey) -> flume::Sender<Error> {
        let Some(job) = key.job() else {
            return self.failures.clone();
        };
        self.failure_sender_for_job_key(job)
    }

    fn failure_sender_for_job_key(&self, key: &JobSessionKey) -> flume::Sender<Error> {
        let mut channels = self
            .failure_channels
            .write()
            .expect("failure channel registry lock");
        channels
            .entry(key.clone())
            .or_insert_with(|| {
                let (sender, receiver) = flume::bounded(self.config.max_failure_queue);
                FailureChannel { sender, receiver }
            })
            .sender
            .clone()
    }

    fn failure_sender_for_registered_job(
        &self,
        key: &JobSessionKey,
    ) -> Option<flume::Sender<Error>> {
        self.failure_channels
            .read()
            .expect("failure channel registry lock")
            .get(key)
            .map(|channel| channel.sender.clone())
    }

    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Registers the local input channel that remote peer quad writes into
    /// (downstream side of a remote edge). The channel's sender drops when the
    /// peer's connection dies, ending the chain's input exactly like a local
    /// upstream finishing (or failing — at-least-once recovery handles both).
    pub fn register_inbound(&self, quad: Quad, sender: flume::Sender<Envelope>) {
        self.inbound
            .write()
            .expect("inbound registry lock")
            .insert(EdgeSessionKey::legacy(quad), sender);
        self.inbound_auth
            .write()
            .expect("inbound auth registry lock")
            .remove(&EdgeSessionKey::legacy(quad));
    }

    /// Registers a downstream quad together with the identity that is allowed
    /// to open it.  In-memory test managers without credentials retain the
    /// old transport behavior; configured data planes require this binding.
    pub fn register_inbound_for_session(
        &self,
        quad: Quad,
        sender: flume::Sender<Envelope>,
        expectation: PeerExpectation,
    ) -> Result<(), Error> {
        if self.config.credentials.is_none() {
            self.claim_legacy_job_key(JobSessionKey {
                job_id: expectation.job_id.clone(),
                generation: expectation.generation,
            })?;
            self.register_inbound(quad, sender);
            return Ok(());
        }
        let key = expectation.edge_key(quad);
        self.inbound
            .write()
            .expect("inbound registry lock")
            .insert(key.clone(), sender);
        self.inbound_auth
            .write()
            .expect("inbound auth registry lock")
            .insert(key, expectation);
        Ok(())
    }

    fn remove_inbound_session(&self, key: &EdgeSessionKey) {
        self.inbound
            .write()
            .expect("inbound registry lock")
            .remove(key);
        self.inbound_auth
            .write()
            .expect("inbound auth registry lock")
            .remove(key);
    }

    /// The per-session-key delivery lock (see `delivery_locks`). Shared by
    /// every connection serving the key, so the check→send→record sequence
    /// is atomic with respect to overlapping connections.
    fn delivery_lock(&self, key: &EdgeSessionKey) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.delivery_locks.lock().expect("delivery lock registry");
        locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    fn claim_legacy_job_key(&self, key: JobSessionKey) -> Result<(), Error> {
        let mut current = self.legacy_job.write().expect("legacy job registry lock");
        match current.as_ref() {
            None => {
                *current = Some(key);
                Ok(())
            }
            Some(existing) if existing == &key => Ok(()),
            Some(existing) => Err(Error::Config(format!(
                "unauthenticated remote edge transport already serves Job '{}' generation {}; it cannot carry Job '{}' generation {}",
                existing.job_id, existing.generation, key.job_id, key.generation
            ))),
        }
    }

    /// Remove every data-plane registration owned by one Job attempt.  This is
    /// called after the kernel has stopped (and on failed graph construction)
    /// so a Job that never established a connection does not leave a failure
    /// channel or route behind for future generations.
    pub fn remove_job_session(&self, job_id: &str, generation: u64) {
        let job = JobSessionKey {
            job_id: job_id.to_owned(),
            generation,
        };
        let legacy_owned = self
            .legacy_job
            .read()
            .expect("legacy job registry lock")
            .as_ref()
            == Some(&job);
        let outbound_keys = {
            let outbound = self.outbound.read().expect("outbound registry lock");
            outbound
                .keys()
                .filter(|key| key.job() == Some(&job) || (legacy_owned && key.job().is_none()))
                .cloned()
                .collect::<Vec<_>>()
        };
        self.session_receipts
            .lock()
            .expect("session receipt lock")
            .retain(|key, route| {
                let keep = key.job() != Some(&job) && !(legacy_owned && key.job().is_none());
                if !keep {
                    route.forwarder.cancel();
                }
                keep
            });
        self.delivered_seq
            .lock()
            .expect("delivered seq lock")
            .retain(|key, _| key.job() != Some(&job) && !(legacy_owned && key.job().is_none()));
        self.delivery_locks
            .lock()
            .expect("delivery lock registry")
            .retain(|key, _| key.job() != Some(&job) && !(legacy_owned && key.job().is_none()));
        self.session_registrations
            .lock()
            .expect("session registration lock")
            .retain(|key, _| key.job() != Some(&job) && !(legacy_owned && key.job().is_none()));
        self.inbound
            .write()
            .expect("inbound registry lock")
            .retain(|key, _| key.job() != Some(&job) && !(legacy_owned && key.job().is_none()));
        self.inbound_auth
            .write()
            .expect("inbound auth registry lock")
            .retain(|key, _| key.job() != Some(&job) && !(legacy_owned && key.job().is_none()));
        let pending = {
            let mut outbound = self.outbound.write().expect("outbound registry lock");
            outbound_keys
                .iter()
                .filter_map(|key| outbound.remove(key))
                .collect::<Vec<_>>()
        };
        for receipts in pending {
            receipts.abort_all();
        }
        self.failure_channels
            .write()
            .expect("failure channel registry lock")
            .remove(&job);
        let mut legacy = self.legacy_job.write().expect("legacy job registry lock");
        if legacy.as_ref() == Some(&job) {
            *legacy = None;
        }
    }

    fn claim_handshake_nonce(&self, nonce: &[u8]) -> Result<(), Error> {
        let now = std::time::Instant::now();
        let mut nonces = self
            .handshake_nonces
            .lock()
            .expect("handshake nonce registry lock");
        nonces.retain(|_, seen| {
            now.checked_duration_since(*seen)
                .is_some_and(|age| age <= self.config.handshake_replay_ttl)
        });
        if nonces.contains_key(nonce) {
            return Err(Error::Authentication(
                "remote edge handshake nonce was replayed".into(),
            ));
        }
        let capacity = self.config.max_connections.saturating_mul(4).max(1);
        if nonces.len() >= capacity {
            return Err(Error::RateLimit(
                "remote edge handshake replay cache is full".into(),
            ));
        }
        nonces.insert(nonce.to_vec(), now);
        Ok(())
    }

    /// Injects an accepted stream (server side). The TCP listener task calls
    /// this per `TcpStream`; tests inject duplex halves.
    pub fn accept_stream(&self, stream: Box<dyn RemoteStream>) {
        let current = self.active_connections.try_update(
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Relaxed,
            |count| (count < self.config.max_connections).then_some(count + 1),
        );
        if current.is_err() {
            self.report_failure(Error::Process(format!(
                "remote edge accepted-connection limit {} reached",
                self.config.max_connections
            )));
            return;
        }
        if self.accepted.0.try_send(stream).is_err() {
            self.active_connections
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            self.report_failure(Error::Process(
                "remote edge accepted-connection queue is full".into(),
            ));
        }
    }

    /// Runs the accept loop until shutdown. Call once per manager.
    /// The configured fleet-CA mTLS material, when enabled. Outbound edge
    /// contexts carry this so remote transports wrap their connections.
    pub fn tls_config(&self) -> Option<&DataPlaneTlsConfig> {
        self.config.tls.as_ref()
    }

    pub fn spawn(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let manager = self.clone();
        tokio::spawn(async move {
            loop {
                let stream = tokio::select! {
                    _ = manager.shutdown.cancelled() => break,
                    stream = manager.accepted.1.recv_async() => match stream {
                        Ok(stream) => stream,
                        Err(_) => break,
                    },
                };
                let per_stream = manager.clone();
                tokio::spawn(async move {
                    per_stream.clone().serve_stream(stream).await;
                    per_stream
                        .active_connections
                        .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                });
            }
        })
    }

    /// Binds the TCP data-plane listener and feeds accepted connections into
    /// the serve loop. Returns the bound port (for registry advertisement).
    pub async fn bind_tcp(self: &Arc<Self>, addr: std::net::SocketAddr) -> Result<u16, Error> {
        if self.config.credentials.is_none() {
            // Serving without credentials would skip the session handshake for
            // every inbound connection; refuse the bind instead.
            return Err(Error::Config(
                "refusing to bind the data-plane listener without credentials".into(),
            ));
        }
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|error| Error::Process(format!("data plane bind failed: {error}")))?;
        let port = listener
            .local_addr()
            .map_err(|error| Error::Process(format!("data plane local addr failed: {error}")))?
            .port();
        let manager = self.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    _ = shutdown.cancelled() => break,
                    accepted = listener.accept() => accepted,
                };
                match accepted {
                    Ok((stream, _peer)) => {
                        let _ = stream.set_nodelay(true);
                        match manager.config.tls.clone() {
                            // Inbound TLS first: the peer must present a
                            // fleet-CA certificate before any frame (and
                            // before the HMAC session handshake) is read.
                            // The handshake runs OFF the accept loop so one
                            // slow or malicious peer cannot stall new
                            // connections; failures take the
                            // connection-failure path — never a plaintext
                            // fallback.
                            Some(tls) => {
                                let handshake_manager = manager.clone();
                                tokio::spawn(async move {
                                    match tls.accept(stream).await {
                                        Ok(tls_stream) => {
                                            handshake_manager.accept_stream(Box::new(tls_stream));
                                        }
                                        Err(error) => {
                                            handshake_manager.report_failure(error);
                                        }
                                    }
                                });
                            }
                            None => manager.accept_stream(Box::new(stream)),
                        }
                    }
                    Err(error) => {
                        tracing::warn!("data plane accept failed: {error}");
                        break;
                    }
                }
            }
        });
        Ok(port)
    }

    /// Opens the upstream side of a remote edge: connects, spawns the outbound
    /// pump and receipt read loop, and returns the edge channel sender to drop
    /// into `EdgeTarget` channel vectors.
    ///
    /// Test-only: bypasses the authenticated session handshake. Production
    /// code opens edges through [`Self::open_edge_for_session`] or
    /// [`Self::open_edge_deferred_for_session`].
    #[cfg(test)]
    pub async fn open_edge(
        self: &Arc<Self>,
        transport: &dyn EdgeTransport,
        quad: Quad,
    ) -> Result<RemoteEdge, Error> {
        let stream = transport.connect(quad).await?;
        Ok(self.open_edge_with_stream(stream, quad))
    }

    /// Opens an authenticated edge using the manager's configured shared
    /// secret and the Job/generation/peer identity supplied by graph build.
    pub async fn open_edge_for_session(
        self: &Arc<Self>,
        transport: &dyn EdgeTransport,
        quad: Quad,
        peer_node: &str,
        job_id: &str,
        generation: u64,
    ) -> Result<RemoteEdge, Error> {
        let stream = transport.connect(quad).await?;
        let auth = self.session_auth(peer_node, job_id, generation, quad)?;
        Ok(self.open_edge_with_auth(stream, quad, Some(auth)))
    }

    /// Like [`Self::open_edge`] but over a caller-provided stream (tests,
    /// without the session handshake).
    #[cfg(test)]
    pub fn open_edge_with_stream(
        self: &Arc<Self>,
        stream: Box<dyn RemoteStream>,
        quad: Quad,
    ) -> RemoteEdge {
        self.open_edge_with_auth(stream, quad, None)
    }

    pub fn open_edge_with_stream_for_session(
        self: &Arc<Self>,
        stream: Box<dyn RemoteStream>,
        quad: Quad,
        peer_node: &str,
        job_id: &str,
        generation: u64,
    ) -> Result<RemoteEdge, Error> {
        let auth = self.session_auth(peer_node, job_id, generation, quad)?;
        Ok(self.open_edge_with_auth(stream, quad, Some(auth)))
    }

    fn open_edge_with_auth(
        self: &Arc<Self>,
        stream: Box<dyn RemoteStream>,
        quad: Quad,
        auth: Option<SessionAuth>,
    ) -> RemoteEdge {
        let (sender, receiver) = flume::bounded::<Envelope>(self.config.channel_capacity);
        let pending = Arc::new(PendingReceipts::with_byte_budget(
            self.config.max_pending_receipts,
            self.config.max_pending_bytes,
        ));
        let session_key = auth
            .as_ref()
            .map(SessionAuth::edge_key)
            .unwrap_or_else(|| EdgeSessionKey::legacy(quad));
        self.outbound
            .write()
            .expect("outbound registry lock")
            .insert(session_key.clone(), pending.clone());
        self.attach_stream(stream, quad, receiver, pending, auth, session_key);
        RemoteEdge { sender }
    }

    /// Like [`Self::open_edge_with_stream`] but connects on a background task,
    /// so synchronous graph builders can wire remote edges without awaiting.
    /// The edge channel already accepts envelopes while connecting — its bound
    /// applies backpressure. A failed connect fails the edge closed: pending
    /// branch acknowledgements abort and the sender's sends error out.
    ///
    /// Test-only: bypasses the authenticated session handshake. Production
    /// graph builders open edges through [`Self::open_edge_deferred_for_session`].
    #[cfg(test)]
    pub fn open_edge_deferred(
        self: &Arc<Self>,
        transport: Arc<dyn EdgeTransport>,
        quad: Quad,
    ) -> RemoteEdge {
        self.open_edge_deferred_with_key(transport, quad, EdgeSessionKey::legacy(quad), None)
    }

    fn open_edge_deferred_with_key(
        self: &Arc<Self>,
        transport: Arc<dyn EdgeTransport>,
        quad: Quad,
        session_key: EdgeSessionKey,
        auth: Option<SessionAuth>,
    ) -> RemoteEdge {
        let (sender, receiver) = flume::bounded::<Envelope>(self.config.channel_capacity);
        let pending = Arc::new(PendingReceipts::with_byte_budget(
            self.config.max_pending_receipts,
            self.config.max_pending_bytes,
        ));
        self.outbound
            .write()
            .expect("outbound registry lock")
            .insert(session_key.clone(), pending.clone());
        let manager = self.clone();
        let failures = self.failure_sender_for_key(&session_key);
        tokio::spawn(async move {
            match transport.connect(quad).await {
                Ok(stream) => {
                    manager.attach_stream_with_redial(
                        stream,
                        quad,
                        receiver,
                        pending,
                        auth,
                        session_key.clone(),
                        Some(transport),
                    );
                }
                Err(error) => {
                    let _ = failures
                        .send_async(Error::Process(format!(
                            "remote edge connect for quad {quad:?} failed: {error}"
                        )))
                        .await;
                    pending.abort_all();
                    // Remove the stale registry entry so a later edge for the
                    // same quad does not route receipts into an aborted map.
                    self_remove_outbound(&manager, &session_key);
                    // Drop the receiver: the graph's sender side observes the
                    // closed edge on its next send.
                    drop(receiver);
                }
            }
        });
        RemoteEdge { sender }
    }

    pub fn open_edge_deferred_for_session(
        self: &Arc<Self>,
        transport: Arc<dyn EdgeTransport>,
        quad: Quad,
        peer_node: String,
        job_id: String,
        generation: u64,
    ) -> Result<RemoteEdge, Error> {
        let auth = self.session_auth(&peer_node, &job_id, generation, quad)?;
        let session_key = auth.edge_key();
        Ok(self.open_edge_deferred_with_key(transport, quad, session_key, Some(auth)))
    }

    /// Opens a deferred, authenticated edge for a planned remote edge. A
    /// manager without data-plane credentials fails the graph build here
    /// instead of degrading to an unauthenticated edge.
    pub fn open_edge_deferred_for_job(
        self: &Arc<Self>,
        transport: Arc<dyn EdgeTransport>,
        quad: Quad,
        peer_node: String,
        job_id: String,
        generation: u64,
    ) -> Result<RemoteEdge, Error> {
        self.open_edge_deferred_for_session(transport, quad, peer_node, job_id, generation)
    }

    fn session_auth(
        &self,
        peer_node: &str,
        job_id: &str,
        generation: u64,
        quad: Quad,
    ) -> Result<SessionAuth, Error> {
        self.config
            .credentials
            .as_ref()
            .map(|credentials| credentials.session(peer_node, job_id, generation, quad))
            .ok_or_else(|| {
                Error::Config(
                    "authenticated remote edge requested without data-plane credentials".into(),
                )
            })
    }

    /// Records a session loss and returns the registration generation the
    /// grace watcher should compare against. The generation is set BELOW the
    /// next registration so the first (re)registration after the loss makes
    /// the comparison mismatch and suppresses the deferred failure.
    fn bump_session_loss(&self, key: &EdgeSessionKey) -> u64 {
        let mut registrations = self
            .session_registrations
            .lock()
            .expect("session registration lock");
        let next = registrations.get(key).copied().unwrap_or(0) + 1;
        // Ensure any re-registration strictly exceeds this snapshot.
        registrations.insert(key.clone(), next);
        next
    }

    /// Marks a session key as (re)registered by a live connection, advancing
    /// its generation so pending loss watchers observe the recovery.
    fn bump_session_registration(&self, key: &EdgeSessionKey) {
        let mut registrations = self
            .session_registrations
            .lock()
            .expect("session registration lock");
        let next = registrations.get(key).copied().unwrap_or(0) + 1;
        registrations.insert(key.clone(), next);
    }

    pub(crate) fn report_failure(&self, error: Error) {
        // This path is synchronous because it is also used by the listener
        // and Ack callbacks. A blocking send preserves the bounded queue's
        // backpressure without silently losing the failure that must cancel
        // the affected edge.
        let _ = self.failures.send(error);
    }

    /// Spawns the receipt read loop and outbound pump over an established
    /// stream (shared by the eager and deferred open paths).
    fn attach_stream(
        self: &Arc<Self>,
        stream: Box<dyn RemoteStream>,
        quad: Quad,
        receiver: flume::Receiver<Envelope>,
        pending: Arc<PendingReceipts>,
        auth: Option<SessionAuth>,
        session_key: EdgeSessionKey,
    ) {
        self.attach_stream_with_redial(stream, quad, receiver, pending, auth, session_key, None);
    }

    /// [`Self::attach_stream`] with a redial handle: when the transport is
    /// provided, a stream-level failure transparently reconnects within the
    /// configured attempt budget, replaying every unacknowledged data frame
    /// (the receiver dedups by sequence number). Budget exhaustion, shutdown,
    /// or a session removed mid-recovery fails the edge closed exactly as a
    /// stream failure does without reconnect.
    #[allow(clippy::too_many_arguments)]
    fn attach_stream_with_redial(
        self: &Arc<Self>,
        stream: Box<dyn RemoteStream>,
        quad: Quad,
        receiver: flume::Receiver<Envelope>,
        pending: Arc<PendingReceipts>,
        auth: Option<SessionAuth>,
        session_key: EdgeSessionKey,
        redial: Option<Arc<dyn EdgeTransport>>,
    ) {
        // A deferred connect can finish after its Job has been stopped.  Do
        // not attach that late stream or recreate the failure channel that
        // `remove_job_session` deliberately dropped.
        if !self
            .outbound
            .read()
            .expect("outbound registry lock")
            .contains_key(&session_key)
        {
            pending.abort_all();
            return;
        }
        let failures = self.failure_sender_for_key(&session_key);
        let config = self.config.clone();
        let manager = self.clone();
        let reconnectable = redial.is_some() && config.reconnect_attempts > 0;
        tokio::spawn(async move {
            let mut attempts_left = config.reconnect_attempts;
            let mut current = Some(stream);
            let mut final_error: Option<Error> = None;
            let mut clean_exit = false;
            while let Some(stream) = current.take() {
                let (pump_result, read_result) = run_edge_connection(
                    stream,
                    quad,
                    receiver.clone(),
                    pending.clone(),
                    config.clone(),
                    auth.clone(),
                    reconnectable,
                    failures.clone(),
                    &manager.shutdown,
                )
                .await;
                // Clean requires BOTH halves to end cleanly: the read loop
                // already classifies a post-drain peer close as Ok, so any Err
                // on either half is a stream-level failure.
                let error = pump_result.err().or(read_result.err());
                match error {
                    None => {
                        clean_exit = true;
                        break;
                    }
                    Some(error) => {
                        // Either the pump hit a wire failure or the read half
                        // dropped the connection; both are stream-level. Retry
                        // within the budget when a redial handle exists and the
                        // session is still live.
                        let retry = reconnectable
                            && attempts_left > 0
                            && !manager.shutdown.is_cancelled()
                            && manager
                                .outbound
                                .read()
                                .expect("outbound registry lock")
                                .contains_key(&session_key);
                        if !retry {
                            final_error = Some(error);
                            break;
                        }
                        attempts_left -= 1;
                        tracing::warn!(
                            attempts_left,
                            quad = ?quad,
                            "remote edge stream failed; transparently reconnecting"
                        );
                        tokio::time::sleep(connect_backoff(
                            config.reconnect_attempts - attempts_left,
                        ))
                        .await;
                        let redial = redial.clone().expect("retry implies redial");
                        match redial.connect(quad).await {
                            Ok(mut stream) => {
                                if let Err(error) =
                                    replay_pending(&mut stream, quad, &pending, &auth, &config)
                                        .await
                                {
                                    final_error = Some(error);
                                    break;
                                }
                                current = Some(stream);
                            }
                            Err(error) => {
                                final_error = Some(Error::Process(format!(
                                    "remote edge reconnect for quad {quad:?} failed: {error}"
                                )));
                                break;
                            }
                        }
                    }
                }
            }
            let failed = final_error.is_some();
            if let Some(error) = final_error {
                let _ = failures.send_async(error).await;
            }
            if !clean_exit || failed {
                // The connection is gone and no receipt can arrive: settle
                // every still-registered branch through the abort path.
                pending.abort_all();
            }
            self_remove_outbound(&manager, &session_key);
        });
    }

    /// Handles one inbound connection (downstream side): decodes data frames
    /// into the registered local channel, forwards signals, and writes the
    /// receipts produced by `RemoteAck`s on this connection back upstream.
    async fn serve_stream(self: Arc<Self>, stream: Box<dyn RemoteStream>) {
        let result = self.serve_stream_inner(stream).await;
        if let Err(failure) = result {
            let sender = match failure.job.as_ref() {
                Some(job) => self.failure_sender_for_registered_job(job),
                None => Some(self.failures.clone()),
            };
            let Some(sender) = sender else {
                // The Job was stopped while the connection was unwinding. Its
                // per-session receiver has already been removed, so reporting
                // this late error must not recreate stale state or fall into a
                // different Job's global queue.
                return;
            };
            // A connection error is part of the Job's failure contract.  Do
            // not use try_send here: a full bounded queue must apply
            // backpressure rather than silently dropping the only signal that
            // can cancel the affected Job.
            let _ = sender.send_async(failure.error).await;
        }
    }

    async fn serve_stream_inner(
        self: &Arc<Self>,
        stream: Box<dyn RemoteStream>,
    ) -> Result<(), ConnectionFailure> {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let (receipt_tx, receipt_rx) =
            flume::bounded::<(Quad, ReceiptFrame)>(self.config.max_receipt_queue);
        let mut decoder = DataDecoder::new();
        let mut served_quads: BTreeMap<EdgeSessionKey, flume::Sender<Envelope>> = BTreeMap::new();
        let mut eos_seen: BTreeSet<EdgeSessionKey> = BTreeSet::new();
        // Session keys this connection registered, advanced once per key so a
        // reconnecting connection marks recovery for pending loss watchers.
        let mut registered_keys: BTreeSet<EdgeSessionKey> = BTreeSet::new();
        // (session key, our receipt sender) pairs whose writer slots this
        // connection installed; released with identity guards on exit.
        let mut writer_slots_installed: Vec<(EdgeSessionKey, flume::Sender<(Quad, ReceiptFrame)>)> =
            Vec::new();
        let mut authenticated_session = None;
        let mut failure_sender = self.failures.clone();
        let connection_cancel = self.shutdown.child_token();

        if let Some(credentials) = self.config.credentials.as_ref() {
            let (header, payload) = read_frame_with_limits(
                &mut reader,
                self.config.max_frame_len,
                Some(self.config.read_idle_timeout),
            )
            .await?;
            let (session, request) = self.authorize_inbound(header, &payload).await?;
            let job = JobSessionKey {
                job_id: request.job_id.clone(),
                generation: request.generation,
            };
            let session_key =
                EdgeSessionKey::for_job(request.job_id.clone(), request.generation, request.quad);
            failure_sender = self.failure_sender_for_job_key(&job);
            authenticated_session = Some(session_key.clone());
            let response = session.server_ack(&request, &credentials.local_node);
            let response_payload = match serde_json::to_vec(&response) {
                Ok(payload) => payload,
                Err(error) => {
                    self.remove_inbound_session(&session_key);
                    return Err(ConnectionFailure {
                        job: Some(job.clone()),
                        error: Error::Process(format!(
                            "remote edge handshake encode failed: {error}"
                        )),
                    });
                }
            };
            if let Err(error) = write_frame_with_limit(
                &mut writer,
                request.quad,
                FrameKind::Handshake,
                &response_payload,
                self.config.max_frame_len,
            )
            .await
            {
                self.remove_inbound_session(&session_key);
                return Err(ConnectionFailure {
                    job: Some(job.clone()),
                    error,
                });
            }
            if let Err(error) = writer.flush().await {
                self.remove_inbound_session(&session_key);
                return Err(ConnectionFailure {
                    job: Some(job.clone()),
                    error: Error::Process(format!("remote edge handshake flush failed: {error}")),
                });
            }
            if wait_for_route(
                &self.inbound,
                &mut served_quads,
                session_key.clone(),
                self.config.registration_grace,
            )
            .await
            .is_none()
            {
                self.remove_inbound_session(&session_key);
                return Err(ConnectionFailure {
                    job: Some(job),
                    error: Error::Process(format!(
                        "authenticated remote edge quad {:?} is no longer registered",
                        request.quad
                    )),
                });
            }
        }

        // Receipt writer: drains RemoteAck outboxes onto the wire, flushed
        // immediately — receipts gate upstream source progress.
        let receipt_config = self.config.clone();
        let receipt_cancel = connection_cancel.clone();
        let receipt_failures = failure_sender.clone();
        let receipt_writer_failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let receipt_writer_failed_task = receipt_writer_failed.clone();
        let receipt_writer_handle = tokio::spawn(async move {
            let mut writer = tokio::io::BufWriter::with_capacity(16 * 1024, writer);
            let result: Result<(), Error> = loop {
                let send = tokio::select! {
                    _ = receipt_cancel.cancelled() => {
                        break Ok(());
                    }
                    next = receipt_rx.recv_async() => next,
                };
                let Ok((quad, receipt)) = send else {
                    break Ok(());
                };
                let payload = match serde_json::to_vec(&receipt) {
                    Ok(payload) => payload,
                    Err(error) => {
                        break Err(Error::Process(format!(
                            "remote edge receipt encode failed: {error}"
                        )))
                    }
                };
                if let Err(error) = write_frame_with_limit(
                    &mut writer,
                    quad,
                    FrameKind::Receipt,
                    &payload,
                    receipt_config.max_frame_len,
                )
                .await
                {
                    break Err(Error::Process(format!(
                        "remote edge receipt write failed: {error}"
                    )));
                }
                if let Err(error) = writer.flush().await {
                    break Err(Error::Process(format!(
                        "remote edge receipt flush failed: {error}"
                    )));
                }
            };
            if let Err(error) = result {
                receipt_writer_failed_task.store(true, std::sync::atomic::Ordering::Release);
                receipt_cancel.cancel();
                // This is deliberately awaited.  Receipt write failure is a
                // fatal data-plane event and must not disappear when the
                // connection task is cleaned up.
                let _ = receipt_failures.send_async(error).await;
            }
        });

        // Quads whose chain observed a forwarded Eos. A connection that ends
        // WITHOUT Eos for a served quad is an upstream death mid-stream, not a
        // clean finish: the chain must fail (at-least-once) instead of
        // silently publishing a prefix and reporting the subgraph finished.
        let failure_reason: Result<(), Error> = loop {
            let frame = tokio::select! {
                _ = connection_cancel.cancelled() => break Ok(()),
                frame = read_frame_with_limits(
                    &mut reader,
                    self.config.max_frame_len,
                    Some(self.config.read_idle_timeout),
                ) => frame,
            };
            match frame {
                Ok((header, payload)) => {
                    if header.kind == FrameKind::Handshake {
                        break Err(Error::Process("duplicate remote edge handshake".into()));
                    }
                    let route_key = authenticated_session
                        .clone()
                        .unwrap_or_else(|| EdgeSessionKey::legacy(header.quad));
                    if authenticated_session
                        .as_ref()
                        .is_some_and(|session| session.quad != header.quad)
                    {
                        break Err(Error::Process(format!(
                            "remote edge frame quad {:?} does not match authenticated quad {:?}",
                            header.quad, authenticated_session
                        )));
                    }
                    let sender = match wait_for_route(
                        &self.inbound,
                        &mut served_quads,
                        route_key.clone(),
                        self.config.registration_grace,
                    )
                    .await
                    {
                        Some(sender) => sender,
                        None => {
                            break Err(Error::Process(format!(
                                "inbound frame for unregistered quad {:?}",
                                header.quad
                            )))
                        }
                    };
                    if registered_keys.insert(route_key.clone()) {
                        self.bump_session_registration(&route_key);
                    }
                    // This connection now serves the session's receipts:
                    // create the route (idempotent) and take the writer slot
                    // (recording the sender so the guarded release on exit
                    // can tell ours from a replacement's).
                    let _ = self.session_receipt_route(&route_key);
                    let ours = receipt_tx.clone();
                    self.set_session_receipt_writer(&route_key, Some(ours.clone()));
                    writer_slots_installed.push((route_key.clone(), ours));
                    match header.kind {
                        FrameKind::Data => {
                            let (batch, seq) = match decoder.decode(&payload) {
                                Ok(decoded) => decoded,
                                Err(error) => {
                                    break Err(Error::Process(format!(
                                        "inbound data frame decode failed: {error}"
                                    )))
                                }
                            };
                            // Delivery-level dedup: a transparent reconnect
                            // replays every frame the upstream has not
                            // receipted. A sequence at or below the last one
                            // DELIVERED (recorded only after the local
                            // channel accepted it) is dropped silently: its
                            // original acknowledgement is still in flight
                            // through the session-scoped receipt route, which
                            // survives the connection swap. No mirror-ack —
                            // completing the upstream branch before the local
                            // chain finishes processing would let source
                            // offsets advance past unprocessed data.
                            // Serialize check→deliver→record per session key:
                            // during a transparent reconnect the old
                            // connection can still be parked on a
                            // backpressured send while the replacement
                            // replays the same sequences. The lock makes the
                            // second passer see the first one's delivered
                            // mark, and the max-update below keeps a late
                            // completion from regressing it.
                            let delivery_lock = self.delivery_lock(&route_key);
                            let _delivery_guard = delivery_lock.lock().await;
                            let duplicate = {
                                let delivered =
                                    self.delivered_seq.lock().expect("delivered seq lock");
                                match delivered.get(&route_key) {
                                    Some(last) => seq <= *last,
                                    None => false,
                                }
                            };
                            if duplicate {
                                continue;
                            }
                            // Session-scoped receipts: the ack hands its
                            // receipt to the session queue, so it completes
                            // on whichever connection serves the session by
                            // the time processing finishes.
                            let outbox = self.session_receipt_route(&route_key);
                            let envelope = Envelope::Data(
                                Arc::new(batch),
                                Arc::new(RemoteAck {
                                    outbox,
                                    quad: header.quad,
                                    seq,
                                    failures: failure_sender.clone(),
                                }),
                            );
                            if sender.send_async(envelope).await.is_err() {
                                break Ok(()); // local chain gone; close the edge
                            }
                            {
                                let mut delivered =
                                    self.delivered_seq.lock().expect("delivered seq lock");
                                let mark = delivered.entry(route_key.clone()).or_insert(0);
                                if seq > *mark {
                                    *mark = seq;
                                }
                            }
                        }
                        FrameKind::Signal => {
                            let signal = match serde_json::from_slice::<WireSignal>(&payload) {
                                Ok(signal) => signal,
                                Err(error) => {
                                    break Err(Error::Process(format!(
                                        "inbound signal frame malformed: {error}"
                                    )))
                                }
                            };
                            if matches!(signal, WireSignal::Eos) {
                                eos_seen.insert(route_key);
                            }
                            if sender.send_async(signal.into_envelope()).await.is_err() {
                                break Ok(());
                            }
                        }
                        FrameKind::Receipt => {
                            break Err(Error::Process(
                                "receipt frame on an inbound (downstream) connection".into(),
                            ));
                        }
                        FrameKind::Handshake => unreachable!("handled before frame dispatch"),
                    }
                }
                Err(error) => {
                    break Err(error);
                }
            }
        };

        connection_cancel.cancel();

        // Session-loss reporting: with transparent reconnect enabled, a lost
        // connection without Eos defers its failure by the reconnect grace —
        // a re-registering upstream within the window suppresses it (the
        // supervisor on the peer redials and replays unacknowledged frames).
        // Protocol-level errors fail immediately: they recur deterministically.
        let reconnect_enabled = self.config.reconnect_attempts > 0;
        let report = |key: EdgeSessionKey, message: String| {
            if !reconnect_enabled {
                let sender = failure_sender.clone();
                tokio::spawn(async move {
                    let _ = sender.send_async(Error::Process(message)).await;
                });
                return;
            }
            let generation = self.bump_session_loss(&key);
            let manager = self.clone();
            let sender = failure_sender.clone();
            let grace = self.config.reconnect_grace;
            tokio::spawn(async move {
                tokio::time::sleep(grace).await;
                if manager.shutdown.is_cancelled() {
                    return;
                }
                let current = manager
                    .session_registrations
                    .lock()
                    .expect("session registration lock")
                    .get(&key)
                    .copied();
                if current.is_some() && current != Some(generation) {
                    // The upstream re-registered: the loss was recovered.
                    return;
                }
                // Confirmed loss: the registration kept for the redialing
                // peer is now stale and must not leak into later jobs.
                manager.remove_session_receipt_route(&key);
                manager
                    .inbound
                    .write()
                    .expect("inbound registry lock")
                    .remove(&key);
                manager
                    .inbound_auth
                    .write()
                    .expect("inbound auth registry lock")
                    .remove(&key);
                let _ = sender.send_async(Error::Process(message)).await;
            });
        };

        if let Err(error) = failure_reason {
            let mut reported_without_eos = false;
            let defer_loss = is_unexpected_eof(&error);
            for key in served_quads.keys() {
                if !eos_seen.contains(key) {
                    reported_without_eos = true;
                    let message = format!(
                        "remote edge for quad {:?} ended without Eos: {error}",
                        key.quad
                    );
                    if defer_loss {
                        report(key.clone(), message);
                    } else {
                        let sender = failure_sender.clone();
                        let _ = sender.send_async(Error::Process(message)).await;
                    }
                }
            }
            // A peer that forwarded Eos for every served quad is allowed to
            // close its write half.  `read_frame_with_limits` reports that
            // close as UnexpectedEof, but it is the normal completion signal,
            // not a failed remote Job.  Keep reporting protocol and transport
            // errors; only suppress the clean EOF case.
            let clean_eos_close =
                !served_quads.is_empty() && !reported_without_eos && is_unexpected_eof(&error);
            if !clean_eos_close && !reported_without_eos {
                let _ = failure_sender.send_async(error).await;
            }
        } else if !self.shutdown.is_cancelled()
            && !receipt_writer_failed.load(std::sync::atomic::Ordering::Acquire)
        {
            // Dropping the served quads' senders closes the chains' input
            // channels. A quad whose connection died without a forwarded Eos
            // is an upstream death mid-stream, not a clean finish.
            for (key, sender) in &served_quads {
                if !eos_seen.contains(key) {
                    report(
                        key.clone(),
                        format!(
                            "remote edge for quad {:?} ended without Eos; upstream likely died mid-stream",
                            key.quad
                        ),
                    );
                    let _ = sender;
                }
            }
        }

        // Dropping the served quads' senders closes the chains' input channels.
        // Release the session writer slots this connection still owns. A
        // reconnecting replacement may have already installed its own
        // sender, so the clear is guarded by channel identity — clobbering
        // the live slot would stall receipts until the next reconnect.
        for (key, ours) in &writer_slots_installed {
            self.clear_session_receipt_writer_if_owned(key, ours);
        }

        // Only entries this connection inserted are removed; a reconnecting
        // upstream re-registers through the graph anyway. A loss under the
        // reconnect grace keeps the registration so the peer's redialing
        // connection can re-route immediately: the confirmed-loss watcher
        // removes it instead when the grace expires without recovery.
        for (key, sender) in served_quads {
            let loss_deferred =
                reconnect_enabled && !eos_seen.contains(&key) && !self.shutdown.is_cancelled();
            if loss_deferred {
                continue;
            }
            let mut registry = self.inbound.write().expect("inbound registry lock");
            if registry
                .get(&key)
                .is_some_and(|registered| registered.same_channel(&sender))
            {
                registry.remove(&key);
                self.inbound_auth
                    .write()
                    .expect("inbound auth registry lock")
                    .remove(&key);
            }
        }
        if receipt_writer_failed.load(std::sync::atomic::Ordering::Acquire) {
            let _ = receipt_writer_handle.await;
        } else {
            receipt_writer_handle.abort();
        }
        Ok(())
    }

    pub(crate) async fn authorize_inbound(
        &self,
        header: FrameHeader,
        payload: &[u8],
    ) -> Result<(SessionAuth, HandshakePayload), Error> {
        if header.kind != FrameKind::Handshake {
            return Err(Error::Process(
                "remote edge frame arrived before handshake".into(),
            ));
        }
        let request: HandshakePayload = serde_json::from_slice(payload)
            .map_err(|error| Error::Process(format!("remote edge handshake malformed: {error}")))?;
        let credentials =
            self.config.credentials.as_ref().ok_or_else(|| {
                Error::Process("remote edge credentials are not configured".into())
            })?;
        if request.protocol_version != DATA_PLANE_PROTOCOL_VERSION
            || request.acknowledgement
            || request.quad != header.quad
            || request.destination_node != credentials.local_node
            || request.nonce.len() != HANDSHAKE_NONCE_LEN
        {
            return Err(Error::Process(
                "remote edge handshake protocol or identity mismatch".into(),
            ));
        }
        verify_handshake_mac(&credentials.shared_secret, &request, b"client")?;
        self.claim_handshake_nonce(&request.nonce)?;
        let session_key =
            EdgeSessionKey::for_job(request.job_id.clone(), request.generation, request.quad);
        let deadline = tokio::time::Instant::now() + self.config.registration_grace;
        loop {
            let expected = {
                let registry = self
                    .inbound_auth
                    .read()
                    .expect("inbound auth registry lock");
                registry.get(&session_key).cloned().or_else(|| {
                    // Keep a useful stale-generation diagnostic without
                    // authorizing against a different session. The exact key
                    // above remains the only successful lookup.
                    registry
                        .iter()
                        .find(|(key, _)| key.quad == request.quad)
                        .map(|(_, expected)| expected.clone())
                })
            };
            if let Some(expected) = expected {
                if expected.source_node != request.source_node
                    || expected.job_id != request.job_id
                    || expected.generation != request.generation
                {
                    return Err(Error::Process(format!(
                        "remote edge handshake authorization mismatch for quad {:?}",
                        request.quad
                    )));
                }
                return Ok((
                    credentials.session(
                        request.source_node.clone(),
                        request.job_id.clone(),
                        request.generation,
                        request.quad,
                    ),
                    request,
                ));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Process(format!(
                    "remote edge handshake for quad {:?} has no registered authorization",
                    request.quad
                )));
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }
}

fn is_unexpected_eof(error: &Error) -> bool {
    matches!(error, Error::Process(message) if message.contains("early eof")
        || message.contains("unexpected end of file"))
}

fn self_remove_outbound(manager: &NetworkManager, key: &EdgeSessionKey) {
    manager
        .outbound
        .write()
        .expect("outbound registry lock")
        .remove(key);
}

/// Looks up (and caches on first sight) the local input channel serving a
/// quad on this connection.
fn resolve_route(
    registry: &std::sync::RwLock<BTreeMap<EdgeSessionKey, flume::Sender<Envelope>>>,
    served_quads: &mut BTreeMap<EdgeSessionKey, flume::Sender<Envelope>>,
    key: EdgeSessionKey,
) -> Option<flume::Sender<Envelope>> {
    if let Some(sender) = served_quads.get(&key) {
        return Some(sender.clone());
    }
    let registered = registry
        .read()
        .expect("inbound registry lock")
        .get(&key)
        .cloned()?;
    served_quads.insert(key, registered.clone());
    Some(registered)
}

async fn wait_for_route(
    registry: &std::sync::RwLock<BTreeMap<EdgeSessionKey, flume::Sender<Envelope>>>,
    served_quads: &mut BTreeMap<EdgeSessionKey, flume::Sender<Envelope>>,
    key: EdgeSessionKey,
    grace: std::time::Duration,
) -> Option<flume::Sender<Envelope>> {
    let deadline = tokio::time::Instant::now() + grace;
    loop {
        if let Some(sender) = resolve_route(registry, served_quads, key.clone()) {
            return Some(sender);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// Drains one quad's edge channel onto its connection, stamping sequences and
/// registering pending receipts. Exits when the channel closes (drained) or
/// the connection fails; failures abort all pending branch acknowledgements.
/// Runs one connection's receipt read loop and outbound pump to completion,
/// returning both results. A failure on either half cancels the shared token,
/// so the other half exits promptly; the caller decides whether to reconnect
/// (replaying unacknowledged frames) or fail the edge closed. With
/// `reconnectable`, the pump defers its failure-path branch aborts to the
/// supervisor: the branches must stay registered so a reconnect can replay
/// them, and the supervisor aborts them if recovery does not succeed.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_edge_connection(
    stream: Box<dyn RemoteStream>,
    quad: Quad,
    receiver: flume::Receiver<Envelope>,
    pending: Arc<PendingReceipts>,
    config: NetworkManagerConfig,
    auth: Option<SessionAuth>,
    reconnectable: bool,
    failures: flume::Sender<Error>,
    shutdown: &tokio_util::sync::CancellationToken,
) -> (Result<(), Error>, Result<(), Error>) {
    let (reader, writer) = tokio::io::split(stream);
    let connection_cancel = shutdown.child_token();
    let outbound_closed = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Receipt read loop: routes receipts to the pending map. Errors cancel
    // the connection; classification (clean EOF after drain, late-failure
    // sweep) is left to the supervisor.
    let receipt_pending = pending.clone();
    let receipt_cancel = connection_cancel.clone();
    let receipt_outbound_closed = outbound_closed.clone();
    let receipt_failures = failures.clone();
    let receipt_auth = auth.clone();
    let read_task = tokio::spawn(async move {
        let mut reader = reader;
        let cancelled = receipt_cancel.clone();
        let mut first_frame = true;
        let mut failure: Option<Error> = None;
        loop {
            tokio::select! {
                _ = cancelled.cancelled() => break,
                frame = read_receipt_frame(
                    &mut reader,
                    config.max_frame_len,
                    config.read_idle_timeout,
                    config.receipt_wait_timeout,
                    &receipt_pending,
                ) => match frame {
                    Ok(TieredFrame::RacedIdleRestart) => {
                        // A pending registration raced the dead-peer timer;
                        // the next iteration reads under the receipt-wait
                        // budget.
                        continue;
                    }
                    Ok(TieredFrame::Frame((header, payload))) => {
                        if first_frame && receipt_auth.is_some() {
                            first_frame = false;
                            let Some(auth) = receipt_auth.as_ref() else {
                                unreachable!()
                            };
                            let result = if header.kind == FrameKind::Handshake
                                && header.quad == auth.quad
                            {
                                serde_json::from_slice::<HandshakePayload>(&payload)
                                    .map_err(|error| Error::Process(format!("remote edge handshake acknowledgement malformed: {error}")))
                                    .and_then(|payload| auth.verify_server_ack(&payload))
                            } else {
                                Err(Error::Process("remote edge receipt arrived before handshake acknowledgement".into()))
                            };
                            if let Err(error) = result {
                                failure = Some(error);
                                break;
                            }
                        } else if header.kind == FrameKind::Receipt {
                            first_frame = false;
                            if let Ok(receipt) = serde_json::from_slice::<ReceiptFrame>(&payload)
                            {
                                receipt_pending.apply(receipt, &receipt_failures);
                            } else {
                                failure = Some(Error::Process(
                                    "remote edge receipt frame malformed".into(),
                                ));
                                break;
                            }
                        } else {
                            failure = Some(Error::Process(format!(
                                "unexpected {:?} frame on a receipt channel",
                                header.kind
                            )));
                            break;
                        }
                    }
                    Err(error) => {
                        // A normal outbound close drops the writer half after
                        // the local edge has drained; the peer may then close
                        // its half with no more frames. Only treat that as
                        // clean when no receipt can still be outstanding.
                        if !receipt_outbound_closed.load(std::sync::atomic::Ordering::Acquire)
                            || !receipt_pending.is_empty()
                        {
                            failure = Some(error);
                        }
                        break;
                    }
                },
            }
        }
        match failure {
            Some(error) => {
                cancelled.cancel();
                Err(error)
            }
            None => Ok(()),
        }
    });

    let pump_pending = pending.clone();
    let pump_cancel = connection_cancel.clone();
    let pump_outbound_closed = outbound_closed.clone();
    let pump_task = tokio::spawn(async move {
        let result = pump_edge(
            receiver,
            writer,
            quad,
            pump_pending,
            pump_cancel.clone(),
            config,
            auth,
            reconnectable,
        )
        .await;
        match result {
            Ok(()) => {
                pump_outbound_closed.store(true, std::sync::atomic::Ordering::Release);
            }
            Err(_) => {
                pump_cancel.cancel();
            }
        }
        result
    });

    let pump_result = pump_task.await.unwrap_or_else(|error| {
        Err(Error::Process(format!(
            "remote edge pump task failed: {error}"
        )))
    });
    if pump_result.is_err() {
        // A failed pump tears the connection down: no receipt can complete a
        // frame that never reached the wire, and the supervisor decides on
        // reconnect. A cleanly drained pump is the opposite case — the read
        // half stays alive to apply the peers' late receipts, exactly as the
        // unsupervised pump did, and ends on its own (peer close, idle
        // timeout, or the manager's shutdown).
        connection_cancel.cancel();
    }
    let read_result = read_task.await.unwrap_or_else(|error| {
        Err(Error::Process(format!(
            "remote edge read task failed: {error}"
        )))
    });
    (pump_result, read_result)
}

/// Replays the session preamble on a freshly reconnected stream: the
/// authenticated handshake plus every unacknowledged data frame in sequence
/// order, reusing each frame's original sequence number so the receiver's
/// delivery-level dedup can drop the ones it already routed.
pub(crate) async fn replay_pending(
    stream: &mut Box<dyn RemoteStream>,
    quad: Quad,
    pending: &Arc<PendingReceipts>,
    auth: &Option<SessionAuth>,
    config: &NetworkManagerConfig,
) -> Result<(), Error> {
    use tokio::io::AsyncWriteExt;
    let mut writer = tokio::io::BufWriter::with_capacity(64 * 1024, &mut **stream);
    if let Some(auth) = auth {
        let handshake = auth.client_handshake()?;
        let payload = serde_json::to_vec(&handshake).map_err(|error| {
            Error::Process(format!("remote edge handshake encode failed: {error}"))
        })?;
        write_frame_with_limit(
            &mut writer,
            quad,
            FrameKind::Handshake,
            &payload,
            config.max_frame_len,
        )
        .await?;
    }
    let mut encoder = DataEncoder::new();
    for (seq, batch) in pending.replay_snapshot() {
        let payload = encoder.encode(&batch, seq)?;
        write_frame_with_limit(
            &mut writer,
            quad,
            FrameKind::Data,
            &payload,
            config.max_frame_len,
        )
        .await?;
    }
    writer
        .flush()
        .await
        .map_err(|error| Error::Process(format!("remote edge replay flush failed: {error}")))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn pump_edge(
    receiver: flume::Receiver<Envelope>,
    writer: impl AsyncWrite + Unpin + Send + 'static,
    quad: Quad,
    pending: Arc<PendingReceipts>,
    shutdown: tokio_util::sync::CancellationToken,
    config: NetworkManagerConfig,
    auth: Option<SessionAuth>,
    reconnectable: bool,
) -> Result<(), Error> {
    let mut writer = tokio::io::BufWriter::with_capacity(64 * 1024, writer);
    let mut encoder = DataEncoder::new();
    let mut flush_tick = tokio::time::interval(std::time::Duration::from_millis(100));
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    if let Some(auth) = auth {
        let handshake = auth.client_handshake()?;
        let payload = serde_json::to_vec(&handshake).map_err(|error| {
            Error::Process(format!("remote edge handshake encode failed: {error}"))
        })?;
        write_frame_with_limit(
            &mut writer,
            quad,
            FrameKind::Handshake,
            &payload,
            config.max_frame_len,
        )
        .await?;
        writer.flush().await.map_err(|error| {
            Error::Process(format!("remote edge handshake flush failed: {error}"))
        })?;
    }
    let result: Result<(), Error>;
    // A drained channel (upstream dropped the sender) is the one clean exit
    // where the connection stays alive: the receipt read loop on the same
    // connection keeps applying late receipts and owns the final abort
    // sweep. Every other exit — wire failure, shutdown, registration
    // failure — tears the connection down, so no receipt can arrive and the
    // pump must abort everything still registered itself.
    let mut drained = false;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                result = Ok(());
                break;
            }
            _ = flush_tick.tick() => {
                if writer.flush().await.is_err() {
                    result = Err(Error::Process("remote edge flush failed".into()));
                    break;
                }
            }
            envelope = receiver.recv_async() => {
                let Ok(envelope) = envelope else {
                    drained = true;
                    result = Ok(());
                    break;
                };
                match envelope {
                    Envelope::Data(batch, branch) => {
                        // Register before writing so a concurrent receipt can
                        // never race the pending map; the wire is strict FIFO
                        // per quad, so receipts cannot precede their frame.
                        let seq = match pending.register(
                            &branch,
                            1,
                            reconnectable.then(|| batch.clone()),
                        ) {
                            Ok(seq) => seq,
                            Err(error) => {
                                let abort_error = branch.abort().await.err();
                                if let Some(abort_error) = abort_error {
                                    result = Err(Error::Process(format!(
                                        "{error}; failed to abort batch rejected by pending receipt limit: {abort_error}"
                                    )));
                                    break;
                                }
                                result = Err(error);
                                break;
                            }
                        };
                        let payload = match encoder.encode(&batch, seq) {
                            Ok(payload) => payload,
                            Err(error) => {
                                result = Err(error);
                                break;
                            }
                        };
                        if let Err(error) = write_frame_with_limit(
                            &mut writer,
                            quad,
                            FrameKind::Data,
                            &payload,
                            config.max_frame_len,
                        )
                        .await
                        {
                            result = Err(Error::Process(format!(
                                "remote edge data write failed: {error}"
                            )));
                            break;
                        }
                    }
                    other => {
                        let Some(signal) = WireSignal::from_envelope(&other) else {
                            continue;
                        };
                        let payload = match serde_json::to_vec(&signal) {
                            Ok(bytes) => bytes,
                            Err(error) => {
                                result = Err(Error::Process(format!(
                                    "signal frame encode failed: {error}"
                                )));
                                break;
                            }
                        };
                        // Control elements broadcast on every quad of the edge;
                        // each pump owns exactly one quad, so one frame each.
                        if let Err(error) = write_frame_with_limit(
                            &mut writer,
                            quad,
                            FrameKind::Signal,
                            &payload,
                            config.max_frame_len,
                        )
                        .await
                        {
                            result = Err(Error::Process(format!(
                                "remote edge signal write failed: {error}"
                            )));
                            break;
                        }
                    }
                }
            }
        }
    }
    let _ = writer.flush().await;
    // Abort pendings only on failure or forced shutdown: those paths tear
    // the connection down, so no receipt can ever arrive and leaving the
    // branches registered would leak source acks indefinitely. A drained
    // channel is the opposite case — the receipt read loop on the same
    // connection is still running, late receipts for flushed frames must
    // keep applying, and that loop owns the final abort sweep once the peer
    // closes or the read idle timeout fires.
    if reconnectable && (result.is_err() || !drained) {
        // A supervisor owns recovery: the branches stay registered so a
        // transparent reconnect can replay them, and the supervisor aborts
        // them if the reconnect budget is exhausted. This includes exits
        // driven by the connection token (a read-half failure cancelling the
        // pump mid-stream): aborting here would erase the replay set.
        return result;
    }
    if result.is_err() || !drained {
        pending.abort_all();
    }
    result
}
