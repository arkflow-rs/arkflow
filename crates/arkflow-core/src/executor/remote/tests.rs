/// Polling budgets for asynchronous disconnect/abort propagation.
/// Under a fully parallel workspace test run, tokio timers can lag
/// several seconds; these waits only observe conditions that normally
/// complete within a tick (~100ms), so a generous ceiling costs nothing
/// when everything is healthy and keeps loaded CI stable.
const TEST_PROPAGATION_BUDGET: std::time::Duration = std::time::Duration::from_secs(60);

use super::*;
use crate::input::Ack as _;
use datafusion::arrow::array::{DictionaryArray, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
use datafusion::arrow::record_batch::RecordBatch;

fn dictionary_batch(input_name: Option<&str>) -> MessageBatch {
    let dictionary: DictionaryArray<Int32Type> =
        vec!["alpha", "beta", "alpha"].into_iter().collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "label",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(dictionary),
    ];
    let batch = RecordBatch::try_new(schema, columns).expect("valid batch");
    let mut message_batch = MessageBatch::new_arrow(batch);
    message_batch.set_input_name(input_name.map(str::to_owned));
    message_batch
}

#[test]
fn header_roundtrips_and_rejects_garbage() {
    let quad = Quad {
        src_op: 12_341,
        src_subtask: 3,
        dst_op: 908,
        dst_subtask: 17,
    };
    let mut bytes = Vec::new();
    FrameHeader {
        quad,
        len: 512,
        kind: FrameKind::Signal,
    }
    .encode_into(&mut bytes);
    assert_eq!(bytes.len(), FRAME_HEADER_LEN);
    let decoded = FrameHeader::decode(&bytes).expect("valid header");
    assert_eq!(decoded.quad, quad);
    assert_eq!(decoded.len, 512);
    assert_eq!(decoded.kind, FrameKind::Signal);

    // A zeroed header (kind 0) is invalid rather than misparsed.
    let zeroed = vec![0u8; FRAME_HEADER_LEN];
    assert!(FrameHeader::decode(&zeroed).is_err());
    // A declared length above the bound is rejected.
    let mut oversized = Vec::new();
    FrameHeader {
        quad,
        len: MAX_FRAME_LEN + 1,
        kind: FrameKind::Data,
    }
    .encode_into(&mut oversized);
    assert!(FrameHeader::decode(&oversized).is_err());
}

#[test]
fn signals_and_receipts_roundtrip() {
    let barrier = WireSignal::Barrier(CheckpointBarrier {
        checkpoint_id: "c-7".into(),
        generation: 9,
        trace_context: None,
    });
    let envelope = Envelope::Barrier(CheckpointBarrier {
        checkpoint_id: "c-7".into(),
        generation: 9,
        trace_context: None,
    });
    assert_eq!(WireSignal::from_envelope(&envelope), Some(barrier.clone()));
    match barrier.clone().into_envelope() {
        Envelope::Barrier(rebuilt) => {
            assert_eq!(
                rebuilt,
                CheckpointBarrier {
                    checkpoint_id: "c-7".into(),
                    generation: 9,
                    trace_context: None
                }
            );
        }
        _ => panic!("expected barrier envelope"),
    }

    for signal in [
        WireSignal::Watermark(1_724_000_000),
        WireSignal::Eos,
        barrier,
    ] {
        let bytes = serde_json::to_vec(&signal).expect("serialize");
        let decoded: WireSignal = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(decoded, signal);
    }

    for kind in [
        ReceiptKind::Acked,
        ReceiptKind::Held,
        ReceiptKind::Released,
        ReceiptKind::Failed,
    ] {
        let receipt = ReceiptFrame { kind, seq: 42 };
        let bytes = serde_json::to_vec(&receipt).expect("serialize");
        let decoded: ReceiptFrame = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(decoded, receipt);
    }
}

#[test]
fn data_frames_roundtrip_with_dictionary_and_schema_cache() {
    let first = dictionary_batch(Some("kafka-in"));
    let second = dictionary_batch(None);

    let mut encoder = DataEncoder::new();
    let mut decoder = DataDecoder::new();

    let payload_a = encoder.encode(&first, 7).expect("encode first");
    let (decoded_a, seq_a) = decoder
        .decode(bytes::Bytes::from(payload_a))
        .expect("decode first");
    assert_eq!(seq_a, 7);
    assert_eq!(decoded_a.get_input_name(), Some("kafka-in".to_owned()));
    assert_eq!(decoded_a.record_batch(), first.record_batch());

    // Second frame with the same schema reuses the cached schema and
    // dictionaries (the encoder emits neither again).
    let payload_b = encoder.encode(&second, 8).expect("encode second");
    let (decoded_b, seq_b) = decoder
        .decode(bytes::Bytes::from(payload_b))
        .expect("decode second");
    assert_eq!(seq_b, 8);
    assert_eq!(decoded_b.get_input_name(), None);
    assert_eq!(decoded_b.record_batch(), second.record_batch());
}

#[test]
fn decoder_rejects_frames_before_any_schema() {
    let mut encoder = DataEncoder::new();
    let mut decoder = DataDecoder::new();
    let payload = encoder.encode(&dictionary_batch(None), 0).expect("encode");
    // Strip the schema piece: corrupt meta to claim no schema is attached.
    let meta_len = u32::from_le_bytes(payload[0..4].try_into().expect("sliced")) as usize;
    let mut meta: WireDataMeta = serde_json::from_slice(&payload[4..4 + meta_len]).expect("meta");
    meta.has_schema = false;
    let rewritten = serde_json::to_vec(&meta).expect("meta encode");
    let mut corrupted = Vec::new();
    corrupted.extend_from_slice(&(rewritten.len() as u32).to_le_bytes());
    corrupted.extend_from_slice(&rewritten);
    corrupted.extend_from_slice(&payload[4 + meta_len..]);
    assert!(decoder.decode(bytes::Bytes::from(corrupted)).is_err());
}

#[test]
fn ipc_body_bounds_are_checked_without_panicking() {
    let piece = Piece {
        bytes: bytes::Bytes::from_static(&[0; 8]),
        flatbuf_len: 0,
    };
    let result = std::panic::catch_unwind(|| piece.body(1));
    assert!(result.is_ok(), "malformed IPC body must not panic");
    assert!(result.unwrap().is_err());

    let truncated = bytes::Bytes::from_static(&[4, 0, 0, 0, 8, 0, 0, 0, 1, 2, 3, 4]);
    assert!(read_piece(&truncated, 0).is_err());
}

#[test]
fn ipc_body_buffer_shares_the_frame_payload_memory() {
    // Zero-copy contract: the decoded body buffer is a shared-ownership
    // slice of the received payload, not a copied region.
    let body_content: Vec<u8> = (0..64u8).collect();
    let mut piece_bytes = Vec::new();
    piece_bytes.extend_from_slice(&((8 + body_content.len()) as u32).to_le_bytes()); // total
    piece_bytes.extend_from_slice(&0u32.to_le_bytes()); // flatbuf_len
    piece_bytes.extend_from_slice(&body_content);
    let payload = bytes::Bytes::from(piece_bytes);
    let (piece, _next) = read_piece(&payload, 0).expect("piece");
    let buffer = piece.body(body_content.len()).expect("body");
    let start = payload.as_ptr() as usize;
    let end = start + payload.len();
    let body_ptr = buffer.as_ptr() as usize;
    assert!(
        body_ptr >= start && body_ptr < end,
        "body buffer must alias the received payload, not a copy"
    );
}

#[tokio::test]
async fn connection_overflow_is_counted_as_a_bounded_resource_rejection() {
    // The accepted-connection cap is 1: the second stream is rejected and
    // must show up in the bounded rejection counter (the observable surface
    // replacing the old unconsumed-channel-only path).
    let config = NetworkManagerConfig {
        max_connections: 1,
        ..Default::default()
    };
    let manager = NetworkManager::with_config(config).unwrap();
    assert_eq!(manager.rejected_connections(), 0);
    let (_client1, server1) = tokio::io::duplex(1024);
    manager.accept_stream(Box::new(server1));
    assert_eq!(manager.rejected_connections(), 0);
    let (_client2, server2) = tokio::io::duplex(1024);
    manager.accept_stream(Box::new(server2));
    assert_eq!(manager.rejected_connections(), 1);
}

/// Auth-overhead quantification (ignored: developer-run timing, not a CI
/// gate — the benchmark contract forbids performance assertions). One-way
/// measurement: sealed writes versus plaintext writes over an in-memory
/// duplex with a raw byte reader (no tag verification on the read side).
/// The ratio includes the 16-byte tags and transport overhead, not only the
/// HMAC computation.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "timing benchmark; run explicitly with --ignored"]
async fn auth_frame_roundtrip_timing() {
    use tokio::io::AsyncReadExt;
    let quad = quad_a_to_b();
    let payload = bytes::Bytes::from(vec![7u8; 64 * 1024]);
    let payload_len = payload.len();
    const FRAMES: usize = 400;

    let credentials = DataPlaneCredentials::new("node-a", "shuffle-secret").unwrap();
    let auth = credentials.session("node-b", "timing-job", 1, quad);
    let (ack_payload, mut c2s, mut s2c) = fabricate_ack_and_derive(&auth);

    let run_plain = async {
        let payload = payload.clone();
        let (mut client, mut server) = tokio::io::duplex(1024 * 1024);
        let writer = tokio::spawn(async move {
            for _ in 0..FRAMES {
                write_frame(&mut client, quad, FrameKind::Data, &payload)
                    .await
                    .unwrap();
            }
            client.flush().await.unwrap();
        });
        let mut read_buf = vec![0u8; payload_len + 24];
        let mut read_total = 0usize;
        while read_total < FRAMES * (payload_len + 24) {
            let n = server.read(&mut read_buf).await.unwrap();
            assert!(n > 0);
            read_total += n;
        }
        writer.await.unwrap();
    };

    let run_sealed = async {
        let payload = payload.clone();
        let (mut client, mut server) = tokio::io::duplex(1024 * 1024);
        let writer = tokio::spawn(async move {
            for _ in 0..FRAMES {
                write_frame_sealed(
                    &mut client,
                    quad,
                    FrameKind::Data,
                    &payload,
                    MAX_FRAME_LEN,
                    &mut c2s,
                )
                .await
                .unwrap();
            }
            client.flush().await.unwrap();
        });
        let frame_len = payload_len + 24 + 16;
        let mut read_buf = vec![0u8; frame_len];
        let mut read_total = 0usize;
        while read_total < FRAMES * frame_len {
            let n = server.read(&mut read_buf).await.unwrap();
            assert!(n > 0);
            read_total += n;
        }
        writer.await.unwrap();
    };
    let _ = (&ack_payload, &mut s2c);

    let plain_start = std::time::Instant::now();
    run_plain.await;
    let plain = plain_start.elapsed();

    let sealed_start = std::time::Instant::now();
    run_sealed.await;
    let sealed = sealed_start.elapsed();

    let megabytes = (FRAMES * payload_len) as f64 / 1_048_576.0;
    println!(
        "auth frame write timing (one-way, raw reader): plain {plain:?} vs sealed {sealed:?} for {megabytes:.0} MiB ({FRAMES} frames); sealed/plain = {:.2} (includes tag bytes and transport overhead, not just HMAC)",
        sealed.as_secs_f64() / plain.as_secs_f64()
    );
}

#[tokio::test]
async fn max_frame_len_rejects_oversized_frames_on_the_send_side() {
    // The oversized payload must be comfortably above MIN_FRAME_LEN under
    // ANY arrow patch release: a 3-row int64/dictionary batch encoded to
    // ~1.1 KiB, and arrow-rs 58.4.2's IPC buffer fixes shrank it below the
    // 1024 floor (921 bytes), silently bypassing the guard this test pins.
    // A 64 KiB string column keeps the frame oversized independent of
    // upstream encoding tweaks.
    let blob = "x".repeat(64 * 1024);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("blob", DataType::Utf8, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1])),
        Arc::new(StringArray::from(vec![blob.as_str()])),
    ];
    let oversized =
        MessageBatch::new_arrow(RecordBatch::try_new(schema, columns).expect("valid batch"));
    let quad = quad_a_to_b();
    let config = NetworkManagerConfig {
        max_frame_len: MIN_FRAME_LEN,
        ..Default::default()
    };
    config
        .validate()
        .expect("the minimum cap is a valid config");
    let (writer, _reader) = tokio::io::duplex(64 * 1024);
    let (tx, rx) = flume::bounded::<Envelope>(4);
    tx.send_async(Envelope::Data(
        Arc::new(oversized),
        Arc::new(RecordingAck::default()),
    ))
    .await
    .unwrap();
    drop(tx);
    let error = pump_edge(
        rx,
        writer,
        quad,
        Arc::new(PendingReceipts::new(64)),
        tokio_util::sync::CancellationToken::new(),
        config,
        None,
        false,
        None,
        false,
        false,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains(&format!("exceeds the {MIN_FRAME_LEN} limit")),
        "{error}"
    );
}

#[test]
fn network_manager_config_rejects_unusable_caps_and_permit_overflow() {
    // A cap below the protocol floor advertises shuffle and then fails every
    // handshake: rejected in validation, not at the first edge.
    let config = NetworkManagerConfig {
        max_frame_len: MIN_FRAME_LEN - 1,
        ..Default::default()
    };
    let error = config.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("must be at least 1024 to carry the protocol handshake"),
        "{error}"
    );
    // Tokio panics when a Semaphore exceeds MAX_PERMITS; validation must catch
    // it so the agent gets the configuration error and its fallback.
    let config = NetworkManagerConfig {
        max_connections: usize::MAX,
        ..Default::default()
    };
    let error = config.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("max_connections must not exceed"),
        "{error}"
    );
    // The defaults remain valid.
    NetworkManagerConfig::default().validate().unwrap();
}

#[test]
fn deterministic_protocol_errors_are_classified_for_no_reconnect() {
    // Classified (terminal on first occurrence, no reconnect budget spent).
    for message in [
        "remote edge frame authentication failed",
        "remote edge frame authentication tag has the wrong length",
        "remote edge handshake credential mismatch",
        "remote edge handshake nonce was replayed",
        "remote edge handshake protocol or identity mismatch",
        "remote edge handshake acknowledgement identity mismatch",
        "remote edge frame has unknown kind 9",
        "remote edge frame declares 99 bytes, above the 64 limit",
        "duplicate remote edge handshake",
        "remote edge frame quad Quad { .. } does not match authenticated quad Quad { .. }",
        "remote edge frame arrived before handshake",
        "unexpected Data frame on a receipt channel",
        "remote edge receipt frame malformed",
        "remote edge handshake acknowledgement malformed",
        "remote edge pending replay budget cannot fit the batch (2048 bytes); the delivery can never fit: remote edge pending replay byte budget 1 reached (0 bytes retained)",
    ] {
        let error = Error::Process(message.into());
        assert!(
            is_deterministic_protocol_error(&error),
            "must classify: {message}"
        );
    }
    // Transport-level faults stay retriable.
    for message in [
        "remote edge closed while reading header: early eof",
        "remote edge closed while reading header: read idle timeout",
        "remote edge write failed: connection reset",
        "remote edge connect to 127.0.0.1:1 failed after 5 attempts",
    ] {
        let error = Error::Process(message.into());
        assert!(
            !is_deterministic_protocol_error(&error),
            "must stay retriable: {message}"
        );
    }
    // Non-Process errors are transport/config-shaped, never deterministic.
    assert!(!is_deterministic_protocol_error(&Error::Config("x".into())));
}

#[tokio::test]
async fn pending_receipts_never_fitting_batch_fails_the_edge() {
    // A batch whose Arrow memory alone exceeds the byte budget can never fit
    // the replay cache even when it is empty: abort-and-redeliver would
    // livelock a rewinding source, so the edge fails closed instead.
    let quad = quad_a_to_b();
    let config = NetworkManagerConfig {
        max_pending_bytes: 1,
        ..Default::default()
    };
    let (writer, _reader) = tokio::io::duplex(64 * 1024);
    let (tx, rx) = flume::bounded::<Envelope>(4);
    let batch = Arc::new(MessageBatch::new_arrow(large_batch(16)));
    assert!(
        batch.record_batch().get_array_memory_size() as u64 > 1,
        "the fixture batch must exceed the 1-byte budget"
    );
    tx.send_async(Envelope::Data(batch, Arc::new(RecordingAck::default())))
        .await
        .unwrap();
    drop(tx);
    // The byte budget guards the RETAINED replay batches, so the pump runs
    // reconnectable here (the same shape that makes abort-and-redeliver
    // livelock a rewinding source).
    let error = pump_edge(
        rx,
        writer,
        quad,
        Arc::new(PendingReceipts::with_byte_budget(64, 1)),
        tokio_util::sync::CancellationToken::new(),
        config,
        None,
        true,
        None,
        false,
        false,
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("pending replay byte budget"),
        "{error}"
    );
}

#[tokio::test]
async fn frames_roundtrip_over_async_buffers() {
    let quad = Quad {
        src_op: 1,
        src_subtask: 0,
        dst_op: 2,
        dst_subtask: 5,
    };
    let (mut client, mut server) = tokio::io::duplex(256);
    write_frame(&mut client, quad, FrameKind::Receipt, b"stub")
        .await
        .expect("write frame");
    let (header, payload) = read_frame(&mut server).await.expect("read frame");
    assert_eq!(header.quad, quad);
    assert_eq!(header.kind, FrameKind::Receipt);
    assert_eq!(payload.as_ref(), b"stub");
}

#[tokio::test]
async fn idle_timeout_resets_after_partial_read_progress() {
    let (mut writer, mut reader) = tokio::io::duplex(16);
    let (first_written_tx, first_written_rx) = tokio::sync::oneshot::channel();
    let writer_task = tokio::spawn(async move {
        writer.write_all(&[1]).await.unwrap();
        first_written_tx.send(()).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        writer.write_all(&[2]).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        writer.write_all(&[3]).await.unwrap();
    });
    first_written_rx.await.unwrap();
    let mut bytes = [0_u8; 3];
    read_exact_with_idle(
        &mut reader,
        &mut bytes,
        Some(std::time::Duration::from_millis(50)),
    )
    .await
    .expect("a slow but progressing frame is not idle");
    writer_task.await.unwrap();
    assert_eq!(bytes, [1, 2, 3]);
}

// ------------------------------------------------------------------
// End-to-end manager tests (task 2.4)
// ------------------------------------------------------------------

/// A branch acknowledgement that records lifecycle transitions so tests
/// can assert receipt aggregation without a real source.
#[derive(Default)]
struct RecordingAck {
    acked: std::sync::atomic::AtomicBool,
    aborted: std::sync::atomic::AtomicBool,
    held: std::sync::atomic::AtomicBool,
    released: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl crate::input::Ack for RecordingAck {
    async fn ack(&self) -> Result<(), Error> {
        self.acked.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn mark_held(&self) {
        self.held.store(true, std::sync::atomic::Ordering::SeqCst);
    }
    fn release_held(&self) {
        self.released
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    async fn abort(&self) -> Result<(), Error> {
        self.aborted
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

/// Shared kill state: the flag plus the currently registered reader
/// waker, so `kill()` wakes a read parked inside the inner stream.
#[derive(Default)]
struct KillState {
    killed: std::sync::atomic::AtomicBool,
    waker: std::sync::Mutex<Option<std::task::Waker>>,
}

impl KillState {
    fn kill(&self) {
        self.killed
            .store(true, std::sync::atomic::Ordering::Release);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

/// Handle side for tests: break the wrapped stream half.
#[derive(Clone, Default)]
struct KillHandle(Arc<KillState>);

impl KillHandle {
    fn kill(&self) {
        self.0.kill();
    }
}

/// Wraps a stream half so the test can "break the connection" after the
/// manager has taken ownership: once killed, pending reads wake and fail
/// (like a reset connection) and writes fail with BrokenPipe.
struct KillableStream {
    inner: Box<dyn RemoteStream>,
    state: Arc<KillState>,
}

impl AsyncRead for KillableStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.state.killed.load(std::sync::atomic::Ordering::Acquire) {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "killed",
            )));
        }
        *self.state.waker.lock().unwrap() = Some(cx.waker().clone());
        let result = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
        if result.is_pending() {
            // Keep the waker registered for a kill() wake.
        } else {
            *self.state.waker.lock().unwrap() = None;
        }
        result
    }
}

impl AsyncWrite for KillableStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if self.state.killed.load(std::sync::atomic::Ordering::Acquire) {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "killed",
            )));
        }
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Hands out pre-made stream halves in order: each `connect` pops the
/// next queued client half, letting tests break a connection by dropping
/// its server half and observe the transparent reconnect onto the
/// following one.
struct QueuedTransport {
    streams: std::sync::Mutex<Vec<Box<dyn RemoteStream>>>,
    fail_after: Option<usize>,
}

impl QueuedTransport {
    fn of(streams: Vec<Box<dyn RemoteStream>>) -> Arc<Self> {
        Arc::new(Self {
            streams: std::sync::Mutex::new(streams),
            fail_after: None,
        })
    }

    fn failing_after(streams: Vec<Box<dyn RemoteStream>>, fail_after: usize) -> Arc<Self> {
        Arc::new(Self {
            streams: std::sync::Mutex::new(streams),
            fail_after: Some(fail_after),
        })
    }
}

#[async_trait::async_trait]
impl EdgeTransport for QueuedTransport {
    async fn connect(&self, _quad: Quad) -> Result<Box<dyn RemoteStream>, Error> {
        let mut streams = self.streams.lock().unwrap();
        if let Some(limit) = self.fail_after {
            if streams.len() <= limit {
                return Err(Error::Process("test transport exhausted".into()));
            }
        }
        if streams.is_empty() {
            return Err(Error::Process("test transport exhausted".into()));
        }
        Ok(streams.remove(0))
    }
}

fn quad_a_to_b() -> Quad {
    Quad {
        src_op: 1,
        src_subtask: 0,
        dst_op: 2,
        dst_subtask: 0,
    }
}

fn authenticated_manager(node: &str, secret: &str) -> Arc<NetworkManager> {
    let credentials = DataPlaneCredentials::new(node, secret).expect("test credentials");
    let config = NetworkManagerConfig {
        credentials: Some(credentials),
        registration_grace: std::time::Duration::from_millis(100),
        ..Default::default()
    };
    NetworkManager::with_config(config).expect("valid test network config")
}

async fn next_envelope(rx: &flume::Receiver<Envelope>) -> Envelope {
    tokio::time::timeout(TEST_PROPAGATION_BUDGET, rx.recv_async())
        .await
        .expect("envelope within timeout")
        .expect("channel open")
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_session_roundtrips_and_binds_job_generation() {
    let quad = quad_a_to_b();
    let upstream = authenticated_manager("node-a", "shuffle-secret");
    let downstream = authenticated_manager("node-b", "shuffle-secret");
    upstream.spawn();
    downstream.spawn();
    let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
    downstream
        .register_inbound_for_session(
            quad,
            input_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "job-1".into(),
                generation: 7,
            },
        )
        .unwrap();
    let (client_side, server_side) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(server_side));
    let edge = upstream
        .open_edge_with_stream_for_session(Box::new(client_side), quad, "node-b", "job-1", 7)
        .unwrap();
    let branch = Arc::new(RecordingAck::default());
    edge.sender
        .send_async(Envelope::Data(
            Arc::new(dictionary_batch(Some("authenticated"))),
            branch.clone(),
        ))
        .await
        .unwrap();
    let received = next_envelope(&input_rx).await;
    let Envelope::Data(batch, ack) = received else {
        panic!("expected authenticated data envelope");
    };
    assert_eq!(batch.get_input_name(), Some("authenticated".into()));
    ack.ack().await.unwrap();
    tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
        while !branch.acked.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("authenticated receipt");
    upstream.shutdown();
    downstream.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_session_rejects_stale_generation_and_wrong_quad() {
    let expected_quad = quad_a_to_b();
    let wrong_quad = Quad {
        dst_subtask: expected_quad.dst_subtask + 1,
        ..expected_quad
    };
    for (secret, quad, generation, expected_message) in [
        ("shuffle-secret", expected_quad, 8, "authorization mismatch"),
        (
            "shuffle-secret",
            wrong_quad,
            7,
            "no registered authorization",
        ),
        ("wrong-secret", expected_quad, 7, "credential mismatch"),
    ] {
        let upstream = authenticated_manager("node-a", secret);
        let downstream = authenticated_manager("node-b", "shuffle-secret");
        upstream.spawn();
        downstream.spawn();
        let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
        downstream
            .register_inbound_for_session(
                expected_quad,
                input_tx,
                PeerExpectation {
                    source_node: "node-a".into(),
                    job_id: "job-1".into(),
                    generation: 7,
                },
            )
            .unwrap();
        let failures = downstream.failure_receiver();
        let (client_side, server_side) = tokio::io::duplex(64 * 1024);
        downstream.accept_stream(Box::new(server_side));
        upstream
            .open_edge_with_stream_for_session(
                Box::new(client_side),
                quad,
                "node-b",
                "job-1",
                generation,
            )
            .unwrap();
        let failure =
            tokio::time::timeout(std::time::Duration::from_secs(2), failures.recv_async())
                .await
                .expect("authentication failure")
                .unwrap();
        assert!(
            failure.to_string().contains(expected_message),
            "unexpected authentication failure: {failure}"
        );
        upstream.shutdown();
        downstream.shutdown();
    }
}

#[tokio::test]
async fn authenticated_handshake_nonce_cannot_be_replayed() {
    let quad = quad_a_to_b();
    let downstream = authenticated_manager("node-b", "shuffle-secret");
    let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
    downstream
        .register_inbound_for_session(
            quad,
            input_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "job-replay".into(),
                generation: 1,
            },
        )
        .unwrap();
    let auth = DataPlaneCredentials::new("node-a", "shuffle-secret")
        .unwrap()
        .session("node-b", "job-replay", 1, quad);
    let request = auth.client_handshake().unwrap();
    let payload = serde_json::to_vec(&request).unwrap();
    let header = FrameHeader {
        quad,
        len: payload.len() as u32,
        kind: FrameKind::Handshake,
    };
    downstream
        .authorize_inbound(header, &payload)
        .await
        .expect("first handshake is accepted");
    let error = match downstream.authorize_inbound(header, &payload).await {
        Ok(_) => panic!("a client nonce must not be reusable"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("replayed"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn clean_eos_close_does_not_report_a_remote_failure() {
    let manager = NetworkManager::new(8);
    manager.spawn();
    let quad = quad_a_to_b();
    let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
    manager.register_inbound(quad, input_tx);
    let failures = manager.failure_receiver();
    let (mut client, server) = tokio::io::duplex(64 * 1024);
    manager.accept_stream(Box::new(server));
    let payload = serde_json::to_vec(&WireSignal::Eos).unwrap();
    write_frame(&mut client, quad, FrameKind::Signal, &payload)
        .await
        .unwrap();
    client.shutdown().await.unwrap();
    assert!(matches!(next_envelope(&input_rx).await, Envelope::Eos));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), failures.recv_async(),)
            .await
            .is_err()
    );
    manager.shutdown();
}

#[test]
fn unauthenticated_job_sessions_are_singleton_and_reusable_after_cleanup() {
    let manager = NetworkManager::new(8);
    let quad = quad_a_to_b();
    let (first_tx, _first_rx) = flume::bounded::<Envelope>(1);
    manager
        .register_inbound_for_session(
            quad,
            first_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "job-1".into(),
                generation: 1,
            },
        )
        .unwrap();
    let (second_tx, _second_rx) = flume::bounded::<Envelope>(1);
    let error = manager
        .register_inbound_for_session(
            quad,
            second_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "job-2".into(),
                generation: 1,
            },
        )
        .expect_err("legacy routing cannot safely multiplex Jobs");
    assert!(error.to_string().contains("cannot carry Job"), "{error}");
    manager.remove_job_session("job-1", 1);
    let (second_tx, _second_rx) = flume::bounded::<Envelope>(1);
    manager
        .register_inbound_for_session(
            quad,
            second_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "job-2".into(),
                generation: 1,
            },
        )
        .unwrap();
}

#[test]
fn removing_a_job_session_reclaims_authenticated_registries() {
    let manager = authenticated_manager("node-b", "shuffle-secret");
    let quad = quad_a_to_b();
    let (input_tx, _input_rx) = flume::bounded::<Envelope>(1);
    manager
        .register_inbound_for_session(
            quad,
            input_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "job-cleanup".into(),
                generation: 4,
            },
        )
        .unwrap();
    let _failures = manager.failure_receiver_for_job("job-cleanup", 4);
    assert_eq!(manager.inbound.read().unwrap().len(), 1);
    assert_eq!(manager.failure_channels.read().unwrap().len(), 1);
    manager.remove_job_session("job-cleanup", 4);
    assert!(manager.inbound.read().unwrap().is_empty());
    assert!(manager.inbound_auth.read().unwrap().is_empty());
    assert!(manager.failure_channels.read().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_listener_rejects_pre_handshake_frame() {
    let quad = quad_a_to_b();
    let downstream = authenticated_manager("node-b", "shuffle-secret");
    downstream.spawn();
    let failures = downstream.failure_receiver();
    let (mut client_side, server_side) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(server_side));
    write_frame(&mut client_side, quad, FrameKind::Signal, b"{}")
        .await
        .unwrap();
    let failure = tokio::time::timeout(std::time::Duration::from_secs(2), failures.recv_async())
        .await
        .expect("pre-handshake failure")
        .unwrap();
    assert!(failure.to_string().contains("before handshake"));
    downstream.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn data_barrier_receipts_flow_end_to_end() {
    let quad = quad_a_to_b();
    let upstream = NetworkManager::new(64);
    let downstream = NetworkManager::new(64);
    upstream.spawn();
    downstream.spawn();

    let (input_tx, input_rx) = flume::bounded::<Envelope>(64);
    downstream.register_inbound(quad, input_tx);

    // Loopback wiring: the upstream connects to a duplex half whose other
    // half is fed into the downstream manager as an accepted stream.
    let (client_side, server_side) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(server_side));
    let edge = upstream.open_edge_with_stream(Box::new(client_side), quad);

    // Data with a recorded branch acknowledgement.
    let branch = std::sync::Arc::new(RecordingAck::default());
    edge.sender
        .send_async(Envelope::Data(
            std::sync::Arc::new(dictionary_batch(Some("in-a"))),
            branch.clone(),
        ))
        .await
        .expect("send data");

    let received = next_envelope(&input_rx).await;
    let (batch, ack) = match received {
        Envelope::Data(batch, ack) => (batch, ack),
        _other => panic!("expected data envelope, got a different variant"),
    };
    assert_eq!(batch.get_input_name(), Some("in-a".to_owned()));
    assert!(!branch.acked.load(std::sync::atomic::Ordering::SeqCst));

    // Downstream processes: acking the RemoteAck mirrors the receipt back
    // and completes the upstream branch.
    ack.ack().await.expect("remote ack");
    tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
        while !branch.acked.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("branch acked via receipt");

    // Signals ride the same edge in FIFO position.
    edge.sender
        .send_async(Envelope::Barrier(CheckpointBarrier {
            checkpoint_id: "c-1".into(),
            generation: 1,
            trace_context: None,
        }))
        .await
        .expect("send barrier");
    edge.sender
        .send_async(Envelope::Watermark(42))
        .await
        .expect("send watermark");
    edge.sender
        .send_async(Envelope::Eos)
        .await
        .expect("send eos");
    for expected in ["barrier", "watermark", "eos"] {
        match next_envelope(&input_rx).await {
            Envelope::Barrier(_) => assert_eq!(expected, "barrier"),
            Envelope::Watermark(ms) => {
                assert_eq!((expected, ms), ("watermark", 42));
            }
            Envelope::Eos => assert_eq!(expected, "eos"),
            Envelope::Data(_, _) => panic!("unexpected data"),
        }
    }

    // Held/Released mirror as parent transitions on the branch.
    let branch2 = std::sync::Arc::new(RecordingAck::default());
    edge.sender
        .send_async(Envelope::Data(
            std::sync::Arc::new(dictionary_batch(None)),
            branch2.clone(),
        ))
        .await
        .expect("send data 2");
    let received = next_envelope(&input_rx).await;
    let Envelope::Data(_, ack2) = received else {
        panic!("expected data");
    };
    ack2.mark_held();
    ack2.release_held();
    tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
        while !branch2.held.load(std::sync::atomic::Ordering::SeqCst)
            || !branch2.released.load(std::sync::atomic::Ordering::SeqCst)
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("held/released mirrored");

    upstream.shutdown();
    downstream.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn pending_receipt_limit_aborts_the_rejected_batch() {
    let quad = quad_a_to_b();
    let upstream_config = NetworkManagerConfig {
        channel_capacity: 8,
        max_pending_receipts: 1,
        reconnect_attempts: 0,
        ..NetworkManagerConfig::default()
    };
    let upstream = NetworkManager::with_config(upstream_config).unwrap();
    let downstream = NetworkManager::new(8);
    upstream.spawn();
    downstream.spawn();

    let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
    downstream.register_inbound(quad, input_tx);
    let (client_side, server_side) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(server_side));
    let edge = upstream.open_edge_with_stream(Box::new(client_side), quad);
    let failures = upstream.failure_receiver();

    let first = Arc::new(RecordingAck::default());
    edge.sender
        .send_async(Envelope::Data(
            Arc::new(dictionary_batch(None)),
            first.clone(),
        ))
        .await
        .unwrap();
    let _ = next_envelope(&input_rx).await;

    let rejected = Arc::new(RecordingAck::default());
    edge.sender
        .send_async(Envelope::Data(
            Arc::new(dictionary_batch(None)),
            rejected.clone(),
        ))
        .await
        .unwrap();

    tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
        while !rejected.aborted.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the batch rejected by the pending limit must be aborted");
    // Delivery-level failure contract: the over-budget delivery is aborted
    // for upstream redelivery while the edge keeps serving — the in-budget
    // delivery stays pending and no edge failure is reported.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        !first.aborted.load(std::sync::atomic::Ordering::SeqCst),
        "the in-budget delivery must stay pending; the edge did not fail"
    );
    let leaked = tokio::time::timeout(std::time::Duration::from_millis(500), failures.recv_async())
        .await
        .is_err();
    assert!(
        leaked,
        "pending-budget overflow must not surface as an edge failure"
    );

    upstream.shutdown();
    downstream.shutdown();
}

/// Spec: pending replay is bounded by an approximate byte budget: a
/// registration that would exceed the budget is rejected, and a
/// completed acknowledgement releases its accounting back to the budget.
#[tokio::test]
async fn pending_replay_byte_budget_rejects_and_releases() {
    struct NoopAck;
    #[async_trait::async_trait]
    impl crate::input::Ack for NoopAck {
        async fn ack(&self) -> Result<(), Error> {
            Ok(())
        }
        async fn abort(&self) -> Result<(), Error> {
            Ok(())
        }
    }
    let batch = dictionary_batch(None);
    let batch_bytes = batch.record_batch().get_array_memory_size() as u64;
    let pending = PendingReceipts::with_byte_budget(64, batch_bytes);
    let branch = Arc::new(NoopAck) as Arc<dyn crate::input::Ack>;
    let (failures_tx, _failures_rx) = flume::unbounded::<Error>();

    let first = pending
        .register(&branch, 1, Some(Arc::new(batch)))
        .expect("a single batch fits the budget");
    let error = pending
        .register(&branch, 1, Some(Arc::new(dictionary_batch(None))))
        .expect_err("the second batch exceeds the budget");
    assert!(error.to_string().contains("byte budget"), "{error}");
    // Completing the first entry releases its bytes; a new registration
    // of the same size fits again.
    pending.apply(
        ReceiptFrame {
            kind: ReceiptKind::Acked,
            seq: first,
        },
        &failures_tx,
    );
    // The ack runs on a spawned task; the map entry and its byte
    // accounting are released synchronously before that task runs.
    pending
        .register(&branch, 1, Some(Arc::new(dictionary_batch(None))))
        .expect("released bytes are available to new registrations");
}

/// Spec: a receipt-overflow escalation with BOTH queues full must not
/// synchronously block the calling (potentially tokio-worker) thread.
#[test]
fn receipt_overflow_escalation_does_not_block() {
    let (outbox_tx, _outbox_rx) = flume::bounded::<(Quad, ReceiptFrame)>(1);
    let (failures_tx, _failures_rx) = flume::bounded::<Error>(1);
    // Fill both queues so both try_send paths fail.
    outbox_tx
        .send((
            quad_a_to_b(),
            ReceiptFrame {
                kind: ReceiptKind::Acked,
                seq: 0,
            },
        ))
        .unwrap();
    failures_tx.send(Error::Process("filler".into())).unwrap();
    let ack = RemoteAck {
        outbox: outbox_tx,
        quad: quad_a_to_b(),
        seq: 7,
        failures: failures_tx,
    };
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        crate::input::Ack::mark_held(&ack);
        crate::input::Ack::release_held(&ack);
        done_tx.send(()).unwrap();
    });
    done_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("mark_held/release_held must not block on full queues");
    worker.join().expect("escalation thread finishes");
}

/// Spec: a connection with receipts still pending is a (possibly slow)
/// live downstream — the short dead-peer idle timeout must not tear it
/// down; only the long receipt-wait budget may.
#[tokio::test(flavor = "multi_thread")]
async fn receipt_wait_budget_keeps_a_busy_connection_alive() {
    let quad = quad_a_to_b();
    let upstream_config = NetworkManagerConfig {
        channel_capacity: 8,
        read_idle_timeout: std::time::Duration::from_millis(200),
        receipt_wait_timeout: std::time::Duration::from_secs(60),
        reconnect_attempts: 0,
        ..NetworkManagerConfig::default()
    };
    let upstream = NetworkManager::with_config(upstream_config).unwrap();
    let downstream = NetworkManager::new(8);
    upstream.spawn();
    downstream.spawn();

    let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
    downstream.register_inbound(quad, input_tx);
    let (client_side, server_side) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(server_side));
    let edge = upstream.open_edge_with_stream(Box::new(client_side), quad);
    let failures = upstream.failure_receiver();

    // One delivered-but-unacknowledged envelope keeps a receipt pending.
    edge.sender
        .send_async(Envelope::Data(
            Arc::new(dictionary_batch(None)),
            Arc::new(RecordingAck::default()),
        ))
        .await
        .unwrap();
    let _ = next_envelope(&input_rx).await;

    // Well past the 200ms dead-peer timeout, with the receipt still
    // outstanding, the edge must remain usable.
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    if let Ok(failure) = failures.try_recv() {
        panic!("a busy edge must survive the dead-peer idle timeout: {failure}");
    }
    edge.sender
        .send_async(Envelope::Data(
            Arc::new(dictionary_batch(None)),
            Arc::new(RecordingAck::default()),
        ))
        .await
        .expect("the connection is still alive");
    let _ = next_envelope(&input_rx).await;

    upstream.shutdown();
    downstream.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn transparent_reconnect_replays_unreceipted_frames() {
    let quad = quad_a_to_b();
    let reconnect = NetworkManagerConfig {
        reconnect_attempts: 3,
        reconnect_grace: std::time::Duration::from_secs(5),
        ..NetworkManagerConfig::default()
    };
    let upstream = NetworkManager::with_config(reconnect.clone()).unwrap();
    let downstream = NetworkManager::with_config(reconnect).unwrap();
    upstream.spawn();
    downstream.spawn();
    let failures = upstream.failure_receiver();

    let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
    downstream.register_inbound(quad, input_tx);

    let (client1, server1) = tokio::io::duplex(64 * 1024);
    let (client2, server2) = tokio::io::duplex(64 * 1024);
    // Kill the DOWNSTREAM-owned half: the serve loop's read errors and
    // unwinds, which closes the real duplex and EOFs the upstream's read
    // immediately — exactly like a peer process dying mid-stream.
    let kill1 = KillHandle::default();
    let kill_state = kill1.0.clone();
    downstream.accept_stream(Box::new(KillableStream {
        inner: Box::new(server1),
        state: kill_state,
    }));
    downstream.accept_stream(Box::new(server2));
    let transport = QueuedTransport::of(vec![Box::new(client1), Box::new(client2)]);
    let edge = upstream.open_edge_deferred(transport.clone(), quad);
    // First connection: the deferred open dials client1.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let branch = Arc::new(RecordingAck::default());
    edge.sender
        .send_async(Envelope::Data(
            Arc::new(dictionary_batch(None)),
            branch.clone(),
        ))
        .await
        .unwrap();
    // The downstream chain receives the frame: delivered, not yet acked.
    let delivered_envelope = next_envelope(&input_rx).await;
    let delivered_ack = match &delivered_envelope {
        Envelope::Data(_, ack) => ack.clone(),
        _ => panic!("expected a data envelope"),
    };
    // Break the connection: killing the shared server half resets the
    // connection from both ends mid-stream.
    kill1.kill();

    // The transparent reconnect dials client2 and replays seq 0; the
    // receiver drops the duplicate silently (no re-delivery).
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(200),
            next_envelope(&input_rx)
        )
        .await
        .is_err(),
        "replayed duplicate must not re-deliver to the local chain"
    );
    // The ORIGINAL acknowledgement still completes — through the
    // session-scoped receipt route on the new connection. Only after
    // the downstream acknowledges processing does the upstream branch
    // settle (no mirror-ack: offsets never advance past unprocessed
    // data).
    delivered_ack.ack().await.expect("original ack completes");
    tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
        while !branch.acked.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("branch acked through the session receipt route");
    assert!(
        !branch.aborted.load(std::sync::atomic::Ordering::SeqCst),
        "transparent recovery must not abort the branch"
    );
    // No edge failure surfaced: the reconnect healed the loss.
    if let Ok(failure) =
        tokio::time::timeout(std::time::Duration::from_millis(300), failures.recv_async()).await
    {
        panic!("unexpected failure: {failure:?}");
    }
    upstream.shutdown();
    downstream.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn overlapping_connections_neither_double_deliver_nor_regress_the_dedup_mark() {
    let quad = quad_a_to_b();
    let reconnect = NetworkManagerConfig {
        reconnect_attempts: 3,
        reconnect_grace: std::time::Duration::from_secs(5),
        ..NetworkManagerConfig::default()
    };
    let upstream = NetworkManager::with_config(reconnect.clone()).unwrap();
    let downstream = NetworkManager::with_config(reconnect).unwrap();
    upstream.spawn();
    downstream.spawn();
    let failures = upstream.failure_receiver();

    // Capacity 1: seq 0 fills the local channel, seq 1 parks the first
    // connection's delivery inside backpressure — the exact window a
    // transparent reconnect overlaps with a still-serving old wire.
    let (input_tx, input_rx) = flume::bounded::<Envelope>(1);
    downstream.register_inbound(quad, input_tx);

    let (client1, server1) = tokio::io::duplex(64 * 1024);
    let (client2, server2) = tokio::io::duplex(64 * 1024);
    let (client3, server3) = tokio::io::duplex(64 * 1024);
    let kill1 = KillHandle::default();
    let kill1_state = kill1.0.clone();
    let kill2 = KillHandle::default();
    let kill2_state = kill2.0.clone();
    downstream.accept_stream(Box::new(KillableStream {
        inner: Box::new(server1),
        state: kill1_state,
    }));
    downstream.accept_stream(Box::new(KillableStream {
        inner: Box::new(server2),
        state: kill2_state,
    }));
    downstream.accept_stream(Box::new(server3));
    let transport = QueuedTransport::of(vec![
        Box::new(client1),
        Box::new(client2),
        Box::new(client3),
    ]);
    let edge = upstream.open_edge_deferred(transport, quad);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Three unreceipted frames: seq 0 is delivered (channel now full),
    // seq 1 parks the old connection's send, seq 2 sits behind it.
    let branch = Arc::new(RecordingAck::default());
    for _ in 0..3 {
        edge.sender
            .send_async(Envelope::Data(
                Arc::new(dictionary_batch(None)),
                branch.clone(),
            ))
            .await
            .unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    kill1.kill();
    // The reconnect dials client2 and replays every unreceipted frame
    // while the old connection's parked send is still in flight.
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    // Drain with a generous budget: exactly three envelopes (seq 0..=2)
    // may arrive. The delivery lock serializes the parked send against
    // the replay, so no sequence enters the local channel twice.
    for expected in 0..3 {
        tokio::time::timeout(TEST_PROPAGATION_BUDGET, next_envelope(&input_rx))
            .await
            .unwrap_or_else(|_| panic!("expected envelope {expected} to arrive"));
    }
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(800),
            next_envelope(&input_rx)
        )
        .await
        .is_err(),
        "overlapping connections must not double-deliver any sequence"
    );

    // Prove the delivered mark never regressed: nothing was acked, so a
    // second reconnect replays seq 0..=2 — every frame must be dropped.
    kill2.kill();
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(800),
            next_envelope(&input_rx)
        )
        .await
        .is_err(),
        "replay after the overlap must drop every sequence (mark regressed?)"
    );
    if let Ok(failure) =
        tokio::time::timeout(std::time::Duration::from_millis(300), failures.recv_async()).await
    {
        panic!("unexpected failure: {failure:?}");
    }
    upstream.shutdown();
    downstream.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn reconnect_budget_exhaustion_fails_closed() {
    let quad = quad_a_to_b();
    let reconnect = NetworkManagerConfig {
        reconnect_attempts: 2,
        reconnect_grace: std::time::Duration::from_millis(200),
        ..NetworkManagerConfig::default()
    };
    let upstream = NetworkManager::with_config(reconnect.clone()).unwrap();
    let downstream = NetworkManager::with_config(reconnect).unwrap();
    upstream.spawn();
    downstream.spawn();
    let failures = upstream.failure_receiver();

    let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
    downstream.register_inbound(quad, input_tx);

    let (client1, server1) = tokio::io::duplex(64 * 1024);
    let kill1 = KillHandle::default();
    let kill_state = kill1.0.clone();
    downstream.accept_stream(Box::new(KillableStream {
        inner: Box::new(server1),
        state: kill_state,
    }));
    // One stream only: after the first break, every redial fails.
    let transport = QueuedTransport::failing_after(vec![Box::new(client1)], 0);
    let edge = upstream.open_edge_deferred(transport, quad);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let branch = Arc::new(RecordingAck::default());
    edge.sender
        .send_async(Envelope::Data(
            Arc::new(dictionary_batch(None)),
            branch.clone(),
        ))
        .await
        .unwrap();
    let _ = next_envelope(&input_rx).await;
    kill1.kill();

    tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
        while !branch.aborted.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("budget exhaustion must abort the branch");
    let failure = tokio::time::timeout(TEST_PROPAGATION_BUDGET, failures.recv_async())
        .await
        .expect("budget exhaustion must report the edge failure")
        .unwrap();
    assert!(failure.to_string().contains("reconnect"), "{failure}");
    upstream.shutdown();
    downstream.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnect_aborts_pending_and_closes_edge() {
    let quad = quad_a_to_b();
    // Reconnect disabled: this test pins the immediate fail-closed
    // behavior that a zero reconnect budget preserves.
    let offline = NetworkManagerConfig {
        reconnect_attempts: 0,
        ..NetworkManagerConfig::default()
    };
    // A short registration grace: this edge never registers a route, so the
    // rejection fires quickly and deterministically instead of stalling on
    // the 10s default under parallel load.
    let upstream = NetworkManager::with_config(offline).unwrap();
    let downstream = NetworkManager::with_config(NetworkManagerConfig {
        reconnect_attempts: 0,
        registration_grace: std::time::Duration::from_millis(200),
        ..NetworkManagerConfig::default()
    })
    .unwrap();
    upstream.spawn();
    downstream.spawn();
    let failures = upstream.failure_receiver();

    // Register the route and wait until the delivery is served end to end
    // BEFORE killing the manager: otherwise shutdown can win the race against
    // the accept loop (the stream is queued but never served, its write half
    // is never dropped, and the upstream waits forever for an EOF). A served
    // frame proves the connection is live, so the disconnect below lands
    // under the pending receipt deterministically.
    let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
    downstream.register_inbound(quad, input_tx);
    let (client_side, server_side) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(server_side));
    let edge = upstream.open_edge_with_stream(Box::new(client_side), quad);

    let branch = std::sync::Arc::new(RecordingAck::default());
    edge.sender
        .send_async(Envelope::Data(
            std::sync::Arc::new(dictionary_batch(None)),
            branch.clone(),
        ))
        .await
        .expect("send data");
    let _ = next_envelope(&input_rx).await;

    // Kill the downstream manager: the connection dies under the pending
    // receipt. The pump fails (or the drain tick flush fails), the pending
    // branch aborts, and the failure surfaces on the manager.
    downstream.shutdown();

    // The failure broadcast happens after the manager aborts pending
    // branches, but abort_all only *spawns* the abort tasks - poll the
    // flag explicitly rather than assuming it is already set.
    let failure = tokio::time::timeout(TEST_PROPAGATION_BUDGET, failures.recv_async())
        .await
        .expect("failure within timeout")
        .expect("failure channel open");
    tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
        while !branch.aborted.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("pending branch aborted on disconnect");
    assert!(
        !branch.acked.load(std::sync::atomic::Ordering::SeqCst),
        "an aborted branch must never complete its source ack"
    );
    assert!(failure.to_string().contains("remote edge") || failure.to_string().contains("inbound"));

    // The edge channel sender fails for new sends after the pump exits.
    tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
        loop {
            let branch_retry = std::sync::Arc::new(RecordingAck::default());
            if edge
                .sender
                .send_async(Envelope::Data(
                    std::sync::Arc::new(dictionary_batch(None)),
                    branch_retry,
                ))
                .await
                .is_err()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("edge sender eventually fails");

    upstream.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_consumer_backpressures_the_edge_sender() {
    // The load-bearing invariant: a stalled downstream must halt the
    // upstream sender instead of growing buffers without bound. The
    // bounded edge channel fills, then the pump blocks on the socket
    // (BufWriter + duplex window), so further sends pend.
    let quad = quad_a_to_b();
    // Upstream edge capacity 1 + downstream input capacity 1: any third
    // in-flight envelope must pend.
    let upstream = NetworkManager::new(1);
    let downstream = NetworkManager::new(64);
    upstream.spawn();
    downstream.spawn();

    // Downstream input capacity 1 makes the bounded-buffer assertion
    // deterministic: one envelope accepted, then pressure must reach the
    // upstream sender (channel bound -> pump write -> TCP window).
    let (input_tx, input_rx) = flume::bounded::<Envelope>(1);
    downstream.register_inbound(quad, input_tx);

    // A tiny duplex window so one large batch saturates pump + socket.
    let (client_side, server_side) = tokio::io::duplex(16 * 1024);
    downstream.accept_stream(Box::new(server_side));
    let edge = upstream.open_edge_with_stream(Box::new(client_side), quad);

    let big_batch = Arc::new(MessageBatch::new_arrow(large_batch(4_000)));
    // Send until one send PENDS. The in-flight budget is finite and small
    // (upstream channel 1 + pump-held 1 + downstream channel 1 + the
    // serve-held envelope + socket buffers), so with a never-draining
    // consumer some send must block after a bounded number of batches —
    // if sends kept completing, buffering would be unbounded.
    let mut blocked = None;
    for sequence in 0..32 {
        let send = edge.sender.send_async(Envelope::Data(
            big_batch.clone(),
            Arc::new(crate::input::NoopAck),
        ));
        match tokio::time::timeout(std::time::Duration::from_millis(200), send).await {
            Ok(Ok(())) => continue,
            Ok(Err(error)) => panic!("edge failed prematurely at {sequence}: {error}"),
            Err(_elapsed) => {
                // This send never completed within 200ms: backpressure
                // reached the sender. Drop the future (the in-flight
                // envelope is abandoned); the drain check below proves
                // the edge still works for a fresh consumer.
                blocked = Some(sequence);
                break;
            }
        }
    }
    let blocked_at =
        blocked.expect("no send ever blocked with a stalled consumer — buffers grew without bound");
    assert!(
        blocked_at < 16,
        "backpressure engaged only after {blocked_at} in-flight batches"
    );

    // Draining the consumer releases the pressure: the pending send
    // completes once the downstream channel accepts frames again.
    let mut received = 0;
    let _ = tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
        while received < 1 {
            if input_rx.try_recv().is_ok() {
                received += 1;
            }
        }
    })
    .await;

    upstream.shutdown();
    downstream.shutdown();
}

/// A lost Held receipt (synchronous outbox send on a dead edge) must be a
/// harmless no-op at the ack site — degradation is the drain timeout
/// upstream, never a panic or data loss here.
#[tokio::test(flavor = "multi_thread")]
async fn held_receipt_on_dead_edge_is_a_noop() {
    let (outbox, _outbox_rx) = flume::unbounded::<(Quad, ReceiptFrame)>();
    drop(_outbox_rx); // the edge writer is gone
    let (failures, _failure_rx) = flume::unbounded::<Error>();
    let ack = RemoteAck {
        outbox,
        quad: quad_a_to_b(),
        seq: 3,
        failures,
    };
    ack.mark_held(); // must not panic on a closed outbox
    ack.release_held();
    assert!(
        ack.ack().await.is_err(),
        "acking a dead edge reports failure"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn failure_reports_are_delivered_sequentially_through_the_global_channel() {
    let config = NetworkManagerConfig {
        max_failure_queue: 1,
        ..NetworkManagerConfig::default()
    };
    let manager = NetworkManager::with_config(config).unwrap();
    let failures = manager.failure_receiver();
    // Deterministic drain order: consume the first report BEFORE the second
    // is sent, so the bounded queue never races its own consumer (a report
    // hitting a full queue is dropped by contract, which would make an
    // order-dependent spawn here flaky under load).
    manager.report_failure(Error::Process("first failure".into()));
    let first = failures.recv_async().await.unwrap();
    assert!(first.to_string().contains("first failure"), "{first}");
    manager.report_failure(Error::Process("second failure".into()));
    let second = tokio::time::timeout(std::time::Duration::from_secs(1), failures.recv_async())
        .await
        .expect("second failure must remain queued")
        .unwrap();
    assert!(second.to_string().contains("second failure"), "{second}");
}

fn large_batch(rows: usize) -> datafusion::arrow::record_batch::RecordBatch {
    use datafusion::arrow::array::{Int64Array, LargeBinaryArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::LargeBinary, false),
    ]));
    let payloads: LargeBinaryArray = (0..rows)
        .map(|row| vec![(row % 256) as u8; 128])
        .collect::<Vec<Vec<u8>>>()
        .into_iter()
        .map(Some)
        .collect();
    datafusion::arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())),
            Arc::new(payloads),
        ],
    )
    .expect("valid large batch")
}

#[tokio::test(flavor = "multi_thread")]
async fn peer_death_without_eos_fails_the_downstream_side() {
    // Spec: a remote edge that dies mid-stream terminates BOTH sides in
    // error. The upstream observes the write failure; here the downstream
    // side must surface a failure too — not treat the dropped connection
    // as a clean upstream finish.
    let quad = quad_a_to_b();
    let upstream = NetworkManager::new(8);
    let downstream = NetworkManager::new(8);
    upstream.spawn();
    downstream.spawn();
    let failures = downstream.failure_receiver();

    let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
    downstream.register_inbound(quad, input_tx);

    let (client_side, server_side) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(server_side));
    let edge = upstream.open_edge_with_stream(Box::new(client_side), quad);

    edge.sender
        .send_async(Envelope::Data(
            Arc::new(dictionary_batch(Some("prefix"))),
            Arc::new(crate::input::NoopAck),
        ))
        .await
        .expect("send data");
    let received = next_envelope(&input_rx).await;
    assert!(matches!(received, Envelope::Data(_, _)));

    // Drop the upstream side WITHOUT Eos: mid-stream death.
    drop(edge);
    upstream.shutdown();

    let failure = tokio::time::timeout(TEST_PROPAGATION_BUDGET, failures.recv_async())
        .await
        .expect("failure within timeout")
        .expect("failure channel open");
    assert!(
        failure.to_string().contains("without Eos"),
        "expected an abnormal-termination failure, got: {failure}"
    );

    downstream.shutdown();
}

#[tokio::test]
async fn bind_tcp_refuses_without_credentials() {
    let manager = NetworkManager::new(64);
    let error = manager
        .bind_tcp("127.0.0.1:0".parse().expect("addr"))
        .await
        .expect_err("bind must refuse without credentials");
    assert!(
        error.to_string().contains("without credentials"),
        "expected a credentials bind refusal, got: {error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_transport_connects_and_receives() {
    let quad = quad_a_to_b();
    let upstream = authenticated_manager("node-a", "shuffle-secret");
    let downstream = authenticated_manager("node-b", "shuffle-secret");
    upstream.spawn();
    downstream.spawn();

    let port = downstream
        .bind_tcp("127.0.0.1:0".parse().expect("addr"))
        .await
        .expect("bind");
    let (input_tx, input_rx) = flume::bounded::<Envelope>(64);
    downstream
        .register_inbound_for_session(
            quad,
            input_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "tcp-job".into(),
                generation: 1,
            },
        )
        .expect("inbound registration succeeds");

    let transport = TcpEdgeTransport {
        tls: None,
        addr: format!("127.0.0.1:{port}").parse().expect("addr"),
        max_attempts: 3,
        peer_node: None,
    };
    let edge = upstream
        .open_edge_for_session(&transport, quad, "node-b", "tcp-job", 1)
        .await
        .expect("connect");

    let branch = std::sync::Arc::new(RecordingAck::default());
    edge.sender
        .send_async(Envelope::Data(
            std::sync::Arc::new(dictionary_batch(Some("tcp"))),
            branch.clone(),
        ))
        .await
        .expect("send");
    let received = next_envelope(&input_rx).await;
    let Envelope::Data(batch, ack) = received else {
        panic!("expected data over TCP");
    };
    assert_eq!(batch.get_input_name(), Some("tcp".to_owned()));
    ack.ack().await.expect("ack");
    tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
        while !branch.acked.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("receipt over TCP");

    // Task 2.4's cleanup claim, as an explicit assertion: after shutdown
    // the data-plane listener is gone — a fresh connection to the bound
    // port must be refused, not accepted into a dead backlog.
    upstream.shutdown();
    downstream.shutdown();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let refused = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await;
    assert!(
        refused.is_err(),
        "data-plane listener still accepts after shutdown"
    );
}
/// One shared fleet CA with a per-node certificate, as a real fleet
/// deploys: all peers verify against the same CA.
fn generate_fleet_material() -> (Vec<(String, String)>, String) {
    use rcgen::CertificateParams;
    use rcgen::KeyPair;
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();
    // rcgen 0.14: the issuer carries the CA params and key, not the built
    // certificate; built once outside the loop so the params move once.
    let ca_issuer = rcgen::Issuer::new(ca_params, &ca_key);
    let mut nodes = Vec::new();
    for _ in 0..2 {
        let node_params =
            CertificateParams::new(vec![super::DATA_PLANE_TLS_SERVER_NAME.to_owned()]).unwrap();
        let node_key = KeyPair::generate().unwrap();
        let node = node_params.signed_by(&node_key, &ca_issuer).unwrap();
        nodes.push((node.pem(), node_key.serialize_pem()));
    }
    (nodes, ca.pem())
}

fn tls_manager_config(
    node: &str,
    secret: &str,
    cert: &str,
    key: &str,
    ca: &str,
) -> NetworkManagerConfig {
    let credentials = DataPlaneCredentials::new(node, secret).expect("test credentials");
    NetworkManagerConfig {
        credentials: Some(credentials),
        registration_grace: std::time::Duration::from_millis(100),
        tls: Some(DataPlaneTlsConfig::from_pem(cert, key, ca).expect("tls")),
        ..Default::default()
    }
}

/// Fleet-CA mTLS end to end over loopback: the full stack (TLS
/// handshake, HMAC session handshake, frames, receipts) behaves exactly
/// like the plaintext path.
#[tokio::test(flavor = "multi_thread")]
async fn tls_transport_connects_and_receives() {
    let quad = quad_a_to_b();
    let (nodes, ca) = generate_fleet_material();
    let (cert_a, key_a) = &nodes[0];
    let (cert_b, key_b) = &nodes[1];
    let upstream = std::sync::Arc::new(
        NetworkManager::with_config(tls_manager_config(
            "node-a",
            "shuffle-secret",
            cert_a,
            key_a,
            &ca,
        ))
        .unwrap(),
    );
    let downstream = std::sync::Arc::new(
        NetworkManager::with_config(tls_manager_config(
            "node-b",
            "shuffle-secret",
            cert_b,
            key_b,
            &ca,
        ))
        .unwrap(),
    );
    upstream.spawn();
    downstream.spawn();

    let port = downstream
        .bind_tcp("127.0.0.1:0".parse().expect("addr"))
        .await
        .expect("bind");
    let (input_tx, input_rx) = flume::bounded::<Envelope>(64);
    downstream
        .register_inbound_for_session(
            quad,
            input_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "tls-job".into(),
                generation: 1,
            },
        )
        .expect("inbound registration succeeds");

    let transport = TcpEdgeTransport {
        addr: format!("127.0.0.1:{port}").parse().expect("addr"),
        max_attempts: 3,
        tls: Some(DataPlaneTlsConfig::from_pem(cert_a, key_a, &ca).expect("client tls")),
        peer_node: None,
    };
    let edge = upstream
        .open_edge_for_session(&transport, quad, "node-b", "tls-job", 1)
        .await
        .expect("connect");

    let branch = std::sync::Arc::new(RecordingAck::default());
    edge.sender
        .send_async(Envelope::Data(
            std::sync::Arc::new(dictionary_batch(Some("tls"))),
            branch.clone(),
        ))
        .await
        .expect("send");
    let received = next_envelope(&input_rx).await;
    let Envelope::Data(batch, ack) = received else {
        panic!("expected data over TLS");
    };
    assert_eq!(batch.get_input_name(), Some("tls".to_owned()));
    ack.ack().await.expect("ack");
    tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
        while !branch.acked.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("receipt over TLS");
    upstream.shutdown();
    downstream.shutdown();
}

/// A plaintext client against a TLS server never completes a session:
/// the connection fails closed (no protocol fallback).
#[tokio::test(flavor = "multi_thread")]
async fn plaintext_transport_against_tls_server_fails_closed() {
    let quad = quad_a_to_b();
    let (nodes, ca) = generate_fleet_material();
    let (cert_b, key_b) = &nodes[1];
    let downstream = std::sync::Arc::new(
        NetworkManager::with_config(tls_manager_config(
            "node-b",
            "shuffle-secret",
            cert_b,
            key_b,
            &ca,
        ))
        .unwrap(),
    );
    downstream.spawn();
    let port = downstream
        .bind_tcp("127.0.0.1:0".parse().expect("addr"))
        .await
        .expect("bind");
    let (input_tx, input_rx) = flume::bounded::<Envelope>(64);
    downstream
        .register_inbound_for_session(
            quad,
            input_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "plain-job".into(),
                generation: 1,
            },
        )
        .expect("inbound registration succeeds");

    let transport = TcpEdgeTransport {
        addr: format!("127.0.0.1:{port}").parse().expect("addr"),
        max_attempts: 1,
        tls: None,
        peer_node: None,
    };
    // The plaintext client may complete the TCP connect, but its bytes
    // are not a TLS record: the server handshake fails and NOTHING is
    // ever delivered into the session channel.
    let upstream = authenticated_manager("node-a", "shuffle-secret");
    upstream.spawn();
    let edge = upstream
        .open_edge_for_session(&transport, quad, "node-b", "plain-job", 1)
        .await
        .expect("stream-level connect (failure surfaces per-session)");
    let branch = std::sync::Arc::new(RecordingAck::default());
    edge.sender
        .send_async(Envelope::Data(
            std::sync::Arc::new(dictionary_batch(Some("plain"))),
            branch.clone(),
        ))
        .await
        .expect("send");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), input_rx.recv_async())
            .await
            .is_err(),
        "a plaintext stream must never deliver frames into a TLS session"
    );
    upstream.shutdown();
    downstream.shutdown();
}

/// A client whose certificate chains to a DIFFERENT CA fails the
/// handshake at connect time — explicit, no protocol fallback.
#[tokio::test(flavor = "multi_thread")]
async fn foreign_ca_client_is_rejected_at_connect() {
    let quad = quad_a_to_b();
    let (nodes, ca) = generate_fleet_material();
    let (cert_b, key_b) = &nodes[1];
    let downstream = std::sync::Arc::new(
        NetworkManager::with_config(tls_manager_config(
            "node-b",
            "shuffle-secret",
            cert_b,
            key_b,
            &ca,
        ))
        .unwrap(),
    );
    downstream.spawn();
    let port = downstream
        .bind_tcp("127.0.0.1:0".parse().expect("addr"))
        .await
        .expect("bind");

    // A second, unrelated fleet.
    let (foreign_nodes, foreign_ca) = generate_fleet_material();
    let (foreign_cert, foreign_key) = &foreign_nodes[0];
    let upstream = std::sync::Arc::new(
        NetworkManager::with_config(tls_manager_config(
            "node-a",
            "shuffle-secret",
            foreign_cert,
            foreign_key,
            &foreign_ca,
        ))
        .unwrap(),
    );
    upstream.spawn();
    let transport = TcpEdgeTransport {
        addr: format!("127.0.0.1:{port}").parse().expect("addr"),
        max_attempts: 1,
        tls: Some(
            DataPlaneTlsConfig::from_pem(foreign_cert, foreign_key, &foreign_ca)
                .expect("client tls"),
        ),
        peer_node: None,
    };
    let result = upstream
        .open_edge_for_session(&transport, quad, "node-b", "foreign-job", 1)
        .await;
    assert!(
        result.is_err(),
        "a foreign-CA client must fail the TLS handshake at connect"
    );
    upstream.shutdown();
    downstream.shutdown();
}

/// Partial TLS material is an explicit construction error.
#[test]
fn tls_partial_material_is_rejected() {
    let (nodes, ca) = generate_fleet_material();
    let (cert, key) = &nodes[0];
    let ca = ca.as_str();
    assert!(DataPlaneTlsConfig::from_pem(cert, key, "").is_err());
    assert!(DataPlaneTlsConfig::from_pem("", key, ca).is_err());
    assert!(DataPlaneTlsConfig::from_pem(cert, "", ca).is_err());
    assert!(DataPlaneTlsConfig::from_pem(cert, key, ca).is_ok());
}

// ---------- codec / framing error-path coverage ----------

#[test]
fn credentials_debug_redacts_the_secret_and_empty_secrets_are_rejected() {
    let credentials =
        DataPlaneCredentials::new("node-a", "super-secret").expect("valid credentials");
    let debug = format!("{credentials:?}");
    assert!(debug.contains("node-a"), "{debug}");
    assert!(debug.contains("<redacted>"), "{debug}");
    assert!(!debug.contains("super-secret"), "{debug}");

    let error = DataPlaneCredentials::new("node-a", "")
        .expect_err("an empty shared secret must be rejected");
    assert!(error.to_string().contains("must not be empty"), "{error}");
}

#[test]
fn server_ack_verification_rejects_identity_mismatch() {
    let credentials = DataPlaneCredentials::new("node-a", "shuffle-secret").unwrap();
    let auth = credentials.session("node-b", "job-1", 3, quad_a_to_b());
    let request = auth.client_handshake().unwrap();
    let mut ack = auth.server_ack(&request, "node-b");
    // A well-formed ack with the same identity verifies.
    auth.verify_server_ack(&ack).expect("matching ack verifies");
    // Any identity drift is a protocol error before the MAC check.
    ack.job_id = "other-job".into();
    let error = auth.verify_server_ack(&ack).unwrap_err();
    assert!(error.to_string().contains("identity mismatch"), "{error}");
}

/// Regression (ack replay): an ack captured from an EARLIER handshake of the
/// same quad is validly MACed, but it echoes the OLD nonce — the
/// length-only check accepted it as the fresh connection's acknowledgement;
/// the recorded-nonce comparison must reject it.
#[test]
fn server_ack_verification_rejects_a_replayed_older_ack() {
    let credentials = DataPlaneCredentials::new("node-a", "shuffle-secret").unwrap();
    let auth = credentials.session("node-b", "job-1", 3, quad_a_to_b());
    // Handshake #1 (an earlier connection) and its validly signed ack.
    let earlier = auth.client_handshake().unwrap();
    let earlier_ack = auth.server_ack(&earlier, "node-b");
    // Handshake #2: the CURRENT connection replaces the recorded nonce.
    let current = auth.client_handshake().unwrap();
    let current_ack = auth.server_ack(&current, "node-b");
    auth.verify_server_ack(&current_ack)
        .expect("the current connection's ack verifies");
    let error = auth.verify_server_ack(&earlier_ack).unwrap_err();
    assert!(
        error.to_string().contains("identity mismatch"),
        "a replayed older ack must fail, got: {error}"
    );
    // An ack for a handshake that was never sent cannot verify either.
    let unsent = auth.client_handshake().unwrap();
    let unsent_ack = auth.server_ack(&unsent, "node-b");
    // Simulate an ack arriving BEFORE any handshake was recorded: a fresh
    // session with no sent nonce must fail closed.
    let fresh = credentials.session("node-b", "job-1", 3, quad_a_to_b());
    let error = fresh.verify_server_ack(&unsent_ack).unwrap_err();
    assert!(
        error.to_string().contains("identity mismatch"),
        "no recorded nonce must fail closed, got: {error}"
    );
}

#[test]
fn wire_signal_ignores_data_envelopes_and_default_encoder_matches_new() {
    let batch = Arc::new(dictionary_batch(None));
    let envelope = Envelope::Data(batch, Arc::new(crate::input::NoopAck));
    assert!(WireSignal::from_envelope(&envelope).is_none());

    // `DataEncoder::default` is the same encoder as `new`.
    let sample = dictionary_batch(Some("default"));
    let mut via_default = DataEncoder::default();
    let mut via_new = DataEncoder::new();
    let payload_default = via_default.encode(&sample, 1).expect("default encode");
    let payload_new = via_new.encode(&sample, 1).expect("new encode");
    let mut decoder = DataDecoder::new();
    let (decoded, seq) = decoder
        .decode(bytes::Bytes::from(payload_default.clone()))
        .expect("default decode");
    assert_eq!(seq, 1);
    assert_eq!(decoded.get_input_name(), Some("default".to_owned()));
    // Both encoders emit byte-identical payloads for the same input.
    assert_eq!(payload_default, payload_new);
}

/// Split a data payload at its meta block: returns the rewritten prefix
/// (with `has_schema` forced to the given value) and the cursor position
/// just after the original meta block.
fn rewrite_meta(payload: &[u8], has_schema: bool) -> (Vec<u8>, usize) {
    let meta_len = u32::from_le_bytes(payload[0..4].try_into().expect("sliced")) as usize;
    let mut meta: WireDataMeta = serde_json::from_slice(&payload[4..4 + meta_len]).expect("meta");
    meta.has_schema = has_schema;
    let rewritten = serde_json::to_vec(&meta).expect("meta encode");
    let mut out = Vec::new();
    out.extend_from_slice(&(rewritten.len() as u32).to_le_bytes());
    out.extend_from_slice(&rewritten);
    (out, 4 + meta_len)
}

/// Re-encode a length-prefixed IPC piece from raw flatbuffer bytes.
fn craft_piece(flatbuf: &[u8]) -> Vec<u8> {
    let mut piece = Vec::new();
    let total = 8 + flatbuf.len();
    piece.extend_from_slice(&(total as u32).to_le_bytes());
    piece.extend_from_slice(&(flatbuf.len() as u32).to_le_bytes());
    piece.extend_from_slice(flatbuf);
    piece
}

#[test]
fn decoder_rejects_schema_id_mismatch_and_unparsable_schema() {
    let mut encoder_a = DataEncoder::new();
    let payload_a = encoder_a
        .encode(&dictionary_batch(None), 0)
        .expect("encode A");
    let mut decoder = DataDecoder::new();
    decoder
        .decode(bytes::Bytes::from(payload_a))
        .expect("learn schema A");

    // A second schema encoded by a fresh encoder, with its schema piece
    // stripped and the meta forced to `has_schema=false`, declares an id
    // the decoder never learned.
    let plain = int64_only_batch();
    let mut encoder_b = DataEncoder::new();
    let payload_b = encoder_b.encode(&plain, 1).expect("encode B");
    let (mut corrupted, cursor) = rewrite_meta(&payload_b, false);
    // Skip B's schema piece: parse and drop it.
    let (_, after_schema) =
        read_piece(&bytes::Bytes::from(payload_b.clone()), cursor).expect("schema piece");
    corrupted.extend_from_slice(&payload_b[after_schema..]);
    let error = decoder.decode(bytes::Bytes::from(corrupted)).unwrap_err();
    assert!(
        error.to_string().contains("does not match declared id"),
        "{error}"
    );

    // A schema-bearing frame whose schema flatbuffer is garbage fails to
    // parse instead of panicking.
    let (mut garbage, _) = rewrite_meta(&payload_b, true);
    garbage.extend_from_slice(&craft_piece(&[0xde, 0xad, 0xbe, 0xef, 0xba, 0xad]));
    let mut fresh_decoder = DataDecoder::new();
    let error = fresh_decoder
        .decode(bytes::Bytes::from(garbage))
        .unwrap_err();
    assert!(
        error.to_string().contains("schema decode failed"),
        "{error}"
    );
}

fn int64_only_batch() -> MessageBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
        .expect("valid batch");
    MessageBatch::new_arrow(batch)
}

#[test]
fn decoder_rejects_invalid_ipc_messages_and_stray_schema_messages() {
    let mut encoder = DataEncoder::new();
    let payload = encoder.encode(&dictionary_batch(None), 0).expect("encode");
    let mut decoder = DataDecoder::new();
    decoder
        .decode(bytes::Bytes::from(payload.clone()))
        .expect("prime the schema cache");

    // A payload whose trailing piece is not a valid IPC message.
    let (mut garbage, _) = rewrite_meta(&payload, false);
    garbage.extend_from_slice(&craft_piece(&[0x11, 0x22, 0x33, 0x44, 0x55]));
    let error = decoder.decode(bytes::Bytes::from(garbage)).unwrap_err();
    assert!(error.to_string().contains("IPC message invalid"), "{error}");

    // A schema message arriving without `has_schema` set is a protocol
    // error: keep the original pieces (schema first) but claim no schema.
    let (mut stray, _) = rewrite_meta(&payload, false);
    stray.extend_from_slice(&payload[4 + meta_len_of(&payload)..]);
    let error = decoder.decode(bytes::Bytes::from(stray)).unwrap_err();
    assert!(
        error.to_string().contains("unexpected schema message"),
        "{error}"
    );
}

fn meta_len_of(payload: &[u8]) -> usize {
    u32::from_le_bytes(payload[0..4].try_into().expect("sliced")) as usize
}

#[test]
fn decoder_rejects_truncated_dictionary_and_record_batch_bodies() {
    let mut encoder = DataEncoder::new();
    let payload = encoder.encode(&dictionary_batch(None), 0).expect("encode");
    let meta_len = meta_len_of(&payload);

    // Collect (start, end, flatbuf_len) for every IPC piece after the
    // meta block.
    let mut pieces = Vec::new();
    let mut cursor = 4 + meta_len;
    while cursor < payload.len() {
        let (piece, next) =
            read_piece(&bytes::Bytes::from(payload.clone()), cursor).expect("piece");
        pieces.push((cursor, next, piece.flatbuf_len));
        cursor = next;
    }
    // [schema, dictionary..., record batch]
    assert!(
        pieces.len() >= 3,
        "expected schema, dictionary and batch pieces, got {}",
        pieces.len()
    );

    // Prime a decoder with the intact frame so the schema is cached.
    let mut primed = DataDecoder::new();
    primed
        .decode(bytes::Bytes::from(payload.clone()))
        .expect("prime the schema cache");

    // Corrupting the body of the dictionary piece (index 1) or the
    // record batch piece (last) must fail the decode instead of yielding
    // wrong data: Arrow validates offsets/lengths against the body.
    // Truncating the body of the dictionary piece (index 1) or the record
    // batch piece (last) past the IPC padding must fail the decode: the
    // declared IPC body no longer fits inside the piece. (Mere byte
    // corruption is NOT reliably detected — the IPC reader defers array
    // validation — so the length contract is the enforced invariant.)
    for index in [1, pieces.len() - 1] {
        let (target_start, _target_end, _flatbuf_len) = pieces[index];
        let mut rebuilt = Vec::new();
        let mut meta: WireDataMeta =
            serde_json::from_slice(&payload[4..4 + meta_len]).expect("meta");
        meta.has_schema = false;
        let meta_bytes = serde_json::to_vec(&meta).expect("meta encode");
        rebuilt.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
        rebuilt.extend_from_slice(&meta_bytes);
        // Skip the schema piece (index 0): the primed decoder already
        // cached it and a stray schema message is a different error.
        for &(start, end, _) in pieces.iter().skip(1) {
            let mut bytes = payload[start..end].to_vec();
            if start == target_start && bytes.len() > 16 {
                let smaller = (bytes.len() - 16) as u32;
                bytes.truncate(smaller as usize);
                bytes[0..4].copy_from_slice(&smaller.to_le_bytes());
            }
            rebuilt.extend_from_slice(&bytes);
        }
        let error = primed.decode(bytes::Bytes::from(rebuilt)).unwrap_err();
        assert!(
            error.to_string().contains("decode failed")
                || error.to_string().contains("IPC body exceeds IPC piece"),
            "piece {index}: {error}"
        );
    }
}

#[test]
fn read_piece_rejects_flatbuf_exceeding_the_piece_length() {
    // total = 12 but the flatbuf claims 100 bytes: structurally invalid.
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&12u32.to_le_bytes());
    bytes.extend_from_slice(&100u32.to_le_bytes());
    bytes.extend_from_slice(&[1, 2, 3, 4]);
    let error = match read_piece(&bytes::Bytes::from(bytes), 0) {
        Err(error) => error,
        Ok(_) => panic!("a flatbuf longer than its piece must be rejected"),
    };
    assert!(
        error.to_string().contains("flatbuf exceeds piece length"),
        "{error}"
    );
}

#[tokio::test]
async fn read_frame_reports_truncated_payloads_and_idle_timeouts() {
    let quad = quad_a_to_b();
    let (mut client, mut server) = tokio::io::duplex(64);
    // A header declaring 4 payload bytes, then the writer disappears.
    let mut header = Vec::new();
    FrameHeader {
        quad,
        len: 4,
        kind: FrameKind::Signal,
    }
    .encode_into(&mut header);
    client.write_all(&header).await.unwrap();
    client.shutdown().await.unwrap();
    let error = read_frame(&mut server).await.unwrap_err();
    assert!(
        error.to_string().contains("closed while reading payload"),
        "{error}"
    );

    // A silent peer under a tiny idle timeout times out on the header.
    let (_quiet_writer, quiet_reader) = tokio::io::duplex(8);
    let error = read_frame_with_limits(
        &mut { quiet_reader },
        MAX_FRAME_LEN,
        Some(std::time::Duration::from_millis(20)),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("read idle timeout"), "{error}");
}

#[tokio::test]
async fn write_frame_with_limit_rejects_oversized_payloads() {
    let (mut client, _server) = tokio::io::duplex(64);
    let error = write_frame_with_limit(
        &mut client,
        quad_a_to_b(),
        FrameKind::Signal,
        b"0123456789abcdef",
        8,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("exceeds the 8 limit"), "{error}");
}

#[test]
fn plaintext_transport_constructor_sets_fields() {
    let transport = TcpEdgeTransport::plaintext("127.0.0.1:9000".parse().unwrap(), 3);
    assert_eq!(transport.addr.port(), 9000);
    assert_eq!(transport.max_attempts, 3);
    assert!(transport.tls.is_none());
}

#[test]
fn tls_rejects_an_unparsable_fleet_ca() {
    let (nodes, _ca) = generate_fleet_material();
    let (cert, key) = &nodes[0];
    // A syntactically valid PEM block whose DER payload is garbage: the
    // CA store ends up empty and startup must fail explicitly.
    let junk_ca = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";
    let error = match DataPlaneTlsConfig::from_pem(cert, key, junk_ca) {
        Err(error) => error,
        Ok(_) => panic!("an unparsable fleet CA must be rejected"),
    };
    assert!(
        error.to_string().contains("no parsable certificates"),
        "{error}"
    );
}

#[test]
fn network_config_validation_rejects_invalid_limits() {
    let invalid = |config: NetworkManagerConfig, what: &str| {
        assert!(config.validate().is_err(), "{what} must be rejected");
    };
    invalid(
        NetworkManagerConfig {
            channel_capacity: 0,
            ..Default::default()
        },
        "a zero channel capacity",
    );
    invalid(
        NetworkManagerConfig {
            max_frame_len: 0,
            ..Default::default()
        },
        "a zero frame limit",
    );
    let error = NetworkManagerConfig {
        max_frame_len: MAX_FRAME_LEN + 1,
        ..Default::default()
    }
    .validate()
    .unwrap_err();
    assert!(error.to_string().contains("must not exceed"), "{error}");
    invalid(
        NetworkManagerConfig {
            read_idle_timeout: std::time::Duration::ZERO,
            ..Default::default()
        },
        "a zero read idle timeout",
    );
    invalid(
        NetworkManagerConfig {
            reconnect_grace: std::time::Duration::ZERO,
            ..Default::default()
        },
        "a zero reconnect grace",
    );
    invalid(
        NetworkManagerConfig {
            registration_grace: std::time::Duration::ZERO,
            ..Default::default()
        },
        "a zero registration grace",
    );
    invalid(
        NetworkManagerConfig {
            handshake_replay_ttl: std::time::Duration::ZERO,
            ..Default::default()
        },
        "a zero replay TTL",
    );
    let error = NetworkManagerConfig {
        credentials: Some(DataPlaneCredentials::new("  ", "s").unwrap()),
        ..Default::default()
    }
    .validate()
    .unwrap_err();
    assert!(
        error.to_string().contains("local_node must not be empty"),
        "{error}"
    );
    assert!(NetworkManagerConfig::default().validate().is_ok());
}

#[test]
fn trace_context_extraction_rejects_unparsable_values() {
    assert!(extract_trace_context("garbage").is_none());
    assert!(extract_trace_context("").is_none());
    let valid = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
    assert!(extract_trace_context(valid).is_some());
}

// ---------- pending receipt / ack lifecycle coverage ----------

/// An acknowledgement whose lifecycle calls always fail.
struct FailingLifecycleAck;

#[async_trait::async_trait]
impl crate::input::Ack for FailingLifecycleAck {
    async fn ack(&self) -> Result<(), Error> {
        Err(Error::Process("ack boom".into()))
    }
    async fn abort(&self) -> Result<(), Error> {
        Err(Error::Process("abort boom".into()))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn failing_branch_lifecycles_surface_as_failures() {
    let pending = PendingReceipts::new(64);
    let (failures_tx, failures_rx) = flume::unbounded::<Error>();

    let seq = pending
        .register(
            &(Arc::new(FailingLifecycleAck) as Arc<dyn crate::input::Ack>),
            1,
            None,
        )
        .expect("register");
    pending.apply(
        ReceiptFrame {
            kind: ReceiptKind::Acked,
            seq,
        },
        &failures_tx,
    );
    let failure = recv_within(&failures_rx, std::time::Duration::from_secs(5)).await;
    assert!(
        failure.contains("branch acknowledgement failed"),
        "{failure}"
    );

    let seq = pending
        .register(
            &(Arc::new(FailingLifecycleAck) as Arc<dyn crate::input::Ack>),
            1,
            None,
        )
        .expect("register");
    pending.apply(
        ReceiptFrame {
            kind: ReceiptKind::Failed,
            seq,
        },
        &failures_tx,
    );
    let failure = recv_within(&failures_rx, std::time::Duration::from_secs(5)).await;
    assert!(failure.contains("branch abort failed"), "{failure}");
}

async fn recv_within(receiver: &flume::Receiver<Error>, budget: std::time::Duration) -> String {
    tokio::time::timeout(budget, receiver.recv_async())
        .await
        .expect("failure within timeout")
        .expect("failure channel open")
        .to_string()
}

#[tokio::test]
async fn remote_ack_abort_on_a_dead_outbox_reports_failure() {
    let (outbox, _outbox_rx) = flume::unbounded::<(Quad, ReceiptFrame)>();
    drop(_outbox_rx);
    let (failures, _failure_rx) = flume::unbounded::<Error>();
    let ack = RemoteAck {
        outbox,
        quad: quad_a_to_b(),
        seq: 9,
        failures,
    };
    let error = ack.abort().await.unwrap_err();
    assert!(
        error.to_string().contains("receipt channel closed"),
        "{error}"
    );
}

// ---------- session receipt routing coverage ----------

#[tokio::test(flavor = "multi_thread")]
async fn session_receipt_forwarder_retries_until_a_live_writer_appears() {
    let manager = NetworkManager::new(8);
    let quad = quad_a_to_b();
    let key = EdgeSessionKey::for_job("route-job", 1, quad);

    // No writer installed yet: the forwarder parks and retries.
    let queue = manager.session_receipt_route(&key);
    queue
        .send_async((
            quad,
            ReceiptFrame {
                kind: ReceiptKind::Acked,
                seq: 0,
            },
        ))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // A dead writer slot is retried past as well.
    let (dead_tx, dead_rx) = flume::bounded::<(Quad, ReceiptFrame)>(1);
    drop(dead_rx);
    manager.set_session_receipt_writer(&key, Some(dead_tx));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // A live writer drains the queued receipt.
    let (live_tx, live_rx) = flume::bounded::<(Quad, ReceiptFrame)>(1);
    manager.set_session_receipt_writer(&key, Some(live_tx));
    let received = tokio::time::timeout(std::time::Duration::from_secs(5), live_rx.recv_async())
        .await
        .expect("receipt forwarded within timeout")
        .expect("writer channel open");
    assert_eq!(received.1.seq, 0);

    // Clearing an unknown key is a no-op; clearing routes cancels the
    // forwarder task.
    let (foreign_tx, _foreign_rx) = flume::bounded::<(Quad, ReceiptFrame)>(1);
    manager.clear_session_receipt_writer_if_owned(
        &EdgeSessionKey::for_job("other-job", 1, quad),
        &foreign_tx,
    );
    manager.remove_session_receipt_route(&key);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
}

#[test]
fn legacy_failure_receiver_is_process_wide() {
    let manager = NetworkManager::new(8);
    // Without credentials the per-Job receiver falls back to the shared
    // process-wide channel.
    let failures = manager.failure_receiver_for_job("legacy", 4);
    manager.report_failure(Error::Process("legacy failure".into()));
    let failure = failures
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert!(failure.to_string().contains("legacy failure"), "{failure}");
}

#[test]
fn tls_config_accessor_reflects_the_configuration() {
    assert!(NetworkManager::new(8).tls_config().is_none());
    let (nodes, ca) = generate_fleet_material();
    let (cert, key) = &nodes[0];
    let manager = NetworkManager::with_config(NetworkManagerConfig {
        credentials: Some(DataPlaneCredentials::new("node-a", "shuffle-secret").unwrap()),
        tls: Some(DataPlaneTlsConfig::from_pem(cert, key, &ca).unwrap()),
        ..Default::default()
    })
    .unwrap();
    assert!(manager.tls_config().is_some());
}

#[tokio::test]
async fn accept_stream_enforces_the_connection_limit() {
    let manager = NetworkManager::with_config(NetworkManagerConfig {
        max_connections: 1,
        ..Default::default()
    })
    .unwrap();
    let failures = manager.failure_receiver();
    let (_c1, s1) = tokio::io::duplex(64);
    manager.accept_stream(Box::new(s1));
    let (_c2, s2) = tokio::io::duplex(64);
    manager.accept_stream(Box::new(s2));
    let failure = failures
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert!(
        failure
            .to_string()
            .contains("accepted-connection limit 1 reached"),
        "{failure}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn eager_open_edge_connects_through_the_transport() {
    let downstream = NetworkManager::new(8);
    downstream.spawn();
    let quad = quad_a_to_b();
    let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
    downstream.register_inbound(quad, input_tx);
    let (client, server) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(server));
    let transport = QueuedTransport::of(vec![Box::new(client)]);
    let upstream = NetworkManager::new(8);
    upstream.spawn();
    let edge = upstream
        .open_edge(&*transport, quad)
        .await
        .expect("eager edge opens over the queued transport");
    edge.sender
        .send_async(Envelope::Eos)
        .await
        .expect("edge accepts envelopes");
    upstream.shutdown();
    downstream.shutdown();
}

#[tokio::test]
async fn authenticated_edges_require_credentials() {
    let manager = NetworkManager::new(8);
    let transport: Arc<dyn EdgeTransport> = QueuedTransport::of(vec![]);
    let error = match manager.open_edge_deferred_for_session(
        transport.clone(),
        quad_a_to_b(),
        "node-b".into(),
        "job".into(),
        1,
    ) {
        Err(error) => error,
        Ok(_) => panic!("an unauthenticated manager must refuse a session edge"),
    };
    assert!(
        error.to_string().contains("without data-plane credentials"),
        "{error}"
    );
    let error = match manager.open_edge_deferred_for_job(
        transport,
        quad_a_to_b(),
        "node-b".into(),
        "job".into(),
        1,
    ) {
        Err(error) => error,
        Ok(_) => panic!("an unauthenticated manager must refuse a job edge"),
    };
    assert!(
        error.to_string().contains("without data-plane credentials"),
        "{error}"
    );
}

/// A transport whose `connect` blocks until the test releases a gate, so
/// a deferred edge's attach can be observed after job-session teardown.
struct GatedTransport {
    gate: Arc<tokio::sync::Mutex<()>>,
    stream: tokio::sync::Mutex<Option<Box<dyn RemoteStream>>>,
}

#[async_trait::async_trait]
impl EdgeTransport for GatedTransport {
    async fn connect(&self, _quad: Quad) -> Result<Box<dyn RemoteStream>, Error> {
        let _guard = self.gate.lock().await;
        self.stream
            .lock()
            .await
            .take()
            .ok_or_else(|| Error::Process("gated transport exhausted".into()))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn late_attach_after_job_removal_aborts_without_recreating_state() {
    let downstream = authenticated_manager("node-b", "shuffle-secret");
    let quad = quad_a_to_b();
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    let _hold = gate.clone().lock_owned().await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let transport = Arc::new(GatedTransport {
        gate: gate.clone(),
        stream: tokio::sync::Mutex::new(Some(Box::new(client))),
    });
    let _server_kept = server;
    let edge = downstream
        .open_edge_deferred_for_session(
            transport.clone(),
            quad,
            "node-a".into(),
            "late-attach-job".into(),
            1,
        )
        .expect("deferred authenticated edge");
    // Create session-scoped routing state for the job, then tear the job
    // session down while the deferred connect is still parked.
    let key = EdgeSessionKey::for_job("late-attach-job", 1, quad);
    let _route = downstream.session_receipt_route(&key);
    let failures = downstream.failure_receiver_for_job("late-attach-job", 1);
    downstream.remove_job_session("late-attach-job", 1);
    assert!(job_session_state_is_cleared(&downstream));
    drop(_hold); // release the connect
                 // The edge fails closed: the sender errors once the aborted receiver
                 // is dropped by the attach path.
    tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
        loop {
            if edge.sender.send_async(Envelope::Eos).await.is_err() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("edge must fail closed after job removal");
    // No failure may land in the recreated per-Job channel: the Job is
    // gone and its state must not be resurrected.
    if let Ok(Ok(failure)) =
        tokio::time::timeout(std::time::Duration::from_secs(2), failures.recv_async()).await
    {
        panic!("unexpected late failure: {failure}");
    }
    downstream.shutdown();
}

fn job_session_state_is_cleared(manager: &NetworkManager) -> bool {
    manager.inbound.read().unwrap().is_empty()
        && manager.failure_channels.read().unwrap().is_empty()
}

// ---------- connection supervision coverage ----------

#[tokio::test(flavor = "multi_thread")]
async fn reconnect_replay_flush_failure_fails_the_edge_closed() {
    let quad = quad_a_to_b();
    let reconnect = NetworkManagerConfig {
        reconnect_attempts: 2,
        reconnect_grace: std::time::Duration::from_millis(200),
        ..Default::default()
    };
    let upstream = NetworkManager::with_config(reconnect.clone()).unwrap();
    let downstream = NetworkManager::with_config(reconnect).unwrap();
    upstream.spawn();
    downstream.spawn();
    let failures = upstream.failure_receiver();

    let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
    downstream.register_inbound(quad, input_tx);

    let (client1, server1) = tokio::io::duplex(64 * 1024);
    let kill1 = KillHandle::default();
    let kill_state = kill1.0.clone();
    downstream.accept_stream(Box::new(KillableStream {
        inner: Box::new(server1),
        state: kill_state,
    }));
    // The second connection dials into a stream whose peer is gone: the
    // replay flush fails and the edge fails closed.
    let (client2, server2) = tokio::io::duplex(64 * 1024);
    drop(server2);
    let transport = QueuedTransport::of(vec![Box::new(client1), Box::new(client2)]);
    let edge = upstream.open_edge_deferred(transport, quad);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let branch = Arc::new(RecordingAck::default());
    edge.sender
        .send_async(Envelope::Data(
            Arc::new(dictionary_batch(None)),
            branch.clone(),
        ))
        .await
        .unwrap();
    let _ = next_envelope(&input_rx).await;
    kill1.kill();

    tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
        while !branch.aborted.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the unreplayable branch must abort");
    let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
    assert!(
        failure.contains("replay flush failed") || failure.contains("reconnect"),
        "{failure}"
    );
    upstream.shutdown();
    downstream.shutdown();
}

/// A stream whose reads never complete and whose writes always fail:
/// isolates the pump's flush-tick failure exit.
struct DeadWriteStream;

impl AsyncRead for DeadWriteStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Pending
    }
}

impl AsyncWrite for DeadWriteStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "dead write",
        )))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "dead flush",
        )))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pump_flush_tick_failure_fails_the_edge() {
    let manager = NetworkManager::new(8);
    manager.spawn();
    let quad = quad_a_to_b();
    let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
    manager.register_inbound(quad, input_tx);
    // A plaintext (unauthenticated) edge: the pump has no handshake to
    // write, so the first failure comes from the flush tick.
    let edge = manager.open_edge_with_stream(Box::new(DeadWriteStream), quad);
    let failures = manager.failure_receiver();
    let _ = edge;
    let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
    assert!(failure.contains("flush failed"), "{failure}");
    manager.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_handshake_flush_failure_fails_the_edge() {
    let manager = authenticated_manager("node-a", "shuffle-secret");
    manager.spawn();
    let quad = quad_a_to_b();
    let (client, server) = tokio::io::duplex(64 * 1024);
    drop(server);
    let failures = manager.failure_receiver_for_job("flush-job", 1);
    let _edge = manager
        .open_edge_with_stream_for_session(Box::new(client), quad, "node-b", "flush-job", 1)
        .unwrap();
    let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
    assert!(failure.contains("handshake flush failed"), "{failure}");
    manager.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn pump_register_rejection_with_failing_abort_combines_errors() {
    let quad = quad_a_to_b();
    let config = NetworkManagerConfig {
        max_pending_receipts: 1,
        ..Default::default()
    };
    let pending = Arc::new(PendingReceipts::new(1));
    let shutdown = tokio_util::sync::CancellationToken::new();
    let (writer, _reader) = tokio::io::duplex(64 * 1024);
    let (tx, rx) = flume::bounded::<Envelope>(4);
    let first = Arc::new(RecordingAck::default());
    tx.send_async(Envelope::Data(
        Arc::new(dictionary_batch(None)),
        first.clone(),
    ))
    .await
    .unwrap();
    tx.send_async(Envelope::Data(
        Arc::new(dictionary_batch(None)),
        Arc::new(FailingLifecycleAck),
    ))
    .await
    .unwrap();
    drop(tx);
    let result = pump_edge(
        rx, writer, quad, pending, shutdown, config, None, false, None, false, false,
    )
    .await;
    let error = result.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("failed to abort batch rejected by pending receipt limit"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn receipt_channel_rejects_protocol_violations() {
    let quad = quad_a_to_b();
    let credentials = DataPlaneCredentials::new("node-a", "shuffle-secret").unwrap();
    let auth = credentials.session("node-b", "rcv-job", 1, quad);
    let config = NetworkManagerConfig {
        read_idle_timeout: std::time::Duration::from_secs(2),
        ..Default::default()
    };

    // (a) A receipt before the handshake acknowledgement.
    {
        let (client, mut server) = tokio::io::duplex(64 * 1024);
        let receipt = serde_json::to_vec(&ReceiptFrame {
            kind: ReceiptKind::Acked,
            seq: 0,
        })
        .unwrap();
        // Frames are written up front (the duplex buffers them) and the
        // peer half stays alive until the harness settles, so the pump's
        // own handshake write cannot race a dropped peer.
        write_frame(&mut server, quad, FrameKind::Receipt, &receipt)
            .await
            .unwrap();
        let (_pump, read) =
            run_receipt_harness(Box::new(client), quad, Some(auth.clone()), config.clone()).await;
        drop(server);
        let error = read.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("arrived before handshake acknowledgement"),
            "{error}"
        );
    }

    // (b) A malformed receipt after a valid acknowledgement.
    {
        let (client, mut server) = tokio::io::duplex(64 * 1024);
        let (ack_payload, _c2s, mut s2c) = fabricate_ack_and_derive(&auth);
        write_frame(&mut server, quad, FrameKind::Handshake, &ack_payload)
            .await
            .unwrap();
        write_frame_sealed(
            &mut server,
            quad,
            FrameKind::Receipt,
            b"not json",
            MAX_FRAME_LEN,
            &mut s2c,
        )
        .await
        .unwrap();
        let (_pump, read) =
            run_receipt_harness(Box::new(client), quad, Some(auth.clone()), config.clone()).await;
        drop(server);
        let error = read.unwrap_err();
        assert!(
            error.to_string().contains("receipt frame malformed"),
            "{error}"
        );
    }

    // (c) A data frame on the receipt channel.
    {
        let (client, mut server) = tokio::io::duplex(64 * 1024);
        let (ack_payload, _c2s, mut s2c) = fabricate_ack_and_derive(&auth);
        write_frame(&mut server, quad, FrameKind::Handshake, &ack_payload)
            .await
            .unwrap();
        write_frame_sealed(
            &mut server,
            quad,
            FrameKind::Data,
            b"stray",
            MAX_FRAME_LEN,
            &mut s2c,
        )
        .await
        .unwrap();
        let (_pump, read) = run_receipt_harness(Box::new(client), quad, Some(auth), config).await;
        drop(server);
        let error = read.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unexpected Data frame on a receipt channel"),
            "{error}"
        );
    }
}

/// Drive `run_edge_connection` over a drained envelope channel and return
/// its (pump, read) results.
async fn run_receipt_harness(
    client: Box<dyn RemoteStream>,
    quad: Quad,
    auth: Option<SessionAuth>,
    config: NetworkManagerConfig,
) -> (Result<(), Error>, Result<(), Error>) {
    let (tx, rx) = flume::bounded::<Envelope>(1);
    drop(tx);
    let pending = Arc::new(PendingReceipts::new(64));
    let (failures_tx, _failures_rx) = flume::unbounded::<Error>();
    let shutdown = tokio_util::sync::CancellationToken::new();
    tokio::time::timeout(
        TEST_PROPAGATION_BUDGET,
        run_edge_connection(
            client,
            quad,
            rx,
            pending,
            config,
            auth,
            false,
            failures_tx,
            &shutdown,
            // The fixture builds the ack from the nonce of the handshake it
            // wrote itself (`auth.client_handshake()` in the test body).
            // Suppress the pump's own handshake so it cannot overwrite that
            // recorded nonce before the read half verifies the ack.
            true,
            false,
        ),
    )
    .await
    .expect("connection harness settles within timeout")
}

// ---------- frame authentication (protocol v2) ----------

#[test]
fn session_keys_are_direction_and_session_isolated() {
    // Keys are not directly inspectable; compare them through the tags they
    // produce for the same (header, payload).
    let tag_of = |crypto: &mut FrameCrypto| {
        let header = FrameHeader {
            quad: quad_a_to_b(),
            len: 4,
            kind: FrameKind::Signal,
        };
        let mut tag = vec![0u8; 16];
        crypto.seal(&header, b"key!", &mut tag);
        tag
    };
    let quad = quad_a_to_b();
    let credentials = DataPlaneCredentials::new("node-a", "shuffle-secret").unwrap();
    let auth = credentials.session("node-b", "key-job", 1, quad);
    let (_ack_a, mut c2s_a, mut s2c_a) = fabricate_ack_and_derive(&auth);
    let (_ack_b, mut c2s_b, mut s2c_b) = fabricate_ack_and_derive(&auth);
    // Fresh nonces every handshake: no two sessions share a direction key.
    assert_ne!(tag_of(&mut c2s_a), tag_of(&mut c2s_b));
    assert_ne!(tag_of(&mut s2c_a), tag_of(&mut s2c_b));
    // The two directions within ONE session are independent keys.
    assert_ne!(tag_of(&mut c2s_a), tag_of(&mut s2c_a));
}

#[test]
fn frame_tag_binds_header_payload_and_sequence() {
    let quad = quad_a_to_b();
    let credentials = DataPlaneCredentials::new("node-a", "shuffle-secret").unwrap();
    let auth = credentials.session("node-b", "tag-job", 1, quad);
    // One handshake, two derivations: sender and receiver of the SAME
    // direction share the key; the opposite direction is an independent key.
    let request = auth.client_handshake().unwrap();
    let ack = auth.server_ack(&request, "node-b");
    let (mut c2s_send, mut s2c) = auth.frame_crypto_client(&ack).unwrap();
    let (mut c2s_recv, _) = auth.frame_crypto_client(&ack).unwrap();
    let payload = b"frame tag payload".as_slice();
    let header = FrameHeader {
        quad,
        len: payload.len() as u32,
        kind: FrameKind::Signal,
    };

    // Roundtrip on one direction: seq 0 then seq 1, in order.
    let mut tag0 = vec![0u8; 16];
    c2s_send.seal(&header, payload, &mut tag0);
    c2s_recv
        .open(&header, payload, &tag0)
        .expect("first frame verifies");
    let mut tag1 = vec![0u8; 16];
    c2s_send.seal(&header, payload, &mut tag1);
    assert_ne!(tag0, tag1, "the sequence number must feed the tag");
    c2s_recv
        .open(&header, payload, &tag1)
        .expect("second frame verifies");

    // Out-of-order frames are rejected: the receiver expects seq 2 next.
    let error = c2s_recv.open(&header, payload, &tag0).unwrap_err();
    assert!(
        error.to_string().contains("frame authentication failed"),
        "{error}"
    );

    // The opposite direction's key cannot verify this direction's frames:
    // tag0 keeps the expected sequence identical, so only the direction
    // differs.
    assert!(s2c.open(&header, payload, &tag0).is_err());

    // Tampering with any covered byte fails closed. Each mutation gets a
    // FRESH verifier (so the expected sequence matches the tag's), and the
    // unmodified frame verifies first — the rejection then demonstrates
    // authentication of the changed field, not a sequence mismatch.
    let mut tampered_header = header;
    tampered_header.len += 1;
    let (mut header_verifier, _) = auth.frame_crypto_client(&ack).unwrap();
    header_verifier
        .open(&header, payload, &tag0)
        .expect("unmodified frame verifies");
    assert!(header_verifier
        .open(&tampered_header, payload, &tag0)
        .is_err());
    let tampered_payload = b"frame tag payloaD".as_slice();
    let (mut payload_verifier, _) = auth.frame_crypto_client(&ack).unwrap();
    payload_verifier
        .open(&header, payload, &tag0)
        .expect("unmodified frame verifies");
    assert!(payload_verifier
        .open(&header, tampered_payload, &tag0)
        .is_err());
    let mut wrong_length_tag = tag0.clone();
    wrong_length_tag.pop();
    let (mut length_verifier, _) = auth.frame_crypto_client(&ack).unwrap();
    assert!(length_verifier
        .open(&header, payload, &wrong_length_tag)
        .is_err());
}

#[test]
fn a_frame_sealed_under_one_session_fails_under_another() {
    let quad = quad_a_to_b();
    let credentials = DataPlaneCredentials::new("node-a", "shuffle-secret").unwrap();
    let auth = credentials.session("node-b", "replay-job", 1, quad);
    let (_ack_a, mut c2s_session_a, _) = fabricate_ack_and_derive(&auth);
    let (_ack_b, mut c2s_session_b, _) = fabricate_ack_and_derive(&auth);
    let payload = b"captured frame".as_slice();
    let header = FrameHeader {
        quad,
        len: payload.len() as u32,
        kind: FrameKind::Data,
    };
    let mut captured_tag = vec![0u8; 16];
    c2s_session_a.seal(&header, payload, &mut captured_tag);
    // A new handshake produced new nonces and therefore new keys: the
    // captured frame cannot be replayed onto the fresh session. The
    // verifier is the same direction (c2s), so only the session differs.
    let error = c2s_session_b
        .open(&header, payload, &captured_tag)
        .unwrap_err();
    assert!(
        error.to_string().contains("frame authentication failed"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_forged_frame_from_wrong_credentials_is_rejected_end_to_end() {
    let quad = quad_a_to_b();
    let downstream = authenticated_manager("node-b", "the-real-secret");
    downstream.spawn();
    let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
    downstream
        .register_inbound_for_session(
            quad,
            input_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "forge-job".into(),
                generation: 1,
            },
        )
        .unwrap();

    let (mut client, server) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(server));
    // The attacker holds a DIFFERENT secret: its handshake MAC fails at the
    // gate, the connection is torn down, and no attacker-controlled byte
    // ever reaches the registered pipeline (the failure surfaces on the
    // manager's unauthenticated queue, not the Job channel — the session
    // was never established).
    let attacker_credentials = DataPlaneCredentials::new("node-a", "attacker-secret").unwrap();
    let attacker_auth = attacker_credentials.session("node-b", "forge-job", 1, quad);
    let request = serde_json::to_vec(&attacker_auth.client_handshake().unwrap()).unwrap();
    write_frame(&mut client, quad, FrameKind::Handshake, &request)
        .await
        .unwrap();
    // The server answers by closing the connection: the attacker's next
    // read hits EOF instead of an acknowledgement.
    let mut probe = [0u8; 1];
    let eof = tokio::time::timeout(
        TEST_PROPAGATION_BUDGET,
        tokio::io::AsyncReadExt::read(&mut client, &mut probe),
    )
    .await;
    match eof {
        Ok(Ok(0)) => {}
        Ok(Ok(_)) => panic!("the server must not acknowledge a failed handshake"),
        Ok(Err(error)) => panic!("unexpected read error: {error}"),
        Err(_) => panic!("the server must close a connection whose handshake MAC failed"),
    }
    let leaked =
        tokio::time::timeout(std::time::Duration::from_millis(300), input_rx.recv_async()).await;
    assert!(
        leaked.is_err(),
        "no attacker-controlled envelope may reach the pipeline"
    );
    downstream.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_frame_captured_from_another_session_is_rejected_end_to_end() {
    let quad = quad_a_to_b();
    let downstream = authenticated_manager("node-b", "shuffle-secret");
    downstream.spawn();
    let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
    downstream
        .register_inbound_for_session(
            quad,
            input_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "xsession-job".into(),
                generation: 1,
            },
        )
        .unwrap();
    let failures = downstream.failure_receiver_for_job("xsession-job", 1);

    let (mut client, server) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(server));
    let auth = DataPlaneCredentials::new("node-a", "shuffle-secret")
        .unwrap()
        .session("node-b", "xsession-job", 1, quad);
    let (_c2s_real, _s2c_real) = handshake_and_derive(&mut client, &auth, quad).await;
    // A frame sealed under a DIFFERENT handshake's keys (attacker replays a
    // capture from an earlier session) must fail the fresh session's check.
    let stale_credentials = DataPlaneCredentials::new("node-a", "shuffle-secret").unwrap();
    let stale_auth = stale_credentials.session("node-b", "xsession-job", 1, quad);
    let (_stale_ack, mut stale_c2s, _) = fabricate_ack_and_derive(&stale_auth);
    let payload = b"captured batch".as_slice();
    write_frame_sealed(
        &mut client,
        quad,
        FrameKind::Data,
        payload,
        MAX_FRAME_LEN,
        &mut stale_c2s,
    )
    .await
    .unwrap();
    let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
    assert!(failure.contains("frame authentication failed"), "{failure}");
    downstream.shutdown();
}

// ---------- inbound serve-loop error coverage ----------

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_handshake_and_quad_mismatch_fail_closed() {
    let quad = quad_a_to_b();
    let downstream = authenticated_manager("node-b", "shuffle-secret");
    downstream.spawn();
    let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
    downstream
        .register_inbound_for_session(
            quad,
            input_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "hs-job".into(),
                generation: 1,
            },
        )
        .unwrap();
    let failures = downstream.failure_receiver_for_job("hs-job", 1);

    let credentials = DataPlaneCredentials::new("node-a", "shuffle-secret").unwrap();
    let auth = credentials.session("node-b", "hs-job", 1, quad);

    // A second handshake frame on an authenticated connection.
    let (mut client1, server1) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(server1));
    let (mut c2s1, _s2c1) = handshake_and_derive(&mut client1, &auth, quad).await;
    write_frame_sealed(
        &mut client1,
        quad,
        FrameKind::Handshake,
        // Payload content is irrelevant: the duplicate KIND is the violation.
        b"{}",
        MAX_FRAME_LEN,
        &mut c2s1,
    )
    .await
    .unwrap();
    let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
    assert!(
        failure
            .to_string()
            .contains("duplicate remote edge handshake"),
        "{failure}"
    );

    // A data frame for a different quad than the authenticated one.
    let wrong_quad = Quad {
        dst_subtask: quad.dst_subtask + 1,
        ..quad
    };
    let (mut client2, server2) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(server2));
    // A fresh handshake: the server's nonce cache rejects replayed ones.
    let (mut c2s2, _s2c2) = handshake_and_derive(&mut client2, &auth, quad).await;
    write_frame_sealed(
        &mut client2,
        wrong_quad,
        FrameKind::Signal,
        serde_json::to_vec(&WireSignal::Eos).unwrap().as_slice(),
        MAX_FRAME_LEN,
        &mut c2s2,
    )
    .await
    .unwrap();
    let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
    assert!(
        failure
            .to_string()
            .contains("does not match authenticated quad"),
        "{failure}"
    );
    downstream.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn inbound_protocol_violations_fail_the_connection() {
    let quad = quad_a_to_b();
    let config = NetworkManagerConfig {
        registration_grace: std::time::Duration::from_millis(100),
        reconnect_attempts: 0,
        ..Default::default()
    };
    let manager = NetworkManager::with_config(config).unwrap();
    manager.spawn();
    let failures = manager.failure_receiver();

    // An unregistered quad: the registration grace expires and the frame
    // is rejected.
    let (mut client1, server1) = tokio::io::duplex(64 * 1024);
    manager.accept_stream(Box::new(server1));
    write_frame(&mut client1, quad, FrameKind::Signal, b"{}")
        .await
        .unwrap();
    let failure = recv_within(&failures, std::time::Duration::from_secs(5)).await;
    assert!(
        failure
            .to_string()
            .contains("inbound frame for unregistered quad"),
        "{failure}"
    );

    // Registered quads, garbage payloads: data and signal frames are
    // rejected as decode failures, receipts are rejected outright. Each
    // sub-case uses a fresh quad so a prior connection's cleanup cannot
    // deregister the route mid-test.
    let quad_b = Quad {
        dst_op: quad.dst_op + 1,
        ..quad
    };
    let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
    manager.register_inbound(quad_b, input_tx);

    let (mut client2, server2) = tokio::io::duplex(64 * 1024);
    manager.accept_stream(Box::new(server2));
    write_frame(&mut client2, quad_b, FrameKind::Data, b"garbage")
        .await
        .unwrap();
    let failure = recv_within(&failures, std::time::Duration::from_secs(5)).await;
    assert!(
        failure
            .to_string()
            .contains("inbound data frame decode failed"),
        "{failure}"
    );

    let quad_c = Quad {
        dst_op: quad.dst_op + 2,
        ..quad
    };
    let (input_tx3, _input_rx3) = flume::bounded::<Envelope>(8);
    manager.register_inbound(quad_c, input_tx3);
    let (mut client3, server3) = tokio::io::duplex(64 * 1024);
    manager.accept_stream(Box::new(server3));
    write_frame(&mut client3, quad_c, FrameKind::Signal, b"garbage")
        .await
        .unwrap();
    let failure = recv_within(&failures, std::time::Duration::from_secs(5)).await;
    assert!(
        failure
            .to_string()
            .contains("inbound signal frame malformed"),
        "{failure}"
    );

    let quad_d = Quad {
        dst_op: quad.dst_op + 3,
        ..quad
    };
    let (input_tx4, _input_rx4) = flume::bounded::<Envelope>(8);
    manager.register_inbound(quad_d, input_tx4);
    let (mut client4, server4) = tokio::io::duplex(64 * 1024);
    manager.accept_stream(Box::new(server4));
    write_frame(
        &mut client4,
        quad_d,
        FrameKind::Receipt,
        serde_json::to_vec(&ReceiptFrame {
            kind: ReceiptKind::Acked,
            seq: 0,
        })
        .unwrap()
        .as_slice(),
    )
    .await
    .unwrap();
    let failure = recv_within(&failures, std::time::Duration::from_secs(5)).await;
    assert!(
        failure
            .to_string()
            .contains("receipt frame on an inbound (downstream) connection"),
        "{failure}"
    );
    drop(input_rx);
    manager.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn local_chain_death_breaks_the_edge_and_reports_midstream_loss() {
    let quad = quad_a_to_b();
    let manager = NetworkManager::with_config(NetworkManagerConfig {
        reconnect_attempts: 0,
        ..Default::default()
    })
    .unwrap();
    manager.spawn();
    let failures = manager.failure_receiver();

    // The local chain vanishes: the serve loop's channel send fails and
    // the connection ends reporting an upstream death without Eos.
    let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
    manager.register_inbound(quad, input_tx);
    drop(input_rx);
    let mut encoder = DataEncoder::new();
    let data_payload = encoder.encode(&dictionary_batch(None), 0).unwrap();
    let (mut client, server) = tokio::io::duplex(64 * 1024);
    manager.accept_stream(Box::new(server));
    write_frame(&mut client, quad, FrameKind::Data, &data_payload)
        .await
        .unwrap();
    let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
    assert!(
        failure.to_string().contains("ended without Eos"),
        "{failure}"
    );

    // A non-Eos signal onto a dead local chain breaks cleanly too (an
    // Eos would mark the quad complete and report nothing).
    let (input_tx2, input_rx2) = flume::bounded::<Envelope>(8);
    manager.register_inbound(quad, input_tx2);
    drop(input_rx2);
    let (mut client2, server2) = tokio::io::duplex(64 * 1024);
    manager.accept_stream(Box::new(server2));
    write_frame(
        &mut client2,
        quad,
        FrameKind::Signal,
        serde_json::to_vec(&WireSignal::Watermark(1))
            .unwrap()
            .as_slice(),
    )
    .await
    .unwrap();
    let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
    assert!(
        failure.to_string().contains("ended without Eos"),
        "{failure}"
    );
    manager.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn deferred_loss_watcher_stops_at_shutdown_without_reporting() {
    let quad = quad_a_to_b();
    let manager = NetworkManager::with_config(NetworkManagerConfig {
        reconnect_grace: std::time::Duration::from_millis(300),
        ..Default::default()
    })
    .unwrap();
    manager.spawn();
    let failures = manager.failure_receiver();
    let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
    manager.register_inbound(quad, input_tx);
    let (mut client, server) = tokio::io::duplex(64 * 1024);
    manager.accept_stream(Box::new(server));
    write_frame(
        &mut client,
        quad,
        FrameKind::Signal,
        serde_json::to_vec(&WireSignal::Eos).unwrap().as_slice(),
    )
    .await
    .unwrap();
    client.shutdown().await.unwrap();
    // The peer closed without Eos reaching any served quad? Eos was
    // forwarded, so this close is clean; instead simulate a mid-stream
    // death by registering a second quad that never sees Eos.
    let quad2 = Quad {
        dst_op: quad.dst_op + 1,
        ..quad
    };
    let (input_tx2, _input_rx2) = flume::bounded::<Envelope>(8);
    manager.register_inbound(quad2, input_tx2);
    let (mut client2, server2) = tokio::io::duplex(64 * 1024);
    manager.accept_stream(Box::new(server2));
    let mut encoder = DataEncoder::new();
    let data_payload = encoder.encode(&dictionary_batch(None), 0).unwrap();
    write_frame(&mut client2, quad2, FrameKind::Data, &data_payload)
        .await
        .unwrap();
    drop(client2);
    // Shut the manager down inside the grace window: the deferred loss
    // watcher must not report a failure afterwards.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    manager.shutdown();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(600), failures.recv_async())
            .await
            .is_err(),
        "shutdown must suppress the deferred loss report"
    );
}

#[tokio::test]
async fn authorize_inbound_rejects_identity_drift() {
    let quad = quad_a_to_b();
    let downstream = authenticated_manager("node-b", "shuffle-secret");
    let credentials = DataPlaneCredentials::new("node-a", "shuffle-secret").unwrap();
    let auth = credentials.session("node-b", "id-job", 1, quad);
    let mut request = auth.client_handshake().unwrap();
    // A protocol-version drift is rejected by the identity check before
    // any MAC or registry work.
    request.protocol_version = DATA_PLANE_PROTOCOL_VERSION + 1;
    let payload = serde_json::to_vec(&request).unwrap();
    let header = FrameHeader {
        quad,
        len: payload.len() as u32,
        kind: FrameKind::Handshake,
    };
    let error = match downstream.authorize_inbound(header, &payload).await {
        Err(error) => error,
        Ok(_) => panic!("a drifted handshake identity must be rejected"),
    };
    assert!(
        error.to_string().contains("protocol or identity mismatch"),
        "{error}"
    );
}

/// Authenticated transparent reconnect RECOVERS: the reconnect path writes
/// the session handshake EXACTLY ONCE on the new wire (by `replay_pending`)
/// and `pump_edge` must not duplicate it — the previous double handshake
/// made the peer's duplicate-handshake guard tear every reconnection down,
/// exhausting the redial budget and failing the edge closed (branches
/// aborted, failure surfaced). This test pins the fixed recovery.
#[tokio::test(flavor = "multi_thread")]
async fn authenticated_reconnect_recovers_without_handshake_duplication() {
    let quad = quad_a_to_b();
    let node_config = |node: &str| NetworkManagerConfig {
        credentials: Some(DataPlaneCredentials::new(node, "shuffle-secret").unwrap()),
        registration_grace: std::time::Duration::from_millis(100),
        reconnect_grace: std::time::Duration::from_millis(800),
        reconnect_attempts: 2,
        ..Default::default()
    };
    let upstream = NetworkManager::with_config(node_config("node-a")).unwrap();
    let downstream = NetworkManager::with_config(node_config("node-b")).unwrap();
    upstream.spawn();
    downstream.spawn();
    let failures = upstream.failure_receiver_for_job("auth-rc-job", 2);

    let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
    downstream
        .register_inbound_for_session(
            quad,
            input_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "auth-rc-job".into(),
                generation: 2,
            },
        )
        .unwrap();

    let (client1, server1) = tokio::io::duplex(64 * 1024);
    let (client2, server2) = tokio::io::duplex(64 * 1024);
    let kill1 = KillHandle::default();
    let kill_state = kill1.0.clone();
    downstream.accept_stream(Box::new(KillableStream {
        inner: Box::new(server1),
        state: kill_state,
    }));
    downstream.accept_stream(Box::new(server2));
    let transport = QueuedTransport::of(vec![Box::new(client1), Box::new(client2)]);
    let edge = upstream
        .open_edge_deferred_for_session(transport, quad, "node-b".into(), "auth-rc-job".into(), 2)
        .expect("deferred authenticated edge");
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let branch = Arc::new(RecordingAck::default());
    edge.sender
        .send_async(Envelope::Data(
            Arc::new(dictionary_batch(Some("auth-rc"))),
            branch.clone(),
        ))
        .await
        .unwrap();
    let delivered = next_envelope(&input_rx).await;
    let Envelope::Data(_batch, _ack) = delivered else {
        panic!("expected a data envelope");
    };
    kill1.kill();

    // The edge transparently reconnects on the second transport entry and
    // post-reconnect data flows: the replayed frame for the first envelope
    // is deduplicated downstream, so the NEXT envelope is what arrives.
    edge.sender
        .send_async(Envelope::Data(
            Arc::new(dictionary_batch(Some("auth-rc-2"))),
            branch.clone(),
        ))
        .await
        .unwrap();
    let delivered = tokio::time::timeout(TEST_PROPAGATION_BUDGET, next_envelope(&input_rx))
        .await
        .expect("the reconnected edge must deliver data");
    let Envelope::Data(_batch, _ack) = delivered else {
        panic!("expected a data envelope after reconnect");
    };
    // A recovered edge must never abort its branches or surface a failure.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        !branch.aborted.load(std::sync::atomic::Ordering::SeqCst),
        "a recovered edge must not abort its branches"
    );
    assert!(
        failures.try_recv().is_err(),
        "a recovered edge must not surface a failure"
    );
    upstream.shutdown();
    downstream.shutdown();
}

// ---------- remaining supervision / registry coverage ----------

/// A stream whose writes fail (reads keep working) after `kill` is set,
/// isolating the receipt writer's write-failure exit without tearing the
/// read half down at the same time.
struct WriteKillStream {
    inner: Box<dyn RemoteStream>,
    killed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl AsyncRead for WriteKillStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for WriteKillStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if self.killed.load(std::sync::atomic::Ordering::Acquire) {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "write killed",
            )));
        }
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// A stream whose writes always fail while flushes succeed, so the pump's
/// periodic flush tick cannot mask the per-frame write failure.
struct WriteFailStream;

impl AsyncRead for WriteFailStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Pending
    }
}

impl AsyncWrite for WriteFailStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "write failed",
        )))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn tcp_transport_reports_failure_after_the_attempt_budget() {
    // Bind, note the port, and drop the listener: the port is (in
    // practice) unbound, so both attempts are refused.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let transport = TcpEdgeTransport::plaintext(format!("127.0.0.1:{port}").parse().unwrap(), 2);
    let error = match transport.connect(quad_a_to_b()).await {
        Err(error) => error,
        Ok(_) => panic!("an unbound peer must exhaust the attempt budget"),
    };
    assert!(
        error.to_string().contains("failed after 2 attempts"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn receipt_routing_settles_partial_replicas_and_ignores_strays() {
    let pending = PendingReceipts::new(64);
    let (failures_tx, _failures_rx) = flume::unbounded::<Error>();
    let first = Arc::new(RecordingAck::default());
    // Two replicas: the first Acked receipt must not complete the branch.
    let first_branch: Arc<dyn crate::input::Ack> = first.clone();
    let seq = pending.register(&first_branch, 2, None).unwrap();
    pending.apply(
        ReceiptFrame {
            kind: ReceiptKind::Acked,
            seq,
        },
        &failures_tx,
    );
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !first.acked.load(std::sync::atomic::Ordering::SeqCst),
        "one of two replicas must not complete the branch"
    );
    pending.apply(
        ReceiptFrame {
            kind: ReceiptKind::Acked,
            seq,
        },
        &failures_tx,
    );
    tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
        while !first.acked.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the second replica completes the branch");
    // A duplicate receipt for the completed sequence is dropped.
    pending.apply(
        ReceiptFrame {
            kind: ReceiptKind::Acked,
            seq,
        },
        &failures_tx,
    );

    // Double Held re-asserts only on the 0→1 transition; the release
    // only forwards once every held replica reported back.
    let second = Arc::new(RecordingAck::default());
    let second_branch: Arc<dyn crate::input::Ack> = second.clone();
    let seq = pending.register(&second_branch, 1, None).unwrap();
    for _ in 0..2 {
        pending.apply(
            ReceiptFrame {
                kind: ReceiptKind::Held,
                seq,
            },
            &failures_tx,
        );
    }
    assert!(second.held.load(std::sync::atomic::Ordering::SeqCst));
    pending.apply(
        ReceiptFrame {
            kind: ReceiptKind::Released,
            seq,
        },
        &failures_tx,
    );
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !second.released.load(std::sync::atomic::Ordering::SeqCst),
        "one of two held replicas must not release the branch"
    );
    pending.apply(
        ReceiptFrame {
            kind: ReceiptKind::Released,
            seq,
        },
        &failures_tx,
    );
    tokio::time::timeout(TEST_PROPAGATION_BUDGET, async {
        while !second.released.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the last held replica releases the branch");
}

#[tokio::test(flavor = "multi_thread")]
async fn receipt_forwarder_stops_on_manager_shutdown() {
    let manager = NetworkManager::new(8);
    let key = EdgeSessionKey::for_job("fwd-shutdown-job", 1, quad_a_to_b());
    // No writer installed: the forwarder parks in its retry loop, then
    // the manager's shutdown must retire it.
    let queue = manager.session_receipt_route(&key);
    queue
        .send_async((
            quad_a_to_b(),
            ReceiptFrame {
                kind: ReceiptKind::Acked,
                seq: 0,
            },
        ))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    manager.shutdown();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
}

fn raw_handshake(node: &str, job: &str, generation: u64, quad: Quad) -> (FrameHeader, Vec<u8>) {
    let credentials = DataPlaneCredentials::new(node, "shuffle-secret").expect("test credentials");
    let auth = credentials.session("node-b", job, generation, quad);
    let payload = serde_json::to_vec(&auth.client_handshake().unwrap()).unwrap();
    let header = FrameHeader {
        quad,
        len: payload.len() as u32,
        kind: FrameKind::Handshake,
    };
    (header, payload)
}

/// Completes the client side of an authenticated handshake against a live
/// manager socket and returns both direction cryptos derived from the
/// manager's REAL acknowledgement: `(c2s, s2c)` — the socket under test
/// sends with c2s and receives with s2c.
async fn handshake_and_derive(
    client: &mut (impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin),
    auth: &SessionAuth,
    quad: Quad,
) -> (FrameCrypto, FrameCrypto) {
    let request = serde_json::to_vec(&auth.client_handshake().unwrap()).unwrap();
    write_frame(client, quad, FrameKind::Handshake, &request)
        .await
        .unwrap();
    let (_ack_header, ack_bytes) = read_frame(client).await.unwrap();
    let ack: HandshakePayload = serde_json::from_slice(&ack_bytes).unwrap();
    auth.frame_crypto_client(&ack).unwrap()
}

/// Fabricates a server-side acknowledgement for THIS auth's handshake and
/// derives both direction cryptos from it — for tests that simulate the
/// serving peer over a duplex. The client reader derives its keys from the
/// same fabricated ack, so both ends agree; returns `(ack_payload, c2s, s2c)`.
fn fabricate_ack_and_derive(auth: &SessionAuth) -> (Vec<u8>, FrameCrypto, FrameCrypto) {
    let request = auth.client_handshake().unwrap();
    let ack = auth.server_ack(&request, "node-b");
    let payload = serde_json::to_vec(&ack).unwrap();
    let (c2s, s2c) = auth.frame_crypto_client(&ack).unwrap();
    (payload, c2s, s2c)
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_protocol_violation_reports_through_the_job_channel() {
    let quad = quad_a_to_b();
    let upstream = authenticated_manager("node-a", "shuffle-secret");
    let downstream = authenticated_manager("node-b", "shuffle-secret");
    upstream.spawn();
    downstream.spawn();
    let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
    downstream
        .register_inbound_for_session(
            quad,
            input_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "job-pv".into(),
                generation: 1,
            },
        )
        .unwrap();
    let failures = downstream.failure_receiver_for_job("job-pv", 1);
    let (mut client, server) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(server));
    let auth = DataPlaneCredentials::new("node-a", "shuffle-secret")
        .unwrap()
        .session("node-b", "job-pv", 1, quad);
    let (mut c2s, _s2c) = handshake_and_derive(&mut client, &auth, quad).await;
    // A malformed signal frame after authentication is a connection
    // failure routed through the Job's registered failure channel.
    write_frame_sealed(
        &mut client,
        quad,
        FrameKind::Signal,
        b"garbage",
        MAX_FRAME_LEN,
        &mut c2s,
    )
    .await
    .unwrap();
    let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
    assert!(
        failure.contains("inbound signal frame malformed"),
        "{failure}"
    );
    upstream.shutdown();
    downstream.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn handshake_write_failure_releases_the_inbound_registration() {
    let quad = quad_a_to_b();
    let downstream = authenticated_manager("node-b", "shuffle-secret");
    downstream.spawn();
    let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
    downstream
        .register_inbound_for_session(
            quad,
            input_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "job-hw".into(),
                generation: 1,
            },
        )
        .unwrap();
    let failures = downstream.failure_receiver_for_job("job-hw", 1);
    let (mut client, server) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(server));
    let (_header, payload) = raw_handshake("node-a", "job-hw", 1, quad);
    write_frame(&mut client, quad, FrameKind::Handshake, &payload)
        .await
        .unwrap();
    // The peer disappears before the acknowledgement can be written.
    drop(client);
    let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
    assert!(
        failure.contains("remote edge write failed") || failure.contains("handshake flush"),
        "{failure}"
    );
    // The session's registration is released with the failed handshake.
    assert!(downstream.inbound.read().unwrap().is_empty());
    assert!(downstream.inbound_auth.read().unwrap().is_empty());
    downstream.shutdown();
}

#[tokio::test]
async fn handshake_nonce_cache_overflow_rejects_new_handshakes() {
    let quad = quad_a_to_b();
    let config = NetworkManagerConfig {
        credentials: Some(
            DataPlaneCredentials::new("node-b", "shuffle-secret").expect("credentials"),
        ),
        max_connections: 1,
        registration_grace: std::time::Duration::from_millis(100),
        ..Default::default()
    };
    let downstream = NetworkManager::with_config(config).expect("valid config");
    let (input_tx, _input_rx) = flume::bounded::<Envelope>(8);
    downstream
        .register_inbound_for_session(
            quad,
            input_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "job-nl".into(),
                generation: 1,
            },
        )
        .unwrap();
    // Capacity is max_connections * 4: four distinct nonces fill it.
    for _ in 0..4 {
        let (header, payload) = raw_handshake("node-a", "job-nl", 1, quad);
        downstream
            .authorize_inbound(header, &payload)
            .await
            .expect("distinct nonces are accepted");
    }
    let (header, payload) = raw_handshake("node-a", "job-nl", 1, quad);
    let error = match downstream.authorize_inbound(header, &payload).await {
        Err(error) => error,
        Ok(_) => panic!("the replay cache is bounded"),
    };
    assert!(
        error.to_string().contains("replay cache is full"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn late_connection_error_after_job_removal_is_swallowed() {
    let quad = quad_a_to_b();
    let downstream = authenticated_manager("node-b", "shuffle-secret");
    downstream.spawn();
    let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
    downstream
        .register_inbound_for_session(
            quad,
            input_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "job-late".into(),
                generation: 1,
            },
        )
        .unwrap();
    let (mut client, server) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(server));
    let auth = DataPlaneCredentials::new("node-a", "shuffle-secret")
        .unwrap()
        .session("node-b", "job-late", 1, quad);
    let (mut c2s, _s2c) = handshake_and_derive(&mut client, &auth, quad).await;
    let mut encoder = DataEncoder::new();
    let data = encoder.encode(&dictionary_batch(None), 0).unwrap();
    write_frame_sealed(
        &mut client,
        quad,
        FrameKind::Data,
        &data,
        MAX_FRAME_LEN,
        &mut c2s,
    )
    .await
    .unwrap();
    let _ = next_envelope(&input_rx).await;
    // The Job is torn down, then the connection fails: reporting the
    // error must not recreate the removed Job's state.
    downstream.remove_job_session("job-late", 1);
    write_frame_sealed(
        &mut client,
        quad,
        FrameKind::Signal,
        b"garbage",
        MAX_FRAME_LEN,
        &mut c2s,
    )
    .await
    .unwrap();
    let global_failures = downstream.failure_receiver();
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(300),
            global_failures.recv_async()
        )
        .await
        .is_err(),
        "a late error after job removal must not surface globally"
    );
    assert!(
        downstream.failure_channels.read().unwrap().is_empty(),
        "the removed Job's failure channel must not be recreated"
    );
    downstream.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn receipt_write_failure_fails_the_serving_connection() {
    let quad = quad_a_to_b();
    let upstream = authenticated_manager("node-a", "shuffle-secret");
    let downstream = authenticated_manager("node-b", "shuffle-secret");
    upstream.spawn();
    downstream.spawn();
    let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
    downstream
        .register_inbound_for_session(
            quad,
            input_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "job-rw".into(),
                generation: 1,
            },
        )
        .unwrap();
    let failures = downstream.failure_receiver_for_job("job-rw", 1);
    let killed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (client, server) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(WriteKillStream {
        inner: Box::new(server),
        killed: killed.clone(),
    }));
    let edge = upstream
        .open_edge_with_stream_for_session(Box::new(client), quad, "node-b", "job-rw", 1)
        .unwrap();
    let branch = Arc::new(RecordingAck::default());
    edge.sender
        .send_async(Envelope::Data(
            Arc::new(dictionary_batch(Some("rw"))),
            branch.clone(),
        ))
        .await
        .unwrap();
    let received = next_envelope(&input_rx).await;
    let Envelope::Data(_, ack) = received else {
        panic!("expected data");
    };
    killed.store(true, std::sync::atomic::Ordering::Release);
    ack.ack().await.expect("the local chain acknowledges");
    let failure = recv_within(&failures, TEST_PROPAGATION_BUDGET).await;
    assert!(
        failure.contains("receipt write failed") || failure.contains("receipt flush failed"),
        "{failure}"
    );
    upstream.shutdown();
    downstream.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn deferred_session_loss_is_suppressed_when_the_upstream_reregisters() {
    let quad = quad_a_to_b();
    let downstream = NetworkManager::with_config(NetworkManagerConfig {
        credentials: Some(
            DataPlaneCredentials::new("node-b", "shuffle-secret").expect("credentials"),
        ),
        registration_grace: std::time::Duration::from_millis(100),
        // A generous grace keeps the recovery window open even when the
        // test binary runs under load.
        reconnect_grace: std::time::Duration::from_secs(3),
        ..Default::default()
    })
    .unwrap();
    downstream.spawn();
    let (input_tx, input_rx) = flume::bounded::<Envelope>(8);
    downstream
        .register_inbound_for_session(
            quad,
            input_tx,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "job-rl".into(),
                generation: 1,
            },
        )
        .unwrap();
    let failures = downstream.failure_receiver_for_job("job-rl", 1);

    // First connection serves a delivery, then dies without Eos.
    let upstream1 = authenticated_manager("node-a", "shuffle-secret");
    upstream1.spawn();
    let (client1, server1) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(server1));
    let edge1 = upstream1
        .open_edge_with_stream_for_session(Box::new(client1), quad, "node-b", "job-rl", 1)
        .unwrap();
    edge1
        .sender
        .send_async(Envelope::Data(
            Arc::new(dictionary_batch(None)),
            Arc::new(crate::input::NoopAck),
        ))
        .await
        .unwrap();
    let _ = next_envelope(&input_rx).await;
    // Shut the first upstream down: its connection dies mid-stream (no
    // Eos), which the downstream defers through the reconnect grace.
    drop(edge1);
    upstream1.shutdown();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // A replacement registers the same session within the grace.
    let upstream2 = authenticated_manager("node-a", "shuffle-secret");
    upstream2.spawn();
    let (client2, server2) = tokio::io::duplex(64 * 1024);
    downstream.accept_stream(Box::new(server2));
    let edge2 = upstream2
        .open_edge_with_stream_for_session(Box::new(client2), quad, "node-b", "job-rl", 1)
        .unwrap();
    // A control signal re-registers the session (data frames from a fresh
    // upstream restart at sequence zero and are dropped by the
    // delivery-level dedup, which exists for same-upstream replays).
    edge2
        .sender
        .send_async(Envelope::Barrier(CheckpointBarrier {
            checkpoint_id: "c-rl".into(),
            generation: 1,
            trace_context: None,
        }))
        .await
        .unwrap();
    assert!(matches!(
        next_envelope(&input_rx).await,
        Envelope::Barrier(_)
    ));

    // Past the grace the loss stays suppressed: the upstream recovered.
    tokio::time::sleep(std::time::Duration::from_millis(3_400)).await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), failures.recv_async())
            .await
            .is_err(),
        "a recovered session must not report a loss"
    );
    upstream2.shutdown();
    downstream.shutdown();
}

#[tokio::test]
async fn authorize_inbound_requires_configured_credentials() {
    let manager = NetworkManager::new(8);
    let (header, payload) = raw_handshake("node-a", "job-nc", 1, quad_a_to_b());
    let error = match manager.authorize_inbound(header, &payload).await {
        Err(error) => error,
        Ok(_) => panic!("a plaintext manager cannot authorize handshakes"),
    };
    assert!(
        error.to_string().contains("credentials are not configured"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pending_receipts_budget_exceeded_fails_delivery_not_attempt() {
    // Pins the delivery-level failure contract: when the pending receipt
    // budget is exceeded, the EXCESS delivery is aborted for upstream
    // redelivery (at-least-once) and the pump keeps serving the channel —
    // the attempt does NOT fail on transient congestion.
    let quad = quad_a_to_b();
    let config = NetworkManagerConfig {
        max_pending_receipts: 1,
        ..Default::default()
    };
    let pending = Arc::new(PendingReceipts::new(1));
    let shutdown = tokio_util::sync::CancellationToken::new();
    let (writer, mut reader) = tokio::io::duplex(256 * 1024);
    let (tx, rx) = flume::bounded::<Envelope>(4);
    let (failures_tx, _failures_rx) = flume::unbounded::<Error>();
    let accepted_branch = Arc::new(RecordingAck::default());
    let rejected_branch = Arc::new(RecordingAck::default());
    let third_branch = Arc::new(RecordingAck::default());
    // The pump runs CONCURRENTLY and the sender stays open: the overflow is
    // observed mid-stream, not by a pump that already drained.
    let pump = tokio::spawn(pump_edge(
        rx,
        writer,
        quad,
        pending.clone(),
        shutdown,
        config,
        None,
        false,
        None,
        false,
        false,
    ));
    tx.send_async(Envelope::Data(
        Arc::new(dictionary_batch(None)),
        accepted_branch.clone(),
    ))
    .await
    .unwrap();
    // Let the pump register seq 0 (the entry budget is 1).
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    tx.send_async(Envelope::Data(
        Arc::new(dictionary_batch(None)),
        rejected_branch.clone(),
    ))
    .await
    .unwrap();
    // Wait until the SECOND batch is actually rejected (proving the slot was
    // still occupied) BEFORE freeing it — a plain sleep would let a correct
    // implementation deliver the second batch into the freed slot.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !rejected_branch
            .aborted
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the over-budget delivery is rejected while the slot is occupied");
    // The attempt survived the budget overflow — and KEPT SERVING: freeing
    // the slot lets a third delivery reach the peer over the same pump.
    pending.apply(
        ReceiptFrame {
            kind: ReceiptKind::Acked,
            seq: 0,
        },
        &failures_tx,
    );
    // The branch ack is applied off the read loop (spawned); poll for it.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !accepted_branch
            .acked
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the receipt completes the first branch");
    tx.send_async(Envelope::Data(
        Arc::new(dictionary_batch(None)),
        third_branch.clone(),
    ))
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    drop(tx);
    pump.await.unwrap().expect("the pump drains cleanly");
    assert!(
        rejected_branch
            .aborted
            .load(std::sync::atomic::Ordering::SeqCst),
        "the over-budget delivery must be aborted for upstream redelivery"
    );
    assert!(
        !rejected_branch
            .acked
            .load(std::sync::atomic::Ordering::SeqCst),
        "an aborted delivery must never complete its source ack"
    );
    assert!(
        !accepted_branch
            .aborted
            .load(std::sync::atomic::Ordering::SeqCst),
        "the delivery that fit the budget is untouched"
    );
    // Exactly two data frames reached the peer: the accepted first and the
    // third after the slot freed — the over-budget batch never hit the wire.
    let mut data_frames = 0;
    while let Ok((header, _payload)) = read_frame(&mut reader).await {
        assert_eq!(header.kind, FrameKind::Data);
        data_frames += 1;
    }
    assert_eq!(data_frames, 2, "first and third deliveries reach the peer");
}

#[tokio::test(flavor = "multi_thread")]
async fn pump_data_write_failures_fail_the_edge() {
    let quad = quad_a_to_b();
    // A data frame larger than the pump's write buffer: it is handed to
    // the stream directly, so a dead wire fails the edge on the spot.
    let (tx, rx) = flume::bounded::<Envelope>(4);
    tx.send_async(Envelope::Data(
        Arc::new(MessageBatch::new_arrow(large_batch(4_000))),
        Arc::new(RecordingAck::default()),
    ))
    .await
    .unwrap();
    drop(tx);
    let error = pump_edge(
        rx,
        Box::new(WriteFailStream),
        quad,
        Arc::new(PendingReceipts::new(64)),
        tokio_util::sync::CancellationToken::new(),
        NetworkManagerConfig::default(),
        None,
        false,
        None,
        false,
        false,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("data write failed"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn hub_provisioned_per_edge_keys_isolate_edges_and_reject_missing_secret() {
    let downstream = NetworkManager::with_config(NetworkManagerConfig {
        credentials: Some(DataPlaneCredentials::hub_provisioned("node-b")),
        registration_grace: std::time::Duration::from_millis(80),
        ..NetworkManagerConfig::default()
    })
    .unwrap();
    let upstream_a = NetworkManager::with_config(NetworkManagerConfig {
        credentials: Some(DataPlaneCredentials::hub_provisioned("node-a")),
        ..NetworkManagerConfig::default()
    })
    .unwrap();
    let upstream_c = NetworkManager::with_config(NetworkManagerConfig {
        credentials: Some(DataPlaneCredentials::hub_provisioned("node-c")),
        ..NetworkManagerConfig::default()
    })
    .unwrap();
    downstream.spawn();
    upstream_a.spawn();
    upstream_c.spawn();
    let port = downstream
        .bind_tcp("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let quad_ab = Quad {
        src_op: 0,
        src_subtask: 0,
        dst_op: 1,
        dst_subtask: 0,
    };
    let quad_cb = Quad {
        src_op: 2,
        src_subtask: 0,
        dst_op: 1,
        dst_subtask: 0,
    };
    let (tx_ab, rx_ab) = flume::bounded(8);
    let (tx_cb, rx_cb) = flume::bounded(8);

    // Hub-provisioned mode refuses edges missing a per-edge secret.
    assert!(downstream
        .register_inbound_for_session_with_secret(
            quad_ab,
            tx_ab.clone(),
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "job-per-edge".into(),
                generation: 1,
            },
            None,
        )
        .is_err());
    assert!(upstream_a
        .open_edge_deferred_for_job_with_secret(
            Arc::new(TcpEdgeTransport::plaintext(addr, 1)),
            quad_ab,
            "node-b".into(),
            "job-per-edge".into(),
            1,
            None,
        )
        .is_err());

    // Register two edges toward node-b with distinct secrets.
    let secret_ab = b"edge-secret-a-to-b-0123456789abcdef";
    let secret_cb = b"edge-secret-c-to-b-fedcba9876543210";
    downstream
        .register_inbound_for_session_with_secret(
            quad_ab,
            tx_ab,
            PeerExpectation {
                source_node: "node-a".into(),
                job_id: "job-per-edge".into(),
                generation: 1,
            },
            Some(secret_ab),
        )
        .unwrap();
    downstream
        .register_inbound_for_session_with_secret(
            quad_cb,
            tx_cb,
            PeerExpectation {
                source_node: "node-c".into(),
                job_id: "job-per-edge".into(),
                generation: 1,
            },
            Some(secret_cb),
        )
        .unwrap();

    // Using secret_cb for node-a -> node-b must fail authentication cleanly.
    let spoof = upstream_a
        .open_edge_deferred_for_job_with_secret(
            Arc::new(TcpEdgeTransport::plaintext(addr, 1)),
            quad_ab,
            "node-b".into(),
            "job-per-edge".into(),
            1,
            Some(secret_cb),
        )
        .unwrap();
    spoof
        .sender
        .send_async(Envelope::Data(
            Arc::new(MessageBatch::new_arrow(large_batch(4))),
            Arc::new(RecordingAck::default()),
        ))
        .await
        .unwrap();
    drop(spoof);
    let err_rx = downstream.failure_receiver();
    let err = tokio::time::timeout(std::time::Duration::from_secs(3), err_rx.recv_async())
        .await
        .expect("spoofed secret must surface handshake failure")
        .unwrap();
    assert!(
        err.to_string().contains("credential mismatch"),
        "expected credential mismatch error, got {err}"
    );
    assert!(
        rx_ab.try_recv().is_err(),
        "spoofed batch must never be delivered"
    );

    // Genuine per-edge secrets transfer data end-to-end on both isolated edges.
    let edge_ab = upstream_a
        .open_edge_deferred_for_job_with_secret(
            Arc::new(TcpEdgeTransport::plaintext(addr, 2)),
            quad_ab,
            "node-b".into(),
            "job-per-edge".into(),
            1,
            Some(secret_ab),
        )
        .unwrap();
    let edge_cb = upstream_c
        .open_edge_deferred_for_job_with_secret(
            Arc::new(TcpEdgeTransport::plaintext(addr, 2)),
            quad_cb,
            "node-b".into(),
            "job-per-edge".into(),
            1,
            Some(secret_cb),
        )
        .unwrap();
    let ack_ab = Arc::new(RecordingAck::default());
    let ack_cb = Arc::new(RecordingAck::default());
    edge_ab
        .sender
        .send_async(Envelope::Data(
            Arc::new(MessageBatch::new_arrow(large_batch(2))),
            ack_ab.clone(),
        ))
        .await
        .unwrap();
    edge_cb
        .sender
        .send_async(Envelope::Data(
            Arc::new(MessageBatch::new_arrow(large_batch(2))),
            ack_cb.clone(),
        ))
        .await
        .unwrap();
    assert!(rx_ab.recv_async().await.is_ok());
    assert!(rx_cb.recv_async().await.is_ok());
    downstream.shutdown();
    upstream_a.shutdown();
    upstream_c.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn data_plane_tls_san_binding_rejects_mismatched_node_uri() {
    use rcgen::{string::Ia5String, CertificateParams, IsCa, Issuer, KeyPair, SanType};
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca_params, &ca_key);

    let make_san_node = |node_id: &str| {
        let mut params =
            CertificateParams::new(vec![super::DATA_PLANE_TLS_SERVER_NAME.to_owned()]).unwrap();
        params.subject_alt_names.push(SanType::URI(
            Ia5String::try_from(format!("urn:arkflow:node:{node_id}")).unwrap(),
        ));
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &issuer).unwrap();
        DataPlaneTlsConfig::from_pem(&cert.pem(), &key.serialize_pem(), &ca.pem()).unwrap()
    };

    let tls_a = make_san_node("node-a");
    let tls_b = make_san_node("node-b");

    let manager_b = NetworkManager::with_config(NetworkManagerConfig {
        credentials: Some(DataPlaneCredentials::new("node-b", b"san-test-secret").unwrap()),
        tls: Some(tls_b),
        ..NetworkManagerConfig::default()
    })
    .unwrap();
    manager_b.spawn();
    let port = manager_b
        .bind_tcp("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    // Outbound transport expecting "node-c" against server presenting SAN node-b must fail.
    let mismatch_transport = TcpEdgeTransport {
        addr,
        max_attempts: 1,
        tls: Some(tls_a.clone()),
        peer_node: Some("node-c".into()),
    };
    let err = mismatch_transport
        .connect(quad_a_to_b())
        .await
        .err()
        .unwrap();
    assert!(
        err.to_string().contains("SAN mismatch"),
        "expected SAN mismatch error, got {err}"
    );

    // Outbound transport expecting "node-b" succeeds TLS + SAN verification;
    // then inbound rejects claim of source_node = "node-wrong" vs client cert SAN node-a.
    let matching_transport = TcpEdgeTransport {
        addr,
        max_attempts: 1,
        tls: Some(tls_a),
        peer_node: Some("node-b".into()),
    };
    let upstream_impostor = NetworkManager::with_config(NetworkManagerConfig {
        credentials: Some(DataPlaneCredentials::new("node-wrong", b"san-test-secret").unwrap()),
        ..NetworkManagerConfig::default()
    })
    .unwrap();
    upstream_impostor.spawn();
    let (tx, _rx) = flume::bounded(4);
    let quad = quad_a_to_b();
    manager_b
        .register_inbound_for_session(
            quad,
            tx,
            PeerExpectation {
                source_node: "node-wrong".into(),
                job_id: "san-job".into(),
                generation: 1,
            },
        )
        .unwrap();
    let err_rx = manager_b.failure_receiver();
    let edge = upstream_impostor
        .open_edge_deferred_for_job(
            Arc::new(matching_transport),
            quad,
            "node-b".into(),
            "san-job".into(),
            1,
        )
        .unwrap();
    edge.sender
        .send_async(Envelope::Data(
            Arc::new(MessageBatch::new_arrow(large_batch(1))),
            Arc::new(RecordingAck::default()),
        ))
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "inbound SAN mismatch must surface");
        let err = tokio::time::timeout(remaining, err_rx.recv_async())
            .await
            .expect("inbound SAN mismatch must report global failure")
            .unwrap();
        if err.to_string().contains("SAN mismatch") {
            break;
        }
    }

    manager_b.shutdown();
    upstream_impostor.shutdown();
}
