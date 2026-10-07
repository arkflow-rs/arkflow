use super::*;
use datafusion::arrow::array::{Float64Array, Int64Array as I64, StringArray};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct FailOnceAck {
    fail: AtomicBool,
}

#[async_trait]
impl Ack for FailOnceAck {
    async fn ack(&self) -> Result<(), Error> {
        if self.fail.swap(false, Ordering::AcqRel) {
            Err(Error::Process("source acknowledgement failed".into()))
        } else {
            Ok(())
        }
    }
}

struct CountingAck {
    acked: AtomicUsize,
}

#[async_trait]
impl Ack for CountingAck {
    async fn ack(&self) -> Result<(), Error> {
        self.acked.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

fn batch(rows: Vec<(i64, &str, i64)>, watermark: Option<i64>) -> MessageBatchRef {
    let mut fields = vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(I64::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.1.to_string()).collect::<Vec<_>>(),
        )),
        Arc::new(I64::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())),
    ];
    if let Some(watermark) = watermark {
        fields.push(Field::new("__watermark_ms", DataType::Int64, false));
        columns.push(Arc::new(I64::from(vec![watermark; rows.len()])));
    }
    Arc::new(crate::MessageBatch::new_arrow(
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
    ))
}

fn eviction_config(max_buffered_keys: usize) -> WindowOperatorConfig {
    WindowOperatorConfig {
        kind: WindowKind::Tumbling { size_ms: 1_000 },
        timestamp_field: "ts".into(),
        key_field: "key".into(),
        value_fields: vec!["value".into()],
        trigger: WindowTrigger::Watermark,
        trigger_interval_ms: 1_000,
        watermark_field: "__watermark_ms".into(),
        allowed_lateness_ms: 0,
        legacy_payload: false,
        max_buffered_keys,
    }
}

#[test]
fn validate_rejects_zero_max_buffered_keys() {
    let error = eviction_config(0).validate().unwrap_err().to_string();
    assert!(error.contains("max_buffered_keys"), "{error}");
}

#[tokio::test]
async fn window_evicts_oldest_windows_over_entry_cap() {
    let backend = Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap());
    let op = ColumnarWindowOperator::new(eviction_config(2), backend, "eviction-test");
    // Four distinct windows (no watermark: nothing fires or cleans up).
    op.process(batch(vec![(100, "a", 1), (1_100, "b", 2)], None))
        .await
        .unwrap();
    op.process(batch(vec![(2_100, "c", 3), (3_100, "d", 4)], None))
        .await
        .unwrap();
    let buffers = op.buffers.lock().unwrap();
    assert_eq!(
        buffers.len(),
        2,
        "the entry cap must hold after four distinct windows"
    );
    let starts = buffers.keys().map(|(start, _)| *start).collect::<Vec<_>>();
    assert_eq!(
        starts,
        vec![2_000, 3_000],
        "eviction is oldest-window first"
    );
}

/// The eviction warn is throttled: the first overflow logs immediately,
/// later overflows inside `WINDOW_EVICTION_LOG_INTERVAL` are suppressed
/// and only counted (the suppressed total rides the next warn line).
#[tokio::test]
async fn window_eviction_warn_is_throttled_with_suppressed_count() {
    let backend = Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap());
    let op = ColumnarWindowOperator::new(eviction_config(1), backend, "eviction-throttle-test");
    // First overflow warns immediately and opens the throttle window.
    op.process(batch(vec![(100, "a", 1)], None)).await.unwrap();
    op.process(batch(vec![(1_100, "b", 2)], None))
        .await
        .unwrap();
    {
        let log = op.eviction_log.lock().unwrap();
        assert!(log.0.is_some(), "the first eviction warns immediately");
        assert_eq!(log.1, 0, "nothing is suppressed before the interval opens");
    }
    // Further evictions inside the interval are suppressed and counted.
    op.process(batch(vec![(2_100, "c", 3)], None))
        .await
        .unwrap();
    op.process(batch(vec![(3_100, "d", 4)], None))
        .await
        .unwrap();
    let log = op.eviction_log.lock().unwrap();
    assert_eq!(
        log.1, 2,
        "evictions inside the interval accumulate silently"
    );
    assert!(log.0.is_some());
}

#[tokio::test]
async fn window_within_entry_cap_keeps_all_windows() {
    let backend = Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap());
    let op = ColumnarWindowOperator::new(eviction_config(4), backend, "no-eviction-test");
    op.process(batch(vec![(100, "a", 1), (1_100, "b", 2)], None))
        .await
        .unwrap();
    op.process(batch(vec![(2_100, "c", 3), (3_100, "d", 4)], None))
        .await
        .unwrap();
    assert_eq!(op.buffers.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn evicted_window_does_not_emit_on_later_watermark() {
    let backend = Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap());
    let op = ColumnarWindowOperator::new(eviction_config(2), backend, "evicted-emit-test");
    // Windows [0,1000) for "a" and [1000,2000) for "b"; inserting the
    // third window evicts the oldest ("a") before any watermark fired.
    op.process(batch(vec![(100, "a", 1), (1_100, "b", 2)], None))
        .await
        .unwrap();
    op.process(batch(vec![(2_500, "c", 0)], None))
        .await
        .unwrap();
    assert!(!op
        .buffers
        .lock()
        .unwrap()
        .keys()
        .any(|(start, key)| *start == 0 && key == "a"));
    // A watermark past the first two windows fires only the survivors.
    let result = op
        .process(batch(vec![(2_500, "c", 0)], Some(2_100)))
        .await
        .unwrap();
    let key_column = |output: &crate::MessageBatchRef| {
        output
            .record_batch()
            .column_by_name("key")
            .map(|column| {
                (0..column.len())
                    .map(|row| {
                        column
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::StringArray>()
                            .unwrap()
                            .value(row)
                            .to_string()
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    };
    let emitted: Vec<String> = match result {
        crate::ProcessResult::Single(output) | crate::ProcessResult::SingleWithAck(output, _) => {
            key_column(&output)
        }
        crate::ProcessResult::Multiple(outputs) => outputs.iter().flat_map(key_column).collect(),
        crate::ProcessResult::MultipleWithAck(outputs) => outputs
            .iter()
            .flat_map(|(output, _)| key_column(output))
            .collect(),
        _ => Vec::new(),
    };
    assert_eq!(emitted, vec!["b"], "the evicted window is gone for good");
}

/// Regression (CR on fix-review-p2-remainder): the entry-cap eviction
/// runs AFTER the session re-key. A merged session's acknowledgements
/// move to the merged key first, so the eviction sees them under the
/// merged key and can never strand them on an evicted buffer — stranded
/// acknowledgements freeze the checkpoint frontier forever.
#[tokio::test]
async fn session_merge_under_entry_cap_never_strands_acknowledgements() {
    let mut config = eviction_config(1);
    config.kind = WindowKind::Session { gap_ms: 1_000 };
    let backend = Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap());
    let op = ColumnarWindowOperator::new(config, backend, "eviction-rekey-test");
    // One open session for "a" starting at 100, holding an
    // acknowledgement under its current key.
    op.process(batch(vec![(100, "a", 1)], None)).await.unwrap();
    op.pending_acks
        .lock()
        .unwrap()
        .insert((100, "a".into()), vec![Arc::new(crate::input::NoopAck)]);
    // An earlier event merges the session (re-key 100 → 50) while the
    // new key for "b" pushes the entry count over the cap of one.
    op.process(batch(vec![(50, "a", 2), (5_000, "b", 3)], None))
        .await
        .unwrap();
    let buffers = op.buffers.lock().unwrap();
    let pending = op.pending_acks.lock().unwrap();
    let stranded: Vec<_> = pending
        .keys()
        .filter(|key| !buffers.contains_key(key))
        .collect();
    assert!(
        stranded.is_empty(),
        "every acknowledged key must still have a buffer (stranded: {stranded:?})"
    );
    assert!(
        buffers.contains_key(&(50, "a".to_string())),
        "the merged session with in-flight acknowledgements survives eviction"
    );
}

fn operator(trigger: WindowTrigger, backend: Arc<dyn StateBackend>) -> ColumnarWindowOperator {
    ColumnarWindowOperator::new(
        WindowOperatorConfig {
            kind: WindowKind::Tumbling { size_ms: 10_000 },
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields: vec!["value".into()],
            trigger,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 0,
            legacy_payload: false,
            max_buffered_keys: default_max_buffered_keys(),
        },
        backend,
        "window-test",
    )
}

#[tokio::test]
async fn aggregates_across_batches_and_fires_on_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let op = operator(WindowTrigger::Watermark, backend);
    // Two batches, same window [0, 10000), keys a/b.
    op.process(batch(vec![(1_000, "a", 1), (2_000, "b", 2)], None))
        .await
        .unwrap();
    let held = op
        .process(batch(vec![(3_000, "a", 3)], None))
        .await
        .unwrap();
    assert!(matches!(held, ProcessResult::None));
    // Watermark 10_000 fires window [0, 10000).
    let fired = op
        .process(batch(vec![(11_000, "a", 5)], Some(10_000)))
        .await
        .unwrap();
    let ProcessResult::Single(fired) = fired else {
        panic!("window should fire");
    };
    let keys = fired
        .record_batch()
        .column_by_name("key")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = fired
        .record_batch()
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let sums = fired
        .record_batch()
        .column_by_name("sum")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(keys.len(), 2);
    assert_eq!(counts.values(), &[2, 1]);
    assert_eq!(sums.value(0) + sums.value(1), 6);
    // 11_000 falls into [10000, 20000) and stays held.
    let held = op
        .process(batch(vec![(12_000, "a", 5)], Some(10_000)))
        .await
        .unwrap();
    assert!(matches!(held, ProcessResult::None));
}

#[tokio::test]
async fn processing_time_trigger_fires_on_idle_tick() {
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let op = operator(WindowTrigger::ProcessingTime, backend);
    // Processing-time mode starts a cadence when data arrives; an idle
    // timer flushes the buffer regardless of event timestamps.
    let now = crate::state::now_ms() as i64;
    let far_past = now - 60_000;
    let ts = far_past - (far_past % 10_000);
    let held = op
        .process(batch(vec![(ts, "a", 1), (ts + 1, "a", 2)], None))
        .await
        .unwrap();
    assert!(matches!(held, ProcessResult::None));
    *op.last_processing_trigger_ms.lock().unwrap() = Some(now - 2_000);
    let fired = op.on_tick().await.unwrap();
    assert!(matches!(
        fired,
        ProcessResult::Single(_) | ProcessResult::SingleWithAck(_, _)
    ));
}

#[tokio::test]
async fn processing_time_window_accepts_batches_without_metadata_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let op = operator(WindowTrigger::ProcessingTime, backend);
    let batch = Arc::new(crate::MessageBatch::new_arrow(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                Arc::new(I64::from(vec![7])) as ArrayRef,
            ],
        )
        .unwrap(),
    ));

    assert!(matches!(
        op.process(batch).await.unwrap(),
        ProcessResult::None
    ));
    *op.last_processing_trigger_ms.lock().unwrap() = Some(crate::state::now_ms() as i64 - 2_000);
    let fired = op.on_tick().await.unwrap();
    let (ProcessResult::Single(fired) | ProcessResult::SingleWithAck(fired, _)) = fired else {
        panic!("processing-time window should flush a metadata-free batch");
    };
    assert_eq!(fired.record_batch().num_rows(), 1);
    assert_eq!(
        fired
            .record_batch()
            .column_by_name("sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
}

#[tokio::test]
async fn state_persist_and_restore_reproduces_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let op = operator(WindowTrigger::Watermark, backend.clone());
    op.process(batch(vec![(1_000, "a", 4)], None))
        .await
        .unwrap();
    op.persist_buffers().unwrap();

    let restored = operator(WindowTrigger::Watermark, backend);
    assert_eq!(restored.restore_buffers().unwrap(), 1);
    let fired = restored
        .process(batch(vec![(20_000, "z", 0)], Some(10_000)))
        .await
        .unwrap();
    let ProcessResult::Single(fired) = fired else {
        panic!("restored window should fire");
    };
    let sums = fired
        .record_batch()
        .column_by_name("sum")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(sums.values(), &[4]);
}

#[tokio::test]
async fn negative_and_boundary_timestamps_assign_deterministically() {
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let op = operator(WindowTrigger::Watermark, backend);
    // Boundary exactly at 10_000 belongs to [10000, 20000).
    op.process(batch(
        vec![(10_000, "a", 1), (9_999, "a", 2), (-1, "a", 3)],
        None,
    ))
    .await
    .unwrap();
    let fired = op
        .process(batch(vec![(0, "z", 0)], Some(9_999)))
        .await
        .unwrap();
    let ProcessResult::Single(fired) = fired else {
        panic!("negative window should fire");
    };
    let starts = fired
        .record_batch()
        .column_by_name("window_start")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    // -1 falls into [-10000, 0); 9_999 into [0, 10000).
    assert_eq!(starts.values(), &[-10_000]);
}

#[tokio::test]
async fn sliding_windows_aggregate_overlapping_memberships() {
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let op = ColumnarWindowOperator::new(
        WindowOperatorConfig {
            kind: WindowKind::Sliding {
                size_ms: 10_000,
                slide_ms: 5_000,
            },
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields: vec!["value".into()],
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 0,
            legacy_payload: false,
            max_buffered_keys: default_max_buffered_keys(),
        },
        backend,
        "sliding-test",
    );
    // Event at 6_000 belongs to [0,10000) and [5000,15000).
    op.process(batch(vec![(6_000, "a", 2)], None))
        .await
        .unwrap();
    // Event at 7_000 also belongs to both; [0,10000) has both, [5000,15000) has both.
    op.process(batch(vec![(7_000, "a", 3)], None))
        .await
        .unwrap();
    let fired = op
        .process(batch(vec![(20_000, "z", 0)], Some(15_000)))
        .await
        .unwrap();
    let ProcessResult::Single(fired) = fired else {
        panic!("sliding should fire")
    };
    let starts = fired
        .record_batch()
        .column_by_name("window_start")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = fired
        .record_batch()
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    // Both overlapping windows closed at watermark 15_000.
    assert_eq!(starts.values(), &[0, 5_000]);
    assert_eq!(counts.values(), &[2, 2]);
}

#[tokio::test]
async fn session_windows_extend_within_gap_and_fire_after() {
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let op = ColumnarWindowOperator::new(
        WindowOperatorConfig {
            kind: WindowKind::Session { gap_ms: 1_000 },
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields: vec![],
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 0,
            legacy_payload: false,
            max_buffered_keys: default_max_buffered_keys(),
        },
        backend,
        "session-test",
    );
    // Each event is within 1000ms of the previous one, so the dynamic
    // session end keeps extending instead of using the original start.
    op.process(batch(
        vec![(1_000, "a", 0), (1_800, "a", 0), (2_700, "a", 0)],
        None,
    ))
    .await
    .unwrap();
    // A far-future watermark fires the merged session.
    let fired = op
        .process(batch(vec![(5_000, "b", 0)], Some(3_700)))
        .await
        .unwrap();
    let ProcessResult::Single(fired) = fired else {
        panic!("session should fire")
    };
    let starts = fired
        .record_batch()
        .column_by_name("window_start")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    // One merged session starting at the first event (1_000).
    assert_eq!(starts.values(), &[1_000]);
    let ends = fired
        .record_batch()
        .column_by_name("window_end")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = fired
        .record_batch()
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(ends.values(), &[3_700]);
    assert_eq!(counts.values(), &[3]);
}

#[tokio::test]
async fn late_session_bridge_preserves_emitted_update_state() {
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let op = ColumnarWindowOperator::with_late_event_policy(
        WindowOperatorConfig {
            kind: WindowKind::Session { gap_ms: 1_000 },
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields: vec!["value".into()],
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 10_000,
            legacy_payload: false,
            max_buffered_keys: default_max_buffered_keys(),
        },
        backend,
        "session-bridge-update-test",
        LateEventPolicy::Update,
        false,
    );

    // The two rows are separate sessions at first: [1000, 2000) and
    // [2500, 3500). Both results are retained after the initial fire so a
    // later out-of-order row can bridge them.
    op.process(batch(vec![(1_000, "a", 1), (2_500, "a", 2)], None))
        .await
        .unwrap();
    let first = op.on_watermark(3_500).await.unwrap();
    let ProcessResult::SingleWithAck(first, first_ack) = first else {
        panic!("the initial sessions should fire");
    };
    first_ack.ack().await.unwrap();
    let initial_updates = first
        .record_batch()
        .column_by_name("__arkflow_window_update")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(!initial_updates.value(0));
    assert!(!initial_updates.value(1));

    // Timestamp 1900 is within the first session and its extended end
    // reaches the second session's start. The merge must remain an update
    // of the already-emitted aggregate, not a fresh initial result.
    let corrected = op
        .process(batch(vec![(1_900, "a", 3)], None))
        .await
        .unwrap();
    let ProcessResult::Single(corrected) = corrected else {
        panic!("the bridged session should emit a correction");
    };
    let updates = corrected
        .record_batch()
        .column_by_name("__arkflow_window_update")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(updates.value(0));
    let starts = corrected
        .record_batch()
        .column_by_name("window_start")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(starts.values(), &[1_000]);
    let counts = corrected
        .record_batch()
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(counts.values(), &[3]);
}

#[tokio::test]
async fn finish_emits_pending_session_corrections_before_cleanup() {
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap());
    let op = ColumnarWindowOperator::with_late_event_policy(
        WindowOperatorConfig {
            kind: WindowKind::Session { gap_ms: 1_000 },
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields: vec!["value".into()],
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 10_000,
            legacy_payload: false,
            max_buffered_keys: default_max_buffered_keys(),
        },
        backend,
        "session-eos-correction-test",
        LateEventPolicy::Update,
        false,
    );

    // Two separate sessions fire at watermark 3_500 and stay retained.
    op.process(batch(vec![(1_000, "a", 1), (2_500, "a", 2)], None))
        .await
        .unwrap();
    let first = op.on_watermark(3_500).await.unwrap();
    let ProcessResult::SingleWithAck(_, first_ack) = first else {
        panic!("the initial sessions should fire");
    };
    first_ack.ack().await.unwrap();

    // A late row at 3_400 extends the second session's end to 4_400,
    // past the current watermark: the merged buffer owes a correction
    // that no watermark has released yet.
    let mid = op
        .process(batch(vec![(3_400, "a", 3)], None))
        .await
        .unwrap();
    assert!(matches!(mid, ProcessResult::None));
    assert!(op
        .buffers
        .lock()
        .unwrap()
        .values()
        .any(|buffer| buffer.updated_since_emit));

    // End-of-stream must emit the pending correction BEFORE the expired
    // cleanup can reclaim the buffer. Before the fix the buffer was
    // deleted unemitted and the correction was lost.
    let finished = op.finish().await.unwrap();
    let (ProcessResult::Single(finished) | ProcessResult::SingleWithAck(finished, _)) = finished
    else {
        panic!("end-of-stream must emit the pending session correction");
    };
    let updates = finished
        .record_batch()
        .column_by_name("__arkflow_window_update")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(updates.value(0), "the EOS emission is a correction");
    let counts = finished
        .record_batch()
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(counts.values(), &[2], "merged aggregate rows 2500 and 3400");
}

#[tokio::test]
async fn null_key_rows_follow_the_invalid_policy_instead_of_silent_skip() {
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap());
    let op = operator(WindowTrigger::Watermark, backend.clone());
    let fields = vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("key", DataType::Utf8, true),
        Field::new("value", DataType::Int64, false),
    ];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(I64::from(vec![1_000, 2_000])),
        Arc::new(StringArray::from(vec![Some("a"), None])),
        Arc::new(I64::from(vec![1, 5])),
    ];
    let mixed = Arc::new(crate::MessageBatch::new_arrow(
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
    ));
    op.process(mixed).await.unwrap();
    // The NULL-key row is counted (late/invalid metric), not silently
    // skipped.
    assert_eq!(
        op.late_event_row_counter().load(Ordering::Relaxed),
        1,
        "the NULL-key row must be counted in the late/invalid metric"
    );
    // Only the keyed row aggregates: firing window [0,10000) yields one
    // row for key "a" with count 1.
    let fired = op
        .process(batch(vec![(11_000, "z", 0)], Some(10_000)))
        .await
        .unwrap();
    let ProcessResult::Single(fired) = fired else {
        panic!("window should fire");
    };
    let keys = fired
        .record_batch()
        .column_by_name("key")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = fired
        .record_batch()
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let a_index = (0..keys.len())
        .find(|index| keys.value(*index) == "a")
        .expect("keyed row must aggregate");
    assert_eq!(
        counts.value(a_index),
        1,
        "the NULL-key row must not aggregate"
    );
}

#[tokio::test]
async fn non_journal_fired_ack_failure_rolls_back_backend_state() {
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let op = operator(WindowTrigger::Watermark, backend.clone());
    // Batch 1 accumulates the row; its delivery ack is held under the
    // window key. Batch 2 (with its own ack, so the fired composite is
    // returned to the caller) advances the watermark and fires window
    // [0,10000). The non-journal path persisted the fired buffer BEFORE
    // that composite acknowledgement.
    let ack: Arc<dyn Ack> = Arc::new(FailOnceAck {
        fail: AtomicBool::new(true),
    });
    op.process_with_ack(batch(vec![(1_000, "a", 1)], None), ack)
        .await
        .unwrap();
    let fired = op
        .process_with_ack(
            batch(vec![(11_000, "z", 1)], Some(10_000)),
            Arc::new(CountingAck {
                acked: AtomicUsize::new(0),
            }),
        )
        .await
        .unwrap();
    let ProcessResult::SingleWithAck(_, fired_ack) = fired else {
        panic!("window should fire");
    };
    assert!(
        fired_ack.ack().await.is_err(),
        "the source acknowledgement fails"
    );
    // The kernel's failure path undoes the fired acknowledgement: this
    // releases the retained operation lock and finalizes the rollback
    // for the replay.
    fired_ack.undo().await.unwrap();
    // The backend must NOT retain the fired aggregate: a replay merging
    // into the emitted buffer would double-count.
    let stored = backend.scan("window-test").unwrap();
    // The fired aggregate must be gone from the backend: a replay that
    // merges into an emitted buffer would double-count. What legitimately
    // remains is the pre-fire working state (unemitted), persisted by the
    // earlier non-firing batch.
    for entry in &stored {
        let state: serde_json::Value =
            serde_json::from_slice(&entry.value).expect("window state is JSON");
        assert_eq!(
            state["emitted"],
            serde_json::json!(false),
            "a failed fired acknowledgement must roll back the emitted window state"
        );
    }
    assert!(
        !stored
            .iter()
            .any(|entry| entry.key == ColumnarWindowOperator::state_key(10_000, "z")),
        "the trigger batch's own state must be rolled back too"
    );
    // The in-memory buffer is restored to the unemitted pre-fire state.
    let (restored_emitted, restored_count) = {
        let buffers = op.buffers.lock().unwrap();
        let buffer = buffers.get(&(0, "a".to_string())).unwrap();
        (buffer.emitted, buffer.count)
    };
    assert!(!restored_emitted);
    assert_eq!(restored_count, 1);
    // Re-firing re-emits the retained aggregate exactly once. The replay
    // batch re-establishes the watermark (the failed acknowledgement
    // rolled it back) and its late row is dropped by the operator, so no
    // new aggregate is manufactured.
    let retried = op
        .process(batch(vec![(1_000, "a", 1)], Some(10_000)))
        .await
        .unwrap();
    let (ProcessResult::Single(fired) | ProcessResult::SingleWithAck(fired, _)) = retried else {
        panic!("the retry should re-fire the window");
    };
    let counts = fired
        .record_batch()
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    // The re-emitted aggregate reflects each DELIVERY exactly once: the
    // original unsettled delivery (restored to the unemitted working
    // buffer) plus the replayed one = 2, never a merge into the emitted
    // backend state (which would fabricate a third contribution across
    // restart/re-emit).
    assert_eq!(counts.value(0), 2);
}

#[tokio::test]
async fn expired_session_rows_are_dropped_without_opening_a_new_session() {
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap());
    let op = ColumnarWindowOperator::with_late_event_policy(
        WindowOperatorConfig {
            kind: WindowKind::Session { gap_ms: 1_000 },
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields: vec!["value".into()],
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 0,
            legacy_payload: false,
            max_buffered_keys: default_max_buffered_keys(),
        },
        backend,
        "session-expiry-test",
        LateEventPolicy::Drop,
        false,
    );

    op.process(batch(vec![(100, "a", 1)], None)).await.unwrap();
    let fired = op
        .process(batch(vec![(3_000, "b", 0)], Some(2_000)))
        .await
        .unwrap();
    assert!(matches!(fired, ProcessResult::Single(_)));

    // The original session ended at 1100 and was already past its
    // allowed-lateness deadline. A late row must be acknowledged/dropped,
    // not create a new [100, 1100) partial session.
    assert!(matches!(
        op.process(batch(vec![(100, "a", 99)], None)).await.unwrap(),
        ProcessResult::None
    ));
    assert!(!op.buffers.lock().unwrap().keys().any(|(_, key)| key == "a"));
}

#[tokio::test]
async fn float64_values_aggregate_with_typed_output() {
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let config = WindowOperatorConfig {
        kind: WindowKind::Tumbling { size_ms: 10_000 },
        timestamp_field: "ts".into(),
        key_field: "key".into(),
        value_fields: vec!["f".into()],
        trigger: WindowTrigger::Watermark,
        trigger_interval_ms: 1_000,
        watermark_field: "__watermark_ms".into(),
        allowed_lateness_ms: 0,
        legacy_payload: false,
        max_buffered_keys: default_max_buffered_keys(),
    };
    let op = ColumnarWindowOperator::new(config, backend, "float-test");
    let fields = vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("f", DataType::Float64, false),
    ];
    let record = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(Int64Array::from(vec![1_000, 2_000])),
            Arc::new(StringArray::from(vec!["a", "a"])),
            Arc::new(Float64Array::from(vec![1.2, 1.3])),
        ],
    )
    .unwrap();
    op.process(Arc::new(crate::MessageBatch::new_arrow(record)))
        .await
        .unwrap();
    let trigger = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("f", DataType::Float64, false),
            Field::new("__watermark_ms", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![20_000])),
            Arc::new(StringArray::from(vec!["z"])),
            Arc::new(Float64Array::from(vec![0.0])),
            Arc::new(Int64Array::from(vec![10_000])),
        ],
    )
    .unwrap();
    let fired = op
        .process(Arc::new(crate::MessageBatch::new_arrow(trigger)))
        .await
        .unwrap();
    let ProcessResult::Single(fired) = fired else {
        panic!("float window should fire");
    };
    let sums = fired
        .record_batch()
        .column_by_name("sum")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64 windows emit a Float64 sum column");
    assert_eq!(sums.len(), 1);
    assert!(
        (sums.value(0) - 2.5).abs() < 1e-9,
        "1.2 + 1.3 sums as floats"
    );
    let mins = fired
        .record_batch()
        .column_by_name("min")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let maxs = fired
        .record_batch()
        .column_by_name("max")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((mins.value(0) - 1.2).abs() < 1e-9);
    assert!((maxs.value(0) - 1.3).abs() < 1e-9);
    assert_eq!(
        fired
            .record_batch()
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        2
    );
}

#[tokio::test]
async fn float32_values_sum_as_numbers_with_float32_schema() {
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let config = WindowOperatorConfig {
        kind: WindowKind::Tumbling { size_ms: 10_000 },
        timestamp_field: "ts".into(),
        key_field: "key".into(),
        value_fields: vec!["f".into()],
        trigger: WindowTrigger::Watermark,
        trigger_interval_ms: 1_000,
        watermark_field: "__watermark_ms".into(),
        allowed_lateness_ms: 0,
        legacy_payload: false,
        max_buffered_keys: default_max_buffered_keys(),
    };
    let op = ColumnarWindowOperator::new(config, backend, "float32-test");
    let record = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("f", DataType::Float32, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1_000])),
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(datafusion::arrow::array::Float32Array::from(vec![1.5f32])),
        ],
    )
    .unwrap();
    op.process(Arc::new(crate::MessageBatch::new_arrow(record)))
        .await
        .unwrap();
    let trigger = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("f", DataType::Float32, false),
            Field::new("__watermark_ms", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![20_000])),
            Arc::new(StringArray::from(vec!["z"])),
            Arc::new(datafusion::arrow::array::Float32Array::from(vec![0.0f32])),
            Arc::new(Int64Array::from(vec![10_000])),
        ],
    )
    .unwrap();
    let fired = op
        .process(Arc::new(crate::MessageBatch::new_arrow(trigger)))
        .await
        .unwrap();
    let ProcessResult::Single(fired) = fired else {
        panic!("float32 window should fire");
    };
    let sums = fired
        .record_batch()
        .column_by_name("sum")
        .unwrap()
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Float32Array>()
        .expect("Float32 windows emit a Float32-compatible schema");
    // Summed as numeric values (1.5), never treated as a count (1).
    assert!((sums.value(0) - 1.5).abs() < 1e-6);
}

#[tokio::test]
async fn unsupported_value_types_are_rejected_explicitly() {
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let config = WindowOperatorConfig {
        kind: WindowKind::Tumbling { size_ms: 10_000 },
        timestamp_field: "ts".into(),
        key_field: "key".into(),
        value_fields: vec!["v".into()],
        trigger: WindowTrigger::Watermark,
        trigger_interval_ms: 1_000,
        watermark_field: "__watermark_ms".into(),
        allowed_lateness_ms: 0,
        legacy_payload: false,
        max_buffered_keys: default_max_buffered_keys(),
    };
    let op = ColumnarWindowOperator::new(config, backend, "unsupported-test");
    let record = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("v", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1_000])),
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(StringArray::from(vec!["not-a-number"])),
        ],
    )
    .unwrap();
    let result = op
        .process(Arc::new(crate::MessageBatch::new_arrow(record)))
        .await;
    let error = result.expect_err("unsupported value type must fail");
    assert!(
        error.to_string().contains("unsupported numeric type"),
        "{error}"
    );
}

#[tokio::test]
async fn typed_state_survives_serialization_roundtrip() {
    let mut buffer = AggregateBuffer::default();
    buffer.observe_float(1.2, NumericKind::Float64);
    buffer.observe_float(1.3, NumericKind::Float64);
    buffer.emitted = true;
    let encoded = encode_buffer(&buffer).unwrap();
    let decoded = decode_buffer(&encoded).unwrap();
    assert_eq!(decoded.kind, NumericKind::Float64);
    assert_eq!(decoded.count, 2);
    assert!((decoded.sum_float - 2.5).abs() < 1e-9);
    assert!(decoded.emitted);
    assert!((decoded.min_float - 1.2).abs() < 1e-9);
    assert!((decoded.max_float - 1.3).abs() < 1e-9);
}

#[test]
fn legacy_integer_state_migrates_and_legacy_float_state_is_rejected() {
    // A legacy integer payload migrates losslessly.
    let legacy =
        br#"{"count":3,"sum_i64":6,"min_i64":1,"max_i64":3,"is_float":false,"session_end_ms":0}"#;
    let migrated = decode_buffer(legacy).unwrap();
    assert_eq!(migrated.kind, NumericKind::Int64);
    assert_eq!(migrated.count, 3);
    assert_eq!(migrated.sum_i64, 6);
    assert_eq!(migrated.min_i64, 1);
    assert_eq!(migrated.max_i64, 3);
    // A legacy float payload stored min/max as integer sentinels; its
    // restore is a compatibility failure instead of silent corruption.
    let legacy_float = br#"{"count":2,"sum_float":2.5,"min_i64":-9223372036854775808,"max_i64":9223372036854775807,"is_float":true}"#;
    assert!(decode_buffer(legacy_float).is_err());
}

/// Task 4.5: a late Update within the allowed-lateness deadline corrects
/// the SAME `(operator, key, window)` aggregate and re-emits a complete
/// result with an update marker; past the deadline the row is dropped
/// from aggregation (the gate's late policy routes or drops it).
#[tokio::test]
async fn late_update_corrects_an_emitted_window() {
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let config = WindowOperatorConfig {
        kind: WindowKind::Tumbling { size_ms: 1_000 },
        timestamp_field: "ts".into(),
        key_field: "key".into(),
        value_fields: vec!["value".into()],
        trigger: WindowTrigger::Watermark,
        trigger_interval_ms: 1_000,
        watermark_field: "__watermark_ms".into(),
        allowed_lateness_ms: 5_000,
        legacy_payload: false,
        max_buffered_keys: default_max_buffered_keys(),
    };
    let op = ColumnarWindowOperator::new(config, backend, "late-update-test");
    // Initial window [0,1000) with one row of value 10.
    op.process(batch(vec![(100, "a", 10)], None)).await.unwrap();
    // Watermark 1000 fires [0,1000).
    let fired = op
        .process(batch(vec![(2_000, "b", 0)], Some(1_000)))
        .await
        .unwrap();
    let ProcessResult::Single(first) = fired else {
        panic!("window should fire");
    };
    let updates = first
        .record_batch()
        .column_by_name("__arkflow_window_update")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(!updates.value(0), "the initial result is not an update");
    let sums = first
        .record_batch()
        .column_by_name("sum")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(sums.value(0), 10);

    // A late update (marked by the gate) lands in the retained window.
    let late = {
        let mut fields = vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![200])),
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(Int64Array::from(vec![5])),
        ];
        fields.push(Field::new(
            "__arkflow_late_event_update",
            DataType::Boolean,
            false,
        ));
        columns.push(Arc::new(BooleanArray::from(vec![true])));
        Arc::new(crate::MessageBatch::new_arrow(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
        ))
    };
    let corrected = op.process(late).await.unwrap();
    let ProcessResult::Single(corrected) = corrected else {
        panic!("the corrected window should re-emit");
    };
    let updates = corrected
        .record_batch()
        .column_by_name("__arkflow_window_update")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(updates.value(0), "the corrected result carries the marker");
    let sums = corrected
        .record_batch()
        .column_by_name("sum")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let starts = corrected
        .record_batch()
        .column_by_name("window_start")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(starts.value(0), 0, "the correction targets the same window");
    assert_eq!(sums.value(0), 15, "10 + the late 5");
}

#[tokio::test]
async fn fired_window_rolls_back_when_source_ack_fails() {
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap());
    let journal = Arc::new(super::super::state_journal::StateJournal::new(
        backend.clone(),
    ));
    let op = ColumnarWindowOperator::with_journal(
        WindowOperatorConfig {
            kind: WindowKind::Tumbling { size_ms: 1_000 },
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields: vec!["value".into()],
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 0,
            legacy_payload: false,
            max_buffered_keys: default_max_buffered_keys(),
        },
        backend.clone(),
        journal.clone(),
        "window-ack-rollback-test",
    );
    let source_ack = Arc::new(FailOnceAck {
        fail: AtomicBool::new(true),
    });

    // The row is buffered and its source acknowledgement is held in the
    // window transaction until the aggregate is emitted.
    op.process_with_ack(batch(vec![(100, "a", 10)], None), source_ack.clone())
        .await
        .unwrap();

    let fired = op
        .process_with_ack(
            batch(vec![(2_000, "b", 0)], Some(1_000)),
            Arc::new(crate::input::NoopAck),
        )
        .await
        .unwrap();
    let ProcessResult::SingleWithAck(_, output_ack) = fired else {
        panic!("watermark should produce an acknowledged window output");
    };

    // The source commit fails after the journal has applied the fired
    // window. The composite acknowledgement must compensate that apply,
    // leaving the input replayable and the backend at the pre-fire cut.
    assert!(output_ack.ack().await.is_err());
    assert!(backend
        .get(
            "window-ack-rollback-test",
            &ColumnarWindowOperator::state_key(0, "a")
        )
        .unwrap()
        .is_none());

    // Retrying the same acknowledgement re-applies the staged mutation;
    // the successful source commit then makes the fired window durable.
    output_ack.ack().await.unwrap();
    let restored = backend
        .get(
            "window-ack-rollback-test",
            &ColumnarWindowOperator::state_key(0, "a"),
        )
        .unwrap()
        .expect("successful retry commits the window state");
    let restored = decode_buffer(&restored).unwrap();
    assert_eq!(restored.count, 1);
    assert_eq!(restored.sum_i64, 10);
    assert!(restored.emitted);
    assert_eq!(journal.pending_transactions(), 1);
}

/// Task 4.5: past the allowed-lateness deadline the retained buffer is
/// cleaned up instead of staying resident forever.
#[tokio::test]
async fn emitted_windows_are_retained_until_the_deadline_then_cleaned() {
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let config = WindowOperatorConfig {
        kind: WindowKind::Tumbling { size_ms: 1_000 },
        timestamp_field: "ts".into(),
        key_field: "key".into(),
        value_fields: vec!["value".into()],
        trigger: WindowTrigger::Watermark,
        trigger_interval_ms: 1_000,
        watermark_field: "__watermark_ms".into(),
        allowed_lateness_ms: 5_000,
        legacy_payload: false,
        max_buffered_keys: default_max_buffered_keys(),
    };
    let op = ColumnarWindowOperator::new(config, backend, "deadline-test");
    op.process(batch(vec![(100, "a", 1)], None)).await.unwrap();
    op.process(batch(vec![(2_000, "b", 0)], Some(1_000)))
        .await
        .unwrap();
    // Within the deadline the buffer is retained.
    {
        let buffers = op.buffers.lock().unwrap();
        assert!(buffers.contains_key(&(0, "a".to_string())));
    }
    // Far past the deadline (watermark 7000 > 1000 + 5000): cleanup.
    op.process(batch(vec![(8_000, "b", 0)], Some(7_000)))
        .await
        .unwrap();
    let buffers = op.buffers.lock().unwrap();
    assert!(
        !buffers.contains_key(&(0, "a".to_string())),
        "past-deadline buffers are cleaned up"
    );
}

#[tokio::test]
async fn journaled_idle_cleanup_removes_expired_backend_rows() {
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap());
    let journal = Arc::new(super::super::state_journal::StateJournal::new(
        backend.clone(),
    ));
    let op = ColumnarWindowOperator::with_journal(
        WindowOperatorConfig {
            kind: WindowKind::Tumbling { size_ms: 1_000 },
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields: vec!["value".into()],
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 5_000,
            legacy_payload: false,
            max_buffered_keys: default_max_buffered_keys(),
        },
        backend.clone(),
        journal,
        "journaled-idle-cleanup-test",
    );

    op.process_with_ack(
        batch(vec![(100, "a", 1)], None),
        Arc::new(crate::input::NoopAck),
    )
    .await
    .unwrap();
    let fired = op.on_watermark(1_000).await.unwrap();
    let ProcessResult::SingleWithAck(_, fired_ack) = fired else {
        panic!("the initial window should fire");
    };
    fired_ack.ack().await.unwrap();

    let state_key = ColumnarWindowOperator::state_key(0, "a");
    assert!(backend
        .get("journaled-idle-cleanup-test", &state_key)
        .unwrap()
        .is_some());

    // No new window is ready at this watermark. Cleanup must still remove
    // the committed expired row instead of waiting for an unrelated fired
    // output to own a journal transaction.
    assert!(matches!(
        op.on_watermark(7_000).await.unwrap(),
        ProcessResult::None
    ));
    assert!(backend
        .get("journaled-idle-cleanup-test", &state_key)
        .unwrap()
        .is_none());
}

/// Legacy Stream windows are compatibility buffers, not aggregate
/// projections: the downstream processor must receive the original rows
/// and schema after the processing-time flush.
#[tokio::test]
async fn legacy_window_flush_preserves_input_payload_and_name() {
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let op = ColumnarWindowOperator::new(
        WindowOperatorConfig {
            kind: WindowKind::Tumbling { size_ms: 60_000 },
            timestamp_field: "__meta_timestamp".into(),
            key_field: "__arkflow_window_all".into(),
            value_fields: Vec::new(),
            trigger: WindowTrigger::ProcessingTime,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 0,
            legacy_payload: true,
            max_buffered_keys: default_max_buffered_keys(),
        },
        backend,
        "legacy-payload-test",
    );
    let mut input = crate::MessageBatch::new_arrow(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![3, 5])),
            ],
        )
        .unwrap(),
    );
    input.set_input_name(Some("legacy-source".into()));
    op.process(Arc::new(input)).await.unwrap();

    *op.last_processing_trigger_ms.lock().unwrap() = Some(crate::state::now_ms() as i64 - 2_000);
    let ProcessResult::SingleWithAck(flushed, _) = op.on_tick().await.unwrap() else {
        panic!("legacy processing-time window should flush");
    };
    assert_eq!(flushed.get_input_name(), Some("legacy-source".into()));
    assert_eq!(flushed.record_batch().schema().fields().len(), 2);
    assert!(flushed.record_batch().column_by_name("id").is_some());
    assert!(flushed.record_batch().column_by_name("amount").is_some());
    assert_eq!(flushed.record_batch().num_rows(), 2);
    let amounts = flushed
        .record_batch()
        .column_by_name("amount")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(amounts.values(), &[3, 5]);

    // A later processing-time flush must contain only the newly arrived
    // rows; the one-shot legacy buffer must not replay the first batch.
    let second = crate::MessageBatch::new_arrow(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["c"])),
                Arc::new(Int64Array::from(vec![7])),
            ],
        )
        .unwrap(),
    );
    op.process(Arc::new(second)).await.unwrap();
    *op.last_processing_trigger_ms.lock().unwrap() = Some(crate::state::now_ms() as i64 - 2_000);
    let ProcessResult::SingleWithAck(flushed, _) = op.on_tick().await.unwrap() else {
        panic!("legacy processing-time window should flush the second batch");
    };
    assert_eq!(flushed.record_batch().num_rows(), 1);
    let amounts = flushed
        .record_batch()
        .column_by_name("amount")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(amounts.values(), &[7]);
}

/// Rows with a nullable Float64 value column.
fn nullable_float_batch(
    rows: Vec<(i64, &str, Option<f64>)>,
    watermark: Option<i64>,
) -> MessageBatchRef {
    let mut fields = vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(I64::from(rows.iter().map(|row| row.0).collect::<Vec<_>>())),
        Arc::new(datafusion::arrow::array::StringArray::from(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
        )),
        Arc::new(datafusion::arrow::array::Float64Array::from(
            rows.iter().map(|row| row.2).collect::<Vec<_>>(),
        )),
    ];
    if let Some(watermark) = watermark {
        fields.push(Field::new("__watermark_ms", DataType::Int64, false));
        columns.push(Arc::new(I64::from(vec![watermark; rows.len()])));
    }
    Arc::new(crate::MessageBatch::new_arrow(
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
    ))
}

/// Regression: a window group whose rows all carried NULL values emits
/// no aggregate, but its delivery acknowledgement must still settle when
/// the window fires — otherwise the acknowledgement strands in
/// `pending_acks` forever, the source frontier freezes, and its journal
/// transaction leaks toward the pending bound.
#[tokio::test]
async fn empty_null_window_settles_its_source_acknowledgement() {
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap());
    let journal = Arc::new(super::super::state_journal::StateJournal::new(
        backend.clone(),
    ));
    let op = ColumnarWindowOperator::with_journal(
        WindowOperatorConfig {
            kind: WindowKind::Tumbling { size_ms: 1_000 },
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields: vec!["value".into()],
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 0,
            legacy_payload: false,
            max_buffered_keys: default_max_buffered_keys(),
        },
        backend.clone(),
        journal.clone(),
        "null-window-ack-test",
    );
    let source_ack = Arc::new(CountingAck {
        acked: AtomicUsize::new(0),
    });

    // A NULL-only group: the row buffers the window but never produces a
    // value, so the group can only be dropped at fire time.
    op.process_with_ack(
        nullable_float_batch(vec![(100, "a", None)], None),
        source_ack.clone() as Arc<dyn Ack>,
    )
    .await
    .unwrap();
    assert!(
        op.pending_acks
            .lock()
            .unwrap()
            .contains_key(&(0, "a".to_string())),
        "the delivery is held by the open window"
    );
    assert_eq!(journal.pending_transactions(), 1);

    // The watermark fires the window; the NULL-only group must settle its
    // delivery and discard its transaction instead of stranding them.
    let fired = op
        .process_with_ack(
            nullable_float_batch(vec![(2_000, "b", Some(1.0))], Some(1_000)),
            Arc::new(crate::input::NoopAck),
        )
        .await
        .unwrap();
    assert!(
        matches!(fired, ProcessResult::Deferred),
        "a NULL-only round emits no aggregate row"
    );

    assert_eq!(
        source_ack.acked.load(Ordering::Acquire),
        1,
        "the NULL-only window's delivery must be acknowledged"
    );
    assert!(
        !op.window_txns
            .lock()
            .unwrap()
            .contains_key(&(0, "a".to_string())),
        "the empty window's transaction must be discarded"
    );
    // The still-open window for key "b" keeps its staged transaction.
    assert_eq!(journal.pending_transactions(), 1);
    {
        let pending = op.pending_acks.lock().unwrap();
        assert!(
            !pending.contains_key(&(0, "a".to_string())),
            "the NULL-only window's acknowledgement must not strand"
        );
        // The open "b" window legitimately holds its delivery.
        assert!(pending.contains_key(&(2_000, "b".to_string())));
    }
}

/// A NULL value must not fabricate a count=0 zero-sentinel aggregate, and
/// a NULL-only buffer firing next to float aggregates must not truncate
/// the batch output kind back to Int64.
#[tokio::test]
async fn null_values_never_fabricate_or_truncate_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let op = operator(WindowTrigger::Watermark, backend);
    op.process(nullable_float_batch(
        vec![
            (1_000, "a", None),
            (2_000, "b", Some(1.5)),
            (3_000, "b", Some(1.0)),
        ],
        None,
    ))
    .await
    .unwrap();
    let fired = op
        .process(nullable_float_batch(
            vec![(11_000, "b", Some(2.0))],
            Some(10_000),
        ))
        .await
        .unwrap();
    let ProcessResult::Single(fired) = fired else {
        panic!("window should fire");
    };
    let keys = fired
        .record_batch()
        .column_by_name("key")
        .unwrap()
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let counts = fired
        .record_batch()
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let sums = fired
        .record_batch()
        .column_by_name("sum")
        .unwrap()
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(
        keys.len(),
        1,
        "the NULL-only key must not emit a phantom aggregate row"
    );
    assert_eq!(keys.value(0), "b");
    assert_eq!(counts.value(0), 2);
    assert_eq!(
        sums.value(0),
        2.5,
        "the float sum must not be truncated to an integer"
    );
    assert_eq!(
        fired
            .record_batch()
            .column_by_name("sum")
            .unwrap()
            .data_type(),
        &DataType::Float64,
    );
}

/// A row released by a source gate after the operator's watermark
/// frontier already fired and cleaned its window must not re-open the
/// window as a fresh aggregate (duplicate initial emission).
#[tokio::test]
async fn released_row_never_reopens_a_fired_and_cleaned_window() {
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
    let op = operator(WindowTrigger::Watermark, backend);
    op.process(batch(vec![(5_000, "a", 1)], None))
        .await
        .unwrap();
    // Watermark 60_000 fires [0, 10000) and holds [60000, 70000);
    // watermark 95_000 fires [60000, 70000); watermark 99_500 cleans
    // both (allowed_lateness_ms = 0), leaving only [90000, 100000).
    let fired = op
        .process(batch(vec![(61_000, "a", 2)], Some(60_000)))
        .await
        .unwrap();
    assert!(matches!(fired, ProcessResult::Single(_)));
    let fired = op
        .process(batch(vec![(96_000, "a", 3)], Some(95_000)))
        .await
        .unwrap();
    assert!(matches!(fired, ProcessResult::Single(_)));
    let cleaned = op
        .process(batch(vec![(99_600, "a", 4)], Some(99_500)))
        .await
        .unwrap();
    assert!(matches!(cleaned, ProcessResult::None));
    // The gate release race: an unmarked row for a closed window
    // arrives after the frontier moved past it. It must be treated as
    // a late membership, not re-open the window.
    let released = op
        .process(batch(vec![(5_000, "a", 7)], None))
        .await
        .unwrap();
    assert!(matches!(released, ProcessResult::None));
    let refired = op
        .process(batch(vec![(199_600, "a", 5)], Some(199_500)))
        .await
        .unwrap();
    let ProcessResult::Single(refired) = refired else {
        panic!("the held far-future window should fire");
    };
    let starts = refired
        .record_batch()
        .column_by_name("window_start")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for index in 0..starts.len() {
        assert_ne!(
            starts.value(index),
            0,
            "the fired-and-cleaned [0,10000) window must not be re-emitted"
        );
    }
}

#[cfg(test)]
mod sliding_enumeration_tests {
    use crate::executor::window::*;

    fn sliding_operator(
        size_ms: i64,
        slide_ms: i64,
        backend: Arc<dyn StateBackend>,
    ) -> ColumnarWindowOperator {
        ColumnarWindowOperator::new(
            WindowOperatorConfig {
                kind: WindowKind::Sliding { size_ms, slide_ms },
                timestamp_field: "ts".into(),
                key_field: "key".into(),
                value_fields: vec![],
                trigger: WindowTrigger::Watermark,
                trigger_interval_ms: 1_000,
                watermark_field: "__watermark_ms".into(),
                allowed_lateness_ms: 0,
                legacy_payload: false,
                max_buffered_keys: default_max_buffered_keys(),
            },
            backend,
            "sliding-enum-test",
        )
    }

    /// Task 4.4: a non-divisible sliding window (`size=5, slide=2`) assigns
    /// timestamp 4 to windows starting at 4, 2, and 0 — no containing start
    /// is omitted because of integer-division truncation.
    #[test]
    fn non_divisible_sliding_window_enumerates_every_containing_start() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let op = sliding_operator(5, 2, backend);
        // The enumeration starts at the latest containing start and steps
        // back; ordering within the assignment is irrelevant to the caller.
        let mut windows = op.windows_for(4);
        windows.sort();
        assert_eq!(
            windows,
            vec![(0, 5), (2, 7), (4, 9)],
            "timestamp 4 belongs to windows [0,5), [2,7), and [4,9)"
        );
        // Boundary cases: the start itself, the first row past a boundary,
        // and the last row of the containing chain.
        let mut windows = op.windows_for(0);
        windows.sort();
        assert_eq!(windows, vec![(-4, 1), (-2, 3), (0, 5)]);
        let mut windows = op.windows_for(5);
        windows.sort();
        // 5 is the exclusive end of [0,5): it belongs to the next chain only.
        assert_eq!(windows, vec![(2, 7), (4, 9)]);
        let mut windows = op.windows_for(9);
        windows.sort();
        // 9 is the exclusive end of [4,9).
        assert_eq!(windows, vec![(6, 11), (8, 13)]);
    }

    #[test]
    fn negative_timestamps_enumerate_aligned_containing_windows() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let op = sliding_operator(5, 2, backend);
        // -1: last start = floor(-1/2)*2 = -2. [-2,3) contains -1; [-4,1)
        // contains -1; [-6,-1) does not (end == -1 is exclusive).
        let mut windows = op.windows_for(-1);
        windows.sort();
        assert_eq!(windows, vec![(-4, 1), (-2, 3)]);
    }

    /// Divisible boundaries keep the classic behavior: size=10, slide=5,
    /// ts=6 belongs to [0,10) and [5,15).
    #[test]
    fn divisible_sliding_windows_keep_two_memberships() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let op = sliding_operator(10, 5, backend);
        let mut windows = op.windows_for(6);
        windows.sort();
        assert_eq!(windows, vec![(0, 10), (5, 15)]);
    }

    #[test]
    fn mixed_int_and_float_observations_keep_both_contributions() {
        // Per-batch JSON schema inference routinely makes the same field
        // Int64 in one delivery and Float64 in the next. A buffer that
        // observed both must fold the integer side into the widened
        // aggregate instead of dropping it (or fabricating a 0.0 boundary).
        let mut buffer = AggregateBuffer::default();
        buffer.observe_i64(100);
        buffer.observe_float(99.5, NumericKind::Float64);
        assert_eq!(buffer.count, 2);
        assert_eq!(buffer.kind, NumericKind::Float64);
        assert!((buffer.widened_sum() - 199.5).abs() < 1e-9);
        assert!((buffer.widened_min() - 99.5).abs() < 1e-9);
        assert!((buffer.widened_max() - 100.0).abs() < 1e-9);
    }

    /// Regression: an integer observation that follows a float one must seed
    /// the integer bounds from ITS OWN first value. Seeding from `count`
    /// instead takes the `else` branch against the `Default` zero and the
    /// widened aggregate then reports a fabricated boundary.
    #[test]
    fn integer_observation_after_float_does_not_fabricate_a_zero_boundary() {
        let mut buffer = AggregateBuffer::default();
        buffer.observe_float(99.5, NumericKind::Float64);
        buffer.observe_i64(100);
        assert_eq!(buffer.count, 2);
        assert_eq!(buffer.int_observations, 1);
        assert_eq!(buffer.float_observations, 1);
        assert_eq!(buffer.kind, NumericKind::Float64);
        assert!(
            (buffer.widened_min() - 99.5).abs() < 1e-9,
            "min must be 99.5, got {}",
            buffer.widened_min()
        );
        assert!(
            (buffer.widened_max() - 100.0).abs() < 1e-9,
            "max must be 100.0, got {}",
            buffer.widened_max()
        );

        // The symmetric case: all-negative floats then a negative integer.
        let mut negative = AggregateBuffer::default();
        negative.observe_float(-10.0, NumericKind::Float64);
        negative.observe_i64(-5);
        assert!(
            (negative.widened_max() - (-5.0)).abs() < 1e-9,
            "max must be -5.0, got {}",
            negative.widened_max()
        );
        assert!((negative.widened_min() - (-10.0)).abs() < 1e-9);
    }

    /// Regression: state written before the observation counters existed
    /// decodes with `count > 0` and zeroed counters. The restored range must
    /// survive the next observation instead of being replaced by it.
    #[test]
    fn restored_buffer_keeps_its_range_before_the_next_observation() {
        let persisted = serde_json::json!({
            "count": 2,
            "kind": "float64",
            "sum_i64": 0,
            "sum_float": 8.0,
            "min_i64": 0,
            "max_i64": 0,
            "min_float": 3.0,
            "max_float": 5.0,
            "emitted": false,
            "updated_since_emit": false,
            "session_end_ms": 0
        });
        let bytes = serde_json::to_vec(&persisted).unwrap();
        let mut buffer = decode_buffer(&bytes).unwrap();
        assert_eq!(buffer.float_observations, 2, "counters are back-filled");
        assert_eq!(buffer.count, 2);

        buffer.observe_float(100.0, NumericKind::Float64);
        assert!(
            (buffer.widened_min() - 3.0).abs() < 1e-9,
            "restored min must survive, got {}",
            buffer.widened_min()
        );
        assert!((buffer.widened_max() - 100.0).abs() < 1e-9);

        // A buffer whose counters already describe its state keeps them
        // exactly, including a mixed payload.
        let consistent = serde_json::json!({
            "count": 3,
            "kind": "float64",
            "sum_i64": 4,
            "sum_float": 1.5,
            "min_i64": 4,
            "max_i64": 4,
            "min_float": 0.5,
            "max_float": 1.0,
            "int_observations": 1,
            "float_observations": 2
        });
        let decoded = decode_buffer(&serde_json::to_vec(&consistent).unwrap()).unwrap();
        assert_eq!(
            (decoded.int_observations, decoded.float_observations),
            (1, 2)
        );
        assert!((decoded.widened_min() - 0.5).abs() < 1e-9);
        assert!((decoded.widened_max() - 4.0).abs() < 1e-9);
    }

    /// Regression: pre-counter state that observed BOTH representations must
    /// keep both. Detecting the integer side from the min/max it accumulated is
    /// what makes this work; a sum-based test would misread a float
    /// contribution that happens to sum to zero (`[-1.0, 1.0]`).
    #[test]
    fn restored_mixed_payload_keeps_both_representations() {
        let persisted = serde_json::json!({
            "count": 3,
            "kind": "float64",
            "sum_i64": 1000,
            "sum_float": 0.0,
            "min_i64": 1000,
            "max_i64": 1000,
            "min_float": -1.0,
            "max_float": 1.0
        });
        let decoded = decode_buffer(&serde_json::to_vec(&persisted).unwrap()).unwrap();
        assert_eq!(
            decoded.int_observations + decoded.float_observations,
            decoded.count,
            "the counters must describe the whole buffer"
        );
        assert!(
            decoded.int_observations > 0,
            "the integer side is evidenced"
        );
        assert!(
            decoded.float_observations > 0,
            "the float side is evidenced"
        );
        assert!((decoded.widened_min() - (-1.0)).abs() < 1e-9);
        assert!(
            (decoded.widened_max() - 1000.0).abs() < 1e-9,
            "the restored integer maximum must survive, got {}",
            decoded.widened_max()
        );
        assert!((decoded.widened_sum() - 1000.0).abs() < 1e-9);
    }

    /// Regression: a migrated legacy buffer must carry observation counters
    /// consistent with its count, so a later merge folds its contribution
    /// instead of dropping it, and an empty legacy buffer must not contribute
    /// its sentinel bounds to that merge.
    #[test]
    fn migrated_legacy_buffer_merges_its_contribution_and_empty_stays_neutral() {
        let legacy = serde_json::json!({
            "count": 2,
            "sum_i64": 10,
            "sum_float": 0.0,
            "min_i64": 4,
            "max_i64": 6,
            "is_float": false,
            "session_end_ms": 0
        });
        let migrated = decode_buffer(&serde_json::to_vec(&legacy).unwrap()).unwrap();
        assert_eq!(migrated.kind, NumericKind::Int64);
        assert_eq!(migrated.count, 2);
        assert_eq!(
            migrated.int_observations, 2,
            "counters describe the payload"
        );

        let mut merged = migrated;
        merged.observe_float(0.5, NumericKind::Float64);
        assert_eq!(merged.kind, NumericKind::Float64);
        assert!(
            (merged.widened_sum() - 10.5).abs() < 1e-9,
            "the migrated integer sum must survive the widening, got {}",
            merged.widened_sum()
        );
        assert!((merged.widened_min() - 0.5).abs() < 1e-9);
        assert!((merged.widened_max() - 6.0).abs() < 1e-9);

        // An empty legacy payload carries i64::MIN / i64::MAX placeholders; a
        // merge must not adopt them as a boundary.
        let empty_legacy = serde_json::json!({
            "count": 0,
            "sum_i64": 0,
            "sum_float": 0.0,
            "min_i64": i64::MIN,
            "max_i64": i64::MAX,
            "is_float": false,
            "session_end_ms": 0
        });
        let mut merged = decode_buffer(&serde_json::to_vec(&empty_legacy).unwrap()).unwrap();
        assert_eq!(merged.count, 0);
        assert_eq!(merged.min_i64, 0, "sentinels do not survive migration");
        merged.observe_i64(7);
        merged.observe_float(2.5, NumericKind::Float64);
        assert!((merged.widened_min() - 2.5).abs() < 1e-9);
        assert!((merged.widened_max() - 7.0).abs() < 1e-9);
    }

    #[test]
    fn merged_buffers_do_not_fabricate_boundaries_from_untouched_kinds() {
        let mut int_only = AggregateBuffer::default();
        int_only.observe_i64(-7);
        let mut float_only = AggregateBuffer::default();
        float_only.observe_float(2.5, NumericKind::Float64);
        int_only.merge(&float_only);
        assert_eq!(int_only.kind, NumericKind::Float64);
        assert!((int_only.widened_sum() - (-4.5)).abs() < 1e-9);
        assert!((int_only.widened_min() - (-7.0)).abs() < 1e-9);
        assert!((int_only.widened_max() - 2.5).abs() < 1e-9);
    }

    #[test]
    fn legacy_float_state_with_sentinel_min_max_fails_to_migrate() {
        // A REAL legacy payload written by the pre-typed kernel: every field
        // present (the typed parse would otherwise succeed with
        // `kind = Int64` and restore fabricated integer sentinels).
        let legacy = serde_json::json!({
            "count": 3,
            "sum_i64": 0,
            "sum_float": 7.5,
            "min_i64": i64::MIN,
            "max_i64": i64::MAX,
            "is_float": true,
            "session_end_ms": 0
        });
        let bytes = serde_json::to_vec(&legacy).unwrap();
        assert!(decode_buffer(&bytes).is_err());
        // Integer legacy aggregates still migrate losslessly.
        let legacy_int = serde_json::json!({
            "count": 2,
            "sum_i64": 9,
            "sum_float": 0.0,
            "min_i64": 4,
            "max_i64": 5,
            "is_float": false,
            "session_end_ms": 0
        });
        let bytes = serde_json::to_vec(&legacy_int).unwrap();
        let migrated = decode_buffer(&bytes).unwrap();
        assert_eq!(migrated.kind, NumericKind::Int64);
        assert_eq!(migrated.sum_i64, 9);
    }

    fn config_with(
        kind: WindowKind,
        legacy_payload: bool,
        value_fields: Vec<String>,
    ) -> WindowOperatorConfig {
        WindowOperatorConfig {
            kind,
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields,
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 0,
            legacy_payload,
            max_buffered_keys: default_max_buffered_keys(),
        }
    }

    #[test]
    fn validate_rejects_pathological_sliding_ratio() {
        let config = config_with(
            WindowKind::Sliding {
                size_ms: 315_360_000_000,
                slide_ms: 1,
            },
            false,
            vec!["value".into()],
        );
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("more than"), "{error}");
    }

    #[test]
    fn validate_rejects_legacy_sliding_and_multiple_value_fields() {
        let legacy_sliding = config_with(
            WindowKind::Sliding {
                size_ms: 10_000,
                slide_ms: 1_000,
            },
            true,
            vec!["value".into()],
        );
        assert!(legacy_sliding.validate().is_err());
        let multi_value = config_with(
            WindowKind::Tumbling { size_ms: 10_000 },
            false,
            vec!["a".into(), "b".into()],
        );
        assert!(multi_value.validate().is_err());
    }
}

/// Coverage-focused tests for the branches the behavioural suites leave
/// cold: aggregate-state normalization shapes, config validation and serde
/// defaults, marker helpers, legacy state decoding, timestamp/key/value
/// column conversions, and the acknowledgement/persistence failure paths.
#[cfg(test)]
mod coverage_gap_tests {
    use crate::executor::window::*;
    use datafusion::arrow::array::{
        BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int32Array,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt32Array,
    };
    use datafusion::arrow::ipc::writer::StreamWriter;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// Ack recording both settlement kinds.
    struct CountingSettlementAck {
        acked: AtomicUsize,
        aborted: AtomicUsize,
    }

    impl CountingSettlementAck {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                acked: AtomicUsize::new(0),
                aborted: AtomicUsize::new(0),
            })
        }

        fn acked(&self) -> usize {
            self.acked.load(Ordering::Acquire)
        }
    }

    #[async_trait]
    impl Ack for CountingSettlementAck {
        async fn ack(&self) -> Result<(), Error> {
            self.acked.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }

        async fn abort(&self) -> Result<(), Error> {
            self.aborted.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    /// Ack that fails exactly once, mirroring a transient source commit error.
    struct FailOnceAck {
        fail: AtomicBool,
    }

    #[async_trait]
    impl Ack for FailOnceAck {
        async fn ack(&self) -> Result<(), Error> {
            if self.fail.swap(false, Ordering::AcqRel) {
                Err(Error::Process("source acknowledgement failed".into()))
            } else {
                Ok(())
            }
        }
    }

    /// Ack whose abort always fails (compensation-failure paths).
    struct FailAbortAck;

    #[async_trait]
    impl Ack for FailAbortAck {
        async fn ack(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn abort(&self) -> Result<(), Error> {
            Err(Error::Process("abort failed (test)".into()))
        }
    }

    /// Backend wrapper whose writes fail while the flag is set, so the
    /// persistence error paths of every entry point can be exercised
    /// deterministically.
    struct FailingWritesBackend {
        inner: Arc<dyn StateBackend>,
        fail_writes: AtomicBool,
    }

    impl FailingWritesBackend {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap()),
                fail_writes: AtomicBool::new(false),
            })
        }

        fn set_failing(&self, failing: bool) {
            self.fail_writes.store(failing, Ordering::Release);
        }
    }

    impl StateBackend for FailingWritesBackend {
        fn format_version(&self) -> u32 {
            self.inner.format_version()
        }

        fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
            self.inner.get(namespace, key)
        }

        fn put_with_ttl(
            &self,
            namespace: &str,
            key: &[u8],
            value: &[u8],
            ttl_ms: Option<u64>,
            now_ms: u64,
        ) -> Result<(), Error> {
            if self.fail_writes.load(Ordering::Acquire) {
                return Err(Error::Process("state write failed (test)".into()));
            }
            self.inner
                .put_with_ttl(namespace, key, value, ttl_ms, now_ms)
        }

        fn update_i64(&self, namespace: &str, key: &[u8], delta: i64) -> Result<i64, Error> {
            self.inner.update_i64(namespace, key, delta)
        }

        fn delete(&self, namespace: &str, key: &[u8]) -> Result<bool, Error> {
            self.inner.delete(namespace, key)
        }

        fn purge_expired(&self, now_ms: u64) -> Result<u64, Error> {
            self.inner.purge_expired(now_ms)
        }

        fn scan(&self, namespace: &str) -> Result<Vec<crate::state::StateEntry>, Error> {
            self.inner.scan(namespace)
        }

        fn snapshot_at(&self, now_ms: u64) -> Result<crate::state::StateSnapshot, Error> {
            self.inner.snapshot_at(now_ms)
        }

        fn restore(&self, snapshot: &crate::state::StateSnapshot) -> Result<(), Error> {
            self.inner.restore(snapshot)
        }

        fn metrics(&self) -> Result<crate::state::StateMetrics, Error> {
            self.inner.metrics()
        }

        fn close(&self) -> Result<(), Error> {
            self.inner.close()
        }
    }

    fn mem_backend() -> Arc<dyn StateBackend> {
        Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap())
    }

    fn tumbling_config(size_ms: i64) -> WindowOperatorConfig {
        WindowOperatorConfig {
            kind: WindowKind::Tumbling { size_ms },
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields: vec!["value".into()],
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 0,
            legacy_payload: false,
            max_buffered_keys: default_max_buffered_keys(),
        }
    }

    fn session_config(gap_ms: i64, allowed_lateness_ms: u64) -> WindowOperatorConfig {
        WindowOperatorConfig {
            kind: WindowKind::Session { gap_ms },
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields: vec!["value".into()],
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms,
            legacy_payload: false,
            max_buffered_keys: default_max_buffered_keys(),
        }
    }

    fn sliding_config(size_ms: i64, slide_ms: i64) -> WindowOperatorConfig {
        WindowOperatorConfig {
            kind: WindowKind::Sliding { size_ms, slide_ms },
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields: vec![],
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 0,
            legacy_payload: false,
            max_buffered_keys: default_max_buffered_keys(),
        }
    }

    fn legacy_config(kind: WindowKind, key_field: &str) -> WindowOperatorConfig {
        WindowOperatorConfig {
            kind,
            timestamp_field: "__meta_timestamp".into(),
            key_field: key_field.into(),
            value_fields: vec![],
            trigger: WindowTrigger::ProcessingTime,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 0,
            legacy_payload: true,
            max_buffered_keys: default_max_buffered_keys(),
        }
    }

    /// Standard `(ts, key, value)` batch with an optional watermark column.
    fn std_batch(rows: Vec<(i64, &str, i64)>, watermark: Option<i64>) -> MessageBatchRef {
        flexible_batch(
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1.to_string()).collect::<Vec<_>>(),
            )),
            Some(Arc::new(Int64Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            ))),
            watermark,
            Vec::new(),
        )
    }

    /// Batch assembled from arbitrary column types plus optional markers.
    #[allow(clippy::type_complexity)]
    fn flexible_batch(
        ts: ArrayRef,
        key: ArrayRef,
        value: Option<ArrayRef>,
        watermark: Option<i64>,
        extra: Vec<(&'static str, ArrayRef)>,
    ) -> MessageBatchRef {
        let rows = ts.len();
        let mut fields = vec![
            Field::new("ts", ts.data_type().clone(), true),
            Field::new("key", key.data_type().clone(), true),
        ];
        let mut columns = vec![ts, key];
        if let Some(value) = value {
            fields.push(Field::new("value", value.data_type().clone(), true));
            columns.push(value);
        }
        for (name, column) in extra {
            fields.push(Field::new(name, column.data_type().clone(), true));
            columns.push(column);
        }
        if let Some(watermark) = watermark {
            fields.push(Field::new("__watermark_ms", DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(vec![watermark; rows])));
        }
        Arc::new(crate::MessageBatch::new_arrow(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
        ))
    }

    fn utf8(values: Vec<&str>) -> ArrayRef {
        Arc::new(StringArray::from(values))
    }

    fn fired_single(result: ProcessResult) -> MessageBatchRef {
        match result {
            ProcessResult::Single(batch) => batch,
            ProcessResult::SingleWithAck(batch, _) => batch,
            _ => panic!("expected a fired window output"),
        }
    }

    // ------------------------------------------------------------------
    // Aggregate buffer arithmetic and state normalization
    // ------------------------------------------------------------------

    #[test]
    fn merge_combines_float_bounds_and_widens_kinds() {
        let mut left = AggregateBuffer::default();
        left.observe_float(2.5, NumericKind::Float64);
        left.observe_float(1.5, NumericKind::Float64);
        let mut right = AggregateBuffer::default();
        right.observe_float(0.5, NumericKind::Float64);
        right.observe_float(3.5, NumericKind::Float64);
        left.merge(&right);
        assert_eq!(left.count, 4);
        assert_eq!(left.kind, NumericKind::Float64);
        assert!((left.widened_min() - 0.5).abs() < 1e-9);
        assert!((left.widened_max() - 3.5).abs() < 1e-9);
        assert!((left.widened_sum() - 8.0).abs() < 1e-9);

        let mut f32_pair = AggregateBuffer::default();
        f32_pair.observe_float(1.0, NumericKind::Float32);
        let mut f32_other = AggregateBuffer::default();
        f32_other.observe_float(2.0, NumericKind::Float32);
        f32_pair.merge(&f32_other);
        assert_eq!(f32_pair.kind, NumericKind::Float32);

        let mut widened = AggregateBuffer::default();
        widened.observe_float(1.0, NumericKind::Float32);
        let mut float64 = AggregateBuffer::default();
        float64.observe_float(2.0, NumericKind::Float64);
        widened.merge(&float64);
        assert_eq!(widened.kind, NumericKind::Float64);
    }

    #[test]
    fn observe_float_accepts_the_int64_kind_without_widening() {
        let mut buffer = AggregateBuffer::default();
        buffer.observe_float(1.5, NumericKind::Int64);
        assert_eq!(buffer.kind, NumericKind::Int64);
        assert_eq!(buffer.float_observations, 1);
        assert_eq!(buffer.count, 1);
        assert!((buffer.widened_sum() - 1.5).abs() < 1e-9);
    }

    fn decode_json(value: serde_json::Value) -> AggregateBuffer {
        decode_buffer(&serde_json::to_vec(&value).unwrap()).unwrap()
    }

    #[test]
    fn normalize_counters_repairs_every_legacy_payload_shape() {
        // A zero-count payload zeroes drifted counters.
        let zeroed = decode_json(serde_json::json!({
            "count": 0, "kind": "int64", "sum_i64": 0, "sum_float": 0.0,
            "min_i64": 0, "max_i64": 0, "int_observations": 2, "float_observations": 1
        }));
        assert_eq!(
            (zeroed.int_observations, zeroed.float_observations),
            (0, 0),
            "a countless buffer has no observations"
        );

        // An Int64-kind payload attributes everything to the integer side.
        let int_kind = decode_json(serde_json::json!({
            "count": 3, "kind": "int64", "sum_i64": 6, "sum_float": 0.0,
            "min_i64": 1, "max_i64": 2, "int_observations": 1
        }));
        assert_eq!(
            (int_kind.int_observations, int_kind.float_observations),
            (3, 0)
        );

        // A float-kind payload that named float observations attributes the
        // unnamed remainder to the integer side it accumulated.
        let named = decode_json(serde_json::json!({
            "count": 5, "kind": "float64", "sum_i64": 2, "sum_float": 3.0,
            "min_i64": 2, "max_i64": 4, "min_float": 1.0, "max_float": 2.0,
            "int_observations": 1, "float_observations": 1
        }));
        assert_eq!((named.int_observations, named.float_observations), (4, 1));

        // Evidence on both sides without any named counters reserves one
        // observation for the integer side.
        let evidenced = decode_json(serde_json::json!({
            "count": 4, "kind": "float64", "sum_i64": 3, "sum_float": 3.0,
            "min_i64": 3, "max_i64": 3, "min_float": 1.0, "max_float": 2.0
        }));
        assert_eq!(
            (evidenced.int_observations, evidenced.float_observations),
            (1, 3)
        );

        // A single integer-evidenced observation stays integer-only, and the
        // widened bounds report the integer side alone.
        let single = decode_json(serde_json::json!({
            "count": 1, "kind": "float64", "sum_i64": 3, "sum_float": 0.0,
            "min_i64": 3, "max_i64": 3
        }));
        assert_eq!((single.int_observations, single.float_observations), (1, 0));
        assert!((single.widened_min() - 3.0).abs() < 1e-9);
        assert!((single.widened_max() - 3.0).abs() < 1e-9);
        assert!((single.widened_sum() - 3.0).abs() < 1e-9);

        // Over-named counters collapse back to a consistent integer split.
        let over = decode_json(serde_json::json!({
            "count": 2, "kind": "float64", "sum_i64": 0, "sum_float": 3.0,
            "min_i64": 0, "max_i64": 0, "min_float": 1.0, "max_float": 2.0,
            "int_observations": 2, "float_observations": 2
        }));
        assert_eq!((over.int_observations, over.float_observations), (2, 0));
    }

    #[test]
    fn numeric_array_folds_values_into_the_requested_kind() {
        let values = vec![
            NumericValue::Int(3),
            NumericValue::Float(2.5, NumericKind::Float64),
        ];
        let ints = numeric_array(&values, NumericKind::Int64).unwrap();
        assert_eq!(
            ints.as_any().downcast_ref::<Int64Array>().unwrap().values(),
            &[3, 2]
        );
        let floats32 = numeric_array(&values, NumericKind::Float32).unwrap();
        let floats32 = floats32.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(floats32.value(0), 3.0);
        assert_eq!(floats32.value(1), 2.5);
        let floats64 = numeric_array(&values, NumericKind::Float64).unwrap();
        let floats64 = floats64.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(floats64.value(0), 3.0);
        assert_eq!(floats64.value(1), 2.5);
    }

    // ------------------------------------------------------------------
    // Config validation and serde defaults
    // ------------------------------------------------------------------

    #[test]
    fn validate_rejects_blank_fields_and_degenerate_arithmetic() {
        let mut config = tumbling_config(1_000);
        config.timestamp_field = "  ".into();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("timestamp_field"),
            "blank timestamp field"
        );

        let mut config = tumbling_config(1_000);
        config.key_field = String::new();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("key_field"),
            "blank key field"
        );

        let mut config = tumbling_config(1_000);
        config.trigger_interval_ms = 0;
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("trigger_interval_ms"),
            "zero trigger interval"
        );

        let mut config = tumbling_config(1_000);
        config.kind = WindowKind::Sliding {
            size_ms: 5,
            slide_ms: 10,
        };
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("slide_ms must not exceed size_ms"),
            "slide larger than size"
        );

        let config = tumbling_config(0);
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("tumbling"),
            "non-positive tumbling size"
        );

        let mut config = tumbling_config(1_000);
        config.kind = WindowKind::Sliding {
            size_ms: 0,
            slide_ms: 0,
        };
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("sliding"),
            "non-positive sliding arithmetic"
        );

        let mut config = tumbling_config(1_000);
        config.kind = WindowKind::Session { gap_ms: -5 };
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("session"),
            "non-positive session gap"
        );

        // Well-formed variants of every kind stay accepted.
        assert!(tumbling_config(1_000).validate().is_ok());
        assert!(sliding_config(10_000, 5_000).validate().is_ok());
        assert!(session_config(1_000, 0).validate().is_ok());
    }

    #[test]
    fn serde_defaults_fill_the_optional_window_fields() {
        // The kind is flattened with an internal tag: `kind` names the
        // variant and the variant's fields sit beside it.
        let config: WindowOperatorConfig = serde_json::from_str(
            r#"{"kind":"tumbling","size_ms":1000,"timestamp_field":"ts","key_field":"k"}"#,
        )
        .unwrap();
        assert_eq!(config.trigger, WindowTrigger::Watermark);
        assert_eq!(config.trigger_interval_ms, 5_000);
        assert_eq!(config.watermark_field, "__watermark_ms");
        assert!(config.value_fields.is_empty());
        assert_eq!(config.allowed_lateness_ms, 0);
        assert!(!config.legacy_payload);
        // Round trip keeps the enriched shape.
        let encoded = serde_json::to_string(&config).unwrap();
        let reparsed: WindowOperatorConfig = serde_json::from_str(&encoded).unwrap();
        assert_eq!(reparsed, config);
    }

    // ------------------------------------------------------------------
    // Batch marker helpers and acknowledgement plumbing
    // ------------------------------------------------------------------

    #[test]
    fn filter_window_batch_rejects_a_length_mismatch() {
        let batch = std_batch(vec![(1, "a", 1)], None);
        let Err(error) = filter_window_batch(&batch, &[]) else {
            panic!("a length mismatch must fail");
        };
        assert!(error.to_string().contains("length differs"), "{error}");
    }

    #[test]
    fn mark_late_session_batch_appends_replaces_and_validates() {
        let batch = std_batch(vec![(1, "a", 1)], None);

        // Fresh batch: the route marker is appended; without invalid rows the
        // invalid marker stays absent.
        let marked = mark_late_session_batch(batch.clone(), &[false]).unwrap();
        assert!(marked
            .record_batch()
            .column_by_name("__arkflow_late_event_route")
            .is_some());
        assert!(marked
            .record_batch()
            .column_by_name("__arkflow_invalid_timestamp_route")
            .is_none());

        // An invalid row adds the second marker.
        let marked = mark_late_session_batch(batch.clone(), &[true]).unwrap();
        let invalid = marked
            .record_batch()
            .column_by_name("__arkflow_invalid_timestamp_route")
            .and_then(|column| column.as_any().downcast_ref::<BooleanArray>())
            .unwrap();
        assert!(invalid.value(0));

        // Pre-existing marker columns are replaced in place instead of
        // duplicated.
        let premarked = flexible_batch(
            Arc::new(Int64Array::from(vec![1])),
            utf8(vec!["a"]),
            None,
            None,
            vec![
                (
                    "__arkflow_late_event_route",
                    Arc::new(BooleanArray::from(vec![false])),
                ),
                (
                    "__arkflow_invalid_timestamp_route",
                    Arc::new(BooleanArray::from(vec![false])),
                ),
            ],
        );
        let resealed = mark_late_session_batch(premarked, &[true]).unwrap();
        assert_eq!(resealed.record_batch().num_columns(), 4);
        let route = resealed
            .record_batch()
            .column_by_name("__arkflow_late_event_route")
            .and_then(|column| column.as_any().downcast_ref::<BooleanArray>())
            .unwrap();
        assert!(route.value(0));
        let invalid = resealed
            .record_batch()
            .column_by_name("__arkflow_invalid_timestamp_route")
            .and_then(|column| column.as_any().downcast_ref::<BooleanArray>())
            .unwrap();
        assert!(invalid.value(0));

        // A length mismatch is rejected.
        let Err(error) = mark_late_session_batch(batch, &[true, false]) else {
            panic!("a length mismatch must fail");
        };
        assert!(error.to_string().contains("length differs"), "{error}");
    }

    #[test]
    fn append_late_session_output_wraps_every_process_result_variant() {
        let late = (
            std_batch(vec![(1, "a", 1)], None),
            Arc::new(crate::input::NoopAck) as Arc<dyn Ack>,
        );

        // Single grows into a two-output acknowledgement group.
        let wrapped = append_late_session_output(
            ProcessResult::Single(std_batch(vec![(1, "a", 1)], None)),
            Some(late.clone()),
        );
        assert!(matches!(
            &wrapped,
            ProcessResult::MultipleWithAck(outputs) if outputs.len() == 2
        ));

        let wrapped = append_late_session_output(
            ProcessResult::Multiple(vec![std_batch(vec![(1, "a", 1)], None)]),
            Some(late.clone()),
        );
        assert!(matches!(
            &wrapped,
            ProcessResult::MultipleWithAck(outputs) if outputs.len() == 2
        ));

        let wrapped = append_late_session_output(
            ProcessResult::SingleWithAck(std_batch(vec![(1, "a", 1)], None), late.1.clone()),
            Some(late.clone()),
        );
        assert!(matches!(
            &wrapped,
            ProcessResult::MultipleWithAck(outputs) if outputs.len() == 2
        ));

        let wrapped = append_late_session_output(
            ProcessResult::MultipleWithAck(vec![late.clone()]),
            Some(late.clone()),
        );
        assert!(matches!(
            &wrapped,
            ProcessResult::MultipleWithAck(outputs) if outputs.len() == 2
        ));

        // Deferred and None have no main-path output: the late branch is the
        // only one.
        let wrapped = append_late_session_output(ProcessResult::Deferred, Some(late.clone()));
        assert!(matches!(
            &wrapped,
            ProcessResult::MultipleWithAck(outputs) if outputs.len() == 1
        ));
        let wrapped = append_late_session_output(ProcessResult::None, Some(late));
        assert!(matches!(
            &wrapped,
            ProcessResult::MultipleWithAck(outputs) if outputs.len() == 1
        ));

        // Without a late output the result passes through untouched.
        assert!(matches!(
            append_late_session_output(ProcessResult::Deferred, None),
            ProcessResult::Deferred
        ));
    }

    #[tokio::test]
    async fn compensate_window_acks_combines_error_reports() {
        // A successful compensation keeps the primary error.
        let error = compensate_window_acks(Error::Process("primary".into()), Vec::new()).await;
        assert!(error.to_string().contains("primary"), "{error}");

        // A failing compensation is reported alongside the primary error.
        let error = compensate_window_acks(
            Error::Process("primary".into()),
            vec![Arc::new(FailAbortAck)],
        )
        .await;
        let message = error.to_string();
        assert!(message.contains("primary"), "{message}");
        assert!(message.contains("compensation failed"), "{message}");
    }

    // ------------------------------------------------------------------
    // Buffer state decoding: JSON legacy envelopes and pre-IPC Arrow streams
    // ------------------------------------------------------------------

    #[test]
    fn decode_buffer_rejects_unknown_payloads() {
        assert!(decode_buffer(b"not-a-payload").is_err());
    }

    #[test]
    fn decode_buffer_falls_back_to_the_legacy_json_envelope() {
        // The typed V2 parse fails (required float fields are missing) while
        // the legacy envelope that only demands `count` still migrates.
        let migrated = decode_buffer(br#"{"count":1,"sum_i64":5}"#).unwrap();
        assert_eq!(migrated.count, 1);
        assert_eq!(migrated.sum_i64, 5);
        assert_eq!(migrated.kind, NumericKind::Int64);

        // A boolean is_float flag routes to the migration guard even when the
        // rest of the payload cannot be parsed as legacy state.
        assert!(decode_buffer(br#"{"is_float":true,"count":"not-a-number"}"#).is_err());
    }

    /// Serialize a one-row IPC stream with the pre-typed kernel's columns.
    fn legacy_ipc_stream(columns: Vec<ArrayRef>) -> Vec<u8> {
        let fields = columns
            .iter()
            .map(|column| Field::new("c", column.data_type().clone(), true))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let mut buffer = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buffer, batch.schema().as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        buffer
    }

    #[test]
    fn decode_buffer_reads_pre_ipc_arrow_stream_state() {
        let full = legacy_ipc_stream(vec![
            Arc::new(UInt64Array::from(vec![3u64])),   // count
            Arc::new(Int64Array::from(vec![6i64])),    // sum_i64
            Arc::new(Int64Array::from(vec![0i64])),    // unused
            Arc::new(Int64Array::from(vec![1i64])),    // min_i64
            Arc::new(Int64Array::from(vec![3i64])),    // max_i64
            Arc::new(BooleanArray::from(vec![false])), // is_float
            Arc::new(Int64Array::from(vec![42i64])),   // session_end_ms
        ]);
        let decoded = decode_buffer(&full).unwrap();
        assert_eq!(decoded.count, 3);
        assert_eq!(decoded.sum_i64, 6);
        assert_eq!(decoded.min_i64, 1);
        assert_eq!(decoded.max_i64, 3);
        assert_eq!(decoded.session_end_ms, 42);

        // Without the optional flag columns the payload still migrates as an
        // integer aggregate.
        let short = legacy_ipc_stream(vec![
            Arc::new(UInt64Array::from(vec![2u64])),
            Arc::new(Int64Array::from(vec![9i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![4i64])),
            Arc::new(Int64Array::from(vec![5i64])),
        ]);
        let decoded = decode_buffer(&short).unwrap();
        assert_eq!(decoded.count, 2);
        assert_eq!(decoded.sum_i64, 9);

        // A float payload written with sentinel bounds is unrecoverable.
        let float = legacy_ipc_stream(vec![
            Arc::new(UInt64Array::from(vec![2u64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![i64::MIN])),
            Arc::new(Int64Array::from(vec![i64::MAX])),
            Arc::new(BooleanArray::from(vec![true])),
        ]);
        assert!(decode_buffer(&float).is_err());
    }

    #[test]
    fn restore_buffers_rejects_truncated_state_keys() {
        let backend = mem_backend();
        backend
            .put(
                "corrupt-ns",
                b"short",
                &encode_buffer(&AggregateBuffer::default()).unwrap(),
            )
            .unwrap();
        let op = ColumnarWindowOperator::new(tumbling_config(1_000), backend, "corrupt-ns");
        let Err(error) = op.restore_buffers() else {
            panic!("a truncated state key must fail the restore");
        };
        assert!(
            error.to_string().contains("corrupt window state key"),
            "{error}"
        );
    }

    // ------------------------------------------------------------------
    // Window arithmetic helpers
    // ------------------------------------------------------------------

    #[test]
    fn windows_for_handles_legacy_groups_and_extreme_sliding_inputs() {
        let legacy_session = ColumnarWindowOperator::new(
            legacy_config(WindowKind::Session { gap_ms: 500 }, "__arkflow_window_all"),
            mem_backend(),
            "legacy-session",
        );
        assert_eq!(legacy_session.windows_for(123), vec![(0, 500)]);

        let legacy_tumbling = ColumnarWindowOperator::new(
            legacy_config(
                WindowKind::Tumbling { size_ms: 300 },
                "__arkflow_window_all",
            ),
            mem_backend(),
            "legacy-tumbling",
        );
        assert_eq!(legacy_tumbling.windows_for(999), vec![(0, 300)]);

        // Stepping back from the lowest aligned start underflows: the loop
        // stops after the single representable membership.
        let sliding =
            ColumnarWindowOperator::new(sliding_config(4, 2), mem_backend(), "sliding-floor");
        assert_eq!(
            sliding.windows_for(i64::MIN),
            vec![(i64::MIN, i64::MIN + 4)]
        );
    }

    #[test]
    fn session_end_falls_back_to_start_plus_gap_for_upgraded_buffers() {
        let upgraded = AggregateBuffer::default();
        assert_eq!(
            ColumnarWindowOperator::session_end(100, &upgraded, 50),
            150,
            "a pre-session-end buffer uses start + gap"
        );
        let extended = AggregateBuffer {
            session_end_ms: 999,
            ..Default::default()
        };
        assert_eq!(ColumnarWindowOperator::session_end(100, &extended, 50), 999);
    }

    // ------------------------------------------------------------------
    // Timestamp / key / value column conversion battery
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn timestamp_columns_of_every_supported_type_assign_windows() {
        let cases: Vec<(&str, ArrayRef)> = vec![
            (
                "timestamp_second",
                Arc::new(TimestampSecondArray::from(vec![2_i64])),
            ),
            (
                "timestamp_millisecond",
                Arc::new(TimestampMillisecondArray::from(vec![3_000_i64])),
            ),
            (
                "timestamp_microsecond",
                Arc::new(TimestampMicrosecondArray::from(vec![4_000_000_i64])),
            ),
            (
                "timestamp_nanosecond",
                Arc::new(TimestampNanosecondArray::from(vec![5_000_000_000_i64])),
            ),
            ("date32", Arc::new(Date32Array::from(vec![0_i32]))),
            ("date64", Arc::new(Date64Array::from(vec![6_000_i64]))),
            ("int32", Arc::new(Int32Array::from(vec![7_000_i32]))),
            ("uint32", Arc::new(UInt32Array::from(vec![8_000_u32]))),
        ];
        for (name, ts) in cases {
            let op = ColumnarWindowOperator::new(tumbling_config(10_000), mem_backend(), name);
            op.process(flexible_batch(
                ts,
                utf8(vec!["a"]),
                Some(Arc::new(Int64Array::from(vec![1_i64]))),
                None,
                Vec::new(),
            ))
            .await
            .unwrap_or_else(|error| panic!("{name}: {error}"));
            let fired = op
                .process(std_batch(vec![(20_000, "z", 0)], Some(10_000)))
                .await
                .unwrap();
            let fired = fired_single(fired);
            let starts = fired
                .record_batch()
                .column_by_name("window_start")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(starts.values(), &[0], "{name} assigns to [0, 10000)");
            let counts = fired
                .record_batch()
                .column_by_name("count")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            assert_eq!(counts.values(), &[1], "{name}");
        }
    }

    #[tokio::test]
    async fn unsupported_timestamp_inputs_fail_with_actionable_errors() {
        // Second-unit values that overflow milliseconds.
        let op = ColumnarWindowOperator::new(tumbling_config(10_000), mem_backend(), "sec-of");
        let result = op
            .process(flexible_batch(
                Arc::new(TimestampSecondArray::from(vec![i64::MAX])),
                utf8(vec!["a"]),
                None,
                None,
                Vec::new(),
            ))
            .await;
        assert!(result.unwrap_err().to_string().contains("overflows"));

        // Date32 days never overflow i64 milliseconds (i32::MAX days is well
        // below i64::MAX ms): the extreme value assigns normally.
        let op = ColumnarWindowOperator::new(tumbling_config(10_000), mem_backend(), "date-max");
        let out = op
            .process(flexible_batch(
                Arc::new(Date32Array::from(vec![i32::MAX])),
                utf8(vec!["a"]),
                Some(Arc::new(Int64Array::from(vec![1_i64]))),
                None,
                Vec::new(),
            ))
            .await
            .unwrap();
        assert!(matches!(out, ProcessResult::None));

        // A timestamp column of an unsupported type.
        let op = ColumnarWindowOperator::new(tumbling_config(10_000), mem_backend(), "ts-type");
        let result = op
            .process(flexible_batch(
                utf8(vec!["2024-01-01"]),
                utf8(vec!["a"]),
                None,
                None,
                Vec::new(),
            ))
            .await;
        assert!(result.unwrap_err().to_string().contains("unsupported type"));

        // A missing timestamp column in event-time mode.
        let batch = Arc::new(crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)])),
                vec![utf8(vec!["a"])],
            )
            .unwrap(),
        ));
        let op = ColumnarWindowOperator::new(tumbling_config(10_000), mem_backend(), "ts-miss");
        let result = op.process(batch).await;
        assert!(result.unwrap_err().to_string().contains("is missing"));
    }

    #[tokio::test]
    async fn integer_key_columns_are_cast_to_strings() {
        for (name, key) in [
            ("int64", Arc::new(Int64Array::from(vec![7_i64])) as ArrayRef),
            ("int32", Arc::new(Int32Array::from(vec![3_i32])) as ArrayRef),
        ] {
            let op = ColumnarWindowOperator::new(tumbling_config(10_000), mem_backend(), name);
            op.process(flexible_batch(
                Arc::new(Int64Array::from(vec![1_000_i64])),
                key,
                Some(Arc::new(Int64Array::from(vec![1_i64]))),
                None,
                Vec::new(),
            ))
            .await
            .unwrap();
            let fired = op
                .process(std_batch(vec![(20_000, "z", 0)], Some(10_000)))
                .await
                .unwrap();
            let fired = fired_single(fired);
            let keys = fired
                .record_batch()
                .column_by_name("key")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(keys.value(0), if name == "int64" { "7" } else { "3" });
        }

        // An unsupported key type and a missing key column both fail.
        let op = ColumnarWindowOperator::new(tumbling_config(10_000), mem_backend(), "key-type");
        let result = op
            .process(flexible_batch(
                Arc::new(Int64Array::from(vec![1_000_i64])),
                Arc::new(Float64Array::from(vec![1.5])),
                None,
                None,
                Vec::new(),
            ))
            .await;
        assert!(result.unwrap_err().to_string().contains("unsupported type"));

        let mut config = tumbling_config(10_000);
        config.key_field = "nope".into();
        let op = ColumnarWindowOperator::new(config, mem_backend(), "key-miss");
        let result = op.process(std_batch(vec![(1_000, "a", 1)], None)).await;
        assert!(result.unwrap_err().to_string().contains("key field"));
    }

    #[tokio::test]
    async fn narrow_integer_value_columns_aggregate_through_a_cast() {
        let op = ColumnarWindowOperator::new(tumbling_config(10_000), mem_backend(), "int-value");
        op.process(flexible_batch(
            Arc::new(Int64Array::from(vec![1_000_i64, 2_000_i64])),
            utf8(vec!["a", "a"]),
            Some(Arc::new(Int32Array::from(vec![5_i32, 7_i32]))),
            None,
            Vec::new(),
        ))
        .await
        .unwrap();
        let fired = op
            .process(std_batch(vec![(20_000, "z", 0)], Some(10_000)))
            .await
            .unwrap();
        let fired = fired_single(fired);
        let sums = fired
            .record_batch()
            .column_by_name("sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(sums.values(), &[12]);
    }

    #[tokio::test]
    async fn a_missing_value_field_fails_the_batch() {
        let op = ColumnarWindowOperator::new(tumbling_config(10_000), mem_backend(), "val-miss");
        let result = op
            .process(flexible_batch(
                Arc::new(Int64Array::from(vec![1_000_i64])),
                utf8(vec!["a"]),
                None,
                None,
                Vec::new(),
            ))
            .await;
        assert!(
            result.unwrap_err().to_string().contains("value field"),
            "the configured value column must exist"
        );
    }

    #[tokio::test]
    async fn repeated_watermark_columns_only_advance_the_frontier() {
        let op = ColumnarWindowOperator::new(tumbling_config(10_000), mem_backend(), "wm");
        op.process(std_batch(vec![(1_000, "a", 1)], Some(1_000)))
            .await
            .unwrap();
        op.process(std_batch(vec![(2_000, "a", 1)], Some(2_000)))
            .await
            .unwrap();
        // A stale watermark column never rewinds the frontier.
        op.process(std_batch(vec![(3_000, "a", 1)], Some(500)))
            .await
            .unwrap();
        assert_eq!(*op.watermark_ms.lock().unwrap(), Some(2_000));
    }

    // ------------------------------------------------------------------
    // Session late-event routing / dropping / invalid rows
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn session_rows_with_null_timestamps_are_counted_and_dropped() {
        let op = ColumnarWindowOperator::with_late_event_policy(
            session_config(1_000, 0),
            mem_backend(),
            "session-null-ts",
            LateEventPolicy::Update,
            false,
        );
        op.process(std_batch(vec![(100, "a", 1)], None))
            .await
            .unwrap();
        op.process(std_batch(vec![(3_000, "b", 2)], Some(2_500)))
            .await
            .unwrap();

        // A null timestamp can never reach a session deadline: it is counted
        // as invalid and dropped (no route configured).
        let out = op
            .process(flexible_batch(
                Arc::new(Int64Array::from(vec![Option::<i64>::None])),
                utf8(vec!["a"]),
                Some(Arc::new(Int64Array::from(vec![9_i64]))),
                None,
                Vec::new(),
            ))
            .await
            .unwrap();
        assert!(matches!(out, ProcessResult::None));
        assert_eq!(
            op.late_event_row_counter().load(Ordering::Relaxed),
            1,
            "the invalid row is counted"
        );
    }

    #[tokio::test]
    async fn routed_session_late_rows_share_the_delivery_with_fired_windows() {
        let op = ColumnarWindowOperator::with_late_event_policy(
            session_config(1_000, 0),
            mem_backend(),
            "session-route",
            LateEventPolicy::Route,
            true,
        );
        op.process(std_batch(vec![(100, "a", 1)], None))
            .await
            .unwrap();
        // Watermark 2_500 fires session [100, 1100) and leaves [3000, 4000)
        // open for key b.
        op.process(std_batch(vec![(3_000, "b", 2)], Some(2_500)))
            .await
            .unwrap();

        let ack = CountingSettlementAck::new();
        let out = op
            .process_with_ack(std_batch(vec![(100, "a", 9)], Some(4_000)), ack.clone())
            .await
            .unwrap();
        // The late row is routed to the side output while the same delivery's
        // watermark fires key b's session.
        let ProcessResult::MultipleWithAck(outputs) = out else {
            panic!("a routed session delivery combines window and late outputs");
        };
        assert_eq!(outputs.len(), 2);
        assert!(
            outputs[1]
                .0
                .record_batch()
                .column_by_name("__arkflow_late_event_route")
                .is_some(),
            "the late branch carries the route marker"
        );
        assert_eq!(
            op.late_event_row_counter().load(Ordering::Relaxed),
            1,
            "the routed row is counted"
        );
        for (_, output_ack) in outputs {
            output_ack.ack().await.unwrap();
        }
        assert_eq!(ack.acked(), 1, "the source delivery settles once");
    }

    #[tokio::test]
    async fn dropped_session_late_rows_settle_without_an_ack_flow() {
        let op = ColumnarWindowOperator::with_late_event_policy(
            session_config(1_000, 0),
            mem_backend(),
            "session-drop",
            LateEventPolicy::Drop,
            false,
        );
        op.process(std_batch(vec![(100, "a", 1)], None))
            .await
            .unwrap();
        op.process(std_batch(vec![(3_000, "b", 2)], Some(2_500)))
            .await
            .unwrap();

        // Nothing new fires: the dropped late row yields no output at all.
        let out = op
            .process(std_batch(vec![(100, "a", 9)], None))
            .await
            .unwrap();
        assert!(matches!(out, ProcessResult::None));

        // With a watermark that closes key b's session the aggregate fires
        // through the no-ack path.
        let out = op
            .process(std_batch(vec![(100, "a", 9)], Some(4_000)))
            .await
            .unwrap();
        assert!(matches!(out, ProcessResult::Single(_)));
    }

    #[tokio::test]
    async fn a_failing_dropped_late_ack_surfaces_the_error() {
        let op = ColumnarWindowOperator::with_late_event_policy(
            session_config(1_000, 0),
            mem_backend(),
            "session-drop-fail",
            LateEventPolicy::Drop,
            false,
        );
        op.process(std_batch(vec![(100, "a", 1)], None))
            .await
            .unwrap();
        op.process(std_batch(vec![(3_000, "b", 2)], Some(2_500)))
            .await
            .unwrap();

        let failing = Arc::new(FailOnceAck {
            fail: AtomicBool::new(true),
        });
        let result = op
            .process_with_ack(std_batch(vec![(100, "a", 9)], None), failing)
            .await;
        assert!(
            result.is_err(),
            "a failing drop acknowledgement must surface, not be swallowed"
        );
    }

    #[tokio::test]
    async fn an_accumulate_failure_after_late_rows_compensates_the_delivery() {
        let op = ColumnarWindowOperator::with_late_event_policy(
            session_config(1_000, 0),
            mem_backend(),
            "session-acc-fail",
            LateEventPolicy::Drop,
            false,
        );
        op.process(std_batch(vec![(100, "a", 1)], None))
            .await
            .unwrap();
        op.process(std_batch(vec![(3_000, "b", 2)], Some(2_500)))
            .await
            .unwrap();

        // One late row (dropped) plus one accepted row whose value column is
        // a string: the accumulate failure must compensate the whole split.
        let bad = flexible_batch(
            Arc::new(Int64Array::from(vec![100_i64, 5_000_i64])),
            utf8(vec!["a", "b"]),
            Some(utf8(vec!["x", "y"])),
            None,
            Vec::new(),
        );
        let result = op.process_with_ack(bad, CountingSettlementAck::new()).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unsupported numeric type"));
    }

    // ------------------------------------------------------------------
    // Gate-marker consumption inside accumulate
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn excluded_window_ends_never_reopen_a_cleaned_membership() {
        let op = ColumnarWindowOperator::new(tumbling_config(10_000), mem_backend(), "excl");
        op.process(std_batch(vec![(5_000, "a", 1)], None))
            .await
            .unwrap();
        // Fire and clean [0, 10000).
        op.process(std_batch(vec![(21_000, "z", 0)], Some(20_000)))
            .await
            .unwrap();

        // The gate marked the closed membership: the row must not re-open it.
        let marked = flexible_batch(
            Arc::new(Int64Array::from(vec![5_000_i64])),
            utf8(vec!["a"]),
            Some(Arc::new(Int64Array::from(vec![1_i64]))),
            Some(20_000),
            vec![(
                "__arkflow_late_window_ends",
                Arc::new(StringArray::from(vec![Some("10000")])),
            )],
        );
        let out = op.process(marked).await.unwrap();
        assert!(matches!(out, ProcessResult::None));
        assert!(!op
            .buffers
            .lock()
            .unwrap()
            .contains_key(&(0, "a".to_string())));
    }

    #[tokio::test]
    async fn targeted_late_updates_skip_only_their_marked_windows() {
        let mut config = tumbling_config(1_000);
        config.allowed_lateness_ms = 5_000;
        let op = ColumnarWindowOperator::new(config, mem_backend(), "targeted");
        op.process(std_batch(vec![(100, "a", 1)], None))
            .await
            .unwrap();
        // Fire [0, 1000) and retain it through the lateness deadline.
        op.process(std_batch(vec![(2_000, "b", 0)], Some(1_000)))
            .await
            .unwrap();
        // Past the deadline the retained buffer is cleaned up.
        op.process(std_batch(vec![(9_000, "z", 0)], Some(7_000)))
            .await
            .unwrap();

        // A targeted late update for the cleaned window must not fabricate a
        // fresh aggregate.
        let marked = flexible_batch(
            Arc::new(Int64Array::from(vec![200_i64])),
            utf8(vec!["a"]),
            Some(Arc::new(Int64Array::from(vec![5_i64]))),
            Some(7_000),
            vec![
                (
                    "__arkflow_late_event_update",
                    Arc::new(BooleanArray::from(vec![true])),
                ),
                (
                    "__arkflow_late_window_updates",
                    Arc::new(StringArray::from(vec![Some("1000")])),
                ),
            ],
        );
        let out = op.process(marked).await.unwrap();
        assert!(matches!(out, ProcessResult::None));
        assert!(!op
            .buffers
            .lock()
            .unwrap()
            .contains_key(&(0, "a".to_string())));
    }

    #[tokio::test]
    async fn routed_marker_batches_are_acknowledged_and_skipped() {
        let op = ColumnarWindowOperator::new(tumbling_config(10_000), mem_backend(), "route-pass");
        let route_batch = flexible_batch(
            Arc::new(Int64Array::from(vec![1_000_i64])),
            utf8(vec!["a"]),
            Some(Arc::new(Int64Array::from(vec![1_i64]))),
            None,
            vec![(
                "__arkflow_late_event_route",
                Arc::new(BooleanArray::from(vec![true])),
            )],
        );
        let ack = CountingSettlementAck::new();
        let out = op
            .process_with_ack(route_batch.clone(), ack.clone())
            .await
            .unwrap();
        assert!(matches!(out, ProcessResult::None));
        assert_eq!(ack.acked(), 1, "the routed delivery settles immediately");
        assert!(op.buffers.lock().unwrap().is_empty());

        // A failing acknowledgement surfaces after compensation.
        let failing = Arc::new(FailOnceAck {
            fail: AtomicBool::new(true),
        });
        assert!(op.process_with_ack(route_batch, failing).await.is_err());
    }

    // ------------------------------------------------------------------
    // Journal integration: re-keys, immediate commits, one-shot payloads
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn journaled_session_rekey_deletes_old_state_and_moves_pending_acks() {
        let backend = mem_backend();
        let journal = Arc::new(crate::executor::state_journal::StateJournal::new(
            backend.clone(),
        ));
        let op = ColumnarWindowOperator::with_journal(
            session_config(2_000, 0),
            backend.clone(),
            journal,
            "rekey-ns",
        );

        let first = CountingSettlementAck::new();
        let second = CountingSettlementAck::new();
        op.process_with_ack(std_batch(vec![(100, "a", 1)], None), first.clone())
            .await
            .unwrap();
        op.process_with_ack(std_batch(vec![(2_500, "a", 2)], None), second)
            .await
            .unwrap();
        assert!(op
            .pending_acks
            .lock()
            .unwrap()
            .contains_key(&(100, "a".to_string())));
        assert!(op
            .pending_acks
            .lock()
            .unwrap()
            .contains_key(&(2_500, "a".to_string())));

        // The bridging row merges both sessions into [100, 4500).
        op.process(std_batch(vec![(2_000, "a", 3)], None))
            .await
            .unwrap();
        {
            let pending = op.pending_acks.lock().unwrap();
            assert!(
                !pending.contains_key(&(2_500, "a".to_string())),
                "the merged-away session's delivery moved to the merged key"
            );
            assert_eq!(
                pending.get(&(100, "a".to_string())).map(Vec::len),
                Some(2),
                "both held deliveries live under the merged session"
            );
        }
        assert!(
            !op.window_txns
                .lock()
                .unwrap()
                .contains_key(&(2_500, "a".to_string())),
            "the obsolete transaction is discarded"
        );

        // The unacknowledged path commits immediately: the merged aggregate
        // is durable and the old key's bytes are gone.
        assert!(backend
            .get("rekey-ns", &ColumnarWindowOperator::state_key(2_500, "a"))
            .unwrap()
            .is_none());
        let merged = backend
            .get("rekey-ns", &ColumnarWindowOperator::state_key(100, "a"))
            .unwrap()
            .expect("the merged session is durable");
        let merged = decode_buffer(&merged).unwrap();
        assert_eq!(merged.count, 3);
    }

    #[tokio::test]
    async fn unacknowledged_deliveries_commit_their_window_transactions() {
        let backend = mem_backend();
        let journal = Arc::new(crate::executor::state_journal::StateJournal::new(
            backend.clone(),
        ));
        let op = ColumnarWindowOperator::with_journal(
            tumbling_config(1_000),
            backend.clone(),
            journal,
            "commit-ns",
        );
        op.process(std_batch(vec![(100, "a", 1)], None))
            .await
            .unwrap();
        // No acknowledgement flow gates the commit: the working buffer is
        // durable immediately.
        assert!(backend
            .get("commit-ns", &ColumnarWindowOperator::state_key(0, "a"))
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn journaled_legacy_flushes_its_one_shot_payload() {
        let backend = mem_backend();
        let journal = Arc::new(crate::executor::state_journal::StateJournal::new(
            backend.clone(),
        ));
        let op = ColumnarWindowOperator::with_journal(
            legacy_config(
                WindowKind::Tumbling { size_ms: 60_000 },
                "__arkflow_window_all",
            ),
            backend.clone(),
            journal,
            "legacy-journal-ns",
        );
        let input = crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("amount", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(StringArray::from(vec!["a"])),
                    Arc::new(Int64Array::from(vec![3])),
                ],
            )
            .unwrap(),
        );
        op.process_with_ack(
            Arc::new(input),
            Arc::new(crate::input::NoopAck) as Arc<dyn Ack>,
        )
        .await
        .unwrap();

        *op.last_processing_trigger_ms.lock().unwrap() =
            Some(crate::state::now_ms() as i64 - 2_000);
        let out = op.on_tick().await.unwrap();
        let ProcessResult::SingleWithAck(flushed, ack) = out else {
            panic!("the legacy buffer should flush");
        };
        ack.ack().await.unwrap();
        assert_eq!(flushed.record_batch().num_rows(), 1);
        // The one-shot buffer's state row is deleted with the commit.
        assert!(backend
            .get(
                "legacy-journal-ns",
                &ColumnarWindowOperator::state_key(0, "__all__")
            )
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn fired_ack_exposes_held_markers_undo_and_abort() {
        let backend = mem_backend();
        let journal = Arc::new(crate::executor::state_journal::StateJournal::new(
            backend.clone(),
        ));
        let op = ColumnarWindowOperator::with_journal(
            tumbling_config(1_000),
            backend,
            journal,
            "ack-surface-ns",
        );
        op.process_with_ack(
            std_batch(vec![(100, "a", 10)], None),
            Arc::new(crate::input::NoopAck),
        )
        .await
        .unwrap();
        let fired = op
            .process_with_ack(
                std_batch(vec![(2_000, "b", 0)], Some(1_000)),
                Arc::new(crate::input::NoopAck),
            )
            .await
            .unwrap();
        let ProcessResult::SingleWithAck(_, ack) = fired else {
            panic!("the window should fire with an acknowledgement");
        };

        // Held-marker plumbing forwards to the wrapped acknowledgement.
        ack.mark_held();
        ack.release_held();

        ack.ack().await.unwrap();
        // After a successful settlement the operation guard is released: the
        // compensation entry points take the lock again instead of reusing it.
        ack.undo().await.unwrap();
        ack.abort().await.unwrap();
    }

    // ------------------------------------------------------------------
    // Trigger cadence and delivery settlement shapes
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn watermark_only_deliveries_settle_or_ride_the_fired_output() {
        let op = ColumnarWindowOperator::new(tumbling_config(10_000), mem_backend(), "wm-only");
        op.process(std_batch(vec![(5_000, "a", 1)], None))
            .await
            .unwrap();
        // Fire [0, 10000) (retained through its deadline) and open
        // [60000, 70000) for key b.
        op.process(std_batch(vec![(61_000, "b", 2)], Some(60_000)))
            .await
            .unwrap();
        // A far-later watermark fires b's window and reclaims every expired
        // buffer, leaving only [90000, 100000) for key c.
        op.process(std_batch(vec![(96_000, "c", 3)], Some(95_000)))
            .await
            .unwrap();

        // A delivery whose only row targets the cleaned window: no row is
        // admitted and nothing fires, so the source acknowledgement settles
        // directly.
        let ack = CountingSettlementAck::new();
        let out = op
            .process_with_ack(std_batch(vec![(5_000, "a", 1)], Some(95_000)), ack.clone())
            .await
            .unwrap();
        assert!(matches!(out, ProcessResult::Deferred));
        assert_eq!(ack.acked(), 1, "the delivery settles immediately");

        // The same shape with a watermark that closes key c's window rides
        // the fired output's acknowledgement instead of settling eagerly.
        let ack = CountingSettlementAck::new();
        let out = op
            .process_with_ack(std_batch(vec![(5_000, "a", 1)], Some(100_000)), ack.clone())
            .await
            .unwrap();
        let ProcessResult::SingleWithAck(_, output_ack) = out else {
            panic!("the still-open window should fire");
        };
        assert_eq!(ack.acked(), 0, "the delivery settles with the output");
        output_ack.ack().await.unwrap();
        assert_eq!(ack.acked(), 1);
    }

    #[tokio::test]
    async fn processing_time_windows_flush_from_process_when_the_cadence_is_due() {
        let mut config = tumbling_config(10_000);
        config.trigger = WindowTrigger::ProcessingTime;
        let op = ColumnarWindowOperator::new(config, mem_backend(), "pt-cadence");

        // The first delivery starts the cadence without emitting.
        let out = op
            .process(std_batch(vec![(1_000, "a", 1)], None))
            .await
            .unwrap();
        assert!(matches!(out, ProcessResult::None));

        // Inside the interval nothing fires.
        let out = op
            .process(std_batch(vec![(2_000, "a", 2)], None))
            .await
            .unwrap();
        assert!(matches!(out, ProcessResult::None));

        // Past the interval the next delivery flushes everything it holds.
        *op.last_processing_trigger_ms.lock().unwrap() =
            Some(crate::state::now_ms() as i64 - 2_000);
        let out = op
            .process(std_batch(vec![(3_000, "a", 3)], None))
            .await
            .unwrap();
        let fired = fired_single(out);
        let counts = fired
            .record_batch()
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(counts.values(), &[3]);
    }

    #[tokio::test]
    async fn legacy_session_ticks_flush_only_after_the_activity_gap() {
        let op = ColumnarWindowOperator::new(
            legacy_config(
                WindowKind::Session { gap_ms: 1_000 },
                "__arkflow_window_all",
            ),
            mem_backend(),
            "legacy-session-tick",
        );
        let input = |amount: i64| {
            Arc::new(crate::MessageBatch::new_arrow(
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("id", DataType::Utf8, false),
                        Field::new("amount", DataType::Int64, false),
                    ])),
                    vec![
                        Arc::new(StringArray::from(vec!["a"])),
                        Arc::new(Int64Array::from(vec![amount])),
                    ],
                )
                .unwrap(),
            ))
        };
        op.process(input(3)).await.unwrap();

        // An immediate tick is inside the activity gap: nothing is due.
        let out = op.on_tick().await.unwrap();
        assert!(matches!(out, ProcessResult::None));

        // Once the gap passes the idle tick flushes the one-shot buffer.
        *op.last_processing_activity_ms.lock().unwrap() =
            Some(crate::state::now_ms() as i64 - 2_000);
        let out = op.on_tick().await.unwrap();
        let ProcessResult::SingleWithAck(flushed, _) = out else {
            panic!("the legacy session buffer should flush after the gap");
        };
        assert_eq!(flushed.record_batch().num_rows(), 1);
    }

    #[tokio::test]
    async fn legacy_session_null_keys_settle_through_the_late_path() {
        let op = ColumnarWindowOperator::new(
            legacy_config(WindowKind::Session { gap_ms: 1_000 }, "key"),
            mem_backend(),
            "legacy-late-path",
        );
        let batch = flexible_batch(
            Arc::new(Int64Array::from(vec![1_000_i64])),
            Arc::new(StringArray::from(vec![Option::<&str>::None])),
            None,
            None,
            Vec::new(),
        );
        let ack = CountingSettlementAck::new();
        let out = op
            .process_with_ack(batch, ack.clone() as Arc<dyn Ack>)
            .await
            .unwrap();
        // A NULL key row can never join a keyed aggregate: it settles as a
        // dropped invalid row and nothing flushes.
        assert!(matches!(out, ProcessResult::Deferred));
        assert!(ack.acked() >= 1, "the dropped branch settles");
        assert_eq!(
            op.late_event_row_counter().load(Ordering::Relaxed),
            1,
            "the NULL-key row is counted"
        );
    }

    #[tokio::test]
    async fn legacy_fire_without_retained_payloads_fails_loudly() {
        let op = ColumnarWindowOperator::new(
            legacy_config(
                WindowKind::Tumbling { size_ms: 60_000 },
                "__arkflow_window_all",
            ),
            mem_backend(),
            "legacy-empty-fire",
        );
        // Force the lazy backend load first so the injected buffer survives.
        op.restore_buffers().unwrap();
        op.buffers.lock().unwrap().insert(
            (0, "__all__".to_string()),
            AggregateBuffer {
                count: 3,
                ..Default::default()
            },
        );
        *op.last_processing_trigger_ms.lock().unwrap() =
            Some(crate::state::now_ms() as i64 - 2_000);
        let Err(error) = op.on_tick().await else {
            panic!("a legacy buffer without retained payloads must fail");
        };
        assert!(error.to_string().contains("unavailable"), "{error}");
    }

    // ------------------------------------------------------------------
    // Persistence failures surface from every entry point
    // ------------------------------------------------------------------

    /// A corrupted retained legacy payload makes every firing entry point
    /// fail: the error must surface (and roll the runtime back) wherever the
    /// fire is attempted from.
    #[tokio::test]
    async fn legacy_fire_errors_surface_from_every_entry_point() {
        let mut config = legacy_config(WindowKind::Tumbling { size_ms: 60_000 }, "key");
        config.trigger = WindowTrigger::Watermark;
        config.timestamp_field = "ts".into();
        let op = ColumnarWindowOperator::new(config, mem_backend(), "legacy-fire-err");
        // Load eagerly so the injected corrupt buffer survives.
        op.restore_buffers().unwrap();
        op.buffers.lock().unwrap().insert(
            (0, "__all__".to_string()),
            AggregateBuffer {
                count: 3,
                legacy_batches: vec![b"garbage".to_vec()],
                ..Default::default()
            },
        );

        // A watermark-driven delivery fails its firing round.
        let result = op
            .process_with_ack(
                flexible_batch(
                    Arc::new(Int64Array::from(vec![61_000_i64])),
                    utf8(vec!["a"]),
                    None,
                    Some(60_000),
                    Vec::new(),
                ),
                Arc::new(crate::input::NoopAck),
            )
            .await;
        assert!(result.is_err());

        // The watermark entry point fails the same way.
        assert!(op.on_watermark(60_000).await.is_err());
        // So does the end-of-stream flush.
        assert!(op.finish().await.is_err());

        // And the late-row split path (a NULL key) surfaces the same error
        // after compensating its acknowledgements.
        let result = op
            .process_with_ack(
                flexible_batch(
                    Arc::new(Int64Array::from(vec![1_000_i64])),
                    Arc::new(StringArray::from(vec![Option::<&str>::None])),
                    None,
                    Some(60_000),
                    Vec::new(),
                ),
                Arc::new(crate::input::NoopAck),
            )
            .await;
        assert!(result.is_err());
    }

    /// A processing-time operator that receives a late (NULL-key) row runs the
    /// cadence threshold through the late split instead of the direct path.
    #[tokio::test]
    async fn processing_time_late_rows_run_the_cadence_threshold() {
        let mut config = tumbling_config(10_000);
        config.trigger = WindowTrigger::ProcessingTime;
        let op = ColumnarWindowOperator::new(config, mem_backend(), "pt-late");
        let mixed = flexible_batch(
            Arc::new(Int64Array::from(vec![1_000_i64, 2_000_i64])),
            Arc::new(StringArray::from(vec![Some("a"), None])),
            Some(Arc::new(Int64Array::from(vec![1_i64, 5_i64]))),
            None,
            Vec::new(),
        );
        let ack = CountingSettlementAck::new();
        let out = op
            .process_with_ack(mixed, ack.clone() as Arc<dyn Ack>)
            .await
            .unwrap();
        // The first delivery starts the cadence: nothing fires, the accepted
        // row stays held and the NULL-key row is dropped and counted.
        assert!(matches!(out, ProcessResult::Deferred));
        assert_eq!(ack.acked(), 0, "the held window still owns the delivery");
        assert_eq!(
            op.late_event_row_counter().load(Ordering::Relaxed),
            1,
            "the NULL-key row is counted"
        );
        assert!(op
            .pending_acks
            .lock()
            .unwrap()
            .contains_key(&(0, "a".to_string())));
    }

    #[tokio::test]
    async fn persistence_failures_surface_from_every_entry_point() {
        let failing = FailingWritesBackend::new();

        // finish()
        let op =
            ColumnarWindowOperator::new(tumbling_config(1_000), failing.clone(), "finish-fail");
        op.process(std_batch(vec![(100, "a", 1)], None))
            .await
            .unwrap();
        failing.set_failing(true);
        assert!(op.finish().await.is_err());

        // on_watermark
        failing.set_failing(false);
        let op =
            ColumnarWindowOperator::new(tumbling_config(1_000), failing.clone(), "watermark-fail");
        op.process(std_batch(vec![(100, "a", 1)], None))
            .await
            .unwrap();
        failing.set_failing(true);
        assert!(op.on_watermark(1_000).await.is_err());

        // on_tick (processing-time cadence)
        failing.set_failing(false);
        let mut config = tumbling_config(1_000);
        config.trigger = WindowTrigger::ProcessingTime;
        let op = ColumnarWindowOperator::new(config, failing.clone(), "tick-fail");
        op.process(std_batch(vec![(100, "a", 1)], None))
            .await
            .unwrap();
        *op.last_processing_trigger_ms.lock().unwrap() =
            Some(crate::state::now_ms() as i64 - 2_000);
        failing.set_failing(true);
        assert!(op.on_tick().await.is_err());

        // process with an acknowledgement flow
        failing.set_failing(false);
        let op =
            ColumnarWindowOperator::new(tumbling_config(1_000), failing.clone(), "process-fail");
        op.process(std_batch(vec![(100, "a", 1)], None))
            .await
            .unwrap();
        failing.set_failing(true);
        assert!(op
            .process_with_ack(
                std_batch(vec![(2_000, "b", 0)], Some(1_000)),
                Arc::new(crate::input::NoopAck),
            )
            .await
            .is_err());

        // the late-only session path
        failing.set_failing(false);
        let op = ColumnarWindowOperator::with_late_event_policy(
            session_config(1_000, 0),
            failing.clone(),
            "late-only-fail",
            LateEventPolicy::Drop,
            false,
        );
        op.process(std_batch(vec![(100, "a", 1)], None))
            .await
            .unwrap();
        op.process(std_batch(vec![(3_000, "b", 2)], Some(2_500)))
            .await
            .unwrap();
        failing.set_failing(true);
        assert!(op
            .process_with_ack(
                std_batch(vec![(100, "a", 9)], None),
                Arc::new(crate::input::NoopAck),
            )
            .await
            .is_err());

        // the accepted-row session path
        failing.set_failing(false);
        let op = ColumnarWindowOperator::with_late_event_policy(
            session_config(1_000, 0),
            failing.clone(),
            "late-accepted-fail",
            LateEventPolicy::Drop,
            false,
        );
        op.process(std_batch(vec![(100, "a", 1)], None))
            .await
            .unwrap();
        op.process(std_batch(vec![(3_000, "b", 2)], Some(2_500)))
            .await
            .unwrap();
        failing.set_failing(true);
        assert!(op
            .process_with_ack(
                std_batch(vec![(100, "a", 9), (5_000, "b", 1)], None),
                Arc::new(crate::input::NoopAck),
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn an_all_null_watermark_column_leaves_the_frontier_untouched() {
        let op =
            ColumnarWindowOperator::new(tumbling_config(10_000), mem_backend(), "null-watermark");
        let batch = flexible_batch(
            Arc::new(Int64Array::from(vec![1_000i64, 2_000i64])),
            utf8(vec!["a", "b"]),
            Some(Arc::new(Int64Array::from(vec![1i64, 2i64]))),
            None,
            vec![(
                "__watermark_ms",
                Arc::new(Int64Array::from(vec![None::<i64>, None::<i64>])) as ArrayRef,
            )],
        );
        let out = op.process(batch).await.unwrap();
        assert!(matches!(out, ProcessResult::None));
        assert_eq!(
            *op.watermark_ms.lock().unwrap(),
            None,
            "a null-only watermark column must not move the frontier"
        );

        // A watermark column of a non-Int64 type is ignored the same way.
        let op = ColumnarWindowOperator::new(
            tumbling_config(10_000),
            mem_backend(),
            "null-watermark-int32",
        );
        let batch = flexible_batch(
            Arc::new(Int64Array::from(vec![1_000i64])),
            utf8(vec!["a"]),
            Some(Arc::new(Int64Array::from(vec![1i64]))),
            None,
            vec![(
                "__watermark_ms",
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![500i32])) as ArrayRef,
            )],
        );
        let out = op.process(batch).await.unwrap();
        assert!(matches!(out, ProcessResult::None));
        assert_eq!(
            *op.watermark_ms.lock().unwrap(),
            None,
            "a non-Int64 watermark column must not move the frontier"
        );
    }

    #[tokio::test]
    async fn null_timestamp_rows_are_skipped_inside_the_accumulator() {
        let op = ColumnarWindowOperator::new(tumbling_config(10_000), mem_backend(), "null-ts");
        // The tumbling/watermark path has no session gate and the key is
        // present, so the NULL timestamp row survives into the accumulator
        // and must be skipped there rather than failing the batch.
        let batch = flexible_batch(
            Arc::new(Int64Array::from(vec![None::<i64>, Some(1_000i64)])),
            utf8(vec!["a", "a"]),
            Some(Arc::new(Int64Array::from(vec![Some(1i64), Some(2i64)]))),
            None,
            Vec::new(),
        );
        let out = op.process(batch).await.unwrap();
        assert!(matches!(out, ProcessResult::None));
        let buffers = op.buffers.lock().unwrap();
        let buffer = buffers
            .get(&(0, "a".to_string()))
            .expect("the timestamped row aggregated");
        assert_eq!(buffer.count, 1);
        assert_eq!(buffer.sum_i64, 2);
    }

    #[tokio::test]
    async fn a_late_update_row_admits_its_unmarked_window_memberships() {
        let op = ColumnarWindowOperator::new(tumbling_config(10_000), mem_backend(), "late-admit");
        // The update marker names a window this row does NOT belong to: the
        // row's own membership must still be admitted even though no buffer
        // exists for it yet.
        let batch = flexible_batch(
            Arc::new(Int64Array::from(vec![1_000i64])),
            utf8(vec!["a"]),
            Some(Arc::new(Int64Array::from(vec![7i64]))),
            Some(20_000),
            vec![
                (
                    "__arkflow_late_event_update",
                    Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
                ),
                (
                    "__arkflow_late_window_updates",
                    Arc::new(StringArray::from(vec!["30000"])) as ArrayRef,
                ),
            ],
        );
        let out = op.process(batch).await.unwrap();
        let fired = match out {
            ProcessResult::Single(batch) => batch,
            _ => panic!("the admitted membership must fire"),
        };
        let counts = fired
            .record_batch()
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(counts.values(), &[1]);
    }

    #[tokio::test]
    async fn a_route_marker_delivery_acknowledges_and_returns_nothing() {
        let op = ColumnarWindowOperator::new(session_config(1_000, 0), mem_backend(), "route-ack");
        let batch = flexible_batch(
            Arc::new(Int64Array::from(vec![1_000i64])),
            utf8(vec!["a"]),
            None,
            None,
            vec![(
                "__arkflow_late_event_route",
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            )],
        );
        let ack = CountingSettlementAck::new();
        let out = op
            .process_with_ack(batch, ack.clone() as Arc<dyn Ack>)
            .await
            .unwrap();
        assert!(matches!(out, ProcessResult::None));
        assert_eq!(
            ack.acked(),
            1,
            "the route delivery is acknowledged and skipped"
        );

        // Without an acknowledgement flow the marker batch simply passes.
        let marker_only = flexible_batch(
            Arc::new(Int64Array::from(vec![1_000i64])),
            utf8(vec!["a"]),
            None,
            None,
            vec![(
                "__arkflow_late_event_route",
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            )],
        );
        let out = op.process(marker_only).await.unwrap();
        assert!(matches!(out, ProcessResult::None));
    }

    #[tokio::test]
    async fn legacy_tumbling_late_rows_take_the_legacy_cadence_arm() {
        let op = ColumnarWindowOperator::new(
            legacy_config(WindowKind::Tumbling { size_ms: 10_000 }, "key"),
            mem_backend(),
            "legacy-late",
        );
        // The NULL-key row drives the late split; the legacy tumbling trigger
        // arm records the first processing-time activity without firing.
        let batch = flexible_batch(
            Arc::new(Int64Array::from(vec![1_000i64, 2_000i64])),
            Arc::new(StringArray::from(vec![Some("a"), None])),
            None,
            None,
            Vec::new(),
        );
        let ack = CountingSettlementAck::new();
        let out = op
            .process_with_ack(batch, ack.clone() as Arc<dyn Ack>)
            .await
            .unwrap();
        assert!(matches!(out, ProcessResult::Deferred));
        assert!(
            op.buffers
                .lock()
                .unwrap()
                .contains_key(&(0, "a".to_string())),
            "the accepted row keyed the synthetic legacy group"
        );
    }

    #[tokio::test]
    async fn processing_time_late_rows_fire_once_the_cadence_elapses() {
        let mut config = tumbling_config(10_000);
        config.trigger = WindowTrigger::ProcessingTime;
        config.trigger_interval_ms = 1;
        let op = ColumnarWindowOperator::new(config, mem_backend(), "pt-late");
        let delivery = |ts: i64| {
            flexible_batch(
                Arc::new(Int64Array::from(vec![ts, ts])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
                Some(Arc::new(Int64Array::from(vec![1i64, 1i64]))),
                None,
                Vec::new(),
            )
        };
        let ack = CountingSettlementAck::new();
        let first = op
            .process_with_ack(delivery(1_000), ack.clone() as Arc<dyn Ack>)
            .await
            .unwrap();
        assert!(
            matches!(first, ProcessResult::Deferred),
            "the first delivery starts the cadence"
        );
        // Tolerate instrumentation slowdown: the 1ms cadence has a 20ms
        // margin before the second delivery.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let second = op
            .process_with_ack(delivery(2_000), ack.clone() as Arc<dyn Ack>)
            .await
            .unwrap();
        assert!(
            matches!(second, ProcessResult::SingleWithAck(_, _)),
            "the due cadence flushes the open window"
        );
    }

    #[tokio::test]
    async fn a_late_path_delivery_settles_a_dropped_empty_group() {
        let op = ColumnarWindowOperator::with_late_event_policy(
            session_config(10_000, 0),
            mem_backend(),
            "late-empty-drop",
            LateEventPolicy::Drop,
            false,
        );
        // First delivery: a NULL-value row opens an empty session window and
        // parks its acknowledgement there (no watermark yet, nothing fires).
        let first = flexible_batch(
            Arc::new(Int64Array::from(vec![0i64])),
            utf8(vec!["a"]),
            Some(Arc::new(Int64Array::from(vec![None::<i64>]))),
            None,
            Vec::new(),
        );
        let ack1 = CountingSettlementAck::new();
        assert!(matches!(
            op.process_with_ack(first, ack1.clone() as Arc<dyn Ack>)
                .await
                .unwrap(),
            ProcessResult::Deferred
        ));
        assert_eq!(ack1.acked(), 0);
        // Second delivery: a NULL-key row drives the late split while the
        // watermark makes the empty session window droppable; its parked
        // acknowledgement must settle through the dropped-group path.
        let second = flexible_batch(
            Arc::new(Int64Array::from(vec![9_000i64])),
            Arc::new(StringArray::from(vec![Option::<&str>::None])),
            None,
            Some(10_000),
            Vec::new(),
        );
        let ack2 = CountingSettlementAck::new();
        assert!(matches!(
            op.process_with_ack(second, ack2.clone() as Arc<dyn Ack>)
                .await
                .unwrap(),
            ProcessResult::Deferred
        ));
        assert_eq!(
            ack1.acked(),
            1,
            "the dropped empty group settled its parked delivery"
        );
        assert!(
            op.buffers.lock().unwrap().is_empty(),
            "the empty session window was dropped"
        );
    }

    #[tokio::test]
    async fn a_touchless_late_path_delivery_still_acknowledges_the_source() {
        let op = ColumnarWindowOperator::new(tumbling_config(10_000), mem_backend(), "touchless");
        // The NULL-key row drives the late split; the other row's window
        // already ended behind the watermark with no retained buffer, so the
        // admission guard skips it and the delivery touches no window at all.
        let batch = flexible_batch(
            Arc::new(Int64Array::from(vec![1_000i64, 5_000i64])),
            Arc::new(StringArray::from(vec![Some("b"), None])),
            Some(Arc::new(Int64Array::from(vec![1i64, 1i64]))),
            Some(10_000),
            Vec::new(),
        );
        let ack = CountingSettlementAck::new();
        let out = op
            .process_with_ack(batch, ack.clone() as Arc<dyn Ack>)
            .await
            .unwrap();
        assert!(matches!(out, ProcessResult::Deferred));
        assert_eq!(
            ack.acked(),
            1,
            "a delivery that touches no window still settles"
        );
    }

    #[test]
    fn decode_buffer_rejects_unusable_and_empty_ipc_streams() {
        // An IPC stream whose count column is not UInt64 cannot migrate.
        let bad_count = legacy_ipc_stream(vec![
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(Int64Array::from(vec![1i64])),
        ]);
        assert!(decode_buffer(&bad_count).is_err());
        // A UInt64 count beside a non-Int64 sum column fails the same way.
        let bad_sum = legacy_ipc_stream(vec![
            Arc::new(UInt64Array::from(vec![1u64])),
            Arc::new(UInt64Array::from(vec![1u64])),
        ]);
        assert!(decode_buffer(&bad_sum).is_err());
        // A schema-only IPC stream carries no batch to migrate.
        let empty = {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "count",
                DataType::UInt64,
                true,
            )]));
            let mut buffer = Vec::new();
            let mut writer = StreamWriter::try_new(&mut buffer, schema.as_ref()).unwrap();
            writer.finish().unwrap();
            buffer
        };
        assert!(decode_buffer(&empty).is_err());
        // A stream truncated mid-batch fails the batch read.
        let full = legacy_ipc_stream(vec![
            Arc::new(UInt64Array::from(vec![1u64])),
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
        ]);
        let truncated = &full[..full.len() / 2];
        assert!(decode_buffer(truncated).is_err());
    }

    #[tokio::test]
    async fn coverage_doubles_expose_working_trait_defaults() {
        let abort_ack = FailAbortAck;
        abort_ack.ack().await.unwrap();
        assert!(abort_ack.abort().await.is_err());

        let backend = FailingWritesBackend::new();
        assert_eq!(backend.format_version(), 1);
        assert_eq!(backend.update_i64("ns", b"counter", 1).unwrap(), 1);
        assert_eq!(backend.purge_expired(0).unwrap(), 0);
        let _ = backend.scan("ns").unwrap();
        let snapshot = backend.snapshot_at(0).unwrap();
        assert!(snapshot.verify());
        backend.restore(&backend.snapshot_at(0).unwrap()).unwrap();
        let _ = backend.metrics().unwrap();
        backend.close().unwrap();

        let batch = Arc::new(crate::MessageBatch::new_arrow(RecordBatch::new_empty(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
        )));
        assert_eq!(fired_single(ProcessResult::Single(batch.clone())).len(), 0);
        assert_eq!(
            fired_single(ProcessResult::SingleWithAck(
                batch,
                Arc::new(crate::input::NoopAck) as Arc<dyn Ack>
            ))
            .len(),
            0
        );
    }
}

/// Batch whose value column is Int32 (narrow integer) with mixed nulls —
/// the column kind that used to be re-cast to Int64 per row inside
/// `accumulate`, making wide batches quadratic.
fn int32_value_batch(
    rows: Vec<(i64, &str, Option<i32>)>,
    watermark: Option<i64>,
) -> MessageBatchRef {
    use datafusion::arrow::array::Int32Array;
    let mut fields = vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int32, true),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(I64::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.1.to_string()).collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(
            rows.iter().map(|r| r.2).collect::<Vec<_>>(),
        )),
    ];
    if let Some(watermark) = watermark {
        fields.push(Field::new("__watermark_ms", DataType::Int64, false));
        columns.push(Arc::new(I64::from(vec![watermark; rows.len()])));
    }
    Arc::new(crate::MessageBatch::new_arrow(
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
    ))
}

/// Batch with the same logical rows but an Int64 value column, as the
/// equivalence reference for the narrow-integer path.
fn int64_value_batch(
    rows: Vec<(i64, &str, Option<i32>)>,
    watermark: Option<i64>,
) -> MessageBatchRef {
    let mut fields = vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, true),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(I64::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.1.to_string()).collect::<Vec<_>>(),
        )),
        Arc::new(I64::from(
            rows.iter().map(|r| r.2.map(i64::from)).collect::<Vec<_>>(),
        )),
    ];
    if let Some(watermark) = watermark {
        fields.push(Field::new("__watermark_ms", DataType::Int64, false));
        columns.push(Arc::new(I64::from(vec![watermark; rows.len()])));
    }
    Arc::new(crate::MessageBatch::new_arrow(
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
    ))
}

fn fired_counts_sums(fired: &crate::MessageBatch) -> (Vec<u64>, Vec<i64>) {
    let counts = fired
        .record_batch()
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let sums = fired
        .record_batch()
        .column_by_name("sum")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (counts.values().to_vec(), sums.values().to_vec())
}

/// Narrow-integer value columns normalize once per batch: aggregates match
/// the Int64 path exactly (nulls skipped), and a large batch's cost stays
/// linear — the per-row full-column cast made it quadratic.
#[tokio::test]
async fn narrow_int_value_columns_match_int64_and_stay_linear() {
    let rows = vec![
        (1_000, "a", Some(1)),
        (2_000, "a", None),
        (3_000, "b", Some(3)),
        (4_000, "b", Some(7)),
    ];

    let backend: Arc<dyn StateBackend> =
        Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap());
    let narrow = operator(WindowTrigger::Watermark, backend);
    narrow
        .process(int32_value_batch(rows.clone(), None))
        .await
        .unwrap();
    let fired_narrow = narrow
        .process(int32_value_batch(
            vec![(11_000, "a", Some(5))],
            Some(10_000),
        ))
        .await
        .unwrap();
    let ProcessResult::Single(fired_narrow) = fired_narrow else {
        panic!("narrow-int window should fire");
    };
    // (window 0, key a): 1 non-null observation; (0, b): 2 observations.
    let narrow_stats = fired_counts_sums(&fired_narrow);

    let backend64: Arc<dyn StateBackend> =
        Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap());
    let wide = operator(WindowTrigger::Watermark, backend64);
    wide.process(int64_value_batch(rows, None)).await.unwrap();
    let fired_wide = wide
        .process(int64_value_batch(
            vec![(11_000, "a", Some(5))],
            Some(10_000),
        ))
        .await
        .unwrap();
    let ProcessResult::Single(fired_wide) = fired_wide else {
        panic!("int64 window should fire");
    };
    assert_eq!(narrow_stats, fired_counts_sums(&fired_wide));
    assert_eq!(narrow_stats.0, vec![1, 2], "null values are skipped");
    assert_eq!(narrow_stats.1, vec![1, 10]);

    // Linear-cost guard. The timed rows target FRESH windows beyond the
    // watermark so every row is admitted — rows landing in the already
    // fired window would take the late-skip path and never reach
    // accumulation. The ratio form is machine-independent: linear
    // accumulation costs ~4x for 4x rows (20k vs 5k, one window each),
    // while the old per-membership full-column cast made it ~16x
    // (quadratic). The absolute cap stays as a belt-and-suspenders bound.
    let small_rows: Vec<(i64, &str, Option<i32>)> = (0..5_000)
        .map(|i| (100_000 + (i % 100), "g", Some((i % 1000) as i32)))
        .collect();
    let big_rows: Vec<(i64, &str, Option<i32>)> = (0..20_000)
        .map(|i| (200_000 + (i % 100), "h", Some((i % 1000) as i32)))
        .collect();
    let start = std::time::Instant::now();
    narrow
        .process(int32_value_batch(small_rows, None))
        .await
        .unwrap();
    let small_elapsed = start.elapsed();
    let start = std::time::Instant::now();
    narrow
        .process(int32_value_batch(big_rows, None))
        .await
        .unwrap();
    let big_elapsed = start.elapsed();
    assert!(
        big_elapsed.as_secs() < 10,
        "20k-row Int32 batch took {big_elapsed:?}; narrow-int normalization must stay per-batch, not per-row"
    );
    assert!(
        big_elapsed <= small_elapsed * 10,
        "20k rows took {big_elapsed:?} vs 5k rows {small_elapsed:?}; \
         linear cost expects ~4x — the per-row full-column cast was quadratic"
    );
}
