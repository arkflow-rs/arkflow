//! Job-wide resource lifecycle guard for the unified kernel.
//!
//! All resources a Job needs — temporary stores, sources, sinks, and state
//! backends — are connected in dependency order BEFORE any task event loop
//! spawns, so a processor's first `get` cannot race a temporary's `connect`
//! and input consumption cannot start while a sink is still down. A failure
//! part-way through the startup closes everything already connected in
//! reverse order (an open redb WAL or Kafka consumer must not leak its
//! exclusive handle). Shutdown closes in the same reverse order and is
//! idempotent.

use crate::input::Input;
use crate::output::Output;
use crate::state::StateBackend;
use crate::temporary::Temporary;
use crate::Error;
use std::sync::Arc;
use std::sync::Mutex;

/// One connected resource, closed in reverse connection order.
enum Guarded {
    Temporary {
        name: String,
        temporary: Arc<dyn Temporary>,
    },
    Source(Arc<dyn Input>),
    Sink(Arc<dyn Output>),
    State {
        namespace: String,
        backend: Arc<dyn StateBackend>,
    },
}

impl Guarded {
    fn label(&self) -> String {
        match self {
            Self::Temporary { name, .. } => format!("temporary '{name}'"),
            Self::Source(source) => format!("source {}", source_name(source)),
            Self::Sink(_) => "sink".to_string(),
            Self::State { namespace, .. } => format!("state backend '{namespace}'"),
        }
    }

    async fn close(&self) -> Result<(), Error> {
        match self {
            Self::Temporary { temporary, .. } => temporary.close().await,
            Self::Source(source) => source.close().await,
            Self::Sink(sink) => sink.close().await,
            Self::State { backend, .. } => backend.close(),
        }
    }
}

fn source_name(source: &Arc<dyn Input>) -> &'static str {
    // `Input` exposes no type name; the label is only used for close logs.
    let _ = source;
    "input"
}

/// Connects a Job's resources in dependency order and closes them in
/// reverse. Cheap to drop without `close` when nothing was connected.
#[derive(Default)]
pub struct JobResourceGuard {
    connected: Mutex<Vec<Guarded>>,
}

impl JobResourceGuard {
    pub fn new() -> Self {
        Self::default()
    }

    /// Connect temporary stores, sources, and sinks in dependency order.
    /// Temporary stores come first (processors resolve them during their
    /// first `get`), then sources, then sinks. On failure, everything
    /// connected so far is closed in reverse order and the error is
    /// returned with the partially built guard consumed.
    pub async fn connect(
        temporaries: &[Arc<dyn Temporary>],
        sources: &[Arc<dyn Input>],
        sinks: &[Arc<dyn Output>],
        states: &[(String, Arc<dyn StateBackend>)],
    ) -> Result<Self, Error> {
        let guard = Self::new();
        let mut connected = Vec::new();
        // State backends open eagerly at construction; registering them first
        // means a later connect failure closes them too.
        for (namespace, backend) in states {
            if let Err(error) = connect_guarded(
                &mut connected,
                Guarded::State {
                    namespace: namespace.clone(),
                    backend: backend.clone(),
                },
            )
            .await
            {
                let _ = reverse_close(&mut connected).await;
                return Err(error);
            }
        }
        for (index, temporary) in temporaries.iter().enumerate() {
            if let Err(error) = connect_guarded(
                &mut connected,
                Guarded::Temporary {
                    name: index.to_string(),
                    temporary: temporary.clone(),
                },
            )
            .await
            {
                let _ = reverse_close(&mut connected).await;
                return Err(error);
            }
        }
        for source in sources {
            if let Err(error) =
                connect_guarded(&mut connected, Guarded::Source(source.clone())).await
            {
                let _ = reverse_close(&mut connected).await;
                return Err(error);
            }
        }
        for sink in sinks {
            if let Err(error) = connect_guarded(&mut connected, Guarded::Sink(sink.clone())).await {
                let _ = reverse_close(&mut connected).await;
                return Err(error);
            }
        }
        *guard.connected.lock().unwrap() = connected;
        Ok(guard)
    }

    /// Register state backends and temporary stores with an externally
    /// managed guard (sources/sinks already connected by the caller, e.g. a
    /// recovery path that restored positions before the graph started).
    pub fn attach_shutdown_only(
        temporaries: &[Arc<dyn Temporary>],
        states: &[(String, Arc<dyn StateBackend>)],
    ) -> Self {
        let guard = Self::new();
        let mut connected = Vec::new();
        for (namespace, backend) in states {
            connected.push(Guarded::State {
                namespace: namespace.clone(),
                backend: backend.clone(),
            });
        }
        for (index, temporary) in temporaries.iter().enumerate() {
            connected.push(Guarded::Temporary {
                name: index.to_string(),
                temporary: temporary.clone(),
            });
        }
        *guard.connected.lock().unwrap() = connected;
        guard
    }

    /// Hand ownership of stream resources (sources and sinks) to the chain
    /// event loops, which already close their own components on every exit
    /// path. Temporary stores and state backends stay with the guard.
    pub fn hand_off_stream_resources(&self) {
        self.connected
            .lock()
            .unwrap()
            .retain(|resource| !matches!(resource, Guarded::Source(_) | Guarded::Sink(_)));
    }

    /// Close every connected resource in reverse order. Idempotent; each
    /// component error is surfaced as an aggregated error so shutdown still
    /// reaches every resource.
    pub async fn close(&self) -> Result<(), Error> {
        let mut connected = std::mem::take(&mut *self.connected.lock().unwrap());
        reverse_close(&mut connected).await
    }
}

async fn connect_guarded(connected: &mut Vec<Guarded>, resource: Guarded) -> Result<(), Error> {
    let result = match &resource {
        Guarded::Temporary { temporary, .. } => temporary.connect().await,
        Guarded::Source(source) => source.connect().await,
        Guarded::Sink(sink) => sink.connect().await,
        // State backends are open from construction.
        Guarded::State { .. } => Ok(()),
    };
    if let Err(error) = result {
        // A connector may allocate a WAL/consumer before discovering a
        // startup error. It is not in `connected` yet, so close this failed
        // resource explicitly before the caller cleans up earlier resources.
        if let Err(close_error) = resource.close().await {
            tracing::warn!(
                %close_error,
                resource = %resource.label(),
                "failed to close resource whose connection failed"
            );
        }
        return Err(error);
    }
    connected.push(resource);
    Ok(())
}

async fn reverse_close(connected: &mut Vec<Guarded>) -> Result<(), Error> {
    let mut first_error = None;
    while let Some(resource) = connected.pop() {
        if let Err(error) = resource.close().await {
            tracing::warn!(%error, resource = %resource.label(), "failed to close job resource");
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct RecordingTemporary {
        connects: AtomicUsize,
        closes: AtomicUsize,
        fail_connect: bool,
        fail_close: bool,
    }

    #[async_trait]
    impl Temporary for RecordingTemporary {
        async fn connect(&self) -> Result<(), Error> {
            self.connects.fetch_add(1, Ordering::SeqCst);
            if self.fail_connect {
                return Err(Error::Connection("injected temporary failure".into()));
            }
            Ok(())
        }
        async fn get(
            &self,
            _keys: &[datafusion::logical_expr::ColumnarValue],
        ) -> Result<Option<crate::MessageBatch>, Error> {
            Ok(None)
        }
        async fn close(&self) -> Result<(), Error> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            if self.fail_close {
                return Err(Error::Connection("injected temporary close failure".into()));
            }
            Ok(())
        }
    }

    struct RecordingInput {
        connects: AtomicUsize,
        closes: AtomicUsize,
        fail_connect: bool,
        fail_close: bool,
    }

    #[async_trait]
    impl Input for RecordingInput {
        async fn connect(&self) -> Result<(), Error> {
            self.connects.fetch_add(1, Ordering::SeqCst);
            if self.fail_connect {
                return Err(Error::Connection("injected source failure".into()));
            }
            Ok(())
        }
        async fn read(
            &self,
        ) -> Result<(crate::MessageBatchRef, Arc<dyn crate::input::Ack>), Error> {
            Err(Error::EOF)
        }
        async fn close(&self) -> Result<(), Error> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            if self.fail_close {
                return Err(Error::Connection("injected source close failure".into()));
            }
            Ok(())
        }
    }

    struct RecordingOutput {
        connects: AtomicUsize,
        closes: AtomicUsize,
        fail_connect: bool,
        fail_close: bool,
    }

    #[async_trait]
    impl Output for RecordingOutput {
        async fn connect(&self) -> Result<(), Error> {
            self.connects.fetch_add(1, Ordering::SeqCst);
            if self.fail_connect {
                return Err(Error::Connection("injected sink failure".into()));
            }
            Ok(())
        }
        async fn write(&self, _msg: crate::MessageBatchRef) -> Result<(), Error> {
            Ok(())
        }
        async fn close(&self) -> Result<(), Error> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            if self.fail_close {
                return Err(Error::Connection("injected sink close failure".into()));
            }
            Ok(())
        }
    }

    /// State backend double delegating to the real in-memory backend so the
    /// guard exercises the production `StateBackend` object path while
    /// counting (and optionally failing) `close`.
    struct RecordingState {
        inner: crate::state::InMemoryStateBackend,
        closes: AtomicUsize,
        fail_close: bool,
    }

    impl RecordingState {
        fn new() -> Self {
            Self {
                inner: crate::state::InMemoryStateBackend::new(1).unwrap(),
                closes: AtomicUsize::new(0),
                fail_close: false,
            }
        }
    }

    impl StateBackend for RecordingState {
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
            self.closes.fetch_add(1, Ordering::SeqCst);
            if self.fail_close {
                return Err(Error::Connection("injected state close failure".into()));
            }
            Ok(())
        }
    }

    #[tokio::test]
    // The concrete Arcs are asserted on below, so the one-element
    // slices rely on array-literal trait coercion instead of from_ref.
    #[allow(clippy::cloned_ref_to_slice_refs)]
    async fn connects_in_dependency_order_and_closes_in_reverse() {
        let temporary = recording_temporary(false, false);
        let source = recording_input(false, false);
        let sink = recording_output(false, false);
        let guard = JobResourceGuard::connect(
            &[temporary.clone()],
            &[source.clone()],
            &[sink.clone()],
            &[],
        )
        .await
        .unwrap();
        assert_eq!(temporary.connects.load(Ordering::SeqCst), 1);
        assert_eq!(source.connects.load(Ordering::SeqCst), 1);
        assert_eq!(sink.connects.load(Ordering::SeqCst), 1);

        guard.close().await.unwrap();
        // Idempotent: a second close is a no-op.
        guard.close().await.unwrap();
        assert_eq!(temporary.closes.load(Ordering::SeqCst), 1);
        assert_eq!(source.closes.load(Ordering::SeqCst), 1);
        assert_eq!(sink.closes.load(Ordering::SeqCst), 1);
    }

    /// Task 2.1: one resource failing to connect closes everything already
    /// connected in reverse order before the error surfaces.
    #[tokio::test]
    // The concrete Arcs are asserted on below, so the one-element
    // slices rely on array-literal trait coercion instead of from_ref.
    #[allow(clippy::cloned_ref_to_slice_refs)]
    async fn partial_connect_failure_cleans_up_in_reverse_order() {
        let temporary = recording_temporary(false, false);
        let source = recording_input(false, false);
        let failing_sink = recording_output(true, false);
        let result = JobResourceGuard::connect(
            &[temporary.clone()],
            &[source.clone()],
            &[failing_sink.clone()],
            &[],
        )
        .await;
        assert!(result.is_err());
        // The temporary and source connected before the failure were closed.
        assert_eq!(temporary.closes.load(Ordering::SeqCst), 1);
        assert_eq!(source.closes.load(Ordering::SeqCst), 1);
        // The failing connector itself may have acquired resources before
        // returning its error, so it is closed as part of startup cleanup.
        assert_eq!(failing_sink.closes.load(Ordering::SeqCst), 1);
    }

    fn recording_temporary(fail_connect: bool, fail_close: bool) -> Arc<RecordingTemporary> {
        Arc::new(RecordingTemporary {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
            fail_connect,
            fail_close,
        })
    }

    fn recording_input(fail_connect: bool, fail_close: bool) -> Arc<RecordingInput> {
        Arc::new(RecordingInput {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
            fail_connect,
            fail_close,
        })
    }

    fn recording_output(fail_connect: bool, fail_close: bool) -> Arc<RecordingOutput> {
        Arc::new(RecordingOutput {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
            fail_connect,
            fail_close,
        })
    }

    fn recording_state() -> Arc<RecordingState> {
        Arc::new(RecordingState::new())
    }

    /// A temporary failing to connect still closes the state backends that
    /// were registered before it, so an eager redb handle cannot leak its
    /// exclusive lock across a failed startup.
    #[tokio::test]
    #[allow(clippy::cloned_ref_to_slice_refs)]
    async fn temporary_connect_failure_closes_already_registered_states() {
        let state = recording_state();
        let failing_temporary = recording_temporary(true, false);
        let result = JobResourceGuard::connect(
            &[failing_temporary.clone()],
            &[],
            &[],
            &[("job".to_string(), state.clone())],
        )
        .await;
        let error = result
            .err()
            .expect("temporary connect failure must fail the guard");
        assert!(error.to_string().contains("injected temporary failure"));
        assert_eq!(state.closes.load(Ordering::SeqCst), 1);
        assert_eq!(failing_temporary.closes.load(Ordering::SeqCst), 1);
    }

    /// A source failing to connect closes the temporaries connected before it.
    #[tokio::test]
    #[allow(clippy::cloned_ref_to_slice_refs)]
    async fn source_connect_failure_closes_earlier_temporaries() {
        let temporary = recording_temporary(false, false);
        let failing_source = recording_input(true, false);
        let result =
            JobResourceGuard::connect(&[temporary.clone()], &[failing_source.clone()], &[], &[])
                .await;
        assert!(result.is_err());
        assert_eq!(temporary.closes.load(Ordering::SeqCst), 1);
        assert_eq!(failing_source.closes.load(Ordering::SeqCst), 1);
    }

    /// A connector whose connect fails AND whose close fails surfaces the
    /// connect error; the close failure is logged, not swallowed into the
    /// returned error.
    #[tokio::test]
    #[allow(clippy::cloned_ref_to_slice_refs)]
    async fn connect_failure_with_failing_close_still_reports_the_connect_error() {
        let failing_temporary = recording_temporary(true, true);
        let error = JobResourceGuard::connect(&[failing_temporary], &[], &[], &[])
            .await
            .err()
            .expect("connect failure must surface");
        assert!(
            error.to_string().contains("injected temporary failure"),
            "{error}"
        );
    }

    /// The recovery path registers states and temporaries without connecting
    /// them; close still reaches everything in reverse order.
    #[tokio::test]
    #[allow(clippy::cloned_ref_to_slice_refs)]
    async fn attach_shutdown_only_closes_states_and_temporaries_in_reverse_order() {
        let state = recording_state();
        let temporary = recording_temporary(false, false);
        let guard = JobResourceGuard::attach_shutdown_only(
            &[temporary.clone()],
            &[("job".to_string(), state.clone())],
        );
        assert_eq!(temporary.connects.load(Ordering::SeqCst), 0);
        guard.close().await.unwrap();
        assert_eq!(state.closes.load(Ordering::SeqCst), 1);
        assert_eq!(temporary.closes.load(Ordering::SeqCst), 1);
    }

    /// Closing a resource of every kind surfaces the first close error while
    /// still closing the remaining resources.
    #[tokio::test]
    #[allow(clippy::cloned_ref_to_slice_refs)]
    async fn close_failures_are_aggregated_and_every_resource_is_still_closed() {
        let state = Arc::new(RecordingState {
            inner: crate::state::InMemoryStateBackend::new(1).unwrap(),
            closes: AtomicUsize::new(0),
            fail_close: true,
        });
        let temporary = recording_temporary(false, true);
        let source = recording_input(false, true);
        let sink = recording_output(false, true);
        let guard = JobResourceGuard::connect(
            &[temporary.clone()],
            &[source.clone()],
            &[sink.clone()],
            &[("job".to_string(), state.clone())],
        )
        .await
        .unwrap();
        let error = guard
            .close()
            .await
            .expect_err("close failures must surface");
        // Every close failed: the aggregated error is one of the injected ones.
        assert!(error.to_string().contains("injected"), "{error}");
        assert_eq!(state.closes.load(Ordering::SeqCst), 1);
        assert_eq!(temporary.closes.load(Ordering::SeqCst), 1);
        assert_eq!(source.closes.load(Ordering::SeqCst), 1);
        assert_eq!(sink.closes.load(Ordering::SeqCst), 1);
    }

    /// Handing stream resources off leaves states and temporaries owned by
    /// the guard so a later close still releases them.
    #[tokio::test]
    #[allow(clippy::cloned_ref_to_slice_refs)]
    async fn hand_off_stream_resources_keeps_states_and_temporaries_for_shutdown() {
        let state = recording_state();
        let temporary = recording_temporary(false, false);
        let source = recording_input(false, false);
        let sink = recording_output(false, false);
        let guard = JobResourceGuard::connect(
            &[temporary.clone()],
            &[source.clone()],
            &[sink.clone()],
            &[("job".to_string(), state.clone())],
        )
        .await
        .unwrap();
        guard.hand_off_stream_resources();
        // The chains now own the source and sink: a guard close must not
        // double-close them.
        guard.close().await.unwrap();
        assert_eq!(source.closes.load(Ordering::SeqCst), 0);
        assert_eq!(sink.closes.load(Ordering::SeqCst), 0);
        assert_eq!(temporary.closes.load(Ordering::SeqCst), 1);
        assert_eq!(state.closes.load(Ordering::SeqCst), 1);
    }

    /// The recording doubles' remaining trait methods are exercised so the
    /// doubles themselves stay fully covered.
    #[tokio::test]
    async fn recording_doubles_expose_working_trait_defaults() {
        let temporary = recording_temporary(false, false);
        assert!(temporary
            .get(&[])
            .await
            .expect("temporary get must succeed")
            .is_none());
        let input = recording_input(false, false);
        assert!(input.read().await.is_err());
        let output = recording_output(false, false);
        let batch = datafusion::arrow::record_batch::RecordBatch::try_from_iter(vec![(
            "value",
            std::sync::Arc::new(datafusion::arrow::array::Int64Array::from(vec![1]))
                as std::sync::Arc<dyn datafusion::arrow::array::Array>,
        )])
        .unwrap();
        output
            .write(std::sync::Arc::new(crate::MessageBatch::new_arrow(batch)))
            .await
            .unwrap();
    }

    /// The state double delegates every backend method to the real in-memory
    /// backend; keep each delegation exercised so the double itself stays
    /// fully covered.
    #[tokio::test]
    async fn recording_state_double_delegates_every_backend_method() {
        let state = recording_state();
        assert_eq!(state.format_version(), 1);
        assert!(state.get("ns", b"key").unwrap().is_none());
        state
            .put_with_ttl("ns", b"key", b"value", None, 0)
            .unwrap();
        assert_eq!(state.get("ns", b"key").unwrap().as_deref(), Some(b"value".as_slice()));
        assert_eq!(state.update_i64("ns", b"counter", 2).unwrap(), 2);
        assert!(!state.scan("ns").unwrap().is_empty());
        assert!(state.delete("ns", b"key").unwrap());
        assert_eq!(state.purge_expired(0).unwrap(), 0);
        let snapshot = state.snapshot_at(0).unwrap();
        assert!(snapshot.verify());
        state.restore(&snapshot).unwrap();
        let _ = state.metrics().unwrap();
        state.close().unwrap();
        assert_eq!(state.closes.load(Ordering::SeqCst), 1);
    }
}
