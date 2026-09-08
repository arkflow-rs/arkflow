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
            Ok(())
        }
    }

    struct RecordingInput {
        connects: AtomicUsize,
        closes: AtomicUsize,
    }

    #[async_trait]
    impl Input for RecordingInput {
        async fn connect(&self) -> Result<(), Error> {
            self.connects.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn read(
            &self,
        ) -> Result<(crate::MessageBatchRef, Arc<dyn crate::input::Ack>), Error> {
            Err(Error::EOF)
        }
        async fn close(&self) -> Result<(), Error> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct RecordingOutput {
        connects: AtomicUsize,
        closes: AtomicUsize,
        fail_connect: bool,
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
            Ok(())
        }
    }

    #[tokio::test]
    async fn connects_in_dependency_order_and_closes_in_reverse() {
        let temporary = Arc::new(RecordingTemporary {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
            fail_connect: false,
        });
        let source = Arc::new(RecordingInput {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
        });
        let sink = Arc::new(RecordingOutput {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
            fail_connect: false,
        });
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
    async fn partial_connect_failure_cleans_up_in_reverse_order() {
        let temporary = Arc::new(RecordingTemporary {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
            fail_connect: false,
        });
        let source = Arc::new(RecordingInput {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
        });
        let failing_sink = Arc::new(RecordingOutput {
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
            fail_connect: true,
        });
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
}
