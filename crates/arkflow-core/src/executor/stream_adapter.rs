//! Job component adapter that rebuilds compiled-stream components verbatim.
//!
//! The stream compiler (see `stream_compiler`) stashes the original
//! `InputConfig`/`OutputConfig` payloads inside the JobSpec source/sink
//! configs. This adapter reverses that mapping so the unified kernel builds
//! exactly the components the legacy runtime would have built — including
//! codecs, names, temporary tables, and the WAL-backed input wrapper.

use crate::Error;
use crate::executor::stream_compiler::CODEC_PAYLOAD_KEY;
use crate::input::{Input, InputConfig};
use crate::job::{JobComponentAdapter, OperatorSpec, SinkSpec, SourceSpec};
use crate::{codec::CodecConfig, output::{Output, OutputConfig}, processor::{Processor, ProcessorConfig}, Resource};
use crate::wal::{Wal, WalAck, WalConfig};
use std::sync::Arc;

/// Adapter for one compiled stream: rebuilds components from the original
/// `StreamConfig` pieces embedded in the JobSpec, and attaches the stream's
/// WAL to the built input (the WAL is an input durability property, not an
/// execution-model property).
pub struct StreamJobAdapter {
    wal: Option<Arc<Wal>>,
}

impl StreamJobAdapter {
    /// Build the adapter for a compiled stream, opening the WAL when the
    /// stream declares enabled durability.
    pub fn new(durability: Option<&WalConfig>) -> Result<Self, Error> {
        let wal = match durability {
            Some(config) if config.enabled => Some(Wal::open(config)?),
            _ => None,
        };
        Ok(Self { wal })
    }

    pub fn wal(&self) -> Option<&Arc<Wal>> {
        self.wal.as_ref()
    }
}

fn decode_codec(payload: &serde_json::Value) -> Result<Option<CodecConfig>, Error> {
    match payload.get(CODEC_PAYLOAD_KEY) {
        Some(value) => serde_json::from_value(value.clone())
            .map(Some)
            .map_err(|error| {
                Error::Config(format!("compiled stream codec payload is invalid: {error}"))
            }),
        None => Ok(None),
    }
}

fn decode_name(payload: &serde_json::Value) -> Option<String> {
    payload
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

/// Wrap an input with the adapter's WAL: appends batches and gates the ack on
/// the WAL commit, exactly like the legacy `Stream::do_input` path.
pub struct WalInput {
    inner: Arc<dyn Input>,
    wal: Arc<Wal>,
}

#[async_trait::async_trait]
impl Input for WalInput {
    async fn connect(&self) -> Result<(), Error> {
        self.inner.connect().await
    }

    async fn read(&self) -> Result<(crate::MessageBatchRef, Arc<dyn crate::input::Ack>), Error> {
        let (batch, ack) = self.inner.read().await?;
        let seq = self.wal.append(&batch).await?;
        Ok((batch, Arc::new(WalAck::new(self.wal.clone(), seq, ack))))
    }

    async fn restore_positions(&self, positions: &[crate::checkpoint::SourcePosition]) -> Result<(), Error> {
        self.inner.restore_positions(positions).await
    }

    async fn current_positions(&self) -> Result<Vec<crate::checkpoint::SourcePosition>, Error> {
        self.inner.current_positions().await
    }

    fn supports_partitioning(&self) -> bool {
        self.inner.supports_partitioning()
    }

    fn assign_partition(&self, partition: u32) -> Result<(), Error> {
        self.inner.assign_partition(partition)
    }

    async fn close(&self) -> Result<(), Error> {
        self.inner.close().await
    }
}

impl JobComponentAdapter for StreamJobAdapter {
    fn build_input(
        &self,
        source: &SourceSpec,
        resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        let payload = source.config.clone();
        let config = InputConfig {
            input_type: source.input_type.clone(),
            name: decode_name(&payload),
            codec: decode_codec(&payload)?,
            config: strip_payload_keys(payload),
        };
        let input = config.build(resource)?;
        match &self.wal {
            Some(wal) => Ok(Arc::new(WalInput {
                inner: input,
                wal: wal.clone(),
            })),
            None => Ok(input),
        }
    }

    fn build_output(&self, sink: &SinkSpec, resource: &Resource) -> Result<Arc<dyn Output>, Error> {
        let payload = sink.config.clone();
        OutputConfig {
            output_type: sink.output_type.clone(),
            name: decode_name(&payload),
            codec: decode_codec(&payload)?,
            config: strip_payload_keys(payload),
        }
        .build(resource)
    }

    fn build_processor(
        &self,
        operator: &OperatorSpec,
        resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        let processor_type = operator
            .config
            .get("type")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| {
                Error::Config(format!("operator '{}' requires config.type", operator.id))
            })?;
        let mut config = operator.config.clone();
        if let Some(object) = config.as_object_mut() {
            object.remove("type");
            object.remove("name");
        }
        ProcessorConfig {
            processor_type,
            name: decode_name(&operator.config),
            config: Some(config),
        }
        .build(resource)
    }
}

fn strip_payload_keys(mut payload: serde_json::Value) -> Option<serde_json::Value> {
    if let Some(object) = payload.as_object_mut() {
        object.remove(CODEC_PAYLOAD_KEY);
        object.remove("name");
    }
    // Plugins require a present (non-null) config object; hand them `{}` when
    // nothing user-facing remains after stripping compiler metadata.
    Some(match payload {
        serde_json::Value::Null => serde_json::json!({}),
        value => value,
    })
}
