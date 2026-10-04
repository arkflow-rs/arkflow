/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! Helper functions for codec integration in input components

use arkflow_core::codec::Codec;
use arkflow_core::{Bytes, Error, MessageBatch, MessageBatchRef};
use std::sync::Arc;

/// Apply codec to payload bytes
///
/// # Arguments
/// * `payload` - The raw payload bytes
/// * `codec` - Optional codec to apply
///
/// # Returns
/// * `Ok(MessageBatch)` - Decoded or binary-wrapped message batch
/// * `Err(Error)` - If codec application fails
pub async fn apply_codec_to_payload(
    payload: &[u8],
    codec: &Option<Arc<dyn Codec>>,
) -> Result<MessageBatch, Error> {
    if let Some(c) = codec {
        c.decode(vec![payload.to_vec()]).await
    } else {
        MessageBatch::new_binary(vec![payload.to_vec()])
    }
}

/// Apply codec to multiple payload bytes
///
/// # Arguments
/// * `payloads` - Multiple raw payload bytes
/// * `codec` - Optional codec to apply
///
/// # Returns
/// * `Ok(MessageBatch)` - Decoded or binary-wrapped message batch
/// * `Err(Error)` - If codec application fails
pub async fn apply_codec_to_payloads(
    payloads: Vec<Bytes>,
    codec: &Option<Arc<dyn Codec>>,
) -> Result<MessageBatch, Error> {
    if let Some(c) = codec {
        c.decode(payloads).await
    } else {
        MessageBatch::new_binary(payloads)
    }
}

/// A connector's decoded delivery travelling on its internal channel.
///
/// Producers (the connector's background receive task) decode payloads and
/// pair them with their acknowledgement BEFORE claiming a channel slot, so
/// `Input::read` waits on exactly one await point (`recv_async`) whose
/// completion is the delivery. The engine's source loop drops pending
/// `read()` futures at any select! branch loss (idle ticks fire every
/// 100ms) — with deliveries pre-decoded, a dropped read loses nothing.
/// This is the `Input::read` cancellation-safety contract in arkflow-core.
pub(crate) enum Delivery {
    /// One decoded batch and the acknowledgement that settles it.
    Data(MessageBatchRef, Arc<dyn arkflow_core::input::Ack>),
    /// A producer-side failure (e.g. a codec decode error) surfaced to read.
    Err(Error),
}

/// Decode one payload into a finished [`Delivery`] on the producer side:
/// codec applied, input name stamped, ack paired. Decode failures become
/// `Delivery::Err` so `read()` can surface them without claiming a slot
/// first.
pub(crate) async fn decode_delivery(
    payload: &[u8],
    codec: &Option<Arc<dyn Codec>>,
    input_name: Option<String>,
    ack: Arc<dyn arkflow_core::input::Ack>,
) -> Delivery {
    match apply_codec_to_payload(payload, codec).await {
        Ok(mut batch) => {
            batch.set_input_name(input_name);
            Delivery::Data(Arc::new(batch), ack)
        }
        Err(error) => Delivery::Err(error),
    }
}

/// Cancellation-safety contract test scaffolding shared by the channel
/// inputs: a gate codec whose decode blocks until released (a
/// deterministic "decode in flight" window), and an engine-shaped
/// cancellation probe that discards a pending read exactly like a lost
/// select! branch.
#[cfg(test)]
pub(crate) mod contract {
    use super::*;
    use arkflow_core::codec::{Decoder, Encoder};
    use arkflow_core::input::{Ack, Input};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    /// A codec whose decode parks until released. The codec half is handed
    /// to the input under test; the handle half stays with the test — both
    /// share the atomic flags (no trait-object downcasting needed).
    /// Handshakes are atomic flags polled at 1ms — no notify registration
    /// races.
    pub(crate) struct GateCodec {
        decode_entered: Arc<AtomicBool>,
        released: Arc<AtomicBool>,
    }

    #[derive(Clone)]
    pub(crate) struct GateHandle {
        decode_entered: Arc<AtomicBool>,
        released: Arc<AtomicBool>,
    }

    pub(crate) fn gate() -> (Arc<GateCodec>, GateHandle) {
        let decode_entered = Arc::new(AtomicBool::new(false));
        let released = Arc::new(AtomicBool::new(false));
        (
            Arc::new(GateCodec {
                decode_entered: decode_entered.clone(),
                released: released.clone(),
            }),
            GateHandle {
                decode_entered,
                released,
            },
        )
    }

    impl GateHandle {
        pub fn decode_entered(&self) -> bool {
            self.decode_entered.load(Ordering::SeqCst)
        }

        pub fn release(&self) {
            self.released.store(true, Ordering::SeqCst);
        }

        /// Wait until a decode call is parked inside the gate.
        pub async fn wait_entered(&self) {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while !self.decode_entered() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for a decode to enter the gate"
                );
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }
    }

    #[async_trait]
    impl Encoder for GateCodec {
        async fn encode(&self, _messages: MessageBatch) -> Result<Vec<Bytes>, Error> {
            Err(Error::Process("gate codec does not encode".into()))
        }
    }

    #[async_trait]
    impl Decoder for GateCodec {
        async fn decode(&self, b: Vec<Bytes>) -> Result<MessageBatch, Error> {
            self.decode_entered.store(true, Ordering::SeqCst);
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            while !self.released.load(Ordering::SeqCst) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "gate codec never released"
                );
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            MessageBatch::new_binary(b)
        }
    }

    /// The engine-shaped cancellation probe: start a read, wait until the
    /// gate codec's decode is in flight (inside `read` for a violating
    /// implementation, inside the producer task for a fixed one), discard
    /// the pending read exactly like a lost select! branch, release the
    /// gate, and read again. A cancellation-safe input returns the very
    /// same delivery on the second read; a claim-then-decode input has
    /// already lost it.
    pub(crate) async fn cancel_pending_read_then_expect_delivery(
        input: Arc<dyn Input>,
        gate: &GateHandle,
    ) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        let completed_early = {
            let mut first = std::pin::pin!(input.read());
            let outcome = tokio::select! {
                outcome = &mut first => Some(outcome),
                _ = gate.wait_entered() => None,
            };
            outcome
            // Leaving the block discards the pending read exactly like a
            // lost select! branch in the engine's source loop.
        };
        gate.release();
        match completed_early {
            // The delivery finished before any cancellation could land —
            // a valid outcome; hand it straight back.
            Some(outcome) => outcome,
            None => input.read().await,
        }
    }

    /// The historical violating shape, kept as the harness's own
    /// effectiveness proof: the channel carries raw payloads and `read`
    /// claims one, THEN awaits the codec decode inside the (droppable)
    /// read future. The engine drops the read mid-decode and the claimed
    /// payload is lost forever.
    pub(crate) struct ClaimThenDecodeInput {
        receiver: flume::Receiver<Vec<u8>>,
        codec: Option<Arc<dyn Codec>>,
    }

    impl ClaimThenDecodeInput {
        pub fn pair(codec: Option<Arc<dyn Codec>>) -> (flume::Sender<Vec<u8>>, Arc<Self>) {
            let (sender, receiver) = flume::bounded(8);
            (sender, Arc::new(Self { receiver, codec }))
        }
    }

    #[async_trait]
    impl Input for ClaimThenDecodeInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            let payload = self.receiver.recv_async().await.map_err(|_| Error::EOF)?;
            // Side effect (claim) happened above; this await is where a
            // dropped read future loses the delivery.
            let mut batch = apply_codec_to_payload(&payload, &self.codec).await?;
            batch.set_input_name(Some("violating".into()));
            Ok((Arc::new(batch), Arc::new(arkflow_core::input::NoopAck)))
        }
    }

    /// The compliant shape every channel input converges to after the fix:
    /// a producer task decodes (the gate parks there, outside any engine
    /// select) and the channel carries finished [`Delivery`] values; `read`
    /// awaits `recv` only, so a dropped read future loses nothing.
    pub(crate) struct ProducerDecodedInput {
        receiver: flume::Receiver<Delivery>,
    }

    impl ProducerDecodedInput {
        pub fn pair() -> (flume::Sender<Delivery>, Arc<Self>) {
            let (sender, receiver) = flume::bounded(8);
            (sender, Arc::new(Self { receiver }))
        }
    }

    #[async_trait]
    impl Input for ProducerDecodedInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            match self.receiver.recv_async().await {
                Ok(Delivery::Data(batch, ack)) => Ok((batch, ack)),
                Ok(Delivery::Err(error)) => Err(error),
                Err(_) => Err(Error::EOF),
            }
        }
    }

    /// Drive a compliant input's producer side: one payload through the
    /// gate codec into a finished delivery.
    pub(crate) async fn produce_one_delivery(
        sender: flume::Sender<Delivery>,
        codec: Arc<GateCodec>,
    ) {
        let codec: Option<Arc<dyn Codec>> = Some(codec);
        let ack: Arc<dyn arkflow_core::input::Ack> = Arc::new(arkflow_core::input::NoopAck);
        let delivery = decode_delivery(b"payload", &codec, None, ack).await;
        let _ = sender.send_async(delivery).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_apply_codec_to_payload_no_codec() {
        let payload = b"test data";
        let codec: Option<Arc<dyn Codec>> = None;

        let result = apply_codec_to_payload(payload, &codec).await;
        assert!(result.is_ok());

        let batch = result.unwrap();
        assert_eq!(batch.len(), 1);
    }

    #[tokio::test]
    async fn test_apply_codec_to_payloads_no_codec() {
        let payloads = vec![b"data1".to_vec(), b"data2".to_vec()];
        let codec: Option<Arc<dyn Codec>> = None;

        let result = apply_codec_to_payloads(payloads, &codec).await;
        assert!(result.is_ok());

        let batch = result.unwrap();
        assert_eq!(batch.len(), 2);
    }

    #[tokio::test]
    async fn decode_delivery_without_codec_pairs_batch_and_ack() {
        let delivery = decode_delivery(
            b"raw",
            &None,
            Some("source-a".into()),
            Arc::new(arkflow_core::input::NoopAck),
        )
        .await;
        let Delivery::Data(batch, _ack) = delivery else {
            panic!("no-codec decode cannot fail");
        };
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.get_input_name(), Some("source-a".to_string()));
    }

    /// Spec "框架能抓到认领后解码的违例实现": the historical
    /// claim-then-decode shape loses the delivery when the engine drops the
    /// read mid-decode — the probe's second read parks on the drained
    /// channel forever, which the probe detects via timeout.
    #[tokio::test]
    async fn harness_catches_claim_then_decode_loss() {
        use crate::input::codec_helper::contract::*;
        let (codec, gate) = gate();
        let (sender, input) = ClaimThenDecodeInput::pair(Some(codec));
        sender.send_async(b"payload".to_vec()).await.unwrap();

        let probe = cancel_pending_read_then_expect_delivery(input, &gate);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), probe)
                .await
                .is_err(),
            "violating input must fail the probe: the claimed payload was lost by the discarded read"
        );
    }

    /// Spec "合规实现零误报" (pattern level): a producer-decoded input passes
    /// the same probe — the dropped read loses nothing and the second read
    /// delivers exactly the one message.
    #[tokio::test]
    async fn compliant_producer_decoded_input_passes_the_probe() {
        use crate::input::codec_helper::contract::*;
        let (codec, gate) = gate();
        let (sender, input) = ProducerDecodedInput::pair();
        tokio::spawn(produce_one_delivery(sender, codec));

        let (batch, _ack) = cancel_pending_read_then_expect_delivery(input, &gate)
            .await
            .expect("compliant input delivers after cancellation");
        assert_eq!(batch.len(), 1);
    }
}
