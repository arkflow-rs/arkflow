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

//! Stream component module
//!
//! A stream is a complete data processing unit, containing input, pipeline, and output.

use crate::buffer::Buffer;
use crate::input::{Ack, NoopAck};
use crate::wal::{Wal, WalAck, WalConfig};
use crate::{
    input::Input, output::Output, pipeline::Pipeline, Error, MessageBatchRef, ProcessResult,
    Resource,
};
use flume::{Receiver, Sender};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

const BACKPRESSURE_THRESHOLD: u64 = 1024;

/// A stream structure, containing input, pipe, output, and an optional buffer.
pub struct Stream {
    input: Arc<dyn Input>,
    pipeline: Arc<Pipeline>,
    output: Arc<dyn Output>,
    error_output: Option<Arc<dyn Output>>,
    thread_num: u32,
    buffer: Option<Arc<dyn Buffer>>,
    wal: Option<Arc<Wal>>,
    resource: Resource,
    sequence_counter: Arc<AtomicU64>,
    next_seq: Arc<AtomicU64>,
}

enum ProcessorData {
    Err(MessageBatchRef, Error),
    Ok(Vec<MessageBatchRef>),
}

impl Stream {
    /// Create a new stream.
    pub fn new(
        input: Arc<dyn Input>,
        pipeline: Pipeline,
        output: Arc<dyn Output>,
        error_output: Option<Arc<dyn Output>>,
        buffer: Option<Arc<dyn Buffer>>,
        wal: Option<Arc<Wal>>,
        resource: Resource,
        thread_num: u32,
    ) -> Self {
        Self {
            input,
            pipeline: Arc::new(pipeline),
            output,
            error_output,
            buffer,
            wal,
            resource,
            thread_num,
            sequence_counter: Arc::new(AtomicU64::new(0)),
            next_seq: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Running stream processing
    pub async fn run(&mut self, cancellation_token: CancellationToken) -> Result<(), Error> {
        let result = self.run_inner(cancellation_token).await;

        // Always close, even on error, so already-connected resources
        // (input/output/error_output/temporaries/WAL) are released when
        // recovery or worker setup fails. close() logs internally and
        // continues on per-component errors, matching the established
        // close-time policy.
        info!("Closing....");
        if let Err(e) = self.close().await {
            error!("Failed to close: {}", e);
        }
        info!("Closed.");
        info!("Exited.");

        result
    }

    async fn run_inner(&mut self, cancellation_token: CancellationToken) -> Result<(), Error> {
        // Connect input and output
        self.input.connect().await?;
        self.output.connect().await?;
        if let Some(ref error_output) = self.error_output {
            error_output.connect().await?;
        }
        for temporary in self.resource.temporary.values() {
            temporary.connect().await?
        }

        let (input_sender, input_receiver) =
            flume::bounded::<(MessageBatchRef, Arc<dyn Ack>)>(self.thread_num as usize * 4);
        let (output_sender, output_receiver) =
            flume::bounded::<(ProcessorData, Arc<dyn Ack>, u64)>(self.thread_num as usize * 4);

        let tracker = TaskTracker::new();

        // Spawn the downstream workers (buffer, processor, output) BEFORE WAL
        // replay. Recovery forwards unacked entries into the same bounded
        // `input_sender` (or buffer) that `do_input` uses — if consumers
        // weren't running yet, a large backlog would fill the channel and
        // `send_async` would await forever. Starting consumers first lets
        // `Self::forward` drain naturally during recovery.
        //
        // `do_input` is intentionally spawned AFTER replay so new input is
        // only read once pending entries have been restored (spec scenario 4).
        if let Some(buffer) = self.buffer.clone() {
            tracker.spawn(Self::do_buffer(
                cancellation_token.clone(),
                buffer,
                input_sender.clone(),
            ));
        }

        for i in 0..self.thread_num {
            tracker.spawn(Self::do_processor(
                i,
                self.pipeline.clone(),
                input_receiver.clone(),
                output_sender.clone(),
                self.sequence_counter.clone(),
                self.next_seq.clone(),
            ));
        }

        // All processor workers have their own clone; drop ours so the output
        // receiver observes end-of-stream once the last processor exits.
        drop(output_sender);

        tracker.spawn(Self::do_output(
            self.next_seq.clone(),
            output_receiver,
            self.output.clone(),
            self.error_output.clone(),
        ));

        // WAL recovery: replay any entries past the committed cursor before
        // `do_input` starts. Lives in `run_inner` (not `do_input`) so a
        // failure here fails stream startup rather than silently continuing
        // into the input loop. Replayed entries carry a NoopAck source (we
        // replay from the WAL, not a live source) wrapped in WalAck so the
        // cursor advances on downstream confirmation.
        if let Some(wal) = &self.wal {
            let entries = wal.read_after_cursor()?;
            info!("WAL recovery: replaying {} entries", entries.len());
            for (seq, msg) in entries {
                let ack: Arc<dyn Ack> = Arc::new(WalAck::new(wal.clone(), seq, Arc::new(NoopAck)));
                Self::forward(msg, ack, &self.buffer, &input_sender).await?;
            }
        }

        tracker.spawn(Self::do_input(
            cancellation_token.clone(),
            self.input.clone(),
            input_sender,
            self.buffer.clone(),
            self.wal.clone(),
        ));

        tracker.close();
        tracker.wait().await;

        Ok(())
    }

    /// Forward a (message, ack) pair to the buffer if present, otherwise to the
    /// processor input channel. Shared by normal ingestion and WAL recovery.
    async fn forward(
        msg: MessageBatchRef,
        ack: Arc<dyn Ack>,
        buffer_option: &Option<Arc<dyn Buffer>>,
        input_sender: &Sender<(MessageBatchRef, Arc<dyn Ack>)>,
    ) -> Result<(), Error> {
        if let Some(buffer) = buffer_option {
            buffer.write(msg, ack).await
        } else {
            input_sender
                .send_async((msg, ack))
                .await
                .map_err(|e| Error::Process(format!("Failed to send input message: {}", e)))
        }
    }

    async fn do_input(
        cancellation_token: CancellationToken,
        input: Arc<dyn Input>,
        input_sender: Sender<(MessageBatchRef, Arc<dyn Ack>)>,
        buffer_option: Option<Arc<dyn Buffer>>,
        wal: Option<Arc<Wal>>,
    ) {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                },
                result = input.read() =>{
                    match result {
                    Ok((msg, ack)) => {
                            let ack: Arc<dyn Ack> = if let Some(wal) = &wal {
                                match wal.append(&msg).await {
                                    Ok(seq) => Arc::new(WalAck::new(wal.clone(), seq, ack)),
                                    Err(e) => {
                                        error!("Failed to persist message to WAL: {}", e);
                                        break;
                                    }
                                }
                            } else {
                                ack
                            };

                            if let Err(e) = Self::forward(msg, ack, &buffer_option, &input_sender).await {
                                error!("Failed to send input message: {}", e);
                                break;
                            }
                    }
                    Err(e) => {
                        match e {
                            Error::EOF => {
                                // When input is complete, close the sender to notify all workers
                                cancellation_token.cancel();
                                break;
                            }
                            Error::Disconnection => loop {
                                match input.connect().await {
                                    Ok(_) => {
                                        info!("input reconnected");
                                        break;
                                    }
                                    Err(e) => {
                                        error!("{}", e);
                                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                    }
                                };
                            },
                            Error::Config(e) => {
                                error!("{}", e);
                                break;
                            }
                            _ => {
                                error!("{}", e);
                            }
                        };
                    }
                    };
                }
            }
        }
        info!("Input stopped");
    }

    async fn do_buffer(
        cancellation_token: CancellationToken,
        buffer: Arc<dyn Buffer>,
        input_sender: Sender<(MessageBatchRef, Arc<dyn Ack>)>,
    ) {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                },
                result = buffer.read() =>{
                    match result {
                        Ok(Some((v, ack))) => {
                             if let Err(e) = input_sender.send_async((v, ack)).await {
                                    error!("Failed to send input message: {}", e);
                                    break;
                                }
                        }
                        Err(e) => {
                            error!("Failed to read buffer:{}", e);
                        }
                        _=>{}
                    }
                }
            }
        }

        if let Err(e) = buffer.flush().await {
            error!("Failed to flush buffer: {}", e);
        }

        info!("Buffer flushed");

        if let Ok(Some((v, ack))) = buffer.read().await {
            if let Err(e) = input_sender.send_async((v, ack)).await {
                error!("Failed to send input message: {}", e);
            }
        }
        info!("Buffer stopped");
    }

    async fn do_processor(
        i: u32,
        pipeline: Arc<Pipeline>,
        input_receiver: Receiver<(MessageBatchRef, Arc<dyn Ack>)>,
        output_sender: Sender<(ProcessorData, Arc<dyn Ack>, u64)>,
        sequence_counter: Arc<AtomicU64>,
        next_seq: Arc<AtomicU64>,
    ) {
        let i = i + 1;
        info!("Processor worker {} started", i);
        loop {
            // Backpressure control
            let pending_messages =
                sequence_counter.load(Ordering::Acquire) - next_seq.load(Ordering::Acquire);
            if pending_messages > BACKPRESSURE_THRESHOLD {
                let wait_time = std::cmp::min(
                    500,
                    100 + (pending_messages - BACKPRESSURE_THRESHOLD) / 100 * 10,
                );
                tokio::time::sleep(std::time::Duration::from_millis(wait_time)).await;
                continue;
            }

            let Ok((msg, ack)) = input_receiver.recv_async().await else {
                break;
            };

            let processed = pipeline.process(msg.clone()).await;
            let seq = sequence_counter.fetch_add(1, Ordering::AcqRel);

            match processed {
                Ok(ProcessResult::Single(result_msg)) => {
                    if let Err(e) = output_sender
                        .send_async((ProcessorData::Ok(vec![result_msg]), ack, seq))
                        .await
                    {
                        error!("Failed to send processed message: {}", e);
                        break;
                    }
                }
                Ok(ProcessResult::Multiple(result_msgs)) => {
                    if let Err(e) = output_sender
                        .send_async((ProcessorData::Ok(result_msgs), ack, seq))
                        .await
                    {
                        error!("Failed to send processed message: {}", e);
                        break;
                    }
                }
                Ok(ProcessResult::None) => {
                    // Message filtered out, just ACK
                    if let Err(e) = ack.ack().await {
                        error!("Failed to ack filtered message: {}", e);
                    }
                }
                Err(e) => {
                    if let Err(e) = output_sender
                        .send_async((ProcessorData::Err(msg, e), ack, seq))
                        .await
                    {
                        error!("Failed to send processed message: {}", e);
                        break;
                    }
                }
            }
        }
        info!("Processor worker {} stopped", i);
    }

    async fn do_output(
        next_seq: Arc<AtomicU64>,
        output_receiver: Receiver<(ProcessorData, Arc<dyn Ack>, u64)>,
        output: Arc<dyn Output>,
        err_output: Option<Arc<dyn Output>>,
    ) {
        let mut tree_map: BTreeMap<u64, (ProcessorData, Arc<dyn Ack>)> = BTreeMap::new();

        loop {
            let Ok((data, new_ack, new_seq)) = output_receiver.recv_async().await else {
                for (_, (data, x)) in tree_map {
                    Self::output(data, &x, &output, err_output.as_ref()).await;
                }
                break;
            };

            tree_map.insert(new_seq, (data, new_ack));

            loop {
                let Some((current_seq, _)) = tree_map.first_key_value() else {
                    break;
                };
                let next_seq_val = next_seq.load(Ordering::Acquire);
                if next_seq_val != *current_seq {
                    break;
                }

                let Some((data, ack)) = tree_map.remove(&next_seq_val) else {
                    break;
                };

                Self::output(data, &ack, &output, err_output.as_ref()).await;
                next_seq.fetch_add(1, Ordering::Release);
            }
        }

        info!("Output stopped")
    }

    async fn output(
        data: ProcessorData,
        ack: &Arc<dyn Ack>,
        output: &Arc<dyn Output>,
        err_output: Option<&Arc<dyn Output>>,
    ) {
        match data {
            ProcessorData::Err(msg, e) => match err_output {
                None => {
                    if let Err(err) = ack.ack().await {
                        error!("Failed to ack errored message: {}", err);
                    }
                    error!("{e}");
                }
                Some(err_output) => match err_output.write(msg).await {
                    Ok(_) => {
                        if let Err(err) = ack.ack().await {
                            error!("Failed to ack errored message: {}", err);
                        }
                    }
                    Err(e) => {
                        error!("{}", e);
                    }
                },
            },
            ProcessorData::Ok(msgs) => {
                let size = msgs.len();
                let mut success_cnt = 0;
                for msg in msgs {
                    match output.write(msg).await {
                        Ok(_) => {
                            success_cnt += 1;
                        }
                        Err(e) => {
                            error!("{}", e);
                        }
                    }
                }

                if success_cnt >= size {
                    if let Err(e) = ack.ack().await {
                        error!("Failed to ack message: {}", e);
                    }
                }
            }
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        // Closing order: input -> buffer -> pipeline -> output -> error output
        // -> WAL. The WAL is closed last so that any in-flight ack has already
        // been drained by the output worker before the background flusher is
        // stopped; this guarantees pending group-commit/periodic appends are
        // flushed to disk before the stream terminates.
        info!("input close...");
        if let Err(e) = self.input.close().await {
            error!("Failed to close input: {}", e);
        }
        info!("input closed");

        info!("buffer close...");
        if let Some(buffer) = &self.buffer {
            if let Err(e) = buffer.close().await {
                error!("Failed to close buffer: {}", e);
            }
        }
        info!("buffer closed");

        info!("pipeline close...");
        if let Err(e) = self.pipeline.close().await {
            error!("Failed to close pipeline: {}", e);
        }
        info!("pipeline closed");

        info!("output close...");
        if let Err(e) = self.output.close().await {
            error!("Failed to close output: {}", e);
        }
        info!("output closed");

        info!("error output close...");
        if let Some(error_output) = &self.error_output {
            if let Err(e) = error_output.close().await {
                error!("Failed to close error output: {}", e);
            }
        }
        info!("error output closed");

        info!("wal close...");
        if let Some(wal) = &self.wal {
            if let Err(e) = wal.close().await {
                error!("Failed to close WAL: {}", e);
            }
        }
        info!("wal closed");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::Buffer;
    use crate::input::{Input, NoopAck};
    use crate::output::Output;
    use crate::pipeline::Pipeline;
    use crate::wal::SyncPolicy;
    use crate::MessageBatch;
    use async_trait::async_trait;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::record_batch::RecordBatch;
    use std::collections::VecDeque;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use tokio::sync::Mutex;

    struct StubInput {
        connected: std::sync::atomic::AtomicBool,
        queue: Mutex<VecDeque<MessageBatch>>,
    }

    #[async_trait]
    impl Input for StubInput {
        async fn connect(&self) -> Result<(), Error> {
            self.connected
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }

        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn crate::input::Ack>), Error> {
            let msg = {
                let mut q = self.queue.lock().await;
                q.pop_front()
            };
            match msg {
                Some(m) => Ok((Arc::new(m), Arc::new(NoopAck))),
                None => Err(Error::EOF),
            }
        }

        async fn close(&self) -> Result<(), Error> {
            self.connected
                .store(false, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    struct StubOutput;

    #[async_trait]
    impl Output for StubOutput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn write(&self, _msg: MessageBatchRef) -> Result<(), Error> {
            // Always fail so the output worker never advances the WAL cursor,
            // leaving the message pending. This isolates the close-time flush
            // behavior from the ack path.
            Err(Error::Process("stub output intentionally fails".into()))
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    fn sample_batch() -> MessageBatch {
        sample_batch_with_value(1)
    }

    fn sample_batch_with_value(v: i64) -> MessageBatch {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![std::sync::Arc::new(Int64Array::from(vec![v]))])
                .unwrap();
        MessageBatch::new_arrow(batch)
    }

    fn temp_dir() -> std::path::PathBuf {
        static C: AtomicU64 = AtomicU64::new(0);
        let n = C.fetch_add(1, AtomicOrdering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "arkflow-stream-wal-test-{}-{}",
            std::process::id(),
            n
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Drive a stream that uses a group-commit WAL through `Stream::close()`
    /// and assert that a pending append is durable on reopen. This exercises
    /// the production close chain rather than calling `Wal::close()` directly.
    #[tokio::test]
    async fn stream_close_flushes_group_commit_pending() {
        let dir = temp_dir();
        let cfg = WalConfig {
            enabled: true,
            path: dir.to_string_lossy().to_string(),
            sync: SyncPolicy::GroupCommit,
        };
        let wal = Wal::open(&cfg).unwrap();

        // Build a stream with one queued message and an empty pipeline.
        let mut input_queue = VecDeque::new();
        input_queue.push_back(sample_batch());
        let input = Arc::new(StubInput {
            connected: std::sync::atomic::AtomicBool::new(false),
            queue: Mutex::new(input_queue),
        });
        let output: Arc<dyn Output> = Arc::new(StubOutput);
        let pipeline = Pipeline::new(vec![]);
        let mut stream = Stream::new(
            input.clone(),
            pipeline,
            output,
            None,
            None,
            Some(wal.clone()),
            Resource {
                temporary: HashMap::new(),
                input_names: RefCell::default(),
            },
            1,
        );

        // Drive the stream to EOF and then exercise the full close path.
        let cancel = CancellationToken::new();
        let run_handle = tokio::spawn({
            let cancel = cancel.clone();
            async move { stream.run(cancel).await }
        });
        // Stream exits on EOF (the stub input returns EOF after the single
        // queued message is consumed). Wait for run() to return.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), run_handle)
            .await
            .expect("stream did not terminate in time")
            .unwrap()
            .unwrap();

        // Drop the local Arc so the redb file lock is released before reopen.
        drop(wal);

        // Reopen the WAL and confirm the queued message was persisted despite
        // the group-commit flusher never running on its timer.
        let wal2 = Wal::open(&cfg).unwrap();
        let pending = wal2.read_after_cursor().unwrap();
        assert_eq!(
            pending.len(),
            1,
            "group-commit WAL must flush pending entries on Stream::close"
        );
        wal2.close().await.unwrap();
    }

    /// Same property for `periodic` sync policy — the periodic timer is much
    /// longer than the test, so the only thing that can flush the staged entry
    /// is `Stream::close()`.
    #[tokio::test]
    async fn stream_close_flushes_periodic_pending() {
        let dir = temp_dir();
        let cfg = WalConfig {
            enabled: true,
            path: dir.to_string_lossy().to_string(),
            sync: SyncPolicy::Periodic(std::time::Duration::from_secs(60)),
        };
        let wal = Wal::open(&cfg).unwrap();

        let mut input_queue = VecDeque::new();
        input_queue.push_back(sample_batch());
        let input = Arc::new(StubInput {
            connected: std::sync::atomic::AtomicBool::new(false),
            queue: Mutex::new(input_queue),
        });
        let output: Arc<dyn Output> = Arc::new(StubOutput);
        let pipeline = Pipeline::new(vec![]);
        let mut stream = Stream::new(
            input.clone(),
            pipeline,
            output,
            None,
            None,
            Some(wal.clone()),
            Resource {
                temporary: HashMap::new(),
                input_names: RefCell::default(),
            },
            1,
        );

        let cancel = CancellationToken::new();
        let run_handle = tokio::spawn({
            let cancel = cancel.clone();
            async move { stream.run(cancel).await }
        });
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), run_handle)
            .await
            .expect("stream did not terminate in time")
            .unwrap()
            .unwrap();

        drop(wal);

        let wal2 = Wal::open(&cfg).unwrap();
        let pending = wal2.read_after_cursor().unwrap();
        assert_eq!(
            pending.len(),
            1,
            "periodic WAL must flush pending entries on Stream::close"
        );
        wal2.close().await.unwrap();
    }

    /// Streams without a WAL configured must close unchanged: no WAL step
    /// should be attempted and the close should still return Ok.
    #[tokio::test]
    async fn stream_without_wal_close_is_unchanged() {
        let input = Arc::new(StubInput {
            connected: std::sync::atomic::AtomicBool::new(false),
            queue: Mutex::new(VecDeque::new()),
        });
        let output: Arc<dyn Output> = Arc::new(StubOutput);
        let pipeline = Pipeline::new(vec![]);
        let mut stream = Stream::new(
            input.clone(),
            pipeline,
            output,
            None,
            None,
            None,
            Resource {
                temporary: HashMap::new(),
                input_names: RefCell::default(),
            },
            1,
        );

        let cancel = CancellationToken::new();
        let run_handle = tokio::spawn({
            let cancel = cancel.clone();
            async move { stream.run(cancel).await }
        });
        let res = tokio::time::timeout(std::time::Duration::from_secs(5), run_handle)
            .await
            .expect("stream did not terminate in time")
            .unwrap();
        assert!(res.is_ok(), "stream without WAL must close cleanly");
    }

    /// Buffer that always fails on `write`. Used to drive recovery's
    /// `Self::forward` into the error path during a durability-enabled
    /// stream's startup.
    struct FailingBuffer;

    #[async_trait]
    impl Buffer for FailingBuffer {
        async fn write(&self, _msg: MessageBatchRef, _ack: Arc<dyn Ack>) -> Result<(), Error> {
            Err(Error::Process("stub buffer intentionally fails".into()))
        }

        async fn read(&self) -> Result<Option<(MessageBatchRef, Arc<dyn Ack>)>, Error> {
            Ok(None)
        }

        async fn flush(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    /// WAL corruption must surface before the stream enters its running state.
    ///
    /// redb 2.6.3 verifies the entire B-tree at `Database::open` time (via
    /// `verify_primary_checksums` in `redb/src/db.rs:783 do_repair`), so
    /// corruptions that would make `read_after_cursor` return `Err` are
    /// detected when `Wal::open` is called from `StreamConfig::build` —
    /// before `Stream::run` is reached. The structural `?` propagation in
    /// `Stream::run_inner` covers the hypothetical case where redb ever
    /// lets a corruption through to read time, but constructing that
    /// scenario deterministically requires mocking `Wal`, which is out of
    /// scope for this change.
    #[tokio::test]
    async fn wal_corruption_surfaces_before_stream_run() {
        let dir = temp_dir();
        let cfg = WalConfig {
            enabled: true,
            path: dir.to_string_lossy().to_string(),
            sync: SyncPolicy::PerEntry,
        };

        // Phase 1: write a valid WAL with one unacked entry, then close.
        {
            let wal = Wal::open(&cfg).unwrap();
            wal.append(&Arc::new(sample_batch())).await.unwrap();
            wal.close().await.unwrap();
        }

        // Phase 2: corrupt the WAL file (same手法 as
        // `wal::tests::corrupted_store_surfaces_error`).
        let db_path = dir.join("wal.redb");
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&db_path)
            .unwrap();
        f.write_all(&[0xFFu8; 256]).unwrap();
        f.flush().unwrap();
        drop(f);

        // Phase 3: re-opening the WAL fails. In the production flow this
        // error surfaces from `StreamConfig::build` (which calls
        // `Wal::open`), so the stream never reaches `run()`.
        let result = Wal::open(&cfg);
        assert!(
            result.is_err(),
            "WAL corruption must surface before stream startup completes"
        );
    }

    /// When `Wal::read_after_cursor()` returns entries but forwarding one of
    /// them into the downstream buffer fails, `Stream::run` MUST return
    /// `Err` rather than silently breaking out of the replay loop and
    /// reading new input. The WAL cursor MUST NOT advance for the failed
    /// entry, so the entry remains pending for the next restart.
    #[tokio::test]
    async fn stream_run_returns_err_when_recovery_forward_fails() {
        let dir = temp_dir();
        let cfg = WalConfig {
            enabled: true,
            path: dir.to_string_lossy().to_string(),
            sync: SyncPolicy::PerEntry,
        };
        let wal = Wal::open(&cfg).unwrap();

        // Persist one unacked entry so `read_after_cursor` returns it.
        wal.append(&Arc::new(sample_batch())).await.unwrap();
        assert_eq!(wal.cursor(), 0, "cursor must start at 0");

        // The failing buffer is configured on the stream, so recovery's
        // `Self::forward(msg, ack, &self.buffer, &input_sender)` routes via
        // `buffer.write`, which always returns `Err`.
        let buffer: Arc<dyn Buffer> = Arc::new(FailingBuffer);

        let input = Arc::new(StubInput {
            connected: std::sync::atomic::AtomicBool::new(false),
            queue: Mutex::new(VecDeque::new()),
        });
        let output: Arc<dyn Output> = Arc::new(StubOutput);
        let pipeline = Pipeline::new(vec![]);
        let mut stream = Stream::new(
            input.clone(),
            pipeline,
            output,
            None,
            Some(buffer),
            Some(wal.clone()),
            Resource {
                temporary: HashMap::new(),
                input_names: RefCell::default(),
            },
            1,
        );

        let result = stream.run(CancellationToken::new()).await;
        assert!(
            result.is_err(),
            "Stream::run must surface recovery forward failure as Err (got {result:?})"
        );
        // Cursor must not advance — the failed entry remains pending.
        assert_eq!(
            wal.cursor(),
            0,
            "cursor must not advance on recovery forward failure"
        );

        wal.close().await.unwrap();
    }

    /// Records the first cell of column 0 for every batch written. Used to
    /// assert ordering between replayed entries and freshly-read input.
    #[derive(Default)]
    struct RecordingOutput {
        received: Mutex<Vec<i64>>,
    }

    #[async_trait]
    impl Output for RecordingOutput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
            let arr = msg.column(0);
            let arr = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("RecordingOutput expects an Int64 column 0");
            self.received.lock().await.push(arr.value(0));
            Ok(())
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    /// End-to-end coverage for the happy recovery path (spec scenario 4):
    ///
    /// - Stream starts with an unacked entry persisted in the WAL.
    /// - `Stream::run_inner` replays it via `WalAck(NoopAck)` before any
    ///   worker is spawned.
    /// - The replayed entry flows through the pipeline, the output worker
    ///   acknowledges it, and the WAL cursor advances.
    /// - Only after replay does `do_input` read new input, so the output
    ///   observes `[replayed, new]` in that order.
    ///
    /// Existing WAL-layer tests cover the cursor mechanics; this test pins
    /// the `Stream::run`-level ordering and ack feedback loop.
    #[tokio::test]
    async fn stream_run_replays_unacked_entries_before_new_input() {
        let dir = temp_dir();
        let cfg = WalConfig {
            enabled: true,
            path: dir.to_string_lossy().to_string(),
            sync: SyncPolicy::PerEntry,
        };

        // Phase 1: persist one unacked entry to the WAL, simulating a crash
        // before downstream acknowledgement.
        const REPLAYED_VALUE: i64 = 100;
        {
            let wal = Wal::open(&cfg).unwrap();
            wal.append(&Arc::new(sample_batch_with_value(REPLAYED_VALUE)))
                .await
                .unwrap();
            assert_eq!(wal.cursor(), 0, "cursor must stay at 0 until ack");
            wal.close().await.unwrap();
        }

        // Phase 2: reopen the WAL (simulating restart) and wire a fresh input
        // message. The stream must replay the WAL entry *before* consuming the
        // new input.
        const NEW_VALUE: i64 = 200;
        let wal = Wal::open(&cfg).unwrap();
        let mut input_queue = VecDeque::new();
        input_queue.push_back(sample_batch_with_value(NEW_VALUE));
        let input = Arc::new(StubInput {
            connected: std::sync::atomic::AtomicBool::new(false),
            queue: Mutex::new(input_queue),
        });
        let recording_output = Arc::new(RecordingOutput::default());
        let output: Arc<dyn Output> = recording_output.clone();
        let pipeline = Pipeline::new(vec![]);
        let mut stream = Stream::new(
            input.clone(),
            pipeline,
            output,
            None,
            None,
            Some(wal.clone()),
            Resource {
                temporary: HashMap::new(),
                input_names: RefCell::default(),
            },
            1,
        );

        let cancel = CancellationToken::new();
        let run_result =
            tokio::time::timeout(std::time::Duration::from_secs(5), stream.run(cancel))
                .await
                .expect("stream did not terminate in time");
        assert!(
            run_result.is_ok(),
            "happy recovery path must complete without error (got {run_result:?})"
        );

        // Cursor advanced twice: once for the replayed entry (acked via
        // WalAck(NoopAck)) and once for the freshly-read input (acked via
        // WalAck(source_ack) after the output worker confirms). Both
        // acknowledgements flowing through `WalAck` is exactly the contract
        // spec scenario 4 calls for.
        assert_eq!(
            wal.cursor(),
            2,
            "WAL cursor must advance for both the replayed entry and the new input"
        );

        // The new input message was consumed after replay. Both reached the
        // output in the order [replayed, new] — proving replay is not skipped
        // and is sequenced before fresh ingestion.
        let received = recording_output.received.lock().await.clone();
        assert_eq!(
            received,
            vec![REPLAYED_VALUE, NEW_VALUE],
            "replayed entry must reach output before newly-read input"
        );

        wal.close().await.unwrap();
    }

    /// Regression: WAL recovery must not deadlock when the backlog exceeds
    /// the bounded input channel capacity (`thread_num * 4`).
    ///
    /// Before downstream workers were spawned ahead of replay, recovery
    /// loop's `Self::forward` would `send_async` into a bounded channel
    /// with no consumers; once the backlog exceeded the capacity, the
    /// `send_async` awaited forever and `Stream::run` hung. This test
    /// writes 50 unacked entries (cap = 4 with `thread_num=1`) and asserts
    /// the stream terminates cleanly.
    #[tokio::test]
    async fn stream_run_replays_more_entries_than_channel_capacity_without_deadlock() {
        let dir = temp_dir();
        let cfg = WalConfig {
            enabled: true,
            path: dir.to_string_lossy().to_string(),
            sync: SyncPolicy::PerEntry,
        };

        const ENTRY_COUNT: i64 = 50;
        {
            let wal = Wal::open(&cfg).unwrap();
            for i in 0..ENTRY_COUNT {
                wal.append(&Arc::new(sample_batch_with_value(i)))
                    .await
                    .unwrap();
            }
            assert_eq!(
                wal.cursor(),
                0,
                "cursor must stay at 0 — none of the appended entries are acked"
            );
            wal.close().await.unwrap();
        }

        let wal = Wal::open(&cfg).unwrap();
        // Empty input queue → after replay completes, `do_input` hits EOF and
        // the stream drains naturally.
        let input = Arc::new(StubInput {
            connected: std::sync::atomic::AtomicBool::new(false),
            queue: Mutex::new(VecDeque::new()),
        });
        let recording_output = Arc::new(RecordingOutput::default());
        let output: Arc<dyn Output> = recording_output.clone();
        let pipeline = Pipeline::new(vec![]);
        // thread_num=1 → input channel capacity = 4. ENTRY_COUNT=50 > 4, so
        // the old spawn-order would deadlock on the 5th `send_async`.
        let mut stream = Stream::new(
            input.clone(),
            pipeline,
            output,
            None,
            None,
            Some(wal.clone()),
            Resource {
                temporary: HashMap::new(),
                input_names: RefCell::default(),
            },
            1,
        );

        let cancel = CancellationToken::new();
        let run_result =
            tokio::time::timeout(std::time::Duration::from_secs(10), stream.run(cancel))
                .await
                .expect(
                    "Stream::run must not deadlock when backlog exceeds input channel capacity",
                );
        assert!(
            run_result.is_ok(),
            "large replay must complete without error (got {run_result:?})"
        );

        let received = recording_output.received.lock().await.clone();
        assert_eq!(
            received.len(),
            ENTRY_COUNT as usize,
            "every replayed entry must reach the output"
        );
        for (i, v) in received.iter().enumerate() {
            assert_eq!(*v, i as i64, "replay must preserve sequence order");
        }

        assert_eq!(
            wal.cursor(),
            ENTRY_COUNT as u64,
            "cursor must advance past every replayed entry once acked"
        );

        wal.close().await.unwrap();
    }
}

/// Stream configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StreamConfig {
    pub input: crate::input::InputConfig,
    pub pipeline: crate::pipeline::PipelineConfig,
    pub output: crate::output::OutputConfig,
    pub error_output: Option<crate::output::OutputConfig>,
    pub buffer: Option<crate::buffer::BufferConfig>,
    pub durability: Option<WalConfig>,
    pub temporary: Option<Vec<crate::temporary::TemporaryConfig>>,
}

impl StreamConfig {
    /// Build stream based on configuration
    pub fn build(&self) -> Result<Stream, Error> {
        let mut resource = Resource {
            temporary: HashMap::new(),
            input_names: RefCell::default(),
        };

        if let Some(temporary_configs) = &self.temporary {
            resource.temporary = HashMap::with_capacity(temporary_configs.len());
            for temporary_config in temporary_configs {
                resource.temporary.insert(
                    temporary_config.name.clone(),
                    temporary_config.build(&resource)?,
                );
            }
        };

        let input = self.input.build(&resource)?;
        let (pipeline, thread_num) = self.pipeline.build(&resource)?;
        let output = self.output.build(&resource)?;
        let error_output = if let Some(error_output_config) = &self.error_output {
            Some(error_output_config.build(&resource)?)
        } else {
            None
        };
        let buffer = if let Some(buffer_config) = &self.buffer {
            Some(buffer_config.build(&resource)?)
        } else {
            None
        };
        // Open the durable ingest WAL only when a `durability:` section is
        // present and enabled. Absent or disabled → today's in-memory behavior.
        let wal = if let Some(wal_config) = &self.durability {
            if wal_config.enabled {
                Some(Wal::open(wal_config)?)
            } else {
                None
            }
        } else {
            None
        };

        Ok(Stream::new(
            input,
            pipeline,
            output,
            error_output,
            buffer,
            wal,
            resource,
            thread_num,
        ))
    }
}
