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

pub mod benchmark;
pub mod buffer;
pub mod codec;
pub mod component;
pub mod context_pool;
pub mod expr;
pub mod input;
pub mod kafka_security;
pub mod kafka_txn;
pub mod mqtt_tls;
pub mod output;
pub mod processor;
pub mod pulsar;
pub mod rate_limiter;
pub mod temporary;
pub mod time;
pub mod udf;
pub mod vector_util;
pub mod wal;

use arkflow_core::Error;
use std::sync::Mutex;

/// Successful initialization is latched once per process; failures are not
/// cached, so a later `initialize()` call retries the registration chain.
static INITIALIZATION: Mutex<Option<()>> = Mutex::new(None);

/// Per-kind success latches: the per-kind `init()` functions are insert-only
/// (re-running one reports duplicate registration), so a retried chain must
/// skip the kinds that already succeeded and resume at the first failure.
/// The latches live here rather than inside each kind's `init()` so the
/// retry policy stays in one place.
static INPUT_DONE: Mutex<Option<()>> = Mutex::new(None);
static OUTPUT_DONE: Mutex<Option<()>> = Mutex::new(None);
static PROCESSOR_DONE: Mutex<Option<()>> = Mutex::new(None);
static BUFFER_DONE: Mutex<Option<()>> = Mutex::new(None);
static TEMPORARY_DONE: Mutex<Option<()>> = Mutex::new(None);
static CODEC_DONE: Mutex<Option<()>> = Mutex::new(None);
static WAL_DONE: Mutex<Option<()>> = Mutex::new(None);

/// Register the built-in component catalogue once per process.
///
/// Both the local Engine and the standalone Hub expose this metadata to
/// operators, so their startup paths must share one idempotent initializer.
/// A failed registration run is retried on the next call rather than cached:
/// kinds that already registered are skipped, so the retry resumes where
/// the failure happened instead of tripping over duplicate registrations.
pub fn initialize() -> Result<(), Error> {
    let mut guard = INITIALIZATION
        .lock()
        .map_err(|_| Error::Config("plugin initialization lock poisoned".to_string()))?;
    if guard.is_some() {
        return Ok(());
    }
    let outcome = register_components();
    if outcome.is_ok() {
        *guard = Some(());
    }
    outcome
}

/// Run one registration step under its success latch: an already-registered
/// kind short-circuits, a failure stays unlatched (retryable), a success is
/// latched exactly once.
fn init_step(
    latch: &'static Mutex<Option<()>>,
    init: fn() -> Result<(), Error>,
) -> Result<(), Error> {
    let mut guard = latch
        .lock()
        .map_err(|_| Error::Config("plugin initialization lock poisoned".to_string()))?;
    if guard.is_some() {
        return Ok(());
    }
    let outcome = init();
    if outcome.is_ok() {
        *guard = Some(());
    }
    outcome
}

fn register_components() -> Result<(), Error> {
    run_registration_chain().map_err(|error| Error::Config(error.to_string()))
}

fn run_registration_chain() -> Result<(), Error> {
    init_step(&INPUT_DONE, input::init)?;
    init_step(&OUTPUT_DONE, output::init)?;
    init_step(&PROCESSOR_DONE, processor::init)?;
    init_step(&BUFFER_DONE, buffer::init)?;
    init_step(&TEMPORARY_DONE, temporary::init)?;
    init_step(&CODEC_DONE, codec::init)?;
    init_step(&WAL_DONE, wal::init)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU8, Ordering};

    /// Success latches: the chain runs once, later calls short-circuit.
    #[test]
    fn initialize_latches_success() {
        initialize().expect("first init succeeds");
        initialize().expect("latched init succeeds");
    }

    /// A failed step stays retryable: the failure is not cached, the retry
    /// re-invokes the step, and once it succeeds the latch short-circuits
    /// all later calls. This is the resume path a mid-chain failure takes.
    #[test]
    fn init_step_latches_success_and_retries_failure() {
        static LATCH: Mutex<Option<()>> = Mutex::new(None);
        static CALLS: AtomicU8 = AtomicU8::new(0);
        fn fail_once_then_succeed() -> Result<(), Error> {
            if CALLS.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(Error::Process("transient registration failure".into()));
            }
            Ok(())
        }
        fn unused() -> Result<(), Error> {
            unreachable!("latched steps must not invoke their init fn");
        }
        assert!(
            init_step(&LATCH, fail_once_then_succeed).is_err(),
            "first failure surfaces"
        );
        assert_eq!(CALLS.load(Ordering::SeqCst), 1);
        init_step(&LATCH, fail_once_then_succeed).expect("retry is not cached");
        // Latched: the init fn is never invoked again.
        init_step(&LATCH, unused).expect("latched step short-circuits");
        assert_eq!(CALLS.load(Ordering::SeqCst), 2);
    }
}
