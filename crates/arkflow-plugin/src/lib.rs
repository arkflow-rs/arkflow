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

/// Run one registration step under a success-only latch.
///
/// Component registration is insert-only (registering an already-registered
/// name reports a duplicate), so a step that already succeeded
/// short-circuits, while a failure stays unlatched: the next call re-runs
/// the step and can succeed once the failure cause is resolved. The latch
/// also serializes concurrent first calls of the same step.
///
/// Every kind's `init()` routes through this helper (and multi-step
/// registrations latch per step), which is what makes `initialize()`
/// retryable end to end — an inner `OnceLock<Result<..>>` would cache the
/// first failure and defeat the retry.
pub(crate) fn init_latched(
    latch: &'static Mutex<Option<()>>,
    register: fn() -> Result<(), Error>,
) -> Result<(), Error> {
    let mut guard = latch
        .lock()
        .map_err(|_| Error::Config("plugin initialization lock poisoned".to_string()))?;
    if guard.is_some() {
        return Ok(());
    }
    let outcome = register();
    if outcome.is_ok() {
        *guard = Some(());
    }
    outcome
}

/// Register the built-in component catalogue once per process.
///
/// Both the local Engine and the standalone Hub expose this metadata to
/// operators, so their startup paths must share one idempotent initializer.
/// A failed registration run is not cached: the next call re-runs the chain,
/// every already-registered kind short-circuits through its own success
/// latch, and the retry resumes at the first failed kind.
pub fn initialize() -> Result<(), Error> {
    input::init()?;
    output::init()?;
    processor::init()?;
    buffer::init()?;
    temporary::init()?;
    codec::init()?;
    wal::init()
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

    /// Every kind's init() is directly re-runnable: direct callers (the
    /// benchmark harness, integration tests) bypass `initialize()`, so the
    /// per-kind success latches — not a top-level cache — carry the
    /// idempotency.
    #[test]
    fn kind_inits_are_directly_idempotent() {
        input::init().expect("input re-init");
        output::init().expect("output re-init");
        processor::init().expect("processor re-init");
        buffer::init().expect("buffer re-init");
        temporary::init().expect("temporary re-init");
        codec::init().expect("codec re-init");
        wal::init().expect("wal re-init");
    }

    /// A failed step stays retryable: the failure is not cached, the retry
    /// re-invokes the step, and once it succeeds the latch short-circuits
    /// all later calls.
    #[test]
    fn init_latched_retries_failure_and_latches_success() {
        static LATCH: Mutex<Option<()>> = Mutex::new(None);
        static CALLS: AtomicU8 = AtomicU8::new(0);
        fn fail_once_then_succeed() -> Result<(), Error> {
            if CALLS.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(Error::Process("transient registration failure".into()));
            }
            Ok(())
        }
        fn unused() -> Result<(), Error> {
            unreachable!("latched steps must not invoke their register fn");
        }
        assert!(
            init_latched(&LATCH, fail_once_then_succeed).is_err(),
            "first failure surfaces"
        );
        assert_eq!(CALLS.load(Ordering::SeqCst), 1);
        init_latched(&LATCH, fail_once_then_succeed).expect("retry is not cached");
        // Latched: the register fn is never invoked again.
        init_latched(&LATCH, unused).expect("latched step short-circuits");
        assert_eq!(CALLS.load(Ordering::SeqCst), 2);
    }
}
