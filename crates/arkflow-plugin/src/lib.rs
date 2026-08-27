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

pub mod buffer;
pub mod codec;
pub mod component;
pub mod context_pool;
pub mod expr;
pub mod input;
pub mod output;
pub mod processor;
pub mod pulsar;
pub mod rate_limiter;
pub mod temporary;
pub mod time;
pub mod udf;
pub mod wal;

use arkflow_core::Error;
use std::sync::OnceLock;

static INITIALIZATION: OnceLock<Result<(), String>> = OnceLock::new();

/// Register the built-in component catalogue once per process.
///
/// Both the local Engine and the standalone Hub expose this metadata to
/// operators, so their startup paths must share one idempotent initializer.
pub fn initialize() -> Result<(), Error> {
    match INITIALIZATION.get_or_init(|| {
        input::init()
            .and_then(|_| output::init())
            .and_then(|_| processor::init())
            .and_then(|_| buffer::init())
            .and_then(|_| temporary::init())
            .and_then(|_| codec::init())
            .and_then(|_| wal::init())
            .map_err(|error| error.to_string())
    }) {
        Ok(()) => Ok(()),
        Err(error) => Err(Error::Config(error.clone())),
    }
}
