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

//! Input component module
//!
//! The input component is responsible for receiving data from various sources such as message queues, file systems, HTTP endpoints, and so on.

use arkflow_core::Error;

pub mod codec_helper;
pub mod file;
pub mod generate;
pub mod http;
pub mod kafka;
pub mod memory;
pub mod modbus;
pub mod mqtt;
pub mod multiple_inputs;
pub mod nats;
pub mod pulsar;
pub mod redis;
pub mod sql;
pub mod websocket;

fn register_components() -> Result<(), Error> {
    generate::init()?;
    http::init()?;
    kafka::init()?;
    memory::init()?;
    mqtt::init()?;
    nats::init()?;
    pulsar::init()?;
    redis::init()?;
    sql::init()?;
    websocket::init()?;
    multiple_inputs::init()?;
    modbus::init()?;
    file::init()?;
    Ok(())
}

/// Component registration is process-global, so `init()` is idempotent:
/// the first successful call registers every builder and later calls
/// (tests, multi-entry binaries) short-circuit without touching the
/// registries again. A failed registration is NOT cached — the next call
/// re-runs it, so a transient failure does not permanently break the
/// process (see `init_latched`).
static INIT: std::sync::Mutex<Option<()>> = std::sync::Mutex::new(None);

pub fn init() -> Result<(), Error> {
    crate::init_latched(&INIT, register_components)
}
