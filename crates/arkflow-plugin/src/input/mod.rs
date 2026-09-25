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

/// Component registration is process-global, so `init()` is idempotent: the
/// first call registers every builder and later calls (tests, multi-entry
/// binaries) return the first result without touching the registries again.
/// A registration failure is stored and re-returned, so the process cannot
/// end up with a silently partial registry that still reports successful
/// initialization.
pub fn init() -> Result<(), Error> {
    static INIT: std::sync::OnceLock<Result<(), String>> = std::sync::OnceLock::new();
    match INIT.get_or_init(|| register_components().map_err(|error| error.to_string())) {
        Ok(()) => Ok(()),
        Err(error) => Err(Error::Config(error.clone())),
    }
}
