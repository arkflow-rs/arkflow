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

#[cfg(feature = "connectors-amqp09")]
pub mod amqp09;
pub mod codec_helper;
pub mod file;
pub mod generate;
pub mod http;
#[cfg(feature = "connectors-kafka")]
pub mod kafka;
pub mod memory;
#[cfg(feature = "connectors-modbus")]
pub mod modbus;
#[cfg(feature = "connectors-mqtt")]
pub mod mqtt;
pub mod multiple_inputs;
#[cfg(feature = "connectors-nats")]
pub mod nats;
#[cfg(feature = "connectors-pulsar")]
pub mod pulsar;
#[cfg(feature = "connectors-redis")]
pub mod redis;
#[cfg(feature = "connectors-sql")]
pub mod sql;
#[cfg(feature = "connectors-websocket")]
pub mod websocket;

fn register_components() -> Result<(), Error> {
    // `mut` is exercised only when family features append their init steps;
    // with every family off the vec is complete at construction.
    #[allow(unused_mut)]
    let mut steps: Vec<fn() -> Result<(), Error>> = vec![
        generate::init,
        http::init,
        memory::init,
        multiple_inputs::init,
        file::init,
    ];
    #[cfg(feature = "connectors-amqp09")]
    steps.push(amqp09::init);
    #[cfg(feature = "connectors-kafka")]
    steps.push(kafka::init);
    #[cfg(feature = "connectors-mqtt")]
    steps.push(mqtt::init);
    #[cfg(feature = "connectors-nats")]
    steps.push(nats::init);
    #[cfg(feature = "connectors-pulsar")]
    steps.push(pulsar::init);
    #[cfg(feature = "connectors-redis")]
    steps.push(redis::init);
    #[cfg(feature = "connectors-sql")]
    steps.push(sql::init);
    #[cfg(feature = "connectors-websocket")]
    steps.push(websocket::init);
    #[cfg(feature = "connectors-modbus")]
    steps.push(modbus::init);
    crate::init_steps(&COMPLETED, &steps)
}

/// Per-component success markers so a retry after a later component failed
/// does not re-register an earlier one.
static COMPLETED: std::sync::Mutex<std::collections::BTreeSet<usize>> =
    std::sync::Mutex::new(std::collections::BTreeSet::new());

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
