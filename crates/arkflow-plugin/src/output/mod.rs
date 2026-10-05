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

//! Output component module
//!
//! The output component is responsible for sending the processed data to the target system.

use arkflow_core::Error;

pub mod codec_helper;
pub mod drop;
pub mod http;
pub mod influxdb;
pub mod kafka;
pub mod milvus;
pub mod mongodb;
pub mod mqtt;
pub mod nats;
pub mod payload;
pub mod pgvector;
pub mod pulsar;
pub mod qdrant;
pub mod redis;
pub mod sql;
pub mod stdout;

fn register_components() -> Result<(), Error> {
    drop::init()?;
    http::init()?;
    influxdb::init()?;
    kafka::init()?;
    milvus::init()?;
    mqtt::init()?;
    mongodb::init()?;
    nats::init()?;
    pgvector::init()?;
    pulsar::init()?;
    qdrant::init()?;
    redis::init()?;
    sql::init()?;
    stdout::init()?;
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
