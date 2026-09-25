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
