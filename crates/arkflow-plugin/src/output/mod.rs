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

#[cfg(feature = "connectors-amqp09")]
pub mod amqp09;
pub mod codec_helper;
pub mod drop;
pub mod http;
#[cfg(feature = "connectors-iceberg")]
pub mod iceberg;
pub mod influxdb;
#[cfg(feature = "connectors-kafka")]
pub mod kafka;
#[cfg(feature = "ai-vector")]
pub mod milvus;
#[cfg(feature = "connectors-mongodb")]
pub mod mongodb;
#[cfg(feature = "connectors-mqtt")]
pub mod mqtt;
#[cfg(feature = "connectors-nats")]
pub mod nats;
pub mod payload;
#[cfg(feature = "ai-vector")]
pub mod pgvector;
#[cfg(feature = "connectors-pulsar")]
pub mod pulsar;
#[cfg(feature = "ai-vector")]
pub mod qdrant;
#[cfg(feature = "connectors-redis")]
pub mod redis;
#[cfg(feature = "connectors-sql")]
pub mod sql;
pub mod stdout;

fn register_components() -> Result<(), Error> {
    // `mut` is exercised only when family features append their init steps;
    // with every family off the vec is complete at construction.
    #[allow(unused_mut)]
    let mut steps: Vec<fn() -> Result<(), Error>> =
        vec![drop::init, http::init, influxdb::init, stdout::init];
    #[cfg(feature = "connectors-amqp09")]
    steps.push(amqp09::init);
    #[cfg(feature = "connectors-kafka")]
    steps.push(kafka::init);
    #[cfg(feature = "connectors-iceberg")]
    steps.push(iceberg::init);
    #[cfg(feature = "ai-vector")]
    steps.push(milvus::init);
    #[cfg(feature = "connectors-mqtt")]
    steps.push(mqtt::init);
    #[cfg(feature = "connectors-mongodb")]
    steps.push(mongodb::init);
    #[cfg(feature = "connectors-nats")]
    steps.push(nats::init);
    #[cfg(feature = "ai-vector")]
    steps.push(pgvector::init);
    #[cfg(feature = "connectors-pulsar")]
    steps.push(pulsar::init);
    #[cfg(feature = "ai-vector")]
    steps.push(qdrant::init);
    #[cfg(feature = "connectors-redis")]
    steps.push(redis::init);
    #[cfg(feature = "connectors-sql")]
    steps.push(sql::init);
    crate::init_steps(&COMPLETED, &steps)
}

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
