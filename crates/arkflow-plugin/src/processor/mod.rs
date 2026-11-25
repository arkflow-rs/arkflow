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

//! Processor component module
//!
//! The processor component is responsible for transforming, filtering, enriching, and so on.

use arkflow_core::Error;

pub mod batch;
#[cfg(feature = "ai-vector")]
pub mod embedding;
pub mod json;
#[cfg(feature = "ai-vector")]
pub mod llm;
pub mod lookup;
#[cfg(feature = "ai-vector")]
pub mod milvus_search;
#[cfg(feature = "ai-vector")]
pub mod pgvector_search;
#[cfg(feature = "codecs-protobuf")]
pub mod protobuf;
#[cfg(feature = "connectors-python")]
pub mod python;
pub mod sql;
#[cfg(feature = "ai-vector")]
pub mod vector_search;
#[cfg(feature = "processors-vrl")]
pub mod vrl;

fn register_components() -> Result<(), Error> {
    // `mut` is exercised only when family features append their init steps;
    // with every family off the vec is complete at construction.
    #[allow(unused_mut)]
    let mut steps: Vec<fn() -> Result<(), Error>> =
        vec![batch::init, json::init, lookup::init, sql::init];
    #[cfg(feature = "ai-vector")]
    steps.push(llm::init);
    #[cfg(feature = "ai-vector")]
    steps.push(pgvector_search::init);
    #[cfg(feature = "codecs-protobuf")]
    steps.push(protobuf::init);
    #[cfg(feature = "processors-vrl")]
    steps.push(vrl::init);
    #[cfg(feature = "connectors-python")]
    steps.push(python::init);
    #[cfg(feature = "ai-vector")]
    steps.push(embedding::init);
    #[cfg(feature = "ai-vector")]
    steps.push(vector_search::init);
    #[cfg(feature = "ai-vector")]
    steps.push(milvus_search::init);
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
