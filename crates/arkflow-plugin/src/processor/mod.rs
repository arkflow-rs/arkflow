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
pub mod embedding;
pub mod json;
pub mod llm;
pub mod milvus_search;
pub mod pgvector_search;
pub mod protobuf;
pub mod python;
pub mod sql;
pub mod vector_search;
pub mod vrl;

fn register_components() -> Result<(), Error> {
    batch::init()?;
    json::init()?;
    llm::init()?;
    pgvector_search::init()?;
    protobuf::init()?;
    sql::init()?;
    vrl::init()?;
    python::init()?;
    embedding::init()?;
    vector_search::init()?;
    milvus_search::init()?;
    Ok(())
}

/// Component registration is process-global, so `init()` is idempotent: the
/// first call registers every builder and later calls (tests, multi-entry
/// binaries) return immediately without touching the registries again.
pub fn init() -> Result<(), Error> {
    static INIT: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    INIT.get_or_init(|| {
        let _ = register_components();
    });
    Ok(())
}
