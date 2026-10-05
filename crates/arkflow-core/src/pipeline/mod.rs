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

//! Pipeline Component Module
//!
//! A pipeline is an ordered collection of processors that defines how data flows from input to output, through a series of processing steps.

use serde::{Deserialize, Serialize};

/// Pipeline configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    #[serde(default = "default_thread_num")]
    pub thread_num: u32,
    pub processors: Vec<crate::processor::ProcessorConfig>,
}

pub(crate) fn default_thread_num() -> u32 {
    num_cpus::get() as u32
}
