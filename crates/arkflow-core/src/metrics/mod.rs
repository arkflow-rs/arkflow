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

//! Metrics module for Prometheus monitoring
//!
//! This module provides Prometheus metrics export functionality for monitoring
//! the stream processing engine. It includes:
//! - Core metric definitions (counters, gauges, histograms)
//! - Metric registry management
//! - HTTP endpoint for metrics scraping

pub mod definitions;
pub mod registry;

pub use definitions::*;
pub use registry::*;
