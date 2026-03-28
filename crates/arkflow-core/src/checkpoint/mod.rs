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

//! Checkpoint mechanism for fault tolerance
//!
//! This module provides state snapshot and recovery capabilities for ArkFlow streams,
//! enabling automatic recovery from failures without data loss.

pub mod barrier;
pub mod coordinator;
pub mod metadata;
pub mod state;
pub mod storage;

pub use barrier::{Barrier, BarrierId, BarrierManager};
pub use coordinator::{CheckpointConfig, CheckpointCoordinator};
pub use metadata::{CheckpointId, CheckpointMetadata, CheckpointStatus};
pub use state::{StateSerializer, StateSnapshot};
pub use storage::{CheckpointStorage, CloudStorage, LocalFileStorage};

use crate::Error;

/// Result type for checkpoint operations
pub type CheckpointResult<T> = Result<T, Error>;
