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

//! Transaction module for exactly-once semantics
//!
//! This module provides the infrastructure for two-phase commit (2PC),
//! write-ahead logging (WAL), and idempotency tracking to ensure
//! exactly-once processing guarantees.

pub mod coordinator;
pub mod idempotency;
pub mod types;
pub mod wal;

pub use coordinator::{TransactionCoordinator, TransactionCoordinatorConfig};
pub use idempotency::{IdempotencyCache, IdempotencyConfig};
// Re-export commonly used types
pub use types::{TransactionId, TransactionRecord, TransactionState};
pub use wal::{FileWal, WalConfig, WriteAheadLog};
