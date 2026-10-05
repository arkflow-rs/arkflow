//! Columnar window operator: vectorized tumbling window assignment with
//! keyed aggregation state, watermark and processing-time triggers.
//!
//! Assignment is O(1) full-column computations over the timestamp column
//! (`window_start = ts.div_euclid(size) * size`); rows are grouped per batch
//! and merged into per-(window, key) aggregate buffers held in the state
//! backend. Watermarks (or the processing-time trigger) fire windows whose
//! end has passed, emitting the aggregate batch downstream.

mod aggregate;
mod firing;
mod operator;

#[cfg(test)]
mod tests;

pub(crate) use firing::{WindowKind, WindowOperatorConfig, WindowTrigger};
pub(crate) use operator::ColumnarWindowOperator;

// Names below exist for the extracted `tests` module (and the test modules
// nested inside it), reached through `use super::*` /
// `use crate::executor::window::*`; they are compiled out of the non-test
// library build.
#[cfg(test)]
pub(crate) use aggregate::{
    decode_buffer, encode_buffer, numeric_array, AggregateBuffer, NumericKind, NumericValue,
};
#[cfg(test)]
pub(crate) use firing::default_max_buffered_keys;
#[cfg(test)]
pub(crate) use operator::{
    append_late_session_output, compensate_window_acks, filter_window_batch,
    mark_late_session_batch,
};

// Bindings the extracted test modules reach through glob imports, mirroring
// the former file-level import list (test-used subset only).
#[cfg(test)]
use crate::input::Ack;
#[cfg(test)]
use crate::job::LateEventPolicy;
#[cfg(test)]
use crate::processor::Processor;
#[cfg(test)]
use crate::state::StateBackend;
#[cfg(test)]
use crate::Error;
#[cfg(test)]
use crate::MessageBatchRef;
#[cfg(test)]
use crate::ProcessResult;
#[cfg(test)]
use async_trait::async_trait;
#[cfg(test)]
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, StringArray, UInt64Array,
};
#[cfg(test)]
use datafusion::arrow::datatypes::{DataType, Field, Schema};
#[cfg(test)]
use datafusion::arrow::record_batch::RecordBatch;
#[cfg(test)]
use std::sync::Arc;
