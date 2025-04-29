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

//! Rust stream processing engine

use crate::message::{Message, Value};
use chrono::{DateTime, Timelike};
use datafusion::arrow;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, DurationMicrosecondArray,
    DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray, Float16Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray,
    NullArray, StringArray, StringViewArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::data_type::AsBytes;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use thiserror::Error;

pub mod buffer;
pub mod cli;
pub mod config;
pub mod engine;
pub mod input;
pub mod output;
pub mod pipeline;
pub mod processor;

pub mod message;
pub mod stream;

/// Error in the stream processing engine
#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Read error: {0}")]
    Read(String),

    #[error("Process errors: {0}")]
    Process(String),

    #[error("Connection error: {0}")]
    Connection(String),

    /// Reconnection should be attempted after a connection loss.
    #[error("Connection lost")]
    Disconnection,

    #[error("Timeout error")]
    Timeout,

    #[error("Unknown error: {0}")]
    Unknown(String),

    #[error("EOF")]
    EOF,
}

/// Represents a message in a stream processing engine.
#[derive(Clone, Debug)]
pub struct MessageBatch(Vec<Message>);

impl MessageBatch {
    pub fn new_binary(content: Vec<Vec<u8>>) -> Result<Self, Error> {
        Ok(Self(
            content
                .into_iter()
                .map(|x| Value::Bytes(x.into()).into())
                .collect(),
        ))
    }

    pub fn new_binary_with_origin(&self, content: Vec<Vec<u8>>) -> Result<Self, Error> {
        // let schema = self.schema();
        // let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
        //
        // fields.push(Arc::new(Field::new(
        //     DEFAULT_BINARY_VALUE_FIELD,
        //     DataType::Binary,
        //     false,
        // )));
        // let new_schema = Arc::new(Schema::new(fields));
        //
        // let mut columns: Vec<ArrayRef> = Vec::new();
        // for i in 0..schema.fields().len() {
        //     columns.push(self.column(i).clone());
        // }
        //
        // let binary_data: Vec<&[u8]> = content.iter().map(|v| v.as_slice()).collect();
        // columns.push(Arc::new(BinaryArray::from(binary_data)));
        //
        // let new_msg = RecordBatch::try_new(new_schema, columns)
        //     .map_err(|e| Error::Process(format!("Creating an Arrow record batch failed: {}", e)))?;
        // Ok(MessageBatch::new_arrow(new_msg))
        todo!()
    }

    pub fn filter_columns(
        &self,
        field_names_to_include: &HashSet<String>,
    ) -> Result<MessageBatch, Error> {
        // let schema = self.schema();
        //
        // let cap = field_names_to_include.len();
        // let mut new_columns = Vec::with_capacity(cap);
        // let mut fields = Vec::with_capacity(cap);
        //
        // for (i, col) in self.columns().iter().enumerate() {
        //     let field = schema.field(i);
        //     let name = field.name();
        //
        //     if field_names_to_include.contains(name.as_str()) {
        //         new_columns.push(col.clone());
        //         fields.push(field.clone());
        //     }
        // }
        //
        // let new_schema: SchemaRef = SchemaRef::new(Schema::new(fields));
        // let batch = RecordBatch::try_new(new_schema, new_columns)
        //     .map_err(|e| Error::Process(format!("Creating an Arrow record batch failed: {}", e)))?;
        // Ok(batch.into())
        todo!()
    }

    pub fn from_json<T: Serialize>(value: &T) -> Result<Self, Error> {
        let content = serde_json::to_vec(value)?;
        Ok(Self::new_binary(vec![content])?)
    }

    /// Create a message from a string.
    pub fn from_string(content: &str) -> Result<Self, Error> {
        Self::new_binary(vec![content.as_bytes().to_vec()])
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn to_binary(&self) -> Result<Vec<&[u8]>, Error> {
        let mut vec_bytes = Vec::with_capacity(self.len());

        for x in &self.0 {
            match x.value {
                Value::Bytes(ref v) => vec_bytes.push(v.as_bytes()),
                _ => {
                    return Err(Error::Process("not support data type".to_string()));
                }
            }
        }
        Ok(vec_bytes)
    }
}

impl Deref for MessageBatch {
    type Target = Vec<Message>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryInto<RecordBatch> for MessageBatch {
    type Error = Error;

    fn try_into(self) -> Result<RecordBatch, Self::Error> {
        let mut batches = Vec::with_capacity(self.len());
        for x in self.0 {
            match x.value {
                Value::Object(obj) => batches.push(Self::object_to_recordbatch(obj)?),
                _ => {
                    return Err(Error::Process("not support data type".to_string()));
                }
            }
        }

        let schema = batches[0].schema();
        arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| Error::Process(format!("Merge batches failed: {}", e)))
    }
}

impl MessageBatch {
    fn object_to_recordbatch(value: BTreeMap<String, Value>) -> Result<RecordBatch, Error> {
        let mut fields = Vec::with_capacity(value.len());
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(value.len());

        for (k, v) in value {
            match v {
                Value::Null => {
                    fields.push(Field::new(k, DataType::Null, true));
                    cols.push(Arc::new(NullArray::new(1)));
                }
                Value::Int8(v) => {
                    fields.push(Field::new(k, DataType::Int8, true));
                    cols.push(Arc::new(Int8Array::from(vec![v])))
                }
                Value::Int16(v) => {
                    fields.push(Field::new(k, DataType::Int16, true));
                    cols.push(Arc::new(Int16Array::from(vec![v])))
                }
                Value::Int32(v) => {
                    fields.push(Field::new(k, DataType::Int32, true));
                    cols.push(Arc::new(Int32Array::from(vec![v])))
                }
                Value::Bytes(v) => {
                    fields.push(Field::new(k, DataType::Binary, true));
                    cols.push(Arc::new(BinaryArray::from(vec![v.as_bytes()])))
                }
                Value::Float32(v) => {
                    fields.push(Field::new(k, DataType::Float32, true));
                    cols.push(Arc::new(Float32Array::from(vec![v])))
                }
                Value::Float64(v) => {
                    fields.push(Field::new(k, DataType::Float64, true));
                    cols.push(Arc::new(Float64Array::from(vec![v])))
                }
                Value::Int64(v) => {
                    fields.push(Field::new(k, DataType::Int64, true));
                    cols.push(Arc::new(Int64Array::from(vec![v])))
                }
                Value::Uint8(v) => {
                    fields.push(Field::new(k, DataType::UInt8, true));
                    cols.push(Arc::new(UInt8Array::from(vec![v])))
                }
                Value::Uint16(v) => {
                    fields.push(Field::new(k, DataType::UInt16, true));
                    cols.push(Arc::new(UInt16Array::from(vec![v])))
                }
                Value::Uint32(v) => {
                    fields.push(Field::new(k, DataType::UInt32, true));
                    cols.push(Arc::new(UInt32Array::from(vec![v])))
                }
                Value::Uint64(v) => {
                    fields.push(Field::new(k, DataType::UInt64, true));
                    cols.push(Arc::new(UInt64Array::from(vec![v])))
                }
                Value::String(v) => {
                    fields.push(Field::new(k, DataType::Utf8, true));
                    cols.push(Arc::new(StringArray::from(vec![v])))
                }
                Value::Bool(v) => {
                    fields.push(Field::new(k, DataType::Boolean, true));
                    cols.push(Arc::new(BooleanArray::from(vec![v])))
                }

                Value::Timestamp(v) => {
                    fields.push(Field::new(
                        k,
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        true,
                    ));
                    cols.push(Arc::new(TimestampNanosecondArray::from(vec![
                        v.nanosecond() as i64,
                    ])))
                }
                _ => {
                    return Err(Error::Process("not support data type".to_string()));
                }
            }
        }
        let schema = SchemaRef::new(Schema::new(fields));

        todo!()
    }
}
impl TryFrom<RecordBatch> for MessageBatch {
    type Error = Error;

    fn try_from(value: RecordBatch) -> Result<Self, Self::Error> {
        let rows = value.num_rows();
        let mut message_batch = Vec::with_capacity(rows);
        for _ in 0..rows {
            message_batch.push(BTreeMap::new())
        }

        let schema = value.schema();
        for (i, field) in schema.fields().iter().enumerate() {
            let field_name = field.name();
            let col = value.column(i);
            match field.data_type() {
                DataType::Null => {
                    let Some(value) = col.as_any().downcast_ref::<NullArray>() else {
                        continue;
                    };
                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            obj.insert(field_name.clone(), Value::Null);
                        }
                    }
                }
                DataType::Boolean => {
                    let Some(value) = col.as_any().downcast_ref::<BooleanArray>() else {
                        continue;
                    };
                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            obj.insert(field_name.clone(), Value::Bool(value.value(j)));
                        }
                    }
                }
                DataType::Int8 => {
                    let Some(value) = col.as_any().downcast_ref::<Int8Array>() else {
                        continue;
                    };
                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            obj.insert(field_name.clone(), Value::Int8(value.value(j)));
                        }
                    }
                }
                DataType::Int16 => {
                    let Some(value) = col.as_any().downcast_ref::<Int16Array>() else {
                        continue;
                    };
                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            obj.insert(field_name.clone(), Value::Int16(value.value(j)));
                        }
                    }
                }
                DataType::Int32 => {
                    let Some(value) = col.as_any().downcast_ref::<Int32Array>() else {
                        continue;
                    };

                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            obj.insert(field_name.clone(), Value::Int32(value.value(j)));
                        }
                    }
                }
                DataType::Int64 => {
                    let Some(value) = col.as_any().downcast_ref::<Int64Array>() else {
                        continue;
                    };

                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            obj.insert(field_name.clone(), Value::Int64(value.value(j)));
                        }
                    }
                }
                DataType::UInt8 => {
                    let Some(value) = col.as_any().downcast_ref::<UInt8Array>() else {
                        continue;
                    };

                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            obj.insert(field_name.clone(), Value::Uint8(value.value(j)));
                        }
                    }
                }
                DataType::UInt16 => {
                    let Some(value) = col.as_any().downcast_ref::<UInt16Array>() else {
                        continue;
                    };
                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            obj.insert(field_name.clone(), Value::Uint16(value.value(j)));
                        }
                    }
                }
                DataType::UInt32 => {
                    let Some(value) = col.as_any().downcast_ref::<UInt32Array>() else {
                        continue;
                    };
                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            obj.insert(field_name.clone(), Value::Uint32(value.value(j)));
                        }
                    }
                }
                DataType::UInt64 => {
                    let Some(value) = col.as_any().downcast_ref::<UInt64Array>() else {
                        continue;
                    };
                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            obj.insert(field_name.clone(), Value::Uint64(value.value(j)));
                        }
                    }
                }
                DataType::Float16 => {
                    let Some(value) = col.as_any().downcast_ref::<Float16Array>() else {
                        continue;
                    };
                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            obj.insert(field_name.clone(), Value::Float32(value.value(j).to_f32()));
                        }
                    }
                }
                DataType::Float32 => {
                    let Some(value) = col.as_any().downcast_ref::<Float32Array>() else {
                        continue;
                    };
                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            obj.insert(field_name.clone(), Value::Float32(value.value(j)));
                        }
                    }
                }
                DataType::Float64 => {
                    let Some(value) = col.as_any().downcast_ref::<Float64Array>() else {
                        continue;
                    };
                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            obj.insert(field_name.clone(), Value::Float64(value.value(j)));
                        }
                    }
                }
                DataType::Timestamp(tu, a) => match tu {
                    TimeUnit::Second => {
                        let Some(value) = col.as_any().downcast_ref::<TimestampSecondArray>()
                        else {
                            continue;
                        };
                        for j in 0..value.len() {
                            if let Some(obj) = message_batch.get_mut(j) {
                                let ts = value.value(j);
                                obj.insert(
                                    field_name.clone(),
                                    Value::Timestamp(DateTime::from_timestamp(ts, 0).unwrap()),
                                );
                            }
                        }
                    }
                    TimeUnit::Millisecond => {
                        let Some(value) = col.as_any().downcast_ref::<TimestampMicrosecondArray>()
                        else {
                            continue;
                        };
                        for j in 0..value.len() {
                            if let Some(obj) = message_batch.get_mut(j) {
                                let ts = value.value(j);
                                obj.insert(
                                    field_name.clone(),
                                    Value::Timestamp(DateTime::from_timestamp_millis(ts).unwrap()),
                                );
                            }
                        }
                    }
                    TimeUnit::Microsecond => {
                        let Some(value) = col.as_any().downcast_ref::<TimestampMicrosecondArray>()
                        else {
                            continue;
                        };
                        for j in 0..value.len() {
                            if let Some(obj) = message_batch.get_mut(j) {
                                let ts = value.value(j);
                                obj.insert(
                                    field_name.clone(),
                                    Value::Timestamp(DateTime::from_timestamp_micros(ts).unwrap()),
                                );
                            }
                        }
                    }
                    TimeUnit::Nanosecond => {
                        let Some(value) = col.as_any().downcast_ref::<TimestampNanosecondArray>()
                        else {
                            continue;
                        };
                        for j in 0..value.len() {
                            if let Some(obj) = message_batch.get_mut(j) {
                                let ts = value.value(j);
                                obj.insert(
                                    field_name.clone(),
                                    Value::Timestamp(DateTime::from_timestamp_nanos(ts)),
                                );
                            }
                        }
                    }
                },
                DataType::Date32 => {
                    let Some(value) = col.as_any().downcast_ref::<Date32Array>() else {
                        continue;
                    };
                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            let ts = value.value(j);
                            obj.insert(
                                field_name.clone(),
                                Value::Timestamp(DateTime::from_timestamp(ts as i64, 0).unwrap()),
                            );
                        }
                    }
                }
                DataType::Date64 => {
                    let Some(value) = col.as_any().downcast_ref::<Date64Array>() else {
                        continue;
                    };
                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            let ts = value.value(j);
                            obj.insert(
                                field_name.clone(),
                                Value::Timestamp(DateTime::from_timestamp_millis(ts).unwrap()),
                            );
                        }
                    }
                }
                DataType::Time32(tu) => match tu {
                    TimeUnit::Second => {
                        let Some(value) = col.as_any().downcast_ref::<Time32SecondArray>() else {
                            continue;
                        };
                        for j in 0..value.len() {
                            if let Some(obj) = message_batch.get_mut(j) {
                                let ts = value.value(j);
                                obj.insert(
                                    field_name.clone(),
                                    Value::Timestamp(
                                        DateTime::from_timestamp(ts as i64, 0).unwrap(),
                                    ),
                                );
                            }
                        }
                    }
                    TimeUnit::Millisecond => {
                        let Some(value) = col.as_any().downcast_ref::<Time32MillisecondArray>()
                        else {
                            continue;
                        };
                        for j in 0..value.len() {
                            if let Some(obj) = message_batch.get_mut(j) {
                                let ts = value.value(j);
                                if let Some(v) = DateTime::from_timestamp_millis(ts as i64) {
                                    obj.insert(field_name.clone(), Value::Timestamp(v));
                                }
                            }
                        }
                    }
                    _ => {}
                },
                DataType::Time64(tu) => match tu {
                    TimeUnit::Microsecond => {
                        let Some(value) = col.as_any().downcast_ref::<Time64MicrosecondArray>()
                        else {
                            continue;
                        };
                        for j in 0..value.len() {
                            if let Some(obj) = message_batch.get_mut(j) {
                                let ts = value.value(j);
                                obj.insert(
                                    field_name.clone(),
                                    Value::Timestamp(DateTime::from_timestamp_micros(ts).unwrap()),
                                );
                            }
                        }
                    }
                    TimeUnit::Nanosecond => {
                        let Some(value) = col.as_any().downcast_ref::<Time64NanosecondArray>()
                        else {
                            continue;
                        };
                        for j in 0..value.len() {
                            if let Some(obj) = message_batch.get_mut(j) {
                                let ts = value.value(j);
                                obj.insert(
                                    field_name.clone(),
                                    Value::Timestamp(DateTime::from_timestamp_nanos(ts)),
                                );
                            }
                        }
                    }
                    _ => {}
                },
                DataType::Duration(tu) => match tu {
                    TimeUnit::Second => {
                        let Some(value) = col.as_any().downcast_ref::<DurationSecondArray>() else {
                            continue;
                        };
                        for j in 0..value.len() {
                            if let Some(obj) = message_batch.get_mut(j) {
                                obj.insert(
                                    field_name.clone(),
                                    Value::String(format!("{}s", value.value(j))),
                                );
                            }
                        }
                    }
                    TimeUnit::Millisecond => {
                        let Some(value) = col.as_any().downcast_ref::<DurationMillisecondArray>()
                        else {
                            continue;
                        };
                        for j in 0..value.len() {
                            if let Some(obj) = message_batch.get_mut(j) {
                                obj.insert(
                                    field_name.clone(),
                                    Value::String(format!("{}ms", value.value(j))),
                                );
                            }
                        }
                    }
                    TimeUnit::Microsecond => {
                        let Some(value) = col.as_any().downcast_ref::<DurationMicrosecondArray>()
                        else {
                            continue;
                        };
                        for j in 0..value.len() {
                            if let Some(obj) = message_batch.get_mut(j) {
                                obj.insert(
                                    field_name.clone(),
                                    Value::String(format!("{}us", value.value(j))),
                                );
                            }
                        }
                    }
                    TimeUnit::Nanosecond => {
                        let Some(value) = col.as_any().downcast_ref::<DurationNanosecondArray>()
                        else {
                            continue;
                        };
                        for j in 0..value.len() {
                            if let Some(obj) = message_batch.get_mut(j) {
                                obj.insert(
                                    field_name.clone(),
                                    Value::String(format!("{}ns", value.value(j))),
                                );
                            }
                        }
                    }
                },
                DataType::Binary => {
                    let Some(value) = col.as_any().downcast_ref::<BinaryArray>() else {
                        continue;
                    };
                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            obj.insert(
                                field_name.clone(),
                                Value::Bytes(value.value(j).to_vec().into()),
                            );
                        }
                    }
                }
                DataType::Utf8 => {
                    let Some(value) = col.as_any().downcast_ref::<StringArray>() else {
                        continue;
                    };
                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            obj.insert(
                                field_name.clone(),
                                Value::String(value.value(j).to_string()),
                            );
                        }
                    }
                }
                DataType::LargeUtf8 => {
                    let Some(value) = col.as_any().downcast_ref::<LargeStringArray>() else {
                        continue;
                    };
                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            obj.insert(
                                field_name.clone(),
                                Value::String(value.value(j).to_string()),
                            );
                        }
                    }
                }
                DataType::Utf8View => {
                    let Some(value) = col.as_any().downcast_ref::<StringViewArray>() else {
                        continue;
                    };
                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {
                            obj.insert(
                                field_name.clone(),
                                Value::String(value.value(j).to_string()),
                            );
                        }
                    }
                }
                _ => {
                    return Err(Error::Process("not support data type".to_string()));
                }
            }
        }

        Ok(Self(
            message_batch
                .into_iter()
                .map(|x| Value::Object(x).into())
                .collect(),
        ))
    }
}
impl From<Vec<Message>> for MessageBatch {
    fn from(value: Vec<Message>) -> Self {
        Self(value)
    }
}

impl From<MessageBatch> for Vec<Message> {
    fn from(value: MessageBatch) -> Self {
        value.0
    }
}

impl TryFrom<Vec<Vec<u8>>> for MessageBatch {
    type Error = Error;

    fn try_from(value: Vec<Vec<u8>>) -> Result<Self, Self::Error> {
        Self::new_binary(value)
    }
}

impl TryFrom<Vec<String>> for MessageBatch {
    type Error = Error;

    fn try_from(value: Vec<String>) -> Result<Self, Self::Error> {
        Self::new_binary(value.into_iter().map(|s| s.into_bytes()).collect())
    }
}

impl TryFrom<Vec<&str>> for MessageBatch {
    type Error = Error;

    fn try_from(value: Vec<&str>) -> Result<Self, Self::Error> {
        Self::new_binary(value.into_iter().map(|s| s.as_bytes().to_vec()).collect())
    }
}

impl DerefMut for MessageBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        todo!()
    }
}

pub const DEFAULT_BINARY_VALUE_FIELD: &str = "__value__";
