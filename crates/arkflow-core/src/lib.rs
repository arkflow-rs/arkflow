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
        let fields = vec![Field::new(
            DEFAULT_BINARY_VALUE_FIELD,
            DataType::Binary,
            false,
        )];

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

    pub fn len(&self) -> usize {
        self.0.len()
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

impl TryFrom<MessageBatch> for RecordBatch {
    type Error = Error;

    fn try_from(value: MessageBatch) -> Result<Self, Self::Error> {
        let Some(message) = value.get(0) else {
            return Ok(Self::new_empty(SchemaRef::new(Schema::empty())));
        };
        // let cols: Vec<ArrayRef>;
        let (fields) = match &message.value {
            Value::Bytes(_) => {
                let mut rows: Vec<&[u8]> = Vec::with_capacity(value.len());
                for x in value.0.into_iter() {
                    match x.value {
                        Value::Bytes(v) => {
                            rows.push(v.to_vec().as_slice());
                        }
                        _ => {
                            return Err(Error::Process("not support data type".to_string()));
                        }
                    }
                }

                let fields = vec![Field::new(
                    DEFAULT_BINARY_VALUE_FIELD,
                    DataType::Binary,
                    false,
                )];
                RecordBatch::try_new(
                    Arc::new(Schema::new(fields)),
                    vec![Arc::new(BinaryArray::from(rows))],
                )
            }
            Value::Object(obj) => {
                let fields = obj
                    .iter()
                    .map(|(k, v)| match v {
                        Value::Null => Field::new(k, DataType::Null, false),
                        Value::Bytes(_) => Field::new(k, DataType::Binary, false),
                        Value::Float32(_) => Field::new(k, DataType::Float32, false),
                        Value::Float64(_) => Field::new(k, DataType::Float64, false),
                        Value::Int8(_) => Field::new(k, DataType::Int8, false),
                        Value::Int16(_) => Field::new(k, DataType::Int16, false),
                        Value::Int32(_) => Field::new(k, DataType::Int32, false),
                        Value::Int64(_) => Field::new(k, DataType::Int64, false),
                        Value::Uint8(_) => Field::new(k, DataType::UInt8, false),
                        Value::Uint16(_) => Field::new(k, DataType::UInt16, false),
                        Value::Uint32(_) => Field::new(k, DataType::UInt32, false),
                        Value::Uint64(_) => Field::new(k, DataType::UInt64, false),
                        Value::String(_) => Field::new(k, DataType::Utf8, false),
                        Value::Bool(_) => Field::new(k, DataType::Boolean, false),
                        Value::Object(_) => {
                            todo!()
                        }
                        Value::Array(_) => {
                            todo!()
                        }
                        Value::Timestamp(s) => {
                            Field::new(k, DataType::Timestamp(TimeUnit::Millisecond, None), false)
                        }
                    })
                    .collect::<Vec<Field>>();
                let fields_map = fields
                    .iter()
                    .map(|x| (x.name(), x.data_type()))
                    .collect::<HashMap<&String, &DataType>>();

                let mut rows: Vec<ArrayRef> = Vec::with_capacity(value.len());
                let mut cols: HashMap<String, ArrayRef> = HashMap::with_capacity(fields.len());
                for x in &fields {
                    match x.data_type() {
                        DataType::Null => {
                            cols.insert(x.name().clone(), Arc::new(NullArray::new(value.len())));
                        }
                        DataType::Boolean => {
                            cols.insert(
                                x.name().clone(),
                                Arc::new(BooleanArray::from(vec![false; value.len()])),
                            );
                        }
                        DataType::Int8 => {
                            cols.insert(
                                x.name().clone(),
                                Arc::new(Int8Array::from(vec![0; value.len()])),
                            );
                        }
                        DataType::Int16 => {
                            cols.insert(
                                x.name().clone(),
                                Arc::new(Int16Array::from(vec![0; value.len()])),
                            );
                        }
                        DataType::Int32 => {}
                        DataType::Int64 => {}
                        DataType::UInt8 => {}
                        DataType::UInt16 => {}
                        DataType::UInt32 => {}
                        DataType::UInt64 => {}
                        DataType::Float16 => {}
                        DataType::Float32 => {}
                        DataType::Float64 => {}
                        DataType::Timestamp(_, _) => {}
                        DataType::Date32 => {}
                        DataType::Date64 => {}
                        DataType::Time32(_) => {}
                        DataType::Time64(_) => {}
                        DataType::Duration(_) => {}
                        DataType::Interval(_) => {}
                        DataType::Binary => {}
                        DataType::FixedSizeBinary(_) => {}
                        DataType::LargeBinary => {}
                        DataType::BinaryView => {}
                        DataType::Utf8 => {}
                        DataType::LargeUtf8 => {}
                        DataType::Utf8View => {}
                        DataType::List(_) => {}
                        DataType::ListView(_) => {}
                        DataType::FixedSizeList(_, _) => {}
                        DataType::LargeList(_) => {}
                        DataType::LargeListView(_) => {}
                        DataType::Struct(_) => {}
                        DataType::Union(_, _) => {}
                        DataType::Dictionary(_, _) => {}
                        DataType::Decimal128(_, _) => {}
                        DataType::Decimal256(_, _) => {}
                        DataType::Map(_, _) => {}
                        DataType::RunEndEncoded(_, _) => {}
                    }
                }
                for x in value.0.into_iter() {
                    match x.value {
                        Value::Object(obj) => for (k, v) in obj {
                            let option = cols.get_mut(&k);

                        },
                        _ => {
                            return Err(Error::Process("not support data type".to_string()));
                        }
                    }
                }
                RecordBatch::try_new(
                    Arc::new(Schema::new(fields)),
                    vec![Arc::new(BinaryArray::from(rows))],
                )
            }
            _ => {
                todo!()
            }
        };

        // Ok(RecordBatch::try_new(
        //     SchemaRef::new(Schema::new(fields)),
        //     vec![Arc::new(BinaryArray::from(vec![message.value.as_bytes()]))],
        // )?)
        Err(Error::Process("not support data type".to_string()))
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
                DataType::Timestamp(tu, a) => {
                    match tu {
                        TimeUnit::Second => {
                            let Some(value) = col.as_any().downcast_ref::<TimestampSecondArray>()
                            else {
                                continue;
                            };
                            for j in 0..value.len() {
                                if let Some(obj) = message_batch.get_mut(j) {
                                    let i1 = value.value(j);
                                    // obj.insert(field_name, Value::Timestamp())
                                }
                            }
                        }
                        TimeUnit::Millisecond => {
                            let Some(value) =
                                col.as_any().downcast_ref::<TimestampMicrosecondArray>()
                            else {
                                continue;
                            };
                            for j in 0..value.len() {
                                if let Some(obj) = message_batch.get_mut(j) {
                                    let i1 = value.value(j);
                                    // obj.insert(field_name, Value::Timestamp())
                                }
                            }
                        }
                        TimeUnit::Microsecond => {
                            let Some(value) =
                                col.as_any().downcast_ref::<TimestampMicrosecondArray>()
                            else {
                                continue;
                            };
                            for j in 0..value.len() {
                                if let Some(obj) = message_batch.get_mut(j) {
                                    let i1 = value.value(j);
                                    // obj.insert(field_name, Value::Timestamp())
                                }
                            }
                        }
                        TimeUnit::Nanosecond => {
                            let Some(value) =
                                col.as_any().downcast_ref::<TimestampNanosecondArray>()
                            else {
                                continue;
                            };
                            for j in 0..value.len() {
                                if let Some(obj) = message_batch.get_mut(j) {
                                    let i1 = value.value(j);
                                    // obj.insert(field_name, Value::Timestamp())
                                }
                            }
                        }
                    }
                }
                DataType::Date32 => {
                    let Some(value) = col.as_any().downcast_ref::<Date32Array>() else {
                        continue;
                    };
                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {}
                    }
                }
                DataType::Date64 => {
                    let Some(value) = col.as_any().downcast_ref::<Date64Array>() else {
                        continue;
                    };
                    for j in 0..value.len() {
                        if let Some(obj) = message_batch.get_mut(j) {}
                    }
                }
                DataType::Time32(tu) => match tu {
                    TimeUnit::Second => {
                        let Some(value) = col.as_any().downcast_ref::<Time32SecondArray>() else {
                            continue;
                        };
                    }
                    TimeUnit::Millisecond => {
                        let Some(value) = col.as_any().downcast_ref::<Time32MillisecondArray>()
                        else {
                            continue;
                        };
                    }
                    _ => {}
                },
                DataType::Time64(tu) => match tu {
                    TimeUnit::Microsecond => {
                        let Some(value) = col.as_any().downcast_ref::<Time64MicrosecondArray>()
                        else {
                            continue;
                        };
                    }
                    TimeUnit::Nanosecond => {
                        let Some(value) = col.as_any().downcast_ref::<Time64NanosecondArray>()
                        else {
                            continue;
                        };
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
                DataType::Interval(_) => {}
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
                DataType::FixedSizeBinary(_) => {}
                DataType::LargeBinary => {}
                DataType::BinaryView => {}
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
