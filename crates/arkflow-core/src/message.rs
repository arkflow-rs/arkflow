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
use crate::Error;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::ToByteSlice;
use serde_json::Number;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct Message {
    pub value: Value,
}

#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Bytes(Bytes),
    Float32(f32),
    Float64(f64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Uint8(u8),
    Uint16(u16),
    Uint32(u32),
    Uint64(u64),
    String(String),
    Bool(bool),
    Object(BTreeMap<String, Value>),
    Array(Vec<Value>),
    Timestamp(DateTime<Utc>),
}

impl TryInto<serde_json::Value> for Value {
    type Error = Error;

    fn try_into(self) -> Result<serde_json::Value, Self::Error> {
        Ok(match self {
            Value::Bytes(v) => {
                serde_json::Value::String(String::from_utf8_lossy(v.to_byte_slice()).to_string())
            }
            Value::Null => serde_json::Value::Null,
            Value::Float32(v) => serde_json::Value::Number(
                Number::from_f64(v as f64)
                    .ok_or_else(|| Error::Process("Float32 conversion error".to_string()))?,
            ),
            Value::Float64(v) => serde_json::Value::Number(
                Number::from_f64(v)
                    .ok_or_else(|| Error::Process("Float64 conversion error".to_string()))?,
            ),
            Value::Int8(v) => serde_json::Value::Number(Number::from(v)),
            Value::Int16(v) => serde_json::Value::Number(Number::from(v)),
            Value::Int32(v) => serde_json::Value::Number(Number::from(v)),
            Value::Int64(v) => serde_json::Value::Number(Number::from(v)),
            Value::Uint8(v) => serde_json::Value::Number(Number::from(v)),
            Value::Uint16(v) => serde_json::Value::Number(Number::from(v)),
            Value::Uint32(v) => serde_json::Value::Number(Number::from(v)),
            Value::Uint64(v) => serde_json::Value::Number(Number::from(v)),
            Value::String(v) => serde_json::Value::String(v),
            Value::Bool(v) => serde_json::Value::Bool(v),
            Value::Object(v) => serde_json::Value::Object(
                v.into_iter()
                    .map(|(k, v)| v.try_into().map(|v| (k, v)))
                    .collect::<Result<_, Self::Error>>()?,
            ),
            Value::Array(v) => serde_json::Value::Array(
                v.into_iter()
                    .map(|v| v.try_into().map(|x| x))
                    .collect::<Result<Vec<serde_json::Value>, Self::Error>>()?,
            ),
            Value::Timestamp(v) => serde_json::Value::String(v.to_rfc3339()),
        })
    }
}

impl TryFrom<serde_json::Value> for Value {
    type Error = Error;
    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        Ok(match value {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(v) => Value::Bool(v),
            serde_json::Value::Number(v) => Value::Float64(
                v.as_f64()
                    .ok_or_else(|| Error::Process("Number conversion error".to_string()))?,
            ),
            serde_json::Value::String(v) => Value::String(v),
            serde_json::Value::Array(v) => Value::Array(
                v.into_iter()
                    .map(|v| v.try_into().map(|x| x))
                    .collect::<Result<_, Self::Error>>()?,
            ),
            serde_json::Value::Object(v) => Value::Object(
                v.into_iter()
                    .map(|(k, v)| v.try_into().map(|v| (k, v)))
                    .collect::<Result<_, Self::Error>>()?,
            ),
        })
    }
}

impl Into<Message> for Value {
    fn into(self) -> Message {
        Message { value: self }
    }
}
