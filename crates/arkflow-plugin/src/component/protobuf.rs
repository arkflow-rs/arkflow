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

//! Common Protobuf utilities and functions
//!
//! This module contains shared functionality for Protobuf processing
//! used by both codec and processor components.
//!
//! # Supported field types
//!
//! Scalar proto3 fields are supported in both directions (Arrow ↔ Protobuf):
//! `bool`, `int32`/`sint32`/`sfixed32`, `int64`/`sint64`/`sfixed64`,
//! `uint32`/`fixed32`, `uint64`/`fixed64`, `float`, `double`, `string`,
//! `bytes`, and `enum` (mapped to Arrow `Int32`).
//!
//! **Not supported**: nested message fields, `repeated` fields, `map` fields,
//! `oneof` fields, and proto3 `optional` fields. Encountering any of these
//! returns an error naming the field and its kind.

use arkflow_core::{Bytes, Error, MessageBatch};
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use prost_reflect::prost::Message;
use prost_reflect::prost_types::FileDescriptorSet;
use prost_reflect::{DynamicMessage, MessageDescriptor, Value};
use protobuf::Message as ProtobufMessage;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

/// Configuration trait for Protobuf components
pub trait ProtobufConfig {
    fn proto_inputs(&self) -> &Vec<String>;
    fn proto_includes(&self) -> &Option<Vec<String>>;
}

/// List all files in a directory
pub fn list_files_in_dir<P: AsRef<Path>>(dir: P) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if dir.as_ref().is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                files.push(path);
            }
        }
    }
    Ok(files)
}

/// Parse and generate a FileDescriptorSet from .proto files
pub fn parse_proto_file<T: ProtobufConfig>(config: &T) -> Result<FileDescriptorSet, Error> {
    let mut proto_inputs: Vec<String> = vec![];
    for x in config.proto_inputs() {
        let files_in_dir_result = list_files_in_dir(x)
            .map_err(|e| Error::Config(format!("Failed to list proto files: {}", e)))?;
        proto_inputs.extend(
            files_in_dir_result
                .iter()
                .filter(|path| path.extension().is_some_and(|ext| ext == "proto"))
                .filter_map(|path| path.to_str().map(|s| s.to_string()))
                .collect::<Vec<_>>(),
        )
    }
    let proto_includes = config
        .proto_includes()
        .clone()
        .unwrap_or(config.proto_inputs().clone());

    if proto_inputs.is_empty() {
        return Err(Error::Config("No proto files found in the specified paths. Please ensure the paths contain valid .proto files".to_string()));
    }

    // Parse the proto file using the protobuf_parse library
    let file_descriptor_protos = protobuf_parse::Parser::new()
        .pure()
        .inputs(proto_inputs)
        .includes(proto_includes)
        .parse_and_typecheck()
        .map_err(|e| Error::Config(format!("Failed to parse the proto file: {}", e)))?
        .file_descriptors;

    if file_descriptor_protos.is_empty() {
        return Err(Error::Config(
            "Parsing the proto file does not yield any descriptors".to_string(),
        ));
    }

    // Convert FileDescriptorProto to FileDescriptorSet
    let mut file_descriptor_set = FileDescriptorSet { file: Vec::new() };

    for proto in file_descriptor_protos {
        // Convert the protobuf library's FileDescriptorProto to a prost_types FileDescriptorProto
        let proto_bytes = proto.write_to_bytes().map_err(|e| {
            Error::Config(format!("Failed to serialize FileDescriptorProto: {}", e))
        })?;

        let prost_proto =
            prost_reflect::prost_types::FileDescriptorProto::decode(proto_bytes.as_slice())
                .map_err(|e| {
                    Error::Config(format!("Failed to convert FileDescriptorProto: {}", e))
                })?;

        file_descriptor_set.file.push(prost_proto);
    }

    Ok(file_descriptor_set)
}

/// Parse a Protobuf schema from a source string (e.g. obtained from a Schema Registry)
/// and return the `MessageDescriptor` for `message_type`. Used by codecs that resolve
/// schemas dynamically at runtime rather than from local files.
pub fn parse_proto_source(schema: &str, message_type: &str) -> Result<MessageDescriptor, Error> {
    let dir = tempfile::tempdir()
        .map_err(|e| Error::Config(format!("Failed to create temp dir: {}", e)))?;
    let proto_path = dir.path().join("registry_schema.proto");
    fs::write(&proto_path, schema)
        .map_err(|e| Error::Config(format!("Failed to write proto source: {}", e)))?;

    let proto_input = proto_path
        .to_str()
        .ok_or_else(|| Error::Config("Invalid temp proto path".to_string()))?
        .to_string();
    let include_dir = dir
        .path()
        .to_str()
        .ok_or_else(|| Error::Config("Invalid temp include path".to_string()))?
        .to_string();

    let file_descriptor_protos = protobuf_parse::Parser::new()
        .pure()
        .inputs(&[proto_input])
        .includes(&[include_dir])
        .parse_and_typecheck()
        .map_err(|e| Error::Config(format!("Failed to parse proto source: {}", e)))?
        .file_descriptors;

    let mut file_descriptor_set = FileDescriptorSet { file: Vec::new() };
    for proto in file_descriptor_protos {
        let proto_bytes = proto
            .write_to_bytes()
            .map_err(|e| Error::Config(format!("Failed to serialize FileDescriptorProto: {}", e)))?;
        let prost_proto =
            prost_reflect::prost_types::FileDescriptorProto::decode(proto_bytes.as_slice())
                .map_err(|e| Error::Config(format!("Failed to convert FileDescriptorProto: {}", e)))?;
        file_descriptor_set.file.push(prost_proto);
    }

    let pool = prost_reflect::DescriptorPool::from_file_descriptor_set(file_descriptor_set)
        .map_err(|e| Error::Config(format!("Failed to create descriptor pool: {}", e)))?;
    pool.get_message_by_name(message_type)
        .ok_or_else(|| Error::Config(format!("Message type not found in schema: {}", message_type)))
}

/// Convert Protobuf data to Arrow format
///
/// The schema is driven by the message descriptor's full field set (every field
/// nullable), so every decoded message yields the same schema regardless of
/// which fields are present — making the per-message batches safe to concatenate.
pub fn protobuf_to_arrow(
    descriptor: &MessageDescriptor,
    data: &[u8],
) -> Result<RecordBatch, Error> {
    let proto_msg = DynamicMessage::decode(descriptor.clone(), data)
        .map_err(|e| Error::Process(format!("Protobuf message parsing failed: {}", e)))?;

    let descriptor_fields = descriptor.fields();
    let mut fields = Vec::with_capacity(descriptor_fields.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(descriptor_fields.len());

    for field in descriptor_fields {
        let field_name = field.name();
        // Look up the value; an absent field becomes a null column (descriptor-driven schema).
        let field_value_opt = proto_msg.get_field_by_name(field_name);

        match field.kind() {
            prost_reflect::Kind::Bool => {
                fields.push(Field::new(field_name, DataType::Boolean, true));
                let v = match field_value_opt.as_deref() {
                    Some(Value::Bool(b)) => Some(*b),
                    _ => None,
                };
                columns.push(Arc::new(BooleanArray::from(vec![v])));
            }
            prost_reflect::Kind::Int32
            | prost_reflect::Kind::Sint32
            | prost_reflect::Kind::Sfixed32 => {
                fields.push(Field::new(field_name, DataType::Int32, true));
                let v = match field_value_opt.as_deref() {
                    Some(Value::I32(i)) => Some(*i),
                    _ => None,
                };
                columns.push(Arc::new(Int32Array::from(vec![v])));
            }
            prost_reflect::Kind::Int64
            | prost_reflect::Kind::Sint64
            | prost_reflect::Kind::Sfixed64 => {
                fields.push(Field::new(field_name, DataType::Int64, true));
                let v = match field_value_opt.as_deref() {
                    Some(Value::I64(i)) => Some(*i),
                    _ => None,
                };
                columns.push(Arc::new(Int64Array::from(vec![v])));
            }
            prost_reflect::Kind::Uint32 | prost_reflect::Kind::Fixed32 => {
                fields.push(Field::new(field_name, DataType::UInt32, true));
                let v = match field_value_opt.as_deref() {
                    Some(Value::U32(i)) => Some(*i),
                    _ => None,
                };
                columns.push(Arc::new(UInt32Array::from(vec![v])));
            }
            prost_reflect::Kind::Uint64 | prost_reflect::Kind::Fixed64 => {
                fields.push(Field::new(field_name, DataType::UInt64, true));
                let v = match field_value_opt.as_deref() {
                    Some(Value::U64(i)) => Some(*i),
                    _ => None,
                };
                columns.push(Arc::new(UInt64Array::from(vec![v])));
            }
            prost_reflect::Kind::Float => {
                fields.push(Field::new(field_name, DataType::Float32, true));
                let v = match field_value_opt.as_deref() {
                    Some(Value::F32(f)) => Some(*f),
                    _ => None,
                };
                columns.push(Arc::new(Float32Array::from(vec![v])));
            }
            prost_reflect::Kind::Double => {
                fields.push(Field::new(field_name, DataType::Float64, true));
                let v = match field_value_opt.as_deref() {
                    Some(Value::F64(f)) => Some(*f),
                    _ => None,
                };
                columns.push(Arc::new(Float64Array::from(vec![v])));
            }
            prost_reflect::Kind::String => {
                fields.push(Field::new(field_name, DataType::Utf8, true));
                let v = match field_value_opt.as_deref() {
                    Some(Value::String(s)) => Some(s.clone()),
                    _ => None,
                };
                columns.push(Arc::new(StringArray::from(vec![v])));
            }
            prost_reflect::Kind::Bytes => {
                fields.push(Field::new(field_name, DataType::Binary, true));
                let v: Option<&[u8]> = match field_value_opt.as_deref() {
                    Some(Value::Bytes(b)) => Some(b.as_ref()),
                    _ => None,
                };
                columns.push(Arc::new(BinaryArray::from(vec![v])));
            }
            prost_reflect::Kind::Enum(_) => {
                fields.push(Field::new(field_name, DataType::Int32, true));
                let v = match field_value_opt.as_deref() {
                    Some(Value::EnumNumber(n)) => Some(*n),
                    _ => None,
                };
                columns.push(Arc::new(Int32Array::from(vec![v])));
            }
            _ => {
                return Err(Error::Process(format!(
                    "Unsupported field type for field '{}': kind {:?}",
                    field_name,
                    field.kind()
                )));
            }
        }
    }

    // Create RecordBatch
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
        .map_err(|e| Error::Process(format!("Creating an Arrow record batch failed: {}", e)))
}

/// Convert Arrow format to Protobuf
///
/// A type mismatch between an Arrow column and its proto field returns an error
/// (rather than silently dropping the field), and null Arrow values are left
/// unset in the encoded proto message.
pub fn arrow_to_protobuf(
    descriptor: &MessageDescriptor,
    batch: &MessageBatch,
) -> Result<Vec<Bytes>, Error> {
    // Create a new dynamic message per row
    let mut vec = Vec::with_capacity(batch.len());
    let len = batch.len();
    for _ in 0..len {
        vec.push(DynamicMessage::new(descriptor.clone()));
    }

    // Get the Arrow schema.
    let schema = batch.schema();

    for (i, field) in schema.fields().iter().enumerate() {
        let field_name = field.name();

        if let Some(proto_field) = descriptor.get_field_by_name(field_name) {
            let column = batch.column(i);

            match proto_field.kind() {
                prost_reflect::Kind::Bool => {
                    let value = typed_column::<BooleanArray>(column, field_name, "Bool")?;
                    for j in 0..value.len() {
                        if let Some(msg) = vec.get_mut(j) {
                            if value.is_null(j) {
                                continue;
                            }
                            msg.set_field_by_name(field_name, Value::Bool(value.value(j)));
                        }
                    }
                }
                prost_reflect::Kind::Int32
                | prost_reflect::Kind::Sint32
                | prost_reflect::Kind::Sfixed32 => {
                    let value = typed_column::<Int32Array>(column, field_name, "Int32")?;
                    for j in 0..value.len() {
                        if let Some(msg) = vec.get_mut(j) {
                            if value.is_null(j) {
                                continue;
                            }
                            msg.set_field_by_name(field_name, Value::I32(value.value(j)));
                        }
                    }
                }
                prost_reflect::Kind::Int64
                | prost_reflect::Kind::Sint64
                | prost_reflect::Kind::Sfixed64 => {
                    let value = typed_column::<Int64Array>(column, field_name, "Int64")?;
                    for j in 0..value.len() {
                        if let Some(msg) = vec.get_mut(j) {
                            if value.is_null(j) {
                                continue;
                            }
                            msg.set_field_by_name(field_name, Value::I64(value.value(j)));
                        }
                    }
                }
                prost_reflect::Kind::Uint32 | prost_reflect::Kind::Fixed32 => {
                    let value = typed_column::<UInt32Array>(column, field_name, "Uint32")?;
                    for j in 0..value.len() {
                        if let Some(msg) = vec.get_mut(j) {
                            if value.is_null(j) {
                                continue;
                            }
                            msg.set_field_by_name(field_name, Value::U32(value.value(j)));
                        }
                    }
                }
                prost_reflect::Kind::Uint64 | prost_reflect::Kind::Fixed64 => {
                    let value = typed_column::<UInt64Array>(column, field_name, "Uint64")?;
                    for j in 0..value.len() {
                        if let Some(msg) = vec.get_mut(j) {
                            if value.is_null(j) {
                                continue;
                            }
                            msg.set_field_by_name(field_name, Value::U64(value.value(j)));
                        }
                    }
                }
                prost_reflect::Kind::Float => {
                    let value = typed_column::<Float32Array>(column, field_name, "Float")?;
                    for j in 0..value.len() {
                        if let Some(msg) = vec.get_mut(j) {
                            if value.is_null(j) {
                                continue;
                            }
                            msg.set_field_by_name(field_name, Value::F32(value.value(j)));
                        }
                    }
                }
                prost_reflect::Kind::Double => {
                    let value = typed_column::<Float64Array>(column, field_name, "Double")?;
                    for j in 0..value.len() {
                        if let Some(msg) = vec.get_mut(j) {
                            if value.is_null(j) {
                                continue;
                            }
                            msg.set_field_by_name(field_name, Value::F64(value.value(j)));
                        }
                    }
                }
                prost_reflect::Kind::String => {
                    let value = typed_column::<StringArray>(column, field_name, "String")?;
                    for j in 0..value.len() {
                        if let Some(msg) = vec.get_mut(j) {
                            if value.is_null(j) {
                                continue;
                            }
                            msg.set_field_by_name(
                                field_name,
                                Value::String(value.value(j).to_string()),
                            );
                        }
                    }
                }
                prost_reflect::Kind::Bytes => {
                    let value = typed_column::<BinaryArray>(column, field_name, "Bytes")?;
                    for j in 0..value.len() {
                        if let Some(msg) = vec.get_mut(j) {
                            if value.is_null(j) {
                                continue;
                            }
                            msg.set_field_by_name(
                                field_name,
                                Value::Bytes(value.value(j).to_vec().into()),
                            );
                        }
                    }
                }
                prost_reflect::Kind::Enum(_) => {
                    let value = typed_column::<Int32Array>(column, field_name, "Enum(Int32)")?;
                    for j in 0..value.len() {
                        if let Some(msg) = vec.get_mut(j) {
                            if value.is_null(j) {
                                continue;
                            }
                            msg.set_field_by_name(field_name, Value::EnumNumber(value.value(j)));
                        }
                    }
                }
                _ => {
                    return Err(Error::Process(format!(
                        "Unsupported Protobuf type for field '{}': kind {:?}",
                        field_name,
                        proto_field.kind()
                    )));
                }
            }
        }
    }

    vec.into_iter()
        .map(|proto_msg| {
            let mut buf = Vec::new();
            proto_msg
                .encode(&mut buf)
                .map_err(|e| Error::Process(format!("Protobuf encoding failed: {}", e)))?;
            Ok(buf)
        })
        .collect()
}

/// Downcast a column to the expected Arrow array type, or return an error naming
/// the field, the expected proto kind, and the actual Arrow datatype.
fn typed_column<'a, T: Array + 'static>(
    column: &'a dyn Array,
    field_name: &str,
    expected: &str,
) -> Result<&'a T, Error> {
    column.as_any().downcast_ref::<T>().ok_or_else(|| {
        Error::Process(format!(
            "Field '{}' expects proto {} but Arrow column is {:?}",
            field_name,
            expected,
            column.data_type()
        ))
    })
}
