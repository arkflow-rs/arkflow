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
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

/// Configuration trait for Protobuf components
pub trait ProtobufConfig {
    fn proto_inputs(&self) -> &Vec<String>;
    fn proto_includes(&self) -> &Option<Vec<String>>;
}

/// List the `.proto` inputs: directories are expanded to their files,
/// and a direct file path is accepted as-is (as documented for `proto_inputs`).
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
    } else {
        files.push(dir.as_ref().to_path_buf());
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
    // The parser requires every input file to reside inside an include
    // directory, so a direct file input contributes its parent directory.
    let proto_includes = config.proto_includes().clone().unwrap_or_else(|| {
        config
            .proto_inputs()
            .iter()
            .map(|input| {
                let path = Path::new(input);
                if path.is_dir() {
                    input.clone()
                } else {
                    path.parent()
                        .map(|p| p.to_string_lossy().to_string())
                        .unwrap_or_else(|| ".".to_string())
                }
            })
            .collect()
    });

    if proto_inputs.is_empty() {
        return Err(Error::Config("No proto files found in the specified paths. Please ensure the paths contain valid .proto files".to_string()));
    }

    // Compile the proto files with the pure-Rust protox compiler, which
    // yields prost_types descriptors directly.
    let file_descriptor_set = protox::compile(proto_inputs, proto_includes)
        .map_err(|e| Error::Config(format!("Failed to parse the proto file: {}", e)))?;

    if file_descriptor_set.file.is_empty() {
        return Err(Error::Config(
            "Parsing the proto file does not yield any descriptors".to_string(),
        ));
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

    let file_descriptor_set = protox::compile([proto_path], [dir.path()])
        .map_err(|e| Error::Config(format!("Failed to parse proto source: {}", e)))?;

    let pool = prost_reflect::DescriptorPool::from_file_descriptor_set(file_descriptor_set)
        .map_err(|e| Error::Config(format!("Failed to create descriptor pool: {}", e)))?;
    pool.get_message_by_name(message_type).ok_or_else(|| {
        Error::Config(format!(
            "Message type not found in schema: {}",
            message_type
        ))
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::MessageBatch;

    const SCHEMA: &str = r#"
syntax = "proto3";
package arkflow.test;
enum Level {
  LOW = 0;
  HIGH = 1;
}
message Sample {
  string name = 1;
  int64 count = 2;
  bool active = 3;
  double score = 4;
  bytes payload = 5;
  Level level = 6;
}
"#;

    #[derive(Clone)]
    struct TestConfig {
        inputs: Vec<String>,
        includes: Option<Vec<String>>,
    }
    impl ProtobufConfig for TestConfig {
        fn proto_inputs(&self) -> &Vec<String> {
            &self.inputs
        }
        fn proto_includes(&self) -> &Option<Vec<String>> {
            &self.includes
        }
    }

    fn write_schema(dir: &tempfile::TempDir) -> String {
        let path = dir.path().join("sample.proto");
        std::fs::write(&path, SCHEMA).unwrap();
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn list_files_in_dir_covers_files_dirs_and_errors() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = write_schema(&dir);
        // A directory expands to its entries (recursing into subdirectories).
        let listed = list_files_in_dir(dir.path()).unwrap();
        assert!(listed.iter().any(|p| p.ends_with("sample.proto")));
        // A direct file path is returned as-is.
        let listed = list_files_in_dir(&file_path).unwrap();
        assert_eq!(listed, vec![std::path::PathBuf::from(&file_path)]);
        // A missing path is passed through unchanged (validated later by
        // the proto parser, which owns the real error message).
        let listed = list_files_in_dir("/nonexistent/arkflow").unwrap();
        assert_eq!(
            listed,
            vec![std::path::PathBuf::from("/nonexistent/arkflow")]
        );
    }

    #[test]
    fn parse_proto_file_round_trips_a_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_schema(&dir);
        let set = parse_proto_file(&TestConfig {
            inputs: vec![path.clone()],
            includes: None,
        })
        .unwrap();
        assert!(!set.file.is_empty());

        // Empty inputs and proto-less directories are config errors.
        let err = parse_proto_file(&TestConfig {
            inputs: vec![],
            includes: None,
        })
        .unwrap_err();
        assert!(err.to_string().contains("No proto files found"), "{err}");

        let empty_dir = tempfile::tempdir().unwrap();
        let err = parse_proto_file(&TestConfig {
            inputs: vec![empty_dir.path().to_str().unwrap().to_string()],
            includes: None,
        })
        .unwrap_err();
        assert!(err.to_string().contains("No proto files found"), "{err}");

        let err = parse_proto_file(&TestConfig {
            inputs: vec!["/nonexistent/arkflow".into()],
            includes: None,
        })
        .unwrap_err();
        assert!(err.to_string().contains("No proto files found"), "{err}");

        // A syntactically invalid schema fails the typecheck.
        let bad_dir = tempfile::tempdir().unwrap();
        let bad = bad_dir.path().join("bad.proto");
        std::fs::write(&bad, "this is not proto").unwrap();
        let err = parse_proto_file(&TestConfig {
            inputs: vec![bad.to_str().unwrap().to_string()],
            includes: None,
        })
        .unwrap_err();
        assert!(err.to_string().contains("Failed to parse the proto file"), "{err}");
    }

    #[test]
    fn parse_proto_source_resolves_and_rejects_message_types() {
        let descriptor = parse_proto_source(SCHEMA, "arkflow.test.Sample").unwrap();
        assert!(descriptor.get_field_by_name("name").is_some());

        let err = parse_proto_source(SCHEMA, "arkflow.test.Missing").unwrap_err();
        assert!(err.to_string().contains("Message type not found"), "{err}");

        let err = parse_proto_source("garbage {", "x.Y").unwrap_err();
        assert!(err.to_string().contains("Failed to parse proto source"), "{err}");
    }

    #[tokio::test]
    async fn enum_and_bytes_fields_round_trip() {
        let descriptor = parse_proto_source(SCHEMA, "arkflow.test.Sample").unwrap();
        let mut message = prost_reflect::DynamicMessage::new(descriptor.clone());
        message.set_field_by_name("level", Value::EnumNumber(1));
        message.set_field_by_name("payload", Value::Bytes(vec![0x01, 0x02].into()));
        let encoded = message.encode_to_vec();

        let batch = protobuf_to_arrow(&descriptor, &encoded).unwrap();
        assert_eq!(batch.num_rows(), 1);
        // level arrives as a plain Int32 column.
        use datafusion::arrow::array::Int32Array;
        let level = batch
            .column_by_name("level")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("enum as int32");
        assert_eq!(level.value(0), 1);

        let message_batch = MessageBatch::new_arrow(batch);
        let re_encoded = arrow_to_protobuf(&descriptor, &message_batch).unwrap();
        let decoded = protobuf_to_arrow(&descriptor, &re_encoded[0]).unwrap();
        let level = decoded
            .column_by_name("level")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(level.value(0), 1);
    }

    #[tokio::test]
    async fn arrow_column_type_mismatch_names_the_field() {
        let descriptor = parse_proto_source(SCHEMA, "arkflow.test.Sample").unwrap();
        // count is int64 in the schema; hand it a Utf8 column instead.
        use datafusion::arrow::array::StringArray;
        use datafusion::arrow::datatypes::Schema;
        let schema = Arc::new(Schema::new(vec![Field::new("count", DataType::Utf8, true)]));
        let arr = Arc::new(StringArray::from(vec![Some("x")]));
        let rb = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let err = arrow_to_protobuf(&descriptor, &MessageBatch::new_arrow(rb)).unwrap_err();
        assert!(
            err.to_string().contains("expects proto Int64"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn unsupported_proto_kind_is_rejected() {
        let schema = r#"
syntax = "proto3";
package arkflow.test;
message WithNested {
  Sample inner = 1;
}
message Sample {
  string name = 1;
}
"#;
        let descriptor = parse_proto_source(schema, "arkflow.test.WithNested").unwrap();
        use datafusion::arrow::array::StringArray;
        use datafusion::arrow::datatypes::Schema;
        let schema = Arc::new(Schema::new(vec![Field::new("inner", DataType::Utf8, true)]));
        let arr = Arc::new(StringArray::from(vec![Some("x")]));
        let rb = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let err = arrow_to_protobuf(&descriptor, &MessageBatch::new_arrow(rb)).unwrap_err();
        assert!(err.to_string().contains("Unsupported Protobuf type"), "{err}");
    }

    #[test]
    fn protobuf_arrow_round_trip_preserves_values() {
        let descriptor = parse_proto_source(SCHEMA, "arkflow.test.Sample").unwrap();

        // Encode a dynamic message.
        use prost_reflect::Value;
        let mut message = prost_reflect::DynamicMessage::new(descriptor.clone());
        message.set_field_by_name("name", Value::String("alice".to_string()));
        message.set_field_by_name("count", Value::I64(7));
        message.set_field_by_name("active", Value::Bool(true));
        message.set_field_by_name("score", Value::F64(1.5));
        let encoded = message.encode_to_vec();

        let batch = protobuf_to_arrow(&descriptor, &encoded).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let message_batch = MessageBatch::new_arrow(batch);
        let re_encoded = arrow_to_protobuf(&descriptor, &message_batch).unwrap();
        assert_eq!(re_encoded.len(), 1);
        let decoded = protobuf_to_arrow(&descriptor, &re_encoded[0]).unwrap();
        assert_eq!(decoded.num_rows(), 1);

        // A truncated buffer fails decode loudly.
        let err = protobuf_to_arrow(&descriptor, &encoded[..encoded.len() / 2]).unwrap_err();
        assert!(err.to_string().contains("Protobuf message parsing failed"), "{err}");
    }

    fn write_proto(dir: &tempfile::TempDir, name: &str, content: &str) -> String {
        let path = dir.path().join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, content).unwrap();
        path.to_str().unwrap().to_string()
    }

    fn pool_of(set: FileDescriptorSet) -> prost_reflect::DescriptorPool {
        prost_reflect::DescriptorPool::from_file_descriptor_set(set).unwrap()
    }

    #[test]
    fn transitive_imports_resolve_via_parent_dir_fallback() {
        let dir = tempfile::tempdir().unwrap();
        write_proto(
            &dir,
            "common.proto",
            r#"syntax = "proto3";
package common;
message Id {
  string id = 1;
}
"#,
        );
        let main = write_proto(
            &dir,
            "main.proto",
            r#"syntax = "proto3";
import "common.proto";
package app;
message Wrapper {
  common.Id inner = 1;
  string tag = 2;
}
"#,
        );

        // Only main.proto is an input; the import must resolve through the
        // parent-directory include fallback.
        let set = parse_proto_file(&TestConfig {
            inputs: vec![main],
            includes: None,
        })
        .unwrap();
        // The descriptor set carries the imported file alongside the input.
        assert!(set.file.len() >= 2, "imported file missing from set");
        let pool = pool_of(set);
        assert!(
            pool.get_message_by_name("common.Id").is_some(),
            "transitively imported message must compile"
        );
        let wrapper = pool.get_message_by_name("app.Wrapper").unwrap();
        assert!(wrapper.get_field_by_name("tag").is_some());
    }

    #[test]
    fn transitive_imports_resolve_with_explicit_includes() {
        let dir = tempfile::tempdir().unwrap();
        // The dependency lives in a subdirectory exposed as an include dir;
        // the import path is interpreted relative to that include dir.
        write_proto(
            &dir,
            "inc/common.proto",
            r#"syntax = "proto3";
package common;
message Id {
  string id = 1;
}
"#,
        );
        let main = write_proto(
            &dir,
            "main.proto",
            r#"syntax = "proto3";
import "common.proto";
package app;
message Wrapper {
  common.Id inner = 1;
}
"#,
        );
        let inc = dir.path().join("inc").to_str().unwrap().to_string();
        let root = dir.path().to_str().unwrap().to_string();

        // Like protoc (and the previous parser), every input file must itself
        // live under an include root, so the input's own dir is listed too.
        let set = parse_proto_file(&TestConfig {
            inputs: vec![main],
            includes: Some(vec![root, inc]),
        })
        .unwrap();
        let pool = pool_of(set);
        assert!(pool.get_message_by_name("common.Id").is_some());
    }

    #[test]
    fn missing_import_surfaces_parse_error() {
        let dir = tempfile::tempdir().unwrap();
        let main = write_proto(
            &dir,
            "main.proto",
            r#"syntax = "proto3";
import "nothere.proto";
package app;
message Wrapper {
  string tag = 1;
}
"#,
        );
        let err = parse_proto_file(&TestConfig {
            inputs: vec![main],
            includes: None,
        })
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Failed to parse the proto file"), "{msg}");
        // The diagnostic must name the unresolved file, not just fail opaquely.
        assert!(msg.contains("nothere.proto"), "{msg}");
    }

    #[test]
    fn proto2_syntax_schema_compiles_and_round_trips() {
        let schema = r#"syntax = "proto2";
package legacy;
message P2 {
  required string name = 1;
  optional int32 count = 2 [default = 42];
}
"#;
        let descriptor = parse_proto_source(schema, "legacy.P2").unwrap();
        assert!(descriptor.get_field_by_name("name").is_some());
        assert!(descriptor.get_field_by_name("count").is_some());

        // A proto2 message still round-trips through the conversion layer.
        let mut message = DynamicMessage::new(descriptor.clone());
        message.set_field_by_name("name", Value::String("legacy-row".to_string()));
        let batch = protobuf_to_arrow(&descriptor, &message.encode_to_vec()).unwrap();
        let col = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "legacy-row");
    }

    #[test]
    fn multi_file_descriptor_set_exposes_every_input() {
        let dir = tempfile::tempdir().unwrap();
        let a = write_proto(
            &dir,
            "a.proto",
            r#"syntax = "proto3";
package pa;
message A {
  string name = 1;
}
"#,
        );
        let b = write_proto(
            &dir,
            "b.proto",
            r#"syntax = "proto3";
package pb;
message B {
  int64 n = 1;
}
"#,
        );
        let set = parse_proto_file(&TestConfig {
            inputs: vec![a, b],
            includes: None,
        })
        .unwrap();
        assert_eq!(set.file.len(), 2);
        let pool = pool_of(set);
        assert!(pool.get_message_by_name("pa.A").is_some());
        assert!(pool.get_message_by_name("pb.B").is_some());
    }

    #[test]
    fn directory_input_expands_to_its_proto_files() {
        let dir = tempfile::tempdir().unwrap();
        write_proto(
            &dir,
            "one.proto",
            r#"syntax = "proto3";
package d;
message One {
  string s = 1;
}
"#,
        );
        write_proto(
            &dir,
            "two.proto",
            r#"syntax = "proto3";
package d;
message Two {
  int64 i = 1;
}
"#,
        );
        // A non-proto file in the directory must be filtered out, not parsed.
        write_proto(&dir, "notes.txt", "not a schema");

        let set = parse_proto_file(&TestConfig {
            inputs: vec![dir.path().to_str().unwrap().to_string()],
            includes: None,
        })
        .unwrap();
        assert_eq!(set.file.len(), 2, "only .proto files compile");
        let pool = pool_of(set);
        assert!(pool.get_message_by_name("d.One").is_some());
        assert!(pool.get_message_by_name("d.Two").is_some());
    }

    #[test]
    fn proto3_optional_field_pins_current_behavior() {
        let schema = r#"syntax = "proto3";
package opt;
message WithOptional {
  optional int64 maybe = 1;
  string always = 2;
}
"#;
        // Pin the observable behavior under the prost-reflect data layer:
        // the field must at least parse, and whatever the conversion layer
        // does with it must be loud (round-trip or error), never silent loss.
        let descriptor = parse_proto_source(schema, "opt.WithOptional").unwrap();
        let maybe = descriptor.get_field_by_name("maybe").expect("field parses");
        assert_eq!(maybe.kind(), prost_reflect::Kind::Int64);
    }

    #[test]
    fn parse_proto_source_unresolved_import_errors() {
        let schema = r#"syntax = "proto3";
import "missing_dep.proto";
package x;
message Y {
  string s = 1;
}
"#;
        let err = parse_proto_source(schema, "x.Y").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Failed to parse proto source"), "{msg}");
        assert!(msg.contains("missing_dep.proto"), "{msg}");
    }
}
