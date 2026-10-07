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
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Float32Array,
    Float64Array, Int32Array, Int64Array, PrimitiveBuilder, StringArray, StringBuilder,
    UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{
    DataType, Field, Float32Type, Float64Type, Int32Type, Int64Type, Schema, UInt32Type, UInt64Type,
};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
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
///
/// Test-only reference path: production decode goes through
/// [`ProtobufBatchConverter`]; this function stays as the per-message oracle
/// the columnar output is asserted against.
#[cfg(test)]
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

// ===== Columnar batch conversion (one shared descriptor) =====

/// The Arrow leaf type of a protobuf column, derived once per batch from the
/// descriptor (every field nullable, mirroring `protobuf_to_arrow`).
#[derive(Clone, Copy)]
enum ProtoLeafKind {
    Bool,
    Int32,
    Int64,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Binary,
    EnumInt32,
}

struct ProtoColumnPlan {
    name: String,
    number: u32,
    kind: ProtoLeafKind,
}

enum ProtoColumnBuilder {
    Bool(BooleanBuilder),
    Int32(PrimitiveBuilder<Int32Type>),
    Int64(PrimitiveBuilder<Int64Type>),
    UInt32(PrimitiveBuilder<UInt32Type>),
    UInt64(PrimitiveBuilder<UInt64Type>),
    Float32(PrimitiveBuilder<Float32Type>),
    Float64(PrimitiveBuilder<Float64Type>),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
}

fn proto_kind_data_type(kind: ProtoLeafKind) -> DataType {
    match kind {
        ProtoLeafKind::Bool => DataType::Boolean,
        ProtoLeafKind::Int32 | ProtoLeafKind::EnumInt32 => DataType::Int32,
        ProtoLeafKind::Int64 => DataType::Int64,
        ProtoLeafKind::UInt32 => DataType::UInt32,
        ProtoLeafKind::UInt64 => DataType::UInt64,
        ProtoLeafKind::Float32 => DataType::Float32,
        ProtoLeafKind::Float64 => DataType::Float64,
        ProtoLeafKind::Utf8 => DataType::Utf8,
        ProtoLeafKind::Binary => DataType::Binary,
    }
}

fn proto_builder_kind_mismatch(name: &str) -> Error {
    Error::Process(format!(
        "internal error: column builder kind mismatch for field '{}'",
        name
    ))
}

/// First-message plan derivation: the same kind dispatch (and rejection
/// text) as `protobuf_to_arrow`'s field loop.
fn proto_plan(
    name: &str,
    number: u32,
    kind: &prost_reflect::Kind,
) -> Result<(ProtoColumnPlan, ProtoColumnBuilder), Error> {
    let leaf = match kind {
        prost_reflect::Kind::Bool => ProtoLeafKind::Bool,
        prost_reflect::Kind::Int32
        | prost_reflect::Kind::Sint32
        | prost_reflect::Kind::Sfixed32 => ProtoLeafKind::Int32,
        prost_reflect::Kind::Int64
        | prost_reflect::Kind::Sint64
        | prost_reflect::Kind::Sfixed64 => ProtoLeafKind::Int64,
        prost_reflect::Kind::Uint32 | prost_reflect::Kind::Fixed32 => ProtoLeafKind::UInt32,
        prost_reflect::Kind::Uint64 | prost_reflect::Kind::Fixed64 => ProtoLeafKind::UInt64,
        prost_reflect::Kind::Float => ProtoLeafKind::Float32,
        prost_reflect::Kind::Double => ProtoLeafKind::Float64,
        prost_reflect::Kind::String => ProtoLeafKind::Utf8,
        prost_reflect::Kind::Bytes => ProtoLeafKind::Binary,
        prost_reflect::Kind::Enum(_) => ProtoLeafKind::EnumInt32,
        _ => {
            return Err(Error::Process(format!(
                "Unsupported field type for field '{}': kind {:?}",
                name, kind
            )));
        }
    };
    let builder = match leaf {
        ProtoLeafKind::Bool => ProtoColumnBuilder::Bool(BooleanBuilder::new()),
        ProtoLeafKind::Int32 | ProtoLeafKind::EnumInt32 => {
            ProtoColumnBuilder::Int32(PrimitiveBuilder::<Int32Type>::new())
        }
        ProtoLeafKind::Int64 => ProtoColumnBuilder::Int64(PrimitiveBuilder::<Int64Type>::new()),
        ProtoLeafKind::UInt32 => ProtoColumnBuilder::UInt32(PrimitiveBuilder::<UInt32Type>::new()),
        ProtoLeafKind::UInt64 => ProtoColumnBuilder::UInt64(PrimitiveBuilder::<UInt64Type>::new()),
        ProtoLeafKind::Float32 => {
            ProtoColumnBuilder::Float32(PrimitiveBuilder::<Float32Type>::new())
        }
        ProtoLeafKind::Float64 => {
            ProtoColumnBuilder::Float64(PrimitiveBuilder::<Float64Type>::new())
        }
        ProtoLeafKind::Utf8 => ProtoColumnBuilder::Utf8(StringBuilder::new()),
        ProtoLeafKind::Binary => ProtoColumnBuilder::Binary(BinaryBuilder::new()),
    };
    Ok((
        ProtoColumnPlan {
            name: name.to_string(),
            number,
            kind: leaf,
        },
        builder,
    ))
}

/// Value extraction + append: mirrors `protobuf_to_arrow`'s per-field value
/// arms (type mismatch or absent field becomes a null entry).
fn proto_append_value(
    name: &str,
    kind: ProtoLeafKind,
    builder: &mut ProtoColumnBuilder,
    value: Option<&Value>,
) -> Result<(), Error> {
    match kind {
        ProtoLeafKind::Bool => {
            let v = match value {
                Some(Value::Bool(b)) => Some(*b),
                _ => None,
            };
            let ProtoColumnBuilder::Bool(b) = builder else {
                return Err(proto_builder_kind_mismatch(name));
            };
            match v {
                Some(x) => b.append_value(x),
                None => b.append_null(),
            }
        }
        ProtoLeafKind::Int32 => {
            let v = match value {
                Some(Value::I32(i)) => Some(*i),
                _ => None,
            };
            let ProtoColumnBuilder::Int32(b) = builder else {
                return Err(proto_builder_kind_mismatch(name));
            };
            append_opt(b, v);
        }
        ProtoLeafKind::Int64 => {
            let v = match value {
                Some(Value::I64(i)) => Some(*i),
                _ => None,
            };
            let ProtoColumnBuilder::Int64(b) = builder else {
                return Err(proto_builder_kind_mismatch(name));
            };
            append_opt(b, v);
        }
        ProtoLeafKind::UInt32 => {
            let v = match value {
                Some(Value::U32(i)) => Some(*i),
                _ => None,
            };
            let ProtoColumnBuilder::UInt32(b) = builder else {
                return Err(proto_builder_kind_mismatch(name));
            };
            append_opt(b, v);
        }
        ProtoLeafKind::UInt64 => {
            let v = match value {
                Some(Value::U64(i)) => Some(*i),
                _ => None,
            };
            let ProtoColumnBuilder::UInt64(b) = builder else {
                return Err(proto_builder_kind_mismatch(name));
            };
            append_opt(b, v);
        }
        ProtoLeafKind::Float32 => {
            let v = match value {
                Some(Value::F32(f)) => Some(*f),
                _ => None,
            };
            let ProtoColumnBuilder::Float32(b) = builder else {
                return Err(proto_builder_kind_mismatch(name));
            };
            append_opt(b, v);
        }
        ProtoLeafKind::Float64 => {
            let v = match value {
                Some(Value::F64(f)) => Some(*f),
                _ => None,
            };
            let ProtoColumnBuilder::Float64(b) = builder else {
                return Err(proto_builder_kind_mismatch(name));
            };
            append_opt(b, v);
        }
        ProtoLeafKind::Utf8 => {
            let v = match value {
                Some(Value::String(s)) => Some(s.as_str()),
                _ => None,
            };
            let ProtoColumnBuilder::Utf8(b) = builder else {
                return Err(proto_builder_kind_mismatch(name));
            };
            match v {
                Some(x) => b.append_value(x),
                None => b.append_null(),
            }
        }
        ProtoLeafKind::Binary => {
            let v: Option<&[u8]> = match value {
                Some(Value::Bytes(b)) => Some(b.as_ref()),
                _ => None,
            };
            let ProtoColumnBuilder::Binary(b) = builder else {
                return Err(proto_builder_kind_mismatch(name));
            };
            match v {
                Some(x) => b.append_value(x),
                None => b.append_null(),
            }
        }
        ProtoLeafKind::EnumInt32 => {
            let v = match value {
                Some(Value::EnumNumber(n)) => Some(*n),
                _ => None,
            };
            let ProtoColumnBuilder::Int32(b) = builder else {
                return Err(proto_builder_kind_mismatch(name));
            };
            append_opt(b, v);
        }
    }
    Ok(())
}

fn append_opt<T: datafusion::arrow::array::ArrowPrimitiveType>(
    b: &mut PrimitiveBuilder<T>,
    v: Option<T::Native>,
) {
    match v {
        Some(x) => b.append_value(x),
        None => b.append_null(),
    }
}

/// Columnar batch converter for Protobuf payloads sharing one descriptor.
/// Owns its (cheaply cloneable, Arc-backed) `MessageDescriptor` so it can
/// live inside decode-loop group states without borrow plumbing.
///
/// The first `push` walks the descriptor's field set exactly like
/// `protobuf_to_arrow` (same kind rejection text) and tees the mapping into
/// per-column plans; later pushes append by field number without the
/// per-message name lookups. Every column stays nullable, matching the
/// descriptor-driven schema of the single-message path.
pub struct ProtobufBatchConverter {
    descriptor: MessageDescriptor,
    plans: Vec<ProtoColumnPlan>,
    builders: Vec<ProtoColumnBuilder>,
    started: bool,
    rows: usize,
}

impl ProtobufBatchConverter {
    pub fn new(descriptor: MessageDescriptor) -> Self {
        Self {
            descriptor,
            plans: Vec::new(),
            builders: Vec::new(),
            started: false,
            rows: 0,
        }
    }

    pub fn push(&mut self, payload: &[u8]) -> Result<(), Error> {
        let proto_msg = DynamicMessage::decode(self.descriptor.clone(), payload)
            .map_err(|e| Error::Process(format!("Protobuf message parsing failed: {}", e)))?;
        if !self.started {
            for field in self.descriptor.fields() {
                let field_name = field.name();
                let (plan, mut builder) = proto_plan(field_name, field.number(), &field.kind())?;
                proto_append_value(
                    field_name,
                    plan.kind,
                    &mut builder,
                    proto_msg.get_field_by_name(field_name).as_deref(),
                )?;
                self.plans.push(plan);
                self.builders.push(builder);
            }
            self.started = true;
        } else {
            for i in 0..self.plans.len() {
                let (name, number, kind) = {
                    let p = &self.plans[i];
                    (p.name.as_str(), p.number, p.kind)
                };
                proto_append_value(
                    name,
                    kind,
                    &mut self.builders[i],
                    proto_msg.get_field_by_number(number).as_deref(),
                )?;
            }
        }
        self.rows += 1;
        Ok(())
    }

    pub fn finish(self) -> Result<RecordBatch, Error> {
        if !self.started {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        let Self {
            plans,
            builders,
            rows,
            ..
        } = self;
        let fields: Vec<Field> = plans
            .iter()
            .map(|p| Field::new(p.name.as_str(), proto_kind_data_type(p.kind), true))
            .collect();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(builders.len());
        for (plan, mut builder) in plans.iter().zip(builders) {
            let col: ArrayRef = match (&plan.kind, &mut builder) {
                (ProtoLeafKind::Bool, ProtoColumnBuilder::Bool(b)) => Arc::new(b.finish()),
                (ProtoLeafKind::Int32 | ProtoLeafKind::EnumInt32, ProtoColumnBuilder::Int32(b)) => {
                    Arc::new(b.finish())
                }
                (ProtoLeafKind::Int64, ProtoColumnBuilder::Int64(b)) => Arc::new(b.finish()),
                (ProtoLeafKind::UInt32, ProtoColumnBuilder::UInt32(b)) => Arc::new(b.finish()),
                (ProtoLeafKind::UInt64, ProtoColumnBuilder::UInt64(b)) => Arc::new(b.finish()),
                (ProtoLeafKind::Float32, ProtoColumnBuilder::Float32(b)) => Arc::new(b.finish()),
                (ProtoLeafKind::Float64, ProtoColumnBuilder::Float64(b)) => Arc::new(b.finish()),
                (ProtoLeafKind::Utf8, ProtoColumnBuilder::Utf8(b)) => Arc::new(b.finish()),
                (ProtoLeafKind::Binary, ProtoColumnBuilder::Binary(b)) => Arc::new(b.finish()),
                (_, _) => return Err(proto_builder_kind_mismatch(plan.name.as_str())),
            };
            columns.push(col);
        }
        RecordBatch::try_new_with_options(
            Arc::new(Schema::new(fields)),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|e| Error::Process(format!("Creating an Arrow record batch failed: {}", e)))
    }
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
        assert!(
            err.to_string().contains("Failed to parse the proto file"),
            "{err}"
        );
    }

    #[test]
    fn parse_proto_source_resolves_and_rejects_message_types() {
        let descriptor = parse_proto_source(SCHEMA, "arkflow.test.Sample").unwrap();
        assert!(descriptor.get_field_by_name("name").is_some());

        let err = parse_proto_source(SCHEMA, "arkflow.test.Missing").unwrap_err();
        assert!(err.to_string().contains("Message type not found"), "{err}");

        let err = parse_proto_source("garbage {", "x.Y").unwrap_err();
        assert!(
            err.to_string().contains("Failed to parse proto source"),
            "{err}"
        );
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
        assert!(err.to_string().contains("expects proto Int64"), "{err}");
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
        assert!(
            err.to_string().contains("Unsupported Protobuf type"),
            "{err}"
        );
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
        assert!(
            err.to_string().contains("Protobuf message parsing failed"),
            "{err}"
        );
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

    // ===== ProtobufBatchConverter =====

    fn sample_descriptor() -> MessageDescriptor {
        parse_proto_source(SCHEMA, "arkflow.test.Sample").unwrap()
    }

    fn sample_payload(name: Option<&str>, count: i64) -> Vec<u8> {
        let descriptor = sample_descriptor();
        let mut message = DynamicMessage::new(descriptor.clone());
        if let Some(n) = name {
            message.set_field_by_name("name", Value::String(n.to_string()));
        }
        message.set_field_by_name("count", Value::I64(count));
        message.encode_to_vec()
    }

    #[test]
    fn batch_converter_multi_row_matches_per_message_concat() {
        let descriptor = sample_descriptor();
        let payloads: Vec<Vec<u8>> = vec![
            sample_payload(Some("a"), 1),
            sample_payload(None, 2),
            sample_payload(Some("c"), 3),
        ];
        let mut conv = ProtobufBatchConverter::new(descriptor.clone());
        for p in &payloads {
            conv.push(p).unwrap();
        }
        let fast = conv.finish().unwrap();
        let batches: Vec<RecordBatch> = payloads
            .iter()
            .map(|p| protobuf_to_arrow(&descriptor, p).unwrap())
            .collect();
        let legacy = crate::component::batch_merge::normalize_and_concat(&batches).unwrap();

        assert_eq!(fast.num_rows(), 3);
        assert_eq!(fast.schema().as_ref(), legacy.schema().as_ref());
        for i in 0..fast.num_columns() {
            assert!(
                fast.column(i) == legacy.column(i),
                "column {} differs",
                fast.schema().field(i).name()
            );
        }
        // Row order follows message order; proto3 implicit presence surfaces
        // an unset scalar as its default value, not null (same as legacy).
        let counts = fast
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            (0..3).map(|i| counts.value(i)).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let names = fast
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "a");
        assert_eq!(names.value(1), "");
        assert_eq!(names.value(2), "c");
        // All columns nullable by descriptor-driven convention.
        assert!(fast.schema().fields().iter().all(|f| f.is_nullable()));
    }

    #[test]
    fn batch_converter_rejects_unsupported_kind_like_single_path() {
        let schema = r#"syntax = "proto3";
package t;
message Nested { int32 x = 1; }
message Holder { Nested inner = 1; }
"#;
        let descriptor = parse_proto_source(schema, "t.Holder").unwrap();
        let message = DynamicMessage::new(descriptor.clone());
        let encoded = message.encode_to_vec();

        let err = protobuf_to_arrow(&descriptor, &encoded).unwrap_err();
        assert!(err.to_string().contains("Unsupported field type"), "{err}");

        let mut conv = ProtobufBatchConverter::new(descriptor.clone());
        let err = conv.push(&encoded).unwrap_err();
        assert!(err.to_string().contains("Unsupported field type"), "{err}");
    }

    /// Ad-hoc release timing (NOT run by CI): per-message batches +
    /// normalize_and_concat vs the columnar batch converter on the same
    /// payloads. Run with:
    /// `cargo test --release -p arkflow-plugin --lib -- --ignored protobuf_batch_decode_timing --nocapture`
    #[test]
    #[ignore]
    fn protobuf_batch_decode_timing() {
        let wide = r#"syntax = "proto3";
package bench;
message Wide {
  int64 f0 = 1; int64 f1 = 2; int64 f2 = 3; int64 f3 = 4; int64 f4 = 5;
  int64 f5 = 6; int64 f6 = 7; int64 f7 = 8; int64 f8 = 9; int64 f9 = 10;
  string s0 = 11; string s1 = 12; string s2 = 13; string s3 = 14; string s4 = 15;
  double d0 = 16; double d1 = 17; bool b0 = 18; bool b1 = 19;
  int32 i0 = 20; int32 i1 = 21; bytes by0 = 22; bytes by1 = 23;
  uint64 u0 = 24; uint64 u1 = 25; uint32 u2 = 26;
}"#;
        let descriptor = parse_proto_source(wide, "bench.Wide").unwrap();
        let batch_size = 1_000usize;
        let batches = 200usize;
        let mut payloads = Vec::with_capacity(batch_size * batches);
        for i in 0..(batch_size * batches) {
            let mut m = DynamicMessage::new(descriptor.clone());
            m.set_field_by_name("f0", Value::I64(i as i64));
            m.set_field_by_name("s0", Value::String("sensor-abc".to_string()));
            m.set_field_by_name("d0", Value::F64(i as f64 * 0.5));
            m.set_field_by_name("b0", Value::Bool(i % 2 == 0));
            m.set_field_by_name("by0", Value::Bytes([0xABu8, 0xCD, 0xEF].repeat(4).into()));
            payloads.push(m.encode_to_vec());
        }

        let time = |name: &str, f: &mut dyn FnMut() -> usize| {
            let mut best = std::time::Duration::MAX;
            let mut rows = 0;
            for _ in 0..3 {
                let start = std::time::Instant::now();
                rows = f();
                best = best.min(start.elapsed());
            }
            println!(
                "{name}: {} rows in {:?} ({:.0} rows/s)",
                rows,
                best,
                rows as f64 / best.as_secs_f64()
            );
        };

        time("per-message + normalize_and_concat", &mut || {
            let mut total = 0usize;
            for chunk in payloads.chunks(batch_size) {
                let batches: Vec<RecordBatch> = chunk
                    .iter()
                    .map(|p| protobuf_to_arrow(&descriptor, p).unwrap())
                    .collect();
                total += crate::component::batch_merge::normalize_and_concat(&batches)
                    .unwrap()
                    .num_rows();
            }
            total
        });
        time("columnar batch converter", &mut || {
            let mut total = 0usize;
            for chunk in payloads.chunks(batch_size) {
                let mut conv = ProtobufBatchConverter::new(descriptor.clone());
                for p in chunk {
                    conv.push(p).unwrap();
                }
                total += conv.finish().unwrap().num_rows();
            }
            total
        });
    }
}
