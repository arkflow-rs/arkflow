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

//! Protobuf Codec Components
//!
//! The codec used to convert between Protobuf data and the Arrow format

use arkflow_core::codec::{Codec, CodecBuilder, Decoder, Encoder};
use arkflow_core::{codec, Bytes, Error, MessageBatch, Resource};
use datafusion::arrow;
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
use serde::{Deserialize, Serialize};
use serde_json;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

/// Protobuf codec configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProtobufCodecConfig {
    /// Protobuf message type descriptor file paths
    proto_inputs: Vec<String>,
    /// Include paths for proto files
    proto_includes: Option<Vec<String>>,
    /// Protobuf message type name
    message_type: String,
}

/// Protobuf Codec
struct ProtobufCodec {
    config: ProtobufCodecConfig,
    descriptor: MessageDescriptor,
}

impl ProtobufCodec {
    /// Create a new Protobuf codec
    fn new(config: ProtobufCodecConfig) -> Result<Self, Error> {
        let file_descriptor_set = Self::parse_proto_file(&config)?;

        let descriptor_pool = prost_reflect::DescriptorPool::from_file_descriptor_set(
            file_descriptor_set,
        )
        .map_err(|e| Error::Config(format!("Unable to create Protobuf descriptor pool: {}", e)))?;

        let message_descriptor = descriptor_pool
            .get_message_by_name(&config.message_type)
            .ok_or_else(|| {
                Error::Config(format!(
                    "The message type could not be found: {}",
                    config.message_type
                ))
            })?;

        Ok(Self {
            config,
            descriptor: message_descriptor,
        })
    }

    /// Parse and generate a FileDescriptorSet from the .proto file
    fn parse_proto_file(c: &ProtobufCodecConfig) -> Result<FileDescriptorSet, Error> {
        let mut proto_inputs: Vec<String> = vec![];
        for x in &c.proto_inputs {
            let files_in_dir_result = list_files_in_dir(x)
                .map_err(|e| Error::Config(format!("Failed to list proto files: {}", e)))?;
            proto_inputs.extend(
                files_in_dir_result
                    .iter()
                    .filter(|path| path.extension().map_or(false, |ext| ext == "proto"))
                    .filter_map(|path| path.to_str().map(|s| s.to_string()))
                    .collect::<Vec<_>>(),
            )
        }
        let proto_includes = c.proto_includes.clone().unwrap_or(c.proto_inputs.clone());

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

    /// Convert Protobuf data to Arrow format
    fn protobuf_to_arrow(&self, data: &[u8]) -> Result<RecordBatch, Error> {
        let proto_msg = DynamicMessage::decode(self.descriptor.clone(), data)
            .map_err(|e| Error::Process(format!("Protobuf message parsing failed: {}", e)))?;

        let descriptor_fields = self.descriptor.fields();
        // Building an Arrow Schema
        let mut fields = Vec::with_capacity(descriptor_fields.len());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(descriptor_fields.len());

        // Iterate over all fields of a Protobuf message
        for field in descriptor_fields {
            let field_name = field.name();

            let field_value_opt = proto_msg.get_field_by_name(field_name);
            if field_value_opt.is_none() {
                continue;
            }
            let field_value = field_value_opt.unwrap();
            match field_value.as_ref() {
                Value::Bool(value) => {
                    fields.push(Field::new(field_name, DataType::Boolean, false));
                    columns.push(Arc::new(BooleanArray::from(vec![value.clone()])));
                }
                Value::I32(value) => {
                    fields.push(Field::new(field_name, DataType::Int32, false));
                    columns.push(Arc::new(Int32Array::from(vec![value.clone()])));
                }
                Value::I64(value) => {
                    fields.push(Field::new(field_name, DataType::Int64, false));
                    columns.push(Arc::new(Int64Array::from(vec![value.clone()])));
                }
                Value::U32(value) => {
                    fields.push(Field::new(field_name, DataType::UInt32, false));
                    columns.push(Arc::new(UInt32Array::from(vec![value.clone()])));
                }
                Value::U64(value) => {
                    fields.push(Field::new(field_name, DataType::UInt64, false));
                    columns.push(Arc::new(UInt64Array::from(vec![value.clone()])));
                }
                Value::F32(value) => {
                    fields.push(Field::new(field_name, DataType::Float32, false));
                    columns.push(Arc::new(Float32Array::from(vec![value.clone()])))
                }
                Value::F64(value) => {
                    fields.push(Field::new(field_name, DataType::Float64, false));
                    columns.push(Arc::new(Float64Array::from(vec![value.clone()])));
                }
                Value::String(value) => {
                    fields.push(Field::new(field_name, DataType::Utf8, false));
                    columns.push(Arc::new(StringArray::from(vec![value.clone()])));
                }
                Value::Bytes(value) => {
                    fields.push(Field::new(field_name, DataType::Binary, false));
                    columns.push(Arc::new(BinaryArray::from(vec![value.as_ref()])));
                }
                Value::EnumNumber(value) => {
                    fields.push(Field::new(field_name, DataType::Int32, false));
                    columns.push(Arc::new(Int32Array::from(vec![value.clone()])));
                }
                _ => {
                    return Err(Error::Process(format!(
                        "Unsupported field type: {}",
                        field_name
                    )));
                }
            }
        }

        // Create RecordBatch
        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns)
            .map_err(|e| Error::Process(format!("Creating an Arrow record batch failed: {}", e)))
    }

    /// Convert Arrow format to Protobuf.
    fn arrow_to_protobuf(&self, batch: &MessageBatch) -> Result<Vec<Bytes>, Error> {
        // Create a new dynamic message
        let mut vec = Vec::with_capacity(batch.len());
        let len = batch.len();
        for _ in 0..len {
            let proto_msg = DynamicMessage::new(self.descriptor.clone());
            vec.push(proto_msg);
        }

        // Get the Arrow schema.
        let schema = batch.schema();

        for (i, field) in schema.fields().iter().enumerate() {
            let field_name = field.name();

            if let Some(proto_field) = self.descriptor.get_field_by_name(field_name) {
                let column = batch.column(i);

                match proto_field.kind() {
                    prost_reflect::Kind::Bool => {
                        if let Some(value) = column.as_any().downcast_ref::<BooleanArray>() {
                            for j in 0..value.len() {
                                if let Some(msg) = vec.get_mut(j) {
                                    msg.set_field_by_name(field_name, Value::Bool(value.value(j)));
                                }
                            }
                        }
                    }
                    prost_reflect::Kind::Int32
                    | prost_reflect::Kind::Sint32
                    | prost_reflect::Kind::Sfixed32 => {
                        if let Some(value) = column.as_any().downcast_ref::<Int32Array>() {
                            for j in 0..value.len() {
                                if let Some(msg) = vec.get_mut(j) {
                                    msg.set_field_by_name(field_name, Value::I32(value.value(j)));
                                }
                            }
                        }
                    }
                    prost_reflect::Kind::Int64
                    | prost_reflect::Kind::Sint64
                    | prost_reflect::Kind::Sfixed64 => {
                        if let Some(value) = column.as_any().downcast_ref::<Int64Array>() {
                            for j in 0..value.len() {
                                if let Some(msg) = vec.get_mut(j) {
                                    msg.set_field_by_name(field_name, Value::I64(value.value(j)));
                                }
                            }
                        }
                    }
                    prost_reflect::Kind::Uint32 | prost_reflect::Kind::Fixed32 => {
                        if let Some(value) = column.as_any().downcast_ref::<UInt32Array>() {
                            for j in 0..value.len() {
                                if let Some(msg) = vec.get_mut(j) {
                                    msg.set_field_by_name(field_name, Value::U32(value.value(j)));
                                }
                            }
                        }
                    }
                    prost_reflect::Kind::Uint64 | prost_reflect::Kind::Fixed64 => {
                        if let Some(value) = column.as_any().downcast_ref::<UInt64Array>() {
                            for j in 0..value.len() {
                                if let Some(msg) = vec.get_mut(j) {
                                    msg.set_field_by_name(field_name, Value::U64(value.value(j)));
                                }
                            }
                        }
                    }
                    prost_reflect::Kind::Float => {
                        if let Some(value) = column.as_any().downcast_ref::<Float32Array>() {
                            for j in 0..value.len() {
                                if let Some(msg) = vec.get_mut(j) {
                                    msg.set_field_by_name(field_name, Value::F32(value.value(j)));
                                }
                            }
                        }
                    }
                    prost_reflect::Kind::Double => {
                        if let Some(value) = column.as_any().downcast_ref::<Float64Array>() {
                            for j in 0..value.len() {
                                if let Some(msg) = vec.get_mut(j) {
                                    msg.set_field_by_name(field_name, Value::F64(value.value(j)));
                                }
                            }
                        }
                    }
                    prost_reflect::Kind::String => {
                        if let Some(value) = column.as_any().downcast_ref::<StringArray>() {
                            for j in 0..value.len() {
                                if let Some(msg) = vec.get_mut(j) {
                                    msg.set_field_by_name(
                                        field_name,
                                        Value::String(value.value(j).to_string()),
                                    );
                                }
                            }
                        }
                    }
                    prost_reflect::Kind::Bytes => {
                        if let Some(value) = column.as_any().downcast_ref::<BinaryArray>() {
                            for j in 0..value.len() {
                                if let Some(msg) = vec.get_mut(j) {
                                    msg.set_field_by_name(
                                        field_name,
                                        Value::Bytes(value.value(j).to_vec().into()),
                                    );
                                }
                            }
                        }
                    }
                    prost_reflect::Kind::Enum(_) => {
                        if let Some(value) = column.as_any().downcast_ref::<Int32Array>() {
                            for j in 0..value.len() {
                                if let Some(msg) = vec.get_mut(j) {
                                    msg.set_field_by_name(
                                        field_name,
                                        Value::EnumNumber(value.value(j)),
                                    );
                                }
                            }
                        }
                    }
                    _ => {
                        return Err(Error::Process(format!(
                            "Unsupported Protobuf type: {:?}",
                            proto_field.kind()
                        )))
                    }
                }
            }
        }

        Ok(vec
            .into_iter()
            .map(|proto_msg| {
                let mut buf = Vec::new();
                proto_msg
                    .encode(&mut buf)
                    .map_err(|e| Error::Process(format!("Protobuf encoding failed: {}", e)))?;
                Ok(buf)
            })
            .collect::<Result<Vec<_>, Error>>()?)
    }
}

impl Encoder for ProtobufCodec {
    fn encode(&self, b: MessageBatch) -> Result<Vec<Bytes>, Error> {
        Ok(self.arrow_to_protobuf(&b)?)
    }
}

impl Decoder for ProtobufCodec {
    fn decode(&self, b: Vec<Bytes>) -> Result<MessageBatch, Error> {
        let mut batches = Vec::with_capacity(b.len());

        for data in b {
            let record_batch = self.protobuf_to_arrow(&data)?;
            batches.push(record_batch);
        }

        if batches.is_empty() {
            return Ok(MessageBatch::new_arrow(RecordBatch::new_empty(Arc::new(
                Schema::empty(),
            ))));
        }

        let schema = batches[0].schema();
        let merged_batch = arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| Error::Process(format!("Batch merge failed: {}", e)))?;

        Ok(MessageBatch::new_arrow(merged_batch))
    }
}

struct ProtobufCodecBuilder;

impl CodecBuilder for ProtobufCodecBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Codec>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Protobuf codec configuration is missing".to_string(),
            ));
        }

        let config: ProtobufCodecConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(ProtobufCodec::new(config)?))
    }
}

fn list_files_in_dir<P: AsRef<Path>>(dir: P) -> io::Result<Vec<PathBuf>> {
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

pub(crate) fn init() -> Result<(), Error> {
    codec::register_codec_builder("protobuf", Arc::new(ProtobufCodecBuilder))?;
    Ok(())
}
