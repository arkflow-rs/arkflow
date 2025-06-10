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

//! Protobuf Processor Components
//!
//! The processor used to convert between Protobuf data and the Arrow format

use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Bytes, Error, MessageBatch, Resource, DEFAULT_BINARY_VALUE_FIELD};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::data_type::AsBytes;
use prost_reflect::prost::Message;
use prost_reflect::prost_types::FileDescriptorSet;
use prost_reflect::{DynamicMessage, MessageDescriptor, Value};
use protobuf::Message as ProtobufMessage;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

/// Protobuf format conversion processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProtobufProcessorConfig {
    /// Protobuf message type descriptor file path
    proto_inputs: Vec<String>,
    proto_includes: Option<Vec<String>>,
    /// Protobuf message type name
    message_type: String,
    mode: ToType,
    fields_to_include: Option<HashSet<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ToType {
    ArrowToProtobuf,
    ProtobufToArrow(ArrowConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArrowConfig {
    value_field: Option<String>,
}
/// Protobuf Format Conversion Processor
struct ProtobufProcessor {
    _config: ProtobufProcessorConfig,
    descriptor: MessageDescriptor,
}

impl ProtobufProcessor {
    /// Create a new Protobuf format conversion processor
    fn new(config: ProtobufProcessorConfig) -> Result<Self, Error> {
        // Check the file extension to see if it's a proto file or a binary descriptor file
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
            _config: config.clone(),
            descriptor: message_descriptor,
        })
    }

    /// Parse and generate a FileDescriptorSet from the .proto file
    fn parse_proto_file(c: &ProtobufProcessorConfig) -> Result<FileDescriptorSet, Error> {
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
                    columns.push(Arc::new(BinaryArray::from(vec![value.as_bytes()])));
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
                } // Value::Message(_) => {}
                  // Value::List(_) => {}
                  // Value::Map(_) => {}
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
                Ok(buf) // 修改这里，返回 Result<Vec<u8>, Error>
            })
            .collect::<Result<Vec<_>, Error>>()?)
    }
}

#[async_trait]
impl Processor for ProtobufProcessor {
    async fn process(&self, msg: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        if msg.is_empty() {
            return Ok(vec![]);
        }

        match self._config.mode {
            ToType::ArrowToProtobuf => {
                // Convert Arrow format to Protobuf.
                let proto_data = if let Some(ref fields_to_include) = self._config.fields_to_include
                {
                    let filter_msg = msg.filter_columns(fields_to_include)?;
                    self.arrow_to_protobuf(&filter_msg)?
                } else {
                    self.arrow_to_protobuf(&msg)?
                };

                Ok(vec![msg.new_binary_with_origin(proto_data)?])
            }
            ToType::ProtobufToArrow(ref c) => {
                if msg.is_empty() {
                    return Ok(vec![]);
                }

                let mut batches = Vec::with_capacity(msg.len());
                let result = msg.to_binary(
                    c.value_field
                        .as_deref()
                        .unwrap_or(DEFAULT_BINARY_VALUE_FIELD),
                )?;
                for x in result {
                    // Convert Protobuf messages to Arrow format.
                    let batch = self.protobuf_to_arrow(x)?;
                    batches.push(batch)
                }

                let schema = batches[0].schema();
                let batch = arrow::compute::concat_batches(&schema, &batches)
                    .map_err(|e| Error::Process(format!("Batch merge failed: {}", e)))?;
                Ok(vec![MessageBatch::new_arrow(batch)])
            }
        }
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProtobufToArrowProcessorConfig {
    #[serde(flatten)]
    c: CommonProtobufProcessorConfig,
    value_field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommonProtobufProcessorConfig {
    proto_inputs: Vec<String>,
    proto_includes: Option<Vec<String>>,
    /// Protobuf message type name
    message_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArrowToProtobufProcessorConfig {
    c: CommonProtobufProcessorConfig,
    fields_to_include: Option<HashSet<String>>,
}

impl From<ArrowToProtobufProcessorConfig> for ProtobufProcessorConfig {
    fn from(config: ArrowToProtobufProcessorConfig) -> Self {
        Self {
            proto_inputs: config.c.proto_inputs,
            proto_includes: config.c.proto_includes,
            message_type: config.c.message_type,
            mode: ToType::ArrowToProtobuf,
            fields_to_include: config.fields_to_include,
        }
    }
}

impl From<ProtobufToArrowProcessorConfig> for ProtobufProcessorConfig {
    fn from(config: ProtobufToArrowProcessorConfig) -> Self {
        Self {
            proto_inputs: config.c.proto_inputs,
            proto_includes: config.c.proto_includes,
            message_type: config.c.message_type,
            mode: ToType::ProtobufToArrow(ArrowConfig {
                value_field: config.value_field,
            }),
            fields_to_include: None,
        }
    }
}

struct ProtobufToArrowProcessorBuilder;
impl ProcessorBuilder for ProtobufToArrowProcessorBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "ProtobufToArrow processor configuration is missing".to_string(),
            ));
        }
        let config: ProtobufToArrowProcessorConfig =
            serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(ProtobufProcessor::new(config.into())?))
    }
}
struct ArrowToProtobufProcessorBuilder;
impl ProcessorBuilder for ArrowToProtobufProcessorBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "ArrowToProtobuf processor configuration is missing".to_string(),
            ));
        }
        let config: ArrowToProtobufProcessorConfig =
            serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(ProtobufProcessor::new(config.into())?))
    }
}

pub fn init() -> Result<(), Error> {
    register_processor_builder(
        "arrow_to_protobuf",
        Arc::new(ArrowToProtobufProcessorBuilder),
    )?;
    register_processor_builder(
        "protobuf_to_arrow",
        Arc::new(ProtobufToArrowProcessorBuilder),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::processor::ProcessorBuilder;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::cell::RefCell;
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::{tempdir, TempDir};

    fn create_test_proto_file() -> Result<(TempDir, PathBuf), Error> {
        let dir =
            tempdir().map_err(|e| Error::Process(format!("Failed to create temp dir: {}", e)))?;
        let proto_dir = dir.path().join("proto");
        std::fs::create_dir_all(&proto_dir)
            .map_err(|e| Error::Process(format!("Failed to create proto dir: {}", e)))?;

        let proto_file_path = proto_dir.join("test_message.proto");
        let mut file = File::create(&proto_file_path)
            .map_err(|e| Error::Process(format!("Failed to create proto file: {}", e)))?;

        let proto_content = r#"syntax = "proto3";

package test;

message TestMessage {
  int64 timestamp = 1;
  double value = 2;
  string sensor = 3;
}
"#;

        file.write_all(proto_content.as_bytes())
            .map_err(|e| Error::Process(format!("Failed to write proto file: {}", e)))?;

        file.flush()
            .map_err(|e| Error::Process(format!("Failed to flush proto file: {}", e)))?;

        Ok((dir, proto_dir))
    }

    #[tokio::test]
    async fn test_protobuf_to_arrow_conversion() -> Result<(), Error> {
        let (_x, proto_dir) = create_test_proto_file()?;

        let config = ProtobufToArrowProcessorConfig {
            c: CommonProtobufProcessorConfig {
                proto_inputs: vec![proto_dir.to_string_lossy().to_string()],
                proto_includes: None,
                message_type: "test.TestMessage".to_string(),
            },
            value_field: Some(DEFAULT_BINARY_VALUE_FIELD.to_string()),
        };

        let processor = ProtobufProcessor::new(config.into())?;

        let descriptor = processor.descriptor.clone();
        let mut test_message = DynamicMessage::new(descriptor);

        test_message.set_field_by_name("timestamp", Value::I64(1634567890));
        test_message.set_field_by_name("value", Value::F64(42.5));
        test_message.set_field_by_name("sensor", Value::String("temperature".to_string()));

        let mut encoded = Vec::new();
        test_message.encode(&mut encoded).unwrap();
        let msg_batch = MessageBatch::new_binary(vec![encoded])?;

        let result = processor.process(msg_batch).await?;
        assert_eq!(result.len(), 1);

        let batch = &result[0];

        assert_eq!(batch.schema().fields().len(), 3);

        let schema = batch.schema();
        let field_names: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        assert!(field_names.contains(&String::from("timestamp")));
        assert!(field_names.contains(&String::from("value")));
        assert!(field_names.contains(&String::from("sensor")));

        Ok(())
    }

    #[tokio::test]
    async fn test_arrow_to_protobuf_conversion() -> Result<(), Error> {
        let (_x, proto_dir) = create_test_proto_file()?;

        let config = ArrowToProtobufProcessorConfig {
            c: CommonProtobufProcessorConfig {
                proto_inputs: vec![proto_dir.to_string_lossy().to_string()],
                proto_includes: None,
                message_type: "test.TestMessage".to_string(),
            },
            fields_to_include: None,
        };

        let processor = ProtobufProcessor::new(config.into())?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("sensor", DataType::Utf8, false),
        ]));

        let timestamp_array = Int64Array::from(vec![1634567890]);
        let value_array = Float64Array::from(vec![42.5]);
        let sensor_array = StringArray::from(vec!["temperature"]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(timestamp_array),
                Arc::new(value_array),
                Arc::new(sensor_array),
            ],
        )
        .map_err(|e| Error::Process(format!("Failed to create record batch: {}", e)))?;

        let msg_batch = MessageBatch::new_arrow(batch);

        let result = processor.process(msg_batch).await?;
        assert_eq!(result.len(), 1);

        let binary_data = result[0].to_binary(DEFAULT_BINARY_VALUE_FIELD)?;
        assert_eq!(binary_data.len(), 1);

        let decoded_msg =
            DynamicMessage::decode(processor.descriptor.clone(), binary_data[0].as_ref())
                .map_err(|e| Error::Process(format!("Failed to decode protobuf: {}", e)))?;

        let timestamp = decoded_msg.get_field_by_name("timestamp").unwrap();
        let value = decoded_msg.get_field_by_name("value").unwrap();
        let sensor = decoded_msg.get_field_by_name("sensor").unwrap();

        assert_eq!(timestamp.as_ref(), &Value::I64(1634567890));
        assert_eq!(value.as_ref(), &Value::F64(42.5));
        assert_eq!(sensor.as_ref(), &Value::String("temperature".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn test_protobuf_processor_empty_batch() -> Result<(), Error> {
        let (_x, proto_dir) = create_test_proto_file()?;

        let config = ProtobufToArrowProcessorConfig {
            c: CommonProtobufProcessorConfig {
                proto_inputs: vec![proto_dir.to_string_lossy().to_string()],
                proto_includes: None,
                message_type: "test.TestMessage".to_string(),
            },
            value_field: None,
        };

        let processor = ProtobufProcessor::new(config.into())?;

        let empty_batch = MessageBatch::new_binary(vec![])?;

        let result = processor.process(empty_batch).await?;
        assert_eq!(result.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_processor_builder() {
        let result = ProtobufToArrowProcessorBuilder.build(
            None,
            &None,
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        );
        assert!(result.is_err());

        let result = ArrowToProtobufProcessorBuilder.build(
            None,
            &None,
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        );
        assert!(result.is_err());

        let (_x, proto_dir) = create_test_proto_file().unwrap();
        let config = serde_json::to_value(ProtobufToArrowProcessorConfig {
            c: CommonProtobufProcessorConfig {
                proto_inputs: vec![proto_dir.to_string_lossy().to_string()],
                proto_includes: None,
                message_type: "test.TestMessage".to_string(),
            },
            value_field: None,
        })
        .unwrap();

        let result = ProtobufToArrowProcessorBuilder.build(
            None,
            &Some(config),
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        );
        assert!(result.is_ok());
    }
}
