//! Protobuf Processor Components
//!
//! The processor used to convert between Protobuf data and the Arrow format

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
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::{fs, io};

use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Content, Error, MessageBatch};
use protobuf::Message as ProtobufMessage;

/// Protobuf format conversion processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtobufProcessorConfig {
    /// Protobuf message type descriptor file path
    pub proto_inputs: Vec<String>,
    pub proto_includes: Option<Vec<String>>,
    /// Protobuf message type name
    pub message_type: String,
}

/// Protobuf Format Conversion Processor
pub struct ProtobufProcessor {
    _config: ProtobufProcessorConfig,
    descriptor: MessageDescriptor,
}

impl ProtobufProcessor {
    /// Create a new Protobuf format conversion processor
    pub fn new(config: ProtobufProcessorConfig) -> Result<Self, Error> {
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
                    .filter(|path| path.ends_with(".proto"))
                    .map(|path| format!("{}/{}", x, path))
                    .collect::<Vec<_>>(),
            )
        }
        let proto_includes = c.proto_includes.clone().unwrap_or(c.proto_inputs.clone());

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
        // 解析Protobuf消息
        let proto_msg = DynamicMessage::decode(self.descriptor.clone(), data)
            .map_err(|e| Error::Process(format!("Protobuf message parsing failed: {}", e)))?;

        // Building an Arrow Schema
        let mut fields = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();

        // Iterate over all fields of a Protobuf message
        for field in self.descriptor.fields() {
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
    fn arrow_to_protobuf(&self, batch: &RecordBatch) -> Result<Vec<u8>, Error> {
        // Create a new dynamic message
        let mut proto_msg = DynamicMessage::new(self.descriptor.clone());

        // Get the Arrow schema.
        let schema = batch.schema();

        // Ensure there is only one line of data.
        if batch.num_rows() != 1 {
            return Err(Error::Process(
                "Only supports single-line Arrow data conversion to Protobuf.".to_string(),
            ));
        }

        for (i, field) in schema.fields().iter().enumerate() {
            let field_name = field.name();

            if let Some(proto_field) = self.descriptor.get_field_by_name(field_name) {
                let column = batch.column(i);

                match proto_field.kind() {
                    prost_reflect::Kind::Bool => {
                        if let Some(value) = column.as_any().downcast_ref::<BooleanArray>() {
                            if value.len() > 0 {
                                proto_msg
                                    .set_field_by_name(field_name, Value::Bool(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::Int32
                    | prost_reflect::Kind::Sint32
                    | prost_reflect::Kind::Sfixed32 => {
                        if let Some(value) = column.as_any().downcast_ref::<Int32Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::I32(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::Int64
                    | prost_reflect::Kind::Sint64
                    | prost_reflect::Kind::Sfixed64 => {
                        if let Some(value) = column.as_any().downcast_ref::<Int64Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::I64(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::Uint32 | prost_reflect::Kind::Fixed32 => {
                        if let Some(value) = column.as_any().downcast_ref::<UInt32Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::U32(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::Uint64 | prost_reflect::Kind::Fixed64 => {
                        if let Some(value) = column.as_any().downcast_ref::<UInt64Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::U64(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::Float => {
                        if let Some(value) = column.as_any().downcast_ref::<Float32Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::F32(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::Double => {
                        if let Some(value) = column.as_any().downcast_ref::<Float64Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::F64(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::String => {
                        if let Some(value) = column.as_any().downcast_ref::<StringArray>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(
                                    field_name,
                                    Value::String(value.value(0).to_string()),
                                );
                            }
                        }
                    }
                    prost_reflect::Kind::Bytes => {
                        if let Some(value) = column.as_any().downcast_ref::<BinaryArray>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(
                                    field_name,
                                    Value::Bytes(value.value(0).to_vec().into()),
                                );
                            }
                        }
                    }
                    prost_reflect::Kind::Enum(_) => {
                        if let Some(value) = column.as_any().downcast_ref::<Int32Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(
                                    field_name,
                                    Value::EnumNumber(value.value(0)),
                                );
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

        let mut buf = Vec::new();
        proto_msg
            .encode(&mut buf)
            .map_err(|e| Error::Process(format!("Protobuf encoding failed: {}", e)))?;

        Ok(buf)
    }
}

#[async_trait]
impl Processor for ProtobufProcessor {
    async fn process(&self, msg: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        if msg.is_empty() {
            return Ok(vec![]);
        }
        match msg.content {
            Content::Arrow(v) => {
                // Convert Arrow format to Protobuf.
                let proto_data = self.arrow_to_protobuf(&v)?;
                let new_msg = MessageBatch::new_binary(vec![proto_data]);

                Ok(vec![new_msg])
            }
            Content::Binary(v) => {
                if v.is_empty() {
                    return Ok(vec![]);
                }
                let mut batches = Vec::with_capacity(v.len());
                for x in v {
                    // Convert Protobuf messages to Arrow format.
                    let batch = self.protobuf_to_arrow(&x)?;
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

fn list_files_in_dir<P: AsRef<Path>>(dir: P) -> io::Result<Vec<String>> {
    let mut files = Vec::new();
    if dir.as_ref().is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(file_name) = path.file_name() {
                    if let Some(file_name_str) = file_name.to_str() {
                        files.push(file_name_str.to_string());
                    }
                }
            }
        }
    }
    Ok(files)
}

pub(crate) struct ProtobufProcessorBuilder;
impl ProcessorBuilder for ProtobufProcessorBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Batch processor configuration is missing".to_string(),
            ));
        }
        let config: ProtobufProcessorConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(ProtobufProcessor::new(config)?))
    }
}

pub fn init() {
    register_processor_builder("protobuf", Arc::new(ProtobufProcessorBuilder));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    // Helper function to create a temporary proto file for testing
    fn create_test_proto_file() -> (tempfile::TempDir, String, String) {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let proto_dir = temp_dir.path().to_str().unwrap().to_string();

        // Create a simple proto file
        let proto_file_path = temp_dir.path().join("test.proto");
        let mut file = File::create(&proto_file_path).unwrap();

        // Write proto content
        let proto_content = r#"syntax = "proto3";

package test;

message TestMessage {
    bool bool_field = 1;
    int32 int32_field = 2;
    int64 int64_field = 3;
    uint32 uint32_field = 4;
    uint64 uint64_field = 5;
    float float_field = 6;
    double double_field = 7;
    string string_field = 8;
    bytes bytes_field = 9;
    enum TestEnum {
        UNKNOWN = 0;
        VALUE1 = 1;
        VALUE2 = 2;
    }
    TestEnum enum_field = 10;
}
"#;

        file.write_all(proto_content.as_bytes()).unwrap();

        (temp_dir, proto_dir, "test.TestMessage".to_string())
    }

    // Helper function to create a test Protobuf message
    fn create_test_protobuf_message(descriptor: &MessageDescriptor) -> Vec<u8> {
        let mut message = DynamicMessage::new(descriptor.clone());

        // Set field values
        message.set_field_by_name("bool_field", Value::Bool(true));
        message.set_field_by_name("int32_field", Value::I32(42));
        message.set_field_by_name("int64_field", Value::I64(1234567890));
        message.set_field_by_name("uint32_field", Value::U32(42));
        message.set_field_by_name("uint64_field", Value::U64(1234567890));
        message.set_field_by_name("float_field", Value::F32(3.14));
        message.set_field_by_name("double_field", Value::F64(2.71828));
        message.set_field_by_name("string_field", Value::String("test string".to_string()));
        message.set_field_by_name("bytes_field", Value::Bytes(vec![1, 2, 3, 4].into()));
        message.set_field_by_name("enum_field", Value::EnumNumber(1));

        // Encode the message
        let mut buf = Vec::new();
        message.encode(&mut buf).unwrap();
        buf
    }

    // Helper function to create a test Arrow RecordBatch
    fn create_test_arrow_batch() -> RecordBatch {
        // Create fields for the schema
        let fields = vec![
            Field::new("bool_field", DataType::Boolean, false),
            Field::new("int32_field", DataType::Int32, false),
            Field::new("int64_field", DataType::Int64, false),
            Field::new("uint32_field", DataType::UInt32, false),
            Field::new("uint64_field", DataType::UInt64, false),
            Field::new("float_field", DataType::Float32, false),
            Field::new("double_field", DataType::Float64, false),
            Field::new("string_field", DataType::Utf8, false),
            Field::new("bytes_field", DataType::Binary, false),
            Field::new("enum_field", DataType::Int32, false),
        ];

        // Create columns
        let columns: Vec<ArrayRef> = vec![
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(Int32Array::from(vec![42])),
            Arc::new(Int64Array::from(vec![1234567890])),
            Arc::new(UInt32Array::from(vec![42])),
            Arc::new(UInt64Array::from(vec![1234567890])),
            Arc::new(Float32Array::from(vec![3.14])),
            Arc::new(Float64Array::from(vec![2.71828])),
            Arc::new(StringArray::from(vec!["test string"])),
            Arc::new(BinaryArray::from(vec![&[1, 2, 3, 4][..]])),
            Arc::new(Int32Array::from(vec![1])),
        ];

        // Create schema and record batch
        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns).unwrap()
    }

    #[tokio::test]
    async fn test_protobuf_processor_creation() {
        // Create a test proto file
        let (temp_dir, proto_dir, message_type) = create_test_proto_file();

        // Create processor config
        let config = ProtobufProcessorConfig {
            proto_inputs: vec![proto_dir],
            proto_includes: None,
            message_type,
        };

        // Test processor creation
        let processor = ProtobufProcessor::new(config);
        assert!(
            processor.is_ok(),
            "Failed to create ProtobufProcessor: {:?}",
            processor.err()
        );

        // Clean up is handled automatically when temp_dir goes out of scope
        drop(temp_dir);
    }

    #[tokio::test]
    async fn test_protobuf_to_arrow_conversion() {
        // Create a test proto file
        let (temp_dir, proto_dir, message_type) = create_test_proto_file();

        // Create processor config
        let config = ProtobufProcessorConfig {
            proto_inputs: vec![proto_dir],
            proto_includes: None,
            message_type,
        };

        // Create processor
        let processor = ProtobufProcessor::new(config).unwrap();

        // Create test protobuf message
        let proto_data = create_test_protobuf_message(&processor.descriptor);

        // Test conversion from protobuf to arrow
        let arrow_batch = processor.protobuf_to_arrow(&proto_data);
        assert!(
            arrow_batch.is_ok(),
            "Failed to convert Protobuf to Arrow: {:?}",
            arrow_batch.err()
        );

        let batch = arrow_batch.unwrap();

        // Verify the converted data
        assert_eq!(batch.num_rows(), 1, "Expected 1 row in the Arrow batch");
        assert_eq!(
            batch.num_columns(),
            10,
            "Expected 10 columns in the Arrow batch"
        );

        // Verify specific field values
        let bool_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(bool_array.value(0), true);

        let int32_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(int32_array.value(0), 42);

        let string_array = batch
            .column(7)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(string_array.value(0), "test string");

        // Clean up
        drop(temp_dir);
    }

    #[tokio::test]
    async fn test_arrow_to_protobuf_conversion() {
        // Create a test proto file
        let (temp_dir, proto_dir, message_type) = create_test_proto_file();

        // Create processor config
        let config = ProtobufProcessorConfig {
            proto_inputs: vec![proto_dir],
            proto_includes: None,
            message_type,
        };

        // Create processor
        let processor = ProtobufProcessor::new(config).unwrap();

        // Create test Arrow batch
        let arrow_batch = create_test_arrow_batch();

        // Test conversion from Arrow to Protobuf
        let proto_data = processor.arrow_to_protobuf(&arrow_batch);
        assert!(
            proto_data.is_ok(),
            "Failed to convert Arrow to Protobuf: {:?}",
            proto_data.err()
        );

        // Verify the converted data by converting it back to Arrow
        let proto_bytes = proto_data.unwrap();
        let arrow_batch_2 = processor.protobuf_to_arrow(&proto_bytes);
        assert!(
            arrow_batch_2.is_ok(),
            "Failed to convert back to Arrow: {:?}",
            arrow_batch_2.err()
        );

        let batch = arrow_batch_2.unwrap();

        // Verify specific field values after round-trip conversion
        let bool_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(bool_array.value(0), true);

        let int32_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(int32_array.value(0), 42);

        let string_array = batch
            .column(7)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(string_array.value(0), "test string");

        // Clean up
        drop(temp_dir);
    }

    #[tokio::test]
    async fn test_process_empty_batch() {
        // Create a test proto file
        let (temp_dir, proto_dir, message_type) = create_test_proto_file();

        // Create processor config
        let config = ProtobufProcessorConfig {
            proto_inputs: vec![proto_dir],
            proto_includes: None,
            message_type,
        };

        // Create processor
        let processor = ProtobufProcessor::new(config).unwrap();

        // Test processing empty batch
        let empty_batch = MessageBatch::new_binary(vec![]);
        let result = processor.process(empty_batch).await;

        assert!(
            result.is_ok(),
            "Failed to process empty batch: {:?}",
            result.err()
        );
        assert!(
            result.unwrap().is_empty(),
            "Expected empty result for empty batch"
        );

        // Clean up
        drop(temp_dir);
    }

    #[tokio::test]
    async fn test_process_binary_to_arrow() {
        // Create a test proto file
        let (temp_dir, proto_dir, message_type) = create_test_proto_file();

        // Create processor config
        let config = ProtobufProcessorConfig {
            proto_inputs: vec![proto_dir],
            proto_includes: None,
            message_type,
        };

        // Create processor
        let processor = ProtobufProcessor::new(config).unwrap();

        // Create test protobuf message
        let proto_data = create_test_protobuf_message(&processor.descriptor);

        // Create message batch with binary content
        let msg_batch = MessageBatch::new_binary(vec![proto_data]);

        // Test processing
        let result = processor.process(msg_batch).await;
        assert!(
            result.is_ok(),
            "Failed to process binary to arrow: {:?}",
            result.err()
        );

        let batches = result.unwrap();
        assert_eq!(batches.len(), 1, "Expected 1 message batch");

        // Verify the result is in Arrow format
        match &batches[0].content {
            Content::Arrow(batch) => {
                assert_eq!(batch.num_rows(), 1, "Expected 1 row");
                assert_eq!(batch.num_columns(), 10, "Expected 10 columns");
            }
            _ => panic!("Expected Arrow content"),
        }

        // Clean up
        drop(temp_dir);
    }

    #[tokio::test]
    async fn test_process_arrow_to_binary() {
        // Create a test proto file
        let (temp_dir, proto_dir, message_type) = create_test_proto_file();

        // Create processor config
        let config = ProtobufProcessorConfig {
            proto_inputs: vec![proto_dir],
            proto_includes: None,
            message_type,
        };

        // Create processor
        let processor = ProtobufProcessor::new(config).unwrap();

        // Create test Arrow batch
        let arrow_batch = create_test_arrow_batch();

        // Create message batch with Arrow content
        let msg_batch = MessageBatch::new_arrow(arrow_batch);

        // Test processing
        let result = processor.process(msg_batch).await;
        assert!(
            result.is_ok(),
            "Failed to process arrow to binary: {:?}",
            result.err()
        );

        let batches = result.unwrap();
        assert_eq!(batches.len(), 1, "Expected 1 message batch");

        // Verify the result is in Binary format
        match &batches[0].content {
            Content::Binary(data) => {
                assert_eq!(data.len(), 1, "Expected 1 binary message");
            }
            _ => panic!("Expected Binary content"),
        }

        // Clean up
        drop(temp_dir);
    }
}
