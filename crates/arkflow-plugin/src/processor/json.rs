//! Arrow Processor Components
//!
//! A processor for converting between binary data and the Arrow format

use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Bytes, Content, Error, MessageBatch};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, NullArray, StringArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::Value;
use std::sync::Arc;

/// Arrow format conversion processor configuration

/// Arrow Format Conversion Processor

pub struct JsonToArrowProcessor;

#[async_trait]
impl Processor for JsonToArrowProcessor {
    async fn process(&self, msg_batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        match msg_batch.content {
            Content::Arrow(_) => Err(Error::Process("The input must be binary data".to_string())),
            Content::Binary(v) => {
                let mut batches = Vec::with_capacity(v.len());
                for x in v {
                    let record_batch = json_to_arrow(&x)?;
                    batches.push(record_batch)
                }
                if batches.is_empty() {
                    return Ok(vec![]);
                }

                let schema = batches[0].schema();
                let batch = arrow::compute::concat_batches(&schema, &batches)
                    .map_err(|e| Error::Process(format!("Merge batches failed: {}", e)))?;
                Ok(vec![MessageBatch::new_arrow(batch)])
            }
        }
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

pub struct ArrowToJsonProcessor;

#[async_trait]
impl Processor for ArrowToJsonProcessor {
    async fn process(&self, msg_batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        match msg_batch.content {
            Content::Arrow(v) => {
                let json_data = arrow_to_json(&v)?;
                Ok(vec![MessageBatch::new_binary(vec![json_data])])
            }
            Content::Binary(_) => Err(Error::Process(
                "The input must be in Arrow format".to_string(),
            )),
        }
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

fn json_to_arrow(content: &Bytes) -> Result<RecordBatch, Error> {
    // 解析JSON内容
    let json_value: Value = serde_json::from_slice(content)
        .map_err(|e| Error::Process(format!("JSON解析错误: {}", e)))?;

    match json_value {
        Value::Object(obj) => {
            // 单个对象转换为单行表
            let mut fields = Vec::new();
            let mut columns: Vec<ArrayRef> = Vec::new();

            // 提取所有字段和值
            for (key, value) in obj {
                match value {
                    Value::Null => {
                        fields.push(Field::new(&key, DataType::Null, true));
                        // 空值列处理
                        columns.push(Arc::new(NullArray::new(1)));
                    }
                    Value::Bool(v) => {
                        fields.push(Field::new(&key, DataType::Boolean, false));
                        columns.push(Arc::new(BooleanArray::from(vec![v])));
                    }
                    Value::Number(v) => {
                        if v.is_i64() {
                            fields.push(Field::new(&key, DataType::Int64, false));
                            columns.push(Arc::new(Int64Array::from(vec![v.as_i64().unwrap()])));
                        } else if v.is_u64() {
                            fields.push(Field::new(&key, DataType::UInt64, false));
                            columns.push(Arc::new(UInt64Array::from(vec![v.as_u64().unwrap()])));
                        } else {
                            fields.push(Field::new(&key, DataType::Float64, false));
                            columns.push(Arc::new(Float64Array::from(vec![v
                                .as_f64()
                                .unwrap_or(0.0)])));
                        }
                    }
                    Value::String(v) => {
                        fields.push(Field::new(&key, DataType::Utf8, false));
                        columns.push(Arc::new(StringArray::from(vec![v])));
                    }
                    Value::Array(v) => {
                        fields.push(Field::new(&key, DataType::Utf8, false));
                        if let Ok(x) = serde_json::to_string(&v) {
                            columns.push(Arc::new(StringArray::from(vec![x])));
                        } else {
                            columns.push(Arc::new(StringArray::from(vec!["[]".to_string()])));
                        }
                    }
                    Value::Object(v) => {
                        fields.push(Field::new(&key, DataType::Utf8, false));
                        if let Ok(x) = serde_json::to_string(&v) {
                            columns.push(Arc::new(StringArray::from(vec![x])));
                        } else {
                            columns.push(Arc::new(StringArray::from(vec!["{}".to_string()])));
                        }
                    }
                };
            }

            // 创建schema和记录批次
            let schema = Arc::new(Schema::new(fields));
            RecordBatch::try_new(schema, columns)
                .map_err(|e| Error::Process(format!("创建Arrow记录批次失败: {}", e)))
        }
        _ => Err(Error::Process("输入必须是JSON对象".to_string())),
    }
}

/// Convert Arrow format to JSON
fn arrow_to_json(batch: &RecordBatch) -> Result<Vec<u8>, Error> {
    // 使用Arrow的JSON序列化功能
    let mut buf = Vec::new();
    let mut writer = arrow::json::ArrayWriter::new(&mut buf);
    writer
        .write(batch)
        .map_err(|e| Error::Process(format!("Arrow JSON序列化错误: {}", e)))?;
    writer
        .finish()
        .map_err(|e| Error::Process(format!("Arrow JSON序列化完成错误: {}", e)))?;

    Ok(buf)
}

pub(crate) struct JsonToArrowProcessorBuilder;
impl ProcessorBuilder for JsonToArrowProcessorBuilder {
    fn build(&self, _: &Option<serde_json::Value>) -> Result<Arc<dyn Processor>, Error> {
        Ok(Arc::new(JsonToArrowProcessor))
    }
}
pub(crate) struct ArrowToJsonProcessorBuilder;
impl ProcessorBuilder for ArrowToJsonProcessorBuilder {
    fn build(&self, _: &Option<serde_json::Value>) -> Result<Arc<dyn Processor>, Error> {
        Ok(Arc::new(ArrowToJsonProcessor))
    }
}

pub fn init() {
    register_processor_builder("arrow_to_json", Arc::new(ArrowToJsonProcessorBuilder));
    register_processor_builder("json_to_arrow", Arc::new(JsonToArrowProcessorBuilder));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // Helper function to create a simple JSON object for testing
    fn create_test_json() -> Vec<u8> {
        // Create a JSON object with different data types
        let mut map = HashMap::new();
        map.insert("null_field", Value::Null);
        map.insert("bool_field", Value::Bool(true));
        map.insert("int_field", Value::Number(serde_json::Number::from(42)));
        map.insert(
            "uint_field",
            Value::Number(serde_json::Number::from(100u64)),
        );
        map.insert(
            "float_field",
            Value::Number(serde_json::Number::from_f64(3.14).unwrap()),
        );
        map.insert("string_field", Value::String("test".to_string()));
        map.insert(
            "array_field",
            Value::Array(vec![Value::Number(serde_json::Number::from(1))]),
        );
        map.insert(
            "object_field",
            Value::Object({
                let mut inner = serde_json::Map::new();
                inner.insert("key".to_string(), Value::String("value".to_string()));
                inner
            }),
        );

        // Serialize to JSON bytes
        serde_json::to_vec(&map).unwrap()
    }

    #[tokio::test]
    async fn test_json_to_arrow_processor_success() {
        // Test successful conversion from JSON to Arrow
        let processor = JsonToArrowProcessor;
        let json_data = create_test_json();

        // Create a message batch with binary content
        let msg_batch = MessageBatch::new_binary(vec![json_data]);

        // Process the message batch
        let result = processor.process(msg_batch).await.unwrap();

        // Verify the result
        assert_eq!(result.len(), 1, "Should return one message batch");
        match &result[0].content {
            Content::Arrow(batch) => {
                // Verify the schema and data
                assert_eq!(batch.num_rows(), 1, "Should have one row");
                assert_eq!(batch.num_columns(), 8, "Should have 8 columns");

                // Verify column names
                let schema = batch.schema();
                let field_names: Vec<&str> =
                    schema.fields().iter().map(|f| f.name().as_str()).collect();
                assert!(field_names.contains(&"null_field"));
                assert!(field_names.contains(&"bool_field"));
                assert!(field_names.contains(&"int_field"));
                assert!(field_names.contains(&"uint_field"));
                assert!(field_names.contains(&"float_field"));
                assert!(field_names.contains(&"string_field"));
                assert!(field_names.contains(&"array_field"));
                assert!(field_names.contains(&"object_field"));
            }
            _ => panic!("Expected Arrow content"),
        }
    }

    #[tokio::test]
    async fn test_json_to_arrow_processor_empty_input() {
        // Test with empty input
        let processor = JsonToArrowProcessor;
        let msg_batch = MessageBatch::new_binary(vec![]);

        // Process the message batch
        let result = processor.process(msg_batch).await.unwrap();

        // Verify the result
        assert!(
            result.is_empty(),
            "Should return empty result for empty input"
        );
    }

    #[tokio::test]
    async fn test_json_to_arrow_processor_invalid_input() {
        // Test with invalid JSON input
        let processor = JsonToArrowProcessor;
        let invalid_json = b"{invalid json";

        // Create a message batch with invalid JSON content
        let msg_batch = MessageBatch::new_binary(vec![invalid_json.to_vec()]);

        // Process the message batch should fail
        let result = processor.process(msg_batch).await;
        assert!(result.is_err(), "Should return error for invalid JSON");
    }

    #[tokio::test]
    async fn test_json_to_arrow_processor_non_object_input() {
        // Test with JSON that is not an object (e.g., array)
        let processor = JsonToArrowProcessor;
        let array_json = serde_json::to_vec(&[1, 2, 3]).unwrap();

        // Create a message batch with array JSON content
        let msg_batch = MessageBatch::new_binary(vec![array_json]);

        // Process the message batch should fail
        let result = processor.process(msg_batch).await;
        assert!(result.is_err(), "Should return error for non-object JSON");
    }

    #[tokio::test]
    async fn test_json_to_arrow_processor_wrong_content_type() {
        // Test with Arrow content instead of Binary
        let processor = JsonToArrowProcessor;

        // Create a simple Arrow record batch
        let schema = Arc::new(Schema::new(vec![Field::new("test", DataType::Utf8, false)]));
        let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec!["test"]))];
        let record_batch = RecordBatch::try_new(schema, columns).unwrap();

        // Create a message batch with Arrow content
        let msg_batch = MessageBatch::new_arrow(record_batch);

        // Process the message batch should fail
        let result = processor.process(msg_batch).await;
        assert!(result.is_err(), "Should return error for Arrow content");
    }

    #[tokio::test]
    async fn test_arrow_to_json_processor_success() {
        // Test successful conversion from Arrow to JSON
        let processor = ArrowToJsonProcessor;

        // Create a simple Arrow record batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("string_field", DataType::Utf8, false),
            Field::new("int_field", DataType::Int64, false),
            Field::new("bool_field", DataType::Boolean, false),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["test"])),
            Arc::new(Int64Array::from(vec![42])),
            Arc::new(BooleanArray::from(vec![true])),
        ];

        let record_batch = RecordBatch::try_new(schema, columns).unwrap();

        // Create a message batch with Arrow content
        let msg_batch = MessageBatch::new_arrow(record_batch);

        // Process the message batch
        let result = processor.process(msg_batch).await.unwrap();

        // Verify the result
        assert_eq!(result.len(), 1, "Should return one message batch");
        match &result[0].content {
            Content::Binary(v) => {
                assert_eq!(v.len(), 1, "Should have one binary item");

                // Parse the JSON to verify content
                let json_str = String::from_utf8_lossy(&v[0]);
                let json_value: serde_json::Value = serde_json::from_str(&json_str).unwrap();

                // Verify it's a valid JSON array with one object
                assert!(json_value.is_array(), "Should be a JSON array");
                let array = json_value.as_array().unwrap();
                assert_eq!(array.len(), 1, "Should have one object in array");

                let obj = &array[0];
                assert!(obj.is_object(), "Should be a JSON object");
                let obj_map = obj.as_object().unwrap();

                // Verify fields
                assert_eq!(obj_map["string_field"], "test");
                assert_eq!(obj_map["int_field"], 42);
                assert_eq!(obj_map["bool_field"], true);
            }
            _ => panic!("Expected Binary content"),
        }
    }

    #[tokio::test]
    async fn test_arrow_to_json_processor_wrong_content_type() {
        // Test with Binary content instead of Arrow
        let processor = ArrowToJsonProcessor;
        let binary_data = vec![1, 2, 3];

        // Create a message batch with Binary content
        let msg_batch = MessageBatch::new_binary(vec![binary_data]);

        // Process the message batch should fail
        let result = processor.process(msg_batch).await;
        assert!(result.is_err(), "Should return error for Binary content");
    }

    #[tokio::test]
    async fn test_json_to_arrow_function() {
        // Test the json_to_arrow function directly
        let json_data = create_test_json();
        let result = json_to_arrow(&json_data).unwrap();

        // Verify the result
        assert_eq!(result.num_rows(), 1, "Should have one row");
        assert_eq!(result.num_columns(), 8, "Should have 8 columns");

        // Verify specific values
        let schema = result.schema();
        for (i, field) in schema.fields().iter().enumerate() {
            match field.name().as_str() {
                "bool_field" => {
                    let array = result
                        .column(i)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap();
                    assert_eq!(array.value(0), true);
                }
                "int_field" => {
                    let array = result
                        .column(i)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    assert_eq!(array.value(0), 42);
                }
                "string_field" => {
                    let array = result
                        .column(i)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    assert_eq!(array.value(0), "test");
                }
                _ => {}
            }
        }
    }

    #[tokio::test]
    async fn test_arrow_to_json_function() {
        // Test the arrow_to_json function directly
        // Create a simple Arrow record batch
        let schema = Arc::new(Schema::new(vec![Field::new(
            "test_field",
            DataType::Utf8,
            false,
        )]));

        let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec!["test_value"]))];

        let record_batch = RecordBatch::try_new(schema, columns).unwrap();

        // Convert to JSON
        let json_bytes = arrow_to_json(&record_batch).unwrap();

        // Verify the result
        let json_str = String::from_utf8_lossy(&json_bytes);
        let json_value: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        // Verify it's a valid JSON array with one object
        assert!(json_value.is_array(), "Should be a JSON array");
        let array = json_value.as_array().unwrap();
        assert_eq!(array.len(), 1, "Should have one object in array");

        let obj = &array[0];
        assert!(obj.is_object(), "Should be a JSON object");
        let obj_map = obj.as_object().unwrap();

        // Verify field
        assert_eq!(obj_map["test_field"], "test_value");
    }
}
