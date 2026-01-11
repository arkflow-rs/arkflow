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

#[cfg(test)]
mod tests {
    use crate::{Error, MessageBatch, DEFAULT_BINARY_VALUE_FIELD};
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn test_message_batch_new_binary() {
        let data = vec![b"test1".to_vec(), b"test2".to_vec(), b"test3".to_vec()];
        let batch = MessageBatch::new_binary(data.clone()).unwrap();

        assert_eq!(batch.len(), 3);
        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 1);

        let binary_data = batch.to_binary(DEFAULT_BINARY_VALUE_FIELD).unwrap();
        assert_eq!(binary_data.len(), 3);

        for (i, original) in data.iter().enumerate() {
            assert_eq!(binary_data[i], original.as_slice());
        }
    }

    #[test]
    fn test_message_batch_new_binary_with_field_name() {
        let data = vec![b"test".to_vec()];
        let batch = MessageBatch::new_binary_with_field_name(data, Some("custom_field")).unwrap();

        assert_eq!(batch.len(), 1);

        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.fields()[0].name(), "custom_field");
        assert_eq!(schema.fields()[0].data_type(), &DataType::Binary);
    }

    #[test]
    fn test_message_batch_new_arrow() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap();

        let msg_batch = MessageBatch::new_arrow(batch);
        assert_eq!(msg_batch.len(), 3);
    }

    #[test]
    fn test_message_batch_set_input_name() {
        let data = vec![b"test".to_vec()];
        let mut batch = MessageBatch::new_binary(data).unwrap();

        assert!(batch.get_input_name().is_none());

        batch.set_input_name(Some("test_input".to_string()));
        assert_eq!(batch.get_input_name(), Some("test_input".to_string()));

        batch.set_input_name(None);
        assert!(batch.get_input_name().is_none());
    }

    #[test]
    fn test_message_batch_empty() {
        let data = vec![];
        let batch = MessageBatch::new_binary(data).unwrap();

        assert_eq!(batch.len(), 0);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_message_batch_schema() {
        let batch = MessageBatch::new_binary(vec![b"test".to_vec()]).unwrap();
        let schema = batch.schema();

        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.fields()[0].name(), DEFAULT_BINARY_VALUE_FIELD);
    }

    #[test]
    fn test_message_batch_into_record_batch() {
        let data = vec![b"test".to_vec()];
        let batch = MessageBatch::new_binary(data).unwrap();

        let record_batch: RecordBatch = batch.into();
        assert_eq!(record_batch.num_rows(), 1);
        assert_eq!(record_batch.num_columns(), 1);
    }

    #[test]
    fn test_error_display() {
        let err = Error::Config("test config error".to_string());
        assert_eq!(format!("{}", err), "Configuration error: test config error");

        let err = Error::Process("test process error".to_string());
        assert_eq!(format!("{}", err), "Process errors: test process error");

        let err = Error::Connection("test connection error".to_string());
        assert_eq!(format!("{}", err), "Connection error: test connection error");
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }

    #[test]
    fn test_error_from_serialization() {
        let ser_err = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
        let err: Error = Error::Serialization(ser_err);
        assert!(matches!(err, Error::Serialization(_)));
        assert!(err.to_string().contains("Serialization error"));
    }

    #[test]
    fn test_error_eof() {
        let err = Error::EOF;
        assert_eq!(format!("{}", err), "EOF");
    }

    #[test]
    fn test_error_timeout() {
        let err = Error::Timeout;
        assert_eq!(format!("{}", err), "Timeout error");
    }

    #[test]
    fn test_error_disconnection() {
        let err = Error::Disconnection;
        assert_eq!(format!("{}", err), "Connection lost");
    }

    #[test]
    fn test_error_unknown() {
        let err = Error::Unknown("something happened".to_string());
        assert_eq!(format!("{}", err), "Unknown error: something happened");
    }

    #[test]
    fn test_message_batch_clone() {
        let data = vec![b"test1".to_vec(), b"test2".to_vec()];
        let batch = MessageBatch::new_binary(data).unwrap();

        let batch_clone = batch.clone();
        assert_eq!(batch.len(), batch_clone.len());
        assert_eq!(batch.schema(), batch_clone.schema());
    }

    #[test]
    fn test_message_batch_to_binary_field_not_found() {
        let schema = Arc::new(Schema::new(vec![Field::new("other_field", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["test"]))],
        )
        .unwrap();

        let msg_batch = MessageBatch::new_arrow(batch);
        let result = msg_batch.to_binary("non_existent_field");
        assert!(result.is_err());
    }

    #[test]
    fn test_message_batch_to_binary_with_custom_field() {
        let schema = Arc::new(Schema::new(vec![Field::new("custom_data", DataType::Binary, false)]));
        let array = datafusion::arrow::array::BinaryArray::from_vec(vec![
            b"data1".as_ref(),
            b"data2".as_ref(),
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

        let msg_batch = MessageBatch::new_arrow(batch);
        let binary_data = msg_batch.to_binary("custom_data").unwrap();

        assert_eq!(binary_data.len(), 2);
        assert_eq!(binary_data[0], b"data1");
        assert_eq!(binary_data[1], b"data2");
    }
}
