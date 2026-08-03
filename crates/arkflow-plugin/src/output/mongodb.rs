/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 */

//! MongoDB output component.

use arkflow_core::codec::Codec;
use arkflow_core::component::{register_output_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, Resource};
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::DataType;
use mongodb::bson::{Binary, Bson, Document};
use mongodb::{Client, Collection};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

fn map_mongodb_error(context: &str, error: &mongodb::error::Error) -> Error {
    use mongodb::error::ErrorKind;

    let message = format!("{}: {}", context, error);
    match error.kind.as_ref() {
        ErrorKind::Authentication { .. } => Error::Authentication(message),
        ErrorKind::ConnectionPoolCleared { .. }
        | ErrorKind::DnsResolve { .. }
        | ErrorKind::Io(_)
        | ErrorKind::InvalidTlsConfig { .. }
        | ErrorKind::ServerSelection { .. }
        | ErrorKind::Shutdown => Error::Connection(message),
        _ => Error::Process(message),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MongoDBOutputConfig {
    pub uri: String,
    pub database: String,
    pub collection: String,
}

impl MongoDBOutputConfig {
    fn validate(&self) -> Result<(), Error> {
        for (name, value) in [
            ("uri", self.uri.as_str()),
            ("database", self.database.as_str()),
            ("collection", self.collection.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(Error::Config(format!(
                    "MongoDB output field '{}' must not be empty",
                    name
                )));
            }
        }
        if !self.uri.starts_with("mongodb://") && !self.uri.starts_with("mongodb+srv://") {
            return Err(Error::Config(
                "MongoDB output uri must start with mongodb:// or mongodb+srv://".to_string(),
            ));
        }
        Ok(())
    }
}

pub struct MongoDBOutput {
    config: MongoDBOutputConfig,
    collection: Arc<Mutex<Option<Collection<Document>>>>,
}

impl MongoDBOutput {
    pub fn new(config: MongoDBOutputConfig) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self {
            config,
            collection: Arc::new(Mutex::new(None)),
        })
    }

    fn row_to_document(msg: &MessageBatch, row: usize) -> Result<Document, Error> {
        let mut document = Document::new();
        for (column_index, field) in msg.schema().fields().iter().enumerate() {
            let column = msg.column(column_index);
            let value = array_value_to_bson(column.as_ref(), row, field.name())?;
            document.insert(field.name().clone(), value);
        }
        Ok(document)
    }

    fn documents(msg: &MessageBatch) -> Result<Vec<Document>, Error> {
        (0..msg.len())
            .map(|row| Self::row_to_document(msg, row))
            .collect()
    }
}

#[async_trait]
impl Output for MongoDBOutput {
    async fn connect(&self) -> Result<(), Error> {
        let client = Client::with_uri_str(&self.config.uri)
            .await
            .map_err(|e| Error::Config(format!("Invalid MongoDB URI: {}", e)))?;
        client
            .database(&self.config.database)
            .run_command(mongodb::bson::doc! { "ping": 1 })
            .await
            .map_err(|e| map_mongodb_error("Failed to connect to MongoDB", &e))?;

        let mut collection = self.collection.lock().await;
        *collection = Some(
            client
                .database(&self.config.database)
                .collection::<Document>(&self.config.collection),
        );
        Ok(())
    }

    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
        let collection = self.collection.lock().await;
        let collection = collection.as_ref().ok_or(Error::Disconnection)?;
        let documents = Self::documents(&msg)?;
        if documents.is_empty() {
            return Ok(());
        }

        collection
            .insert_many(documents)
            .await
            .map_err(|e| map_mongodb_error("Failed to insert documents into MongoDB", &e))?;
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        *self.collection.lock().await = None;
        Ok(())
    }
}

fn array_value_to_bson(array: &dyn Array, row: usize, field: &str) -> Result<Bson, Error> {
    if array.is_null(row) {
        return Ok(Bson::Null);
    }

    let value = match array.data_type() {
        DataType::Utf8 => Bson::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Arrow Utf8 array type is StringArray")
                .value(row)
                .to_owned(),
        ),
        DataType::LargeUtf8 => Bson::String(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("Arrow LargeUtf8 array type is LargeStringArray")
                .value(row)
                .to_owned(),
        ),
        DataType::Int8 => Bson::Int32(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("Arrow Int8 array type is Int8Array")
                .value(row) as i32,
        ),
        DataType::Int16 => Bson::Int32(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .expect("Arrow Int16 array type is Int16Array")
                .value(row) as i32,
        ),
        DataType::Int32 => Bson::Int32(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Arrow Int32 array type is Int32Array")
                .value(row),
        ),
        DataType::Int64 => Bson::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Arrow Int64 array type is Int64Array")
                .value(row),
        ),
        DataType::UInt8 => Bson::Int32(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .expect("Arrow UInt8 array type is UInt8Array")
                .value(row) as i32,
        ),
        DataType::UInt16 => Bson::Int32(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .expect("Arrow UInt16 array type is UInt16Array")
                .value(row) as i32,
        ),
        DataType::UInt32 => Bson::Int64(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("Arrow UInt32 array type is UInt32Array")
                .value(row) as i64,
        ),
        DataType::UInt64 => {
            let value = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Arrow UInt64 array type is UInt64Array")
                .value(row);
            Bson::Int64(value.try_into().map_err(|_| {
                Error::Process(format!(
                    "MongoDB field '{}' exceeds BSON int64 range",
                    field
                ))
            })?)
        }
        DataType::Float32 => Bson::Double(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("Arrow Float32 array type is Float32Array")
                .value(row) as f64,
        ),
        DataType::Float64 => Bson::Double(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Arrow Float64 array type is Float64Array")
                .value(row),
        ),
        DataType::Boolean => Bson::Boolean(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("Arrow Boolean array type is BooleanArray")
                .value(row),
        ),
        DataType::Binary => Bson::Binary(Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("Arrow Binary array type is BinaryArray")
                .value(row)
                .to_vec(),
        }),
        DataType::LargeBinary => Bson::Binary(Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .expect("Arrow LargeBinary array type is LargeBinaryArray")
                .value(row)
                .to_vec(),
        }),
        data_type => {
            return Err(Error::Process(format!(
                "MongoDB field '{}' has unsupported Arrow type {:?}",
                field, data_type
            )))
        }
    };
    Ok(value)
}

struct MongoDBOutputBuilder;

impl OutputBuilder for MongoDBOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        if codec.is_some() {
            return Err(Error::Config(
                "MongoDB output does not support codecs; it writes structured BSON documents"
                    .to_string(),
            ));
        }
        let config: MongoDBOutputConfig = parse_config(config, "MongoDB output")?;
        Ok(Arc::new(MongoDBOutput::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("mongodb", Arc::new(MongoDBOutputBuilder))?;
    register_output_metadata(
        ComponentMetadata::with_schema(
            "mongodb",
            "Writes Arrow rows to MongoDB as BSON documents.",
            serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "uri": {"type": "string", "description": "MongoDB connection URI."},
                    "database": {"type": "string", "description": "Target database name."},
                    "collection": {"type": "string", "description": "Target collection name."}
                },
                "required": ["uri", "database", "collection"]
            }),
        )
        .with_example(serde_json::json!({
            "uri": "mongodb://localhost:27017",
            "database": "arkflow",
            "collection": "events"
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        ArrayRef, Int64Array, StringArray, TimestampMillisecondArray, UInt64Array,
    };
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    fn batch(columns: Vec<(&str, ArrayRef)>) -> MessageBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, column)| Field::new(*name, column.data_type().clone(), true))
            .collect();
        MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(fields)),
                columns.into_iter().map(|(_, c)| c).collect(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn converts_scalars_and_nulls() {
        let msg = batch(vec![
            ("name", Arc::new(StringArray::from(vec![Some("Ada")]))),
            ("count", Arc::new(Int64Array::from(vec![Some(3)]))),
            ("missing", Arc::new(StringArray::from(vec![None::<&str>]))),
        ]);
        let document = MongoDBOutput::row_to_document(&msg, 0).unwrap();
        assert_eq!(document.get_str("name").unwrap(), "Ada");
        assert_eq!(document.get_i64("count").unwrap(), 3);
        assert!(matches!(document.get("missing"), Some(Bson::Null)));
    }

    #[test]
    fn rejects_uint64_overflow() {
        let msg = batch(vec![("value", Arc::new(UInt64Array::from(vec![u64::MAX])))]);
        let error = MongoDBOutput::row_to_document(&msg, 0).unwrap_err();
        assert!(error.to_string().contains("value"));
        assert!(error.to_string().contains("int64"));
    }

    #[test]
    fn rejects_unsupported_arrow_type() {
        let msg = batch(vec![(
            "event_time",
            Arc::new(TimestampMillisecondArray::from(vec![Some(1)])),
        )]);
        let error = MongoDBOutput::row_to_document(&msg, 0).unwrap_err();
        assert!(error.to_string().contains("event_time"));
        assert!(error.to_string().contains("unsupported Arrow type"));
    }

    #[test]
    fn validates_non_empty_configuration() {
        let result = MongoDBOutput::new(MongoDBOutputConfig {
            uri: " ".to_string(),
            database: "db".to_string(),
            collection: "events".to_string(),
        });
        let error = match result {
            Ok(_) => panic!("empty URI should be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("uri"));
    }

    #[test]
    fn empty_batch_has_no_documents() {
        let msg = batch(vec![(
            "value",
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        )]);
        assert!(MongoDBOutput::documents(&msg).unwrap().is_empty());
    }
}
