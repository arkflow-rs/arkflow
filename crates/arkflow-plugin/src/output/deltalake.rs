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

//! Delta Lake output component
//!
//! This component writes data to Delta Lake tables stored in object storage.

use std::collections::HashMap;
use std::sync::Arc;

use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatch, Resource};
use async_trait::async_trait;
use deltalake::kernel::Schema;
use deltalake::{
    arrow::record_batch::RecordBatch, operations::create::CreateBuilder, DeltaOps, DeltaTable,
    DeltaTableBuilder,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, info};

/// Delta Lake output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltaLakeOutputConfig {
    /// Delta table URI (e.g., s3://bucket/path/to/table, azure://container/path, gs://bucket/path)
    table_uri: String,
    /// Write mode: append, overwrite, error, ignore
    #[serde(default = "default_write_mode")]
    write_mode: String,
    /// Storage options for authentication and configuration
    #[serde(default)]
    storage_options: HashMap<String, String>,
    /// Whether to create table if it doesn't exist
    #[serde(default = "default_create_if_not_exists")]
    create_if_not_exists: bool,
    /// Schema for table creation (optional, inferred from data if not provided)
    schema: Option<String>,
    /// Partition columns (optional)
    #[serde(default)]
    partition_columns: Vec<String>,
}

fn default_write_mode() -> String {
    "append".to_string()
}

fn default_create_if_not_exists() -> bool {
    true
}

/// Delta Lake output component
struct DeltaLakeOutput {
    config: DeltaLakeOutputConfig,
    table: Arc<Mutex<Option<DeltaTable>>>,
}

impl DeltaLakeOutput {
    /// Create a new Delta Lake output component
    pub fn new(config: DeltaLakeOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config,
            table: Arc::new(Mutex::new(None)),
        })
    }

    /// Get or create the Delta table
    async fn get_or_create_table(&self) -> Result<DeltaTable, Error> {
        let mut table_guard = self.table.lock().await;

        if let Some(table) = table_guard.as_ref() {
            return Ok(table.clone());
        }

        // Try to load existing table
        let mut builder = DeltaTableBuilder::from_uri(&self.config.table_uri);

        // Add storage options
        builder = builder.with_storage_options(self.config.storage_options.clone());

        match builder.load().await {
            Ok(table) => {
                info!("Loaded existing Delta table from {}", self.config.table_uri);
                table_guard.replace(table.clone());
                Ok(table)
            }
            Err(_) if self.config.create_if_not_exists => {
                info!("Creating new Delta table at {}", self.config.table_uri);
                self.create_table().await
            }
            Err(e) => Err(Error::Connection(format!(
                "Failed to load Delta table and create_if_not_exists is false: {}",
                e
            ))),
        }
    }

    /// Create a new Delta table
    async fn create_table(&self) -> Result<DeltaTable, Error> {
        let mut create_builder = CreateBuilder::new()
            .with_location(&self.config.table_uri)
            .with_storage_options(self.config.storage_options.clone());

        // Add partition columns if specified
        if !self.config.partition_columns.is_empty() {
            create_builder =
                create_builder.with_partition_columns(self.config.partition_columns.clone());
        }

        // If schema is provided, parse and use it
        if let Some(schema_str) = &self.config.schema {
            let schema = self.parse_schema(schema_str)?;
            let fields: Vec<_> = schema.fields().cloned().collect();
            create_builder = create_builder.with_columns(fields);
        }

        let table = create_builder
            .await
            .map_err(|e| Error::Connection(format!("Failed to create Delta table: {}", e)))?;

        let mut table_guard = self.table.lock().await;
        table_guard.replace(table.clone());

        Ok(table)
    }

    /// Validate write mode string
    fn validate_write_mode(&self) -> Result<(), Error> {
        match self.config.write_mode.to_lowercase().as_str() {
            "append" | "overwrite" | "error" | "ignore" => Ok(()),
            _ => Err(Error::Config(format!(
                "Invalid write mode: {}. Supported modes: append, overwrite, error, ignore",
                self.config.write_mode
            ))),
        }
    }

    /// Parse schema from JSON string
    fn parse_schema(&self, schema_str: &str) -> Result<Schema, Error> {
        serde_json::from_str(schema_str)
            .map_err(|e| Error::Config(format!("Invalid schema JSON: {}", e)))
    }
}

#[async_trait]
impl Output for DeltaLakeOutput {
    async fn connect(&self) -> Result<(), Error> {
        // Initialize the table connection
        self.get_or_create_table().await?;
        info!("Connected to Delta table at {}", self.config.table_uri);
        Ok(())
    }

    async fn write(&self, msg: MessageBatch) -> Result<(), Error> {
        let table = self.get_or_create_table().await?;
        self.validate_write_mode()?;

        // Convert MessageBatch to RecordBatch
        let record_batch: RecordBatch = msg.into();

        debug!("Writing {} rows to Delta table", record_batch.num_rows());

        // Use DeltaOps for writing
        let ops = DeltaOps::from(table);

        let result = match self.config.write_mode.to_lowercase().as_str() {
            "append" => ops
                .write([record_batch])
                .await
                .map_err(|e| Error::Process(format!("Failed to append to Delta table: {}", e)))?,
            "overwrite" => ops
                .write([record_batch])
                .with_save_mode(deltalake::protocol::SaveMode::Overwrite)
                .await
                .map_err(|e| Error::Process(format!("Failed to overwrite Delta table: {}", e)))?,
            _ => {
                return Err(Error::Config(format!(
                    "Write mode '{}' not implemented yet",
                    self.config.write_mode
                )));
            }
        };

        debug!(
            "Successfully wrote data to Delta table, version: {}",
            result.version()
        );

        // Update the table reference with the new version
        let mut table_guard = self.table.lock().await;
        table_guard.replace(result);

        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        info!("Closing Delta Lake output connection");
        let mut table_guard = self.table.lock().await;
        table_guard.take();
        Ok(())
    }
}

/// Delta Lake output builder
pub(crate) struct DeltaLakeOutputBuilder;

impl OutputBuilder for DeltaLakeOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Delta Lake output configuration is missing".to_string(),
            ));
        }

        // Parse the configuration
        let config: DeltaLakeOutputConfig = serde_json::from_value(config.clone().unwrap())
            .map_err(|e| Error::Config(format!("Invalid Delta Lake configuration: {}", e)))?;

        // Validate required fields
        if config.table_uri.is_empty() {
            return Err(Error::Config("table_uri is required".to_string()));
        }

        Ok(Arc::new(DeltaLakeOutput::new(config)?))
    }
}

/// Initialize the Delta Lake output component
pub fn init() -> Result<(), Error> {
    register_output_builder("deltalake", Arc::new(DeltaLakeOutputBuilder))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_config_parsing() {
        let config_json = json!({
            "table_uri": "s3://my-bucket/my-table",
            "write_mode": "append",
            "storage_options": {
                "AWS_ACCESS_KEY_ID": "test",
                "AWS_SECRET_ACCESS_KEY": "test"
            },
            "create_if_not_exists": true,
            "partition_columns": ["year", "month"]
        });

        let config: DeltaLakeOutputConfig = serde_json::from_value(config_json).unwrap();
        assert_eq!(config.table_uri, "s3://my-bucket/my-table");
        assert_eq!(config.write_mode, "append");
        assert_eq!(config.storage_options.len(), 2);
        assert!(config.create_if_not_exists);
        assert_eq!(config.partition_columns, vec!["year", "month"]);
    }

    #[test]
    fn test_write_mode_validation() {
        let config = DeltaLakeOutputConfig {
            table_uri: "test".to_string(),
            write_mode: "append".to_string(),
            storage_options: HashMap::new(),
            create_if_not_exists: true,
            schema: None,
            partition_columns: vec![],
        };

        let output = DeltaLakeOutput::new(config).unwrap();
        assert!(output.validate_write_mode().is_ok());

        let invalid_config = DeltaLakeOutputConfig {
            table_uri: "test".to_string(),
            write_mode: "invalid".to_string(),
            storage_options: HashMap::new(),
            create_if_not_exists: true,
            schema: None,
            partition_columns: vec![],
        };

        let invalid_output = DeltaLakeOutput::new(invalid_config).unwrap();
        assert!(invalid_output.validate_write_mode().is_err());
    }
}
