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

//! Filter Processor Component
//!
//! Filters messages based on field conditions

use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, ProcessResult, Resource};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, BooleanArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Filter operator
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum FilterOperator {
    /// Equals
    Eq,
    /// Not equals
    Ne,
    /// Greater than
    Gt,
    /// Greater than or equal
    Gte,
    /// Less than
    Lt,
    /// Less than or equal
    Lte,
    /// Contains (for strings)
    Contains,
    /// Starts with (for strings)
    StartsWith,
    /// Ends with (for strings)
    EndsWith,
    /// Is null
    IsNull,
    /// Is not null
    IsNotNull,
}

/// Filter condition
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FilterCondition {
    /// Field name to filter on
    field: String,
    /// Operator to apply
    operator: FilterOperator,
    /// Value to compare with (optional for IsNull/IsNotNull)
    value: Option<serde_json::Value>,
}

/// Filter processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FilterProcessorConfig {
    /// Filter conditions (AND logic - all must match)
    #[serde(default)]
    conditions: Vec<FilterCondition>,
    /// Invert the filter result (NOT logic)
    #[serde(default)]
    invert: bool,
}

/// Filter processor
pub struct FilterProcessor {
    config: FilterProcessorConfig,
}

impl FilterProcessor {
    /// Create a new filter processor
    fn new(config: FilterProcessorConfig) -> Result<Self, Error> {
        if config.conditions.is_empty() {
            return Err(Error::Config(
                "Filter processor requires at least one condition".to_string(),
            ));
        }
        Ok(Self { config })
    }

    /// Evaluate a single condition on a batch
    fn evaluate_condition(
        &self,
        batch: &RecordBatch,
        condition: &FilterCondition,
    ) -> Result<BooleanArray, Error> {
        let schema = batch.schema();

        // Get the column index
        let column_index = schema
            .column_with_name(&condition.field)
            .ok_or_else(|| {
                Error::Process(format!("Field '{}' not found in schema", condition.field))
            })?
            .0;

        let column = batch.column(column_index);

        match &condition.operator {
            FilterOperator::Eq => self.evaluate_eq(column, &condition.value),
            FilterOperator::Ne => self.evaluate_ne(column, &condition.value),
            FilterOperator::Gt => self.evaluate_gt(column, &condition.value),
            FilterOperator::Gte => self.evaluate_gte(column, &condition.value),
            FilterOperator::Lt => self.evaluate_lt(column, &condition.value),
            FilterOperator::Lte => self.evaluate_lte(column, &condition.value),
            FilterOperator::Contains => self.evaluate_contains(column, &condition.value),
            FilterOperator::StartsWith => self.evaluate_starts_with(column, &condition.value),
            FilterOperator::EndsWith => self.evaluate_ends_with(column, &condition.value),
            FilterOperator::IsNull => self.evaluate_is_null(column, &condition.value),
            FilterOperator::IsNotNull => self.evaluate_is_not_null(column, &condition.value),
        }
    }

    fn evaluate_eq(
        &self,
        column: &Arc<dyn Array>,
        value: &Option<serde_json::Value>,
    ) -> Result<BooleanArray, Error> {
        let value = value
            .as_ref()
            .ok_or_else(|| Error::Config("Eq operator requires a value".to_string()))?;

        match column.data_type() {
            DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                let target = value.as_str().ok_or_else(|| {
                    Error::Config("String value expected for Utf8 column".to_string())
                })?;
                Ok(array.iter().map(|v| v.map(|s| s == target)).collect())
            }
            DataType::Int64 => {
                let array = datafusion::arrow::array::Int64Array::from(column.to_data());
                let target = value.as_i64().ok_or_else(|| {
                    Error::Config("Integer value expected for Int64 column".to_string())
                })?;
                Ok(array.iter().map(|v| v.map(|i| i == target)).collect())
            }
            DataType::Float64 => {
                let array = datafusion::arrow::array::Float64Array::from(column.to_data());
                let target = value.as_f64().ok_or_else(|| {
                    Error::Config("Float value expected for Float64 column".to_string())
                })?;
                Ok(array
                    .iter()
                    .map(|v| v.map(|f| (f - target).abs() < 1e-9))
                    .collect())
            }
            DataType::Boolean => {
                let array = datafusion::arrow::array::BooleanArray::from(column.to_data());
                let target = value.as_bool().ok_or_else(|| {
                    Error::Config("Boolean value expected for Boolean column".to_string())
                })?;
                Ok(array.iter().map(|v| v.map(|b| b == target)).collect())
            }
            _ => Err(Error::Process(format!(
                "Unsupported data type for Eq operator: {:?}",
                column.data_type()
            ))),
        }
    }

    fn evaluate_ne(
        &self,
        column: &Arc<dyn Array>,
        value: &Option<serde_json::Value>,
    ) -> Result<BooleanArray, Error> {
        let eq_result = self.evaluate_eq(column, value)?;
        Ok(eq_result.iter().map(|b| b.map(|v| !v)).collect())
    }

    fn evaluate_gt(
        &self,
        column: &Arc<dyn Array>,
        value: &Option<serde_json::Value>,
    ) -> Result<BooleanArray, Error> {
        let value = value
            .as_ref()
            .ok_or_else(|| Error::Config("Gt operator requires a value".to_string()))?;

        match column.data_type() {
            DataType::Int64 => {
                let array = datafusion::arrow::array::Int64Array::from(column.to_data());
                let target = value.as_i64().ok_or_else(|| {
                    Error::Config("Integer value expected for Int64 column".to_string())
                })?;
                Ok(array.iter().map(|v| v.map(|i| i > target)).collect())
            }
            DataType::Float64 => {
                let array = datafusion::arrow::array::Float64Array::from(column.to_data());
                let target = value.as_f64().ok_or_else(|| {
                    Error::Config("Float value expected for Float64 column".to_string())
                })?;
                Ok(array.iter().map(|v| v.map(|f| f > target)).collect())
            }
            _ => Err(Error::Process(format!(
                "Unsupported data type for Gt operator: {:?}",
                column.data_type()
            ))),
        }
    }

    fn evaluate_gte(
        &self,
        column: &Arc<dyn Array>,
        value: &Option<serde_json::Value>,
    ) -> Result<BooleanArray, Error> {
        let value = value
            .as_ref()
            .ok_or_else(|| Error::Config("Gte operator requires a value".to_string()))?;

        match column.data_type() {
            DataType::Int64 => {
                let array = datafusion::arrow::array::Int64Array::from(column.to_data());
                let target = value.as_i64().ok_or_else(|| {
                    Error::Config("Integer value expected for Int64 column".to_string())
                })?;
                Ok(array.iter().map(|v| v.map(|i| i >= target)).collect())
            }
            DataType::Float64 => {
                let array = datafusion::arrow::array::Float64Array::from(column.to_data());
                let target = value.as_f64().ok_or_else(|| {
                    Error::Config("Float value expected for Float64 column".to_string())
                })?;
                Ok(array.iter().map(|v| v.map(|f| f >= target)).collect())
            }
            _ => Err(Error::Process(format!(
                "Unsupported data type for Gte operator: {:?}",
                column.data_type()
            ))),
        }
    }

    fn evaluate_lt(
        &self,
        column: &Arc<dyn Array>,
        value: &Option<serde_json::Value>,
    ) -> Result<BooleanArray, Error> {
        let value = value
            .as_ref()
            .ok_or_else(|| Error::Config("Lt operator requires a value".to_string()))?;

        match column.data_type() {
            DataType::Int64 => {
                let array = datafusion::arrow::array::Int64Array::from(column.to_data());
                let target = value.as_i64().ok_or_else(|| {
                    Error::Config("Integer value expected for Int64 column".to_string())
                })?;
                Ok(array.iter().map(|v| v.map(|i| i < target)).collect())
            }
            DataType::Float64 => {
                let array = datafusion::arrow::array::Float64Array::from(column.to_data());
                let target = value.as_f64().ok_or_else(|| {
                    Error::Config("Float value expected for Float64 column".to_string())
                })?;
                Ok(array.iter().map(|v| v.map(|f| f < target)).collect())
            }
            _ => Err(Error::Process(format!(
                "Unsupported data type for Lt operator: {:?}",
                column.data_type()
            ))),
        }
    }

    fn evaluate_lte(
        &self,
        column: &Arc<dyn Array>,
        value: &Option<serde_json::Value>,
    ) -> Result<BooleanArray, Error> {
        let value = value
            .as_ref()
            .ok_or_else(|| Error::Config("Lte operator requires a value".to_string()))?;

        match column.data_type() {
            DataType::Int64 => {
                let array = datafusion::arrow::array::Int64Array::from(column.to_data());
                let target = value.as_i64().ok_or_else(|| {
                    Error::Config("Integer value expected for Int64 column".to_string())
                })?;
                Ok(array.iter().map(|v| v.map(|i| i <= target)).collect())
            }
            DataType::Float64 => {
                let array = datafusion::arrow::array::Float64Array::from(column.to_data());
                let target = value.as_f64().ok_or_else(|| {
                    Error::Config("Float value expected for Float64 column".to_string())
                })?;
                Ok(array.iter().map(|v| v.map(|f| f <= target)).collect())
            }
            _ => Err(Error::Process(format!(
                "Unsupported data type for Lte operator: {:?}",
                column.data_type()
            ))),
        }
    }

    fn evaluate_contains(
        &self,
        column: &Arc<dyn Array>,
        value: &Option<serde_json::Value>,
    ) -> Result<BooleanArray, Error> {
        let value = value
            .as_ref()
            .ok_or_else(|| Error::Config("Contains operator requires a value".to_string()))?;

        match column.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                let target = value.as_str().ok_or_else(|| {
                    Error::Config("String value expected for Contains operator".to_string())
                })?;
                Ok(array
                    .iter()
                    .map(|v| v.map(|s| s.contains(target)))
                    .collect())
            }
            _ => Err(Error::Process(format!(
                "Unsupported data type for Contains operator: {:?}",
                column.data_type()
            ))),
        }
    }

    fn evaluate_starts_with(
        &self,
        column: &Arc<dyn Array>,
        value: &Option<serde_json::Value>,
    ) -> Result<BooleanArray, Error> {
        let value = value
            .as_ref()
            .ok_or_else(|| Error::Config("StartsWith operator requires a value".to_string()))?;

        match column.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                let target = value.as_str().ok_or_else(|| {
                    Error::Config("String value expected for StartsWith operator".to_string())
                })?;
                Ok(array
                    .iter()
                    .map(|v| v.map(|s| s.starts_with(target)))
                    .collect())
            }
            _ => Err(Error::Process(format!(
                "Unsupported data type for StartsWith operator: {:?}",
                column.data_type()
            ))),
        }
    }

    fn evaluate_ends_with(
        &self,
        column: &Arc<dyn Array>,
        value: &Option<serde_json::Value>,
    ) -> Result<BooleanArray, Error> {
        let value = value
            .as_ref()
            .ok_or_else(|| Error::Config("EndsWith operator requires a value".to_string()))?;

        match column.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                let target = value.as_str().ok_or_else(|| {
                    Error::Config("String value expected for EndsWith operator".to_string())
                })?;
                Ok(array
                    .iter()
                    .map(|v| v.map(|s| s.ends_with(target)))
                    .collect())
            }
            _ => Err(Error::Process(format!(
                "Unsupported data type for EndsWith operator: {:?}",
                column.data_type()
            ))),
        }
    }

    fn evaluate_is_null(
        &self,
        column: &Arc<dyn Array>,
        _value: &Option<serde_json::Value>,
    ) -> Result<BooleanArray, Error> {
        let num_rows = column.len();
        let mut values = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            values.push(column.is_null(i));
        }
        Ok(BooleanArray::from(values))
    }

    fn evaluate_is_not_null(
        &self,
        column: &Arc<dyn Array>,
        _value: &Option<serde_json::Value>,
    ) -> Result<BooleanArray, Error> {
        let num_rows = column.len();
        let mut values = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            values.push(column.is_valid(i));
        }
        Ok(BooleanArray::from(values))
    }

    /// Apply all conditions (AND logic)
    fn apply_filter(&self, batch: &RecordBatch) -> Result<Vec<usize>, Error> {
        let num_rows = batch.num_rows();
        let mut mask = vec![true; num_rows];

        for condition in &self.config.conditions {
            let condition_result = self.evaluate_condition(batch, condition)?;
            for (i, result) in condition_result.iter().enumerate() {
                if let Some(true) = result {
                    // Condition passed, keep mask as is
                } else {
                    // Condition failed, mark as false
                    mask[i] = false;
                }
            }
        }

        // Apply invert if configured
        if self.config.invert {
            mask.iter_mut().for_each(|m| *m = !*m);
        }

        // Collect indices of rows that passed the filter
        let indices: Vec<usize> = mask
            .iter()
            .enumerate()
            .filter_map(|(i, &passed)| if passed { Some(i) } else { None })
            .collect();

        Ok(indices)
    }
}

#[async_trait]
impl Processor for FilterProcessor {
    async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        let batch_ref = batch.as_ref();

        let indices = self.apply_filter(batch_ref)?;

        if indices.is_empty() {
            // All rows filtered out
            return Ok(ProcessResult::None);
        }

        // Filter the batch by collecting matching rows
        let filtered_batch = batch_ref.slice(
            indices[0],
            (indices[indices.len() - 1] - indices[0] + 1) as usize,
        );

        Ok(ProcessResult::Single(Arc::new(MessageBatch::new_arrow(
            filtered_batch,
        ))))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// Filter processor builder
pub struct FilterProcessorBuilder;

#[async_trait]
impl ProcessorBuilder for FilterProcessorBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        let config_json = config.as_ref().ok_or_else(|| {
            Error::Config("Filter processor configuration is missing".to_string())
        })?;

        let processor_config: FilterProcessorConfig =
            serde_json::from_value(config_json.clone())
                .map_err(|e| Error::Config(format!("Invalid filter processor config: {}", e)))?;

        let processor = FilterProcessor::new(processor_config)?;
        Ok(Arc::new(processor))
    }
}

/// Initialize the filter processor
pub fn init() -> Result<(), Error> {
    register_processor_builder("filter", Arc::new(FilterProcessorBuilder))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{Field, Schema};

    #[test]
    fn test_evaluate_eq_string() {
        let schema = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);
        let array = StringArray::from(vec!["Alice", "Bob", "Charlie", "Alice"]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        let config = FilterProcessorConfig {
            conditions: vec![FilterCondition {
                field: "name".to_string(),
                operator: FilterOperator::Eq,
                value: Some(serde_json::json!("Alice")),
            }],
            invert: false,
        };

        let processor = FilterProcessor::new(config).unwrap();
        let indices = processor.apply_filter(&batch).unwrap();
        assert_eq!(indices, vec![0, 3]);
    }

    #[test]
    fn test_evaluate_gt_int() {
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let array = Int64Array::from(vec![10, 20, 30, 40]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        let config = FilterProcessorConfig {
            conditions: vec![FilterCondition {
                field: "value".to_string(),
                operator: FilterOperator::Gt,
                value: Some(serde_json::json!(25)),
            }],
            invert: false,
        };

        let processor = FilterProcessor::new(config).unwrap();
        let indices = processor.apply_filter(&batch).unwrap();
        assert_eq!(indices, vec![2, 3]);
    }

    #[test]
    fn test_evaluate_contains() {
        let schema = Schema::new(vec![Field::new("message", DataType::Utf8, false)]);
        let array = StringArray::from(vec!["error: timeout", "warning: retry", "error: failed"]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        let config = FilterProcessorConfig {
            conditions: vec![FilterCondition {
                field: "message".to_string(),
                operator: FilterOperator::Contains,
                value: Some(serde_json::json!("error")),
            }],
            invert: false,
        };

        let processor = FilterProcessor::new(config).unwrap();
        let indices = processor.apply_filter(&batch).unwrap();
        assert_eq!(indices, vec![0, 2]);
    }

    #[test]
    fn test_invert() {
        let schema = Schema::new(vec![Field::new("status", DataType::Utf8, false)]);
        let array = StringArray::from(vec!["active", "inactive", "active", "pending"]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        let config = FilterProcessorConfig {
            conditions: vec![FilterCondition {
                field: "status".to_string(),
                operator: FilterOperator::Eq,
                value: Some(serde_json::json!("active")),
            }],
            invert: true,
        };

        let processor = FilterProcessor::new(config).unwrap();
        let indices = processor.apply_filter(&batch).unwrap();
        assert_eq!(indices, vec![1, 3]);
    }
}
