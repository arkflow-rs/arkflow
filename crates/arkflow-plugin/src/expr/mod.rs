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
use arkflow_core::Error;
use datafusion::arrow::array::{Array, RecordBatch, StringArray};
use datafusion::common::{DFSchema, DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::prelude::*;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::RwLock;

static EXPR_CACHE: Lazy<RwLock<HashMap<String, Arc<dyn PhysicalExpr>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Expr<T> {
    Expr { expr: String },
    Value { value: T },
}

pub enum EvaluateResult<T> {
    Scalar(T),
    Vec(Vec<T>),
}

impl<T> EvaluateResult<T> {
    pub fn get(&self, i: usize) -> Option<&T> {
        match self {
            EvaluateResult::Scalar(val) => Some(val),
            EvaluateResult::Vec(vec) => vec.get(i),
        }
    }
}

impl Expr<String> {
    pub async fn evaluate_expr(
        &self,
        batch: &RecordBatch,
    ) -> Result<EvaluateResult<String>, Error> {
        match self {
            Expr::Expr { expr } => {
                let result = evaluate_expr(expr, batch)
                    .await
                    .map_err(|e| Error::Process(format!("Failed to evaluate expression: {}", e)))?;

                match result {
                    ColumnarValue::Array(v) => {
                        let v_option = v.as_any().downcast_ref::<StringArray>();
                        if let Some(v) = v_option {
                            // Row alignment invariant: the result must carry
                            // one cell per batch row, so consumers can index
                            // by row. A null cell has no destination meaning
                            // (topic/key/subject) — dropping it would shift
                            // every later row onto the wrong destination, so
                            // it is a loud error instead.
                            let mut x: Vec<String> = Vec::with_capacity(v.len());
                            for (row, cell) in v.iter().enumerate() {
                                match cell {
                                    Some(s) => x.push(s.to_string()),
                                    None => {
                                        return Err(Error::Process(format!(
                                            "Expression `{expr}` evaluated to NULL at row {row}; \
                                             per-row destinations (topic/key/subject) must be \
                                             non-null — wrap the expression in COALESCE or \
                                             filter the rows upstream",
                                        )))
                                    }
                                }
                            }
                            Ok(EvaluateResult::Vec(x))
                        } else {
                            Err(Error::Process("Failed to evaluate expression".to_string()))
                        }
                    }
                    ColumnarValue::Scalar(v) => match v {
                        ScalarValue::Utf8(Some(s)) => Ok(EvaluateResult::Scalar(s.clone())),
                        ScalarValue::Utf8(None) => {
                            Err(Error::Process("Null string value".to_string()))
                        }
                        _ => Err(Error::Process(format!(
                            "Unsupported scalar type: {}",
                            v.data_type()
                        ))),
                    },
                }
            }
            Expr::Value { value } => Ok(EvaluateResult::Scalar(value.clone())),
        }
    }
}

pub async fn evaluate_expr(
    expr_str: &str,
    batch: &RecordBatch,
) -> Result<ColumnarValue, DataFusionError> {
    let df_schema = DFSchema::try_from(batch.schema())?;

    {
        if let Some(expr) = EXPR_CACHE.read().await.get(expr_str) {
            return expr.evaluate(batch);
        }
    }

    let physical_expr = {
        let mut cache = EXPR_CACHE.write().await;
        if let Some(expr) = cache.get(expr_str) {
            expr.clone()
        } else {
            // TODO: Maybe you can reuse session_context?
            let session_context = SessionContext::new();
            let expr = session_context.parse_sql_expr(expr_str, &df_schema)?;
            let physical_expr = session_context.create_physical_expr(expr, &df_schema)?;
            cache.insert(expr_str.to_string(), physical_expr.clone());
            physical_expr
        }
    };

    physical_expr.evaluate(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::common::ScalarValue;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_sql_processor() {
        let batch =
            RecordBatch::try_from_iter([("a", Arc::new(Int32Array::from(vec![4, 230, 21])) as _)])
                .unwrap();
        let sql = r#" 0.9"#;
        let result = evaluate_expr(sql, &batch).await.unwrap();
        match result {
            ColumnarValue::Array(_) => {
                panic!("unexpected scalar value");
            }
            ColumnarValue::Scalar(x) => match x {
                ScalarValue::Float64(v) => {
                    assert_eq!(v, Some(0.9));
                }
                _ => panic!("unexpected scalar value"),
            },
        }
    }

    #[tokio::test]
    async fn test_string_expr() {
        let batch = RecordBatch::try_from_iter([(
            "name",
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as _,
        )])
        .unwrap();

        // Test string expression
        let expr = Expr::Expr {
            expr: "concat(name, ' is here')".to_string(),
        };
        let result = expr.evaluate_expr(&batch).await.unwrap();
        match result {
            EvaluateResult::Vec(v) => {
                assert_eq!(v, vec!["Alice is here", "Bob is here", "Charlie is here"]);
            }
            _ => panic!("Expected vector result"),
        }

        // Test direct value
        let value_expr = Expr::Value {
            value: "test value".to_string(),
        };
        let result = value_expr.evaluate_expr(&batch).await.unwrap();
        match result {
            EvaluateResult::Scalar(v) => {
                assert_eq!(v, "test value");
            }
            _ => panic!("Expected scalar result"),
        }
    }

    #[tokio::test]
    async fn test_evaluate_result_get() {
        let scalar_result = EvaluateResult::Scalar("test".to_string());
        assert_eq!(scalar_result.get(0).map(|s| s.as_str()), Some("test"));
        assert_eq!(scalar_result.get(1).map(|s| s.as_str()), Some("test"));

        let vec_result = EvaluateResult::Vec(vec!["a".to_string(), "b".to_string()]);
        assert_eq!(vec_result.get(0).map(|s| s.as_str()), Some("a"));
        assert_eq!(vec_result.get(1).map(|s| s.as_str()), Some("b"));
        assert_eq!(vec_result.get(2), None);
    }

    #[tokio::test]
    async fn test_error_cases() {
        let batch = RecordBatch::try_from_iter([(
            "name",
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as _,
        )])
        .unwrap();

        // Test invalid SQL expression
        let expr = Expr::Expr {
            expr: "invalid sql".to_string(),
        };
        assert!(expr.evaluate_expr(&batch).await.is_err());

        // Test type mismatch
        let expr = Expr::Expr {
            expr: "1 + name".to_string(), // Trying to add number to string
        };
        assert!(expr.evaluate_expr(&batch).await.is_err());
    }

    #[tokio::test]
    async fn test_null_cell_errors_with_expression_and_row() {
        // A null cell must never shrink the result vector: consumers index
        // destinations (topic/key/subject) by row, so a dropped cell would
        // shift every later row onto the wrong destination.
        let batch = RecordBatch::try_from_iter([(
            "name",
            Arc::new(StringArray::from(vec![Some("Alice"), None, Some("Charlie")])) as _,
        )])
        .unwrap();

        // A bare column reference keeps the null (functions like `concat`
        // swallow null arguments, so they would not exercise the path).
        let expr = Expr::Expr {
            expr: "name".to_string(),
        };
        let err = match expr.evaluate_expr(&batch).await {
            Err(e) => e,
            Ok(_) => panic!("null cell must fail the evaluation"),
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("`name`") && msg.contains("row 1"),
            "error must name the expression and the null row, got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_non_null_column_result_is_row_aligned() {
        let batch = RecordBatch::try_from_iter([(
            "name",
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])) as _,
        )])
        .unwrap();

        let expr = Expr::Expr {
            expr: "name".to_string(),
        };
        match expr.evaluate_expr(&batch).await.unwrap() {
            EvaluateResult::Vec(v) => {
                assert_eq!(v.len(), batch.num_rows(), "one cell per row, no gaps");
                assert_eq!(v, vec!["a", "b", "c"]);
            }
            _ => panic!("Expected vector result"),
        }
    }
}
