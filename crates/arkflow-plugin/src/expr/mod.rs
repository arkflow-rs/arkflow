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
use datafusion::arrow::array::{RecordBatch, StringArray};
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
                            let x: Vec<String> = v
                                .into_iter()
                                .filter_map(|x| x.map(|s| s.to_string()))
                                .collect();
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
            return expr.evaluate(&batch);
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

    physical_expr.evaluate(&batch)
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
}
