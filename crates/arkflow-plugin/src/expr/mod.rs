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
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};

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

pub trait EvaluateExpr<T> {
    fn evaluate_expr(&self, batch: &RecordBatch) -> Result<EvaluateResult<T>, Error>;
}

impl<T> EvaluateResult<T> {
    pub fn get(&self, i: usize) -> Option<&T> {
        match self {
            EvaluateResult::Scalar(val) => Some(val),
            EvaluateResult::Vec(vec) => vec.get(i),
        }
    }
}

impl EvaluateExpr<String> for Expr<String> {
    fn evaluate_expr(&self, batch: &RecordBatch) -> Result<EvaluateResult<String>, Error> {
        match self {
            Expr::Expr { expr } => {
                let result = evaluate_expr(expr, batch)
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
                        ScalarValue::Utf8(_) => Ok(EvaluateResult::Scalar(v.to_string())),
                        _ => Err(Error::Process("Failed to evaluate expression".to_string())),
                    },
                }
            }
            Expr::Value { value: s } => Ok(EvaluateResult::Scalar(s.to_string())),
        }
    }
}

fn evaluate_expr(expr_str: &str, batch: &RecordBatch) -> Result<ColumnarValue, DataFusionError> {
    let df_schema = DFSchema::try_from(batch.schema())?;

    let context = SessionContext::new();
    let expr = context.parse_sql_expr(expr_str, &df_schema)?;
    let physical_expr = context.create_physical_expr(expr, &df_schema)?;
    physical_expr.evaluate(&batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::common::ScalarValue;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_sql_processor() {
        let batch =
            RecordBatch::try_from_iter([("a", Arc::new(Int32Array::from(vec![4, 0230, 21])) as _)])
                .unwrap();
        let sql = r#" 0.9"#;
        let result = evaluate_expr(sql, &batch).unwrap();
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
}
