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
    /// Retrieves a reference to a value from the evaluation result by index.
    ///
    /// If the result is a scalar, this method always returns the contained value regardless of the provided index.
    /// If the result is a vector, it returns the element at the specified index, if it exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::expr::EvaluateResult;
    ///
    /// // For a scalar result, a reference to the contained value is always returned.
    /// let scalar_result = EvaluateResult::Scalar(42);
    /// assert_eq!(scalar_result.get(0), Some(&42));
    /// assert_eq!(scalar_result.get(999), Some(&42)); // any index returns the scalar value
    ///
    /// // For a vector result, returns the element at the given index if available.
    /// let vector_result = EvaluateResult::Vec(vec![10, 20, 30]);
    /// assert_eq!(vector_result.get(1), Some(&20));
    /// assert_eq!(vector_result.get(3), None);
    /// ```
    pub fn get(&self, i: usize) -> Option<&T> {
        match self {
            EvaluateResult::Scalar(val) => Some(val),
            EvaluateResult::Vec(vec) => vec.get(i),
        }
    }
}

impl EvaluateExpr<String> for Expr<String> {
    /// Evaluates the expression or literal value contained in the enum variant against a given RecordBatch.
    /// 
    /// For an expression variant (`Expr::Expr { expr }`), it parses and evaluates the SQL expression using DataFusion:
    /// - If the evaluation produces an array, the method attempts to downcast it to a StringArray and returns its contents as a vector.
    /// - If the evaluation produces a scalar, it verifies that it is a UTF-8 string and returns it as a scalar.
    /// 
    /// For a literal value variant (`Expr::Value { value }`), the contained string is directly returned as a scalar.
    /// 
    /// Any failure during expression evaluation or type conversion results in a processing error.
    /// 
    /// # Examples
    /// 
    /// ```
    /// # use arrow::datatypes::{DataType, Field, Schema};
    /// # use arrow::array::{Int32Array, ArrayRef};
    /// # use arrow::record_batch::RecordBatch;
    /// # use std::sync::Arc;
    /// # use your_crate::{Expr, EvaluateExpr, EvaluateResult, Error, evaluate_expr}; // adjust import paths as needed
    /// 
    /// // Create a dummy RecordBatch (not used in a literal value evaluation but required by the interface)
    /// let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    /// let array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
    /// let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    /// 
    /// // Evaluate a literal value expression.
    /// let expr = Expr::Value { value: "hello".to_string() };
    /// let result = expr.evaluate_expr(&batch).unwrap();
    /// if let EvaluateResult::Scalar(val) = result {
    ///     assert_eq!(val, "hello");
    /// } else {
    ///     panic!("Expected a scalar result");
    /// }
    /// ```
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
