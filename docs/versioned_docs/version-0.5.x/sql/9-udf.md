# User Defined Functions (UDFs)

User Defined Functions (UDFs) allow you to extend the functionality of SQL by defining custom functions in Rust and then using them within your SQL queries.

This project supports three types of UDFs:

1.  **Scalar UDF**: Operates on a single row and returns a single value for each row. For example, a function that converts a string to uppercase.
2.  **Aggregate UDF**: Operates on a group of rows and returns a single aggregate value. For example, calculating a custom average for a set of values.
3.  **Window UDF**: Operates on a window (a set of rows) related to the current row. For example, calculating a moving average within a window.

## Registering UDFs

To use a custom UDF, you first need to register it with the system. Registration is done by calling the `register` function in the corresponding module:

-   **Scalar UDF**: Use `arkflow_plugin::processor::udf::scalar_udf::register(udf: ScalarUDF)`
-   **Aggregate UDF**: Use `arkflow_plugin::processor::udf::aggregate_udf::register(udf: AggregateUDF)`
-   **Window UDF**: Use `arkflow_plugin::processor::udf::window_udf::register(udf: WindowUDF)`

These `register` functions add your UDF to a global list.

```rust
use datafusion::logical_expr::{ScalarUDF, AggregateUDF, WindowUDF};
use arkflow_plugin::processor::udf::{scalar_udf, aggregate_udf, window_udf};

// Example: Registering a scalar UDF
// let my_scalar_udf = ScalarUDF::new(...);
// scalar_udf::register(my_scalar_udf);

// Example: Registering an aggregate UDF
// let my_aggregate_udf = AggregateUDF::new(...);
// aggregate_udf::register(my_aggregate_udf);

// Example: Registering a window UDF
// let my_window_udf = WindowUDF::new(...);
// window_udf::register(my_window_udf);
```

## Initialization

Registered UDFs are not immediately available in SQL queries. They are automatically added to DataFusion's `FunctionRegistry` during the processor's execution context initialization via an internal call to the `arkflow_plugin::processor::udf::init` function. This `init` function iterates through all registered scalar, aggregate, and window UDFs and registers them with the current DataFusion context.

Once initialization is complete, you can use your registered UDFs in SQL queries just like built-in functions.