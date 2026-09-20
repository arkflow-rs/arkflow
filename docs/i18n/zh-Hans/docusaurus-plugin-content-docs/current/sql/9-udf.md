---
description: ArkFlow 文档页面。
---

# 用户自定义函数(User Defined Functions,UDF)

用户自定义函数(UDF,User Defined Functions)允许你通过在 Rust 中定义自定义函数来扩展 SQL 的功能,然后在 SQL 查询中使用它们。

本项目支持三种类型的 UDF:

1.  **标量 UDF(Scalar UDF)**:作用于单行,并为每一行返回一个值。例如,将字符串转换为大写的函数。
2.  **聚合 UDF(Aggregate UDF)**:作用于一组行,并返回单个聚合值。例如,为一组值计算自定义平均值。
3.  **窗口 UDF(Window UDF)**:作用于与当前行相关的窗口(一组行)。例如,在窗口内计算移动平均值。

## 注册 UDF(Registering UDFs)

要使用自定义 UDF,首先需要将其注册到系统中。注册通过调用相应模块中的 `register` 函数完成:

-   **标量 UDF**:使用 `arkflow_plugin::processor::udf::scalar_udf::register(udf: ScalarUDF)`
-   **聚合 UDF**:使用 `arkflow_plugin::processor::udf::aggregate_udf::register(udf: AggregateUDF)`
-   **窗口 UDF**:使用 `arkflow_plugin::processor::udf::window_udf::register(udf: WindowUDF)`

这些 `register` 函数会将你的 UDF 添加到一个全局列表中。

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

## 初始化(Initialization)

已注册的 UDF 并不会立即在 SQL 查询中可用。在处理器的执行上下文初始化期间,系统会通过内部调用 `arkflow_plugin::processor::udf::init` 函数,将它们自动添加到 DataFusion 的 `FunctionRegistry` 中。该 `init` 函数会遍历所有已注册的标量、聚合和窗口 UDF,并将它们注册到当前的 DataFusion 上下文中。

初始化完成后,你就可以像使用内置函数一样,在 SQL 查询中使用已注册的 UDF。
