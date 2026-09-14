---
slug: sql
title: SQL
description: SQL query engine documentation, including comprehensive coverage of data types, operators, query syntax, subqueries, aggregate functions, window functions, and more
sidebar_position: 7
---

# SQL reference

ArkFlow's SQL processor is powered by Apache DataFusion. Use it to filter,
project, aggregate, and reshape columnar batches inside a pipeline — with
window functions for event-time analytics and user-defined functions when
the built-ins are not enough.

| Section | Contents |
|---------|----------|
| [Data types](./sql/0-data-types.md) | Supported SQL types and their Arrow equivalents. |
| [Operators and literals](./sql/1-operators.md) | Arithmetic, comparison, and logical operators. |
| [SELECT](./sql/2-select.md) | Query syntax, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT. |
| [Subqueries](./sql/4-subqueries.md) | Scalar, IN, and EXISTS subqueries. |
| [Aggregate functions](./sql/5-aggregate_functions.md) | count, sum, avg, and friends. |
| [Window functions](./sql/6-window_functions.md) | OVER clauses, ranking, and analytic functions. |
| [Scalar functions](./sql/7-scalar_functions.md) | The full built-in function catalog. |
| [Special functions](./sql/8-special_functions.md) | Encoding, time, and utility functions. |
| [UDFs](./sql/9-udf.md) | Registering and calling user-defined functions. |

A `FROM batch` clause refers to the current Arrow batch flowing through the
pipeline — see the [SQL processor](./components/2-processors/sql.md) for
configuration and semantics, and the
[recipes](./build/recipes/1-kafka-to-sql.md) for end-to-end usage.
