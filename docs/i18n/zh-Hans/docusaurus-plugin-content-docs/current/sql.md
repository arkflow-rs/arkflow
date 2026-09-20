---
slug: sql
title: SQL
description: SQL 查询引擎文档,全面涵盖数据类型、运算符、查询语法、子查询、聚合函数、窗口函数等内容
sidebar_position: 7
---

# SQL 参考

ArkFlow 的 SQL 处理器由 Apache DataFusion 驱动。你可以在流水线中使用它对列式批次进行过滤、投影、聚合与重塑——窗口函数可用于事件时间分析,内置函数不够用时还可以注册用户自定义函数(UDF)。

| 章节 | 内容 |
|---------|----------|
| [数据类型(Data Types)](./sql/0-data-types.md) | 受支持的 SQL 类型及其对应的 Arrow 类型。 |
| [运算符与字面量(Operators and Literals)](./sql/1-operators.md) | 算术、比较和逻辑运算符。 |
| [SELECT](./sql/2-select.md) | 查询语法,WHERE、GROUP BY、HAVING、ORDER BY、LIMIT。 |
| [子查询(Subqueries)](./sql/4-subqueries.md) | 标量、IN 与 EXISTS 子查询。 |
| [聚合函数(Aggregate Functions)](/zh-Hans/docs/sql/aggregate_functions) | count、sum、avg 等聚合函数。 |
| [窗口函数(Window Functions)](./sql/6-window_functions.md) | OVER 子句、排名与分析函数。 |
| [标量函数(Scalar Functions)](/zh-Hans/docs/sql/scalar_functions) | 完整的内置函数目录。 |
| [特殊函数(Special Functions)](/zh-Hans/docs/sql/special_functions) | 编码、时间与实用函数。 |
| [UDF](/zh-Hans/docs/sql/udf) | 注册和调用用户自定义函数。 |

`FROM batch` 子句指代流经流水线的当前 Arrow 批次——配置与语义参见 [SQL 处理器](/zh-Hans/docs/components/processors/sql),端到端用法参见[实战配方](/zh-Hans/docs/build/recipes/kafka-to-sql)。
