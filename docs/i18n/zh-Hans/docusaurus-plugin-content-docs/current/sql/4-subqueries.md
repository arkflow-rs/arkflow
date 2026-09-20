---
description: ArkFlow 文档页面。
---

# 子查询(Subqueries)

子查询(subquery,也称内层查询或嵌套查询)是查询中的
查询。
子查询可以用于 `SELECT`、`FROM`、`WHERE` 和 `HAVING` 子句。

以下示例基于下面两张表。

```sql
SELECT * FROM x;

+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
| 2        | 4        |
+----------+----------+
```

```sql
SELECT * FROM y;

+--------+--------+
| number | string |
+--------+--------+
| 1      | one    |
+--------+--------+
| 2      | two    |
+--------+--------+
| 3      | three  |
+--------+--------+
| 4      | four   |
+--------+--------+
```

## 子查询运算符(Subquery Operators)

- [[ NOT ] EXISTS](#-not--exists)
- [[ NOT ] IN](#-not--in)

### [ NOT ] EXISTS

`EXISTS` 运算符返回所有满足以下条件的行:_[相关子查询](#correlated-subqueries)_
(correlated subquery)对该行产生一个或多个匹配。`NOT EXISTS` 返回 _相关子查询_
对该行产生零个匹配的所有行。仅支持 _相关子查询_。

```sql
[NOT] EXISTS (subquery)
```

### [ NOT ] IN

`IN` 运算符返回所有满足以下条件的行:给定表达式的值能在
_[相关子查询](#correlated-subqueries)_ 的结果中找到。
`NOT IN` 返回给定表达式的值在子查询结果或值列表中找不到的所有行。

```sql
expression [NOT] IN (subquery|list-literal)
```

#### 示例

```sql
SELECT * FROM x WHERE column_1 IN (1,3);

+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
```

```sql
SELECT * FROM x WHERE column_1 NOT IN (1,3);

+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 2        | 4        |
+----------+----------+
```

## SELECT 子句中的子查询

`SELECT` 子句子查询把内层查询返回的值用在外层查询的 `SELECT` 列表中。
`SELECT` 子句仅支持 [标量子查询](#scalar-subqueries),即每次执行内层查询
只返回一个值。
返回的值可以逐行不同。

```sql
SELECT [expression1[, expression2, ..., expressionN],] (<subquery>)
```

**注意**:`SELECT` 子句子查询可以用来替代 `JOIN`
操作。

### 示例

```sql
SELECT
  column_1,
  (
    SELECT
      first_value(string)
    FROM
      y
    WHERE
      number = x.column_1
  ) AS "numeric string"
FROM
  x;

+----------+----------------+
| column_1 | numeric string |
+----------+----------------+
|        1 | one            |
|        2 | two            |
+----------+----------------+
```

## FROM 子句中的子查询

`FROM` 子句子查询返回一组结果,外层查询再对这组结果进行
查询和运算。

```sql
SELECT expression1[, expression2, ..., expressionN] FROM (<subquery>)
```

### 示例

下面的查询返回每个房间最大值的平均值。
内层查询返回每个房间中每个字段的最大值。
外层查询使用内层查询的结果,返回每个字段的平均
最大值。

```sql
SELECT
  column_2
FROM
  (
    SELECT
      *
    FROM
      x
    WHERE
      column_1 > 1
  );

+----------+
| column_2 |
+----------+
|        4 |
+----------+
```

## WHERE 子句中的子查询

`WHERE` 子句子查询将表达式与子查询的结果进行比较,
返回 _true_ 或 _false_。
求值为 _false_ 或 NULL 的行会从结果中过滤掉。
`WHERE` 子句支持相关(correlated)与非相关(non-correlated)子查询,
也支持标量与非标量子查询(取决于谓词表达式中使用的运算符)。

```sql
SELECT
  expression1[, expression2, ..., expressionN]
FROM
  <measurement>
WHERE
  expression operator (<subquery>)
```

**注意**:`WHERE` 子句子查询可以用来替代 `JOIN`
操作。

### 示例

#### `WHERE` 子句与标量子查询

下面的查询返回所有 `column_2` 值高于 `y` 中所有 `number` 值
平均值的行。

```sql
SELECT
  *
FROM
  x
WHERE
  column_2 > (
    SELECT
      AVG(number)
    FROM
      y
  );

+----------+----------+
| column_1 | column_2 |
+----------+----------+
|        2 |        4 |
+----------+----------+
```

#### `WHERE` 子句与非标量子查询

非标量子查询必须使用 `[NOT] IN` 或 `[NOT] EXISTS` 运算符,
并且只能返回一列。
返回列中的值会作为一个列表来求值。

下面的查询返回表 `x` 中 `column_2` 值位于表
`y` 中字符串长度大于 3 的数字列表内的所有行。

```sql
SELECT
  *
FROM
  x
WHERE
  column_2 IN (
    SELECT
      number
    FROM
      y
    WHERE
      length(string) > 3
  );

+----------+----------+
| column_1 | column_2 |
+----------+----------+
|        2 |        4 |
+----------+----------+
```

### `WHERE` 子句与相关子查询

下面的查询返回表 `x` 中 `column_2` 值大于表 `y` 中 `string` 值
平均长度的行。
`WHERE` 子句中的子查询使用外层
查询的 `column_1` 值,返回该特定值对应的 `string` 值平均长度。

```sql
SELECT
  *
FROM
  x
WHERE
  column_2 > (
    SELECT
      AVG(length(string))
    FROM
      y
    WHERE
      number = x.column_1
  );

+----------+----------+
| column_1 | column_2 |
+----------+----------+
|        2 |        4 |
+----------+----------+
```

## HAVING 子句中的子查询

`HAVING` 子句子查询把 `SELECT` 子句中聚合函数返回的聚合值
组成的表达式与子查询的结果进行比较,返回 _true_ 或 _false_。
求值为 _false_ 的行会从结果中过滤掉。
`HAVING` 子句支持相关与非相关子查询,
也支持标量与非标量子查询(取决于谓词表达式中使用的运算符)。

```sql
SELECT
  aggregate_expression1[, aggregate_expression2, ..., aggregate_expressionN]
FROM
  <measurement>
WHERE
  <conditional_expression>
GROUP BY
  column_expression1[, column_expression2, ..., column_expressionN]
HAVING
  expression operator (<subquery>)
```

### 示例

下面的查询计算表 `y` 中偶数和奇数的平均值,
并返回等于表 `x` 中 `column_1` 最大值的那些平均值。

#### `HAVING` 子句与标量子查询

```sql
SELECT
  AVG(number) AS avg,
  (number % 2 = 0) AS even
FROM
  y
GROUP BY
  even
HAVING
  avg = (
    SELECT
      MAX(column_1)
    FROM
      x
  );

+-------+--------+
|   avg | even   |
+-------+--------+
|     2 | false  |
+-------+--------+
```

#### `HAVING` 子句与非标量子查询

非标量子查询必须使用 `[NOT] IN` 或 `[NOT] EXISTS` 运算符,
并且只能返回一列。
返回列中的值会作为一个列表来求值。

下面的查询计算表 `y` 中偶数和奇数的平均值,
并返回位于表 `x` 的 `column_1` 中的那些平均值。

```sql
SELECT
  AVG(number) AS avg,
  (number % 2 = 0) AS even
FROM
  y
GROUP BY
  even
HAVING
  avg IN (
    SELECT
      column_1
    FROM
      x
  );

+-------+--------+
|   avg | even   |
+-------+--------+
|     2 | false  |
+-------+--------+
```

## 子查询的分类(Subquery Categories)

根据子查询的行为,可以将其归入以下一个或多个类别:

- [相关](#correlated-subqueries)或
  [非相关](#non-correlated-subqueries)
- [标量](#scalar-subqueries)或[非标量](#non-scalar-subqueries)

### 相关子查询(Correlated subqueries) {#correlated-subqueries}

在**相关**子查询中,内层查询依赖于当前正在处理的行
的值。

**注意**:DataFusion 会在内部把相关子查询改写为 JOIN 以
提升性能。一般来说,相关子查询的性能**不如**
非相关子查询。

### 非相关子查询(Non-correlated subqueries) {#non-correlated-subqueries}

在**非相关**子查询中,内层查询_不_依赖外层
查询,可以独立执行。
内层查询先执行,然后把结果传递给外层查询。

### 标量子查询(Scalar subqueries) {#scalar-subqueries}

**标量**子查询返回单个值(一行中的一列)。
如果没有返回任何行,子查询返回 NULL。

### 非标量子查询(Non-scalar subqueries) {#non-scalar-subqueries}

**非标量**子查询返回 0 行、1 行或多行,每行可以
包含 1 列或多列。对每一列来说,如果没有可返回的值,
子查询返回 NULL。如果没有符合条件的行,子查询
返回 0 行。
