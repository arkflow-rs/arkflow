---
description: ArkFlow 文档页面。
---

# SELECT 语法

DataFusion 的查询会扫描表中的数据并返回 0 行或多行。
请注意:查询中的列名会被转为小写,但推断出的模式(schema)并不会。因此,如果
要查询大写的字段,请务必使用双引号。详情请参见这个
[示例](https://datafusion.apache.org/user-guide/example-usage.html)。
本文档描述 DataFusion 的 SQL 语法。

DataFusion 支持以下查询语法:

```sql
[ WITH with_query [, ...] ]  
SELECT  [ ALL | DISTINCT ] select_expr [, ...]  
[ FROM from_item [, ...] ]  
[ JOIN join_item [, ...] ] 
[ WHERE condition ]  
[ GROUP BY grouping_element [, ...] ]  
[ HAVING condition]  
[ UNION [ ALL | select ]  
[ ORDER BY expression [ ASC | DESC ][, ...] ] 
[ LIMIT count ]
[ EXCLUDE | EXCEPT ]

```
 


 

## WITH 子句

WITH 子句允许为查询命名,并通过名称引用它们。

```sql
WITH x AS (SELECT a, MAX(b) AS b FROM t GROUP BY a)
SELECT a, b FROM x;
```

## SELECT 子句

示例:

```sql
SELECT a, b, a + b FROM table
```

可以添加 `DISTINCT` 限定符,让查询返回所有不重复的行。
默认使用 `ALL`,即返回所有行。

```sql
SELECT DISTINCT person, age FROM employees
```

## FROM 子句

示例:

```sql
SELECT t.a FROM table AS t
```

## WHERE 子句

示例:

```sql
SELECT a FROM table WHERE a > 10
```

## JOIN 子句

DataFusion 支持 `INNER JOIN`、`LEFT OUTER JOIN`、`RIGHT OUTER JOIN`、`FULL OUTER JOIN`、`NATURAL JOIN` 和 `CROSS JOIN`。

以下示例基于这张表:

```sql
select * from x;
+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
```

### INNER JOIN

关键字 `JOIN` 或 `INNER JOIN` 定义的连接(join)只显示在两张表中都存在匹配的行。

```sql
select * from x inner join x y ON x.column_1 = y.column_1;
+----------+----------+----------+----------+
| column_1 | column_2 | column_1 | column_2 |
+----------+----------+----------+----------+
| 1        | 2        | 1        | 2        |
+----------+----------+----------+----------+
```

### LEFT OUTER JOIN

关键字 `LEFT JOIN` 或 `LEFT OUTER JOIN` 定义的连接会包含左表的所有行,即使右表中没有匹配。
当没有匹配时,连接的右侧会产生 NULL 值。

```sql
select * from x left join x y ON x.column_1 = y.column_2;
+----------+----------+----------+----------+
| column_1 | column_2 | column_1 | column_2 |
+----------+----------+----------+----------+
| 1        | 2        |          |          |
+----------+----------+----------+----------+
```

### RIGHT OUTER JOIN

关键字 `RIGHT JOIN` 或 `RIGHT OUTER JOIN` 定义的连接会包含右表的所有行,即使左表中没有匹配。
当没有匹配时,连接的左侧会产生 NULL 值。

```sql
select * from x right join x y ON x.column_1 = y.column_2;
+----------+----------+----------+----------+
| column_1 | column_2 | column_1 | column_2 |
+----------+----------+----------+----------+
|          |          | 1        | 2        |
+----------+----------+----------+----------+
```

### FULL OUTER JOIN

关键字 `FULL JOIN` 或 `FULL OUTER JOIN` 定义的连接实质上是 `LEFT OUTER JOIN` 和
`RIGHT OUTER JOIN` 的并集。它会显示连接左右两侧的所有行,并在任一侧没有匹配的地方产生 NULL 值。

```sql
select * from x full outer join x y ON x.column_1 = y.column_2;
+----------+----------+----------+----------+
| column_1 | column_2 | column_1 | column_2 |
+----------+----------+----------+----------+
| 1        | 2        |          |          |
|          |          | 1        | 2        |
+----------+----------+----------+----------+
```

### NATURAL JOIN

自然连接(natural join)基于输入表之间共同的列名定义内连接。当找不到共同的
列名时,其行为类似于交叉连接。

```sql
select * from x natural join x y;
+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
```

### CROSS JOIN

交叉连接(cross join)会产生笛卡尔积,将连接左侧的每一行与右侧的每一行进行匹配。

```sql
select * from x cross join x y;
+----------+----------+----------+----------+
| column_1 | column_2 | column_1 | column_2 |
+----------+----------+----------+----------+
| 1        | 2        | 1        | 2        |
+----------+----------+----------+----------+
```

## GROUP BY 子句

示例:

```sql
SELECT a, b, MAX(c) FROM table GROUP BY a, b
```

一些聚合函数接受可选的排序要求,例如 `ARRAY_AGG`。如果给出了排序要求,
聚合会按该顺序计算。

示例:

```sql
SELECT a, b, ARRAY_AGG(c, ORDER BY d) FROM table GROUP BY a, b
```

## HAVING 子句

示例:

```sql
SELECT a, b, MAX(c) FROM table GROUP BY a, b HAVING MAX(c) > 10
```

## UNION 子句

示例:

```sql
SELECT
    a,
    b,
    c
FROM table1
UNION ALL
SELECT
    a,
    b,
    c
FROM table2
```

## ORDER BY 子句

按引用的表达式对结果排序。默认为升序(`ASC`)。
在排序表达式之后添加 `DESC` 即可改为降序。

示例:

```sql
SELECT age, person FROM table ORDER BY age;
SELECT age, person FROM table ORDER BY age DESC;
SELECT age, person FROM table ORDER BY age, person DESC;
```

## LIMIT 子句

将返回的行数限制为最多 `count` 行。`count` 必须是非负整数。

示例:

```sql
SELECT age, person FROM table
LIMIT 10
```

## EXCLUDE 与 EXCEPT 子句

从查询结果中排除指定名称的列。

例如选择除 `age` 和 `person` 之外的所有列:

```sql
SELECT * EXCEPT(age, person)
FROM table;
```

```sql
SELECT * EXCLUDE(age, person)
FROM table;
```
