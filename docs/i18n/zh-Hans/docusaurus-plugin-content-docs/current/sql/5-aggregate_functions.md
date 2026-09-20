---
description: ArkFlow 文档页面。
---

# 聚合函数(Aggregate Functions)

聚合函数(aggregate function)对一组值进行运算,从而计算出一个结果。

## 常规函数(General Functions)

- [array_agg](#array_agg)
- [avg](#avg)
- [bit_and](#bit_and)
- [bit_or](#bit_or)
- [bit_xor](#bit_xor)
- [bool_and](#bool_and)
- [bool_or](#bool_or)
- [count](#count)
- [first_value](#first_value)
- [grouping](#grouping)
- [last_value](#last_value)
- [max](#max)
- [mean](#mean)
- [median](#median)
- [min](#min)
- [string_agg](#string_agg)
- [sum](#sum)
- [var](#var)
- [var_pop](#var_pop)
- [var_population](#var_population)
- [var_samp](#var_samp)
- [var_sample](#var_sample)

### `array_agg`

返回由表达式元素构成的数组。如果需要排序,元素会按指定的顺序插入。
仅当排序表达式与参数表达式完全相同时,该聚合函数才能同时使用 DISTINCT 和 ORDER BY。

```sql
array_agg(expression [ORDER BY expression])
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> SELECT array_agg(column_name ORDER BY other_column) FROM table_name;
+-----------------------------------------------+
| array_agg(column_name ORDER BY other_column)  |
+-----------------------------------------------+
| [element1, element2, element3]                |
+-----------------------------------------------+
> SELECT array_agg(DISTINCT column_name ORDER BY column_name) FROM table_name;
+--------------------------------------------------------+
| array_agg(DISTINCT column_name ORDER BY column_name)  |
+--------------------------------------------------------+
| [element1, element2, element3]                         |
+--------------------------------------------------------+
```

### `avg`

返回指定列中数值的平均值。

```sql
avg(expression)
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> SELECT avg(column_name) FROM table_name;
+---------------------------+
| avg(column_name)           |
+---------------------------+
| 42.75                      |
+---------------------------+
```

#### 别名

- mean

### `bit_and`

计算所有非空输入值的按位与(bitwise AND)。

```sql
bit_and(expression)
```

#### 参数

- **expression**: 要操作的整数表达式。可以是常量、列或函数,以及任意运算符的组合。

### `bit_or`

计算所有非空输入值的按位或(bitwise OR)。

```sql
bit_or(expression)
```

#### 参数

- **expression**: 要操作的整数表达式。可以是常量、列或函数,以及任意运算符的组合。

### `bit_xor`

计算所有非空输入值的按位异或(bitwise XOR)。

```sql
bit_xor(expression)
```

#### 参数

- **expression**: 要操作的整数表达式。可以是常量、列或函数,以及任意运算符的组合。

### `bool_and`

如果所有非空输入值都为 true,则返回 true,否则返回 false。

```sql
bool_and(expression)
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> SELECT bool_and(column_name) FROM table_name;
+----------------------------+
| bool_and(column_name)       |
+----------------------------+
| true                        |
+----------------------------+
```

### `bool_or`

如果任意一个非空输入值为 true,则返回 true,否则返回 false。

```sql
bool_and(expression)
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> SELECT bool_and(column_name) FROM table_name;
+----------------------------+
| bool_and(column_name)       |
+----------------------------+
| true                        |
+----------------------------+
```

### `count`

返回指定列中非空值的数量。若要在总数中包含空值,请使用 `count(*)`。

```sql
count(expression)
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> SELECT count(column_name) FROM table_name;
+-----------------------+
| count(column_name)     |
+-----------------------+
| 100                   |
+-----------------------+

> SELECT count(*) FROM table_name;
+------------------+
| count(*)         |
+------------------+
| 120              |
+------------------+
```

### `first_value`

根据所请求的排序返回聚合组中的第一个元素。如果未给出排序,则返回组中的任意一个元素。

```sql
first_value(expression [ORDER BY expression])
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> SELECT first_value(column_name ORDER BY other_column) FROM table_name;
+-----------------------------------------------+
| first_value(column_name ORDER BY other_column)|
+-----------------------------------------------+
| first_element                                 |
+-----------------------------------------------+
```

### `grouping`

如果数据按指定列进行了聚合,则返回 1;如果结果集中未按该列聚合,则返回 0。

```sql
grouping(expression)
```

#### 参数

- **expression**: 用于求值数据是否按指定列聚合的表达式。可以是常量、列或函数。

#### 示例

```sql
> SELECT column_name, GROUPING(column_name) AS group_column
  FROM table_name
  GROUP BY GROUPING SETS ((column_name), ());
+-------------+-------------+
| column_name | group_column |
+-------------+-------------+
| value1      | 0           |
| value2      | 0           |
| NULL        | 1           |
+-------------+-------------+
```

### `last_value`

根据所请求的排序返回聚合组中的最后一个元素。如果未给出排序,则返回组中的任意一个元素。

```sql
last_value(expression [ORDER BY expression])
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> SELECT last_value(column_name ORDER BY other_column) FROM table_name;
+-----------------------------------------------+
| last_value(column_name ORDER BY other_column) |
+-----------------------------------------------+
| last_element                                  |
+-----------------------------------------------+
```

### `max`

返回指定列中的最大值。

```sql
max(expression)
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> SELECT max(column_name) FROM table_name;
+----------------------+
| max(column_name)      |
+----------------------+
| 150                  |
+----------------------+
```

### `mean`

_[avg](#avg) 的别名。_

### `median`

返回指定列中的中位数。

```sql
median(expression)
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> SELECT median(column_name) FROM table_name;
+----------------------+
| median(column_name)   |
+----------------------+
| 45.5                 |
+----------------------+
```

### `min`

返回指定列中的最小值。

```sql
min(expression)
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> SELECT min(column_name) FROM table_name;
+----------------------+
| min(column_name)      |
+----------------------+
| 12                   |
+----------------------+
```

### `string_agg`

将字符串表达式的值连接起来,并在它们之间放置分隔符。

```sql
string_agg(expression, delimiter)
```

#### 参数

- **expression**: 要连接的字符串表达式。可以是列或任何有效的字符串表达式。
- **delimiter**: 用作连接值之间分隔符的字面量字符串。

#### 示例

```sql
> SELECT string_agg(name, ', ') AS names_list
  FROM employee;
+--------------------------+
| names_list               |
+--------------------------+
| Alice, Bob, Charlie      |
+--------------------------+
```

### `sum`

返回指定列中所有值的总和。

```sql
sum(expression)
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> SELECT sum(column_name) FROM table_name;
+-----------------------+
| sum(column_name)       |
+-----------------------+
| 12345                 |
+-----------------------+
```

### `var`

返回一组数值的统计样本方差(sample variance)。

```sql
var(expression)
```

#### 参数

- **expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 别名

- var_sample
- var_samp

### `var_pop`

返回一组数值的统计总体方差(population variance)。

```sql
var_pop(expression)
```

#### 参数

- **expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 别名

- var_population

### `var_population`

_[var_pop](#var_pop) 的别名。_

### `var_samp`

_[var](#var) 的别名。_

### `var_sample`

_[var](#var) 的别名。_

## 统计函数(Statistical Functions)

- [corr](#corr)
- [covar](#covar)
- [covar_pop](#covar_pop)
- [covar_samp](#covar_samp)
- [nth_value](#nth_value)
- [regr_avgx](#regr_avgx)
- [regr_avgy](#regr_avgy)
- [regr_count](#regr_count)
- [regr_intercept](#regr_intercept)
- [regr_r2](#regr_r2)
- [regr_slope](#regr_slope)
- [regr_sxx](#regr_sxx)
- [regr_sxy](#regr_sxy)
- [regr_syy](#regr_syy)
- [stddev](#stddev)
- [stddev_pop](#stddev_pop)
- [stddev_samp](#stddev_samp)

### `corr`

返回两个数值之间的相关系数。

```sql
corr(expression1, expression2)
```

#### 参数

- **expression1**: 要操作的第一个表达式。可以是常量、列或函数,以及任意运算符的组合。
- **expression2**: 要操作的第二个表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> SELECT corr(column1, column2) FROM table_name;
+--------------------------------+
| corr(column1, column2)         |
+--------------------------------+
| 0.85                           |
+--------------------------------+
```

### `covar`

_[covar_samp](#covar_samp) 的别名。_

### `covar_pop`

返回一组数字对的总体协方差(population covariance)。

```sql
covar_samp(expression1, expression2)
```

#### 参数

- **expression1**: 要操作的第一个表达式。可以是常量、列或函数,以及任意运算符的组合。
- **expression2**: 要操作的第二个表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> SELECT covar_samp(column1, column2) FROM table_name;
+-----------------------------------+
| covar_samp(column1, column2)      |
+-----------------------------------+
| 8.25                              |
+-----------------------------------+
```

### `covar_samp`

返回一组数字对的样本协方差(sample covariance)。

```sql
covar_samp(expression1, expression2)
```

#### 参数

- **expression1**: 要操作的第一个表达式。可以是常量、列或函数,以及任意运算符的组合。
- **expression2**: 要操作的第二个表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> SELECT covar_samp(column1, column2) FROM table_name;
+-----------------------------------+
| covar_samp(column1, column2)      |
+-----------------------------------+
| 8.25                              |
+-----------------------------------+
```

#### 别名

- covar

### `nth_value`

返回一组值中的第 n 个值。

```sql
nth_value(expression, n ORDER BY expression)
```

#### 参数

- **expression**: 要从中获取第 n 个值的列或表达式。
- **n**: 要获取的值按排序所处的位置(第 n 个)。

#### 示例

```sql
> SELECT dept_id, salary, NTH_VALUE(salary, 2) OVER (PARTITION BY dept_id ORDER BY salary ASC) AS second_salary_by_dept
  FROM employee;
+---------+--------+-------------------------+
| dept_id | salary | second_salary_by_dept   |
+---------+--------+-------------------------+
| 1       | 30000  | NULL                    |
| 1       | 40000  | 40000                   |
| 1       | 50000  | 40000                   |
| 2       | 35000  | NULL                    |
| 2       | 45000  | 45000                   |
+---------+--------+-------------------------+
```

### `regr_avgx`

对非空的配对数据点计算自变量(输入)expression_x 的平均值。

```sql
regr_avgx(expression_y, expression_x)
```

#### 参数

- **expression_y**: 要操作的因变量表达式。可以是常量、列或函数,以及任意运算符的组合。
- **expression_x**: 要操作的自变量表达式。可以是常量、列或函数,以及任意运算符的组合。

### `regr_avgy`

对非空的配对数据点计算因变量(输出)expression_y 的平均值。

```sql
regr_avgy(expression_y, expression_x)
```

#### 参数

- **expression_y**: 要操作的因变量表达式。可以是常量、列或函数,以及任意运算符的组合。
- **expression_x**: 要操作的自变量表达式。可以是常量、列或函数,以及任意运算符的组合。

### `regr_count`

统计非空配对数据点的数量。

```sql
regr_count(expression_y, expression_x)
```

#### 参数

- **expression_y**: 要操作的因变量表达式。可以是常量、列或函数,以及任意运算符的组合。
- **expression_x**: 要操作的自变量表达式。可以是常量、列或函数,以及任意运算符的组合。

### `regr_intercept`

计算线性回归直线的 y 截距。对于方程 (y = kx + b),该函数返回 b。

```sql
regr_intercept(expression_y, expression_x)
```

#### 参数

- **expression_y**: 要操作的因变量表达式。可以是常量、列或函数,以及任意运算符的组合。
- **expression_x**: 要操作的自变量表达式。可以是常量、列或函数,以及任意运算符的组合。

### `regr_r2`

计算自变量与因变量之间相关系数的平方。

```sql
regr_r2(expression_y, expression_x)
```

#### 参数

- **expression_y**: 要操作的因变量表达式。可以是常量、列或函数,以及任意运算符的组合。
- **expression_x**: 要操作的自变量表达式。可以是常量、列或函数,以及任意运算符的组合。

### `regr_slope`

返回聚合列中非空配对数据点的线性回归直线斜率。给定输入列 Y 和 X:regr_slope(Y, X) 使用最小 RSS 拟合返回斜率(Y = k\*X + b 中的 k)。

```sql
regr_slope(expression_y, expression_x)
```

#### 参数

- **expression_y**: 要操作的因变量表达式。可以是常量、列或函数,以及任意运算符的组合。
- **expression_x**: 要操作的自变量表达式。可以是常量、列或函数,以及任意运算符的组合。

### `regr_sxx`

计算自变量的平方和。

```sql
regr_sxx(expression_y, expression_x)
```

#### 参数

- **expression_y**: 要操作的因变量表达式。可以是常量、列或函数,以及任意运算符的组合。
- **expression_x**: 要操作的自变量表达式。可以是常量、列或函数,以及任意运算符的组合。

### `regr_sxy`

计算配对数据点的乘积之和。

```sql
regr_sxy(expression_y, expression_x)
```

#### 参数

- **expression_y**: 要操作的因变量表达式。可以是常量、列或函数,以及任意运算符的组合。
- **expression_x**: 要操作的自变量表达式。可以是常量、列或函数,以及任意运算符的组合。

### `regr_syy`

计算因变量的平方和。

```sql
regr_syy(expression_y, expression_x)
```

#### 参数

- **expression_y**: 要操作的因变量表达式。可以是常量、列或函数,以及任意运算符的组合。
- **expression_x**: 要操作的自变量表达式。可以是常量、列或函数,以及任意运算符的组合。

### `stddev`

返回一组数值的标准差。

```sql
stddev(expression)
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> SELECT stddev(column_name) FROM table_name;
+----------------------+
| stddev(column_name)   |
+----------------------+
| 12.34                |
+----------------------+
```

#### 别名

- stddev_samp

### `stddev_pop`

返回一组数值的总体标准差(population standard deviation)。

```sql
stddev_pop(expression)
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> SELECT stddev_pop(column_name) FROM table_name;
+--------------------------+
| stddev_pop(column_name)   |
+--------------------------+
| 10.56                    |
+--------------------------+
```

### `stddev_samp`

_[stddev](#stddev) 的别名。_

## 近似函数(Approximate Functions)

- [approx_distinct](#approx_distinct)
- [approx_median](#approx_median)
- [approx_percentile_cont](#approx_percentile_cont)
- [approx_percentile_cont_with_weight](#approx_percentile_cont_with_weight)

### `approx_distinct`

返回使用 HyperLogLog 算法计算的去重输入值的近似数量。

```sql
approx_distinct(expression)
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> SELECT approx_distinct(column_name) FROM table_name;
+-----------------------------------+
| approx_distinct(column_name)      |
+-----------------------------------+
| 42                                |
+-----------------------------------+
```

### `approx_median`

返回输入值的近似中位数(第 50 百分位数)。它是 `approx_percentile_cont(x, 0.5)` 的别名。

```sql
approx_median(expression)
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> SELECT approx_median(column_name) FROM table_name;
+-----------------------------------+
| approx_median(column_name)        |
+-----------------------------------+
| 23.5                              |
+-----------------------------------+
```

### `approx_percentile_cont`

使用 t-digest 算法返回输入值的近似百分位数。

```sql
approx_percentile_cont(expression, percentile, centroids)
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意运算符的组合。
- **percentile**: 要计算的百分位数。必须是 0 到 1 之间(含边界)的浮点值。
- **centroids**: t-digest 算法使用的质心数量。_默认为 100_。数值越大,近似越精确,但需要更多内存。

#### 示例

```sql
> SELECT approx_percentile_cont(column_name, 0.75, 100) FROM table_name;
+-------------------------------------------------+
| approx_percentile_cont(column_name, 0.75, 100)  |
+-------------------------------------------------+
| 65.0                                            |
+-------------------------------------------------+
```

### `approx_percentile_cont_with_weight`

使用 t-digest 算法返回输入值的加权近似百分位数。

```sql
approx_percentile_cont_with_weight(expression, weight, percentile)
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意运算符的组合。
- **weight**: 用作权重的表达式。可以是常量、列或函数,以及任意算术运算符的组合。
- **percentile**: 要计算的百分位数。必须是 0 到 1 之间(含边界)的浮点值。

#### 示例

```sql
> SELECT approx_percentile_cont_with_weight(column_name, weight_column, 0.90) FROM table_name;
+----------------------------------------------------------------------+
| approx_percentile_cont_with_weight(column_name, weight_column, 0.90) |
+----------------------------------------------------------------------+
| 78.5                                                                 |
+----------------------------------------------------------------------+
```
