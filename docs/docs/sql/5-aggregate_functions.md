# Aggregate Functions

Aggregate functions operate on a set of values to compute a single result.

## General Functions

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

Returns an array created from the expression elements. If ordering is required, elements are inserted in the specified order.
This aggregation function can only mix DISTINCT and ORDER BY if the ordering expression is exactly the same as the argument expression.

```sql
array_agg(expression [ORDER BY expression])
```

#### Arguments

- **expression**: The expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Example

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

Returns the average of numeric values in the specified column.

```sql
avg(expression)
```

#### Arguments

- **expression**: The expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Example

```sql
> SELECT avg(column_name) FROM table_name;
+---------------------------+
| avg(column_name)           |
+---------------------------+
| 42.75                      |
+---------------------------+
```

#### Aliases

- mean

### `bit_and`

Computes the bitwise AND of all non-null input values.

```sql
bit_and(expression)
```

#### Arguments

- **expression**: Integer expression to operate on. Can be a constant, column, or function, and any combination of operators.

### `bit_or`

Computes the bitwise OR of all non-null input values.

```sql
bit_or(expression)
```

#### Arguments

- **expression**: Integer expression to operate on. Can be a constant, column, or function, and any combination of operators.

### `bit_xor`

Computes the bitwise exclusive OR of all non-null input values.

```sql
bit_xor(expression)
```

#### Arguments

- **expression**: Integer expression to operate on. Can be a constant, column, or function, and any combination of operators.

### `bool_and`

Returns true if all non-null input values are true, otherwise false.

```sql
bool_and(expression)
```

#### Arguments

- **expression**: The expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Example

```sql
> SELECT bool_and(column_name) FROM table_name;
+----------------------------+
| bool_and(column_name)       |
+----------------------------+
| true                        |
+----------------------------+
```

### `bool_or`

Returns true if all non-null input values are true, otherwise false.

```sql
bool_and(expression)
```

#### Arguments

- **expression**: The expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Example

```sql
> SELECT bool_and(column_name) FROM table_name;
+----------------------------+
| bool_and(column_name)       |
+----------------------------+
| true                        |
+----------------------------+
```

### `count`

Returns the number of non-null values in the specified column. To include null values in the total count, use `count(*)`.

```sql
count(expression)
```

#### Arguments

- **expression**: The expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Example

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

Returns the first element in an aggregation group according to the requested ordering. If no ordering is given, returns an arbitrary element from the group.

```sql
first_value(expression [ORDER BY expression])
```

#### Arguments

- **expression**: The expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Example

```sql
> SELECT first_value(column_name ORDER BY other_column) FROM table_name;
+-----------------------------------------------+
| first_value(column_name ORDER BY other_column)|
+-----------------------------------------------+
| first_element                                 |
+-----------------------------------------------+
```

### `grouping`

Returns 1 if the data is aggregated across the specified column, or 0 if it is not aggregated in the result set.

```sql
grouping(expression)
```

#### Arguments

- **expression**: Expression to evaluate whether data is aggregated across the specified column. Can be a constant, column, or function.

#### Example

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

Returns the last element in an aggregation group according to the requested ordering. If no ordering is given, returns an arbitrary element from the group.

```sql
last_value(expression [ORDER BY expression])
```

#### Arguments

- **expression**: The expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Example

```sql
> SELECT last_value(column_name ORDER BY other_column) FROM table_name;
+-----------------------------------------------+
| last_value(column_name ORDER BY other_column) |
+-----------------------------------------------+
| last_element                                  |
+-----------------------------------------------+
```

### `max`

Returns the maximum value in the specified column.

```sql
max(expression)
```

#### Arguments

- **expression**: The expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Example

```sql
> SELECT max(column_name) FROM table_name;
+----------------------+
| max(column_name)      |
+----------------------+
| 150                  |
+----------------------+
```

### `mean`

_Alias of [avg](#avg)._

### `median`

Returns the median value in the specified column.

```sql
median(expression)
```

#### Arguments

- **expression**: The expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Example

```sql
> SELECT median(column_name) FROM table_name;
+----------------------+
| median(column_name)   |
+----------------------+
| 45.5                 |
+----------------------+
```

### `min`

Returns the minimum value in the specified column.

```sql
min(expression)
```

#### Arguments

- **expression**: The expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Example

```sql
> SELECT min(column_name) FROM table_name;
+----------------------+
| min(column_name)      |
+----------------------+
| 12                   |
+----------------------+
```

### `string_agg`

Concatenates the values of string expressions and places separator values between them.

```sql
string_agg(expression, delimiter)
```

#### Arguments

- **expression**: The string expression to concatenate. Can be a column or any valid string expression.
- **delimiter**: A literal string used as a separator between the concatenated values.

#### Example

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

Returns the sum of all values in the specified column.

```sql
sum(expression)
```

#### Arguments

- **expression**: The expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Example

```sql
> SELECT sum(column_name) FROM table_name;
+-----------------------+
| sum(column_name)       |
+-----------------------+
| 12345                 |
+-----------------------+
```

### `var`

Returns the statistical sample variance of a set of numbers.

```sql
var(expression)
```

#### Arguments

- **expression**: Numeric expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Aliases

- var_sample
- var_samp

### `var_pop`

Returns the statistical population variance of a set of numbers.

```sql
var_pop(expression)
```

#### Arguments

- **expression**: Numeric expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Aliases

- var_population

### `var_population`

_Alias of [var_pop](#var_pop)._

### `var_samp`

_Alias of [var](#var)._

### `var_sample`

_Alias of [var](#var)._

## Statistical Functions

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

Returns the coefficient of correlation between two numeric values.

```sql
corr(expression1, expression2)
```

#### Arguments

- **expression1**: First expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **expression2**: Second expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Example

```sql
> SELECT corr(column1, column2) FROM table_name;
+--------------------------------+
| corr(column1, column2)         |
+--------------------------------+
| 0.85                           |
+--------------------------------+
```

### `covar`

_Alias of [covar_samp](#covar_samp)._

### `covar_pop`

Returns the sample covariance of a set of number pairs.

```sql
covar_samp(expression1, expression2)
```

#### Arguments

- **expression1**: First expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **expression2**: Second expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Example

```sql
> SELECT covar_samp(column1, column2) FROM table_name;
+-----------------------------------+
| covar_samp(column1, column2)      |
+-----------------------------------+
| 8.25                              |
+-----------------------------------+
```

### `covar_samp`

Returns the sample covariance of a set of number pairs.

```sql
covar_samp(expression1, expression2)
```

#### Arguments

- **expression1**: First expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **expression2**: Second expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Example

```sql
> SELECT covar_samp(column1, column2) FROM table_name;
+-----------------------------------+
| covar_samp(column1, column2)      |
+-----------------------------------+
| 8.25                              |
+-----------------------------------+
```

#### Aliases

- covar

### `nth_value`

Returns the nth value in a group of values.

```sql
nth_value(expression, n ORDER BY expression)
```

#### Arguments

- **expression**: The column or expression to retrieve the nth value from.
- **n**: The position (nth) of the value to retrieve, based on the ordering.

#### Example

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

Computes the average of the independent variable (input) expression_x for the non-null paired data points.

```sql
regr_avgx(expression_y, expression_x)
```

#### Arguments

- **expression_y**: Dependent variable expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **expression_x**: Independent variable expression to operate on. Can be a constant, column, or function, and any combination of operators.

### `regr_avgy`

Computes the average of the dependent variable (output) expression_y for the non-null paired data points.

```sql
regr_avgy(expression_y, expression_x)
```

#### Arguments

- **expression_y**: Dependent variable expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **expression_x**: Independent variable expression to operate on. Can be a constant, column, or function, and any combination of operators.

### `regr_count`

Counts the number of non-null paired data points.

```sql
regr_count(expression_y, expression_x)
```

#### Arguments

- **expression_y**: Dependent variable expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **expression_x**: Independent variable expression to operate on. Can be a constant, column, or function, and any combination of operators.

### `regr_intercept`

Computes the y-intercept of the linear regression line. For the equation (y = kx + b), this function returns b.

```sql
regr_intercept(expression_y, expression_x)
```

#### Arguments

- **expression_y**: Dependent variable expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **expression_x**: Independent variable expression to operate on. Can be a constant, column, or function, and any combination of operators.

### `regr_r2`

Computes the square of the correlation coefficient between the independent and dependent variables.

```sql
regr_r2(expression_y, expression_x)
```

#### Arguments

- **expression_y**: Dependent variable expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **expression_x**: Independent variable expression to operate on. Can be a constant, column, or function, and any combination of operators.

### `regr_slope`

Returns the slope of the linear regression line for non-null pairs in aggregate columns. Given input column Y and X: regr_slope(Y, X) returns the slope (k in Y = k\*X + b) using minimal RSS fitting.

```sql
regr_slope(expression_y, expression_x)
```

#### Arguments

- **expression_y**: Dependent variable expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **expression_x**: Independent variable expression to operate on. Can be a constant, column, or function, and any combination of operators.

### `regr_sxx`

Computes the sum of squares of the independent variable.

```sql
regr_sxx(expression_y, expression_x)
```

#### Arguments

- **expression_y**: Dependent variable expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **expression_x**: Independent variable expression to operate on. Can be a constant, column, or function, and any combination of operators.

### `regr_sxy`

Computes the sum of products of paired data points.

```sql
regr_sxy(expression_y, expression_x)
```

#### Arguments

- **expression_y**: Dependent variable expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **expression_x**: Independent variable expression to operate on. Can be a constant, column, or function, and any combination of operators.

### `regr_syy`

Computes the sum of squares of the dependent variable.

```sql
regr_syy(expression_y, expression_x)
```

#### Arguments

- **expression_y**: Dependent variable expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **expression_x**: Independent variable expression to operate on. Can be a constant, column, or function, and any combination of operators.

### `stddev`

Returns the standard deviation of a set of numbers.

```sql
stddev(expression)
```

#### Arguments

- **expression**: The expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Example

```sql
> SELECT stddev(column_name) FROM table_name;
+----------------------+
| stddev(column_name)   |
+----------------------+
| 12.34                |
+----------------------+
```

#### Aliases

- stddev_samp

### `stddev_pop`

Returns the population standard deviation of a set of numbers.

```sql
stddev_pop(expression)
```

#### Arguments

- **expression**: The expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Example

```sql
> SELECT stddev_pop(column_name) FROM table_name;
+--------------------------+
| stddev_pop(column_name)   |
+--------------------------+
| 10.56                    |
+--------------------------+
```

### `stddev_samp`

_Alias of [stddev](#stddev)._

## Approximate Functions

- [approx_distinct](#approx_distinct)
- [approx_median](#approx_median)
- [approx_percentile_cont](#approx_percentile_cont)
- [approx_percentile_cont_with_weight](#approx_percentile_cont_with_weight)

### `approx_distinct`

Returns the approximate number of distinct input values calculated using the HyperLogLog algorithm.

```sql
approx_distinct(expression)
```

#### Arguments

- **expression**: The expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Example

```sql
> SELECT approx_distinct(column_name) FROM table_name;
+-----------------------------------+
| approx_distinct(column_name)      |
+-----------------------------------+
| 42                                |
+-----------------------------------+
```

### `approx_median`

Returns the approximate median (50th percentile) of input values. It is an alias of `approx_percentile_cont(x, 0.5)`.

```sql
approx_median(expression)
```

#### Arguments

- **expression**: The expression to operate on. Can be a constant, column, or function, and any combination of operators.

#### Example

```sql
> SELECT approx_median(column_name) FROM table_name;
+-----------------------------------+
| approx_median(column_name)        |
+-----------------------------------+
| 23.5                              |
+-----------------------------------+
```

### `approx_percentile_cont`

Returns the approximate percentile of input values using the t-digest algorithm.

```sql
approx_percentile_cont(expression, percentile, centroids)
```

#### Arguments

- **expression**: The expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **percentile**: Percentile to compute. Must be a float value between 0 and 1 (inclusive).
- **centroids**: Number of centroids to use in the t-digest algorithm. _Default is 100_. A higher number results in more accurate approximation but requires more memory.

#### Example

```sql
> SELECT approx_percentile_cont(column_name, 0.75, 100) FROM table_name;
+-------------------------------------------------+
| approx_percentile_cont(column_name, 0.75, 100)  |
+-------------------------------------------------+
| 65.0                                            |
+-------------------------------------------------+
```

### `approx_percentile_cont_with_weight`

Returns the weighted approximate percentile of input values using the t-digest algorithm.

```sql
approx_percentile_cont_with_weight(expression, weight, percentile)
```

#### Arguments

- **expression**: The expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **weight**: Expression to use as weight. Can be a constant, column, or function, and any combination of arithmetic operators.
- **percentile**: Percentile to compute. Must be a float value between 0 and 1 (inclusive).

#### Example

```sql
> SELECT approx_percentile_cont_with_weight(column_name, weight_column, 0.90) FROM table_name;
+----------------------------------------------------------------------+
| approx_percentile_cont_with_weight(column_name, weight_column, 0.90) |
+----------------------------------------------------------------------+
| 78.5                                                                 |
+----------------------------------------------------------------------+
```