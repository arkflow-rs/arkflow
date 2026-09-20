---
description: ArkFlow 文档页面。
---

# 窗口函数(Window Functions)

_窗口函数_ 会对一组与当前行以某种方式相关的表行执行计算。
这类似于聚合函数(aggregate function)所能完成的计算。
但窗口函数不会像非窗口聚合调用那样把行分组成单个输出行,
而是让各行保持独立。在底层,窗口函数能访问的不只是查询结果的当前行

下面的示例展示如何比较每位员工的薪水与其所在部门的平均薪水:

```sql
SELECT depname, empno, salary, avg(salary) OVER (PARTITION BY depname) FROM empsalary;

+-----------+-------+--------+-------------------+
| depname   | empno | salary | avg               |
+-----------+-------+--------+-------------------+
| personnel | 2     | 3900   | 3700.0            |
| personnel | 5     | 3500   | 3700.0            |
| develop   | 8     | 6000   | 5020.0            |
| develop   | 10    | 5200   | 5020.0            |
| develop   | 11    | 5200   | 5020.0            |
| develop   | 9     | 4500   | 5020.0            |
| develop   | 7     | 4200   | 5020.0            |
| sales     | 1     | 5000   | 4866.666666666667 |
| sales     | 4     | 4800   | 4866.666666666667 |
| sales     | 3     | 4800   | 4866.666666666667 |
+-----------+-------+--------+-------------------+
```

窗口函数调用总是包含一个直接跟在窗口函数名和参数之后的 OVER 子句——这正是它在语法上区别于普通函数或非窗口聚合的地方。OVER 子句决定了查询的行如何被切分并交给窗口函数处理。OVER 内的 PARTITION BY 子句按照 PARTITION BY 表达式的相同取值把行分成组,即分区(partition)。对每一行来说,窗口函数是在与当前行落入同一分区的所有行上计算的。前面的示例展示了如何按分区计算某一列的平均值。

你还可以使用 OVER 内的 ORDER BY 来控制窗口函数处理行的顺序。(窗口的 ORDER BY 甚至不必与行的输出顺序一致。)示例如下:

```sql
SELECT depname, empno, salary,
       rank() OVER (PARTITION BY depname ORDER BY salary DESC)
FROM empsalary;

+-----------+-------+--------+--------+
| depname   | empno | salary | rank   |
+-----------+-------+--------+--------+
| personnel | 2     | 3900   | 1      |
| develop   | 8     | 6000   | 1      |
| develop   | 10    | 5200   | 2      |
| develop   | 11    | 5200   | 2      |
| develop   | 9     | 4500   | 4      |
| develop   | 7     | 4200   | 5      |
| sales     | 1     | 5000   | 1      |
| sales     | 4     | 4800   | 2      |
| personnel | 5     | 3500   | 2      |
| sales     | 3     | 4800   | 2      |
+-----------+-------+--------+--------+
```

与窗口函数相关的另一个重要概念是:对每一行来说,其分区内有一组行称为它的窗口帧(window frame)。有些窗口函数只作用于窗口帧中的行,而不是整个分区。下面是在查询中使用窗口帧的示例:

```sql
SELECT depname, empno, salary,
    avg(salary) OVER(ORDER BY salary ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS avg,
    min(salary) OVER(ORDER BY empno ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_min
FROM empsalary
ORDER BY empno ASC;

+-----------+-------+--------+--------------------+---------+
| depname   | empno | salary | avg                | cum_min |
+-----------+-------+--------+--------------------+---------+
| sales     | 1     | 5000   | 5000.0             | 5000    |
| personnel | 2     | 3900   | 3866.6666666666665 | 3900    |
| sales     | 3     | 4800   | 4700.0             | 3900    |
| sales     | 4     | 4800   | 4866.666666666667  | 3900    |
| personnel | 5     | 3500   | 3700.0             | 3500    |
| develop   | 7     | 4200   | 4200.0             | 3500    |
| develop   | 8     | 6000   | 5600.0             | 3500    |
| develop   | 9     | 4500   | 4500.0             | 3500    |
| develop   | 10    | 5200   | 5133.333333333333  | 3500    |
| develop   | 11    | 5200   | 5466.666666666667  | 3500    |
+-----------+-------+--------+--------------------+---------+
```

当查询涉及多个窗口函数时,可以为每个函数各写一个 OVER 子句,但如果多个函数想要相同的窗口行为,这样做既重复又容易出错。更好的做法是在 WINDOW 子句中为每种窗口行为命名,然后在 OVER 中引用。例如:

```sql
SELECT sum(salary) OVER w, avg(salary) OVER w
FROM empsalary
WINDOW w AS (PARTITION BY depname ORDER BY salary DESC);
```

## 语法(Syntax)

OVER 子句的语法为

```sql
function([expr])
  OVER(
    [PARTITION BY expr[, …]]
    [ORDER BY expr [ ASC | DESC ][, …]]
    [ frame_clause ]
    )
```

其中 **frame_clause** 为以下之一:

```sql
  { RANGE | ROWS | GROUPS } frame_start
  { RANGE | ROWS | GROUPS } BETWEEN frame_start AND frame_end
```

**frame_start** 和 **frame_end** 可以为以下之一

```sql
UNBOUNDED PRECEDING
offset PRECEDING
CURRENT ROW
offset FOLLOWING
UNBOUNDED FOLLOWING
```

其中 **offset** 是一个非负整数。

RANGE 和 GROUPS 模式要求有 ORDER BY 子句(使用 RANGE 时,ORDER BY 必须恰好指定一列)。

## 聚合函数(Aggregate functions)

所有[聚合函数](/zh-Hans/docs/sql/aggregate_functions)都可以用作窗口函数。

## 排名函数(Ranking Functions)

- [cume_dist](#cume_dist)
- [dense_rank](#dense_rank)
- [ntile](#ntile)
- [percent_rank](#percent_rank)
- [rank](#rank)
- [row_number](#row_number)

### `cume_dist`

当前行的相对排名:(前于或并列于当前行的行数)/(总行数)。

```sql
cume_dist()
```

### `dense_rank`

返回当前行的排名,中间不留空隙。该函数以紧凑方式对行排名,即使值相同也会分配连续的排名。

```sql
dense_rank()
```

### `ntile`

返回 1 到参数值之间的整数,将分区尽可能平均地划分

```sql
ntile(expression)
```

#### 参数

- **expression**:一个整数,表示分区应被划分成的组数

### `percent_rank`

返回当前行在其分区内的百分比排名。取值范围为 0 到 1,计算方式为 `(rank - 1) / (total_rows - 1)`。

```sql
percent_rank()
```

### `rank`

返回当前行在其分区内的排名,排名之间允许有间隔。该函数提供的排名类似于 `row_number`,但会为相同的值跳过排名。

```sql
rank()
```

### `row_number`

当前行在其分区内的序号,从 1 开始计数。

```sql
row_number()
```

## 分析函数(Analytical Functions)

- [first_value](#first_value)
- [lag](#lag)
- [last_value](#last_value)
- [lead](#lead)
- [nth_value](#nth_value)

### `first_value`

返回在窗口帧第一行上求得的值。

```sql
first_value(expression)
```

#### 参数

- **expression**:要操作的表达式

### `lag`

返回在分区内位于当前行之前 offset 行的那一行上求得的值;如果不存在这样的行,则返回 default(其类型必须与 value 相同)。

```sql
lag(expression, offset, default)
```

#### 参数

- **expression**:要操作的表达式
- **offset**:整数。指定向前取多少行来获取 expression 的值。默认为 1。
- **default**:当 offset 超出分区范围时使用的默认值。类型必须与 expression 相同。

### `last_value`

返回在窗口帧最后一行上求得的值。

```sql
last_value(expression)
```

#### 参数

- **expression**:要操作的表达式

### `lead`

返回在分区内位于当前行之后 offset 行的那一行上求得的值;如果不存在这样的行,则返回 default(其类型必须与 value 相同)。

```sql
lead(expression, offset, default)
```

#### 参数

- **expression**:要操作的表达式
- **offset**:整数。指定向后取多少行来获取 expression 的值。默认为 1。
- **default**:当 offset 超出分区范围时使用的默认值。类型必须与 expression 相同。

### `nth_value`

返回在窗口帧第 n 行(从 1 开始计数)上求得的值;如果不存在这样的行则返回 NULL。

```sql
nth_value(expression, n)
```

#### 参数

- **expression**:要获取第 n 个值的那一列的列名
- **n**:整数。指定 nth 中的 n
