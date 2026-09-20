---
description: ArkFlow 文档页。
---

# 标量函数(Scalar Functions)

## 数学函数(Math Functions)

- [abs](#abs)
- [acos](#acos)
- [acosh](#acosh)
- [asin](#asin)
- [asinh](#asinh)
- [atan](#atan)
- [atan2](#atan2)
- [atanh](#atanh)
- [cbrt](#cbrt)
- [ceil](#ceil)
- [cos](#cos)
- [cosh](#cosh)
- [cot](#cot)
- [degrees](#degrees)
- [exp](#exp)
- [factorial](#factorial)
- [floor](#floor)
- [gcd](#gcd)
- [isnan](#isnan)
- [iszero](#iszero)
- [lcm](#lcm)
- [ln](#ln)
- [log](#log)
- [log10](#log10)
- [log2](#log2)
- [nanvl](#nanvl)
- [pi](#pi)
- [pow](#pow)
- [power](#power)
- [radians](#radians)
- [random](#random)
- [round](#round)
- [signum](#signum)
- [sin](#sin)
- [sinh](#sinh)
- [sqrt](#sqrt)
- [tan](#tan)
- [tanh](#tanh)
- [trunc](#trunc)

### `abs`

返回数字的绝对值。

```sql
abs(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式(numeric expression)。可以是常量、列或函数,以及任意运算符的组合。

### `acos`

返回数字的反余弦。

```sql
acos(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `acosh`

返回数字的反双曲余弦。

```sql
acosh(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `asin`

返回数字的反正弦。

```sql
asin(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `asinh`

返回数字的反双曲正弦。

```sql
asinh(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `atan`

返回数字的反正切。

```sql
atan(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `atan2`

返回 `expression_y / expression_x` 的反正切。

```sql
atan2(expression_y, expression_x)
```

#### 参数

- **expression_y**: 要操作的第一个数值表达式。
  可以是常量、列或函数,以及任意算术运算符的组合。
- **expression_x**: 要操作的第二个数值表达式。
  可以是常量、列或函数,以及任意算术运算符的组合。

### `atanh`

返回数字的反双曲正切。

```sql
atanh(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `cbrt`

返回数字的立方根。

```sql
cbrt(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `ceil`

返回大于或等于该数字的最近整数。

```sql
ceil(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `cos`

返回数字的余弦。

```sql
cos(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `cosh`

返回数字的双曲余弦。

```sql
cosh(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `cot`

返回数字的余切。

```sql
cot(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `degrees`

将弧度转换为角度。

```sql
degrees(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `exp`

返回数字以 e 为底的指数。

```sql
exp(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `factorial`

阶乘。如果值小于 2 则返回 1。

```sql
factorial(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `floor`

返回小于或等于该数字的最近整数。

```sql
floor(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `gcd`

返回 `expression_x` 和 `expression_y` 的最大公约数。如果两个输入均为零则返回 0。

```sql
gcd(expression_x, expression_y)
```

#### 参数

- **expression_x**: 要操作的第一个数值表达式。可以是常量、列或函数,以及任意运算符的组合。
- **expression_y**: 要操作的第二个数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `isnan`

如果给定数字是 +NaN 或 -NaN 则返回 true,否则返回 false。

```sql
isnan(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `iszero`

如果给定数字是 +0.0 或 -0.0 则返回 true,否则返回 false。

```sql
iszero(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `lcm`

返回 `expression_x` 和 `expression_y` 的最小公倍数。如果任一输入为零则返回 0。

```sql
lcm(expression_x, expression_y)
```

#### 参数

- **expression_x**: 要操作的第一个数值表达式。可以是常量、列或函数,以及任意运算符的组合。
- **expression_y**: 要操作的第二个数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `ln`

返回数字的自然对数。

```sql
ln(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `log`

返回数字以 x 为底的对数。可以指定底数,若省略则默认以 10 为底。

```sql
log(base, numeric_expression)
log(numeric_expression)
```

#### 参数

- **base**: 要作为底数的数值表达式。可以是常量、列或函数,以及任意运算符的组合。
- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `log10`

返回数字以 10 为底的对数。

```sql
log10(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `log2`

返回数字以 2 为底的对数。

```sql
log2(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `nanvl`

如果第一个参数不是 _NaN_,则返回第一个参数。
否则返回第二个参数。

```sql
nanvl(expression_x, expression_y)
```

#### 参数

- **expression_x**: 非 _NaN_ 时要返回的数值表达式。可以是常量、列或函数,以及任意算术运算符的组合。
- **expression_y**: 第一个表达式为 _NaN_ 时要返回的数值表达式。可以是常量、列或函数,以及任意算术运算符的组合。

### `pi`

返回 π 的近似值。

```sql
pi()
```

### `pow`

_[power](#power) 的别名。_

### `power`

返回底数表达式的指数次幂。

```sql
power(base, exponent)
```

#### 参数

- **base**: 要作为底数的数值表达式。可以是常量、列或函数,以及任意运算符的组合。
- **exponent**: 要作为指数的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 别名

- pow

### `radians`

将角度转换为弧度。

```sql
radians(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `random`

返回 [0, 1) 范围内的随机浮点值。
随机种子对每一行都是唯一的。

```sql
random()
```

### `round`

将数字四舍五入到最接近的整数。

```sql
round(numeric_expression[, decimal_places])
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。
- **decimal_places**: 可选。要四舍五入到的小数位数。默认值为 0。

### `signum`

返回数字的符号。
负数返回 `-1`。
零和正数返回 `1`。

```sql
signum(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `sin`

返回数字的正弦。

```sql
sin(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `sinh`

返回数字的双曲正弦。

```sql
sinh(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `sqrt`

返回数字的平方根。

```sql
sqrt(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `tan`

返回数字的正切。

```sql
tan(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `tanh`

返回数字的双曲正切。

```sql
tanh(numeric_expression)
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。

### `trunc`

将数字截断为整数,或截断到指定的小数位数。

```sql
trunc(numeric_expression[, decimal_places])
```

#### 参数

- **numeric_expression**: 要操作的数值表达式。可以是常量、列或函数,以及任意运算符的组合。
- **decimal_places**: 可选。要截断到的小数位数。默认值为 0(截断为整数)。
  如果 `decimal_places` 是正整数,则截断小数点右侧的数字。
  如果 `decimal_places` 是负整数,则将小数点左侧的数字替换为 `0`。

## 条件函数(Conditional Functions)

- [coalesce](#coalesce)
- [greatest](#greatest)
- [ifnull](#ifnull)
- [least](#least)
- [nullif](#nullif)
- [nvl](#nvl)
- [nvl2](#nvl2)

### `coalesce`

返回参数中第一个不为 _null_ 的值。如果所有参数都为 _null_,则返回 _null_。此函数常用于将 _null_ 值替换为默认值。

```sql
coalesce(expression1[, ..., expression_n])
```

#### 参数

- **expression1, expression_n**: 前面的表达式为 _null_ 时要使用的表达式。可以是常量、列或函数,以及任意算术运算符的组合。可按需传入任意数量的表达式参数。

#### 示例

```sql
> select coalesce(null, null, 'datafusion');
+----------------------------------------+
| coalesce(NULL,NULL,Utf8("datafusion")) |
+----------------------------------------+
| datafusion                             |
+----------------------------------------+
```

### `greatest`

返回表达式列表中的最大值。如果所有表达式都为 _null_,则返回 _null_。

```sql
greatest(expression1[, ..., expression_n])
```

#### 参数

- **expression1, expression_n**: 要相互比较并返回最大值的表达式。可以是常量、列或函数,以及任意算术运算符的组合。可按需传入任意数量的表达式参数。

#### 示例

```sql
> select greatest(4, 7, 5);
+---------------------------+
| greatest(4,7,5)           |
+---------------------------+
| 7                         |
+---------------------------+
```

### `ifnull`

_[nvl](#nvl) 的别名。_

### `least`

返回表达式列表中的最小值。如果所有表达式都为 _null_,则返回 _null_。

```sql
least(expression1[, ..., expression_n])
```

#### 参数

- **expression1, expression_n**: 要相互比较并返回最小值的表达式。可以是常量、列或函数,以及任意算术运算符的组合。可按需传入任意数量的表达式参数。

#### 示例

```sql
> select least(4, 7, 5);
+---------------------------+
| least(4,7,5)              |
+---------------------------+
| 4                         |
+---------------------------+
```

### `nullif`

如果 _expression1_ 等于 _expression2_,则返回 _null_;否则返回 _expression1_。
可用于执行 [`coalesce`](#coalesce) 的逆运算。

```sql
nullif(expression1, expression2)
```

#### 参数

- **expression1**: 要与 expression2 进行比较并在相等时返回的表达式。可以是常量、列或函数,以及任意运算符的组合。
- **expression2**: 要与 expression1 进行比较的表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select nullif('datafusion', 'data');
+-----------------------------------------+
| nullif(Utf8("datafusion"),Utf8("data")) |
+-----------------------------------------+
| datafusion                              |
+-----------------------------------------+
> select nullif('datafusion', 'datafusion');
+-----------------------------------------------+
| nullif(Utf8("datafusion"),Utf8("datafusion")) |
+-----------------------------------------------+
|                                               |
+-----------------------------------------------+
```

### `nvl`

如果 _expression1_ 为 NULL,则返回 _expression2_,否则返回 _expression1_。

```sql
nvl(expression1, expression2)
```

#### 参数

- **expression1**: 不为 null 时要返回的表达式。可以是常量、列或函数,以及任意运算符的组合。
- **expression2**: expr1 为 null 时要返回的表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select nvl(null, 'a');
+---------------------+
| nvl(NULL,Utf8("a")) |
+---------------------+
| a                   |
+---------------------+\
> select nvl('b', 'a');
+--------------------------+
| nvl(Utf8("b"),Utf8("a")) |
+--------------------------+
| b                        |
+--------------------------+
```

#### 别名

- ifnull

### `nvl2`

如果 _expression1_ 不为 NULL,则返回 _expression2_;否则返回 _expression3_。

```sql
nvl2(expression1, expression2, expression3)
```

#### 参数

- **expression1**: 要进行 null 测试的表达式。可以是常量、列或函数,以及任意运算符的组合。
- **expression2**: expr1 不为 null 时要返回的表达式。可以是常量、列或函数,以及任意运算符的组合。
- **expression3**: expr1 为 null 时要返回的表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select nvl2(null, 'a', 'b');
+--------------------------------+
| nvl2(NULL,Utf8("a"),Utf8("b")) |
+--------------------------------+
| b                              |
+--------------------------------+
> select nvl2('data', 'a', 'b');
+----------------------------------------+
| nvl2(Utf8("data"),Utf8("a"),Utf8("b")) |
+----------------------------------------+
| a                                      |
+----------------------------------------+
```

## 字符串函数(String Functions)

- [ascii](#ascii)
- [bit_length](#bit_length)
- [btrim](#btrim)
- [char_length](#char_length)
- [character_length](#character_length)
- [chr](#chr)
- [concat](#concat)
- [concat_ws](#concat_ws)
- [contains](#contains)
- [ends_with](#ends_with)
- [find_in_set](#find_in_set)
- [initcap](#initcap)
- [instr](#instr)
- [left](#left)
- [length](#length)
- [levenshtein](#levenshtein)
- [lower](#lower)
- [lpad](#lpad)
- [ltrim](#ltrim)
- [octet_length](#octet_length)
- [overlay](#overlay)
- [position](#position)
- [repeat](#repeat)
- [replace](#replace)
- [reverse](#reverse)
- [right](#right)
- [rpad](#rpad)
- [rtrim](#rtrim)
- [split_part](#split_part)
- [starts_with](#starts_with)
- [strpos](#strpos)
- [substr](#substr)
- [substr_index](#substr_index)
- [substring](#substring)
- [substring_index](#substring_index)
- [to_hex](#to_hex)
- [translate](#translate)
- [trim](#trim)
- [upper](#upper)
- [uuid](#uuid)

### `ascii`

返回字符串第一个字符的 Unicode 字符码。

```sql
ascii(str)
```

#### 参数

- **str**: 要操作的字符串表达式(string expression)。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select ascii('abc');
+--------------------+
| ascii(Utf8("abc")) |
+--------------------+
| 97                 |
+--------------------+
> select ascii('🚀');
+-------------------+
| ascii(Utf8("🚀")) |
+-------------------+
| 128640            |
+-------------------+
```

**相关函数**:

- [chr](#chr)

### `bit_length`

返回字符串的位长度。

```sql
bit_length(str)
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select bit_length('datafusion');
+--------------------------------+
| bit_length(Utf8("datafusion")) |
+--------------------------------+
| 80                             |
+--------------------------------+
```

**相关函数**:

- [length](#length)
- [octet_length](#octet_length)

### `btrim`

从字符串的开头和结尾去除指定的修剪字符串。如果未提供修剪字符串,则去除输入字符串开头和结尾的所有空白字符。

```sql
btrim(str[, trim_str])
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **trim_str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。_默认为空白字符。_

#### 示例

```sql
> select btrim('__datafusion____', '_');
+-------------------------------------------+
| btrim(Utf8("__datafusion____"),Utf8("_")) |
+-------------------------------------------+
| datafusion                                |
+-------------------------------------------+
```

#### 替代语法

```sql
trim(BOTH trim_str FROM str)
```

```sql
trim(trim_str FROM str)
```

#### 别名

- trim

**相关函数**:

- [ltrim](#ltrim)
- [rtrim](#rtrim)

### `char_length`

_[character_length](#character_length) 的别名。_

### `character_length`

返回字符串中的字符数。

```sql
character_length(str)
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select character_length('Ångström');
+------------------------------------+
| character_length(Utf8("Ångström")) |
+------------------------------------+
| 8                                  |
+------------------------------------+
```

#### 别名

- length
- char_length

**相关函数**:

- [bit_length](#bit_length)
- [octet_length](#octet_length)

### `chr`

返回具有指定 ASCII 或 Unicode 码值的字符。

```sql
chr(expression)
```

#### 参数

- **expression**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select chr(128640);
+--------------------+
| chr(Int64(128640)) |
+--------------------+
| 🚀                 |
+--------------------+
```

**相关函数**:

- [ascii](#ascii)

### `concat`

将多个字符串拼接在一起。

```sql
concat(str[, ..., str_n])
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **str_n**: 要拼接的后续字符串表达式。

#### 示例

```sql
> select concat('data', 'f', 'us', 'ion');
+-------------------------------------------------------+
| concat(Utf8("data"),Utf8("f"),Utf8("us"),Utf8("ion")) |
+-------------------------------------------------------+
| datafusion                                            |
+-------------------------------------------------------+
```

**相关函数**:

- [concat_ws](#concat_ws)

### `concat_ws`

使用指定分隔符将多个字符串拼接在一起。

```sql
concat_ws(separator, str[, ..., str_n])
```

#### 参数

- **separator**: 在拼接的字符串之间插入的分隔符。
- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **str_n**: 要拼接的后续字符串表达式。

#### 示例

```sql
> select concat_ws('_', 'data', 'fusion');
+--------------------------------------------------+
| concat_ws(Utf8("_"),Utf8("data"),Utf8("fusion")) |
+--------------------------------------------------+
| data_fusion                                      |
+--------------------------------------------------+
```

**相关函数**:

- [concat](#concat)

### `contains`

如果在 string 中找到 search_str 则返回 true(区分大小写)。

```sql
contains(str, search_str)
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **search_str**: 要在 str 中搜索的字符串。

#### 示例

```sql
> select contains('the quick brown fox', 'row');
+---------------------------------------------------+
| contains(Utf8("the quick brown fox"),Utf8("row")) |
+---------------------------------------------------+
| true                                              |
+---------------------------------------------------+
```

### `ends_with`

测试字符串是否以某个子字符串结尾。

```sql
ends_with(str, substr)
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **substr**: 要测试的子字符串。

#### 示例

```sql
>  select ends_with('datafusion', 'soin');
+--------------------------------------------+
| ends_with(Utf8("datafusion"),Utf8("soin")) |
+--------------------------------------------+
| false                                      |
+--------------------------------------------+
> select ends_with('datafusion', 'sion');
+--------------------------------------------+
| ends_with(Utf8("datafusion"),Utf8("sion")) |
+--------------------------------------------+
| true                                       |
+--------------------------------------------+
```

### `find_in_set`

如果字符串 str 位于由 N 个子字符串组成的字符串列表 strlist 中,则返回 1 到 N 之间的值。

```sql
find_in_set(str, strlist)
```

#### 参数

- **str**: 要在 strlist 中查找的字符串表达式。
- **strlist**: 字符串列表,即由 , 字符分隔的子字符串组成的字符串。

#### 示例

```sql
> select find_in_set('b', 'a,b,c,d');
+----------------------------------------+
| find_in_set(Utf8("b"),Utf8("a,b,c,d")) |
+----------------------------------------+
| 2                                      |
+----------------------------------------+
```

### `initcap`

将输入字符串中每个单词的首字符转换为大写。单词由非字母数字字符分隔。

```sql
initcap(str)
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select initcap('apache datafusion');
+------------------------------------+
| initcap(Utf8("apache datafusion")) |
+------------------------------------+
| Apache Datafusion                  |
+------------------------------------+
```

**相关函数**:

- [lower](#lower)
- [upper](#upper)

### `instr`

_[strpos](#strpos) 的别名。_

### `left`

返回字符串左侧指定数量的字符。

```sql
left(str, n)
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **n**: 要返回的字符数。

#### 示例

```sql
> select left('datafusion', 4);
+-----------------------------------+
| left(Utf8("datafusion"),Int64(4)) |
+-----------------------------------+
| data                              |
+-----------------------------------+
```

**相关函数**:

- [right](#right)

### `length`

_[character_length](#character_length) 的别名。_

### `levenshtein`

返回两个给定字符串之间的 [`Levenshtein distance`](https://en.wikipedia.org/wiki/Levenshtein_distance)。

```sql
levenshtein(str1, str2)
```

#### 参数

- **str1**: 用于计算与 str2 之间 Levenshtein 距离的字符串表达式。
- **str2**: 用于计算与 str1 之间 Levenshtein 距离的字符串表达式。

#### 示例

```sql
> select levenshtein('kitten', 'sitting');
+---------------------------------------------+
| levenshtein(Utf8("kitten"),Utf8("sitting")) |
+---------------------------------------------+
| 3                                           |
+---------------------------------------------+
```

### `lower`

将字符串转换为小写。

```sql
lower(str)
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select lower('Ångström');
+-------------------------+
| lower(Utf8("Ångström")) |
+-------------------------+
| ångström                |
+-------------------------+
```

**相关函数**:

- [initcap](#initcap)
- [upper](#upper)

### `lpad`

用另一个字符串将字符串的左侧填充到指定的字符串长度。

```sql
lpad(str, n[, padding_str])
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **n**: 要填充到的字符串长度。
- **padding_str**: 可选的填充用字符串表达式。可以是常量、列或函数,以及任意字符串运算符的组合。_默认为空格。_

#### 示例

```sql
> select lpad('Dolly', 10, 'hello');
+---------------------------------------------+
| lpad(Utf8("Dolly"),Int64(10),Utf8("hello")) |
+---------------------------------------------+
| helloDolly                                  |
+---------------------------------------------+
```

**相关函数**:

- [rpad](#rpad)

### `ltrim`

从字符串的开头去除指定的修剪字符串。如果未提供修剪字符串,则去除输入字符串开头的所有空白字符。

```sql
ltrim(str[, trim_str])
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **trim_str**: 要从输入字符串开头去除的字符串表达式。可以是常量、列或函数,以及任意算术运算符的组合。_默认为空白字符。_

#### 示例

```sql
> select ltrim('  datafusion  ');
+-------------------------------+
| ltrim(Utf8("  datafusion  ")) |
+-------------------------------+
| datafusion                    |
+-------------------------------+
> select ltrim('___datafusion___', '_');
+-------------------------------------------+
| ltrim(Utf8("___datafusion___"),Utf8("_")) |
+-------------------------------------------+
| datafusion___                             |
+-------------------------------------------+
```

#### 替代语法

```sql
trim(LEADING trim_str FROM str)
```

**相关函数**:

- [btrim](#btrim)
- [rtrim](#rtrim)

### `octet_length`

返回字符串的字节长度。

```sql
octet_length(str)
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select octet_length('Ångström');
+--------------------------------+
| octet_length(Utf8("Ångström")) |
+--------------------------------+
| 10                             |
+--------------------------------+
```

**相关函数**:

- [bit_length](#bit_length)
- [length](#length)

### `overlay`

返回从指定位置开始、以指定长度用另一个字符串替换后的字符串。

```sql
overlay(str PLACING substr FROM pos [FOR count])
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **substr**: 要替换到 str 中的子字符串。
- **pos**: 在 str 中开始替换的起始位置。
- **count**: 从 str 起始位置开始要替换的字符数。如果未指定,则使用 substr 的长度。

#### 示例

```sql
> select overlay('Txxxxas' placing 'hom' from 2 for 4);
+--------------------------------------------------------+
| overlay(Utf8("Txxxxas"),Utf8("hom"),Int64(2),Int64(4)) |
+--------------------------------------------------------+
| Thomas                                                 |
+--------------------------------------------------------+
```

### `position`

_[strpos](#strpos) 的别名。_

### `repeat`

返回将输入字符串重复指定次数后的字符串。

```sql
repeat(str, n)
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **n**: 输入字符串的重复次数。

#### 示例

```sql
> select repeat('data', 3);
+-------------------------------+
| repeat(Utf8("data"),Int64(3)) |
+-------------------------------+
| datadatadata                  |
+-------------------------------+
```

### `replace`

将字符串中所有出现的指定子字符串替换为新的子字符串。

```sql
replace(str, substr, replacement)
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **substr**: 要在输入字符串中替换的子字符串表达式。要操作的子字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **replacement**: 要操作的替换子字符串表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select replace('ABabbaBA', 'ab', 'cd');
+-------------------------------------------------+
| replace(Utf8("ABabbaBA"),Utf8("ab"),Utf8("cd")) |
+-------------------------------------------------+
| ABcdbaBA                                        |
+-------------------------------------------------+
```

### `reverse`

反转字符串的字符顺序。

```sql
reverse(str)
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select reverse('datafusion');
+-----------------------------+
| reverse(Utf8("datafusion")) |
+-----------------------------+
| noisufatad                  |
+-----------------------------+
```

### `right`

返回字符串右侧指定数量的字符。

```sql
right(str, n)
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **n**: 要返回的字符数。

#### 示例

```sql
> select right('datafusion', 6);
+------------------------------------+
| right(Utf8("datafusion"),Int64(6)) |
+------------------------------------+
| fusion                             |
+------------------------------------+
```

**相关函数**:

- [left](#left)

### `rpad`

用另一个字符串将字符串的右侧填充到指定的字符串长度。

```sql
rpad(str, n[, padding_str])
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **n**: 要填充到的字符串长度。
- **padding_str**: 填充用字符串表达式。可以是常量、列或函数,以及任意字符串运算符的组合。_默认为空格。_

#### 示例

```sql
>  select rpad('datafusion', 20, '_-');
+-----------------------------------------------+
| rpad(Utf8("datafusion"),Int64(20),Utf8("_-")) |
+-----------------------------------------------+
| datafusion_-_-_-_-_-                          |
+-----------------------------------------------+
```

**相关函数**:

- [lpad](#lpad)

### `rtrim`

从字符串的结尾去除指定的修剪字符串。如果未提供修剪字符串,则去除输入字符串结尾的所有空白字符。

```sql
rtrim(str[, trim_str])
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **trim_str**: 要从输入字符串结尾去除的字符串表达式。可以是常量、列或函数,以及任意算术运算符的组合。_默认为空白字符。_

#### 示例

```sql
> select rtrim('  datafusion  ');
+-------------------------------+
| rtrim(Utf8("  datafusion  ")) |
+-------------------------------+
|   datafusion                  |
+-------------------------------+
> select rtrim('___datafusion___', '_');
+-------------------------------------------+
| rtrim(Utf8("___datafusion___"),Utf8("_")) |
+-------------------------------------------+
| ___datafusion                             |
+-------------------------------------------+
```

#### 替代语法

```sql
trim(TRAILING trim_str FROM str)
```

**相关函数**:

- [btrim](#btrim)
- [ltrim](#ltrim)

### `split_part`

根据指定分隔符拆分字符串,并返回指定位置的子字符串。

```sql
split_part(str, delimiter, pos)
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **delimiter**: 用于拆分的字符串或字符。
- **pos**: 要返回部分的位置。

#### 示例

```sql
> select split_part('1.2.3.4.5', '.', 3);
+--------------------------------------------------+
| split_part(Utf8("1.2.3.4.5"),Utf8("."),Int64(3)) |
+--------------------------------------------------+
| 3                                                |
+--------------------------------------------------+
```

### `starts_with`

测试字符串是否以某个子字符串开头。

```sql
starts_with(str, substr)
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **substr**: 要测试的子字符串。

#### 示例

```sql
> select starts_with('datafusion','data');
+----------------------------------------------+
| starts_with(Utf8("datafusion"),Utf8("data")) |
+----------------------------------------------+
| true                                         |
+----------------------------------------------+
```

### `strpos`

返回指定子字符串在字符串中的起始位置。位置从 1 开始。如果子字符串不存在于字符串中,该函数返回 0。

```sql
strpos(str, substr)
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **substr**: 要搜索的子字符串表达式。

#### 示例

```sql
> select strpos('datafusion', 'fus');
+----------------------------------------+
| strpos(Utf8("datafusion"),Utf8("fus")) |
+----------------------------------------+
| 5                                      |
+----------------------------------------+
```

#### 替代语法

```sql
position(substr in origstr)
```

#### 别名

- instr
- position

### `substr`

从字符串中特定起始位置提取指定数量字符的子字符串。

```sql
substr(str, start_pos[, length])
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **start_pos**: 子字符串的起始字符位置。字符串中第一个字符的位置为 1。
- **length**: 要提取的字符数。如果未指定,则返回起始位置之后的其余字符串。

#### 示例

```sql
> select substr('datafusion', 5, 3);
+----------------------------------------------+
| substr(Utf8("datafusion"),Int64(5),Int64(3)) |
+----------------------------------------------+
| fus                                          |
+----------------------------------------------+
```

#### 替代语法

```sql
substring(str from start_pos for length)
```

#### 别名

- substring

### `substr_index`

返回 str 中第 count 次出现分隔符 delim 之前的子字符串。
如果 count 为正数,则返回最后一个分隔符(从左数起)左侧的全部内容。
如果 count 为负数,则返回最后一个分隔符(从右数起)右侧的全部内容。

```sql
substr_index(str, delim, count)
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **delim**: 要在 str 中查找并用于拆分 str 的字符串。
- **count**: 搜索分隔符的次数。可以为正数或负数。

#### 示例

```sql
> select substr_index('www.apache.org', '.', 1);
+---------------------------------------------------------+
| substr_index(Utf8("www.apache.org"),Utf8("."),Int64(1)) |
+---------------------------------------------------------+
| www                                                     |
+---------------------------------------------------------+
> select substr_index('www.apache.org', '.', -1);
+----------------------------------------------------------+
| substr_index(Utf8("www.apache.org"),Utf8("."),Int64(-1)) |
+----------------------------------------------------------+
| org                                                      |
+----------------------------------------------------------+
```

#### 别名

- substring_index

### `substring`

_[substr](#substr) 的别名。_

### `substring_index`

_[substr_index](#substr_index) 的别名。_

### `to_hex`

将整数转换为十六进制字符串。

```sql
to_hex(int)
```

#### 参数

- **int**: 要操作的整数表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select to_hex(12345689);
+-------------------------+
| to_hex(Int64(12345689)) |
+-------------------------+
| bc6159                  |
+-------------------------+
```

### `translate`

将字符串中的字符转换为指定的转换字符。

```sql
translate(str, chars, translation)
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **chars**: 要转换的字符。
- **translation**: 转换字符。转换字符仅替换 **chars** 字符串中相同位置上的字符。

#### 示例

```sql
> select translate('twice', 'wic', 'her');
+--------------------------------------------------+
| translate(Utf8("twice"),Utf8("wic"),Utf8("her")) |
+--------------------------------------------------+
| there                                            |
+--------------------------------------------------+
```

### `trim`

_[btrim](#btrim) 的别名。_

### `upper`

将字符串转换为大写。

```sql
upper(str)
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select upper('dataFusion');
+---------------------------+
| upper(Utf8("dataFusion")) |
+---------------------------+
| DATAFUSION                |
+---------------------------+
```

**相关函数**:

- [initcap](#initcap)
- [lower](#lower)

### `uuid`

返回每行唯一的 [`UUID v4`](<https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_(random)>) 字符串值。

```sql
uuid()
```

#### 示例

```sql
> select uuid();
+--------------------------------------+
| uuid()                               |
+--------------------------------------+
| 6ec17ef8-1934-41cc-8d59-d0c8f9eea1f0 |
+--------------------------------------+
```

## 二进制字符串函数(Binary String Functions)

- [decode](#decode)
- [encode](#encode)

### `decode`

将字符串中的文本表示形式解码为二进制数据。

```sql
decode(expression, format)
```

#### 参数

- **expression**: 包含已编码字符串数据的表达式
- **format**: 参数与 [encode](#encode) 相同

**相关函数**:

- [encode](#encode)

### `encode`

将二进制数据编码为文本表示形式。

```sql
encode(expression, format)
```

#### 参数

- **expression**: 包含字符串或二进制数据的表达式
- **format**: 支持的格式:`base64`、`hex`

**相关函数**:

- [decode](#decode)

## 正则表达式函数(Regular Expression Functions)

Apache DataFusion 使用[类 PCRE](https://en.wikibooks.org/wiki/Regular_Expressions/Perl-Compatible_Regular_Expressions) 的
正则表达式[语法](https://docs.rs/regex/latest/regex/#syntax)
(不支持包括环视和反向引用在内的若干特性)。
支持以下正则表达式函数:

- [regexp_count](#regexp_count)
- [regexp_like](#regexp_like)
- [regexp_match](#regexp_match)
- [regexp_replace](#regexp_replace)

### `regexp_count`

返回[正则表达式](https://docs.rs/regex/latest/regex/#syntax)在字符串中的匹配数量。

```sql
regexp_count(str, regexp[, start, flags])
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **regexp**: 要操作的正则表达式。可以是常量、列或函数,以及任意运算符的组合。
- **start**: - **start**: 可选的搜索起始位置(第一个位置为 1)。可以是常量、列或函数。
- **flags**: 可选的正则表达式标志,用于控制正则表达式的行为。支持以下标志:
    - **i**: 不区分大小写:字母同时匹配大写和小写
    - **m**: 多行模式:^ 和 $ 匹配行的开头/结尾
    - **s**: 允许 . 匹配 \n
    - **R**: 启用 CRLF 模式:启用多行模式时使用 \r\n
    - **U**: 交换 x* 和 x*? 的含义

#### 示例

```sql
> select regexp_count('abcAbAbc', 'abc', 2, 'i');
+---------------------------------------------------------------+
| regexp_count(Utf8("abcAbAbc"),Utf8("abc"),Int64(2),Utf8("i")) |
+---------------------------------------------------------------+
| 1                                                             |
+---------------------------------------------------------------+
```

### `regexp_like`

如果[正则表达式](https://docs.rs/regex/latest/regex/#syntax)在字符串中至少有一个匹配,则返回 true,否则返回 false。

```sql
regexp_like(str, regexp[, flags])
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **regexp**: 要操作的正则表达式。可以是常量、列或函数,以及任意运算符的组合。
- **flags**: 可选的正则表达式标志,用于控制正则表达式的行为。支持以下标志:
    - **i**: 不区分大小写:字母同时匹配大写和小写
    - **m**: 多行模式:^ 和 $ 匹配行的开头/结尾
    - **s**: 允许 . 匹配 \n
    - **R**: 启用 CRLF 模式:启用多行模式时使用 \r\n
    - **U**: 交换 x* 和 x*? 的含义

#### 示例

```sql
select regexp_like('Köln', '[a-zA-Z]ö[a-zA-Z]{2}');
+--------------------------------------------------------+
| regexp_like(Utf8("Köln"),Utf8("[a-zA-Z]ö[a-zA-Z]{2}")) |
+--------------------------------------------------------+
| true                                                   |
+--------------------------------------------------------+
SELECT regexp_like('aBc', '(b|d)', 'i');
+--------------------------------------------------+
| regexp_like(Utf8("aBc"),Utf8("(b|d)"),Utf8("i")) |
+--------------------------------------------------+
| true                                             |
+--------------------------------------------------+
```

更多示例请参见[这里](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/regexp.rs)

### `regexp_match`

返回[正则表达式](https://docs.rs/regex/latest/regex/#syntax)在字符串中的第一个匹配。

```sql
regexp_match(str, regexp[, flags])
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **regexp**: 要匹配的正则表达式。
  可以是常量、列或函数。
- **flags**: 可选的正则表达式标志,用于控制正则表达式的行为。支持以下标志:
    - **i**: 不区分大小写:字母同时匹配大写和小写
    - **m**: 多行模式:^ 和 $ 匹配行的开头/结尾
    - **s**: 允许 . 匹配 \n
    - **R**: 启用 CRLF 模式:启用多行模式时使用 \r\n
    - **U**: 交换 x* 和 x*? 的含义

#### 示例

```sql
            > select regexp_match('Köln', '[a-zA-Z]ö[a-zA-Z]{2}');
            +---------------------------------------------------------+
            | regexp_match(Utf8("Köln"),Utf8("[a-zA-Z]ö[a-zA-Z]{2}")) |
            +---------------------------------------------------------+
            | [Köln]                                                  |
            +---------------------------------------------------------+
            SELECT regexp_match('aBc', '(b|d)', 'i');
            +---------------------------------------------------+
            | regexp_match(Utf8("aBc"),Utf8("(b|d)"),Utf8("i")) |
            +---------------------------------------------------+
            | [B]                                               |
            +---------------------------------------------------+
```

更多示例请参见[这里](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/regexp.rs)

### `regexp_replace`

替换字符串中匹配[正则表达式](https://docs.rs/regex/latest/regex/#syntax)的子字符串。

```sql
regexp_replace(str, regexp, replacement[, flags])
```

#### 参数

- **str**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **regexp**: 要匹配的正则表达式。
  可以是常量、列或函数。
- **replacement**: 要操作的替换字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **flags**: 可选的正则表达式标志,用于控制正则表达式的行为。支持以下标志:
- **g**: (全局)全局搜索,不在第一个匹配后停止
- **i**: 不区分大小写:字母同时匹配大写和小写
- **m**: 多行模式:^ 和 $ 匹配行的开头/结尾
- **s**: 允许 . 匹配 \n
- **R**: 启用 CRLF 模式:启用多行模式时使用 \r\n
- **U**: 交换 x* 和 x*? 的含义

#### 示例

```sql
> select regexp_replace('foobarbaz', 'b(..)', 'X\\1Y', 'g');
+------------------------------------------------------------------------+
| regexp_replace(Utf8("foobarbaz"),Utf8("b(..)"),Utf8("X\1Y"),Utf8("g")) |
+------------------------------------------------------------------------+
| fooXarYXazY                                                            |
+------------------------------------------------------------------------+
SELECT regexp_replace('aBc', '(b|d)', 'Ab\\1a', 'i');
+-------------------------------------------------------------------+
| regexp_replace(Utf8("aBc"),Utf8("(b|d)"),Utf8("Ab\1a"),Utf8("i")) |
+-------------------------------------------------------------------+
| aAbBac                                                            |
+-------------------------------------------------------------------+
```

更多示例请参见[这里](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/regexp.rs)

## 日期/时间函数(Time and Date Functions)

- [current_date](#current_date)
- [current_time](#current_time)
- [current_timestamp](#current_timestamp)
- [date_bin](#date_bin)
- [date_format](#date_format)
- [date_part](#date_part)
- [date_trunc](#date_trunc)
- [datepart](#datepart)
- [datetrunc](#datetrunc)
- [from_unixtime](#from_unixtime)
- [make_date](#make_date)
- [now](#now)
- [to_char](#to_char)
- [to_date](#to_date)
- [to_local_time](#to_local_time)
- [to_timestamp](#to_timestamp)
- [to_timestamp_micros](#to_timestamp_micros)
- [to_timestamp_millis](#to_timestamp_millis)
- [to_timestamp_nanos](#to_timestamp_nanos)
- [to_timestamp_seconds](#to_timestamp_seconds)
- [to_unixtime](#to_unixtime)
- [today](#today)

### `current_date`

返回当前 UTC 日期。

`current_date()` 的返回值在查询时确定,无论该函数在查询计划中的何时执行,都会返回相同的日期。

```sql
current_date()
```

#### 别名

- today

### `current_time`

返回当前 UTC 时间。

`current_time()` 的返回值在查询时确定,无论该函数在查询计划中的何时执行,都会返回相同的时间。

```sql
current_time()
```

### `current_timestamp`

_[now](#now) 的别名。_

### `date_bin`

计算时间区间,并返回距指定时间戳最近的区间的起点。使用 `date_bin` 可将行分组到基于时间的"bin"或"窗口"中,并对每个窗口应用聚合或选择器函数,从而对时间序列数据进行降采样。

例如,如果将数据"分箱"或"开窗"为 15 分钟的区间,输入时间戳 `2023-01-01T18:18:18Z` 将被归整到它所在的 15 分钟区间的起始时间:`2023-01-01T18:15:00Z`。

```sql
date_bin(interval, expression, origin-timestamp)
```

#### 参数

- **interval**: 分箱区间。
- **expression**: 要操作的时间表达式。可以是常量、列或函数。
- **origin-timestamp**: 可选。用于确定区间边界的起点。如果未指定,默认为 1970-01-01T00:00:00Z(UTC 的 UNIX 纪元)。支持以下区间:

    - nanoseconds
    - microseconds
    - milliseconds
    - seconds
    - minutes
    - hours
    - days
    - weeks
    - months
    - years
    - century

#### 示例

```sql
-- Bin the timestamp into 1 day intervals
> SELECT date_bin(interval '1 day', time) as bin
FROM VALUES ('2023-01-01T18:18:18Z'), ('2023-01-03T19:00:03Z')  t(time);
+---------------------+
| bin                 |
+---------------------+
| 2023-01-01T00:00:00 |
| 2023-01-03T00:00:00 |
+---------------------+
2 row(s) fetched.

-- Bin the timestamp into 1 day intervals starting at 3AM on  2023-01-01
> SELECT date_bin(interval '1 day', time,  '2023-01-01T03:00:00') as bin
FROM VALUES ('2023-01-01T18:18:18Z'), ('2023-01-03T19:00:03Z')  t(time);
+---------------------+
| bin                 |
+---------------------+
| 2023-01-01T03:00:00 |
| 2023-01-03T03:00:00 |
+---------------------+
2 row(s) fetched.
```

### `date_format`

_[to_char](#to_char) 的别名。_

### `date_part`

以整数形式返回日期的指定部分。

```sql
date_part(part, expression)
```

#### 参数

- **part**: 要返回的日期部分。支持以下日期部分:

    - year
    - quarter (根据日期位于一年中的哪个四分位,输出 [1, 4] 范围内的值)
    - month
    - week (一年中的第几周)
    - day (一月中的第几天)
    - hour
    - minute
    - second
    - millisecond
    - microsecond
    - nanosecond
    - dow (一周中的第几天)
    - doy (一年中的第几天)
    - epoch (自 Unix 纪元以来的秒数)

- **expression**: 要操作的时间表达式。可以是常量、列或函数。

#### 替代语法

```sql
extract(field FROM source)
```

#### 别名

- datepart

### `date_trunc`

将时间戳值截断到指定的精度。

```sql
date_trunc(precision, expression)
```

#### 参数

- **precision**: 要截断到的时间精度。支持以下精度:

    - year / YEAR
    - quarter / QUARTER
    - month / MONTH
    - week / WEEK
    - day / DAY
    - hour / HOUR
    - minute / MINUTE
    - second / SECOND

- **expression**: 要操作的时间表达式。可以是常量、列或函数。

#### 别名

- datetrunc

### `datepart`

_[date_part](#date_part) 的别名。_

### `datetrunc`

_[date_trunc](#date_trunc) 的别名。_

### `from_unixtime`

将整数转换为 RFC3339 时间戳格式(`YYYY-MM-DDT00:00:00.000000000Z`)。整数和无符号整数被解释为自 UNIX 纪元(`1970-01-01T00:00:00Z`)以来的纳秒数,并返回对应的时间戳。

```sql
from_unixtime(expression[, timezone])
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意运算符的组合。
- **timezone**: 可选。将整数转换为时间戳时使用的时区。如果未提供,默认时区为 UTC。

#### 示例

```sql
> select from_unixtime(1599572549, 'America/New_York');
+-----------------------------------------------------------+
| from_unixtime(Int64(1599572549),Utf8("America/New_York")) |
+-----------------------------------------------------------+
| 2020-09-08T09:42:29-04:00                                 |
+-----------------------------------------------------------+
```

### `make_date`

根据年/月/日各部分构造日期。

```sql
make_date(year, month, day)
```

#### 参数

- **year**: 构造日期时使用的年份。可以是常量、列或函数,以及任意算术运算符的组合。
- **month**: 构造日期时使用的月份。可以是常量、列或函数,以及任意算术运算符的组合。
- **day**: 构造日期时使用的日期。可以是常量、列或函数,以及任意算术运算符的组合。

#### 示例

```sql
> select make_date(2023, 1, 31);
+-------------------------------------------+
| make_date(Int64(2023),Int64(1),Int64(31)) |
+-------------------------------------------+
| 2023-01-31                                |
+-------------------------------------------+
> select make_date('2023', '01', '31');
+-----------------------------------------------+
| make_date(Utf8("2023"),Utf8("01"),Utf8("31")) |
+-----------------------------------------------+
| 2023-01-31                                    |
+-----------------------------------------------+
```

更多示例请参见[这里](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/make_date.rs)

### `now`

返回当前 UTC 时间戳。

`now()` 的返回值在查询时确定,无论该函数在查询计划中的何时执行,都会返回相同的时间戳。

```sql
now()
```

#### 别名

- current_timestamp

### `to_char`

基于 [Chrono 格式](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)返回日期、时间、时间戳或持续时间的字符串表示。与 PostgreSQL 中的同名函数不同,不支持数值格式化。

```sql
to_char(expression, format)
```

#### 参数

- **expression**: 要操作的表达式。可以是结果为日期、时间、时间戳或持续时间的常量、列或函数。
- **format**: 用于转换表达式的 [Chrono 格式](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)字符串。
- **day**: 构造日期时使用的日期。可以是常量、列或函数,以及任意算术运算符的组合。

#### 示例

```sql
> select to_char('2023-03-01'::date, '%d-%m-%Y');
+----------------------------------------------+
| to_char(Utf8("2023-03-01"),Utf8("%d-%m-%Y")) |
+----------------------------------------------+
| 01-03-2023                                   |
+----------------------------------------------+
```

更多示例请参见[这里](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/to_char.rs)

#### 别名

- date_format

### `to_date`

将值转换为日期(`YYYY-MM-DD`)。
支持字符串、整数和双精度浮点类型作为输入。
如果未提供 [Chrono 格式](https://docs.rs/chrono/latest/chrono/format/strftime/index.html),字符串将按 YYYY-MM-DD 解析(例如 '2023-07-20')。
整数和双精度浮点数被解释为自 UNIX 纪元(`1970-01-01T00:00:00Z`)以来的天数。
返回对应的日期。

注意:`to_date` 返回 Date32,它以自 UNIX 纪元(`1970-01-01`)以来的天数表示值,存储为有符号 32 位整数。支持的最大日期值为 `9999-12-31`。

```sql
to_date('2017-05-31', '%Y-%m-%d')
```

#### 参数

- **expression**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **format_n**: 可选的 [Chrono 格式](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)字符串,用于解析表达式。将按其出现的顺序依次尝试各格式,
  并返回第一个成功解析的结果。如果所有格式都无法成功解析表达式,
  则返回错误。

#### 示例

```sql
> select to_date('2023-01-31');
+-------------------------------+
| to_date(Utf8("2023-01-31")) |
+-------------------------------+
| 2023-01-31                    |
+-------------------------------+
> select to_date('2023/01/31', '%Y-%m-%d', '%Y/%m/%d');
+---------------------------------------------------------------------+
| to_date(Utf8("2023/01/31"),Utf8("%Y-%m-%d"),Utf8("%Y/%m/%d")) |
+---------------------------------------------------------------------+
| 2023-01-31                                                          |
+---------------------------------------------------------------------+
```

更多示例请参见[这里](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/to_date.rs)

### `to_local_time`

将带时区的时间戳转换为不带时区的时间戳(不含偏移量或时区信息)。此函数会处理夏令时切换。

```sql
to_local_time(expression)
```

#### 参数

- **expression**: 要操作的时间表达式。可以是常量、列或函数。

#### 示例

```sql
> SELECT to_local_time('2024-04-01T00:00:20Z'::timestamp);
+---------------------------------------------+
| to_local_time(Utf8("2024-04-01T00:00:20Z")) |
+---------------------------------------------+
| 2024-04-01T00:00:20                         |
+---------------------------------------------+

> SELECT to_local_time('2024-04-01T00:00:20Z'::timestamp AT TIME ZONE 'Europe/Brussels');
+---------------------------------------------+
| to_local_time(Utf8("2024-04-01T00:00:20Z")) |
+---------------------------------------------+
| 2024-04-01T00:00:20                         |
+---------------------------------------------+

> SELECT
  time,
  arrow_typeof(time) as type,
  to_local_time(time) as to_local_time,
  arrow_typeof(to_local_time(time)) as to_local_time_type
FROM (
  SELECT '2024-04-01T00:00:20Z'::timestamp AT TIME ZONE 'Europe/Brussels' AS time
);
+---------------------------+------------------------------------------------+---------------------+-----------------------------+
| time                      | type                                           | to_local_time       | to_local_time_type          |
+---------------------------+------------------------------------------------+---------------------+-----------------------------+
| 2024-04-01T00:00:20+02:00 | Timestamp(Nanosecond, Some("Europe/Brussels")) | 2024-04-01T00:00:20 | Timestamp(Nanosecond, None) |
+---------------------------+------------------------------------------------+---------------------+-----------------------------+

# combine `to_local_time()` with `date_bin()` to bin on boundaries in the timezone rather
# than UTC boundaries

> SELECT date_bin(interval '1 day', to_local_time('2024-04-01T00:00:20Z'::timestamp AT TIME ZONE 'Europe/Brussels')) AS date_bin;
+---------------------+
| date_bin            |
+---------------------+
| 2024-04-01T00:00:00 |
+---------------------+

> SELECT date_bin(interval '1 day', to_local_time('2024-04-01T00:00:20Z'::timestamp AT TIME ZONE 'Europe/Brussels')) AT TIME ZONE 'Europe/Brussels' AS date_bin_with_timezone;
+---------------------------+
| date_bin_with_timezone    |
+---------------------------+
| 2024-04-01T00:00:00+02:00 |
+---------------------------+
```

### `to_timestamp`

将值转换为时间戳(`YYYY-MM-DDT00:00:00Z`)。支持字符串、整数、无符号整数和双精度浮点类型作为输入。如果未提供 [Chrono 格式],字符串将按 RFC3339 解析(例如 '2023-07-20T05:44:00')。整数、无符号整数和双精度浮点数被解释为自 UNIX 纪元(`1970-01-01T00:00:00Z`)以来的秒数。返回对应的时间戳。

注意:`to_timestamp` 返回 `Timestamp(Nanosecond)`。整数输入的支持范围为 `-9223372037` 到 `9223372036`。字符串输入的支持范围为 `1677-09-21T00:12:44.0` 到 `2262-04-11T23:47:16.0`。超出支持范围的输入请使用 `to_timestamp_seconds`。

```sql
to_timestamp(expression[, ..., format_n])
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意算术运算符的组合。
- **format_n**: 可选的 [Chrono 格式](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)字符串,用于解析表达式。将按其出现的顺序依次尝试各格式,并返回第一个成功解析的结果。如果所有格式都无法成功解析表达式,则返回错误。

#### 示例

```sql
> select to_timestamp('2023-01-31T09:26:56.123456789-05:00');
+-----------------------------------------------------------+
| to_timestamp(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+-----------------------------------------------------------+
| 2023-01-31T14:26:56.123456789                             |
+-----------------------------------------------------------+
> select to_timestamp('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+--------------------------------------------------------------------------------------------------------+
| to_timestamp(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+--------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00.123456789                                                                          |
+--------------------------------------------------------------------------------------------------------+
```

更多示例请参见[这里](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/to_timestamp.rs)

### `to_timestamp_micros`

将值转换为时间戳(`YYYY-MM-DDT00:00:00.000000Z`)。支持字符串、整数和无符号整数类型作为输入。如果未提供 [Chrono 格式](https://docs.rs/chrono/latest/chrono/format/strftime/index.html),字符串将按 RFC3339 解析(例如 '2023-07-20T05:44:00')。整数和无符号整数被解释为自 UNIX 纪元(`1970-01-01T00:00:00Z`)以来的微秒数。返回对应的时间戳。

```sql
to_timestamp_micros(expression[, ..., format_n])
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意算术运算符的组合。
- **format_n**: 可选的 [Chrono 格式](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)字符串,用于解析表达式。将按其出现的顺序依次尝试各格式,并返回第一个成功解析的结果。如果所有格式都无法成功解析表达式,则返回错误。

#### 示例

```sql
> select to_timestamp_micros('2023-01-31T09:26:56.123456789-05:00');
+------------------------------------------------------------------+
| to_timestamp_micros(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+------------------------------------------------------------------+
| 2023-01-31T14:26:56.123456                                       |
+------------------------------------------------------------------+
> select to_timestamp_micros('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+---------------------------------------------------------------------------------------------------------------+
| to_timestamp_micros(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+---------------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00.123456                                                                                    |
+---------------------------------------------------------------------------------------------------------------+
```

更多示例请参见[这里](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/to_timestamp.rs)

### `to_timestamp_millis`

将值转换为时间戳(`YYYY-MM-DDT00:00:00.000Z`)。支持字符串、整数和无符号整数类型作为输入。如果未提供 [Chrono 格式](https://docs.rs/chrono/latest/chrono/format/strftime/index.html),字符串将按 RFC3339 解析(例如 '2023-07-20T05:44:00')。整数和无符号整数被解释为自 UNIX 纪元(`1970-01-01T00:00:00Z`)以来的毫秒数。返回对应的时间戳。

```sql
to_timestamp_millis(expression[, ..., format_n])
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意算术运算符的组合。
- **format_n**: 可选的 [Chrono 格式](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)字符串,用于解析表达式。将按其出现的顺序依次尝试各格式,并返回第一个成功解析的结果。如果所有格式都无法成功解析表达式,则返回错误。

#### 示例

```sql
> select to_timestamp_millis('2023-01-31T09:26:56.123456789-05:00');
+------------------------------------------------------------------+
| to_timestamp_millis(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+------------------------------------------------------------------+
| 2023-01-31T14:26:56.123                                          |
+------------------------------------------------------------------+
> select to_timestamp_millis('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+---------------------------------------------------------------------------------------------------------------+
| to_timestamp_millis(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+---------------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00.123                                                                                       |
+---------------------------------------------------------------------------------------------------------------+
```

更多示例请参见[这里](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/to_timestamp.rs)

### `to_timestamp_nanos`

将值转换为时间戳(`YYYY-MM-DDT00:00:00.000000000Z`)。支持字符串、整数和无符号整数类型作为输入。如果未提供 [Chrono 格式](https://docs.rs/chrono/latest/chrono/format/strftime/index.html),字符串将按 RFC3339 解析(例如 '2023-07-20T05:44:00')。整数和无符号整数被解释为自 UNIX 纪元(`1970-01-01T00:00:00Z`)以来的纳秒数。返回对应的时间戳。

```sql
to_timestamp_nanos(expression[, ..., format_n])
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意算术运算符的组合。
- **format_n**: 可选的 [Chrono 格式](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)字符串,用于解析表达式。将按其出现的顺序依次尝试各格式,并返回第一个成功解析的结果。如果所有格式都无法成功解析表达式,则返回错误。

#### 示例

```sql
> select to_timestamp_nanos('2023-01-31T09:26:56.123456789-05:00');
+-----------------------------------------------------------------+
| to_timestamp_nanos(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+-----------------------------------------------------------------+
| 2023-01-31T14:26:56.123456789                                   |
+-----------------------------------------------------------------+
> select to_timestamp_nanos('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+--------------------------------------------------------------------------------------------------------------+
| to_timestamp_nanos(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+--------------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00.123456789                                                                                |
+---------------------------------------------------------------------------------------------------------------+
```

更多示例请参见[这里](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/to_timestamp.rs)

### `to_timestamp_seconds`

将值转换为时间戳(`YYYY-MM-DDT00:00:00.000Z`)。支持字符串、整数和无符号整数类型作为输入。如果未提供 [Chrono 格式](https://docs.rs/chrono/latest/chrono/format/strftime/index.html),字符串将按 RFC3339 解析(例如 '2023-07-20T05:44:00')。整数和无符号整数被解释为自 UNIX 纪元(`1970-01-01T00:00:00Z`)以来的秒数。返回对应的时间戳。

```sql
to_timestamp_seconds(expression[, ..., format_n])
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意算术运算符的组合。
- **format_n**: 可选的 [Chrono 格式](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)字符串,用于解析表达式。将按其出现的顺序依次尝试各格式,并返回第一个成功解析的结果。如果所有格式都无法成功解析表达式,则返回错误。

#### 示例

```sql
> select to_timestamp_seconds('2023-01-31T09:26:56.123456789-05:00');
+-------------------------------------------------------------------+
| to_timestamp_seconds(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+-------------------------------------------------------------------+
| 2023-01-31T14:26:56                                               |
+-------------------------------------------------------------------+
> select to_timestamp_seconds('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+----------------------------------------------------------------------------------------------------------------+
| to_timestamp_seconds(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+----------------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00                                                                                            |
+----------------------------------------------------------------------------------------------------------------+
```

更多示例请参见[这里](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/to_timestamp.rs)

### `to_unixtime`

将值转换为自 UNIX 纪元(`1970-01-01T00:00:00Z`)以来的秒数。支持字符串、日期、时间戳和双精度浮点类型作为输入。如果未提供 [Chrono 格式](https://docs.rs/chrono/latest/chrono/format/strftime/index.html),字符串将按 RFC3339 解析(例如 '2023-07-20T05:44:00')。

```sql
to_unixtime(expression[, ..., format_n])
```

#### 参数

- **expression**: 要操作的表达式。可以是常量、列或函数,以及任意算术运算符的组合。
- **format_n**: 可选的 [Chrono 格式](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)字符串,用于解析表达式。将按其出现的顺序依次尝试各格式,并返回第一个成功解析的结果。如果所有格式都无法成功解析表达式,则返回错误。

#### 示例

```sql
> select to_unixtime('2020-09-08T12:00:00+00:00');
+------------------------------------------------+
| to_unixtime(Utf8("2020-09-08T12:00:00+00:00")) |
+------------------------------------------------+
| 1599566400                                     |
+------------------------------------------------+
> select to_unixtime('01-14-2023 01:01:30+05:30', '%q', '%d-%m-%Y %H/%M/%S', '%+', '%m-%d-%Y %H:%M:%S%#z');
+-----------------------------------------------------------------------------------------------------------------------------+
| to_unixtime(Utf8("01-14-2023 01:01:30+05:30"),Utf8("%q"),Utf8("%d-%m-%Y %H/%M/%S"),Utf8("%+"),Utf8("%m-%d-%Y %H:%M:%S%#z")) |
+-----------------------------------------------------------------------------------------------------------------------------+
| 1673638290                                                                                                                  |
+-----------------------------------------------------------------------------------------------------------------------------+
```

### `today`

_[current_date](#current_date) 的别名。_

## 数组函数(Array Functions)

- [array_any_value](#array_any_value)
- [array_append](#array_append)
- [array_cat](#array_cat)
- [array_concat](#array_concat)
- [array_contains](#array_contains)
- [array_dims](#array_dims)
- [array_distance](#array_distance)
- [array_distinct](#array_distinct)
- [array_element](#array_element)
- [array_empty](#array_empty)
- [array_except](#array_except)
- [array_extract](#array_extract)
- [array_has](#array_has)
- [array_has_all](#array_has_all)
- [array_has_any](#array_has_any)
- [array_indexof](#array_indexof)
- [array_intersect](#array_intersect)
- [array_join](#array_join)
- [array_length](#array_length)
- [array_max](#array_max)
- [array_ndims](#array_ndims)
- [array_pop_back](#array_pop_back)
- [array_pop_front](#array_pop_front)
- [array_position](#array_position)
- [array_positions](#array_positions)
- [array_prepend](#array_prepend)
- [array_push_back](#array_push_back)
- [array_push_front](#array_push_front)
- [array_remove](#array_remove)
- [array_remove_all](#array_remove_all)
- [array_remove_n](#array_remove_n)
- [array_repeat](#array_repeat)
- [array_replace](#array_replace)
- [array_replace_all](#array_replace_all)
- [array_replace_n](#array_replace_n)
- [array_resize](#array_resize)
- [array_reverse](#array_reverse)
- [array_slice](#array_slice)
- [array_sort](#array_sort)
- [array_to_string](#array_to_string)
- [array_union](#array_union)
- [arrays_overlap](#arrays_overlap)
- [cardinality](#cardinality)
- [empty](#empty)
- [flatten](#flatten)
- [generate_series](#generate_series)
- [list_any_value](#list_any_value)
- [list_append](#list_append)
- [list_cat](#list_cat)
- [list_concat](#list_concat)
- [list_contains](#list_contains)
- [list_dims](#list_dims)
- [list_distance](#list_distance)
- [list_distinct](#list_distinct)
- [list_element](#list_element)
- [list_empty](#list_empty)
- [list_except](#list_except)
- [list_extract](#list_extract)
- [list_has](#list_has)
- [list_has_all](#list_has_all)
- [list_has_any](#list_has_any)
- [list_indexof](#list_indexof)
- [list_intersect](#list_intersect)
- [list_join](#list_join)
- [list_length](#list_length)
- [list_max](#list_max)
- [list_ndims](#list_ndims)
- [list_pop_back](#list_pop_back)
- [list_pop_front](#list_pop_front)
- [list_position](#list_position)
- [list_positions](#list_positions)
- [list_prepend](#list_prepend)
- [list_push_back](#list_push_back)
- [list_push_front](#list_push_front)
- [list_remove](#list_remove)
- [list_remove_all](#list_remove_all)
- [list_remove_n](#list_remove_n)
- [list_repeat](#list_repeat)
- [list_replace](#list_replace)
- [list_replace_all](#list_replace_all)
- [list_replace_n](#list_replace_n)
- [list_resize](#list_resize)
- [list_reverse](#list_reverse)
- [list_slice](#list_slice)
- [list_sort](#list_sort)
- [list_to_string](#list_to_string)
- [list_union](#list_union)
- [make_array](#make_array)
- [make_list](#make_list)
- [range](#range)
- [string_to_array](#string_to_array)
- [string_to_list](#string_to_list)

### `array_any_value`

返回数组中第一个非 null 元素。

```sql
array_any_value(array)
```

#### 参数

- **array**: 数组表达式(array expression)。可以是常量、列或函数,以及任意数组运算符的组合。

#### 示例

```sql
> select array_any_value([NULL, 1, 2, 3]);
+-------------------------------+
| array_any_value(List([NULL,1,2,3])) |
+-------------------------------------+
| 1                                   |
+-------------------------------------+
```

#### 别名

- list_any_value

### `array_append`

将元素追加到数组末尾。

```sql
array_append(array, element)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **element**: 要追加到数组中的元素。

#### 示例

```sql
> select array_append([1, 2, 3], 4);
+--------------------------------------+
| array_append(List([1,2,3]),Int64(4)) |
+--------------------------------------+
| [1, 2, 3, 4]                         |
+--------------------------------------+
```

#### 别名

- list_append
- array_push_back
- list_push_back

### `array_cat`

_[array_concat](#array_concat) 的别名。_

### `array_concat`

拼接多个数组。

```sql
array_concat(array[, ..., array_n])
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **array_n**: 要拼接的后续数组列或字面量数组。

#### 示例

```sql
> select array_concat([1, 2], [3, 4], [5, 6]);
+---------------------------------------------------+
| array_concat(List([1,2]),List([3,4]),List([5,6])) |
+---------------------------------------------------+
| [1, 2, 3, 4, 5, 6]                                |
+---------------------------------------------------+
```

#### 别名

- array_cat
- list_concat
- list_cat

### `array_contains`

_[array_has](#array_has) 的别名。_

### `array_dims`

返回由数组各维度组成的数组。

```sql
array_dims(array)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。

#### 示例

```sql
> select array_dims([[1, 2, 3], [4, 5, 6]]);
+---------------------------------+
| array_dims(List([1,2,3,4,5,6])) |
+---------------------------------+
| [2, 3]                          |
+---------------------------------+
```

#### 别名

- list_dims

### `array_distance`

返回两个等长输入数组之间的欧几里得距离。

```sql
array_distance(array1, array2)
```

#### 参数

- **array1**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **array2**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。

#### 示例

```sql
> select array_distance([1, 2], [1, 4]);
+------------------------------------+
| array_distance(List([1,2], [1,4])) |
+------------------------------------+
| 2.0                                |
+------------------------------------+
```

#### 别名

- list_distance

### `array_distinct`

去除重复项后,返回数组中的去重值。

```sql
array_distinct(array)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。

#### 示例

```sql
> select array_distinct([1, 3, 2, 3, 1, 2, 4]);
+---------------------------------+
| array_distinct(List([1,2,3,4])) |
+---------------------------------+
| [1, 2, 3, 4]                    |
+---------------------------------+
```

#### 别名

- list_distinct

### `array_element`

从数组中提取索引为 n 的元素。

```sql
array_element(array, index)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **index**: 要从数组中提取元素的索引。

#### 示例

```sql
> select array_element([1, 2, 3, 4], 3);
+-----------------------------------------+
| array_element(List([1,2,3,4]),Int64(3)) |
+-----------------------------------------+
| 3                                       |
+-----------------------------------------+
```

#### 别名

- array_extract
- list_element
- list_extract

### `array_empty`

_[empty](#empty) 的别名。_

### `array_except`

返回出现在第一个数组中但未出现在第二个数组中的元素组成的数组。

```sql
array_except(array1, array2)
```

#### 参数

- **array1**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **array2**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。

#### 示例

```sql
> select array_except([1, 2, 3, 4], [5, 6, 3, 4]);
+----------------------------------------------------+
| array_except([1, 2, 3, 4], [5, 6, 3, 4]);           |
+----------------------------------------------------+
| [1, 2]                                              |
+----------------------------------------------------+
> select array_except([1, 2, 3, 4], [3, 4, 5, 6]);
+----------------------------------------------------+
| array_except([1, 2, 3, 4], [3, 4, 5, 6]);           |
+----------------------------------------------------+
| [1, 2]                                              |
+----------------------------------------------------+
```

#### 别名

- list_except

### `array_extract`

_[array_element](#array_element) 的别名。_

### `array_has`

如果数组包含该元素则返回 true。

```sql
array_has(array, element)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **element**: 标量或数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。

#### 示例

```sql
> select array_has([1, 2, 3], 2);
+-----------------------------+
| array_has(List([1,2,3]), 2) |
+-----------------------------+
| true                        |
+-----------------------------+
```

#### 别名

- list_has
- array_contains
- list_contains

### `array_has_all`

如果子数组的所有元素都存在于数组中则返回 true。

```sql
array_has_all(array, sub-array)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **sub-array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。

#### 示例

```sql
> select array_has_all([1, 2, 3, 4], [2, 3]);
+--------------------------------------------+
| array_has_all(List([1,2,3,4]), List([2,3])) |
+--------------------------------------------+
| true                                       |
+--------------------------------------------+
```

#### 别名

- list_has_all

### `array_has_any`

如果两个数组中存在任何相同元素则返回 true。

```sql
array_has_any(array, sub-array)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **sub-array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。

#### 示例

```sql
> select array_has_any([1, 2, 3], [3, 4]);
+------------------------------------------+
| array_has_any(List([1,2,3]), List([3,4])) |
+------------------------------------------+
| true                                     |
+------------------------------------------+
```

#### 别名

- list_has_any
- arrays_overlap

### `array_indexof`

_[array_position](#array_position) 的别名。_

### `array_intersect`

返回由 array1 和 array2 交集元素组成的数组。

```sql
array_intersect(array1, array2)
```

#### 参数

- **array1**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **array2**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。

#### 示例

```sql
> select array_intersect([1, 2, 3, 4], [5, 6, 3, 4]);
+----------------------------------------------------+
| array_intersect([1, 2, 3, 4], [5, 6, 3, 4]);       |
+----------------------------------------------------+
| [3, 4]                                             |
+----------------------------------------------------+
> select array_intersect([1, 2, 3, 4], [5, 6, 7, 8]);
+----------------------------------------------------+
| array_intersect([1, 2, 3, 4], [5, 6, 7, 8]);       |
+----------------------------------------------------+
| []                                                 |
+----------------------------------------------------+
```

#### 别名

- list_intersect

### `array_join`

_[array_to_string](#array_to_string) 的别名。_

### `array_length`

返回数组维度的长度。

```sql
array_length(array, dimension)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **dimension**: 数组维度。

#### 示例

```sql
> select array_length([1, 2, 3, 4, 5], 1);
+-------------------------------------------+
| array_length(List([1,2,3,4,5]), 1)        |
+-------------------------------------------+
| 5                                         |
+-------------------------------------------+
```

#### 别名

- list_length

### `array_max`

返回数组中的最大值。

```sql
array_max(array)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。

#### 示例

```sql
> select array_max([3,1,4,2]);
+-----------------------------------------+
| array_max(List([3,1,4,2]))              |
+-----------------------------------------+
| 4                                       |
+-----------------------------------------+
```

#### 别名

- list_max

### `array_ndims`

返回数组的维度数。

```sql
array_ndims(array, element)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **element**: 数组元素。

#### 示例

```sql
> select array_ndims([[1, 2, 3], [4, 5, 6]]);
+----------------------------------+
| array_ndims(List([1,2,3,4,5,6])) |
+----------------------------------+
| 2                                |
+----------------------------------+
```

#### 别名

- list_ndims

### `array_pop_back`

返回去掉最后一个元素后的数组。

```sql
array_pop_back(array)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。

#### 示例

```sql
> select array_pop_back([1, 2, 3]);
+-------------------------------+
| array_pop_back(List([1,2,3])) |
+-------------------------------+
| [1, 2]                        |
+-------------------------------+
```

#### 别名

- list_pop_back

### `array_pop_front`

返回去掉第一个元素后的数组。

```sql
array_pop_front(array)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。

#### 示例

```sql
> select array_pop_front([1, 2, 3]);
+-------------------------------+
| array_pop_front(List([1,2,3])) |
+-------------------------------+
| [2, 3]                        |
+-------------------------------+
```

#### 别名

- list_pop_front

### `array_position`

返回指定元素在数组中首次出现的位置。

```sql
array_position(array, element)
array_position(array, element, index)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **element**: 要在数组中搜索位置的元素。
- **index**: 开始搜索的索引。

#### 示例

```sql
> select array_position([1, 2, 2, 3, 1, 4], 2);
+----------------------------------------------+
| array_position(List([1,2,2,3,1,4]),Int64(2)) |
+----------------------------------------------+
| 2                                            |
+----------------------------------------------+
> select array_position([1, 2, 2, 3, 1, 4], 2, 3);
+----------------------------------------------------+
| array_position(List([1,2,2,3,1,4]),Int64(2), Int64(3)) |
+----------------------------------------------------+
| 3                                                  |
+----------------------------------------------------+
```

#### 别名

- list_position
- array_indexof
- list_indexof

### `array_positions`

在数组中搜索元素,返回所有出现的位置。

```sql
array_positions(array, element)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **element**: 要在数组中搜索位置的元素。

#### 示例

```sql
> select array_positions([1, 2, 2, 3, 1, 4], 2);
+-----------------------------------------------+
| array_positions(List([1,2,2,3,1,4]),Int64(2)) |
+-----------------------------------------------+
| [2, 3]                                        |
+-----------------------------------------------+
```

#### 别名

- list_positions

### `array_prepend`

将元素添加到数组开头。

```sql
array_prepend(element, array)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **element**: 要添加到数组开头的元素。

#### 示例

```sql
> select array_prepend(1, [2, 3, 4]);
+---------------------------------------+
| array_prepend(Int64(1),List([2,3,4])) |
+---------------------------------------+
| [1, 2, 3, 4]                          |
+---------------------------------------+
```

#### 别名

- list_prepend
- array_push_front
- list_push_front

### `array_push_back`

_[array_append](#array_append) 的别名。_

### `array_push_front`

_[array_prepend](#array_prepend) 的别名。_

### `array_remove`

从数组中移除第一个等于给定值的元素。

```sql
array_remove(array, element)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **element**: 要从数组中移除的元素。

#### 示例

```sql
> select array_remove([1, 2, 2, 3, 2, 1, 4], 2);
+----------------------------------------------+
| array_remove(List([1,2,2,3,2,1,4]),Int64(2)) |
+----------------------------------------------+
| [1, 2, 3, 2, 1, 4]                           |
+----------------------------------------------+
```

#### 别名

- list_remove

### `array_remove_all`

从数组中移除所有等于给定值的元素。

```sql
array_remove_all(array, element)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **element**: 要从数组中移除的元素。

#### 示例

```sql
> select array_remove_all([1, 2, 2, 3, 2, 1, 4], 2);
+--------------------------------------------------+
| array_remove_all(List([1,2,2,3,2,1,4]),Int64(2)) |
+--------------------------------------------------+
| [1, 3, 1, 4]                                     |
+--------------------------------------------------+
```

#### 别名

- list_remove_all

### `array_remove_n`

从数组中移除前 `max` 个等于给定值的元素。

```sql
array_remove_n(array, element, max))
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **element**: 要从数组中移除的元素。
- **max**: 要移除的首次出现次数。

#### 示例

```sql
> select array_remove_n([1, 2, 2, 3, 2, 1, 4], 2, 2);
+---------------------------------------------------------+
| array_remove_n(List([1,2,2,3,2,1,4]),Int64(2),Int64(2)) |
+---------------------------------------------------------+
| [1, 3, 2, 1, 4]                                         |
+---------------------------------------------------------+
```

#### 别名

- list_remove_n

### `array_repeat`

返回包含元素 `count` 次的数组。

```sql
array_repeat(element, count)
```

#### 参数

- **element**: 元素表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **count**: 元素重复次数的值。

#### 示例

```sql
> select array_repeat(1, 3);
+---------------------------------+
| array_repeat(Int64(1),Int64(3)) |
+---------------------------------+
| [1, 1, 1]                       |
+---------------------------------+
> select array_repeat([1, 2], 2);
+------------------------------------+
| array_repeat(List([1,2]),Int64(2)) |
+------------------------------------+
| [[1, 2], [1, 2]]                   |
+------------------------------------+
```

#### 别名

- list_repeat

### `array_replace`

将指定元素的首次出现替换为另一个指定元素。

```sql
array_replace(array, from, to)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **from**: 初始元素。
- **to**: 目标元素。

#### 示例

```sql
> select array_replace([1, 2, 2, 3, 2, 1, 4], 2, 5);
+--------------------------------------------------------+
| array_replace(List([1,2,2,3,2,1,4]),Int64(2),Int64(5)) |
+--------------------------------------------------------+
| [1, 5, 2, 3, 2, 1, 4]                                  |
+--------------------------------------------------------+
```

#### 别名

- list_replace

### `array_replace_all`

将指定元素的所有出现替换为另一个指定元素。

```sql
array_replace_all(array, from, to)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **from**: 初始元素。
- **to**: 目标元素。

#### 示例

```sql
> select array_replace_all([1, 2, 2, 3, 2, 1, 4], 2, 5);
+------------------------------------------------------------+
| array_replace_all(List([1,2,2,3,2,1,4]),Int64(2),Int64(5)) |
+------------------------------------------------------------+
| [1, 5, 5, 3, 5, 1, 4]                                      |
+------------------------------------------------------------+
```

#### 别名

- list_replace_all

### `array_replace_n`

将指定元素的前 `max` 次出现替换为另一个指定元素。

```sql
array_replace_n(array, from, to, max)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **from**: 初始元素。
- **to**: 目标元素。
- **max**: 要替换的首次出现次数。

#### 示例

```sql
> select array_replace_n([1, 2, 2, 3, 2, 1, 4], 2, 5, 2);
+-------------------------------------------------------------------+
| array_replace_n(List([1,2,2,3,2,1,4]),Int64(2),Int64(5),Int64(2)) |
+-------------------------------------------------------------------+
| [1, 5, 5, 3, 2, 1, 4]                                             |
+-------------------------------------------------------------------+
```

#### 别名

- list_replace_n

### `array_resize`

将列表调整为包含 size 个元素。新元素用 value 初始化,如果未设置 value 则为空。

```sql
array_resize(array, size, value)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **size**: 给定数组的新大小。
- **value**: 定义新元素的值,如果未设置则为空。

#### 示例

```sql
> select array_resize([1, 2, 3], 5, 0);
+-------------------------------------+
| array_resize(List([1,2,3],5,0))     |
+-------------------------------------+
| [1, 2, 3, 0, 0]                     |
+-------------------------------------+
```

#### 别名

- list_resize

### `array_reverse`

返回元素顺序反转后的数组。

```sql
array_reverse(array)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。

#### 示例

```sql
> select array_reverse([1, 2, 3, 4]);
+------------------------------------------------------------+
| array_reverse(List([1, 2, 3, 4]))                          |
+------------------------------------------------------------+
| [4, 3, 2, 1]                                               |
+------------------------------------------------------------+
```

#### 别名

- list_reverse

### `array_slice`

基于从 1 开始的起止位置返回数组的切片。

```sql
array_slice(array, begin, end)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **begin**: 第一个元素的索引。如果为负数,则从数组末尾向前计数。
- **end**: 最后一个元素的索引。如果为负数,则从数组末尾向前计数。
- **stride**: 数组切片的步长。默认值为 1。

#### 示例

```sql
> select array_slice([1, 2, 3, 4, 5, 6, 7, 8], 3, 6);
+--------------------------------------------------------+
| array_slice(List([1,2,3,4,5,6,7,8]),Int64(3),Int64(6)) |
+--------------------------------------------------------+
| [3, 4, 5, 6]                                           |
+--------------------------------------------------------+
```

#### 别名

- list_slice

### `array_sort`

对数组排序。

```sql
array_sort(array, desc, nulls_first)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **desc**: 是否按降序排序(`ASC` 或 `DESC`)。
- **nulls_first**: 是否将 null 排在前面(`NULLS FIRST` 或 `NULLS LAST`)。

#### 示例

```sql
> select array_sort([3, 1, 2]);
+-----------------------------+
| array_sort(List([3,1,2]))   |
+-----------------------------+
| [1, 2, 3]                   |
+-----------------------------+
```

#### 别名

- list_sort

### `array_to_string`

将每个元素转换为其文本表示。

```sql
array_to_string(array, delimiter[, null_string])
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **delimiter**: 数组元素分隔符。
- **null_string**: 可选。用于替换数组中 null 值的字符串。如果未提供,null 将按默认行为处理。

#### 示例

```sql
> select array_to_string([[1, 2, 3, 4], [5, 6, 7, 8]], ',');
+----------------------------------------------------+
| array_to_string(List([1,2,3,4,5,6,7,8]),Utf8(",")) |
+----------------------------------------------------+
| 1,2,3,4,5,6,7,8                                    |
+----------------------------------------------------+
```

#### 别名

- list_to_string
- array_join
- list_join

### `array_union`

返回同时出现在两个数组中的元素组成的数组(两个数组的所有元素),不含重复项。

```sql
array_union(array1, array2)
```

#### 参数

- **array1**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。
- **array2**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。

#### 示例

```sql
> select array_union([1, 2, 3, 4], [5, 6, 3, 4]);
+----------------------------------------------------+
| array_union([1, 2, 3, 4], [5, 6, 3, 4]);           |
+----------------------------------------------------+
| [1, 2, 3, 4, 5, 6]                                 |
+----------------------------------------------------+
> select array_union([1, 2, 3, 4], [5, 6, 7, 8]);
+----------------------------------------------------+
| array_union([1, 2, 3, 4], [5, 6, 7, 8]);           |
+----------------------------------------------------+
| [1, 2, 3, 4, 5, 6, 7, 8]                           |
+----------------------------------------------------+
```

#### 别名

- list_union

### `arrays_overlap`

_[array_has_any](#array_has_any) 的别名。_

### `cardinality`

返回数组中元素的总数。

```sql
cardinality(array)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。

#### 示例

```sql
> select cardinality([[1, 2, 3, 4], [5, 6, 7, 8]]);
+--------------------------------------+
| cardinality(List([1,2,3,4,5,6,7,8])) |
+--------------------------------------+
| 8                                    |
+--------------------------------------+
```

### `empty`

空数组返回 1,非空数组返回 0。

```sql
empty(array)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。

#### 示例

```sql
> select empty([1]);
+------------------+
| empty(List([1])) |
+------------------+
| 0                |
+------------------+
```

#### 别名

- array_empty
- list_empty

### `flatten`

将嵌套数组转换为扁平数组。

- 适用于任意深度的嵌套数组
- 不改变已经是扁平的数组

扁平化后的数组包含来自所有源数组的全部元素。

```sql
flatten(array)
```

#### 参数

- **array**: 数组表达式。可以是常量、列或函数,以及任意数组运算符的组合。

#### 示例

```sql
> select flatten([[1, 2], [3, 4]]);
+------------------------------+
| flatten(List([1,2], [3,4]))  |
+------------------------------+
| [1, 2, 3, 4]                 |
+------------------------------+
```

### `generate_series`

与 range 函数类似,但包含上界。

```sql
generate_series(start, stop, step)
```

#### 参数

- **start**: 序列的起点。支持整数、时间戳、日期或可强制转换为 Date32 的字符串类型。
- **end**: 序列的终点(包含)。类型必须与 start 相同。
- **step**: 按 step 递增(不能为 0)。只有时间戳范围才支持小于一天的步长。

#### 示例

```sql
> select generate_series(1,3);
+------------------------------------+
| generate_series(Int64(1),Int64(3)) |
+------------------------------------+
| [1, 2, 3]                          |
+------------------------------------+
```

### `list_any_value`

_[array_any_value](#array_any_value) 的别名。_

### `list_append`

_[array_append](#array_append) 的别名。_

### `list_cat`

_[array_concat](#array_concat) 的别名。_

### `list_concat`

_[array_concat](#array_concat) 的别名。_

### `list_contains`

_[array_has](#array_has) 的别名。_

### `list_dims`

_[array_dims](#array_dims) 的别名。_

### `list_distance`

_[array_distance](#array_distance) 的别名。_

### `list_distinct`

_[array_distinct](#array_distinct) 的别名。_

### `list_element`

_[array_element](#array_element) 的别名。_

### `list_empty`

_[empty](#empty) 的别名。_

### `list_except`

_[array_except](#array_except) 的别名。_

### `list_extract`

_[array_element](#array_element) 的别名。_

### `list_has`

_[array_has](#array_has) 的别名。_

### `list_has_all`

_[array_has_all](#array_has_all) 的别名。_

### `list_has_any`

_[array_has_any](#array_has_any) 的别名。_

### `list_indexof`

_[array_position](#array_position) 的别名。_

### `list_intersect`

_[array_intersect](#array_intersect) 的别名。_

### `list_join`

_[array_to_string](#array_to_string) 的别名。_

### `list_length`

_[array_length](#array_length) 的别名。_

### `list_max`

_[array_max](#array_max) 的别名。_

### `list_ndims`

_[array_ndims](#array_ndims) 的别名。_

### `list_pop_back`

_[array_pop_back](#array_pop_back) 的别名。_

### `list_pop_front`

_[array_pop_front](#array_pop_front) 的别名。_

### `list_position`

_[array_position](#array_position) 的别名。_

### `list_positions`

_[array_positions](#array_positions) 的别名。_

### `list_prepend`

_[array_prepend](#array_prepend) 的别名。_

### `list_push_back`

_[array_append](#array_append) 的别名。_

### `list_push_front`

_[array_prepend](#array_prepend) 的别名。_

### `list_remove`

_[array_remove](#array_remove) 的别名。_

### `list_remove_all`

_[array_remove_all](#array_remove_all) 的别名。_

### `list_remove_n`

_[array_remove_n](#array_remove_n) 的别名。_

### `list_repeat`

_[array_repeat](#array_repeat) 的别名。_

### `list_replace`

_[array_replace](#array_replace) 的别名。_

### `list_replace_all`

_[array_replace_all](#array_replace_all) 的别名。_

### `list_replace_n`

_[array_replace_n](#array_replace_n) 的别名。_

### `list_resize`

_[array_resize](#array_resize) 的别名。_

### `list_reverse`

_[array_reverse](#array_reverse) 的别名。_

### `list_slice`

_[array_slice](#array_slice) 的别名。_

### `list_sort`

_[array_sort](#array_sort) 的别名。_

### `list_to_string`

_[array_to_string](#array_to_string) 的别名。_

### `list_union`

_[array_union](#array_union) 的别名。_

### `make_array`

使用指定的输入表达式构造一个数组。

```sql
make_array(expression1[, ..., expression_n])
```

#### 参数

- **expression_n**: 要包含在输出数组中的表达式。可以是常量、列或函数,以及任意算术或字符串运算符的组合。

#### 示例

```sql
> select make_array(1, 2, 3, 4, 5);
+----------------------------------------------------------+
| make_array(Int64(1),Int64(2),Int64(3),Int64(4),Int64(5)) |
+----------------------------------------------------------+
| [1, 2, 3, 4, 5]                                          |
+----------------------------------------------------------+
```

#### 别名

- make_list

### `make_list`

_[make_array](#make_array) 的别名。_

### `range`

返回 start 与 stop 之间、按 step 递增的 Arrow 数组。范围 start..end 包含所有满足 start `<=` x `<` end 的值。如果 start `>=` end,则结果为空。step 不能为 0。

```sql
range(start, stop, step)
```

#### 参数

- **start**: 范围的起点。支持整数、时间戳、日期或可强制转换为 Date32 的字符串类型。
- **end**: 范围的终点(不包含)。类型必须与 start 相同。
- **step**: 按 step 递增(不能为 0)。只有时间戳范围才支持小于一天的步长。

#### 示例

```sql
> select range(2, 10, 3);
+-----------------------------------+
| range(Int64(2),Int64(10),Int64(3))|
+-----------------------------------+
| [2, 5, 8]                         |
+-----------------------------------+

> select range(DATE '1992-09-01', DATE '1993-03-01', INTERVAL '1' MONTH);
+--------------------------------------------------------------+
| range(DATE '1992-09-01', DATE '1993-03-01', INTERVAL '1' MONTH) |
+--------------------------------------------------------------+
| [1992-09-01, 1992-10-01, 1992-11-01, 1992-12-01, 1993-01-01, 1993-02-01] |
+--------------------------------------------------------------+
```

### `string_to_array`

根据分隔符将字符串拆分为子字符串数组。与可选参数 `null_str` 匹配的子字符串会被替换为 NULL。

```sql
string_to_array(str, delimiter[, null_str])
```

#### 参数

- **str**: 要拆分的字符串表达式。
- **delimiter**: 用于拆分的分隔符字符串。
- **null_str**: 要替换为 `NULL` 的子字符串值。

#### 示例

```sql
> select string_to_array('abc##def', '##');
+-----------------------------------+
| string_to_array(Utf8('abc##def'))  |
+-----------------------------------+
| ['abc', 'def']                    |
+-----------------------------------+
> select string_to_array('abc def', ' ', 'def');
+---------------------------------------------+
| string_to_array(Utf8('abc def'), Utf8(' '), Utf8('def')) |
+---------------------------------------------+
| ['abc', NULL]                               |
+---------------------------------------------+
```

#### 别名

- string_to_list

### `string_to_list`

_[string_to_array](#string_to_array) 的别名。_

## 结构体函数(Struct Functions)

- [named_struct](#named_struct)
- [row](#row)
- [struct](#struct)

### `named_struct`

使用指定的名称和输入表达式对构造一个 Arrow 结构体(struct)。

```sql
named_struct(expression1_name, expression1_input[, ..., expression_n_name, expression_n_input])
```

#### 参数

- **expression_n_name**: 列字段的名称。必须是常量字符串。
- **expression_n_input**: 要包含在输出结构体中的表达式。可以是常量、列或函数,以及任意算术或字符串运算符的组合。

#### 示例

例如,下面的查询将两列 `a` 和 `b` 转换为一个包含结构体类型字段 `field_a` 和 `field_b` 的单列:

```sql
> select * from t;
+---+---+
| a | b |
+---+---+
| 1 | 2 |
| 3 | 4 |
+---+---+
> select named_struct('field_a', a, 'field_b', b) from t;
+-------------------------------------------------------+
| named_struct(Utf8("field_a"),t.a,Utf8("field_b"),t.b) |
+-------------------------------------------------------+
| {field_a: 1, field_b: 2}                              |
| {field_a: 3, field_b: 4}                              |
+-------------------------------------------------------+
```

### `row`

_[struct](#struct) 的别名。_

### `struct`

使用指定的输入表达式构造一个 Arrow 结构体,可选择为其命名。
返回的结构体中的字段使用可选名称或 `cN` 命名约定。
例如:`c0`、`c1`、`c2` 等。

```sql
struct(expression1[, ..., expression_n])
```

#### 参数

- **expression1, expression_n**: 要包含在输出结构体中的表达式。可以是常量、列或函数,以及任意算术或字符串运算符的组合。

#### 示例

例如,下面的查询将两列 `a` 和 `b` 转换为一个包含结构体类型字段 `field_a` 和 `c1` 的单列:

```sql
> select * from t;
+---+---+
| a | b |
+---+---+
| 1 | 2 |
| 3 | 4 |
+---+---+

-- use default names `c0`, `c1`
> select struct(a, b) from t;
+-----------------+
| struct(t.a,t.b) |
+-----------------+
| {c0: 1, c1: 2}  |
| {c0: 3, c1: 4}  |
+-----------------+

-- name the first field `field_a`
select struct(a as field_a, b) from t;
+--------------------------------------------------+
| named_struct(Utf8("field_a"),t.a,Utf8("c1"),t.b) |
+--------------------------------------------------+
| {field_a: 1, c1: 2}                              |
| {field_a: 3, c1: 4}                              |
+--------------------------------------------------+
```

#### 别名

- row

## 映射函数(Map Functions)

- [element_at](#element_at)
- [map](#map)
- [map_extract](#map_extract)
- [map_keys](#map_keys)
- [map_values](#map_values)

### `element_at`

_[map_extract](#map_extract) 的别名。_

### `map`

返回包含指定键值对的 Arrow 映射(map)。

`make_map` 函数从两个列表创建映射:一个作为键,一个作为值。每个键必须唯一且非 null。

```sql
map(key, value)
map(key: value)
make_map(['key1', 'key2'], ['value1', 'value2'])
```

#### 参数

- **key**: 对于 `map`:用作键的表达式。可以是常量、列、函数,或任意算术或字符串运算符的组合。
  对于 `make_map`:要在映射中使用的键列表。每个键必须唯一且非 null。
- **value**: 对于 `map`:用作值的表达式。可以是常量、列、函数,或任意算术或字符串运算符的组合。
  对于 `make_map`:要映射到相应键的值列表。

#### 示例

```sql
-- Using map function
SELECT MAP('type', 'test');
----
{type: test}

SELECT MAP(['POST', 'HEAD', 'PATCH'], [41, 33, null]);
----
{POST: 41, HEAD: 33, PATCH: NULL}

SELECT MAP([[1,2], [3,4]], ['a', 'b']);
----
{[1, 2]: a, [3, 4]: b}

SELECT MAP { 'a': 1, 'b': 2 };
----
{a: 1, b: 2}

-- Using make_map function
SELECT MAKE_MAP(['POST', 'HEAD'], [41, 33]);
----
{POST: 41, HEAD: 33}

SELECT MAKE_MAP(['key1', 'key2'], ['value1', null]);
----
{key1: value1, key2: }
```

### `map_extract`

返回包含给定键对应值的列表;如果映射中不存在该键,则返回空列表。

```sql
map_extract(map, key)
```

#### 参数

- **map**: 映射表达式(map expression)。可以是常量、列或函数,以及任意映射运算符的组合。
- **key**: 要从映射中提取的键。可以是常量、列或函数、任意算术或字符串运算符的组合,或是上述表达式的命名表达式。

#### 示例

```sql
SELECT map_extract(MAP {'a': 1, 'b': NULL, 'c': 3}, 'a');
----
[1]

SELECT map_extract(MAP {1: 'one', 2: 'two'}, 2);
----
['two']

SELECT map_extract(MAP {'x': 10, 'y': NULL, 'z': 30}, 'y');
----
[]
```

#### 别名

- element_at

### `map_keys`

返回映射中所有键组成的列表。

```sql
map_keys(map)
```

#### 参数

- **map**: 映射表达式。可以是常量、列或函数,以及任意映射运算符的组合。

#### 示例

```sql
SELECT map_keys(MAP {'a': 1, 'b': NULL, 'c': 3});
----
[a, b, c]

SELECT map_keys(map([100, 5], [42, 43]));
----
[100, 5]
```

### `map_values`

返回映射中所有值组成的列表。

```sql
map_values(map)
```

#### 参数

- **map**: 映射表达式。可以是常量、列或函数,以及任意映射运算符的组合。

#### 示例

```sql
SELECT map_values(MAP {'a': 1, 'b': NULL, 'c': 3});
----
[1, , 3]

SELECT map_values(map([100, 5], [42, 43]));
----
[42, 43]
```

## 哈希函数(Hashing Functions)

- [digest](#digest)
- [md5](#md5)
- [sha224](#sha224)
- [sha256](#sha256)
- [sha384](#sha384)
- [sha512](#sha512)

### `digest`

使用指定算法计算表达式的二进制哈希值。

```sql
digest(expression, algorithm)
```

#### 参数

- **expression**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。
- **algorithm**: 指定所用算法的字符串表达式。必须是以下之一:
    - md5
    - sha224
    - sha256
    - sha384
    - sha512
    - blake2s
    - blake2b
    - blake3

#### 示例

```sql
> select digest('foo', 'sha256');
+------------------------------------------+
| digest(Utf8("foo"), Utf8("sha256"))      |
+------------------------------------------+
| <binary_hash_result>                     |
+------------------------------------------+
```

### `md5`

为字符串表达式计算 MD5 128 位校验和。

```sql
md5(expression)
```

#### 参数

- **expression**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select md5('foo');
+-------------------------------------+
| md5(Utf8("foo"))                    |
+-------------------------------------+
| <md5_checksum_result>               |
+-------------------------------------+
```

### `sha224`

计算二进制字符串的 SHA-224 哈希值。

```sql
sha224(expression)
```

#### 参数

- **expression**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select sha224('foo');
+------------------------------------------+
| sha224(Utf8("foo"))                      |
+------------------------------------------+
| <sha224_hash_result>                     |
+------------------------------------------+
```

### `sha256`

计算二进制字符串的 SHA-256 哈希值。

```sql
sha256(expression)
```

#### 参数

- **expression**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select sha256('foo');
+--------------------------------------+
| sha256(Utf8("foo"))                  |
+--------------------------------------+
| <sha256_hash_result>                 |
+--------------------------------------+
```

### `sha384`

计算二进制字符串的 SHA-384 哈希值。

```sql
sha384(expression)
```

#### 参数

- **expression**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select sha384('foo');
+-----------------------------------------+
| sha384(Utf8("foo"))                     |
+-----------------------------------------+
| <sha384_hash_result>                    |
+-----------------------------------------+
```

### `sha512`

计算二进制字符串的 SHA-512 哈希值。

```sql
sha512(expression)
```

#### 参数

- **expression**: 要操作的字符串表达式。可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select sha512('foo');
+-------------------------------------------+
| sha512(Utf8("foo"))                       |
+-------------------------------------------+
| <sha512_hash_result>                      |
+-------------------------------------------+
```

## 联合函数(Union Functions)

用于处理 union 数据类型的函数,该类型也称为带标签联合(tagged union)、变体类型(variant type)、枚举或和类型(sum type)。注意:与 SQL UNION 运算符无关

- [union_extract](#union_extract)

### `union_extract`

当该字段被选中时返回 union 中给定字段的值,否则返回 NULL。

```sql
union_extract(union, field_name)
```

#### 参数

- **union**: 要操作的 union 表达式。可以是常量、列或函数,以及任意运算符的组合。
- **field_name**: 要操作的字符串表达式。必须是常量。

#### 示例

```sql
❯ select union_column, union_extract(union_column, 'a'), union_extract(union_column, 'b') from table_with_union;
+--------------+----------------------------------+----------------------------------+
| union_column | union_extract(union_column, 'a') | union_extract(union_column, 'b') |
+--------------+----------------------------------+----------------------------------+
| {a=1}        | 1                                |                                  |
| {b=3.0}      |                                  | 3.0                              |
| {a=4}        | 4                                |                                  |
| {b=}         |                                  |                                  |
| {a=}         |                                  |                                  |
+--------------+----------------------------------+----------------------------------+
```

## 其他函数(Other Functions)

- [arrow_cast](#arrow_cast)
- [arrow_typeof](#arrow_typeof)
- [get_field](#get_field)

### `arrow_cast`

将值转换为特定的 Arrow 数据类型(data type)。

```sql
arrow_cast(expression, datatype)
```

#### 参数

- **expression**: 要转换的表达式。该表达式可以是常量、列或函数,以及任意运算符的组合。
- **datatype**: 要转换成的 [Arrow 数据类型](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html)名称,以字符串形式给出。格式与 [`arrow_typeof`] 返回的格式相同。

#### 示例

```sql
> select arrow_cast(-5, 'Int8') as a,
  arrow_cast('foo', 'Dictionary(Int32, Utf8)') as b,
  arrow_cast('bar', 'LargeUtf8') as c,
  arrow_cast('2023-01-02T12:53:02', 'Timestamp(Microsecond, Some("+08:00"))') as d
  ;
+----+-----+-----+---------------------------+
| a  | b   | c   | d                         |
+----+-----+-----+---------------------------+
| -5 | foo | bar | 2023-01-02T12:53:02+08:00 |
+----+-----+-----+---------------------------+
```

### `arrow_typeof`

返回表达式底层 [Arrow 数据类型](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html)的名称。

```sql
arrow_typeof(expression)
```

#### 参数

- **expression**: 要求值的表达式。该表达式可以是常量、列或函数,以及任意运算符的组合。

#### 示例

```sql
> select arrow_typeof('foo'), arrow_typeof(1);
+---------------------------+------------------------+
| arrow_typeof(Utf8("foo")) | arrow_typeof(Int64(1)) |
+---------------------------+------------------------+
| Utf8                      | Int64                  |
+---------------------------+------------------------+
```

### `get_field`

返回映射或结构体中给定键对应的字段。
注意:大多数用户通过字段访问
语法(如 `my_struct_col['field_name']`)间接调用 `get_field`,
这会转化为对
`get_field(my_struct_col, 'field_name')` 的调用。

```sql
get_field(expression1, expression2)
```

#### 参数

- **expression1**: 要从中检索字段的映射或结构体。
- **expression2**: 映射或结构体中要检索数据的字段名。求值结果必须为字符串。

#### 示例

```sql
> create table t (idx varchar, v varchar) as values ('data','fusion'), ('apache', 'arrow');
> select struct(idx, v) from t as c;
+-------------------------+
| struct(c.idx,c.v)       |
+-------------------------+
| {c0: data, c1: fusion}  |
| {c0: apache, c1: arrow} |
+-------------------------+
> select get_field((select struct(idx, v) from t), 'c0');
+-----------------------+
| struct(t.idx,t.v)[c0] |
+-----------------------+
| data                  |
| apache                |
+-----------------------+
> select get_field((select struct(idx, v) from t), 'c1');
+-----------------------+
| struct(t.idx,t.v)[c1] |
+-----------------------+
| fusion                |
| arrow                 |
+-----------------------+
```

## JSON 函数(JSON Functions)

- [json_contains](#json_contains)
- [json_get](#json_get)
- [json_get_str](#json_get_str)
- [json_get_int](#json_get_int)
- [json_get_float](#json_get_float)
- [json_get_bool](#json_get_bool)
- [json_get_json](#json_get_json)
- [json_as_text](#json_as_text)
- [json_length](#json_length)

### `json_contains`

如果 JSON 字符串具有特定键则返回 true。此函数用于 `?` 运算符。

```sql
json_contains(json, key)
```

#### 参数

- **json**: 要操作的 JSON 字符串。可以是常量、列或函数。
- **key**: 要检查是否存在于 JSON 结构中的键。

#### 示例

```sql
> select json_contains('{"a": {"b": 1}}', 'a');
+------------------------------------------------+
| json_contains(Utf8("{\"a\": {\"b\": 1}}"),"a") |
+------------------------------------------------+
| true                                           |
+------------------------------------------------+

> select json_contains('{"user": {"name": "John", "age": 30}}', 'user');
+----------------------------------------------------------------+
| json_contains(Utf8("{\"user\": {\"name\": \"John\", \"age\": 30}}"),"user") |
+----------------------------------------------------------------+
| true                                                             |
+----------------------------------------------------------------+

> select json_contains('[1, 2, 3]', '1');
+----------------------------------------+
| json_contains(Utf8("[1, 2, 3]"),"1") |
+----------------------------------------+
| true                                   |
+----------------------------------------+
```

#### 别名

- `?` 运算符

### `json_get`

通过"路径"从 JSON 字符串中获取值。此函数用于 `->` 运算符。

```sql
json_get(json, key)
```

#### 参数

- **json**: 要操作的 JSON 字符串。可以是常量、列或函数。
- **key**: 指定所需值路径的键或数组索引。

#### 示例

```sql
> select json_get('{"a": {"b": 1}}', 'a', 'b');
+--------------------------------------------+
| json_get(Utf8("{\"a\": {\"b\": 1}}"),"a","b") |
+--------------------------------------------+
| 1                                          |
+--------------------------------------------+

> select json_get('{"user": {"name": "John", "contacts": {"email": "john@example.com"}}}', 'user', 'contacts', 'email');
+--------------------------------------------------------------------------------------------------------+
| json_get(Utf8("{\"user\": {\"name\": \"John\", \"contacts\": {\"email\": \"john@example.com\"}}}"),"user","contacts","email") |
+--------------------------------------------------------------------------------------------------------+
| "john@example.com"                                                                                      |
+--------------------------------------------------------------------------------------------------------+

> select json_get('[{"id": 1}, {"id": 2}]', '1', 'id');
+------------------------------------------------+
| json_get(Utf8("[{\"id\": 1}, {\"id\": 2}]"),"1","id") |
+------------------------------------------------+
| 2                                              |
+------------------------------------------------+
```

#### 别名

- `->` 运算符

### `json_get_str`

通过"路径"从 JSON 字符串中获取字符串值。

```sql
json_get_str(json, key)
```

#### 参数

- **json**: 要操作的 JSON 字符串。可以是常量、列或函数。
- **key**: 指定所需字符串值路径的键或数组索引。

#### 示例

```sql
> select json_get_str('{"user": {"name": "John"}}', 'user', 'name');
+----------------------------------------------------------+
| json_get_str(Utf8("{\"user\": {\"name\": \"John\"}}"),"user","name") |
+----------------------------------------------------------+
| John                                                       |
+----------------------------------------------------------+

> select json_get_str('["apple", "banana", "orange"]', '1');
+-----------------------------------------------------+
| json_get_str(Utf8("[\"apple\", \"banana\", \"orange\"]"),"1") |
+-----------------------------------------------------+
| banana                                                |
+-----------------------------------------------------+
```

### `json_get_int`

通过"路径"从 JSON 字符串中获取整数值。

```sql
json_get_int(json, key)
```

#### 参数

- **json**: 要操作的 JSON 字符串。可以是常量、列或函数。
- **key**: 指定所需整数值路径的键或数组索引。

#### 示例

```sql
> select json_get_int('{"user": {"id": 12345, "age": 30}}', 'user', 'id');
+------------------------------------------------------------+
| json_get_int(Utf8("{\"user\": {\"id\": 12345, \"age\": 30}}"),"user","id") |
+------------------------------------------------------------+
| 12345                                                       |
+------------------------------------------------------------+

> select json_get_int('[10, 20, 30]', '2');
+----------------------------------------+
| json_get_int(Utf8("[10, 20, 30]"),"2") |
+----------------------------------------+
| 30                                     |
+----------------------------------------+
```

### `json_get_float`

通过"路径"从 JSON 字符串中获取浮点数值。

```sql
json_get_float(json, key)
```

#### 参数

- **json**: 要操作的 JSON 字符串。可以是常量、列或函数。
- **key**: 指定所需浮点数值路径的键或数组索引。

#### 示例

```sql
> select json_get_float('{"product": {"price": 99.99, "weight": 1.5}}', 'product', 'price');
+----------------------------------------------------------------------------+
| json_get_float(Utf8("{\"product\": {\"price\": 99.99, \"weight\": 1.5}}"),"product","price") |
+----------------------------------------------------------------------------+
| 99.99                                                                       |
+----------------------------------------------------------------------------+

> select json_get_float('[3.14, 2.718, 1.414]', '1');
+------------------------------------------------+
| json_get_float(Utf8("[3.14, 2.718, 1.414]"),"1") |
+------------------------------------------------+
| 2.718                                          |
+------------------------------------------------+
```

### `json_get_bool`

通过"路径"从 JSON 字符串中获取布尔值。

```sql
json_get_bool(json, key)
```

#### 参数

- **json**: 要操作的 JSON 字符串。可以是常量、列或函数。
- **key**: 指定所需布尔值路径的键或数组索引。

#### 示例

```sql
> select json_get_bool('{"settings": {"active": true, "notifications": false}}', 'settings', 'active');
+------------------------------------------------------------------------------------+
| json_get_bool(Utf8("{\"settings\": {\"active\": true, \"notifications\": false}}"),"settings","active") |
+------------------------------------------------------------------------------------+
| true                                                                               |
+------------------------------------------------------------------------------------+

> select json_get_bool('[true, false, true]', '1');
+---------------------------------------------+
| json_get_bool(Utf8("[true, false, true]"),"1") |
+---------------------------------------------+
| false                                        |
+---------------------------------------------+
```

### `json_get_json`

通过"路径"从 JSON 字符串中获取嵌套的原始 JSON 字符串。

```sql
json_get_json(json, key)
```

#### 参数

- **json**: 要操作的 JSON 字符串。可以是常量、列或函数。
- **key**: 指定所需嵌套 JSON 值路径的键或数组索引。

#### 示例

```sql
> select json_get_json('{"user": {"profile": {"name": "John", "age": 30}}}', 'user', 'profile');
+------------------------------------------------------------------------------------------------+
| json_get_json(Utf8("{\"user\": {\"profile\": {\"name\": \"John\", \"age\": 30}}}"),"user","profile") |
+------------------------------------------------------------------------------------------------+
| {"name": "John", "age": 30}                                                                    |
+------------------------------------------------------------------------------------------------+

> select json_get_json('[{"id": 1}, {"id": 2}]', '1');
+------------------------------------------------+
| json_get_json(Utf8("[{\"id\": 1}, {\"id\": 2}]"),"1") |
+------------------------------------------------+
| {"id": 2}                                      |
+------------------------------------------------+
```

### `json_as_text`

通过"路径"从 JSON 字符串中获取任意值,并以字符串形式表示。此函数用于 `->>` 运算符。

```sql
json_as_text(json, key)
```

#### 参数

- **json**: 要操作的 JSON 字符串。可以是常量、列或函数。
- **key**: 指定所需值路径的键或数组索引。

#### 示例

```sql
> select json_as_text('{"a": {"b": 1}}', 'a', 'b');
+------------------------------------------------+
| json_as_text(Utf8("{\"a\": {\"b\": 1}}"),"a","b") |
+------------------------------------------------+
| "1"                                            |
+------------------------------------------------+

> select json_as_text('{"user": {"details": {"age": 30, "active": true}}}', 'user', 'details');
+-------------------------------------------------------------------------------------------+
| json_as_text(Utf8("{\"user\": {\"details\": {\"age\": 30, \"active\": true}}}"),"user","details") |
+-------------------------------------------------------------------------------------------+
| "{\"age\": 30, \"active\": true}"                                                           |
+-------------------------------------------------------------------------------------------+

> select json_as_text('["apple", "banana", "orange"]', '1');
+----------------------------------------------------+
| json_as_text(Utf8("[\"apple\", \"banana\", \"orange\"]"),"1") |
+----------------------------------------------------+
| "banana"                                            |
+----------------------------------------------------+
```

#### 别名

- `->>` 运算符

### `json_length`

获取 JSON 字符串或数组的长度。

```sql
json_length(json, key)
```

#### 参数

- **json**: 要操作的 JSON 字符串。可以是常量、列或函数。

#### 示例

```sql
> select json_length('[1, 2, 3]');
+--------------------------------+
| json_length(Utf8("[1, 2, 3]")) |
+--------------------------------+
| 3                              |
+--------------------------------+

```

:::tip
带有 `json_get` 的转换表达式会被重写为相应的方法,例如

```sql
select * from foo where json_get(attributes, 'bar')::string='ham'
```
将被重写为:
```sql
select * from foo where json_get_str(attributes, 'bar')='ham'
```
:::
