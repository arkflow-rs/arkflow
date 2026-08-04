# Operators and Literals

## Numerical Operators

- + (plus)
- - (minus)
- \* (multiply)
- / (divide)
- % (modulo)

 

### `+`

Addition

```sql
> SELECT 1 + 2;
+---------------------+
| Int64(1) + Int64(2) |
+---------------------+
| 3                   |
+---------------------+
```

 

### `-`

Subtraction

```sql
> SELECT 4 - 3;
+---------------------+
| Int64(4) - Int64(3) |
+---------------------+
| 1                   |
+---------------------+
```

 

### `*`

Multiplication

```sql
> SELECT 2 * 3;
+---------------------+
| Int64(2) * Int64(3) |
+---------------------+
| 6                   |
+---------------------+
```

 

### `/`

Division (integer division truncates toward zero)

```sql
> SELECT 8 / 4;
+---------------------+
| Int64(8) / Int64(4) |
+---------------------+
| 2                   |
+---------------------+
```

 

### `%`

Modulo (remainder)

```sql
> SELECT 7 % 3;
+---------------------+
| Int64(7) % Int64(3) |
+---------------------+
| 1                   |
+---------------------+
```

## Comparison Operators

- = (equal)
- != (not equal)
- < (less than)
- `<=` (less than or equal to)
- > (greater than)
- >= (greater than or equal to)
- `<=>` (three-way comparison, alias for IS NOT DISTINCT FROM)
- [IS DISTINCT FROM](#is-distinct-from)
- [IS NOT DISTINCT FROM](#is-not-distinct-from)
- ~ (regex match)
- ~\* (regex case-insensitive match)
- !~ (not regex match)
- !~\* (not regex case-insensitive match)

 

### `=`

Equal

```sql
> SELECT 1 = 1;
+---------------------+
| Int64(1) = Int64(1) |
+---------------------+
| true                |
+---------------------+
```

 

### `!=`

Not Equal

```sql
> SELECT 1 != 2;
+----------------------+
| Int64(1) != Int64(2) |
+----------------------+
| true                 |
+----------------------+
```



### `<`

Less Than

```sql
> SELECT 3 < 4;
+---------------------+
| Int64(3) < Int64(4) |
+---------------------+
| true                |
+---------------------+
```



### `<=`

Less Than or Equal To

```sql
> SELECT 3 <= 3;
+----------------------+
| Int64(3) <= Int64(3) |
+----------------------+
| true                 |
+----------------------+
```



### `>`

Greater Than

```sql
> SELECT 6 > 5;
+---------------------+
| Int64(6) > Int64(5) |
+---------------------+
| true                |
+---------------------+
```



### `>=`

Greater Than or Equal To

```sql
> SELECT 5 >= 5;
+----------------------+
| Int64(5) >= Int64(5) |
+----------------------+
| true                 |
+----------------------+
```



### `<=>`

Three-way comparison operator. A NULL-safe operator that returns true if both operands are equal or both are NULL, false otherwise.

```sql
> SELECT NULL <=> NULL;
+--------------------------------+
| NULL IS NOT DISTINCT FROM NULL |
+--------------------------------+
| true                           |
+--------------------------------+
```

```sql
> SELECT 1 <=> NULL;
+------------------------------------+
| Int64(1) IS NOT DISTINCT FROM NULL |
+------------------------------------+
| false                              |
+------------------------------------+
```

```sql
> SELECT 1 <=> 2;
+----------------------------------------+
| Int64(1) IS NOT DISTINCT FROM Int64(2) |
+----------------------------------------+
| false                                  |
+----------------------------------------+
```

```sql
> SELECT 1 <=> 1;
+----------------------------------------+
| Int64(1) IS NOT DISTINCT FROM Int64(1) |
+----------------------------------------+
| true                                   |
+----------------------------------------+
```

### `IS DISTINCT FROM`

Guarantees the result of a comparison is `true` or `false` and not an empty set

```sql
> SELECT 0 IS DISTINCT FROM NULL;
+--------------------------------+
| Int64(0) IS DISTINCT FROM NULL |
+--------------------------------+
| true                           |
+--------------------------------+
```

### `IS NOT DISTINCT FROM`

The negation of `IS DISTINCT FROM`

```sql
> SELECT NULL IS NOT DISTINCT FROM NULL;
+--------------------------------+
| NULL IS NOT DISTINCT FROM NULL |
+--------------------------------+
| true                           |
+--------------------------------+
```



### `~`

Regex Match

```sql
> SELECT 'datafusion' ~ '^datafusion(-cli)*';
+-------------------------------------------------+
| Utf8("datafusion") ~ Utf8("^datafusion(-cli)*") |
+-------------------------------------------------+
| true                                            |
+-------------------------------------------------+
```



### `~*`

Regex Case-Insensitive Match

```sql
> SELECT 'datafusion' ~* '^DATAFUSION(-cli)*';
+--------------------------------------------------+
| Utf8("datafusion") ~* Utf8("^DATAFUSION(-cli)*") |
+--------------------------------------------------+
| true                                             |
+--------------------------------------------------+
```



### `!~`

Not Regex Match

```sql
> SELECT 'datafusion' !~ '^DATAFUSION(-cli)*';
+--------------------------------------------------+
| Utf8("datafusion") !~ Utf8("^DATAFUSION(-cli)*") |
+--------------------------------------------------+
| true                                             |
+--------------------------------------------------+
```



### `!~*`

Not Regex Case-Insensitive Match

```sql
> SELECT 'datafusion' !~* '^DATAFUSION(-cli)+';
+---------------------------------------------------+
| Utf8("datafusion") !~* Utf8("^DATAFUSION(-cli)+") |
+---------------------------------------------------+
| true                                              |
+---------------------------------------------------+
```

### `~~`

Like Match

```sql
SELECT 'datafusion' ~~ 'dat_f%n';
+---------------------------------------+
| Utf8("datafusion") ~~ Utf8("dat_f%n") |
+---------------------------------------+
| true                                  |
+---------------------------------------+
```

### `~~*`

Case-Insensitive Like Match

```sql
SELECT 'datafusion' ~~* 'Dat_F%n';
+----------------------------------------+
| Utf8("datafusion") ~~* Utf8("Dat_F%n") |
+----------------------------------------+
| true                                   |
+----------------------------------------+
```

### `!~~`

Not Like Match

```sql
SELECT 'datafusion' !~~ 'Dat_F%n';
+----------------------------------------+
| Utf8("datafusion") !~~ Utf8("Dat_F%n") |
+----------------------------------------+
| true                                   |
+----------------------------------------+
```

### `!~~*`

Not Case-Insensitive Like Match

```sql
SELECT 'datafusion' !~~* 'Dat%F_n';
+-----------------------------------------+
| Utf8("datafusion") !~~* Utf8("Dat%F_n") |
+-----------------------------------------+
| true                                    |
+-----------------------------------------+
```

## Logical Operators

- [AND](#and)
- [OR](#or)

### `AND`

Logical And

```sql
> SELECT true AND true;
+---------------------------------+
| Boolean(true) AND Boolean(true) |
+---------------------------------+
| true                            |
+---------------------------------+
```

### `OR`

Logical Or

```sql
> SELECT false OR true;
+---------------------------------+
| Boolean(false) OR Boolean(true) |
+---------------------------------+
| true                            |
+---------------------------------+
```

## Bitwise Operators

- & (bitwise and)
- | (bitwise or)
- # (bitwise xor)
- >> (bitwise shift right)
- `<<` (bitwise shift left)



### `&`

Bitwise And

```sql
> SELECT 5 & 3;
+---------------------+
| Int64(5) & Int64(3) |
+---------------------+
| 1                   |
+---------------------+
```



### `|`

Bitwise Or

```sql
> SELECT 5 | 3;
+---------------------+
| Int64(5) | Int64(3) |
+---------------------+
| 7                   |
+---------------------+
```



### `#`

Bitwise Xor (interchangeable with `^`)

```sql
> SELECT 5 # 3;
+---------------------+
| Int64(5) # Int64(3) |
+---------------------+
| 6                   |
+---------------------+
```



### `>>`

Bitwise Shift Right

```sql
> SELECT 5 >> 3;
+----------------------+
| Int64(5) >> Int64(3) |
+----------------------+
| 0                    |
+----------------------+
```



### `<<`

Bitwise Shift Left

```sql
> SELECT 5 << 3;
+----------------------+
| Int64(5) << Int64(3) |
+----------------------+
| 40                   |
+----------------------+
```

## Other Operators

- || (string concatenation)
- @> (array contains)
- `<@` (array is contained by)



### `||`

String Concatenation

```sql
> SELECT 'Hello, ' || 'DataFusion!';
+----------------------------------------+
| Utf8("Hello, ") || Utf8("DataFusion!") |
+----------------------------------------+
| Hello, DataFusion!                     |
+----------------------------------------+
```



### `@>`

Array Contains

```sql
> SELECT make_array(1,2,3) @> make_array(1,3);
+-------------------------------------------------------------------------+
| make_array(Int64(1),Int64(2),Int64(3)) @> make_array(Int64(1),Int64(3)) |
+-------------------------------------------------------------------------+
| true                                                                    |
+-------------------------------------------------------------------------+
```



### `<@`

Array Is Contained By

```sql
> SELECT make_array(1,3) <@ make_array(1,2,3);
+-------------------------------------------------------------------------+
| make_array(Int64(1),Int64(3)) <@ make_array(Int64(1),Int64(2),Int64(3)) |
+-------------------------------------------------------------------------+
| true                                                                    |
+-------------------------------------------------------------------------+
```

## Literals

Use single quotes for literal values. For example, the string `foo bar` is
referred to using `'foo bar'`

```sql
select 'foo';
```

### Escaping

Unlike many other languages, SQL literals do not by default support C-style escape
sequences such as `\n` for newline. Instead all characters in a `'` string are treated
literally.

To escape `'` in SQL literals, use `''`:

```sql
> select 'it''s escaped';
+----------------------+
| Utf8("it's escaped") |
+----------------------+
| it's escaped         |
+----------------------+
1 row(s) fetched.
```

Strings such as `foo\nbar` mean `\` followed by `n` (not newline):

```sql
> select 'foo\nbar';
+------------------+
| Utf8("foo\nbar") |
+------------------+
| foo\nbar         |
+------------------+
1 row(s) fetched.
Elapsed 0.005 seconds.
```

To add escaped characters such as newline or tab, instead of `\n` you use the
`E` style strings. For example, to add the text with a newline

```text
foo
bar
```

You can use `E'foo\nbar'`

```sql
> select E'foo\nbar';
+-----------------+
| Utf8("foo
bar") |
+-----------------+
| foo
bar         |
+-----------------+
```