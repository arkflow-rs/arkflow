---
description: ArkFlow 文档页面。
---

# 特殊函数(Special Functions)

## 展开函数(Expansion Functions)

- [unnest](#unnest)
- [unnest(struct)](#unnest-struct)

### `unnest`

将数组或映射展开为行。

#### 参数

- **array**: 要展开的数组表达式。
  可以是常量、列或函数,以及任意数组运算符的组合。

#### 示例

```sql
> select unnest(make_array(1, 2, 3, 4, 5)) as unnested;
+----------+
| unnested |
+----------+
| 1        |
| 2        |
| 3        |
| 4        |
| 5        |
+----------+
```

```sql
> select unnest(range(0, 10)) as unnested_range;
+----------------+
| unnested_range |
+----------------+
| 0              |
| 1              |
| 2              |
| 3              |
| 4              |
| 5              |
| 6              |
| 7              |
| 8              |
| 9              |
+----------------+
```

### `unnest (struct)`

将 struct 的字段展开为独立的列。

#### 参数

- **struct**: 要展开的对象表达式。
  可以是常量、列或函数,以及任意对象运算符的组合。

#### 示例

```sql
> create table foo as values ({a: 5, b: 'a string'}), ({a:6, b: 'another string'});

> create view foov as select column1 as struct_column from foo;

> select * from foov;
+---------------------------+
| struct_column             |
+---------------------------+
| {a: 5, b: a string}       |
| {a: 6, b: another string} |
+---------------------------+

> select unnest(struct_column) from foov;
+------------------------------------------+------------------------------------------+
| unnest_placeholder(foov.struct_column).a | unnest_placeholder(foov.struct_column).b |
+------------------------------------------+------------------------------------------+
| 5                                        | a string                                 |
| 6                                        | another string                           |
+------------------------------------------+------------------------------------------+
```
