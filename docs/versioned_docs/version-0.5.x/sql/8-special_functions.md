# Special Functions

## Expansion Functions

- [unnest](#unnest)
- [unnest(struct)](#unnest-struct)

### `unnest`

Expands an array or map into rows.

#### Arguments

- **array**: Array expression to unnest.
  Can be a constant, column, or function, and any combination of array operators.

#### Examples

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

Expand a struct fields into individual columns.

#### Arguments

- **struct**: Object expression to unnest.
  Can be a constant, column, or function, and any combination of object operators.

#### Examples

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