---
components: [vrl]
description: ArkFlow 文档页面。
---

# VRL

VRL 处理器使用 Vector Remap Language(VRL)来变换消息——这是一种为可观测性数据流水线设计的安全表达式语言。每个传入批次都会被映射为 VRL 对象,所配置语句的结果再投影回列式批次。VRL 语法参考见 https://vector.dev/docs/reference/vrl/。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `vrl` |
| statement | string | yes | — | 用于变换每条消息的 VRL 程序。 |
| timezone | string | no | — | 在程序中解析/格式化时间值时使用的时区(如 `UTC`、`Asia/Shanghai`)。 |

输入列到 VRL 值的映射采用显式报错而非静默丢失:`UInt64` 值超出 `i64::MAX` 时整批报错(VRL 整数是 i64——不再静默回绕为负数);不在支持集合内的列类型(布尔、整数、浮点、utf8/binary、日期、时间戳、null 之外,如 `List`/`Struct`)会报错并指明列名与类型。请先用 `filter_columns` 或 SQL processor 移除这类列。

## 示例

```yaml validate=fragment wrap=processors
- type: "vrl"
  statement: ".v2, err = .value * 2; ."
```

### 完整流水线示例

```yaml validate=full
streams:
  - input:
      type: "generate"
      context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
      interval: 1s
      batch_size: 1

    pipeline:
      thread_num: 4
      processors:
        - type: "json_to_arrow"
        - type: "vrl"
          statement: ".v2, err = .value * 2; ."
          timezone: "UTC"
        - type: "arrow_to_json"

    output:
      type: "stdout"
```

## 说明

### 输出批粒度

一个输入批**至多产出一个输出批**。程序对每一输入行执行一次，所有结果行按输入顺序收集进同一个列式批。若某行的结果是**对象数组**，其元素按顺序展开为连续多行（扇出）：例如 2 行输入、首行返回两个对象，则产出一个 3 行的批。若所有行都没有产出结果，processor 不发出任何批。

由于多行合并，输出 schema 是该批内出现过的**全部字段的并集**（按首次出现顺序）；某行缺少的字段补为 `null`，列类型取该字段的首个非 null 值。若同一字段在不同行的非 null 值**类型不一致**（如一行整数、另一行字符串或浮点），整批会显式报错并指出字段名与两处类型——一个批只能有一种列类型，请用 `to_float()`/`to_string()` 等统一字段类型。每一行的结果都必须是对象（或对象数组）——任意一行返回标量都会使整批显式报错，而不会被静默填成全 null 行。若程序对每一行都返回空数组 `[]`，则不产出任何批。

批粒度得以保持对下游按批处理的组件很重要：`vrl` 之后的 SQL 聚合看到的是完整的输入批（每批一个聚合结果），而不是每输入行一个单行批。

### 支持的数据类型

VRL 值可与以下 Arrow 类型相互映射:

- **字符串(String)**(Utf8)
- **整数(Integer)**:Int8、Int16、Int32、Int64
- **浮点数(Float)**:Float32、Float64
- **布尔值(Boolean)**
- **二进制(Binary)**
- **时间戳(Timestamp)**
- **空值(Null)**
