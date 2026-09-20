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

### 支持的数据类型

VRL 值可与以下 Arrow 类型相互映射:

- **字符串(String)**(Utf8)
- **整数(Integer)**:Int8、Int16、Int32、Int64
- **浮点数(Float)**:Float32、Float64
- **布尔值(Boolean)**
- **二进制(Binary)**
- **时间戳(Timestamp)**
- **空值(Null)**
