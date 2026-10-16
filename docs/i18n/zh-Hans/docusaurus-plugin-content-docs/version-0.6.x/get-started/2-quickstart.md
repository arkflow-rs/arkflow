---
sidebar_position: 2
---

# 快速入门

本指南运行一条小小的流水线:生成模拟 JSON 读数,用 SQL 过滤,然后把结果打印到控制台。

## 1. 创建配置

将以下内容保存为 `config.yaml`:

```yaml validate=fragment wrap=engine
logging:
  level: info

streams:
  - input:
      type: "generate"
      context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
      interval: 1s
      batch_size: 10
    pipeline:
      thread_num: 4
      processors:
        - type: "json_to_arrow"
        - type: "sql"
          query: "SELECT * FROM flow WHERE value >= 10"
    output:
      type: "stdout"
    error_output:
      type: "stdout"
```

这段配置的作用:

- `generate` 每秒生成一批模拟 JSON 数据。
- `json_to_arrow` 把原始字节解析为 Arrow 列,让 SQL 可以直接读取。
- `sql` 只保留 `value >= 10` 的行。
- `stdout` 把每一批写到控制台;`error_output` 捕获失败的数据。

## 2. 校验

```bash
./target/release/arkflow --config config.yaml --validate
```

## 3. 运行

```bash
./target/release/arkflow --config config.yaml
```

## 接下来去哪里

- [核心概念](../build/architecture.md) —— 理解流、流水线与数据流动。
- [配置参考](/zh-Hans/docs/reference/configuration) —— 完整的顶层 YAML 结构。
- [组件目录](/zh-Hans/docs/reference/component-inventory) —— 挑选真实的输入与输出组件。
