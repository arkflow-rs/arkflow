---
sidebar_position: 3
description: 将消息分批到滚动或会话窗口中,并用 SQL 聚合。
---

# 实战:在窗口中聚合

把消息按时间窗口分批,并用 SQL 计算每个窗口的聚合值。

**前提条件**

- 已[安装](/zh-Hans/docs/get-started/install) ArkFlow
- 无需任何外部服务——本指南使用 `generate` 数据源,可直接原样运行

## 选择窗口类型

| 缓冲(Buffer)类型 | 行为 | 典型用途 |
|-------------|----------|-------------|
| `tumbling_window` | 固定、不重叠的时间区间 | 周期性报表、批量写入 |
| `session_window` | 只要间隔不超过超时时间,窗口就保持打开 | 用户会话 |

重叠/滑动窗口请使用 Job DAG 的 `window` 算子(`kind: sliding`,事件时间)——参见[事件时间语义](/zh-Hans/docs/build/distributed-jobs#事件时间语义)。旧式 `sliding_window` **缓冲**会被流编译器拒绝:其按行数的窗口无法映射到事件时间窗口算子。

## 配置

完整且经过 CI 校验的配置见 [`examples/case_telemetry_windows.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/case_telemetry_windows.yaml):

```yaml validate=full
streams:
  - input:
      type: generate
      context: '{ "sensor": "temp_1", "value": 21.5, "ts": 1757000000000 }'
      interval: 100ms
      batch_size: 1

    buffer:
      type: tumbling_window
      interval: 10s

    state:
      backend: embedded_kv
      durability: ephemeral

    pipeline:
      processors:
        - type: json_to_arrow
        - type: sql
          query: |
            SELECT
              sensor,
              count(*) AS samples,
              min(value) AS min_value,
              max(value) AS max_value,
              avg(value) AS avg_value
            FROM flow
            GROUP BY sensor
        - type: arrow_to_json

    output:
      type: stdout
```

整个流程是:消息在**缓冲**(即窗口)中累积,每隔一个 `interval`,整个窗口作为一个批次释放进流水线,由 SQL 计算聚合。切换为 `session_window` 只需改动 `buffer` 块(`session_window` 接受 `timeout`)。

## 运行与验证

```bash
./target/release/arkflow --config windows.yaml
```

预期结果:大约每 10 秒,每个传感器输出一行汇总,`samples` 约为 100(即 10 秒内 100 条 100 ms 间隔的消息),并附带该窗口的最小值/最大值/平均值。

## 故障排查

- **没有任何输出** —— 窗口缓冲只在时间间隔走完后才释放;请等满一个完整间隔。
- **对字符串 JSON 字段做聚合失败** —— 确认 `json_to_arrow` 已先行解码负载,且查询引用的是解码后的列名。
- **需要 join 两个数据源** —— 带旧式 `join` 字段的窗口缓冲会在编译时失败。流-流 join 是 Job DAG 的能力:运行一个带 `join` 算子(恰好两条入边)的 job——参见[双流 join](/zh-Hans/docs/build/distributed-jobs#双流-join)。
