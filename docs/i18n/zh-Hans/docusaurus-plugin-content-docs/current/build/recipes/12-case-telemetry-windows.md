---
sidebar_position: 12
description: 端到端案例——IoT 传感器遥测按窗口分批,并按传感器聚合。
---

# 案例:IoT 遥测与窗口聚合

一间工厂车间有数百个温度传感器,每隔几秒发布一次读数。把每条原始读数都写进存储是浪费;监控看板只需要每 10 秒一次的按传感器统计。

## 需求

- 聚合而非存储:每个时间间隔对每个传感器求 min/max/avg/count
- 支撑数百个传感器(按传感器键分组)
- 数据源可替换:开发环境用 `generate`,生产环境用 MQTT 或 Kafka,下游配置完全相同

## 架构

```
┌─────────┐ readings ┌──────────────────┐  window flush  ┌───────────┐
│ Sensors │─────────▶│ tumbling_window  │───────────────▶│ SQL group │──▶ stdout / DB
└─────────┘          │ buffer (10 s)    │  one batch     │ by sensor │
                     └──────────────────┘                └───────────┘
```

窗口**缓冲(Buffer)**累积原始读数,每个时间间隔释放一个批次;SQL 处理器在该批次之上计算聚合。

## 配置

已校验示例:[`examples/case_telemetry_windows.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/case_telemetry_windows.yaml)

```yaml validate=full
streams:
  - id: telemetry-window-agg
    input:
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
      thread_num: 4
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

## 运行与预期结果

```bash
./target/release/arkflow --config examples/case_telemetry_windows.yaml --validate
./target/release/arkflow --config examples/case_telemetry_windows.yaml
```

大约每 10 秒出现一行按传感器汇总的记录——输入间隔为 100 ms 时,`samples` 接近 100,min/max/avg 反映该窗口的情况。

## 权衡与变体

- **迟到读数**:若传感器会重试,这一点就必须考虑——窗口释放与迟到处理在[流式作业](/zh-Hans/docs/build/jobs)的事件时间与迟到处理部分,以及[窗口缓冲参考](/zh-Hans/docs/components/buffers/tumbling_window)中均有描述。
- 把 `generate` 换成 `mqtt`,其余保持不变——该数据源产生相同结构的 JSON 负载(参见[MQTT 输入参考](/zh-Hans/docs/components/inputs/mqtt))。
- 切换输出,把同样的聚合结果指向 InfluxDB 而不是 stdout(参见[InfluxDB 输出参考](/zh-Hans/docs/components/outputs/influxdb))。
