---
sidebar_position: 25
title: 基准测试
description: 运行 ArkFlow 可复现的基准套件——场景、方法学与报告。
---

# 基准测试

ArkFlow 内置一套自包含的基准场景,覆盖内核的主要路径。克隆仓库后任何人都可以复现——无需网络、无需外部服务、无需下载数据集。

## 运行

```bash
cargo run -p arkflow --release --example benchmark
```

必须有 release 模式(debug 构建测的是 rustc,不是 ArkFlow)。可用参数:

| Flag | 默认值 | 描述 |
| --- | --- | --- |
| `--count` | `200000` | 每个流场景的行数。codec 与 state 场景会独立缩放操作数,保证整套耗时可控。 |
| `--runs` | `3` | 测量轮数;报告取每场景最优耗时。 |
| `--warmup` | `1` | 不计时的预热轮。 |
| `--json` | 关闭 | 输出机器可读报告而非 markdown。 |

默认参数在笔记本上通常几分钟内跑完(耗时大头是持久化 state-backend 场景),输出一张 markdown 表:每个场景一行,含工作负载、操作数、耗时与吞吐。

## 场景

| 场景 | 路径 | 计量单位 |
| --- | --- | --- |
| `linear-sql` | generate → JSON 解码 → SQL 整批聚合 → drop | rows/s |
| `groupby-sql` | generate → JSON 解码 → SQL 按键聚合(有状态 GROUP BY)→ drop | rows/s |
| `filter-project-sql` | generate → JSON 解码 → SQL 过滤 + 投影 → drop | rows/s |
| `codec-json` | Arrow 批次 → NDJSON → Arrow 批次往返(1000 行/批) | batches/s |
| `state-backend` | redb 状态后端持久化 put + get(逐写提交) | ops/s |
| `avro-decode-w5` / `avro-decode-w25` / `avro-decode-w100` | schema-registry Avro 解码(Confluent wire format,1000 消息/批,5/25/100 字段 schema) | rows/s |

流式场景走引擎自身的公开入口(`compile_stream` + `run_job`),测的就是生产执行路径,而非专用测试台。Avro 场景同样走真实的 `schema_registry` codec(内存版 schema resolver)——消息在(不计时的)setup 阶段预编码,测量轮命中按 id 的 schema 缓存,即生产稳态路径。

## 方法学

- 套件先预热一轮,再测量 `--runs` 轮,取每场景最优耗时(在嘈杂的机器上,best-of 是吞吐测量的标准做法)。
- 基准从不对性能数值做通过/失败断言——在慢机器上会失败的基准毫无价值。回归跟踪由你完成:在同一台机器上对两个 commit 跑同一命令并对比报告。
- `--json` 输出与 markdown 相同的场景与吞吐数值,便于 CI 或仪表盘摄取。
- state-backend 场景测的是持久化写(每次 put 都提交);其操作数刻意小于流行数。

## CI

基准套件也会在 CI 中自动运行(`.github/workflows/benchmark.yml`):每次合并到 `main`、手动触发、以及携带 `benchmark` 标签的 PR。结果以 markdown 表格呈现在 workflow 运行页的 job summary,JSON 报告作为以提交 SHA 命名的 artifact 上传(保留 90 天)供跨运行对比。GitHub 托管 runner 的硬件随代际波动,跨运行对比仅供参考——工作流刻意不对吞吐数值做通过/失败判定。

## 相关

- `crates/arkflow/examples/benchmark.rs` —— CLI 入口。
- `crates/arkflow-plugin/src/benchmark.rs` —— 场景库。
- `crates/arkflow-plugin/tests/kernel_perf_baseline.rs` —— 保留的吞吐回归哨兵测试。
