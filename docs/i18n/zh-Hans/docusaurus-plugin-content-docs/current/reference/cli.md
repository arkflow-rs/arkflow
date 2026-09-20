---
sidebar_position: 10
title: CLI 参考
description: "`arkflow` 的全部命令、标志(flag)与退出行为。"
---

# CLI 参考

`arkflow` 二进制是一个单一可执行文件:它从 YAML 配置运行流与作业,并提供用于发现组件注册表的命令。

```bash
arkflow [OPTIONS] [COMMAND]
```

## 运行引擎

| 选项 | 描述 |
|--------|-------------|
| `-c, --config <FILE>` | YAML 配置文件路径。除发现类子命令外必填。 |
| `-v, --validate` | 校验配置后退出,不启动引擎。 |
| `--version` | 打印版本并退出。 |

```bash
# 运行配置中定义的流与作业
./target/release/arkflow --config config.yaml

# 深度校验一份配置:解析、流 id、声明的作业图、
# 重复 id、算子引用以及配置规则
./target/release/arkflow --config config.yaml --validate
```

`--validate` 不止于反序列化:它检查流 id 的唯一性,校验每一条声明的作业规格(图结构与算子引用),并运行引擎的配置规则检查。校验失败会打印 `Invalid configuration: ...` 并以非零状态退出,因此该标志适用于 CI 流水线与准入钩子。

## `components list`

按类别分组列出所有已注册组件。

```bash
arkflow components list [--kind KIND] [--format FORMAT]
```

| 选项 | 描述 |
|--------|-------------|
| `-k, --kind <KIND>` | 按组件类别过滤:`input`、`output`、`processor`、`buffer`、`codec`、`temporary`。 |
| `-f, --format <FORMAT>` | `text`(默认,对齐列)或 `json`(机器可读的注册表导出)。 |

```bash
$ arkflow components list --kind codec
codec:
  json        JSON codec for parsing and serializing message payloads
  protobuf    Protobuf codec using a compiled .proto descriptor
  ...
```

`json` 格式与生成[组件清单](./component-inventory.md)所用的注册表导出完全相同,因此脚本与文档永远不会和二进制不一致。

## `components show`

打印单个组件的配置 schema。

```bash
arkflow components show <KIND> <NAME> [--format FORMAT]
```

| 参数 | 描述 |
|----------|-------------|
| `<KIND>` | 组件类别(必填)。 |
| `<NAME>` | 已注册组件的类型名(必填)。 |
| `-f, --format <FORMAT>` | `text`(默认)或 `json`。 |

```bash
$ arkflow components show input kafka
kafka: Kafka input component for consuming messages from Kafka topics
kind: input
config_optional: no

Example:
{ ... pretty-printed config example ... }

Config schema:
{ ... JSON Schema for the component config ... }
```

未知名称会失败并列出合法备选项,例如 `Unknown input type: kafaka. Available input types: file, generate, http, kafka, ...`。

## `schema`

打印整个引擎配置的 JSON Schema(draft 2020-12)。把它交给你的编辑器即可获得 YAML 自动补全与悬停文档——参见 [IDE 自动补全](./ide-schema.md)。

```bash
arkflow schema > arkflow.schema.json
```

## 退出行为

| 调用方式 | 行为 |
|------------|----------|
| `components ...`、`schema` | 打印结果并立即退出;不读取任何配置文件。 |
| `--validate` | 校验配置,记录日志 `The config is validated.`,随后退出且不启动引擎。 |
| `--config <FILE>`(默认) | 校验配置,随后启动引擎并阻塞直至关闭。 |
| 缺少 `--config` 且无子命令 | 错误:`missing --config <FILE> (or run a subcommand: components, schema)`。 |

## 相关页面

- [组件清单](./component-inventory.md) — 由 CLI 读取的同一注册表生成。
- [顶层配置](./configuration.md) — `--config` 接受的内容。
- [IDE 自动补全](./ide-schema.md) — 在编辑器中使用 `arkflow schema`。
