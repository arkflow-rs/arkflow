---
sidebar_position: 1
---

# 安装

## 前置条件

- **Rust ≥ 1.97**(工具链,用于从源码构建)

无需安装 protobuf 编译器:Protobuf 编解码器的 `.proto` 文件由内置的纯 Rust `protox` 编译器在**运行时**编译,构建过程也不调用 `protoc`(仓库没有 `build.rs`)。

## 从源码构建

```bash
git clone https://github.com/arkflow-rs/arkflow.git
cd arkflow

# 优化的 release 构建
cargo build --release

# (可选)运行测试套件
cargo test
```

二进制位于 `./target/release/arkflow`。

## 运行

```bash
# 使用配置文件启动
./target/release/arkflow --config config.yaml

# 仅校验配置,不启动引擎
./target/release/arkflow --config config.yaml --validate
```

## 组件发现与 Schema

ArkFlow 提供 CLI 命令,列出所有已注册组件并打印其配置 schema。它们为编辑器自动补全提供数据,在编写配置时也非常顺手。

```bash
# 列出所有 input / output / processor / buffer / codec
./target/release/arkflow components list
./target/release/arkflow components list --kind input

# 查看某个组件的配置 schema(文本或 JSON)
./target/release/arkflow components show input kafka
./target/release/arkflow components show processor sql --format json

# 导出整个引擎配置的 JSON Schema
./target/release/arkflow schema > arkflow.schema.json
```

将编辑器的 YAML language server 指向生成的 schema,即可获得字段级补全与校验。

继续阅读[快速入门](./2-quickstart.md)。
