---
sidebar_position: 21
title: 编写插件
description: 构建并注册一个新组件——trait、builder、注册,以及文档契约。
---

# 编写插件

每个 ArkFlow 组件——输入、输出、处理器、缓冲、codec 或临时表——都遵循同样的四步模式。本页用一个真实的最小示例来演示整个流程:`drop` 输出(`crates/arkflow-plugin/src/output/drop.rs`)。

## 1. 实现组件 trait

每个类别在 `arkflow-core` 中都有一个 trait(`Input`、`Output`、`Processor`、`Buffer`、`Codec`,外加临时表接口)。它们都是 `async_trait`,并围绕 `MessageBatch`——ArkFlow 对 Arrow `RecordBatch` 的封装——展开:

```rust
use arkflow_core::output::{Output, OutputBuilder};
use arkflow_core::{Error, MessageBatchRef, Resource};
use async_trait::async_trait;
use std::sync::Arc;

struct DropOutput;

#[async_trait]
impl Output for DropOutput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn write(&self, _: MessageBatchRef) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}
```

数据的进出都是列式的。如果你的组件处理原始字节,请使用 codec 层,而不是自行转换为行。

## 2. 实现 builder

builder 把 YAML 配置转换为组件实例。它接收组件名、可选的 JSON 配置、可选的 codec 以及资源池:

```rust
struct DropOutputBuilder;

impl OutputBuilder for DropOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        _: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        Ok(Arc::new(DropOutput))
    }
}
```

用 `serde_json::Value` + `serde` 把配置解析到你自己的选项结构体中,让未知字段快速失败,并返回指明违规字段的 `Error`。

## 3. 注册并接线 init

注册发生在插件 crate 的 `init()` 中:

```rust
pub fn init() -> Result<(), Error> {
    register_output_builder("drop", Arc::new(DropOutputBuilder))?;
    register_output_metadata(ComponentMetadata::unit(
        "drop",
        "Discards all messages. Useful for performance benchmarks and dead-end pipelines.",
    ))
}
```

然后沿调用链向上接线:

1. 在 `crates/arkflow-plugin/src/mod.rs` 中,从该类别的 `init()` 调用你模块的 `init()`。
2. 该类别的 `init()` 已经由 `crates/arkflow/src/main.rs` 调用——标准插件无需修改二进制。

你注册的类型名(上文的 `"drop"`)就是用户在 YAML 的 `type:` 字段中写下的那个字符串。

## 4. 履行文档契约

文档会对照注册表做机器校验;以下各项通过之前,插件 PR 不能算完成:

1. **重新生成清单** —— 在你运行以下命令之前,快照测试会一直失败:
   ```bash
   ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot
   ```
   这会依据你的 `ComponentMetadata`(描述、配置 schema、配置示例)刷新 `docs/reference/component-inventory.json` 与 `docs/static/config-schema.json`,所以请让它们保持有意义。
2. **声明页面归属** —— 在 `docs/docs/components/` 下新增或更新页面,并在 front matter 中声明你的组件:
   ```text
   ---
   components: [output/my_component]
   ---
   ```
   有清单条目却没有声明页面会导致 `pnpm docs:check` 失败;声明了未知组件的页面同样会失败。
3. **添加示例 YAML** —— 放在 `examples/` 下并登记到 `docs/reference/example-manifest.json`。示例由一个工作区测试离线深度校验;无法离线校验的示例需要显式的 `"validate": false` 加上原因。
4. **更新 README** —— `README.md` 与 `README_zh.md` 的组件列表必须使用准确的注册表类型名并保持一致(由 CI 校验)。

## 检查清单

- [ ] trait 已实现(`async_trait`,输入输出为 `MessageBatch`)。
- [ ] builder 防御性地解析配置;错误信息指明字段。
- [ ] 以准确的 YAML `type:` 名称注册 + 带描述、schema、示例的元数据。
- [ ] `init()` 经插件 `mod.rs` 中该类别的 `init()` 接线。
- [ ] 清单已重新生成(`ARKFLOW_REGENERATE_DOCS=1 ...`)。
- [ ] 组件页面带 `components:` 归属 front matter。
- [ ] 示例 YAML 已登记并通过校验。
- [ ] `README.md` / `README_zh.md` 列表已同步更新。
- [ ] `cargo test --workspace` 与 `cd docs && pnpm docs:check` 均为绿色。

## 相关页面

- [统一执行内核](./kernel.md) — 你的组件运行在哪里。
- [组件清单](../reference/component-inventory.md) — 生成的注册表表格。
- [IDE 自动补全](../reference/ide-schema.md) — 你的元数据所供给的配置 schema。
