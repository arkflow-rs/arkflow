---
sidebar_position: 2
---

# IDE 自动补全

ArkFlow 发布引擎配置的 JSON Schema,编辑器可以据此校验配置并在输入时提供字段级补全。该 schema 内嵌了每个已注册组件的配置 schema,因此补全能按 `type:` 生效——选择 `kafka`,编辑器就会给出 `brokers`、`topics` 与 `consumer_group`。

## 下载 schema

当前开发版本的 schema 随文档一起发布:

- [`config-schema.json`](/config-schema.json) — 完整的引擎配置
  schema(同时作为站点资源内嵌)

它由引擎自身生成并由 CI 快照:如果引擎的配置面发生变化,必须重新生成提交的资源,否则构建失败。你也可以随时在本地输出它:

```bash
./target/release/arkflow schema > arkflow.schema.json
```

## 编辑器配置

### Visual Studio Code(YAML 扩展)

安装 [YAML 扩展](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml),然后将该 schema 与 ArkFlow 配置文件关联。可以在配置文件顶部添加一行 modeline 注释:

```yaml validate=full
# yaml-language-server: $schema=https://arkflow-rs.com/config-schema.json
```

或者在用户或工作区的 `settings.json` 中全局映射:

```json
{
  "yaml.schemas": {
    "https://arkflow-rs.com/config-schema.json": ["arkflow.yaml", "arkflow.yml"]
  }
}
```

### JetBrains IDE

在 **Settings → Languages & Frameworks →
Schemas and DTDs → JSON Schema Maps** 下启用 JSON Schema 映射(或使用
[File Watchers 插件](https://www.jetbrains.com/help/idea/using-file-watchers.html)
搭配 YAML schema provider),指向
`https://arkflow-rs.com/config-schema.json`。

### 离线使用

让编辑器指向本地生成的 schema,而不是已发布的 URL:

```bash
./target/release/arkflow schema > arkflow.schema.json
```

```yaml validate=full
# yaml-language-server: $schema=./arkflow.schema.json
```

## schema 覆盖范围

- 顶层结构:`logging`、`health_check`、`streams` 与 `jobs`。
- 每个类别下所有已注册组件(input、output、processor、buffer、
  codec、temporary),以 `type` 判别联合表示,包含每个组件各自的配置字段与示例。
- 由统一内核执行的声明式 `jobs` 结构。

已注册组件的权威列表是[组件清单](./component-inventory.md);[组件](/zh-Hans/docs/components/inputs/kafka)下的每个组件页面都深入记录了组件行为。
