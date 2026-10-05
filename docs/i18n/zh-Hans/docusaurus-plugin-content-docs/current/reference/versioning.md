---
title: 版本策略与升级指南
description: ArkFlow 的产品版本策略、v1.0 冻结面范围，以及部署升级步骤。
---

# 版本策略与升级指南

本页说明**产品**的版本策略与升级路径。文档站点自身的版本快照机制请看
[兼容性与版本策略](./compatibility.md)；逐版本的变更清单请看仓库中的
[CHANGELOG](https://github.com/arkflow-rs/arkflow/blob/main/CHANGELOG.md)。

## 版本策略

ArkFlow 自 v1.0 起遵循[语义化版本](https://semver.org/)：

- **主版本（MAJOR）**——对下列任何稳定面的破坏性变更。
- **次版本（MINOR）**——新增组件、配置键与特性，完全向后兼容。
- **修订号（PATCH）**——缺陷修复与内部改动，不影响配置。

v1.0 之前（0.x 版本），次版本可能携带破坏性变更；每一次这类变更都会在该版本的
CHANGELOG 小节顶部明确列出。

### v1.0 冻结面范围

v1.0 之后，以下表面纳入兼容性承诺：

- **YAML 配置面**——既有键保持名称与语义不变；生成的 JSON Schema
  （`arkflow schema`）反映稳定面。
- 两个二进制的 **CLI 契约**：`arkflow`（运行、`--validate`、`components`、
  `schema`）与 `arkflow-server`（启动环境变量与 `migrate` 子命令）。
- **组件注册表类型名**——每个 `input`/`output`/`processor`/`buffer`/`codec`/`wal`
  的类型字符串在次版本升级中保持可用。
- **已发布 crate 的公共 API**（`arkflow-core`、`arkflow-plugin`），面向插件作者。
- **指标与追踪标签词表**（`arkflow_job_*` 指标名与标签键），保证监控看板在升级后
  仍然有效。

运行规格中固化的行为契约（投递语义、checkpoint/恢复保证）属于同一承诺：次版本
升级不得静默削弱 at-least-once 或 exactly-once 保证。

## 发布产物

每个 tag 版本在 GitHub Releases 发布：

- Linux（amd64、arm64）与 macOS（amd64、arm64）的二进制压缩包，内含
  `arkflow`、`arkflow-server`、LICENSE 与 README，并附 SHA-256 校验和。
- 既有的 Docker tag 通道容器镜像，以及仓库中的 Helm chart。

## 升级部署

1. **先读 CHANGELOG。** 标记为破坏性变更的条目会列出需要调整的确切键或参数；
   每个版本小节顶部的升级说明是权威清单。
2. **换二进制前先用新版本校验配置**：

   ```bash
   arkflow --config config.yaml --validate
   ```

   校验器会深度检查流、作业图与配置规则，在变更生效前抓出被移除或改名的键。
3. **备份状态目录**——WAL 目录、checkpoint/状态存储、Hub 存储数据库——以便回滚
   二进制并重放。
4. **单节点引擎**：优雅停止进程（SIGTERM 会触发 WAL close-drain 路径），替换
   二进制后重启；启动时缓冲的输入会从 WAL 重放。
5. **Hub/Agent 集群**：先升级 Hub，再升级 Agent。Agent 会向 Hub 上报兼容状态，
   滚动过程中持续关注它；排水与维护操作见
   [控制面运维指南](../operate/control-plane/operations.md)。
6. **存储 schema 迁移**（Hub）：当版本说明要求时执行 `arkflow-server migrate`——
   子命令契约见 [CLI 参考](./cli.md)。

如果某个版本引入了不兼容的 checkpoint 或状态格式，版本说明会明确声明，恢复路径
会以精确报错拒绝旧快照，而不是误读它。
