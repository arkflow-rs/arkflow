## Why

PLANNING.md 7.3-5 的阶段 4 遗留项「Hub secret: scheme」：secret-references 机制设计时显式将 `${secret:NAME}` 保留为未知 scheme（前向兼容），但尚未提供中心化密钥的实际 scheme。企业部署需要一个**独立于通用环境变量**的密钥命名空间：运维可以只为进程注入 `ARKFLOW_SECRET_*` 前缀的变量、按前缀审计与授权，而不必把业务环境变量与密钥混在同一命名空间。`secret:` scheme 即该约定：`${secret:NAME}` 映射为环境变量 `ARKFLOW_SECRET_<NAME>`，在配置物化进程解析——单进程 Hub 部署由此获得「密钥只进 Hub 进程」的中心化模型；多节点部署在物化节点设置对应变量（文档明示），Hub 侧分发前预解析留待后续。

## What Changes

- `crates/arkflow-core/src/secret.rs`：`resolve_reference` 新增 `secret:` scheme——`${secret:NAME}` 读取环境变量 `ARKFLOW_SECRET_<NAME>`（名字逐字映射，大小写敏感），支持 `${secret:NAME:-default}` 默认值语法；未设置时按既有错误语义（指明配置路径与引用，不含值）。
- 文档：secret-references 节补充 `secret:` scheme 与命名空间约定（en/zh）；部署文档提示多节点场景。

## Capabilities

### New Capabilities

<!-- 无新能力：扩展 secret-references。 -->

### Modified Capabilities

- `secret-references`: 已知 scheme 集合新增 `secret:`（含默认值语法与 ARKFLOW_SECRET_ 前缀映射）；既有行为不变。

## Impact

- `crates/arkflow-core/src/secret.rs`：scheme 分支 + 单测；无新增依赖。
- 文档：secret-references 节（en/zh）。

## Non-goals

- 不做 Hub 分发前预解析（dispatch payload 改写，涉及 hub 存储与分发链路，另行立项）。
- 不做文件/KMS/Vault 后端密钥存储；不做密钥轮换与吊销。
- 不做名字大小写规范化（映射逐字保留，可预测优先）。
